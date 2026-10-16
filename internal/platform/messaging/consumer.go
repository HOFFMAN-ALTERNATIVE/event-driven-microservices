// Package messaging runs idempotent JetStream consumers with bounded retries,
// exponential back-off and dead-lettering.
//
// Processing contract for every message:
//
//  1. Decode the envelope; garbage goes straight to the DLQ.
//  2. In ONE local transaction: insert (consumer, event_id) into the inbox
//     table, run the handler (which mutates state and enqueues follow-up events
//     into the outbox), commit. A duplicate delivery hits the inbox primary key
//     and is acknowledged without side effects.
//  3. Ack after commit. If the ack is lost the message is redelivered and step 2
//     deduplicates it.
//  4. Transient errors are NAKed with back-off; permanent errors, or transient
//     ones that exceed MaxDeliver attempts, are published to "dlq.<consumer>"
//     with diagnostic headers and then terminated.
package messaging

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/events"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/db"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/telemetry"
)

// Dead-letter headers.
const (
	HeaderDLQSubject  = "Dlq-Original-Subject"
	HeaderDLQConsumer = "Dlq-Consumer"
	HeaderDLQError    = "Dlq-Error"
	HeaderDLQAttempts = "Dlq-Attempts"
	HeaderDLQSeq      = "Dlq-Stream-Seq"
)

// ErrPermanent marks failures that retrying cannot fix (validation errors,
// unknown references, business invariant violations).
var ErrPermanent = errors.New("permanent failure")

// Permanent wraps err so the consumer dead-letters it without retrying.
func Permanent(err error) error { return fmt.Errorf("%w: %w", ErrPermanent, err) }

// Handler mutates state inside tx. It must not perform non-transactional side
// effects other than idempotent ones (e.g. an outbound webhook with an
// idempotency key), because a failed commit leads to a retry.
type Handler func(ctx context.Context, tx *sql.Tx, ev events.Envelope) error

// Consumer describes one durable subscription.
type Consumer struct {
	Name        string   // durable consumer name and inbox key
	EventTypes  []string // e.g. events.OrderCreated
	Handler     Handler
	MaxDeliver  int                             // attempts before dead-lettering (default 5)
	Backoff     func(attempt int) time.Duration // default: exponential with jitter
	AckWait     time.Duration                   // default 30s
	AfterCommit func()                          // e.g. outbox relay Kick
}

// Publisher is the subset of JetStream needed to dead-letter messages.
type Publisher interface {
	PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// Runner binds consumers to a database and a JetStream context.
type Runner struct {
	db     *sql.DB
	js     jetstream.JetStream
	dlq    Publisher
	log    *slog.Logger
	tracer trace.Tracer
}

// NewRunner creates a runner. dlq may be nil, in which case js is used.
func NewRunner(d *sql.DB, js jetstream.JetStream, dlq Publisher, log *slog.Logger) *Runner {
	if dlq == nil {
		dlq = js
	}
	return &Runner{db: d, js: js, dlq: dlq, log: log, tracer: telemetry.Tracer("messaging")}
}

// DefaultBackoff is 250ms·2^(attempt-1) with ±20% jitter, capped at 15s.
func DefaultBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := 250 * time.Millisecond << min(attempt-1, 6)
	d = min(d, 15*time.Second)
	jitter := time.Duration(rand.Int64N(int64(d)/5+1)) - d/10 //nolint:gosec // jitter, not security
	return d + jitter
}

func (c *Consumer) defaults() {
	if c.MaxDeliver <= 0 {
		c.MaxDeliver = 5
	}
	if c.Backoff == nil {
		c.Backoff = DefaultBackoff
	}
	if c.AckWait <= 0 {
		c.AckWait = 30 * time.Second
	}
}

// Start creates (or updates) the durable consumer and begins processing.
// The returned stop function drains in-flight messages.
func (r *Runner) Start(ctx context.Context, c Consumer) (stop func(), err error) {
	c.defaults()
	subjects := make([]string, len(c.EventTypes))
	for i, t := range c.EventTypes {
		subjects[i] = events.Subject(t)
	}
	cons, err := r.js.CreateOrUpdateConsumer(ctx, events.StreamEvents, jetstream.ConsumerConfig{
		Durable:        c.Name,
		Description:    "edm consumer " + c.Name,
		FilterSubjects: subjects,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        c.AckWait,
		// Redelivery is unbounded on the server on purpose: the consumer counts
		// attempts itself and dead-letters explicitly, so a crash on the last
		// attempt can never make the server silently drop a message.
		MaxDeliver:    -1,
		MaxAckPending: 256,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("consumer %s: %w", c.Name, err)
	}
	cc, err := cons.Consume(func(m jetstream.Msg) { r.Handle(ctx, c, m) },
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			r.log.Warn("consume error", "consumer", c.Name, "err", err)
		}))
	if err != nil {
		return nil, err
	}
	return cc.Drain, nil
}

// Handle processes a single message. Exported for unit tests.
func (r *Runner) Handle(ctx context.Context, c Consumer, m jetstream.Msg) {
	c.defaults()
	attempt := 1
	var streamSeq uint64
	if md, err := m.Metadata(); err == nil {
		attempt = int(min(md.NumDelivered, 1<<31)) //nolint:gosec // bounded above
		streamSeq = md.Sequence.Stream
	}

	ctx = telemetry.Extract(ctx, m.Headers())
	ctx, span := r.tracer.Start(ctx, "process "+m.Subject(),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.consumer.group.name", c.Name),
			attribute.String("messaging.destination.name", m.Subject()),
			attribute.Int("messaging.delivery.attempt", attempt),
		))
	defer span.End()
	log := r.log.With("consumer", c.Name, "subject", m.Subject(), "attempt", attempt)

	ev, err := events.Decode(m.Data())
	if err != nil {
		r.deadLetter(ctx, c, m, streamSeq, attempt, err, log, span)
		return
	}
	span.SetAttributes(attribute.String("messaging.message.id", ev.ID), attribute.String("order.id", ev.OrderID))

	duplicate := false
	err = r.process(ctx, c, ev, &duplicate)
	switch {
	case err == nil:
		if duplicate {
			span.SetAttributes(attribute.Bool("messaging.duplicate", true))
			log.InfoContext(ctx, "duplicate delivery ignored", "event_id", ev.ID)
		} else if c.AfterCommit != nil {
			c.AfterCommit()
		}
		actx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := m.DoubleAck(actx); err != nil {
			// Safe: redelivery will be deduplicated by the inbox.
			log.WarnContext(ctx, "ack failed", "err", err)
		}
	case errors.Is(err, ErrPermanent) || errors.Is(err, events.ErrMalformed) || attempt >= c.MaxDeliver:
		r.deadLetter(ctx, c, m, streamSeq, attempt, err, log, span)
	default:
		delay := c.Backoff(attempt)
		span.RecordError(err)
		span.SetStatus(codes.Error, "retrying")
		log.WarnContext(ctx, "handler failed, will retry", "err", err, "retry_in", delay.String(), "event_id", ev.ID)
		_ = m.NakWithDelay(delay)
	}
}

func (r *Runner) process(ctx context.Context, c Consumer, ev events.Envelope, duplicate *bool) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("handler panic: %v", p)
		}
	}()
	return db.InTx(ctx, r.db, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO inbox (consumer, event_id) VALUES (?, ?)`, c.Name, ev.ID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			*duplicate = true
			return nil
		}
		return c.Handler(ctx, tx, ev)
	})
}

func (r *Runner) deadLetter(ctx context.Context, c Consumer, m jetstream.Msg, seq uint64, attempt int, cause error, log *slog.Logger, span trace.Span) {
	span.RecordError(cause)
	span.SetStatus(codes.Error, "dead-lettered")
	span.SetAttributes(attribute.Bool("messaging.dead_lettered", true))

	dlq := nats.NewMsg(events.DLQPrefix + c.Name)
	dlq.Data = m.Data()
	for k, vs := range m.Headers() {
		for _, v := range vs {
			dlq.Header.Add(k, v)
		}
	}
	dlq.Header.Del(nats.MsgIdHdr)
	dlq.Header.Set(HeaderDLQSubject, m.Subject())
	dlq.Header.Set(HeaderDLQConsumer, c.Name)
	dlq.Header.Set(HeaderDLQError, truncate(cause.Error(), 1024))
	dlq.Header.Set(HeaderDLQAttempts, strconv.Itoa(attempt))
	dlq.Header.Set(HeaderDLQSeq, strconv.FormatUint(seq, 10))

	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// Msg id makes the DLQ publish itself idempotent if we crash before Term.
	if _, err := r.dlq.PublishMsg(pctx, dlq, jetstream.WithMsgID(c.Name+":"+strconv.FormatUint(seq, 10))); err != nil {
		log.ErrorContext(ctx, "dead-letter publish failed, will retry", "err", err)
		_ = m.NakWithDelay(c.Backoff(attempt))
		return
	}
	log.ErrorContext(ctx, "message dead-lettered", "err", cause, "dlq_subject", dlq.Subject)
	_ = m.TermWithReason("dead-lettered")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

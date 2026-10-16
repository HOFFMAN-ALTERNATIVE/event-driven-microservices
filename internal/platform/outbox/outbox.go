// Package outbox implements the transactional outbox pattern.
//
// Domain state and the events describing it are written in the SAME local
// transaction (Enqueue). A relay then publishes pending rows to JetStream and
// marks them as sent. If the process crashes between publish and mark, the row
// is published again on restart; JetStream drops the duplicate because the
// event id is sent as Nats-Msg-Id, and consumers are idempotent anyway, so the
// end-to-end guarantee is effectively-once processing.
package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/events"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/telemetry"
)

// HeaderEventType is set on every published message for cheap routing/debugging.
const HeaderEventType = "Event-Type"

// Enqueue stores ev in the outbox as part of tx. The current trace context is
// persisted with the row so the asynchronous publish continues the same trace.
func Enqueue(ctx context.Context, tx *sql.Tx, ev events.Envelope) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	headers, err := json.Marshal(telemetry.Inject(ctx))
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO outbox (event_id, subject, payload, headers) VALUES (?, ?, ?, ?)`,
		ev.ID, events.Subject(ev.Type), payload, string(headers))
	if err != nil {
		return fmt.Errorf("outbox enqueue %s: %w", ev.Type, err)
	}
	return nil
}

// Publisher is the subset of jetstream.JetStream used by the relay.
type Publisher interface {
	PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// Relay drains the outbox table to JetStream in commit order.
type Relay struct {
	db        *sql.DB
	pub       Publisher
	log       *slog.Logger
	tracer    trace.Tracer
	kick      chan struct{}
	Interval  time.Duration // poll interval when idle
	BatchSize int
	Retention time.Duration // how long published rows are kept
}

// NewRelay creates a relay with sensible defaults.
func NewRelay(db *sql.DB, pub Publisher, log *slog.Logger) *Relay {
	return &Relay{
		db: db, pub: pub, log: log,
		tracer:    telemetry.Tracer("outbox"),
		kick:      make(chan struct{}, 1),
		Interval:  500 * time.Millisecond,
		BatchSize: 100,
		Retention: 24 * time.Hour,
	}
}

// Kick wakes the relay immediately (call after committing a transaction that
// enqueued events). It never blocks.
func (r *Relay) Kick() {
	select {
	case r.kick <- struct{}{}:
	default:
	}
}

// Run publishes until ctx is cancelled. Failures back off exponentially (capped)
// and never lose or reorder events.
func (r *Relay) Run(ctx context.Context) error {
	wait := r.Interval
	lastPrune := time.Now()
	for {
		n, err := r.Flush(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			r.log.Warn("outbox flush failed", "err", err)
			wait = min(wait*2, 10*time.Second)
		case n == r.BatchSize:
			wait = 0 // more rows are probably waiting
		default:
			wait = r.Interval
		}
		if time.Since(lastPrune) > time.Minute {
			lastPrune = time.Now()
			if err := r.Prune(ctx); err != nil && ctx.Err() == nil {
				r.log.Warn("outbox prune failed", "err", err)
			}
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-r.kick:
			timer.Stop()
		case <-timer.C:
		}
	}
}

type row struct {
	seq     int64
	eventID string
	subject string
	payload []byte
	headers map[string]string
}

// Flush publishes up to BatchSize pending rows in order and returns how many
// were published. It stops at the first failure so ordering is preserved.
func (r *Relay) Flush(ctx context.Context) (int, error) {
	rows, err := r.pending(ctx)
	if err != nil {
		return 0, err
	}
	for i, rw := range rows {
		if err := r.publish(ctx, rw); err != nil {
			r.markFailed(ctx, rw.seq, err)
			return i, err
		}
		if _, err := r.db.ExecContext(ctx,
			`UPDATE outbox SET published_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'), attempts = attempts + 1 WHERE seq = ?`, rw.seq); err != nil {
			return i, fmt.Errorf("mark published: %w", err)
		}
	}
	return len(rows), nil
}

func (r *Relay) pending(ctx context.Context) ([]row, error) {
	q, err := r.db.QueryContext(ctx,
		`SELECT seq, event_id, subject, payload, headers FROM outbox WHERE published_at IS NULL ORDER BY seq LIMIT ?`, r.BatchSize)
	if err != nil {
		return nil, err
	}
	defer q.Close()
	var out []row
	for q.Next() {
		var rw row
		var hdr string
		if err := q.Scan(&rw.seq, &rw.eventID, &rw.subject, &rw.payload, &hdr); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(hdr), &rw.headers); err != nil {
			rw.headers = nil // corrupt trace headers must not block delivery
		}
		out = append(out, rw)
	}
	return out, q.Err()
}

func (r *Relay) publish(ctx context.Context, rw row) error {
	msg := nats.NewMsg(rw.subject)
	msg.Data = rw.payload
	for k, v := range rw.headers {
		msg.Header.Set(k, v)
	}
	// Continue the trace of the transaction that produced the event.
	pctx := telemetry.Extract(ctx, msg.Header)
	pctx, span := r.tracer.Start(pctx, "publish "+rw.subject,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.destination.name", rw.subject),
			attribute.String("messaging.message.id", rw.eventID),
		))
	defer span.End()
	telemetry.Propagator.Inject(pctx, telemetry.HeaderCarrier(msg.Header))
	msg.Header.Set(HeaderEventType, rw.subject[len(events.SubjectPrefix):])

	tctx, cancel := context.WithTimeout(pctx, 5*time.Second)
	defer cancel()
	ack, err := r.pub.PublishMsg(tctx, msg, jetstream.WithMsgID(rw.eventID))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "publish failed")
		return fmt.Errorf("publish %s: %w", rw.subject, err)
	}
	if ack != nil && ack.Duplicate {
		span.SetAttributes(attribute.Bool("messaging.duplicate", true))
		r.log.DebugContext(pctx, "outbox duplicate suppressed by jetstream", "event_id", rw.eventID)
	}
	return nil
}

func (r *Relay) markFailed(ctx context.Context, seq int64, cause error) {
	if errors.Is(cause, context.Canceled) {
		return
	}
	_, _ = r.db.ExecContext(context.WithoutCancel(ctx),
		`UPDATE outbox SET attempts = attempts + 1, last_error = ? WHERE seq = ?`, cause.Error(), seq)
}

// Prune removes rows published longer ago than Retention.
func (r *Relay) Prune(ctx context.Context) error {
	cutoff := time.Now().Add(-r.Retention).UTC().Format("2006-01-02T15:04:05.000Z")
	_, err := r.db.ExecContext(ctx, `DELETE FROM outbox WHERE published_at IS NOT NULL AND published_at < ?`, cutoff)
	return err
}

// Pending returns the number of unpublished rows (exposed on /readyz).
func Pending(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
	return n, err
}

package outbox

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/events"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/db"
)

type fakePub struct {
	mu     sync.Mutex
	msgs   []*nats.Msg
	ids    []string
	failAt int // fail the n-th publish (1-based), 0 = never
	calls  int
}

func (p *fakePub) PublishMsg(_ context.Context, m *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.calls == p.failAt {
		return nil, errors.New("nats unavailable")
	}
	p.msgs = append(p.msgs, m)
	p.ids = append(p.ids, m.Header.Get(HeaderEventType))
	return &jetstream.PubAck{Stream: events.StreamEvents}, nil
}

func newDB(t *testing.T) *sql.DB {
	t.Helper()
	d, err := db.Open(context.Background(), filepath.Join(t.TempDir(), "o.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func enqueue(t *testing.T, ctx context.Context, d *sql.DB, types ...string) []events.Envelope {
	t.Helper()
	var out []events.Envelope
	err := db.InTx(ctx, d, func(tx *sql.Tx) error {
		for _, typ := range types {
			ev, err := events.New(typ, "o-1", map[string]int{"n": 1})
			if err != nil {
				return err
			}
			if err := Enqueue(ctx, tx, ev); err != nil {
				return err
			}
			out = append(out, ev)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestFlushPublishesInOrderWithMsgIDAndTrace(t *testing.T) {
	d := newDB(t)
	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("t").Start(context.Background(), "http POST /orders")
	evs := enqueue(t, ctx, d, events.OrderCreated, events.OrderConfirmed, events.OrderCancelled)
	span.End()

	pub := &fakePub{}
	r := NewRelay(d, pub, quiet())
	n, err := r.Flush(context.Background())
	if err != nil || n != 3 {
		t.Fatalf("flush = %d, %v", n, err)
	}
	for i, m := range pub.msgs {
		if m.Subject != events.Subject(evs[i].Type) {
			t.Fatalf("out of order: %d got %s want %s", i, m.Subject, evs[i].Type)
		}
		if m.Header.Get("traceparent") == "" {
			t.Fatalf("trace context not propagated: %v", m.Header)
		}
	}
	if p, _ := Pending(context.Background(), d); p != 0 {
		t.Fatalf("pending = %d", p)
	}
	// Nothing left to publish.
	if n, _ := r.Flush(context.Background()); n != 0 {
		t.Fatalf("second flush published %d", n)
	}
}

func TestFlushStopsAtFirstFailureAndResumes(t *testing.T) {
	d := newDB(t)
	enqueue(t, context.Background(), d, events.OrderCreated, events.OrderConfirmed, events.OrderCancelled)
	pub := &fakePub{failAt: 2}
	r := NewRelay(d, pub, quiet())
	n, err := r.Flush(context.Background())
	if err == nil || n != 1 {
		t.Fatalf("want 1 published + error, got %d, %v", n, err)
	}
	var attempts int
	var lastErr sql.NullString
	_ = d.QueryRow(`SELECT attempts, last_error FROM outbox WHERE published_at IS NULL ORDER BY seq LIMIT 1`).Scan(&attempts, &lastErr)
	if attempts != 1 || !lastErr.Valid {
		t.Fatalf("failure not recorded: attempts=%d err=%v", attempts, lastErr)
	}
	n, err = r.Flush(context.Background())
	if err != nil || n != 2 {
		t.Fatalf("resume: %d, %v", n, err)
	}
	want := []string{events.OrderCreated, events.OrderConfirmed, events.OrderCancelled}
	for i, w := range want {
		if pub.ids[i] != w {
			t.Fatalf("order broken after retry: %v", pub.ids)
		}
	}
}

func TestEnqueueIsAtomicWithBusinessWrite(t *testing.T) {
	d := newDB(t)
	_ = db.InTx(context.Background(), d, func(tx *sql.Tx) error {
		ev, _ := events.New(events.OrderCreated, "o", struct{}{})
		_ = Enqueue(context.Background(), tx, ev)
		return errors.New("business rule failed")
	})
	if p, _ := Pending(context.Background(), d); p != 0 {
		t.Fatalf("event leaked from rolled back tx: pending=%d", p)
	}
}

func TestPrune(t *testing.T) {
	d := newDB(t)
	enqueue(t, context.Background(), d, events.OrderCreated)
	r := NewRelay(d, &fakePub{}, quiet())
	if _, err := r.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.Retention = -time.Second // everything published is "old"
	if err := r.Prune(context.Background()); err != nil {
		t.Fatal(err)
	}
	var n int
	_ = d.QueryRow(`SELECT count(*) FROM outbox`).Scan(&n)
	if n != 0 {
		t.Fatalf("prune left %d rows", n)
	}
}

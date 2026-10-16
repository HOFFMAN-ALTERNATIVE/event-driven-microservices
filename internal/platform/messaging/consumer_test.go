package messaging

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

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/events"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/db"
)

// fakeMsg implements the parts of jetstream.Msg the runner uses.
type fakeMsg struct {
	jetstream.Msg
	subject   string
	data      []byte
	hdr       nats.Header
	delivered uint64
	seq       uint64

	acked, termed bool
	nakDelay      time.Duration
}

func (m *fakeMsg) Subject() string      { return m.subject }
func (m *fakeMsg) Data() []byte         { return m.data }
func (m *fakeMsg) Headers() nats.Header { return m.hdr }
func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: m.delivered, Sequence: jetstream.SequencePair{Stream: m.seq}}, nil
}
func (m *fakeMsg) DoubleAck(context.Context) error    { m.acked = true; return nil }
func (m *fakeMsg) NakWithDelay(d time.Duration) error { m.nakDelay = d; return nil }
func (m *fakeMsg) TermWithReason(string) error        { m.termed = true; return nil }

type fakePub struct {
	mu   sync.Mutex
	msgs []*nats.Msg
	err  error
}

func (p *fakePub) PublishMsg(_ context.Context, m *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return nil, p.err
	}
	p.msgs = append(p.msgs, m)
	return &jetstream.PubAck{}, nil
}

func setup(t *testing.T) (*sql.DB, *Runner, *fakePub) {
	t.Helper()
	d, err := db.Open(context.Background(), filepath.Join(t.TempDir(), "m.db"), `CREATE TABLE side (event_id TEXT)`)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	pub := &fakePub{}
	return d, NewRunner(d, nil, pub, slog.New(slog.NewTextHandler(io.Discard, nil))), pub
}

func envelope(t *testing.T) []byte {
	t.Helper()
	e, err := events.New(events.OrderCreated, "o-1", events.OrderCreatedV1{CustomerID: "c"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := jsonMarshal(e)
	return b
}

func count(t *testing.T, d *sql.DB, q string) int {
	t.Helper()
	var n int
	if err := d.QueryRow(q).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestHandlerRunsOnceForDuplicates(t *testing.T) {
	d, r, _ := setup(t)
	commits := 0
	c := Consumer{
		Name: "svc", AfterCommit: func() { commits++ },
		Handler: func(ctx context.Context, tx *sql.Tx, ev events.Envelope) error {
			_, err := tx.ExecContext(ctx, `INSERT INTO side VALUES (?)`, ev.ID)
			return err
		},
	}
	body := envelope(t)
	for i := 1; i <= 3; i++ {
		m := &fakeMsg{subject: "events.orders.created", data: body, delivered: uint64(i)}
		r.Handle(context.Background(), c, m)
		if !m.acked {
			t.Fatalf("delivery %d not acked", i)
		}
	}
	if n := count(t, d, `SELECT count(*) FROM side`); n != 1 {
		t.Fatalf("side effect applied %d times, want 1", n)
	}
	if commits != 1 {
		t.Fatalf("AfterCommit called %d times, want 1", commits)
	}
}

func TestTransientErrorRollsBackAndNaks(t *testing.T) {
	d, r, pub := setup(t)
	c := Consumer{Name: "svc", MaxDeliver: 3, Backoff: func(int) time.Duration { return time.Second },
		Handler: func(ctx context.Context, tx *sql.Tx, ev events.Envelope) error {
			_, _ = tx.ExecContext(ctx, `INSERT INTO side VALUES (?)`, ev.ID)
			return errors.New("db hiccup")
		}}
	m := &fakeMsg{subject: "events.orders.created", data: envelope(t), delivered: 1}
	r.Handle(context.Background(), c, m)
	if m.acked || m.termed || m.nakDelay != time.Second {
		t.Fatalf("want nak(1s), got %+v", m)
	}
	if count(t, d, `SELECT count(*) FROM side`)+count(t, d, `SELECT count(*) FROM inbox`) != 0 {
		t.Fatal("transaction was not rolled back")
	}
	if len(pub.msgs) != 0 {
		t.Fatal("should not dead-letter before MaxDeliver")
	}
}

func TestExhaustedRetriesGoToDLQ(t *testing.T) {
	_, r, pub := setup(t)
	c := Consumer{Name: "svc", MaxDeliver: 3,
		Handler: func(context.Context, *sql.Tx, events.Envelope) error { return errors.New("still down") }}
	m := &fakeMsg{subject: "events.orders.created", data: envelope(t), delivered: 3, seq: 42,
		hdr: nats.Header{"traceparent": []string{"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}}}
	r.Handle(context.Background(), c, m)
	if !m.termed || m.acked {
		t.Fatalf("want term, got %+v", m)
	}
	if len(pub.msgs) != 1 {
		t.Fatalf("want 1 DLQ message, got %d", len(pub.msgs))
	}
	got := pub.msgs[0]
	if got.Subject != "dlq.svc" || got.Header.Get(HeaderDLQSubject) != "events.orders.created" ||
		got.Header.Get(HeaderDLQAttempts) != "3" || got.Header.Get(HeaderDLQSeq) != "42" ||
		got.Header.Get(HeaderDLQError) != "still down" || got.Header.Get("traceparent") == "" {
		t.Fatalf("bad DLQ message: %s %v", got.Subject, got.Header)
	}
}

func TestPermanentAndMalformedSkipRetries(t *testing.T) {
	_, r, pub := setup(t)
	perm := Consumer{Name: "svc", Handler: func(context.Context, *sql.Tx, events.Envelope) error {
		return Permanent(errors.New("unknown sku"))
	}}
	m1 := &fakeMsg{subject: "events.orders.created", data: envelope(t), delivered: 1}
	r.Handle(context.Background(), perm, m1)
	m2 := &fakeMsg{subject: "events.orders.created", data: []byte("not json"), delivered: 1}
	r.Handle(context.Background(), perm, m2)
	if !m1.termed || !m2.termed || len(pub.msgs) != 2 {
		t.Fatalf("want both dead-lettered: m1=%+v m2=%+v dlq=%d", m1, m2, len(pub.msgs))
	}
}

func TestDLQPublishFailureNaksInsteadOfDropping(t *testing.T) {
	_, r, pub := setup(t)
	pub.err = errors.New("nats down")
	c := Consumer{Name: "svc", Backoff: func(int) time.Duration { return 7 * time.Millisecond },
		Handler: func(context.Context, *sql.Tx, events.Envelope) error { return nil }}
	m := &fakeMsg{subject: "x", data: []byte("{"), delivered: 1}
	r.Handle(context.Background(), c, m)
	if m.termed || m.nakDelay != 7*time.Millisecond {
		t.Fatalf("message must be retried when DLQ is unavailable: %+v", m)
	}
}

func TestPanicIsRetried(t *testing.T) {
	_, r, _ := setup(t)
	c := Consumer{Name: "svc", Backoff: func(int) time.Duration { return time.Millisecond },
		Handler: func(context.Context, *sql.Tx, events.Envelope) error { panic("bug") }}
	m := &fakeMsg{subject: "x", data: envelope(t), delivered: 1}
	r.Handle(context.Background(), c, m)
	if m.nakDelay != time.Millisecond {
		t.Fatalf("panic should be retried, got %+v", m)
	}
}

func TestDefaultBackoff(t *testing.T) {
	prevMax := time.Duration(0)
	for a := 1; a <= 10; a++ {
		d := DefaultBackoff(a)
		if d <= 0 || d > 18*time.Second {
			t.Fatalf("attempt %d: backoff %v out of range", a, d)
		}
		if a <= 6 && d < prevMax/2 {
			t.Fatalf("attempt %d: backoff %v not growing", a, d)
		}
		prevMax = d
	}
}

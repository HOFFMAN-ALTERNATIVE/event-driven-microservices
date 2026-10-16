//go:build integration

package integration

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/dlq"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/events"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/orders"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/payments"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/messaging"
)

const wait = 15 * time.Second

func TestIntegrationHappyPathConfirmsOrderAndTracesEndToEnd(t *testing.T) {
	c := startCluster(t, options{})
	ctx := context.Background()

	o, code := c.placeOrder("alice", 12_999, "", events.Item{SKU: "sku-keyboard", Qty: 2}, events.Item{SKU: "sku-mouse", Qty: 1})
	if code != http.StatusAccepted || o.Status != orders.StatusPending {
		t.Fatalf("create: %d %+v", code, o)
	}
	got := c.awaitStatus(o.ID, orders.StatusConfirmed, wait)
	if len(got.History) < 3 {
		t.Fatalf("expected PENDING→RESERVED→CONFIRMED history, got %+v", got.History)
	}

	// Reservation committed: available decremented, nothing left reserved.
	eventually(t, wait, "reservation committed", func() bool {
		s, _ := c.inventory.Get(ctx, "sku-keyboard")
		return s.Available == 23 && s.Reserved == 0
	})
	acct, err := c.payments.Account(ctx, "alice")
	if err != nil || acct.BalanceCents != 50_000-12_999 {
		t.Fatalf("balance: %+v %v", acct, err)
	}
	eventually(t, wait, "confirmation notification", func() bool {
		n, _ := c.notifier.List(ctx, o.ID)
		return len(n) == 1 && n[0].Kind == events.OrderConfirmed
	})

	// One trace spans HTTP → outbox → 4 services.
	eventually(t, wait, "spans recorded", func() bool { return len(spansFor(o.ID)) >= 6 })
	spans := spansFor(o.ID)
	trace := spans[0].SpanContext().TraceID()
	consumers := map[string]bool{}
	for _, s := range spans {
		if s.SpanContext().TraceID() != trace {
			t.Fatalf("span %q is in a different trace", s.Name())
		}
		for _, a := range s.Attributes() {
			if a.Key == "messaging.consumer.group.name" {
				consumers[a.Value.AsString()] = true
			}
		}
	}
	for _, want := range []string{"inventory", "payments", "orders-saga", "notifier"} {
		if !consumers[want] {
			t.Fatalf("no span from consumer %s in trace (have %v)", want, consumers)
		}
	}
	root := false
	for _, s := range recorder.Ended() {
		if s.SpanContext().TraceID() == trace && s.Name() == "POST /orders" {
			root = true
		}
	}
	if !root {
		t.Fatal("HTTP server span is not the root of the saga trace")
	}
}

func spansFor(orderID string) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range recorder.Ended() {
		for _, a := range s.Attributes() {
			if a.Key == attribute.Key("order.id") && a.Value.AsString() == orderID {
				out = append(out, s)
			}
		}
	}
	return out
}

func TestIntegrationOutOfStockRejectsWithoutTouchingPayments(t *testing.T) {
	c := startCluster(t, options{})
	ctx := context.Background()
	o, _ := c.placeOrder("bob", 1_000, "", events.Item{SKU: "sku-monitor", Qty: 6})
	got := c.awaitStatus(o.ID, orders.StatusRejected, wait)
	if !strings.Contains(got.Reason, "insufficient stock") {
		t.Fatalf("reason: %q", got.Reason)
	}
	if _, err := c.payments.Payment(ctx, o.ID); err == nil {
		t.Fatal("payment must not be attempted for a rejected order")
	}
	o2, _ := c.placeOrder("bob", 1_000, "", events.Item{SKU: "sku-unknown", Qty: 1})
	if got := c.awaitStatus(o2.ID, orders.StatusRejected, wait); !strings.Contains(got.Reason, "unknown sku") {
		t.Fatalf("reason: %q", got.Reason)
	}
}

func TestIntegrationPaymentFailureCompensatesInventory(t *testing.T) {
	c := startCluster(t, options{})
	ctx := context.Background()

	// Customer can only afford 1.00 via the payments HTTP API.
	req, _ := http.NewRequest(http.MethodPut, c.payHTTP.URL+"/accounts/carol", strings.NewReader(`{"balance_cents":100}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("top-up: %v %v", resp, err)
	}
	resp.Body.Close()

	o, _ := c.placeOrder("carol", 49_900, "", events.Item{SKU: "sku-monitor", Qty: 3})
	got := c.awaitStatus(o.ID, orders.StatusCancelled, wait)
	if got.Reason != "insufficient funds" {
		t.Fatalf("reason %q", got.Reason)
	}
	// Compensation: stock is fully restored and the saga log records it.
	eventually(t, wait, "stock released", func() bool {
		s, _ := c.inventory.Get(ctx, "sku-monitor")
		return s.Available == 5 && s.Reserved == 0
	})
	eventually(t, wait, "compensation recorded in saga history", func() bool {
		o, _ := c.orders.Get(ctx, o.ID)
		for _, h := range o.History {
			if strings.Contains(h.Cause, "compensated") {
				return true
			}
		}
		return false
	})
	if a, _ := c.payments.Account(ctx, "carol"); a.BalanceCents != 100 {
		t.Fatalf("balance changed on failed payment: %d", a.BalanceCents)
	}
	eventually(t, wait, "cancellation notification", func() bool {
		n, _ := c.notifier.List(ctx, o.ID)
		return len(n) == 1 && strings.Contains(n[0].Message, "released")
	})
}

func TestIntegrationNoOversellUnderConcurrency(t *testing.T) {
	c := startCluster(t, options{})
	ctx := context.Background()
	const n = 12
	ids := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			o, code := c.placeOrder("dave", 100, "", events.Item{SKU: "sku-gpu", Qty: 1})
			if code != http.StatusAccepted {
				t.Errorf("create %d: %d", i, code)
			}
			ids[i] = o.ID
		}(i)
	}
	wg.Wait()
	confirmed, rejected := 0, 0
	eventually(t, wait, "all orders terminal", func() bool {
		confirmed, rejected = 0, 0
		for _, id := range ids {
			o, _ := c.orders.Get(ctx, id)
			switch o.Status {
			case orders.StatusConfirmed:
				confirmed++
			case orders.StatusRejected:
				rejected++
			}
		}
		return confirmed+rejected == n
	})
	if confirmed != 1 || rejected != n-1 {
		t.Fatalf("confirmed=%d rejected=%d, want exactly one sale of the single GPU", confirmed, rejected)
	}
	if s, _ := c.inventory.Get(ctx, "sku-gpu"); s.Available != 0 || s.Reserved != 0 {
		t.Fatalf("stock %+v", s)
	}
}

func TestIntegrationIdempotencyKeyAndDuplicateDelivery(t *testing.T) {
	c := startCluster(t, options{})
	ctx := context.Background()

	o1, code1 := c.placeOrder("erin", 2_000, "key-123", events.Item{SKU: "sku-mouse", Qty: 5})
	o2, code2 := c.placeOrder("erin", 2_000, "key-123", events.Item{SKU: "sku-mouse", Qty: 5})
	if code1 != http.StatusAccepted || code2 != http.StatusOK || o1.ID != o2.ID {
		t.Fatalf("idempotency key not honoured: %d %s / %d %s", code1, o1.ID, code2, o2.ID)
	}
	c.awaitStatus(o1.ID, orders.StatusConfirmed, wait)
	eventually(t, wait, "committed", func() bool {
		s, _ := c.inventory.Get(ctx, "sku-mouse")
		return s.Available == 45 && s.Reserved == 0
	})

	// Simulate an at-least-once redelivery of OrderCreated that bypasses the
	// JetStream duplicate window (new Nats-Msg-Id, same event id).
	st, _ := c.js.Stream(ctx, events.StreamEvents)
	raw, err := st.GetLastMsgForSubject(ctx, events.Subject(events.OrderCreated))
	if err != nil {
		t.Fatal(err)
	}
	m := nats.NewMsg(raw.Subject)
	m.Data = raw.Data
	if _, err := c.js.PublishMsg(ctx, m, jetstream.WithMsgID("redelivery-"+o1.ID)); err != nil {
		t.Fatal(err)
	}
	// And the same Nats-Msg-Id again: JetStream itself must drop it.
	ack, err := c.js.PublishMsg(ctx, m, jetstream.WithMsgID("redelivery-"+o1.ID))
	if err != nil || !ack.Duplicate {
		t.Fatalf("jetstream dedupe: ack=%+v err=%v", ack, err)
	}

	// Wait until the inventory consumer has processed the duplicate, then
	// verify nothing was reserved twice and the balance was charged once.
	eventually(t, wait, "duplicate consumed", func() bool {
		cons, err := c.js.Consumer(ctx, events.StreamEvents, "inventory")
		if err != nil {
			return false
		}
		info, err := cons.Info(ctx)
		return err == nil && info.NumPending == 0 && info.NumAckPending == 0
	})
	if s, _ := c.inventory.Get(ctx, "sku-mouse"); s.Available != 45 || s.Reserved != 0 {
		t.Fatalf("duplicate delivery changed stock: %+v", s)
	}
	if a, _ := c.payments.Account(ctx, "erin"); a.BalanceCents != 48_000 {
		t.Fatalf("customer charged more than once: %d", a.BalanceCents)
	}
}

func TestIntegrationTransientFailuresAreRetriedWithBackoff(t *testing.T) {
	var calls atomic.Int32
	fault := func(ev events.Envelope) error {
		if calls.Add(1) <= 2 {
			return payments.ErrTransient
		}
		return nil
	}
	c := startCluster(t, options{fault: fault})
	o, _ := c.placeOrder("frank", 500, "", events.Item{SKU: "sku-mouse", Qty: 1})
	c.awaitStatus(o.ID, orders.StatusConfirmed, wait)
	if calls.Load() != 3 {
		t.Fatalf("expected 2 failed attempts + 1 success, got %d calls", calls.Load())
	}
	if entries, _ := dlq.List(context.Background(), c.js, ""); len(entries) != 0 {
		t.Fatalf("transient failures must not reach the DLQ: %+v", entries)
	}
}

func TestIntegrationPoisonMessagesAreDeadLetteredAndReplayable(t *testing.T) {
	var broken atomic.Bool
	broken.Store(true)
	fault := func(ev events.Envelope) error {
		if broken.Load() {
			return messaging.Permanent(errors.New("card network rejected request"))
		}
		return nil
	}
	c := startCluster(t, options{fault: fault})
	ctx := context.Background()

	// 1) Garbage on the bus goes straight to the DLQ of every subscriber.
	if _, err := c.js.Publish(ctx, events.Subject(events.OrderCreated), []byte("{not json")); err != nil {
		t.Fatal(err)
	}
	// 2) A permanent business failure parks the event; the saga waits.
	o, _ := c.placeOrder("grace", 700, "", events.Item{SKU: "sku-keyboard", Qty: 1})
	c.awaitStatus(o.ID, orders.StatusReserved, wait)

	var entries []dlq.Entry
	eventually(t, wait, "two DLQ entries", func() bool {
		entries, _ = dlq.List(ctx, c.js, "")
		return len(entries) == 2
	})
	var paymentSeq uint64
	for _, e := range entries {
		switch e.Consumer {
		case "inventory":
			if e.OriginalSubject != "events.orders.created" || !strings.Contains(e.Error, "malformed") {
				t.Fatalf("bad inventory entry %+v", e)
			}
		case "payments":
			if e.Attempts != 1 || !strings.Contains(e.Error, "card network") {
				t.Fatalf("bad payments entry %+v", e)
			}
			paymentSeq = e.Seq
		default:
			t.Fatalf("unexpected DLQ entry %+v", e)
		}
	}

	// 3) Operator fixes the root cause and replays: the saga resumes.
	broken.Store(false)
	if err := dlq.Replay(ctx, c.js, paymentSeq); err != nil {
		t.Fatal(err)
	}
	c.awaitStatus(o.ID, orders.StatusConfirmed, wait)
	if left, _ := dlq.List(ctx, c.js, "payments"); len(left) != 0 {
		t.Fatalf("replayed entry still in DLQ: %+v", left)
	}
}

func TestIntegrationWebhookRetriesThenDelivers(t *testing.T) {
	var hits atomic.Int32
	var key atomic.Value
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		key.Store(r.Header.Get("Idempotency-Key"))
		if hits.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer hook.Close()

	c := startCluster(t, options{webhook: hook.URL})
	o, _ := c.placeOrder("heidi", 300, "", events.Item{SKU: "sku-mouse", Qty: 1})
	c.awaitStatus(o.ID, orders.StatusConfirmed, wait)
	eventually(t, wait, "webhook delivered", func() bool {
		n, _ := c.notifier.List(context.Background(), o.ID)
		return len(n) == 1 && n[0].Delivered
	})
	if hits.Load() != 2 || key.Load() == "" {
		t.Fatalf("hits=%d key=%v", hits.Load(), key.Load())
	}
}

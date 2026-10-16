package inventory

import (
	"context"
	"testing"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/events"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/testutil"
)

func newSvc(t *testing.T) *Service {
	b := testutil.Base(t, Schema)
	s, err := New(context.Background(), b, "a=5,b=2")
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func stock(t *testing.T, s *Service, sku string) Stock {
	st, err := s.Get(context.Background(), sku)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func created(t *testing.T, id string, items ...events.Item) events.Envelope {
	return testutil.Event(t, events.OrderCreated, id, events.OrderCreatedV1{CustomerID: "c", AmountCents: 100, Items: items})
}

func TestReserveIsAllOrNothing(t *testing.T) {
	s := newSvc(t)
	// b only has 2: the whole order is rejected and a is untouched.
	if err := testutil.Apply(t, s.b, s.handle, created(t, "o1", events.Item{SKU: "a", Qty: 1}, events.Item{SKU: "b", Qty: 3})); err != nil {
		t.Fatal(err)
	}
	if st := stock(t, s, "a"); st.Available != 5 || st.Reserved != 0 {
		t.Fatalf("partial reservation leaked: %+v", st)
	}
	out := testutil.Outbox(t, s.b)
	if len(out) != 1 || out[0].Type != events.InventoryRejected {
		t.Fatalf("want InventoryRejected, got %+v", out)
	}
}

func TestReserveMergesLinesAndCarriesPaymentData(t *testing.T) {
	s := newSvc(t)
	ev := created(t, "o1", events.Item{SKU: "a", Qty: 2}, events.Item{SKU: "a", Qty: 2}, events.Item{SKU: "b", Qty: 1})
	if err := testutil.Apply(t, s.b, s.handle, ev); err != nil {
		t.Fatal(err)
	}
	if st := stock(t, s, "a"); st.Available != 1 || st.Reserved != 4 {
		t.Fatalf("a: %+v", st)
	}
	out := testutil.Outbox(t, s.b)
	var p events.InventoryReservedV1
	if len(out) != 1 || out[0].Type != events.InventoryReserved || out[0].Payload(&p) != nil || p.AmountCents != 100 || len(p.Items) != 2 {
		t.Fatalf("bad event %+v %+v", out, p)
	}
	// Re-applying the same order (new event id) must not reserve again.
	if err := testutil.Apply(t, s.b, s.handle, created(t, "o1", events.Item{SKU: "a", Qty: 4})); err != nil {
		t.Fatal(err)
	}
	if st := stock(t, s, "a"); st.Reserved != 4 {
		t.Fatalf("double reservation: %+v", st)
	}
}

func TestCompensationAndCommit(t *testing.T) {
	s := newSvc(t)
	_ = testutil.Apply(t, s.b, s.handle, created(t, "o1", events.Item{SKU: "a", Qty: 3}))
	_ = testutil.Apply(t, s.b, s.handle, created(t, "o2", events.Item{SKU: "a", Qty: 2}))

	failed := testutil.Event(t, events.PaymentFailed, "o1", events.PaymentFailedV1{Reason: "x"})
	for i := 0; i < 2; i++ { // second release is a no-op
		if err := testutil.Apply(t, s.b, s.handle, failed); err != nil {
			t.Fatal(err)
		}
	}
	ok := testutil.Event(t, events.PaymentSucceeded, "o2", events.PaymentSucceededV1{})
	if err := testutil.Apply(t, s.b, s.handle, ok); err != nil {
		t.Fatal(err)
	}
	if st := stock(t, s, "a"); st.Available != 3 || st.Reserved != 0 {
		t.Fatalf("after release+commit: %+v", st)
	}
	n := 0
	for _, e := range testutil.Outbox(t, s.b) {
		if e.Type == events.InventoryReleased {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("want exactly one InventoryReleased, got %d", n)
	}
}

func TestInvalidPayloadIsPermanent(t *testing.T) {
	s := newSvc(t)
	ev := created(t, "o1", events.Item{SKU: "a", Qty: -1})
	if err := testutil.Apply(t, s.b, s.handle, ev); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseSeed(t *testing.T) {
	m, err := ParseSeed(" a=1, b = 2 ,")
	if err != nil || m["a"] != 1 || m["b"] != 2 {
		t.Fatalf("%v %v", m, err)
	}
	for _, bad := range []string{"a", "a=x", "=1", "a=-1"} {
		if _, err := ParseSeed(bad); err == nil {
			t.Errorf("ParseSeed(%q) should fail", bad)
		}
	}
}

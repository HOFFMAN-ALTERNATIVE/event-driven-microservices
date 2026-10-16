package payments

import (
	"context"
	"errors"
	"testing"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/events"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/testutil"
)

func reserved(t *testing.T, id, customer string, amount int64) events.Envelope {
	return testutil.Event(t, events.InventoryReserved, id, events.InventoryReservedV1{CustomerID: customer, AmountCents: amount})
}

func TestChargeSucceedsOnceAndFailsWhenBroke(t *testing.T) {
	s := New(testutil.Base(t, Schema), Options{DefaultBalanceCents: 1000})
	for _, ev := range []events.Envelope{
		reserved(t, "o1", "c", 600),
		reserved(t, "o1", "c", 600), // same order again: ignored
		reserved(t, "o2", "c", 600), // only 400 left
	} {
		if err := testutil.Apply(t, s.b, s.charge, ev); err != nil {
			t.Fatal(err)
		}
	}
	a, _ := s.Account(context.Background(), "c")
	if a.BalanceCents != 400 {
		t.Fatalf("balance %d", a.BalanceCents)
	}
	out := testutil.Outbox(t, s.b)
	if len(out) != 2 || out[0].Type != events.PaymentSucceeded || out[1].Type != events.PaymentFailed {
		t.Fatalf("events %+v", out)
	}
	p, _ := s.Payment(context.Background(), "o2")
	if p.Status != "FAILED" || p.Reason != "insufficient funds" {
		t.Fatalf("payment %+v", p)
	}
}

func TestFaultRollsBack(t *testing.T) {
	s := New(testutil.Base(t, Schema), Options{Fault: func(events.Envelope) error { return ErrTransient }})
	if err := testutil.Apply(t, s.b, s.charge, reserved(t, "o1", "c", 1)); !errors.Is(err, ErrTransient) {
		t.Fatalf("want transient, got %v", err)
	}
	if len(testutil.Outbox(t, s.b)) != 0 {
		t.Fatal("outbox must be empty after failed attempt")
	}
}

func TestInvalidRequestIsPermanent(t *testing.T) {
	s := New(testutil.Base(t, Schema), Options{})
	if err := testutil.Apply(t, s.b, s.charge, reserved(t, "o1", "", 10)); err == nil {
		t.Fatal("expected error")
	}
}

func TestRandomFault(t *testing.T) {
	if RandomFault(0) != nil {
		t.Fatal("rate 0 must disable faults")
	}
	if RandomFault(1)(events.Envelope{}) == nil {
		t.Fatal("rate 1 must always fail")
	}
}

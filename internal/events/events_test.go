package events

import (
	"errors"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	e, err := New(OrderCreated, "o-1", OrderCreatedV1{CustomerID: "c", AmountCents: 10, Items: []Item{{SKU: "a", Qty: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := jsonMarshal(e)
	got, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	var p OrderCreatedV1
	if err := got.Payload(&p); err != nil {
		t.Fatal(err)
	}
	if got.ID != e.ID || p.Items[0].SKU != "a" || p.AmountCents != 10 {
		t.Fatalf("round trip mismatch: %+v %+v", got, p)
	}
	if Subject(OrderCreated) != "events.orders.created" {
		t.Fatal("subject")
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	for _, in := range []string{"", "{", `{"id":"x"}`, `{"id":"x","type":"t"}`, `[1,2]`} {
		if _, err := Decode([]byte(in)); !errors.Is(err, ErrMalformed) {
			t.Errorf("Decode(%q) = %v, want ErrMalformed", in, err)
		}
	}
}

func TestPayloadMalformed(t *testing.T) {
	e := Envelope{ID: "1", Type: OrderCreated, OrderID: "o", Data: []byte(`{"amount_cents":"nope"}`)}
	var p OrderCreatedV1
	if err := e.Payload(&p); !errors.Is(err, ErrMalformed) {
		t.Fatalf("want ErrMalformed, got %v", err)
	}
}

func TestIDsAreUniqueAndOrdered(t *testing.T) {
	prev := ""
	for i := 0; i < 1000; i++ {
		e, _ := New(OrderCreated, "o", struct{}{})
		if e.ID <= prev {
			t.Fatalf("ids not monotonic: %s <= %s", e.ID, prev)
		}
		prev = e.ID
	}
}

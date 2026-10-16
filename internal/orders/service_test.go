package orders

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/events"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/testutil"
)

func post(t *testing.T, h http.Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCreateValidation(t *testing.T) {
	s := New(testutil.Base(t, Schema))
	h := s.Routes()
	cases := map[string]int{
		`{`: 400,
		`{"customer_id":"c","amount_cents":1,"items":[{"sku":"a","qty":1}],"extra":1}`: 400,
		`{"customer_id":"","amount_cents":1,"items":[{"sku":"a","qty":1}]}`:            422,
		`{"customer_id":"c","amount_cents":0,"items":[{"sku":"a","qty":1}]}`:           422,
		`{"customer_id":"c","amount_cents":1,"items":[]}`:                              422,
		`{"customer_id":"c","amount_cents":1,"items":[{"sku":"a","qty":0}]}`:           422,
		`{"customer_id":"c","amount_cents":1,"items":[{"sku":"a","qty":1}]}`:           202,
	}
	for body, want := range cases {
		if got := post(t, h, body).Code; got != want {
			t.Errorf("%s => %d, want %d", body, got, want)
		}
	}
	out := testutil.Outbox(t, s.b)
	if len(out) != 1 || out[0].Type != events.OrderCreated {
		t.Fatalf("exactly one OrderCreated expected in outbox, got %+v", out)
	}
}

func TestSagaHandlerEmitsFinishedEventOnce(t *testing.T) {
	s := New(testutil.Base(t, Schema))
	rec := post(t, s.Routes(), `{"customer_id":"c","amount_cents":500,"items":[{"sku":"a","qty":1}]}`)
	id := strings.TrimPrefix(rec.Header().Get("Location"), "/orders/")

	for _, ev := range []events.Envelope{
		testutil.Event(t, events.InventoryReserved, id, events.InventoryReservedV1{}),
		testutil.Event(t, events.PaymentSucceeded, id, events.PaymentSucceededV1{}),
		testutil.Event(t, events.PaymentSucceeded, id, events.PaymentSucceededV1{}), // late duplicate
		testutil.Event(t, events.PaymentFailed, id, events.PaymentFailedV1{Reason: "late"}),
	} {
		if err := testutil.Apply(t, s.b, s.onSagaEvent, ev); err != nil {
			t.Fatal(err)
		}
	}
	o, err := s.Get(context.Background(), id)
	if err != nil || o.Status != StatusConfirmed || len(o.History) != 3 {
		t.Fatalf("order %+v err %v", o, err)
	}
	n := 0
	for _, e := range testutil.Outbox(t, s.b) {
		if e.Type == events.OrderConfirmed {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("OrderConfirmed emitted %d times", n)
	}
}

func TestUnknownOrderIsPermanent(t *testing.T) {
	s := New(testutil.Base(t, Schema))
	err := testutil.Apply(t, s.b, s.onSagaEvent, testutil.Event(t, events.PaymentSucceeded, "nope", struct{}{}))
	if err == nil || !strings.Contains(err.Error(), "permanent") {
		t.Fatalf("want permanent error, got %v", err)
	}
}

package notifier

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/events"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/messaging"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/testutil"
)

func finished(t *testing.T, typ string) events.Envelope {
	return testutil.Event(t, typ, "o1", events.OrderFinishedV1{CustomerID: "c", AmountCents: 12345, Status: "X", Reason: "insufficient funds"})
}

func TestMessages(t *testing.T) {
	var p events.OrderFinishedV1
	ev := finished(t, events.OrderConfirmed)
	_ = ev.Payload(&p)
	if m := Message(ev, p); !strings.Contains(m, "123.45") {
		t.Fatal(m)
	}
	ev = finished(t, events.OrderCancelled)
	if m := Message(ev, p); !strings.Contains(m, "released") {
		t.Fatal(m)
	}
}

func TestWebhookErrorClassification(t *testing.T) {
	status := http.StatusBadRequest
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Idempotency-Key") == "" {
			t.Error("missing idempotency key")
		}
		w.WriteHeader(status)
	}))
	defer hook.Close()
	s := New(testutil.Base(t, Schema), hook.URL)

	if err := testutil.Apply(t, s.b, s.notify, finished(t, events.OrderRejected)); !errors.Is(err, messaging.ErrPermanent) {
		t.Fatalf("4xx should be permanent, got %v", err)
	}
	status = http.StatusBadGateway
	if err := testutil.Apply(t, s.b, s.notify, finished(t, events.OrderRejected)); err == nil || errors.Is(err, messaging.ErrPermanent) {
		t.Fatalf("5xx should be transient, got %v", err)
	}
	status = http.StatusOK
	if err := testutil.Apply(t, s.b, s.notify, finished(t, events.OrderConfirmed)); err != nil {
		t.Fatal(err)
	}
	n, _ := s.List(context.Background(), "o1")
	if len(n) != 1 || !n[0].Delivered {
		t.Fatalf("%+v", n)
	}
}

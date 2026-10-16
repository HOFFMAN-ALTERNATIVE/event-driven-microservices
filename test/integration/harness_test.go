//go:build integration

// Package integration runs all four services in-process against a real
// JetStream server: the one in $NATS_URL (CI uses a nats container) or an
// embedded nats-server when the variable is unset.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/events"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/inventory"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/notifier"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/orders"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/payments"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/service"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/telemetry"
)

var (
	natsURL  string
	recorder = tracetest.NewSpanRecorder()
)

func TestMain(m *testing.M) {
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)))
	otel.SetTextMapPropagator(telemetry.Propagator)

	natsURL = os.Getenv("NATS_URL")
	var srv *server.Server
	if natsURL == "" {
		dir, err := os.MkdirTemp("", "edm-js-*")
		if err != nil {
			panic(err)
		}
		defer os.RemoveAll(dir)
		srv, err = server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: dir, NoLog: true, NoSigs: true})
		if err != nil {
			panic(err)
		}
		go srv.Start()
		if !srv.ReadyForConnections(10 * time.Second) {
			panic("embedded nats-server not ready")
		}
		natsURL = srv.ClientURL()
	}
	code := m.Run()
	if srv != nil {
		srv.Shutdown()
	}
	os.Exit(code)
}

type cluster struct {
	t         *testing.T
	js        jetstream.JetStream
	orders    *orders.Service
	inventory *inventory.Service
	payments  *payments.Service
	notifier  *notifier.Service
	http      *httptest.Server // orders API
	payHTTP   *httptest.Server // payments API
}

type options struct {
	seed    string
	fault   func(events.Envelope) error
	webhook string
}

func quietLog() *slog.Logger {
	if os.Getenv("EDM_TEST_LOGS") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// startCluster resets the streams and boots the four services, each with its
// own fresh database.
func startCluster(t *testing.T, o options) *cluster {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, _ := jetstream.New(nc)
	for _, s := range []string{events.StreamEvents, events.StreamDLQ} {
		if err := js.DeleteStream(ctx, s); err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
			t.Fatal(err)
		}
	}

	dir := t.TempDir()
	base := func(name, schema string) *service.Base {
		b, err := service.New(ctx, service.Config{Name: name, NATSURL: natsURL, DBPath: filepath.Join(dir, name+".db")}, quietLog().With("service", name), schema)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(b.Close)
		return b
	}
	c := &cluster{t: t, js: js}

	ob := base("orders", orders.Schema)
	c.orders = orders.New(ob)
	ib := base("inventory", inventory.Schema)
	if o.seed == "" {
		o.seed = inventory.DefaultSeed
	}
	if c.inventory, err = inventory.New(ctx, ib, o.seed); err != nil {
		t.Fatal(err)
	}
	pb := base("payments", payments.Schema)
	c.payments = payments.New(pb, payments.Options{DefaultBalanceCents: 50_000, Fault: o.fault})
	nb := base("notifier", notifier.Schema)
	c.notifier = notifier.New(nb, o.webhook)

	for _, x := range []struct {
		b    *service.Base
		cons func() []messagingConsumer
	}{
		{ob, c.orders.Consumers}, {ib, c.inventory.Consumers}, {pb, c.payments.Consumers}, {nb, c.notifier.Consumers},
	} {
		stop, err := x.b.Start(ctx, x.cons()...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(stop)
	}
	c.http = httptest.NewServer(ob.Handler(c.orders.Routes()))
	t.Cleanup(c.http.Close)
	c.payHTTP = httptest.NewServer(pb.Handler(c.payments.Routes()))
	t.Cleanup(c.payHTTP.Close)
	return c
}

func (c *cluster) placeOrder(customer string, amount int64, key string, items ...events.Item) (orders.Order, int) {
	c.t.Helper()
	body, _ := json.Marshal(map[string]any{"customer_id": customer, "amount_cents": amount, "items": items})
	req, _ := http.NewRequest(http.MethodPost, c.http.URL+"/orders", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	var o orders.Order
	_ = json.NewDecoder(resp.Body).Decode(&o)
	return o, resp.StatusCode
}

// awaitStatus polls the orders API until the order reaches want.
func (c *cluster) awaitStatus(id string, want orders.Status, timeout time.Duration) orders.Order {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	var last orders.Order
	for time.Now().Before(deadline) {
		resp, err := http.Get(c.http.URL + "/orders/" + id)
		if err == nil {
			_ = json.NewDecoder(resp.Body).Decode(&last)
			resp.Body.Close()
			if last.Status == want {
				return last
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("order %s: status %q after %v, want %q (history %+v)", id, last.Status, timeout, want, last.History)
	return last
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for: %s", timeout, what)
}

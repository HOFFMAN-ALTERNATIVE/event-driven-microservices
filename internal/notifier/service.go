// Package notifier turns terminal order events into customer notifications and
// optionally forwards them to a webhook. It demonstrates a consumer with an
// external, non-transactional side effect made safe by an idempotency key.
package notifier

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/events"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/httpx"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/messaging"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/service"
)

// Schema of the notifier database.
const Schema = `
CREATE TABLE IF NOT EXISTS notifications (
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	event_id    TEXT NOT NULL UNIQUE,
	order_id    TEXT NOT NULL,
	customer_id TEXT NOT NULL,
	kind        TEXT NOT NULL,
	message     TEXT NOT NULL,
	delivered   INTEGER NOT NULL DEFAULT 0,
	created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS notifications_order ON notifications(order_id);
`

// Notification is the API representation.
type Notification struct {
	OrderID    string `json:"order_id"`
	CustomerID string `json:"customer_id"`
	Kind       string `json:"kind"`
	Message    string `json:"message"`
	Delivered  bool   `json:"webhook_delivered"`
	CreatedAt  string `json:"created_at"`
}

// Service is the notifier service.
type Service struct {
	b       *service.Base
	webhook string
	client  *http.Client
}

// New creates the service. webhook may be empty.
func New(b *service.Base, webhook string) *Service {
	return &Service{b: b, webhook: webhook, client: &http.Client{
		Timeout:   5 * time.Second,
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}}
}

// Run starts the service and blocks until ctx is cancelled.
func Run(ctx context.Context, b *service.Base, webhook string) error {
	s := New(b, webhook)
	return b.Run(ctx, s.Routes(), s.Consumers()...)
}

// Consumers returns the subscriptions.
func (s *Service) Consumers() []messaging.Consumer {
	return []messaging.Consumer{{
		Name:       "notifier",
		EventTypes: []string{events.OrderConfirmed, events.OrderCancelled, events.OrderRejected},
		Handler:    s.notify,
		MaxDeliver: 8, // webhooks deserve a little more patience
	}}
}

// Message renders the human-readable text for a terminal order event.
func Message(ev events.Envelope, p events.OrderFinishedV1) string {
	amount := fmt.Sprintf("%d.%02d", p.AmountCents/100, p.AmountCents%100)
	switch ev.Type {
	case events.OrderConfirmed:
		return fmt.Sprintf("Order %s confirmed: %s charged.", ev.OrderID, amount)
	case events.OrderCancelled:
		return fmt.Sprintf("Order %s cancelled: %s. Reserved items were released.", ev.OrderID, p.Reason)
	default:
		return fmt.Sprintf("Order %s rejected: %s.", ev.OrderID, p.Reason)
	}
}

func (s *Service) notify(ctx context.Context, tx *sql.Tx, ev events.Envelope) error {
	var p events.OrderFinishedV1
	if err := ev.Payload(&p); err != nil {
		return err
	}
	msg := Message(ev, p)
	delivered := false
	if s.webhook != "" {
		if err := s.deliver(ctx, ev, p, msg); err != nil {
			return err
		}
		delivered = true
	}
	_, err := tx.ExecContext(ctx,
		`INSERT OR IGNORE INTO notifications (event_id, order_id, customer_id, kind, message, delivered) VALUES (?, ?, ?, ?, ?, ?)`,
		ev.ID, ev.OrderID, p.CustomerID, ev.Type, msg, delivered)
	if err == nil {
		s.b.Log.InfoContext(ctx, "notification", "order_id", ev.OrderID, "kind", ev.Type, "message", msg)
	}
	return err
}

// deliver POSTs to the webhook. The event id is sent as Idempotency-Key, so a
// retry after a crash between delivery and commit is harmless for receivers
// that honour it. 5xx/network errors are retried; 4xx are permanent.
func (s *Service) deliver(ctx context.Context, ev events.Envelope, p events.OrderFinishedV1, msg string) error {
	body, _ := json.Marshal(map[string]any{
		"event_id": ev.ID, "type": ev.Type, "order_id": ev.OrderID,
		"customer_id": p.CustomerID, "status": p.Status, "message": msg,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.webhook, bytes.NewReader(body))
	if err != nil {
		return messaging.Permanent(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", ev.ID)
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook: %w", err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests:
		return messaging.Permanent(fmt.Errorf("webhook rejected notification: %s", resp.Status))
	default:
		return fmt.Errorf("webhook unavailable: %s", resp.Status)
	}
}

// Routes returns the HTTP API.
func (s *Service) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /notifications", s.list)
	return mux
}

// List returns notifications, optionally filtered by order id.
func (s *Service) List(ctx context.Context, orderID string) ([]Notification, error) {
	q := `SELECT order_id, customer_id, kind, message, delivered, created_at FROM notifications`
	var args []any
	if orderID != "" {
		q += ` WHERE order_id = ?`
		args = append(args, orderID)
	}
	q += ` ORDER BY id DESC LIMIT 100`
	rows, err := s.b.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Notification{}
	for rows.Next() {
		var n Notification
		if err := rows.Scan(&n.OrderID, &n.CustomerID, &n.Kind, &n.Message, &n.Delivered, &n.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func (s *Service) list(w http.ResponseWriter, r *http.Request) {
	out, err := s.List(r.Context(), r.URL.Query().Get("order_id"))
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, err.Error())
		return
	}
	httpx.JSON(w, http.StatusOK, out)
}

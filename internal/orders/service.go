package orders

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/events"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/db"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/httpx"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/messaging"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/outbox"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/service"
)

// Schema of the orders database.
const Schema = `
CREATE TABLE IF NOT EXISTS orders (
	id              TEXT PRIMARY KEY,
	customer_id     TEXT NOT NULL,
	amount_cents    INTEGER NOT NULL CHECK (amount_cents > 0),
	items           TEXT NOT NULL,
	status          TEXT NOT NULL,
	reason          TEXT NOT NULL DEFAULT '',
	idempotency_key TEXT UNIQUE,
	created_at      TEXT NOT NULL,
	updated_at      TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS order_history (
	order_id TEXT NOT NULL REFERENCES orders(id),
	status   TEXT NOT NULL,
	cause    TEXT NOT NULL,
	at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS order_history_order ON order_history(order_id);
`

// Order is the API representation.
type Order struct {
	ID          string        `json:"id"`
	CustomerID  string        `json:"customer_id"`
	AmountCents int64         `json:"amount_cents"`
	Items       []events.Item `json:"items"`
	Status      Status        `json:"status"`
	Reason      string        `json:"reason,omitempty"`
	CreatedAt   string        `json:"created_at"`
	UpdatedAt   string        `json:"updated_at"`
	History     []HistoryItem `json:"history,omitempty"`
}

// HistoryItem is one saga step, kept for auditability.
type HistoryItem struct {
	Status Status `json:"status"`
	Cause  string `json:"cause"`
	At     string `json:"at"`
}

// Service is the orders service.
type Service struct{ b *service.Base }

// New creates the service.
func New(b *service.Base) *Service { return &Service{b: b} }

// Run starts the service and blocks until ctx is cancelled.
func Run(ctx context.Context, b *service.Base) error {
	s := New(b)
	return b.Run(ctx, s.Routes(), s.Consumers()...)
}

// Consumers returns the saga subscriptions.
func (s *Service) Consumers() []messaging.Consumer {
	return []messaging.Consumer{{
		Name: "orders-saga",
		EventTypes: []string{
			events.InventoryReserved, events.InventoryRejected, events.InventoryReleased,
			events.PaymentSucceeded, events.PaymentFailed,
		},
		Handler: s.onSagaEvent,
	}}
}

// Routes returns the HTTP API.
func (s *Service) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", s.create)
	mux.HandleFunc("GET /orders/{id}", s.get)
	mux.HandleFunc("GET /orders", s.list)
	return mux
}

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

type createReq struct {
	CustomerID  string        `json:"customer_id"`
	AmountCents int64         `json:"amount_cents"`
	Items       []events.Item `json:"items"`
}

func (r createReq) validate() error {
	if strings.TrimSpace(r.CustomerID) == "" {
		return errors.New("customer_id is required")
	}
	if r.AmountCents <= 0 {
		return errors.New("amount_cents must be positive")
	}
	if len(r.Items) == 0 || len(r.Items) > 100 {
		return errors.New("items must contain 1..100 lines")
	}
	for _, it := range r.Items {
		if strings.TrimSpace(it.SKU) == "" || it.Qty <= 0 || it.Qty > 10000 {
			return fmt.Errorf("invalid item %+v", it)
		}
	}
	return nil
}

// create persists the order and its OrderCreated event atomically and replies
// 202: the saga completes asynchronously. An Idempotency-Key header makes
// client retries safe.
func (s *Service) create(w http.ResponseWriter, r *http.Request) {
	var req createReq
	if err := httpx.Decode(w, r, &req); err != nil {
		httpx.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := req.validate(); err != nil {
		httpx.Error(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	ctx := r.Context()
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key != "" {
		if o, err := s.byIdempotencyKey(ctx, key); err == nil {
			httpx.JSON(w, http.StatusOK, o)
			return
		}
	}

	id, _ := uuid.NewV7()
	o := Order{ID: id.String(), CustomerID: req.CustomerID, AmountCents: req.AmountCents, Items: req.Items,
		Status: StatusPending, CreatedAt: now()}
	o.UpdatedAt = o.CreatedAt
	items, _ := json.Marshal(o.Items)

	err := db.InTx(ctx, s.b.DB, func(tx *sql.Tx) error {
		var k any
		if key != "" {
			k = key
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO orders (id, customer_id, amount_cents, items, status, idempotency_key, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			o.ID, o.CustomerID, o.AmountCents, string(items), o.Status, k, o.CreatedAt, o.UpdatedAt); err != nil {
			return err
		}
		if err := history(ctx, tx, o.ID, o.Status, "http.create"); err != nil {
			return err
		}
		ev, err := events.New(events.OrderCreated, o.ID, events.OrderCreatedV1{
			CustomerID: o.CustomerID, AmountCents: o.AmountCents, Items: o.Items})
		if err != nil {
			return err
		}
		return outbox.Enqueue(ctx, tx, ev)
	})
	if err != nil {
		if key != "" && strings.Contains(err.Error(), "UNIQUE") {
			// Lost a race with a concurrent retry carrying the same key.
			if existing, err2 := s.byIdempotencyKey(ctx, key); err2 == nil {
				httpx.JSON(w, http.StatusOK, existing)
				return
			}
		}
		s.b.Log.ErrorContext(ctx, "create order failed", "err", err)
		httpx.Error(w, http.StatusInternalServerError, "could not create order")
		return
	}
	s.b.Relay.Kick()
	s.b.Log.InfoContext(ctx, "order created", "order_id", o.ID)
	w.Header().Set("Location", "/orders/"+o.ID)
	httpx.JSON(w, http.StatusAccepted, o)
}

func history(ctx context.Context, tx *sql.Tx, id string, st Status, cause string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO order_history (order_id, status, cause, at) VALUES (?, ?, ?, ?)`, id, st, cause, now())
	return err
}

// onSagaEvent advances the saga. It runs inside the consumer transaction, so
// the status change, the audit row and the follow-up event commit together.
func (s *Service) onSagaEvent(ctx context.Context, tx *sql.Tx, ev events.Envelope) error {
	var (
		cur      Status
		customer string
		amount   int64
	)
	err := tx.QueryRowContext(ctx, `SELECT status, customer_id, amount_cents FROM orders WHERE id = ?`, ev.OrderID).
		Scan(&cur, &customer, &amount)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.Permanent(fmt.Errorf("unknown order %s", ev.OrderID))
	}
	if err != nil {
		return err
	}

	if ev.Type == events.InventoryReleased {
		return history(ctx, tx, ev.OrderID, cur, ev.Type+" (compensated)")
	}
	next, changed := Next(cur, ev.Type)
	if !changed {
		s.b.Log.DebugContext(ctx, "saga event ignored", "order_id", ev.OrderID, "status", cur, "event", ev.Type)
		return nil
	}
	reason := ""
	switch ev.Type {
	case events.InventoryRejected:
		var p events.InventoryRejectedV1
		if err := ev.Payload(&p); err != nil {
			return err
		}
		reason = p.Reason
	case events.PaymentFailed:
		var p events.PaymentFailedV1
		if err := ev.Payload(&p); err != nil {
			return err
		}
		reason = p.Reason
	}
	if _, err := tx.ExecContext(ctx, `UPDATE orders SET status = ?, reason = ?, updated_at = ? WHERE id = ?`,
		next, reason, now(), ev.OrderID); err != nil {
		return err
	}
	if err := history(ctx, tx, ev.OrderID, next, ev.Type); err != nil {
		return err
	}
	if !next.Terminal() {
		return nil
	}
	out, err := events.New(FinishedEvent(next), ev.OrderID, events.OrderFinishedV1{
		CustomerID: customer, AmountCents: amount, Status: string(next), Reason: reason})
	if err != nil {
		return err
	}
	return outbox.Enqueue(ctx, tx, out)
}

const selectOrder = `SELECT id, customer_id, amount_cents, items, status, reason, created_at, updated_at FROM orders`

func scanOrder(row interface{ Scan(...any) error }) (Order, error) {
	var o Order
	var items string
	if err := row.Scan(&o.ID, &o.CustomerID, &o.AmountCents, &items, &o.Status, &o.Reason, &o.CreatedAt, &o.UpdatedAt); err != nil {
		return o, err
	}
	err := json.Unmarshal([]byte(items), &o.Items)
	return o, err
}

func (s *Service) byIdempotencyKey(ctx context.Context, key string) (Order, error) {
	return scanOrder(s.b.DB.QueryRowContext(ctx, selectOrder+` WHERE idempotency_key = ?`, key))
}

// Get loads an order with its saga history.
func (s *Service) Get(ctx context.Context, id string) (Order, error) {
	o, err := scanOrder(s.b.DB.QueryRowContext(ctx, selectOrder+` WHERE id = ?`, id))
	if err != nil {
		return o, err
	}
	rows, err := s.b.DB.QueryContext(ctx, `SELECT status, cause, at FROM order_history WHERE order_id = ? ORDER BY rowid`, id)
	if err != nil {
		return o, err
	}
	defer rows.Close()
	for rows.Next() {
		var h HistoryItem
		if err := rows.Scan(&h.Status, &h.Cause, &h.At); err != nil {
			return o, err
		}
		o.History = append(o.History, h)
	}
	return o, rows.Err()
}

func (s *Service) get(w http.ResponseWriter, r *http.Request) {
	o, err := s.Get(r.Context(), r.PathValue("id"))
	if errors.Is(err, sql.ErrNoRows) {
		httpx.Error(w, http.StatusNotFound, "order not found")
		return
	}
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, err.Error())
		return
	}
	httpx.JSON(w, http.StatusOK, o)
}

func (s *Service) list(w http.ResponseWriter, r *http.Request) {
	q := selectOrder
	var args []any
	if st := r.URL.Query().Get("status"); st != "" {
		q += ` WHERE status = ?`
		args = append(args, strings.ToUpper(st))
	}
	q += ` ORDER BY created_at DESC LIMIT 100`
	rows, err := s.b.DB.QueryContext(r.Context(), q, args...)
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	out := []Order{}
	for rows.Next() {
		o, err := scanOrder(rows)
		if err != nil {
			httpx.Error(w, http.StatusInternalServerError, err.Error())
			return
		}
		out = append(out, o)
	}
	httpx.JSON(w, http.StatusOK, out)
}

// Package payments owns customer balances and charges orders once inventory
// has been reserved. A failed charge emits PaymentFailed, which triggers the
// compensating release in inventory and cancels the order.
package payments

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/events"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/httpx"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/messaging"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/outbox"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/service"
)

// Schema of the payments database.
const Schema = `
CREATE TABLE IF NOT EXISTS accounts (
	customer_id   TEXT PRIMARY KEY,
	balance_cents INTEGER NOT NULL CHECK (balance_cents >= 0)
);
CREATE TABLE IF NOT EXISTS payments (
	order_id     TEXT PRIMARY KEY,
	customer_id  TEXT NOT NULL,
	amount_cents INTEGER NOT NULL,
	status       TEXT NOT NULL CHECK (status IN ('SUCCEEDED','FAILED')),
	reason       TEXT NOT NULL DEFAULT '',
	created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
`

// ErrTransient simulates an unavailable payment provider.
var ErrTransient = errors.New("payment provider temporarily unavailable")

// Options configures the service.
type Options struct {
	// DefaultBalanceCents is credited to customers on first use.
	DefaultBalanceCents int64
	// Fault, if set, is called before charging; returning an error simulates a
	// flaky provider and exercises the retry/back-off/DLQ path.
	Fault func(ev events.Envelope) error
}

// RandomFault fails with probability rate (chaos testing via env).
func RandomFault(rate float64) func(events.Envelope) error {
	if rate <= 0 {
		return nil
	}
	return func(events.Envelope) error {
		if rand.Float64() < rate { //nolint:gosec // chaos injection, not security
			return ErrTransient
		}
		return nil
	}
}

// Account is the API representation of a balance.
type Account struct {
	CustomerID   string `json:"customer_id"`
	BalanceCents int64  `json:"balance_cents"`
}

// Payment is the API representation of a charge.
type Payment struct {
	OrderID     string `json:"order_id"`
	CustomerID  string `json:"customer_id"`
	AmountCents int64  `json:"amount_cents"`
	Status      string `json:"status"`
	Reason      string `json:"reason,omitempty"`
	CreatedAt   string `json:"created_at"`
}

// Service is the payments service.
type Service struct {
	b    *service.Base
	opts Options
}

// New creates the service.
func New(b *service.Base, opts Options) *Service {
	if opts.DefaultBalanceCents <= 0 {
		opts.DefaultBalanceCents = 50_000
	}
	return &Service{b: b, opts: opts}
}

// Run starts the service and blocks until ctx is cancelled.
func Run(ctx context.Context, b *service.Base, opts Options) error {
	s := New(b, opts)
	return b.Run(ctx, s.Routes(), s.Consumers()...)
}

// Consumers returns the subscriptions.
func (s *Service) Consumers() []messaging.Consumer {
	return []messaging.Consumer{{
		Name:       "payments",
		EventTypes: []string{events.InventoryReserved},
		Handler:    s.charge,
	}}
}

func (s *Service) charge(ctx context.Context, tx *sql.Tx, ev events.Envelope) error {
	var p events.InventoryReservedV1
	if err := ev.Payload(&p); err != nil {
		return err
	}
	if p.CustomerID == "" || p.AmountCents <= 0 {
		return messaging.Permanent(fmt.Errorf("invalid charge request %+v", p))
	}
	// Business idempotency: one payment per order, whatever happens upstream.
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM payments WHERE order_id = ?`, ev.OrderID).Scan(&exists); err != nil {
		return err
	}
	if exists > 0 {
		return nil
	}
	if s.opts.Fault != nil {
		if err := s.opts.Fault(ev); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO accounts (customer_id, balance_cents) VALUES (?, ?)`,
		p.CustomerID, s.opts.DefaultBalanceCents); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx,
		`UPDATE accounts SET balance_cents = balance_cents - ? WHERE customer_id = ? AND balance_cents >= ?`,
		p.AmountCents, p.CustomerID, p.AmountCents)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		const reason = "insufficient funds"
		if _, err := tx.ExecContext(ctx, `INSERT INTO payments (order_id, customer_id, amount_cents, status, reason) VALUES (?, ?, ?, 'FAILED', ?)`,
			ev.OrderID, p.CustomerID, p.AmountCents, reason); err != nil {
			return err
		}
		out, err := events.New(events.PaymentFailed, ev.OrderID, events.PaymentFailedV1{
			CustomerID: p.CustomerID, AmountCents: p.AmountCents, Reason: reason})
		if err != nil {
			return err
		}
		return outbox.Enqueue(ctx, tx, out)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO payments (order_id, customer_id, amount_cents, status) VALUES (?, ?, ?, 'SUCCEEDED')`,
		ev.OrderID, p.CustomerID, p.AmountCents); err != nil {
		return err
	}
	out, err := events.New(events.PaymentSucceeded, ev.OrderID, events.PaymentSucceededV1{
		CustomerID: p.CustomerID, AmountCents: p.AmountCents})
	if err != nil {
		return err
	}
	return outbox.Enqueue(ctx, tx, out)
}

// Routes returns the HTTP API.
func (s *Service) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /accounts/{id}", s.getAccount)
	mux.HandleFunc("PUT /accounts/{id}", s.putAccount)
	mux.HandleFunc("GET /payments/{order_id}", s.getPayment)
	return mux
}

// Account returns a customer's balance.
func (s *Service) Account(ctx context.Context, id string) (Account, error) {
	a := Account{CustomerID: id}
	err := s.b.DB.QueryRowContext(ctx, `SELECT balance_cents FROM accounts WHERE customer_id = ?`, id).Scan(&a.BalanceCents)
	return a, err
}

func (s *Service) getAccount(w http.ResponseWriter, r *http.Request) {
	a, err := s.Account(r.Context(), r.PathValue("id"))
	if errors.Is(err, sql.ErrNoRows) {
		httpx.Error(w, http.StatusNotFound, "account not found")
		return
	}
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, err.Error())
		return
	}
	httpx.JSON(w, http.StatusOK, a)
}

func (s *Service) putAccount(w http.ResponseWriter, r *http.Request) {
	var req struct {
		BalanceCents int64 `json:"balance_cents"`
	}
	if err := httpx.Decode(w, r, &req); err != nil || req.BalanceCents < 0 {
		httpx.Error(w, http.StatusBadRequest, "body must be {\"balance_cents\": n >= 0}")
		return
	}
	id := r.PathValue("id")
	if _, err := s.b.DB.ExecContext(r.Context(),
		`INSERT INTO accounts (customer_id, balance_cents) VALUES (?, ?) ON CONFLICT(customer_id) DO UPDATE SET balance_cents = excluded.balance_cents`,
		id, req.BalanceCents); err != nil {
		httpx.Error(w, http.StatusInternalServerError, err.Error())
		return
	}
	httpx.JSON(w, http.StatusOK, Account{CustomerID: id, BalanceCents: req.BalanceCents})
}

// Payment returns the charge for an order.
func (s *Service) Payment(ctx context.Context, orderID string) (Payment, error) {
	var p Payment
	err := s.b.DB.QueryRowContext(ctx, `SELECT order_id, customer_id, amount_cents, status, reason, created_at FROM payments WHERE order_id = ?`, orderID).
		Scan(&p.OrderID, &p.CustomerID, &p.AmountCents, &p.Status, &p.Reason, &p.CreatedAt)
	return p, err
}

func (s *Service) getPayment(w http.ResponseWriter, r *http.Request) {
	p, err := s.Payment(r.Context(), r.PathValue("order_id"))
	if errors.Is(err, sql.ErrNoRows) {
		httpx.Error(w, http.StatusNotFound, "payment not found")
		return
	}
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, err.Error())
		return
	}
	httpx.JSON(w, http.StatusOK, p)
}

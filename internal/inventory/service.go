// Package inventory owns stock levels and reservations. It is the first saga
// participant and performs the compensating action (release) when payment fails.
package inventory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/events"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/db"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/httpx"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/messaging"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/outbox"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/service"
)

// Schema of the inventory database. CHECK constraints make overselling
// impossible even if application logic were wrong.
const Schema = `
CREATE TABLE IF NOT EXISTS stock (
	sku       TEXT PRIMARY KEY,
	available INTEGER NOT NULL CHECK (available >= 0),
	reserved  INTEGER NOT NULL DEFAULT 0 CHECK (reserved >= 0)
);
CREATE TABLE IF NOT EXISTS reservations (
	order_id TEXT NOT NULL,
	sku      TEXT NOT NULL REFERENCES stock(sku),
	qty      INTEGER NOT NULL CHECK (qty > 0),
	status   TEXT NOT NULL CHECK (status IN ('RESERVED','RELEASED','COMMITTED')),
	PRIMARY KEY (order_id, sku)
);
`

// DefaultSeed is the demo catalogue.
const DefaultSeed = "sku-keyboard=25,sku-mouse=50,sku-monitor=5,sku-gpu=1"

// Stock is the API representation of one SKU.
type Stock struct {
	SKU       string `json:"sku"`
	Available int    `json:"available"`
	Reserved  int    `json:"reserved"`
}

// Service is the inventory service.
type Service struct{ b *service.Base }

// New creates the service and seeds the catalogue ("sku=qty,sku=qty"); existing
// rows are left untouched so restarts never reset stock.
func New(ctx context.Context, b *service.Base, seed string) (*Service, error) {
	s := &Service{b: b}
	stock, err := ParseSeed(seed)
	if err != nil {
		return nil, err
	}
	for sku, qty := range stock {
		if _, err := b.DB.ExecContext(ctx, `INSERT OR IGNORE INTO stock (sku, available) VALUES (?, ?)`, sku, qty); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// ParseSeed parses "sku=qty,..." into a map.
func ParseSeed(seed string) (map[string]int, error) {
	out := map[string]int{}
	for _, part := range strings.Split(seed, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		sku, qtyS, ok := strings.Cut(part, "=")
		qty, err := strconv.Atoi(strings.TrimSpace(qtyS))
		if !ok || err != nil || qty < 0 || strings.TrimSpace(sku) == "" {
			return nil, fmt.Errorf("invalid seed entry %q", part)
		}
		out[strings.TrimSpace(sku)] = qty
	}
	return out, nil
}

// Run starts the service and blocks until ctx is cancelled.
func Run(ctx context.Context, b *service.Base, seed string) error {
	s, err := New(ctx, b, seed)
	if err != nil {
		return err
	}
	return b.Run(ctx, s.Routes(), s.Consumers()...)
}

// Consumers returns the subscriptions.
func (s *Service) Consumers() []messaging.Consumer {
	return []messaging.Consumer{{
		Name:       "inventory",
		EventTypes: []string{events.OrderCreated, events.PaymentSucceeded, events.PaymentFailed},
		Handler:    s.handle,
	}}
}

func (s *Service) handle(ctx context.Context, tx *sql.Tx, ev events.Envelope) error {
	switch ev.Type {
	case events.OrderCreated:
		return s.reserve(ctx, tx, ev)
	case events.PaymentFailed:
		return s.release(ctx, tx, ev)
	case events.PaymentSucceeded:
		return s.commit(ctx, tx, ev)
	}
	return messaging.Permanent(fmt.Errorf("unexpected event type %q", ev.Type))
}

// merge sums quantities per SKU and returns them in a stable order.
func merge(items []events.Item) []events.Item {
	sum := map[string]int{}
	for _, it := range items {
		sum[it.SKU] += it.Qty
	}
	out := make([]events.Item, 0, len(sum))
	for sku, q := range sum {
		out = append(out, events.Item{SKU: sku, Qty: q})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SKU < out[j].SKU })
	return out
}

// reserve is all-or-nothing: either every line is reserved or none is.
func (s *Service) reserve(ctx context.Context, tx *sql.Tx, ev events.Envelope) error {
	var p events.OrderCreatedV1
	if err := ev.Payload(&p); err != nil {
		return err
	}
	if len(p.Items) == 0 {
		return messaging.Permanent(errors.New("order has no items"))
	}
	for _, it := range p.Items {
		if it.Qty <= 0 || it.SKU == "" {
			return messaging.Permanent(fmt.Errorf("invalid item %+v", it))
		}
	}
	// Business-level idempotency on top of the inbox: never reserve twice.
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM reservations WHERE order_id = ?`, ev.OrderID).Scan(&exists); err != nil {
		return err
	}
	if exists > 0 {
		return nil
	}

	items := merge(p.Items)
	reject := func(reason string) error {
		out, err := events.New(events.InventoryRejected, ev.OrderID, events.InventoryRejectedV1{Reason: reason})
		if err != nil {
			return err
		}
		return outbox.Enqueue(ctx, tx, out)
	}
	for _, it := range items {
		var avail int
		err := tx.QueryRowContext(ctx, `SELECT available FROM stock WHERE sku = ?`, it.SKU).Scan(&avail)
		if errors.Is(err, sql.ErrNoRows) {
			return reject("unknown sku " + it.SKU)
		}
		if err != nil {
			return err
		}
		if avail < it.Qty {
			return reject(fmt.Sprintf("insufficient stock for %s: requested %d, available %d", it.SKU, it.Qty, avail))
		}
	}
	for _, it := range items {
		if _, err := tx.ExecContext(ctx, `UPDATE stock SET available = available - ?, reserved = reserved + ? WHERE sku = ?`, it.Qty, it.Qty, it.SKU); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO reservations (order_id, sku, qty, status) VALUES (?, ?, ?, 'RESERVED')`, ev.OrderID, it.SKU, it.Qty); err != nil {
			return err
		}
	}
	out, err := events.New(events.InventoryReserved, ev.OrderID, events.InventoryReservedV1{
		CustomerID: p.CustomerID, AmountCents: p.AmountCents, Items: items})
	if err != nil {
		return err
	}
	return outbox.Enqueue(ctx, tx, out)
}

func (s *Service) reservations(ctx context.Context, tx *sql.Tx, orderID string) ([]events.Item, error) {
	rows, err := tx.QueryContext(ctx, `SELECT sku, qty FROM reservations WHERE order_id = ? AND status = 'RESERVED' ORDER BY sku`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []events.Item
	for rows.Next() {
		var it events.Item
		if err := rows.Scan(&it.SKU, &it.Qty); err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// release is the compensating transaction for a failed payment.
func (s *Service) release(ctx context.Context, tx *sql.Tx, ev events.Envelope) error {
	items, err := s.reservations(ctx, tx, ev.OrderID)
	if err != nil || len(items) == 0 {
		return err // nothing reserved (or already released): no-op
	}
	for _, it := range items {
		if _, err := tx.ExecContext(ctx, `UPDATE stock SET available = available + ?, reserved = reserved - ? WHERE sku = ?`, it.Qty, it.Qty, it.SKU); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE reservations SET status = 'RELEASED' WHERE order_id = ? AND status = 'RESERVED'`, ev.OrderID); err != nil {
		return err
	}
	out, err := events.New(events.InventoryReleased, ev.OrderID, events.InventoryReleasedV1{Items: items})
	if err != nil {
		return err
	}
	return outbox.Enqueue(ctx, tx, out)
}

// commit turns a reservation into a definitive stock decrement.
func (s *Service) commit(ctx context.Context, tx *sql.Tx, ev events.Envelope) error {
	items, err := s.reservations(ctx, tx, ev.OrderID)
	if err != nil || len(items) == 0 {
		return err
	}
	for _, it := range items {
		if _, err := tx.ExecContext(ctx, `UPDATE stock SET reserved = reserved - ? WHERE sku = ?`, it.Qty, it.SKU); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE reservations SET status = 'COMMITTED' WHERE order_id = ? AND status = 'RESERVED'`, ev.OrderID)
	return err
}

// Routes returns the HTTP API.
func (s *Service) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stock", s.list)
	mux.HandleFunc("GET /stock/{sku}", s.get)
	mux.HandleFunc("PUT /stock/{sku}", s.put)
	return mux
}

// Get returns one SKU.
func (s *Service) Get(ctx context.Context, sku string) (Stock, error) {
	var st Stock
	err := s.b.DB.QueryRowContext(ctx, `SELECT sku, available, reserved FROM stock WHERE sku = ?`, sku).Scan(&st.SKU, &st.Available, &st.Reserved)
	return st, err
}

func (s *Service) get(w http.ResponseWriter, r *http.Request) {
	st, err := s.Get(r.Context(), r.PathValue("sku"))
	if errors.Is(err, sql.ErrNoRows) {
		httpx.Error(w, http.StatusNotFound, "sku not found")
		return
	}
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, err.Error())
		return
	}
	httpx.JSON(w, http.StatusOK, st)
}

func (s *Service) list(w http.ResponseWriter, r *http.Request) {
	rows, err := s.b.DB.QueryContext(r.Context(), `SELECT sku, available, reserved FROM stock ORDER BY sku`)
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	out := []Stock{}
	for rows.Next() {
		var st Stock
		if err := rows.Scan(&st.SKU, &st.Available, &st.Reserved); err != nil {
			httpx.Error(w, http.StatusInternalServerError, err.Error())
			return
		}
		out = append(out, st)
	}
	httpx.JSON(w, http.StatusOK, out)
}

// put sets the available quantity (restock / correction).
func (s *Service) put(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Available int `json:"available"`
	}
	if err := httpx.Decode(w, r, &req); err != nil || req.Available < 0 {
		httpx.Error(w, http.StatusBadRequest, "body must be {\"available\": n >= 0}")
		return
	}
	sku := r.PathValue("sku")
	err := db.InTx(r.Context(), s.b.DB, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(r.Context(),
			`INSERT INTO stock (sku, available) VALUES (?, ?) ON CONFLICT(sku) DO UPDATE SET available = excluded.available`, sku, req.Available)
		return err
	})
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, err.Error())
		return
	}
	st, _ := s.Get(r.Context(), sku)
	httpx.JSON(w, http.StatusOK, st)
}

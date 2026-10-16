// Package events defines the wire contract shared by every service: the
// envelope, the subject naming scheme and the typed payloads of each event.
//
// Services never import each other's domain packages; this package is the only
// coupling point, exactly like a schema registry would be in a larger system.
package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Stream and subject layout.
const (
	// StreamEvents holds every domain event under "events.>".
	StreamEvents = "EVENTS"
	// StreamDLQ holds poison messages under "dlq.>".
	StreamDLQ = "DLQ"

	SubjectPrefix = "events."
	DLQPrefix     = "dlq."
)

// Event types. The NATS subject is SubjectPrefix + type.
const (
	OrderCreated   = "orders.created"
	OrderConfirmed = "orders.confirmed"
	OrderCancelled = "orders.cancelled"
	OrderRejected  = "orders.rejected"

	InventoryReserved = "inventory.reserved"
	InventoryRejected = "inventory.rejected"
	InventoryReleased = "inventory.released"

	PaymentSucceeded = "payments.succeeded"
	PaymentFailed    = "payments.failed"
)

// Subject returns the NATS subject for an event type.
func Subject(eventType string) string { return SubjectPrefix + eventType }

// Envelope is the unit that travels over the bus. ID is globally unique and is
// used both as the JetStream Nats-Msg-Id (publisher-side de-duplication) and as
// the idempotency key in every consumer's inbox table.
type Envelope struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	OrderID    string          `json:"order_id"`
	OccurredAt time.Time       `json:"occurred_at"`
	Data       json.RawMessage `json:"data"`
}

// New builds an envelope with a time-ordered UUIDv7 id.
func New(eventType, orderID string, payload any) (Envelope, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("marshal %s payload: %w", eventType, err)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{
		ID:         id.String(),
		Type:       eventType,
		OrderID:    orderID,
		OccurredAt: time.Now().UTC(),
		Data:       raw,
	}, nil
}

// ErrMalformed marks an envelope that can never be processed.
var ErrMalformed = errors.New("malformed event")

// Decode parses and validates an envelope.
func Decode(b []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(b, &e); err != nil {
		return Envelope{}, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if e.ID == "" || e.Type == "" || e.OrderID == "" {
		return Envelope{}, fmt.Errorf("%w: id, type and order_id are required", ErrMalformed)
	}
	return e, nil
}

// Payload unmarshals the envelope data into v, reporting ErrMalformed on failure.
func (e Envelope) Payload(v any) error {
	if err := json.Unmarshal(e.Data, v); err != nil {
		return fmt.Errorf("%w: %s payload: %w", ErrMalformed, e.Type, err)
	}
	return nil
}

// Item is one order line.
type Item struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

// OrderCreatedV1 starts the saga.
type OrderCreatedV1 struct {
	CustomerID  string `json:"customer_id"`
	AmountCents int64  `json:"amount_cents"`
	Items       []Item `json:"items"`
}

// OrderFinishedV1 is emitted once an order reaches a terminal state.
type OrderFinishedV1 struct {
	CustomerID  string `json:"customer_id"`
	AmountCents int64  `json:"amount_cents"`
	Status      string `json:"status"`
	Reason      string `json:"reason,omitempty"`
}

// InventoryReservedV1 carries forward what the payment step needs, so the
// payments service does not have to query the orders service (no sync calls).
type InventoryReservedV1 struct {
	CustomerID  string `json:"customer_id"`
	AmountCents int64  `json:"amount_cents"`
	Items       []Item `json:"items"`
}

// InventoryRejectedV1 ends the saga early.
type InventoryRejectedV1 struct {
	Reason string `json:"reason"`
}

// InventoryReleasedV1 confirms the compensation step.
type InventoryReleasedV1 struct {
	Items []Item `json:"items"`
}

// PaymentSucceededV1 is emitted after a successful debit.
type PaymentSucceededV1 struct {
	CustomerID  string `json:"customer_id"`
	AmountCents int64  `json:"amount_cents"`
}

// PaymentFailedV1 triggers compensation in inventory.
type PaymentFailedV1 struct {
	CustomerID  string `json:"customer_id"`
	AmountCents int64  `json:"amount_cents"`
	Reason      string `json:"reason"`
}

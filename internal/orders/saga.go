// Package orders owns the Order aggregate and tracks the order saga.
package orders

import "github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/events"

// Status is the saga state of an order.
type Status string

const (
	StatusPending   Status = "PENDING"   // created, waiting for inventory
	StatusReserved  Status = "RESERVED"  // stock reserved, waiting for payment
	StatusConfirmed Status = "CONFIRMED" // paid — terminal
	StatusCancelled Status = "CANCELLED" // payment failed, stock released — terminal
	StatusRejected  Status = "REJECTED"  // out of stock — terminal
)

// Terminal reports whether no further transitions are possible.
func (s Status) Terminal() bool {
	return s == StatusConfirmed || s == StatusCancelled || s == StatusRejected
}

// Next is the saga transition function. It is total and monotonic: events that
// arrive late, twice, or out of order (e.g. after a NAK-induced redelivery)
// never move an order backwards or out of a terminal state; they simply return
// changed=false.
func Next(s Status, eventType string) (next Status, changed bool) {
	if s.Terminal() {
		return s, false
	}
	switch eventType {
	case events.InventoryReserved:
		if s == StatusPending {
			return StatusReserved, true
		}
	case events.InventoryRejected:
		if s == StatusPending {
			return StatusRejected, true
		}
	case events.PaymentSucceeded:
		return StatusConfirmed, true // from PENDING or RESERVED
	case events.PaymentFailed:
		return StatusCancelled, true
	}
	return s, false
}

// FinishedEvent maps a terminal status to the event announcing it.
func FinishedEvent(s Status) string {
	switch s {
	case StatusConfirmed:
		return events.OrderConfirmed
	case StatusCancelled:
		return events.OrderCancelled
	case StatusRejected:
		return events.OrderRejected
	}
	return ""
}

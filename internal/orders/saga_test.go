package orders

import (
	"math/rand/v2"
	"testing"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/events"
)

func TestHappyAndUnhappyPaths(t *testing.T) {
	cases := []struct {
		name string
		evs  []string
		want Status
	}{
		{"confirmed", []string{events.InventoryReserved, events.PaymentSucceeded}, StatusConfirmed},
		{"cancelled", []string{events.InventoryReserved, events.PaymentFailed, events.InventoryReleased}, StatusCancelled},
		{"rejected", []string{events.InventoryRejected}, StatusRejected},
		{"payment before reservation (reordered)", []string{events.PaymentSucceeded, events.InventoryReserved}, StatusConfirmed},
		{"duplicates", []string{events.InventoryReserved, events.InventoryReserved, events.PaymentSucceeded, events.PaymentSucceeded}, StatusConfirmed},
		{"late failure after confirm", []string{events.InventoryReserved, events.PaymentSucceeded, events.PaymentFailed}, StatusConfirmed},
		{"unrelated event", []string{events.OrderCreated}, StatusPending},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := StatusPending
			for _, e := range c.evs {
				s, _ = Next(s, e)
			}
			if s != c.want {
				t.Fatalf("got %s want %s", s, c.want)
			}
		})
	}
}

// Property: whatever sequence of events arrives, once terminal the order never
// changes again, and the first terminal state reached is final.
func TestTerminalStatesAreAbsorbing(t *testing.T) {
	all := []string{events.InventoryReserved, events.InventoryRejected, events.InventoryReleased,
		events.PaymentSucceeded, events.PaymentFailed, events.OrderCreated}
	r := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < 10000; i++ {
		s := StatusPending
		var first Status
		for j := 0; j < 8; j++ {
			n, changed := Next(s, all[r.IntN(len(all))])
			if s.Terminal() && (changed || n != s) {
				t.Fatalf("terminal %s moved to %s", s, n)
			}
			s = n
			if s.Terminal() && first == "" {
				first = s
			}
		}
		if first != "" && s != first {
			t.Fatalf("terminal state changed from %s to %s", first, s)
		}
	}
}

func TestFinishedEvent(t *testing.T) {
	if FinishedEvent(StatusConfirmed) != events.OrderConfirmed || FinishedEvent(StatusCancelled) != events.OrderCancelled ||
		FinishedEvent(StatusRejected) != events.OrderRejected || FinishedEvent(StatusPending) != "" {
		t.Fatal("mapping")
	}
}

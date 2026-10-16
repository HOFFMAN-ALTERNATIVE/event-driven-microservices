// Command inventory runs the inventory service (reservations + compensation).
package main

import (
	"context"
	"os"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/inventory"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/service"
)

func main() {
	seed := os.Getenv("INVENTORY_SEED")
	if seed == "" {
		seed = inventory.DefaultSeed
	}
	service.Main("inventory", inventory.Schema, func(ctx context.Context, b *service.Base) error {
		return inventory.Run(ctx, b, seed)
	})
}

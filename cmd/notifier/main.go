// Command notifier runs the notification service.
package main

import (
	"context"
	"os"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/notifier"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/service"
)

func main() {
	service.Main("notifier", notifier.Schema, func(ctx context.Context, b *service.Base) error {
		return notifier.Run(ctx, b, os.Getenv("WEBHOOK_URL"))
	})
}

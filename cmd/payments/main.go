// Command payments runs the payments service.
package main

import (
	"context"
	"os"
	"strconv"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/payments"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/service"
)

func main() {
	balance, _ := strconv.ParseInt(os.Getenv("PAYMENTS_DEFAULT_BALANCE_CENTS"), 10, 64)
	rate, _ := strconv.ParseFloat(os.Getenv("PAYMENTS_FAILURE_RATE"), 64)
	service.Main("payments", payments.Schema, func(ctx context.Context, b *service.Base) error {
		return payments.Run(ctx, b, payments.Options{DefaultBalanceCents: balance, Fault: payments.RandomFault(rate)})
	})
}

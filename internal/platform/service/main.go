package service

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/telemetry"
)

// Main is the entrypoint shared by all service binaries: signals, telemetry,
// infrastructure, run, exit code.
func Main(name, schema string, run func(ctx context.Context, b *Base) error) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log := telemetry.Logger(name)

	shutdown, err := telemetry.Setup(ctx, name)
	if err != nil {
		log.Error("telemetry setup failed", "err", err)
		os.Exit(1)
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdown(sctx)
	}()

	b, err := New(ctx, ConfigFromEnv(name), log, schema)
	if err != nil {
		log.Error("startup failed", "err", err)
		os.Exit(1) //nolint:gocritic // telemetry has nothing to flush yet
	}
	if err := run(ctx, b); err != nil {
		log.Error("service stopped with error", "err", err)
		stop()
		os.Exit(1)
	}
	log.Info("bye")
}

package service

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/telemetry"
)

// Main is the entrypoint shared by all service binaries: signals, telemetry,
// infrastructure, run, exit code.
func Main(name, schema string, run func(ctx context.Context, b *Base) error) {
	// `<service> healthcheck` probes /healthz over loopback so the distroless
	// image (no shell, no wget) can still run a container healthcheck.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		if err := Healthcheck(os.Getenv("HTTP_ADDR")); err != nil {
			fmt.Fprintln(os.Stderr, "unhealthy:", err)
			os.Exit(1)
		}
		return
	}
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

// Healthcheck GETs /healthz on the port of addr (default :8080) via 127.0.0.1.
func Healthcheck(addr string) error {
	if addr == "" {
		addr = ":8080"
	}
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("invalid port %q", portStr)
	}
	client := &http.Client{Timeout: 2 * time.Second}
	// Host is fixed to loopback and the port is a validated integer.
	resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", port)) //nolint:gosec,noctx // loopback probe
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

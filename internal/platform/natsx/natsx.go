// Package natsx connects to NATS and declares the JetStream topology.
package natsx

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/events"
)

// DedupWindow is how long JetStream remembers Nats-Msg-Id values. Any outbox
// re-publish inside this window is dropped by the server.
const DedupWindow = 2 * time.Minute

// Connect dials NATS with reconnect forever semantics, suitable for services.
func Connect(url, name string, log *slog.Logger) (*nats.Conn, jetstream.JetStream, error) {
	nc, err := nats.Connect(url,
		nats.Name(name),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500*time.Millisecond),
		nats.RetryOnFailedConnect(true),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				log.Warn("nats disconnected", "err", err)
			}
		}),
		nats.ReconnectHandler(func(c *nats.Conn) { log.Info("nats reconnected", "url", c.ConnectedUrl()) }),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("nats connect: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, err
	}
	return nc, js, nil
}

// EnsureTopology idempotently creates/updates the EVENTS and DLQ streams.
// Every service calls it on boot, so start-up order does not matter.
func EnsureTopology(ctx context.Context, js jetstream.JetStream) error {
	streams := []jetstream.StreamConfig{
		{
			Name:        events.StreamEvents,
			Description: "Domain events of all services",
			Subjects:    []string{events.SubjectPrefix + ">"},
			Storage:     jetstream.FileStorage,
			Retention:   jetstream.LimitsPolicy,
			MaxAge:      7 * 24 * time.Hour,
			Duplicates:  DedupWindow,
		},
		{
			Name:        events.StreamDLQ,
			Description: "Poison messages parked for inspection and replay",
			Subjects:    []string{events.DLQPrefix + ">"},
			Storage:     jetstream.FileStorage,
			Retention:   jetstream.LimitsPolicy,
			MaxAge:      30 * 24 * time.Hour,
			Duplicates:  DedupWindow,
		},
	}
	for _, cfg := range streams {
		if _, err := js.CreateOrUpdateStream(ctx, cfg); err != nil {
			return fmt.Errorf("ensure stream %s: %w", cfg.Name, err)
		}
	}
	return nil
}

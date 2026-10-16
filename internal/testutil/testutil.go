// Package testutil builds a NATS-less service.Base for handler-level unit tests.
package testutil

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/events"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/db"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/messaging"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/outbox"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/service"
)

// Base returns a service base backed by a temp SQLite file and no NATS.
func Base(t *testing.T, schema string) *service.Base {
	t.Helper()
	d, err := db.Open(context.Background(), filepath.Join(t.TempDir(), "svc.db"), schema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &service.Base{DB: d, Log: log, Relay: outbox.NewRelay(d, nil, log)}
}

// Apply runs handler h for ev in a transaction, like the consumer runner does.
func Apply(t *testing.T, b *service.Base, h messaging.Handler, ev events.Envelope) error {
	t.Helper()
	return db.InTx(context.Background(), b.DB, func(tx *sql.Tx) error { return h(context.Background(), tx, ev) })
}

// Event builds an envelope or fails the test.
func Event(t *testing.T, typ, orderID string, payload any) events.Envelope {
	t.Helper()
	ev, err := events.New(typ, orderID, payload)
	if err != nil {
		t.Fatal(err)
	}
	return ev
}

// Outbox returns the event types currently pending in the outbox, in order.
func Outbox(t *testing.T, b *service.Base) []events.Envelope {
	t.Helper()
	rows, err := b.DB.Query(`SELECT payload FROM outbox WHERE published_at IS NULL ORDER BY seq`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []events.Envelope
	for rows.Next() {
		var p []byte
		if err := rows.Scan(&p); err != nil {
			t.Fatal(err)
		}
		var e events.Envelope
		if err := json.Unmarshal(p, &e); err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	return out
}

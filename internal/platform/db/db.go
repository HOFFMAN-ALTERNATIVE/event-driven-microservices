// Package db opens a service-owned SQLite database (database-per-service) and
// applies the platform schema (outbox + inbox) plus the service's own schema.
package db

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

	_ "modernc.org/sqlite" // pure-Go driver, no cgo
)

// PlatformSchema is shared by every service: the transactional outbox and the
// consumer inbox used for idempotency.
const PlatformSchema = `
CREATE TABLE IF NOT EXISTS outbox (
	seq          INTEGER PRIMARY KEY AUTOINCREMENT,
	event_id     TEXT    NOT NULL UNIQUE,
	subject      TEXT    NOT NULL,
	payload      BLOB    NOT NULL,
	headers      TEXT    NOT NULL DEFAULT '{}',
	created_at   TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
	published_at TEXT,
	attempts     INTEGER NOT NULL DEFAULT 0,
	last_error   TEXT
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(seq) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS inbox (
	consumer     TEXT NOT NULL,
	event_id     TEXT NOT NULL,
	processed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
	PRIMARY KEY (consumer, event_id)
);
`

// Open opens (or creates) the database at path. Use ":memory:" only in tests
// that do not need to share the connection: SQLite in-memory databases are
// per-connection, which is why the pool is pinned to a single connection.
//
// A single connection also serialises writers, which is exactly SQLite's
// concurrency model; WAL keeps readers from other processes non-blocking.
func Open(ctx context.Context, path string, schema ...string) (*sql.DB, error) {
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "foreign_keys(ON)")
	q.Add("_pragma", "synchronous(NORMAL)")
	q.Set("_txlock", "immediate")
	dsn := "file:" + path + "?" + q.Encode()

	d, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	d.SetMaxOpenConns(1)
	d.SetMaxIdleConns(1)
	d.SetConnMaxLifetime(0)

	if err := d.PingContext(ctx); err != nil {
		_ = d.Close()
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	for _, s := range append([]string{PlatformSchema}, schema...) {
		if _, err := d.ExecContext(ctx, s); err != nil {
			_ = d.Close()
			return nil, fmt.Errorf("migrate %s: %w", path, err)
		}
	}
	return d, nil
}

// InTx runs fn inside a transaction, committing on success and rolling back on
// error or panic.
func InTx(ctx context.Context, d *sql.DB, fn func(*sql.Tx) error) (err error) {
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

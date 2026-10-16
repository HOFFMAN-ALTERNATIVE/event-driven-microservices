package db

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

func TestOpenAppliesSchemaAndInTx(t *testing.T) {
	ctx := context.Background()
	d, err := Open(ctx, filepath.Join(t.TempDir(), "t.db"), `CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)`)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	if err := InTx(ctx, d, func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO kv VALUES ('a','1')`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	if err := InTx(ctx, d, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT INTO kv VALUES ('b','2')`); err != nil {
			return err
		}
		return boom
	}); !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
	var n int
	if err := d.QueryRow(`SELECT count(*) FROM kv`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("rollback failed: n=%d err=%v", n, err)
	}
	for _, tbl := range []string{"outbox", "inbox"} {
		if _, err := d.Exec(`SELECT 1 FROM ` + tbl + ` LIMIT 1`); err != nil {
			t.Fatalf("platform table %s missing: %v", tbl, err)
		}
	}
}

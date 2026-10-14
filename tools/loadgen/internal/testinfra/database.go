package testinfra

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NewDatabase creates a throwaway database from db/schema.sql, returns a pool
// on it, and drops it when the test ends. The database user needs CREATEDB.
func NewDatabase(t testing.TB, adminURL string) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	suffix := make([]byte, 6)
	_, err := rand.Read(suffix)
	if err != nil {
		t.Fatal(err)
	}
	name := "loadgen_test_" + hex.EncodeToString(suffix)

	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("connect to PostgreSQL: %v", err)
	}
	_, err = admin.Exec(ctx, "CREATE DATABASE "+name)
	if err != nil {
		admin.Close(ctx)
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		_, dropErr := admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		if dropErr != nil {
			t.Errorf("drop database %s: %v", name, dropErr)
		}
		admin.Close(ctx)
	})

	parsed, err := url.Parse(adminURL)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + name
	databaseURL := parsed.String()

	// The schema snapshot empties the session's search_path, so it runs on a
	// connection of its own that is closed afterwards, never on a pooled one.
	loader, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to %s: %v", name, err)
	}
	_, err = loader.Exec(ctx, loadSchema(t))
	loader.Close(ctx)
	if err != nil {
		t.Fatalf("apply db/schema.sql: %v", err)
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to %s: %v", name, err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// loadSchema reads the committed snapshot without the psql meta-commands
// (\restrict) pg_dump emits, which the server cannot parse.
func loadSchema(t testing.TB) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	path := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "db", "schema.sql")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	var kept []string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, `\`) {
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}

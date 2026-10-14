package sample

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresCollector reads cheap, load-safe signals. It never counts the bids
// table, which at hundreds of millions of rows would be the load it is meant to
// observe: bid and order totals come from the identity sequences, which is O(1).
// The outbox count is proportional to the unpublished backlog (it reads a
// partial index holding only unpublished rows), so it grows with the very
// problem it reports; the statement timeout bounds it.
type PostgresCollector struct {
	pool *pgxpool.Pool
}

func NewPostgresCollector(pool *pgxpool.Pool) *PostgresCollector {
	return &PostgresCollector{pool: pool}
}

// Collect runs its queries in one read-only transaction.
func (c *PostgresCollector) Collect(ctx context.Context) (PostgresSnapshot, error) {
	var snapshot PostgresSnapshot

	transaction, err := c.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return snapshot, fmt.Errorf("begin: %w", err)
	}
	defer transaction.Rollback(ctx)

	snapshot.BidsIssued, err = sequenceValue(ctx, transaction, "bids_id_seq")
	if err != nil {
		return snapshot, err
	}
	snapshot.OrdersIssued, err = sequenceValue(ctx, transaction, "orders_id_seq")
	if err != nil {
		return snapshot, err
	}

	err = transaction.QueryRow(ctx, `
		SELECT
		  count(*),
		  COALESCE(EXTRACT(EPOCH FROM clock_timestamp() - min(occurred_at)), 0)::float8,
		  count(*) FILTER (WHERE publish_attempts > 0)
		FROM outbox_events
		WHERE published_at IS NULL`).Scan(
		&snapshot.OutboxUnpublished, &snapshot.OutboxOldestAgeSeconds, &snapshot.OutboxRetrying)
	if err != nil {
		return snapshot, fmt.Errorf("read outbox: %w", err)
	}

	err = transaction.QueryRow(ctx, `
		SELECT
		  count(*) FILTER (WHERE wait_event_type = 'Lock'),
		  count(*) FILTER (WHERE state = 'idle in transaction'),
		  count(*) FILTER (WHERE state = 'active')
		FROM pg_stat_activity
		WHERE datname = current_database()
		  AND backend_type = 'client backend'
		  AND pid <> pg_backend_pid()`).Scan(
		&snapshot.LockWaiters, &snapshot.IdleInTransaction, &snapshot.Active)
	if err != nil {
		return snapshot, fmt.Errorf("read connections: %w", err)
	}
	return snapshot, nil
}

// sequenceValue is the highest id issued, or 0 if none was.
func sequenceValue(ctx context.Context, transaction pgx.Tx, sequence string) (int64, error) {
	var last int64
	var called bool
	// The name is one of two constants in this file, never caller input.
	err := transaction.QueryRow(ctx, "SELECT last_value, is_called FROM "+sequence).Scan(&last, &called)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", sequence, err)
	}
	if !called {
		return 0, nil
	}
	return last, nil
}

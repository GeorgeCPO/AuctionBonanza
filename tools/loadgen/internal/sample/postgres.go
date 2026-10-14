package sample

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// collectQuery is one statement, so a reading costs one round trip and one
// snapshot. The sequence names are fixed here; an integration test runs this
// against the real schema, so a renamed sequence fails a test, not a sampler.
const collectQuery = `
SELECT
  (SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM bids_id_seq),
  (SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM orders_id_seq),
  outbox.unpublished, outbox.oldest_age_seconds, outbox.retrying,
  connections.lock_waiters, connections.idle_in_transaction, connections.active
FROM
  (SELECT
     count(*) AS unpublished,
     COALESCE(EXTRACT(EPOCH FROM clock_timestamp() - min(occurred_at)), 0)::float8 AS oldest_age_seconds,
     count(*) FILTER (WHERE publish_attempts > 0) AS retrying
   FROM outbox_events
   WHERE published_at IS NULL) AS outbox,
  (SELECT
     count(*) FILTER (WHERE wait_event_type = 'Lock') AS lock_waiters,
     count(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_transaction,
     count(*) FILTER (WHERE state = 'active') AS active
   FROM pg_stat_activity
   WHERE datname = current_database()
     AND backend_type = 'client backend'
     AND pid <> pg_backend_pid()) AS connections`

// PostgresCollector reads cheap, load-safe signals. It never counts the bids
// table, which at hundreds of millions of rows would be the load it is meant to
// observe: bid and order totals come from the identity sequences, which is O(1).
// The outbox count is proportional to the unpublished backlog (it reads a
// partial index holding only unpublished rows), so it grows with the very
// problem it reports; the connection's statement timeout bounds it.
type PostgresCollector struct {
	pool *pgxpool.Pool
}

func NewPostgresCollector(pool *pgxpool.Pool) *PostgresCollector {
	return &PostgresCollector{pool: pool}
}

func (c *PostgresCollector) Collect(ctx context.Context) (PostgresSnapshot, error) {
	var snapshot PostgresSnapshot
	err := c.pool.QueryRow(ctx, collectQuery).Scan(
		&snapshot.BidsIssued, &snapshot.OrdersIssued,
		&snapshot.OutboxUnpublished, &snapshot.OutboxOldestAgeSeconds, &snapshot.OutboxRetrying,
		&snapshot.LockWaiters, &snapshot.IdleInTransaction, &snapshot.Active,
	)
	if err != nil {
		return PostgresSnapshot{}, fmt.Errorf("read postgres: %w", err)
	}
	return snapshot, nil
}

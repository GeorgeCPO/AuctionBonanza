package sample

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultOutboxCountCap bounds how many unpublished outbox rows one reading
// counts. Beyond it the count saturates, which keeps a reading cheap exactly when
// the backlog is largest; the oldest age stays exact.
const DefaultOutboxCountCap = 1_000_000

// countersQuery reads what is cheap however large the tables are. The sequence
// names are fixed here; an integration test runs this against the real schema,
// so a renamed sequence fails a test, not a sampler.
const countersQuery = `
SELECT
  (SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM bids_id_seq),
  (SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM orders_id_seq),
  count(*) FILTER (WHERE wait_event_type = 'Lock'),
  count(*) FILTER (WHERE state = 'idle in transaction'),
  count(*) FILTER (WHERE state = 'active')
FROM pg_stat_activity
WHERE datname = current_database()
  AND backend_type = 'client backend'
  AND pid <> pg_backend_pid()`

// outboxQuery counts at most $1 unpublished rows from the partial index that
// holds only unpublished rows. The oldest age is min(occurred_at) over the same
// index, which is one index entry however long the backlog.
const outboxQuery = `
SELECT
  counts.unpublished,
  counts.retrying,
  COALESCE(EXTRACT(EPOCH FROM clock_timestamp() - (
    SELECT min(occurred_at) FROM outbox_events WHERE published_at IS NULL
  )), 0)::float8
FROM (
  SELECT count(*) AS unpublished, count(*) FILTER (WHERE publish_attempts > 0) AS retrying
  FROM (SELECT publish_attempts FROM outbox_events WHERE published_at IS NULL LIMIT $1) AS capped
) AS counts`

// PostgresCollector reads cheap, load-safe signals. It never counts the bids
// table, which at hundreds of millions of rows would be the load it is meant to
// observe: bid and order totals come from the identity sequences, which is O(1).
//
// It reads in two statements. The outbox one is the only one whose cost follows
// the backlog; if it fails or times out, the snapshot still carries everything
// else and says why the outbox fields are missing.
type PostgresCollector struct {
	pool     *pgxpool.Pool
	countCap int64
}

func NewPostgresCollector(pool *pgxpool.Pool) *PostgresCollector {
	return &PostgresCollector{pool: pool, countCap: DefaultOutboxCountCap}
}

// Collect fails only when the counters cannot be read.
func (c *PostgresCollector) Collect(ctx context.Context) (PostgresSnapshot, error) {
	var snapshot PostgresSnapshot
	err := c.pool.QueryRow(ctx, countersQuery).Scan(
		&snapshot.BidsIssued, &snapshot.OrdersIssued,
		&snapshot.LockWaiters, &snapshot.IdleInTransaction, &snapshot.Active,
	)
	if err != nil {
		return PostgresSnapshot{}, fmt.Errorf("read postgres counters: %w", err)
	}

	err = c.pool.QueryRow(ctx, outboxQuery, c.countCap).Scan(
		&snapshot.OutboxUnpublished, &snapshot.OutboxRetrying, &snapshot.OutboxOldestAgeSeconds,
	)
	if err != nil {
		snapshot.OutboxUnavailable = err.Error()
		snapshot.OutboxUnpublished, snapshot.OutboxRetrying, snapshot.OutboxOldestAgeSeconds = 0, 0, 0
	}
	return snapshot, nil
}

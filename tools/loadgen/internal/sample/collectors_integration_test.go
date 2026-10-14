//go:build integration

package sample

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/GeorgeCPO/AuctionBonanza/tools/loadgen/internal/murmur2"
	"github.com/GeorgeCPO/AuctionBonanza/tools/loadgen/internal/testinfra"
)

func TestMain(m *testing.M) {
	testinfra.RequireEnvironment("TEST_DATABASE_URL", "TEST_KAFKA_BROKERS")
	os.Exit(m.Run())
}

func exec(t *testing.T, pool *pgxpool.Pool, sql string, arguments ...any) {
	t.Helper()
	_, err := pool.Exec(context.Background(), sql, arguments...)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// insertAuction adds one open auction owned by user 'a', which the caller has
// inserted.
func insertAuction(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	exec(t, pool, `
		INSERT INTO auctions (owner_id, starts_at, ends_at, starting_price_minor, description, title, currency_code, minimum_bid_increment_minor)
		SELECT id, now() - interval '1 hour', now() + interval '1 hour', 1000, 'd', 't', 'USD', 100 FROM users WHERE username = 'a'`)
}

// insertUnpublishedEvents gives every auction one unpublished event, inserted in
// auction order. Auction n gets ages[n-1], or the only age if one is given,
// before now by the database's clock, not the test host's: the collector
// measures age with the database's clock, and the two can differ. The schema
// requires a version bump and its event to commit together, so both go in one
// transaction.
func insertUnpublishedEvents(t *testing.T, pool *pgxpool.Pool, ages ...time.Duration) {
	t.Helper()
	ctx := context.Background()

	seconds := make([]float64, len(ages))
	for i, age := range ages {
		seconds[i] = age.Seconds()
	}
	transaction, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = transaction.Exec(ctx, `UPDATE auctions SET version = 2`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = transaction.Exec(ctx, `
		WITH numbered AS (
		  SELECT a.id AS auction_id,
		         gen_random_uuid() AS id,
		         clock_timestamp() - ($1::float8[])[LEAST(row_number() OVER (ORDER BY a.id), cardinality($1::float8[]))] * interval '1 second' AS at
		  FROM auctions a
		)
		INSERT INTO outbox_events (event_id, event_type, occurred_at, auction_id, auction_version, payload)
		SELECT n.id, 'BidAccepted.v1', n.at, n.auction_id, 2,
		  jsonb_build_object('eventId', n.id::text, 'eventType', 'BidAccepted.v1',
		    'occurredAt', to_char(n.at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
		    'auctionId', n.auction_id::text, 'auctionVersion', '2')
		FROM numbered n ORDER BY n.auction_id`, seconds)
	if err != nil {
		t.Fatal(err)
	}
	err = transaction.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPostgresCollectorOnAnEmptyDatabase(t *testing.T) {
	pool := testinfra.NewDatabase(t)

	snapshot, err := NewPostgresCollector(pool).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if snapshot != (PostgresSnapshot{}) {
		t.Fatalf("an empty database reads as %+v, want all zeros", snapshot)
	}
}

func TestPostgresCollectorReadsCountersAndTheOutbox(t *testing.T) {
	ctx := context.Background()
	pool := testinfra.NewDatabase(t)
	exec(t, pool, `INSERT INTO users (username, password_hash, email) VALUES ('a', 'x', 'a@example.test'), ('b', 'x', 'b@example.test')`)
	insertAuction(t, pool)
	exec(t, pool, `
		INSERT INTO bids (auction_id, bidder_id, amount_minor, idempotency_key, created_at)
		SELECT a.id, u.id, 1000 + n * 100, 'k' || n, now()
		FROM auctions a, users u, generate_series(1, 3) AS n WHERE u.username = 'b'`)

	insertUnpublishedEvents(t, pool, 3*time.Second)

	collector := NewPostgresCollector(pool)
	snapshot, err := collector.Collect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if snapshot.BidsIssued != 3 || snapshot.OrdersIssued != 0 {
		t.Errorf("bids %d orders %d, want 3 and 0", snapshot.BidsIssued, snapshot.OrdersIssued)
	}
	if snapshot.OutboxUnpublished != 1 || snapshot.OutboxRetrying != 0 {
		t.Errorf("outbox %d unpublished %d retrying, want 1 and 0", snapshot.OutboxUnpublished, snapshot.OutboxRetrying)
	}
	if snapshot.OutboxOldestAgeSeconds < 3 || snapshot.OutboxOldestAgeSeconds > 30 {
		t.Errorf("oldest unpublished event is %.1fs old, want about 3", snapshot.OutboxOldestAgeSeconds)
	}

	exec(t, pool, `UPDATE outbox_events SET publish_attempts = 2, next_publish_attempt_at = now()`)
	snapshot, err = collector.Collect(ctx)
	if err != nil || snapshot.OutboxRetrying != 1 {
		t.Errorf("retrying %d (err %v), want 1", snapshot.OutboxRetrying, err)
	}

	exec(t, pool, `UPDATE outbox_events SET published_at = clock_timestamp(), next_publish_attempt_at = NULL`)
	snapshot, err = collector.Collect(ctx)
	if err != nil || snapshot.OutboxUnpublished != 0 || snapshot.OutboxOldestAgeSeconds != 0 {
		t.Errorf("after publishing: %+v (err %v), want an empty backlog", snapshot, err)
	}
}

func TestPostgresCollectorCapsTheOutboxCountButNotTheAge(t *testing.T) {
	ctx := context.Background()
	pool := testinfra.NewDatabase(t)
	exec(t, pool, `INSERT INTO users (username, password_hash, email) VALUES ('a', 'x', 'a@example.test')`)
	for range 5 {
		insertAuction(t, pool)
	}
	insertUnpublishedEvents(t, pool, 7*time.Second)
	collector := NewPostgresCollector(pool)
	collector.countCap = 3

	snapshot, err := collector.Collect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if snapshot.OutboxUnavailable != "" {
		t.Fatalf("outbox unavailable: %s", snapshot.OutboxUnavailable)
	}
	if snapshot.OutboxUnpublished != 3 {
		t.Errorf("counted %d of 5 unpublished events, want the cap of 3", snapshot.OutboxUnpublished)
	}
	if snapshot.OutboxOldestAgeSeconds < 7 {
		t.Errorf("oldest age %.1fs, want it exact (about 7s) even when the count is capped", snapshot.OutboxOldestAgeSeconds)
	}
}

func TestPostgresCollectorCountsRetriesAmongTheOldestEventsWhenCapped(t *testing.T) {
	ctx := context.Background()
	pool := testinfra.NewDatabase(t)
	exec(t, pool, `INSERT INTO users (username, password_hash, email) VALUES ('a', 'x', 'a@example.test')`)
	for range 5 {
		insertAuction(t, pool)
	}
	// Auction 1 is the youngest event and auction 5 the oldest, so the rows sit in
	// the table newest first. An unordered LIMIT 3 would scan auctions 1 to 3; the
	// oldest three are auctions 3 to 5.
	insertUnpublishedEvents(t, pool, 10*time.Second, 20*time.Second, 30*time.Second, 40*time.Second, 50*time.Second)
	// Without an order, which three rows LIMIT returns depends on the plan: a
	// partial-index scan happens to return the oldest first, a sequential scan
	// returns them in insertion order. Forbid index scans so an unordered query
	// would take the wrong three rows; the ordered one still takes the oldest.
	exec(t, pool, `DO $$ BEGIN EXECUTE format(
		'ALTER DATABASE %I SET enable_indexscan = off; ALTER DATABASE %I SET enable_bitmapscan = off; ALTER DATABASE %I SET enable_indexonlyscan = off',
		current_database(), current_database(), current_database()); END $$`)
	pool.Reset()
	collector := NewPostgresCollector(pool)
	collector.countCap = 3
	retrying := func() int64 {
		snapshot, err := collector.Collect(ctx)
		if err != nil || snapshot.OutboxUnavailable != "" {
			t.Fatalf("collect: %v %q", err, snapshot.OutboxUnavailable)
		}
		return snapshot.OutboxRetrying
	}

	// The oldest event is retrying and inside the oldest three. The UPDATE moves
	// that row to the end of the table, so a sequential scan without an order
	// would return auctions 1 to 3 and miss it.
	exec(t, pool, `UPDATE outbox_events SET publish_attempts = 1 WHERE auction_id = (SELECT max(id) FROM auctions)`)
	for range 3 {
		if got := retrying(); got != 1 {
			t.Fatalf("retrying %d, want 1: the oldest event is inside the capped scope", got)
		}
	}

	// The two youngest events are retrying too, but they are outside the scope.
	exec(t, pool, `UPDATE outbox_events SET publish_attempts = 1 WHERE auction_id IN (SELECT id FROM auctions ORDER BY id LIMIT 2)`)
	for range 3 {
		if got := retrying(); got != 1 {
			t.Fatalf("retrying %d, want 1: events outside the oldest three are not counted", got)
		}
	}
}

func TestPostgresCollectorKeepsTheCountersWhenOnlyTheOutboxFails(t *testing.T) {
	ctx := context.Background()
	pool := testinfra.NewDatabase(t)
	exec(t, pool, `INSERT INTO users (username, password_hash, email) VALUES ('a', 'x', 'a@example.test'), ('b', 'x', 'b@example.test')`)
	insertAuction(t, pool)
	exec(t, pool, `
		INSERT INTO bids (auction_id, bidder_id, amount_minor, idempotency_key, created_at)
		SELECT a.id, u.id, 1000 + n * 100, 'k' || n, now()
		FROM auctions a, users u, generate_series(1, 4) AS n WHERE u.username = 'b'`)
	exec(t, pool, `DROP TABLE outbox_events`)

	snapshot, err := NewPostgresCollector(pool).Collect(ctx)

	if err != nil {
		t.Fatalf("a failing outbox read failed the whole snapshot: %v", err)
	}
	if snapshot.OutboxUnavailable == "" {
		t.Error("the outbox failure was not reported")
	}
	if snapshot.BidsIssued != 4 {
		t.Errorf("bids %d, want 4: the counters must survive an outbox failure", snapshot.BidsIssued)
	}
}

// A hot auction's signature: one transaction holds the row, another waits on it.
func TestPostgresCollectorSeesALockWaiterAndAnIdleTransaction(t *testing.T) {
	ctx := context.Background()
	pool := testinfra.NewDatabase(t)
	exec(t, pool, `INSERT INTO users (username, password_hash, email) VALUES ('a', 'x', 'a@example.test')`)
	insertAuction(t, pool)

	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = holder.Exec(ctx, `SELECT id FROM auctions FOR UPDATE`)
	if err != nil {
		t.Fatal(err)
	}

	var waiter sync.WaitGroup
	waiter.Add(1)
	go func() {
		defer waiter.Done()
		blocked, beginErr := pool.Begin(ctx)
		if beginErr != nil {
			t.Error(beginErr)
			return
		}
		_, queryErr := blocked.Exec(ctx, `SELECT id FROM auctions FOR UPDATE`)
		if queryErr != nil {
			t.Error(queryErr)
		}
		_ = blocked.Commit(ctx)
	}()

	collector := NewPostgresCollector(pool)
	deadline := time.Now().Add(5 * time.Second)
	var snapshot PostgresSnapshot
	for time.Now().Before(deadline) {
		snapshot, err = collector.Collect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.LockWaiters >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if snapshot.LockWaiters != 1 {
		t.Errorf("lock waiters %d, want 1", snapshot.LockWaiters)
	}
	if snapshot.IdleInTransaction != 1 {
		t.Errorf("idle in transaction %d, want the lock holder", snapshot.IdleInTransaction)
	}

	err = holder.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	waiter.Wait()
}

func TestKafkaCollectorReadsOffsetsAndGroupLag(t *testing.T) {
	ctx := context.Background()
	client := testinfra.NewKafka(t)
	const partitions = 4
	topic := testinfra.NewTopic(t, client, partitions)

	expected := make([]int64, partitions)
	var records []*kgo.Record
	for id := 1; id <= 60; id++ {
		key := strconv.Itoa(id)
		expected[murmur2.PartitionForKey(key, partitions)]++
		records = append(records, &kgo.Record{Topic: topic, Key: []byte(key), Value: []byte("event")})
	}
	produced := client.ProduceSync(ctx, records...)
	err := produced.FirstErr()
	if err != nil {
		t.Fatal(err)
	}

	collector := NewKafkaCollector(kadm.NewClient(client), topic, []string{"never-committed", "partly-committed"})
	snapshot, err := collector.Collect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(snapshot.Partitions) != partitions {
		t.Fatalf("read %d partitions, want %d", len(snapshot.Partitions), partitions)
	}
	for _, partition := range snapshot.Partitions {
		// franz-go places keyed records with Kafka's murmur2, so this also
		// checks the hand-written hash against an independent implementation.
		// Agreement with librdkafka, which the outbox publisher uses, was
		// checked by hand against a real publisher: see the README.
		if partition.EndOffset != expected[partition.Partition] {
			t.Errorf("partition %d ends at %d, murmur2 predicts %d records", partition.Partition, partition.EndOffset, expected[partition.Partition])
		}
	}
	if len(snapshot.GroupLag) != 0 {
		t.Errorf("groups with no commits reported lag: %v", snapshot.GroupLag)
	}

	committed := kadm.Offsets{}
	committed.Add(kadm.Offset{Topic: topic, Partition: 0, At: min(2, expected[0])})
	committed.Add(kadm.Offset{Topic: topic, Partition: 1, At: expected[1]})
	_, err = kadm.NewClient(client).CommitOffsets(ctx, "partly-committed", committed)
	if err != nil {
		t.Fatal(err)
	}

	snapshot, err = collector.Collect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Partitions 2 and 3 have no commit, so their whole contents are lag.
	wantLag := (expected[0] - min(2, expected[0])) + expected[2] + expected[3]
	if snapshot.GroupLag["partly-committed"] != wantLag {
		t.Errorf("lag %d, want %d", snapshot.GroupLag["partly-committed"], wantLag)
	}
	if _, reported := snapshot.GroupLag["never-committed"]; reported {
		t.Error("a group that never committed was reported")
	}
	first := snapshot.Partitions[0].Groups["partly-committed"]
	if first.Committed != min(2, expected[0]) {
		t.Errorf("partition 0 committed %d, want %d", first.Committed, min(2, expected[0]))
	}
	if uncommitted := snapshot.Partitions[2].Groups["partly-committed"]; uncommitted.Committed != -1 {
		t.Errorf("partition 2 committed %d, want -1 for none", uncommitted.Committed)
	}
}

func TestKafkaCollectorReportsAMissingTopic(t *testing.T) {
	client := testinfra.NewKafka(t)

	_, err := NewKafkaCollector(kadm.NewClient(client), "loadgen-test-no-such-topic", nil).Collect(context.Background())

	if err == nil {
		t.Fatal("a missing topic was reported as an empty snapshot")
	}
}

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

func TestPostgresCollectorOnAnEmptyDatabase(t *testing.T) {
	pool := testinfra.NewDatabase(t, os.Getenv("TEST_DATABASE_URL"))

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
	pool := testinfra.NewDatabase(t, os.Getenv("TEST_DATABASE_URL"))
	exec(t, pool, `INSERT INTO users (username, password_hash, email) VALUES ('a', 'x', 'a@example.test'), ('b', 'x', 'b@example.test')`)
	exec(t, pool, `
		INSERT INTO auctions (owner_id, starts_at, ends_at, starting_price_minor, description, title, currency_code, minimum_bid_increment_minor)
		SELECT id, now() - interval '1 hour', now() + interval '1 hour', 1000, 'd', 't', 'USD', 100 FROM users WHERE username = 'a'`)
	exec(t, pool, `
		INSERT INTO bids (auction_id, bidder_id, amount_minor, idempotency_key, created_at)
		SELECT a.id, u.id, 1000 + n * 100, 'k' || n, now()
		FROM auctions a, users u, generate_series(1, 3) AS n WHERE u.username = 'b'`)

	// One unpublished event. The schema requires a version bump and its event
	// to commit together, so both go in one transaction.
	const eventID = "00000000-0000-4000-8000-000000000001"
	occurredAt := time.Now().Add(-3 * time.Second).UTC().Format(time.RFC3339Nano)
	transaction, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = transaction.Exec(ctx, `UPDATE auctions SET version = 2`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = transaction.Exec(ctx, `
		INSERT INTO outbox_events (event_id, event_type, occurred_at, auction_id, auction_version, payload)
		SELECT $1::uuid, 'BidAccepted.v1', $2::timestamptz, a.id, 2,
		  jsonb_build_object('eventId', $1::text, 'eventType', 'BidAccepted.v1', 'occurredAt', $2::text, 'auctionId', a.id::text, 'auctionVersion', '2')
		FROM auctions a`, eventID, occurredAt)
	if err != nil {
		t.Fatal(err)
	}
	err = transaction.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}

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

// A hot auction's signature: one transaction holds the row, another waits on it.
func TestPostgresCollectorSeesALockWaiterAndAnIdleTransaction(t *testing.T) {
	ctx := context.Background()
	pool := testinfra.NewDatabase(t, os.Getenv("TEST_DATABASE_URL"))
	exec(t, pool, `INSERT INTO users (username, password_hash, email) VALUES ('a', 'x', 'a@example.test')`)
	exec(t, pool, `
		INSERT INTO auctions (owner_id, starts_at, ends_at, starting_price_minor, description, title, currency_code, minimum_bid_increment_minor)
		SELECT id, now() - interval '1 hour', now() + interval '1 hour', 1000, 'd', 't', 'USD', 100 FROM users`)

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
	client := testinfra.NewKafka(t, os.Getenv("TEST_KAFKA_BROKERS"))
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
	wantLag := (expected[0] - min(2, expected[0])) + 0 + expected[2] + expected[3]
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
	client := testinfra.NewKafka(t, os.Getenv("TEST_KAFKA_BROKERS"))

	_, err := NewKafkaCollector(kadm.NewClient(client), "loadgen-test-no-such-topic", nil).Collect(context.Background())

	if err == nil {
		t.Fatal("a missing topic was reported as an empty snapshot")
	}
}

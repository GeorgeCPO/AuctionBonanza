package seed

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GeorgeCPO/AuctionBonanza/tools/loadgen/internal/murmur2"
)

type fakeUsers struct {
	err error
}

func (f fakeUsers) EnsureUsers(_ context.Context, count int) ([]User, error) {
	if f.err != nil {
		return nil, f.err
	}
	users := make([]User, count)
	for i := range users {
		users[i] = User{ID: strconv.Itoa(100 + i), Email: fmt.Sprintf("loadgen-%d@loadgen.invalid", i+1)}
	}
	return users, nil
}

type fakeTokens struct {
	mu    sync.Mutex
	calls []string
	ttls  []time.Duration
}

func (f *fakeTokens) Mint(userID string, _ time.Time, ttl time.Duration) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, userID)
	f.ttls = append(f.ttls, ttl)
	return "token-for-" + userID, nil
}

// fakeCreator hands out ids from a counter and lets a test script failures by
// call number.
type fakeCreator struct {
	next     atomic.Int64
	calls    atomic.Int64
	inFlight atomic.Int64
	peak     atomic.Int64
	failWith func(call int64, bearer string) error
	mu       sync.Mutex
	bodies   []CreateRequest
}

func (f *fakeCreator) Create(_ context.Context, bearer string, body CreateRequest) (CreatedAuction, error) {
	current := f.inFlight.Add(1)
	defer f.inFlight.Add(-1)
	for {
		peak := f.peak.Load()
		if current <= peak || f.peak.CompareAndSwap(peak, current) {
			break
		}
	}
	time.Sleep(time.Millisecond)

	call := f.calls.Add(1)
	if f.failWith != nil {
		err := f.failWith(call, bearer)
		if err != nil {
			return CreatedAuction{}, err
		}
	}

	f.mu.Lock()
	f.bodies = append(f.bodies, body)
	f.mu.Unlock()

	id := strconv.FormatInt(1000+f.next.Add(1), 10)
	return CreatedAuction{
		ID:                       id,
		OwnerID:                  "owner-of-" + id,
		CurrencyCode:             body.CurrencyCode,
		EndsAt:                   body.EndsAt,
		StartingPriceMinor:       body.StartingPriceMinor,
		MinimumBidIncrementMinor: body.MinimumBidIncrementMinor,
	}, nil
}

func testSpec(auctions int) Spec {
	return Spec{Users: 5, Auctions: auctions, AuctionDuration: 24 * time.Hour, CurrencyCode: "EUR"}
}

func testOptions() Options {
	return Options{
		Partitions:  12,
		Concurrency: 8,
		Now:         func() time.Time { return time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC) },
		Backoff:     func(int) time.Duration { return time.Millisecond },
	}
}

func TestRunCreatesEveryAuctionAndRecordsItsPartition(t *testing.T) {
	creator := &fakeCreator{}
	tokens := &fakeTokens{}
	seeder := NewSeeder(fakeUsers{}, creator, tokens, testOptions())

	manifest, err := seeder.Run(context.Background(), testSpec(240))
	if err != nil {
		t.Fatal(err)
	}

	if len(manifest.Auctions) != 240 || manifest.Failed != 0 {
		t.Fatalf("created %d auctions with %d failures, want 240 and 0", len(manifest.Auctions), manifest.Failed)
	}
	if len(manifest.Users) != 5 {
		t.Errorf("manifest lists %d users, want 5", len(manifest.Users))
	}

	total := 0
	for _, count := range manifest.PartitionCounts {
		total += count
	}
	if total != 240 || len(manifest.PartitionCounts) != 12 {
		t.Errorf("partition counts %v do not add up to the 240 created auctions", manifest.PartitionCounts)
	}
	for _, auction := range manifest.Auctions {
		want := murmur2.PartitionForKey(auction.ID, 12)
		if auction.Partition != want {
			t.Errorf("auction %s recorded partition %d, key hashes to %d", auction.ID, auction.Partition, want)
		}
	}
}

func TestRunMintsTokensForTheOwnerWithTheConfiguredTTL(t *testing.T) {
	tokens := &fakeTokens{}
	options := testOptions()
	options.TokenTTL = 9 * time.Minute
	seeder := NewSeeder(fakeUsers{}, &fakeCreator{}, tokens, options)

	_, err := seeder.Run(context.Background(), testSpec(10))
	if err != nil {
		t.Fatal(err)
	}

	if len(tokens.calls) != 10 {
		t.Fatalf("minted %d tokens for 10 auctions", len(tokens.calls))
	}
	owners := map[string]bool{}
	for _, owner := range tokens.calls {
		owners[owner] = true
	}
	if len(owners) != 5 {
		t.Errorf("tokens were minted for %d distinct users, want 5", len(owners))
	}
	for _, ttl := range tokens.ttls {
		if ttl != 9*time.Minute {
			t.Errorf("token ttl %s, want 9m", ttl)
		}
	}
}

func TestRunComputesTheEndFromTheClockAtSendTime(t *testing.T) {
	creator := &fakeCreator{}
	seeder := NewSeeder(fakeUsers{}, creator, &fakeTokens{}, testOptions())

	_, err := seeder.Run(context.Background(), testSpec(3))
	if err != nil {
		t.Fatal(err)
	}

	for _, body := range creator.bodies {
		if body.EndsAt != "2030-01-02T00:00:00Z" {
			t.Errorf("endsAt %q, want one day after the clock", body.EndsAt)
		}
	}
}

func TestRunStaysWithinTheConcurrencyLimit(t *testing.T) {
	creator := &fakeCreator{}
	options := testOptions()
	options.Concurrency = 4
	seeder := NewSeeder(fakeUsers{}, creator, &fakeTokens{}, options)

	_, err := seeder.Run(context.Background(), testSpec(100))
	if err != nil {
		t.Fatal(err)
	}

	if creator.peak.Load() > 4 {
		t.Errorf("%d requests were in flight, limit is 4", creator.peak.Load())
	}
	if creator.peak.Load() < 2 {
		t.Errorf("peak in flight was %d, so requests did not overlap", creator.peak.Load())
	}
}

func TestRunRetriesAnUnsentRequestAndNothingElse(t *testing.T) {
	creator := &fakeCreator{}
	var seen sync.Map
	creator.failWith = func(call int64, bearer string) error {
		// The first time any caller is seen, pretend the dial failed.
		_, repeated := seen.LoadOrStore(bearer, true)
		if !repeated && call%2 == 0 {
			return fmt.Errorf("%w: connection refused", ErrNotSent)
		}
		return nil
	}
	seeder := NewSeeder(fakeUsers{}, creator, &fakeTokens{}, testOptions())

	manifest, err := seeder.Run(context.Background(), testSpec(40))
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Failed != 0 || len(manifest.Auctions) != 40 {
		t.Fatalf("dial failures were not retried: %d created, %d failed", len(manifest.Auctions), manifest.Failed)
	}
	if creator.calls.Load() <= 40 {
		t.Fatalf("only %d requests were sent, so nothing was retried", creator.calls.Load())
	}
}

func TestRunDoesNotRetryAnUnknownOutcome(t *testing.T) {
	creator := &fakeCreator{}
	creator.failWith = func(call int64, _ string) error {
		if call == 1 {
			return errors.New("connection reset by peer")
		}
		return nil
	}
	options := testOptions()
	options.Concurrency = 1
	seeder := NewSeeder(fakeUsers{}, creator, &fakeTokens{}, options)

	manifest, err := seeder.Run(context.Background(), testSpec(5))
	if err != nil {
		t.Fatal(err)
	}

	if creator.calls.Load() != 5 {
		t.Errorf("%d requests were sent for 5 auctions; the lost one must not be sent again", creator.calls.Load())
	}
	if manifest.Failed != 1 || len(manifest.Auctions) != 4 {
		t.Errorf("expected 4 created and 1 failed, got %d and %d", len(manifest.Auctions), manifest.Failed)
	}
	if len(manifest.FailureSamples) != 1 {
		t.Errorf("failure samples %v", manifest.FailureSamples)
	}
}

func TestRunGivesUpAfterThreeUnsentAttempts(t *testing.T) {
	creator := &fakeCreator{}
	creator.failWith = func(int64, string) error { return fmt.Errorf("%w: refused", ErrNotSent) }
	options := testOptions()
	options.Concurrency = 1
	seeder := NewSeeder(fakeUsers{}, creator, &fakeTokens{}, options)

	manifest, err := seeder.Run(context.Background(), testSpec(2))
	if err != nil {
		t.Fatal(err)
	}

	if creator.calls.Load() != 6 {
		t.Errorf("%d attempts for 2 auctions, want 3 each", creator.calls.Load())
	}
	if manifest.Failed != 2 || len(manifest.Auctions) != 0 {
		t.Errorf("expected 2 failures and no auctions, got %d and %d", manifest.Failed, len(manifest.Auctions))
	}
}

func TestRunCapsRecordedFailureSamples(t *testing.T) {
	creator := &fakeCreator{}
	creator.failWith = func(int64, string) error { return errors.New("boom") }
	seeder := NewSeeder(fakeUsers{}, creator, &fakeTokens{}, testOptions())

	manifest, err := seeder.Run(context.Background(), testSpec(30))
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Failed != 30 || len(manifest.FailureSamples) != 5 {
		t.Errorf("failed %d with %d samples, want 30 and 5", manifest.Failed, len(manifest.FailureSamples))
	}
}

func TestRunStopsWhenTheContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	creator := &fakeCreator{}
	creator.failWith = func(call int64, _ string) error {
		if call == 10 {
			cancel()
		}
		return nil
	}
	options := testOptions()
	options.Concurrency = 1
	seeder := NewSeeder(fakeUsers{}, creator, &fakeTokens{}, options)

	manifest, err := seeder.Run(ctx, testSpec(1000))

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if len(manifest.Auctions) < 9 || len(manifest.Auctions) >= 1000 {
		t.Errorf("partial manifest has %d auctions, expected about 10", len(manifest.Auctions))
	}
}

func TestRunFailsWhenUsersCannotBeEnsured(t *testing.T) {
	seeder := NewSeeder(fakeUsers{err: errors.New("db down")}, &fakeCreator{}, &fakeTokens{}, testOptions())

	_, err := seeder.Run(context.Background(), testSpec(5))
	if err == nil {
		t.Fatal("expected the user error to stop the run")
	}
}

func TestRunRejectsAnInvalidSpecBeforeTouchingAnything(t *testing.T) {
	creator := &fakeCreator{}
	seeder := NewSeeder(fakeUsers{}, creator, &fakeTokens{}, testOptions())

	spec := testSpec(5)
	spec.Users = 1
	_, err := seeder.Run(context.Background(), spec)

	if err == nil || creator.calls.Load() != 0 {
		t.Fatalf("invalid spec ran: err=%v calls=%d", err, creator.calls.Load())
	}
}

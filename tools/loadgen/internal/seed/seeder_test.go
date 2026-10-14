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
		users[i] = User{ID: strconv.Itoa(100 + i), Email: fmt.Sprintf(userEmailFormat, i+1)}
	}
	return users, nil
}

type fakeTokens struct {
	mu    sync.Mutex
	calls []string
}

func (f *fakeTokens) Mint(userID string, _ time.Time) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, userID)
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

// accounted is every requested auction the manifest explains: created ones plus
// each failure counter. A run must always account for all it was asked to make.
func accounted(m Manifest) int {
	return len(m.Auctions) + m.Shortfall()
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

	if len(manifest.Auctions) != 240 || manifest.Shortfall() != 0 {
		t.Fatalf("created %d auctions with %d failures, want 240 and 0", len(manifest.Auctions), manifest.Shortfall())
	}
	if len(manifest.Users) != 5 {
		t.Errorf("manifest lists %d users, want 5", len(manifest.Users))
	}

	counts := manifest.PartitionCounts()
	total := 0
	for _, count := range counts {
		total += count
	}
	if total != 240 || len(counts) != 12 {
		t.Errorf("partition counts %v do not add up to the 240 created auctions", counts)
	}
	for _, auction := range manifest.Auctions {
		want := murmur2.PartitionForKey(auction.ID, 12)
		if auction.Partition != want {
			t.Errorf("auction %s recorded partition %d, key hashes to %d", auction.ID, auction.Partition, want)
		}
	}
}

func TestRunMintsTokensForTheOwners(t *testing.T) {
	tokens := &fakeTokens{}
	seeder := NewSeeder(fakeUsers{}, &fakeCreator{}, tokens, testOptions())

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

	peak := creator.peak.Load()
	if peak > 4 {
		t.Errorf("%d requests were in flight, limit is 4", peak)
	}
	if peak < 2 {
		t.Errorf("peak in flight was %d, so requests did not overlap", peak)
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
	if manifest.Shortfall() != 0 || len(manifest.Auctions) != 40 {
		t.Fatalf("dial failures were not retried: %d created, %d failed", len(manifest.Auctions), manifest.Shortfall())
	}
	sent := creator.calls.Load()
	if sent <= 40 {
		t.Fatalf("only %d requests were sent, so nothing was retried", sent)
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

	sent := creator.calls.Load()
	if sent != 5 {
		t.Errorf("%d requests were sent for 5 auctions; the lost one must not be sent again", sent)
	}
	if manifest.Unknown != 1 || manifest.Shortfall() != 1 || len(manifest.Auctions) != 4 {
		t.Errorf("expected 4 created and 1 unknown, got %d created, unknown %d, shortfall %d", len(manifest.Auctions), manifest.Unknown, manifest.Shortfall())
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

	attempts := creator.calls.Load()
	if attempts != 6 {
		t.Errorf("%d attempts for 2 auctions, want 3 each", attempts)
	}
	if manifest.NotSent != 2 || manifest.Shortfall() != 2 || len(manifest.Auctions) != 0 {
		t.Errorf("expected 2 not-sent and no auctions, got not sent %d, shortfall %d, created %d", manifest.NotSent, manifest.Shortfall(), len(manifest.Auctions))
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
	if manifest.Unknown != 30 || len(manifest.FailureSamples) != 5 {
		t.Errorf("unknown %d with %d samples, want 30 and 5", manifest.Unknown, len(manifest.FailureSamples))
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

	var interrupted *InterruptedError
	if !errors.As(err, &interrupted) || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected an InterruptedError caused by context.Canceled, got %v", err)
	}
	if len(manifest.Auctions) < 9 || len(manifest.Auctions) >= 1000 {
		t.Errorf("partial manifest has %d auctions, expected about 10", len(manifest.Auctions))
	}
	if accounted(manifest) != 1000 {
		t.Errorf("manifest accounts for %d of 1000 requested auctions", accounted(manifest))
	}
	if manifest.Rejected != 0 || manifest.Unknown != 0 {
		t.Errorf("stopping mid-run reported %d rejected and %d unknown; requests that were never sent are not failures of either kind", manifest.Rejected, manifest.Unknown)
	}
	if manifest.NotSent < 900 {
		t.Errorf("only %d auctions counted as not sent after stopping at about 10", manifest.NotSent)
	}
}

func TestRunReturnsNoManifestForSetupErrors(t *testing.T) {
	seeder := NewSeeder(fakeUsers{err: errors.New("db down")}, &fakeCreator{}, &fakeTokens{}, testOptions())

	manifest, err := seeder.Run(context.Background(), testSpec(5))

	var interrupted *InterruptedError
	if err == nil || errors.As(err, &interrupted) {
		t.Fatalf("a setup failure must be a plain error, got %v", err)
	}
	if len(manifest.Users) != 0 || len(manifest.Auctions) != 0 {
		t.Errorf("setup failure returned a manifest: %+v", manifest)
	}
}

func TestRunRejectsMissingRequiredOptions(t *testing.T) {
	cases := map[string]func(*Options){
		"no partitions":  func(o *Options) { o.Partitions = 0 },
		"no concurrency": func(o *Options) { o.Concurrency = 0 },
	}

	for name, mutate := range cases {
		options := testOptions()
		mutate(&options)
		creator := &fakeCreator{}
		seeder := NewSeeder(fakeUsers{}, creator, &fakeTokens{}, options)

		_, err := seeder.Run(context.Background(), testSpec(5))

		if err == nil || creator.calls.Load() != 0 {
			t.Errorf("%s: ran anyway (err %v, %d requests); zero workers would hang the feed", name, err, creator.calls.Load())
		}
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

func TestRunClassifiesServerAnswersByWhetherTheAuctionCanExist(t *testing.T) {
	cases := map[string]struct {
		err                             error
		rejected, notSent, unknownCount int
	}{
		"bad request":        {&StatusError{StatusCode: 400}, 1, 0, 0},
		"unauthorized":       {&StatusError{StatusCode: 401}, 1, 0, 0},
		"conflict":           {&StatusError{StatusCode: 409}, 1, 0, 0},
		"server error":       {&StatusError{StatusCode: 500}, 0, 0, 1},
		"bad gateway":        {&StatusError{StatusCode: 502}, 0, 0, 1},
		"lost response":      {errors.New("read: connection reset by peer"), 0, 0, 1},
		"refused connection": {fmt.Errorf("%w: refused", ErrNotSent), 0, 1, 0},
	}

	for name, tc := range cases {
		creator := &fakeCreator{failWith: func(int64, string) error { return tc.err }}
		options := testOptions()
		options.Concurrency = 1
		seeder := NewSeeder(fakeUsers{}, creator, &fakeTokens{}, options)

		manifest, err := seeder.Run(context.Background(), testSpec(1))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}

		if manifest.Rejected != tc.rejected || manifest.NotSent != tc.notSent || manifest.Unknown != tc.unknownCount {
			t.Errorf("%s: rejected %d, not sent %d, unknown %d; want %d, %d, %d",
				name, manifest.Rejected, manifest.NotSent, manifest.Unknown, tc.rejected, tc.notSent, tc.unknownCount)
		}
	}
}

func TestRunAbortsAfterTooManyFailuresInARow(t *testing.T) {
	creator := &fakeCreator{failWith: func(int64, string) error { return &StatusError{StatusCode: 401, Body: "wrong audience"} }}
	options := testOptions()
	options.AbortAfter = 5
	seeder := NewSeeder(fakeUsers{}, creator, &fakeTokens{}, options)

	manifest, err := seeder.Run(context.Background(), testSpec(1000))

	var aborted *AbortedError
	if !errors.As(err, &aborted) || aborted.Consecutive < 5 {
		t.Fatalf("expected an AbortedError after at least 5 failures, got %v", err)
	}
	var status *StatusError
	if !errors.As(err, &status) || status.StatusCode != 401 {
		t.Errorf("the abort does not carry the failure that caused it: %v", err)
	}
	// Requests already in flight, and results waiting in the channel, still count.
	if sent, limit := creator.calls.Load(), int64(5+2*options.Concurrency); sent > limit {
		t.Errorf("%d requests were sent, want at most %d: the run should stop soon after the 5th failure", sent, limit)
	}
	if accounted(manifest) != 1000 {
		t.Errorf("manifest accounts for %d of 1000 requested auctions", accounted(manifest))
	}
	if manifest.NotSent < 900 {
		t.Errorf("%d auctions counted as not sent, want nearly all of the unattempted ones", manifest.NotSent)
	}
}

func TestRunDoesNotAbortWhileSuccessesKeepInterrupting(t *testing.T) {
	creator := &fakeCreator{failWith: func(call int64, _ string) error {
		if call%3 != 0 {
			return &StatusError{StatusCode: 422}
		}
		return nil
	}}
	options := testOptions()
	options.Concurrency = 1
	options.AbortAfter = 3
	seeder := NewSeeder(fakeUsers{}, creator, &fakeTokens{}, options)

	manifest, err := seeder.Run(context.Background(), testSpec(30))

	if err != nil {
		t.Fatalf("two failures then a success is not a streak of 3, got %v", err)
	}
	if len(manifest.Auctions) != 10 || manifest.Rejected != 20 {
		t.Errorf("created %d, rejected %d, want 10 and 20", len(manifest.Auctions), manifest.Rejected)
	}
}

func TestRunNeverAbortsWhenAbortAfterIsZero(t *testing.T) {
	creator := &fakeCreator{failWith: func(int64, string) error { return &StatusError{StatusCode: 400} }}
	seeder := NewSeeder(fakeUsers{}, creator, &fakeTokens{}, testOptions())

	manifest, err := seeder.Run(context.Background(), testSpec(50))

	if err != nil || manifest.Rejected != 50 {
		t.Fatalf("err %v, rejected %d, want every request attempted", err, manifest.Rejected)
	}
}

func TestRunIsNotInterruptedWhenTheContextEndsAfterTheWorkIsDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	creator := &fakeCreator{failWith: func(call int64, _ string) error {
		if call == 5 {
			cancel()
		}
		return nil
	}}
	options := testOptions()
	options.Concurrency = 1
	seeder := NewSeeder(fakeUsers{}, creator, &fakeTokens{}, options)

	// Cancelling during the last request leaves nothing unfinished.
	manifest, err := seeder.Run(ctx, testSpec(5))

	if err != nil || len(manifest.Auctions) != 5 {
		t.Fatalf("a run that finished everything reported %v with %d auctions", err, len(manifest.Auctions))
	}
}

// patientCreator answers after a short wait and gives up only if its context
// ends, like a real HTTP request.
type patientCreator struct {
	calls atomic.Int64
	err   error
	// onCall, if set, runs when a request arrives, with how many have arrived.
	onCall func(call int64)
}

func (c *patientCreator) Create(ctx context.Context, _ string, _ CreateRequest) (CreatedAuction, error) {
	call := c.calls.Add(1)
	if c.onCall != nil {
		c.onCall(call)
	}
	select {
	case <-time.After(5 * time.Millisecond):
		return CreatedAuction{}, c.err
	case <-ctx.Done():
		return CreatedAuction{}, ctx.Err()
	}
}

func TestRunLetsRequestsInFlightFinishWhenItAbortsItself(t *testing.T) {
	creator := &patientCreator{err: &StatusError{StatusCode: 401}}
	options := testOptions()
	options.AbortAfter = 4
	seeder := NewSeeder(fakeUsers{}, creator, &fakeTokens{}, options)

	manifest, err := seeder.Run(context.Background(), testSpec(1000))

	var aborted *AbortedError
	if !errors.As(err, &aborted) {
		t.Fatalf("expected an abort, got %v", err)
	}
	if manifest.Unknown != 0 {
		t.Errorf("%d outcomes are unknown: aborting must not cancel requests that were already sent", manifest.Unknown)
	}
	if accounted(manifest) != 1000 {
		t.Errorf("manifest accounts for %d of 1000 requested auctions", accounted(manifest))
	}
}

func TestRunCutsRequestsInFlightShortWhenTheCallerStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	// Stop once the fourth request has arrived: all four workers are then inside
	// a request that waits 5 ms unless its context ends first.
	creator := &patientCreator{err: errors.New("unused"), onCall: func(call int64) {
		if call == 4 {
			cancel()
		}
	}}
	options := testOptions()
	options.Concurrency = 4
	seeder := NewSeeder(fakeUsers{}, creator, &fakeTokens{}, options)

	manifest, err := seeder.Run(ctx, testSpec(1000))

	var interrupted *InterruptedError
	if !errors.As(err, &interrupted) {
		t.Fatalf("expected an interruption, got %v", err)
	}
	if manifest.Unknown == 0 {
		t.Error("requests cancelled mid-flight may have been processed, so they are unknown")
	}
	if accounted(manifest) != 1000 {
		t.Errorf("manifest accounts for %d of 1000 requested auctions", accounted(manifest))
	}
}

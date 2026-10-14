package seed

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/GeorgeCPO/AuctionBonanza/tools/loadgen/internal/murmur2"
)

const (
	maxSendAttempts = 3
	maxFailureNotes = 5
	progressEvery   = 1000
)

// UserEnsurer provides the seeded users.
type UserEnsurer interface {
	EnsureUsers(ctx context.Context, count int) ([]User, error)
}

// AuctionCreator creates one auction on behalf of the token's user.
type AuctionCreator interface {
	Create(ctx context.Context, bearer string, body CreateRequest) (CreatedAuction, error)
}

// TokenSource mints an access token for a user.
type TokenSource interface {
	Mint(userID string, now time.Time) (string, error)
}

// Options tune a Seeder. Partitions and Concurrency are required; the rest have
// defaults noted per field.
type Options struct {
	// Partitions of auction.events, used to compute each auction's partition.
	Partitions int
	// Concurrency is the number of requests in flight.
	Concurrency int
	// Now is the clock. Default time.Now.
	Now func() time.Time
	// Backoff is how long to wait before send attempt n+1. Default 200 ms times n.
	Backoff func(attempt int) time.Duration
	Logger  *slog.Logger
}

// InterruptedError says a run stopped early. The manifest returned with it is
// valid and lists what was created before the stop; any other error returns no
// manifest.
type InterruptedError struct {
	Cause error
}

func (e *InterruptedError) Error() string {
	return fmt.Sprintf("seeding interrupted: %v", e.Cause)
}

func (e *InterruptedError) Unwrap() error {
	return e.Cause
}

// Seeder carries out a plan against a running stack.
type Seeder struct {
	users   UserEnsurer
	creator AuctionCreator
	tokens  TokenSource
	options Options
}

func NewSeeder(users UserEnsurer, creator AuctionCreator, tokens TokenSource, options Options) *Seeder {
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Backoff == nil {
		options.Backoff = func(attempt int) time.Duration {
			return time.Duration(attempt) * 200 * time.Millisecond
		}
	}
	if options.Logger == nil {
		options.Logger = slog.New(slog.DiscardHandler)
	}
	return &Seeder{users: users, creator: creator, tokens: tokens, options: options}
}

type outcome struct {
	index   int
	auction CreatedAuction
	err     error
}

// Run creates the users and auctions the spec asks for. Results are collected
// as they arrive, so memory holds the plan and the manifest and nothing else.
func (s *Seeder) Run(ctx context.Context, spec Spec) (Manifest, error) {
	if s.options.Partitions <= 0 {
		return Manifest{}, errors.New("partition count must be positive")
	}
	if s.options.Concurrency <= 0 {
		return Manifest{}, errors.New("concurrency must be positive")
	}
	requests, err := BuildPlan(spec)
	if err != nil {
		return Manifest{}, err
	}
	users, err := s.users.EnsureUsers(ctx, spec.Users)
	if err != nil {
		return Manifest{}, err
	}

	manifest := Manifest{
		CreatedAt:  s.options.Now().UTC(),
		Partitions: s.options.Partitions,
		Users:      users,
		Auctions:   make([]ManifestAuction, 0, len(requests)),
	}

	jobs := make(chan int)
	outcomes := make(chan outcome, s.options.Concurrency)
	var workers sync.WaitGroup
	for range s.options.Concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				result := s.create(ctx, users[requests[index].OwnerIndex], requests[index])
				result.index = index
				outcomes <- result
			}
		}()
	}
	go func() {
		defer close(jobs)
		for index := range requests {
			select {
			case jobs <- index:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		workers.Wait()
		close(outcomes)
	}()

	finished := 0
	for result := range outcomes {
		s.record(&manifest, result)
		finished++
		if finished%progressEvery == 0 {
			s.options.Logger.Info("seeding", "finished", finished, "of", len(requests))
		}
	}

	stopped := ctx.Err()
	if stopped != nil {
		return manifest, &InterruptedError{Cause: stopped}
	}
	return manifest, nil
}

// create sends one auction. Only a failure to reach the server is retried:
// after any other failure the auction may exist, and creating it twice would
// silently skew the data.
func (s *Seeder) create(ctx context.Context, owner User, request AuctionRequest) outcome {
	for attempt := 1; ; attempt++ {
		now := s.options.Now()
		bearer, err := s.tokens.Mint(owner.ID, now)
		if err != nil {
			return outcome{err: err}
		}

		created, err := s.creator.Create(ctx, bearer, CreateRequest{
			Title:                    request.Title,
			Description:              request.Description,
			CurrencyCode:             request.CurrencyCode,
			EndsAt:                   now.Add(request.Duration).UTC().Format(time.RFC3339Nano),
			StartingPriceMinor:       request.StartingPriceMinor,
			MinimumBidIncrementMinor: request.MinimumBidIncrementMinor,
		})
		if err == nil {
			return outcome{auction: created}
		}

		retryable := errors.Is(err, ErrNotSent) && attempt < maxSendAttempts
		if !retryable {
			return outcome{err: err}
		}
		waitErr := wait(ctx, s.options.Backoff(attempt))
		if waitErr != nil {
			return outcome{err: waitErr}
		}
	}
}

func (s *Seeder) record(manifest *Manifest, result outcome) {
	if result.err != nil {
		manifest.Failed++
		if len(manifest.FailureSamples) < maxFailureNotes {
			manifest.FailureSamples = append(manifest.FailureSamples, fmt.Sprintf("auction %d: %v", result.index+1, result.err))
		}
		return
	}

	manifest.Auctions = append(manifest.Auctions, ManifestAuction{
		CreatedAuction: result.auction,
		Partition:      murmur2.PartitionForKey(result.auction.ID, s.options.Partitions),
	})
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

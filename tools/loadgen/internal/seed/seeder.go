package seed

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
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
	Mint(userID string, now time.Time, ttl time.Duration) (string, error)
}

// Options tune a Seeder. Zero values take the defaults noted per field.
type Options struct {
	// Partitions of auction.events, used to compute each auction's partition.
	Partitions int
	// Concurrency is the number of requests in flight. Default 16.
	Concurrency int
	// TokenTTL must not exceed auction-service's ACCESS_TOKEN_TTL_SECONDS. Default 5 minutes.
	TokenTTL time.Duration
	// Now is the clock. Default time.Now.
	Now func() time.Time
	// Backoff is how long to wait before send attempt n+1. Default 200 ms times n.
	Backoff func(attempt int) time.Duration
	Logger  *slog.Logger
}

// Seeder carries out a Plan against a running stack.
type Seeder struct {
	users   UserEnsurer
	creator AuctionCreator
	tokens  TokenSource
	options Options
}

func NewSeeder(users UserEnsurer, creator AuctionCreator, tokens TokenSource, options Options) *Seeder {
	if options.Concurrency <= 0 {
		options.Concurrency = 16
	}
	if options.TokenTTL <= 0 {
		options.TokenTTL = 5 * time.Minute
	}
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
	auction CreatedAuction
	err     error
}

// Run creates the users and auctions the spec asks for. When the context ends
// early it returns the manifest of what was created together with the error.
func (s *Seeder) Run(ctx context.Context, spec Spec) (Manifest, error) {
	if s.options.Partitions <= 0 {
		return Manifest{}, errors.New("partition count must be positive")
	}
	plan, err := BuildPlan(spec)
	if err != nil {
		return Manifest{}, err
	}
	users, err := s.users.EnsureUsers(ctx, spec.Users)
	if err != nil {
		return Manifest{}, err
	}

	outcomes := make([]outcome, len(plan.Auctions))
	completed := make([]bool, len(plan.Auctions))
	jobs := make(chan int)
	var finished atomic.Int64
	var workers sync.WaitGroup

	for range s.options.Concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				outcomes[index] = s.create(ctx, users[plan.Auctions[index].OwnerIndex], plan.Auctions[index])
				completed[index] = true
				done := finished.Add(1)
				if done%progressEvery == 0 {
					s.options.Logger.Info("seeding", "created", done, "of", len(plan.Auctions))
				}
			}
		}()
	}

feed:
	for index := range plan.Auctions {
		select {
		case jobs <- index:
		case <-ctx.Done():
			break feed
		}
	}
	close(jobs)
	workers.Wait()

	manifest := s.buildManifest(users, outcomes, completed)
	return manifest, ctx.Err()
}

// create sends one auction. Only a failure to reach the server is retried:
// after any other failure the auction may exist, and creating it twice would
// silently skew the data.
func (s *Seeder) create(ctx context.Context, owner User, request AuctionRequest) outcome {
	for attempt := 1; ; attempt++ {
		now := s.options.Now()
		bearer, err := s.tokens.Mint(owner.ID, now, s.options.TokenTTL)
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
		waitErr := s.wait(ctx, s.options.Backoff(attempt))
		if waitErr != nil {
			return outcome{err: waitErr}
		}
	}
}

func (s *Seeder) wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Seeder) buildManifest(users []User, outcomes []outcome, completed []bool) Manifest {
	manifest := Manifest{
		CreatedAt:       s.options.Now().UTC(),
		Partitions:      s.options.Partitions,
		Users:           users,
		Auctions:        make([]ManifestAuction, 0, len(outcomes)),
		PartitionCounts: make([]int, s.options.Partitions),
	}

	for index, result := range outcomes {
		if !completed[index] {
			continue
		}
		if result.err != nil {
			manifest.Failed++
			if len(manifest.FailureSamples) < maxFailureNotes {
				manifest.FailureSamples = append(manifest.FailureSamples, fmt.Sprintf("auction %d: %v", index+1, result.err))
			}
			continue
		}

		partition := murmur2.PartitionForKey(result.auction.ID, s.options.Partitions)
		manifest.PartitionCounts[partition]++
		manifest.Auctions = append(manifest.Auctions, ManifestAuction{
			ID:                       result.auction.ID,
			OwnerID:                  result.auction.OwnerID,
			CurrencyCode:             result.auction.CurrencyCode,
			EndsAt:                   result.auction.EndsAt,
			StartingPriceMinor:       result.auction.StartingPriceMinor,
			MinimumBidIncrementMinor: result.auction.MinimumBidIncrementMinor,
			Partition:                partition,
		})
	}
	return manifest
}

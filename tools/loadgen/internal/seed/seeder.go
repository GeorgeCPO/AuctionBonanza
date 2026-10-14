package seed

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
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
	// AbortAfter stops the run once this many results in a row are failures, so a
	// wrong audience or a server that is down does not grind through every
	// request. Zero never aborts.
	AbortAfter int
	// Now is the clock. Default time.Now.
	Now func() time.Time
	// Backoff is how long to wait before send attempt n+1. Default 200 ms times n.
	Backoff func(attempt int) time.Duration
	Logger  *slog.Logger
}

// InterruptedError says a run was stopped from outside. The manifest returned
// with it is valid and accounts for every requested auction; any other error
// returns no manifest.
type InterruptedError struct {
	Cause error
}

func (e *InterruptedError) Error() string {
	return fmt.Sprintf("seeding interrupted: %v", e.Cause)
}

func (e *InterruptedError) Unwrap() error {
	return e.Cause
}

// AbortedError says the run stopped itself after too many failures in a row. As
// with InterruptedError, the manifest returned with it is valid.
type AbortedError struct {
	Consecutive int
	Last        error
}

func (e *AbortedError) Error() string {
	return fmt.Sprintf("seeding aborted after %d failures in a row; last: %v", e.Consecutive, e.Last)
}

func (e *AbortedError) Unwrap() error {
	return e.Last
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

type outcomeKind int

const (
	outcomeCreated outcomeKind = iota
	// outcomeRejected: the server refused the request, so no auction exists.
	outcomeRejected
	// outcomeNotSent: the request never reached the server.
	outcomeNotSent
	// outcomeUnknown: the request may have been processed.
	outcomeUnknown
)

func (k outcomeKind) String() string {
	switch k {
	case outcomeRejected:
		return "rejected"
	case outcomeNotSent:
		return "not sent"
	case outcomeUnknown:
		return "unknown"
	default:
		return "created"
	}
}

type outcome struct {
	index   int
	kind    outcomeKind
	auction CreatedAuction
	err     error
}

// classify says whether a failed request can have created an auction. Only a
// 4xx answer or a failure to reach the server rule that out: a 5xx, a timeout or
// a reset connection leave it open.
func classify(err error) outcomeKind {
	if errors.Is(err, ErrNotSent) {
		return outcomeNotSent
	}
	var status *StatusError
	if errors.As(err, &status) && status.StatusCode >= http.StatusBadRequest && status.StatusCode < http.StatusInternalServerError {
		return outcomeRejected
	}
	return outcomeUnknown
}

// Run creates the users and auctions the spec asks for. Results are collected
// as they arrive, so memory holds the plan and the manifest and nothing else.
// Every requested auction ends up either in Auctions or in exactly one of the
// manifest's failure counters.
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

	// dispatchCtx ends when the caller stops the run or the run aborts itself. It
	// only stops new requests from being handed out. Requests already in flight
	// run under ctx, so an abort lets them finish with a real answer instead of
	// turning them into outcomes nobody can tell; only the caller's own
	// cancellation cuts them short.
	dispatchCtx, stopDispatching := context.WithCancel(ctx)
	defer stopDispatching()

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
			case <-dispatchCtx.Done():
				return
			}
		}
	}()
	go func() {
		workers.Wait()
		close(outcomes)
	}()

	finished := 0
	consecutive := 0
	var aborted *AbortedError
	for result := range outcomes {
		s.record(&manifest, result)
		finished++
		if result.kind == outcomeCreated {
			consecutive = 0
		} else {
			consecutive++
		}
		if aborted == nil && s.options.AbortAfter > 0 && consecutive >= s.options.AbortAfter {
			aborted = &AbortedError{Consecutive: consecutive, Last: result.err}
			stopDispatching()
		}
		if finished%progressEvery == 0 {
			s.options.Logger.Info("seeding", "finished", finished, "of", len(requests))
		}
	}
	// Requests never handed to a worker were never sent.
	manifest.NotSent += len(requests) - finished

	if aborted != nil {
		return manifest, aborted
	}
	stopped := ctx.Err()
	if stopped != nil && manifest.Shortfall() > 0 {
		return manifest, &InterruptedError{Cause: stopped}
	}
	return manifest, nil
}

// create sends one auction. Only a failure to reach the server is retried:
// after any other failure the auction may exist, and creating it twice would
// silently skew the data.
func (s *Seeder) create(ctx context.Context, owner User, request AuctionRequest) outcome {
	for attempt := 1; ; attempt++ {
		stopped := ctx.Err()
		if stopped != nil {
			return outcome{kind: outcomeNotSent, err: stopped}
		}

		now := s.options.Now()
		bearer, err := s.tokens.Mint(owner.ID, now)
		if err != nil {
			return outcome{kind: outcomeNotSent, err: err}
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
			return outcome{kind: outcomeCreated, auction: created}
		}

		retryable := errors.Is(err, ErrNotSent) && attempt < maxSendAttempts
		if !retryable {
			return outcome{kind: classify(err), err: err}
		}
		waitErr := wait(ctx, s.options.Backoff(attempt))
		if waitErr != nil {
			return outcome{kind: outcomeNotSent, err: waitErr}
		}
	}
}

func (s *Seeder) record(manifest *Manifest, result outcome) {
	switch result.kind {
	case outcomeCreated:
		manifest.Auctions = append(manifest.Auctions, ManifestAuction{
			CreatedAuction: result.auction,
			Partition:      murmur2.PartitionForKey(result.auction.ID, s.options.Partitions),
		})
		return
	case outcomeRejected:
		manifest.Rejected++
	case outcomeNotSent:
		manifest.NotSent++
	default:
		manifest.Unknown++
	}

	if len(manifest.FailureSamples) < maxFailureNotes {
		manifest.FailureSamples = append(manifest.FailureSamples,
			fmt.Sprintf("auction %d (%s): %v", result.index+1, result.kind, result.err))
	}
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

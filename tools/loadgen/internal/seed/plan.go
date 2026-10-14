// Package seed puts users and auctions into a running stack and records what
// it created in a manifest that later load stages consume.
package seed

import (
	"errors"
	"fmt"
	"time"
)

const (
	minDuration = 2 * time.Minute
	// The API caps an auction at 30 days; the margin absorbs the time between
	// computing the end and the server receiving the request.
	maxDuration = 29 * 24 * time.Hour
)

// Spec says how much to seed.
type Spec struct {
	Users           int
	Auctions        int
	AuctionDuration time.Duration
	CurrencyCode    string
}

// AuctionRequest is one auction to create. It carries a duration, not an end
// time, because the end is computed when the request is sent: a long seeding
// run must not leave later auctions with an end that has nearly passed.
type AuctionRequest struct {
	OwnerIndex               int
	Title                    string
	Description              string
	CurrencyCode             string
	Duration                 time.Duration
	StartingPriceMinor       string
	MinimumBidIncrementMinor string
}

// Plan lists everything a run will create. It is data: BuildPlan decides, the
// Seeder performs it.
type Plan struct {
	Auctions []AuctionRequest
}

// BuildPlan expands a spec into concrete auction requests. Prices follow the
// index so two runs of one spec produce the same plan. Owners rotate through
// the users so every user owns auctions and, with at least two users, has
// auctions it may bid on.
func BuildPlan(spec Spec) (Plan, error) {
	if spec.Users < 2 {
		return Plan{}, errors.New("at least 2 users are needed so bidders differ from owners")
	}
	if spec.Auctions < 0 {
		return Plan{}, errors.New("auction count cannot be negative")
	}
	if spec.AuctionDuration < minDuration || spec.AuctionDuration > maxDuration {
		return Plan{}, fmt.Errorf("auction duration must be between %s and %s", minDuration, maxDuration)
	}
	if len(spec.CurrencyCode) != 3 {
		return Plan{}, errors.New("currency code must have 3 letters")
	}

	auctions := make([]AuctionRequest, spec.Auctions)
	for i := range auctions {
		auctions[i] = AuctionRequest{
			OwnerIndex:               i % spec.Users,
			Title:                    fmt.Sprintf("Load auction %d", i+1),
			Description:              "Created by the loadgen seeder.",
			CurrencyCode:             spec.CurrencyCode,
			Duration:                 spec.AuctionDuration,
			StartingPriceMinor:       fmt.Sprintf("%d", 10_000+(i%90)*100),
			MinimumBidIncrementMinor: "100",
		}
	}
	return Plan{Auctions: auctions}, nil
}

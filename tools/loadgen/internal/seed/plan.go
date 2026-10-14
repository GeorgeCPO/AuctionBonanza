// Package seed puts users and auctions into a running stack and records what
// it created in a manifest that later load stages consume.
package seed

import (
	"errors"
	"fmt"
	"time"
)

const (
	// MinDuration and MaxDuration bound an auction's length. The API accepts 1
	// minute to 30 days; the margins absorb the time between computing the end
	// and the server receiving the request.
	MinDuration = 2 * time.Minute
	MaxDuration = 29 * 24 * time.Hour
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

// BuildPlan expands a spec into the auction requests a run will send. It is
// data: BuildPlan decides, the Seeder performs it. Prices follow the index so
// two runs of one spec produce the same plan. Owners rotate through the users so
// every user owns auctions and, with at least two users, has auctions it may
// bid on.
func BuildPlan(spec Spec) ([]AuctionRequest, error) {
	if spec.Users < 2 {
		return nil, errors.New("at least 2 users are needed so bidders differ from owners")
	}
	if spec.Auctions < 0 {
		return nil, errors.New("auction count cannot be negative")
	}
	if spec.AuctionDuration < MinDuration || spec.AuctionDuration > MaxDuration {
		return nil, fmt.Errorf("auction duration must be between %s and %s", MinDuration, MaxDuration)
	}
	if len(spec.CurrencyCode) != 3 {
		return nil, errors.New("currency code must have 3 letters")
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
	return auctions, nil
}

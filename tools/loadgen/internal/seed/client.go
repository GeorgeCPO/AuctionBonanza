package seed

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
)

// ErrNotSent marks a failure that happened before the request reached the
// server, so the auction was certainly not created and sending it again is
// safe. Every other failure leaves the outcome unknown, and creation has no
// idempotency key, so those are never retried.
var ErrNotSent = errors.New("request was not sent")

// CreateRequest is the body of POST /v1/auctions.
type CreateRequest struct {
	Title                    string `json:"title"`
	Description              string `json:"description"`
	CurrencyCode             string `json:"currencyCode"`
	EndsAt                   string `json:"endsAt"`
	StartingPriceMinor       string `json:"startingPriceMinor"`
	MinimumBidIncrementMinor string `json:"minimumBidIncrementMinor"`
}

// CreatedAuction is the part of the 201 response the manifest keeps.
type CreatedAuction struct {
	ID                       string `json:"id"`
	OwnerID                  string `json:"ownerId"`
	CurrencyCode             string `json:"currencyCode"`
	EndsAt                   string `json:"endsAt"`
	StartingPriceMinor       string `json:"startingPriceMinor"`
	MinimumBidIncrementMinor string `json:"minimumBidIncrementMinor"`
}

// StatusError is a response other than 201.
type StatusError struct {
	StatusCode int
	Body       string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("auction-service answered %d: %s", e.StatusCode, e.Body)
}

// AuctionClient talks to auction-service over HTTP.
type AuctionClient struct {
	baseURL string
	http    *http.Client
}

func NewAuctionClient(baseURL string, client *http.Client) *AuctionClient {
	return &AuctionClient{baseURL: strings.TrimRight(baseURL, "/"), http: client}
}

func (c *AuctionClient) Create(ctx context.Context, bearer string, body CreateRequest) (CreatedAuction, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return CreatedAuction{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/auctions", bytes.NewReader(payload))
	if err != nil {
		return CreatedAuction{}, err
	}
	request.Header.Set("Authorization", "Bearer "+bearer)
	request.Header.Set("Content-Type", "application/json")

	response, err := c.http.Do(request)
	if err != nil {
		if isDialFailure(err) {
			return CreatedAuction{}, fmt.Errorf("%w: %v", ErrNotSent, err)
		}
		return CreatedAuction{}, err
	}
	defer response.Body.Close()

	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return CreatedAuction{}, err
	}
	if response.StatusCode != http.StatusCreated {
		return CreatedAuction{}, &StatusError{StatusCode: response.StatusCode, Body: strings.TrimSpace(string(responseBody))}
	}

	var created CreatedAuction
	err = json.Unmarshal(responseBody, &created)
	if err != nil {
		return CreatedAuction{}, fmt.Errorf("decode created auction: %w", err)
	}
	if created.ID == "" {
		return CreatedAuction{}, errors.New("created auction has no id")
	}
	return created, nil
}

func isDialFailure(err error) bool {
	var operation *net.OpError
	return errors.As(err, &operation) && operation.Op == "dial"
}

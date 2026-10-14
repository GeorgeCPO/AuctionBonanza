package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GeorgeCPO/AuctionBonanza/tools/loadgen/internal/seed"
	"github.com/GeorgeCPO/AuctionBonanza/tools/loadgen/internal/token"
)

func runSeed(ctx context.Context, args []string) int {
	flags := flag.NewFlagSet("seed", flag.ContinueOnError)
	users := flags.Int("users", 100, "number of seeded users (at least 2)")
	auctions := flags.Int("auctions", 1000, "number of auctions to create")
	concurrency := flags.Int("concurrency", 16, "requests in flight")
	duration := flags.Duration("duration", 7*24*time.Hour, "how long each auction stays open (2m to 29d)")
	currency := flags.String("currency", "USD", "auction currency")
	partitions := flags.Int("partitions", 12, "partitions of auction.events, for the manifest")
	out := flags.String("out", "tools/loadgen/out/manifest.json", "manifest path")
	databaseURL := flags.String("database-url", environmentOr("DATABASE_URL", "postgresql://auction:auction_local@localhost:5432/auction_db"), "PostgreSQL URL (DATABASE_URL)")
	apiURL := flags.String("api-url", environmentOr("AUCTION_API_URL", "http://localhost:3002"), "auction-service URL (AUCTION_API_URL)")
	keyPath := flags.String("key", environmentOr("AUTH_SIGNING_PRIVATE_KEY_PATH", "services/identity-service/.local-keys/private-key.pem"), "ES256 private key (AUTH_SIGNING_PRIVATE_KEY_PATH)")
	jwksPath := flags.String("jwks", environmentOr("AUTH_JWKS_PATH", "services/identity-service/.local-keys/jwks.json"), "JWKS holding the key id (AUTH_JWKS_PATH)")
	issuer := flags.String("issuer", environmentOr("AUTH_ISSUER", "http://localhost:3001"), "token issuer (AUTH_ISSUER)")
	audience := flags.String("audience", environmentOr("AUTH_AUDIENCE", "auction-api"), "token audience (AUTH_AUDIENCE)")
	tokenTTL := flags.Duration("token-ttl", 5*time.Minute, "token lifetime; at most auction-service's ACCESS_TOKEN_TTL_SECONDS")
	err := flags.Parse(args)
	if err != nil {
		return 2
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	minter, err := token.Load(*keyPath, *jwksPath, *issuer, *audience)
	if err != nil {
		logger.Error("load signing key (run `pnpm auth:keys:local` first)", "error", err)
		return 1
	}
	pool, err := pgxpool.New(ctx, *databaseURL)
	if err != nil {
		logger.Error("connect to PostgreSQL", "error", err)
		return 1
	}
	defer pool.Close()

	httpClient := &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: *concurrency},
	}
	seeder := seed.NewSeeder(
		seed.NewPostgresUserStore(pool),
		seed.NewAuctionClient(*apiURL, httpClient),
		minter,
		seed.Options{Partitions: *partitions, Concurrency: *concurrency, TokenTTL: *tokenTTL, Logger: logger},
	)

	started := time.Now()
	manifest, runErr := seeder.Run(ctx, seed.Spec{
		Users:           *users,
		Auctions:        *auctions,
		AuctionDuration: *duration,
		CurrencyCode:    *currency,
	})
	if len(manifest.Users) == 0 && runErr != nil {
		logger.Error("seed", "error", runErr)
		return 1
	}

	writeErr := seed.WriteManifest(*out, manifest)
	if writeErr != nil {
		logger.Error("write manifest", "error", writeErr)
		return 1
	}
	printSeedSummary(manifest, *out, time.Since(started))

	if runErr != nil {
		logger.Error("seed ended early", "error", runErr)
		return 1
	}
	if manifest.Failed > 0 {
		return 1
	}
	return 0
}

func printSeedSummary(manifest seed.Manifest, path string, elapsed time.Duration) {
	fmt.Printf("users: %d\nauctions created: %d\nauctions failed: %d\nelapsed: %s\nmanifest: %s\n",
		len(manifest.Users), len(manifest.Auctions), manifest.Failed, elapsed.Round(time.Millisecond), path)
	for _, note := range manifest.FailureSamples {
		fmt.Printf("  failure: %s\n", note)
	}
	fmt.Println("auctions per partition:")
	for partition, count := range manifest.PartitionCounts {
		fmt.Printf("  %2d  %d\n", partition, count)
	}
}

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GeorgeCPO/AuctionBonanza/tools/loadgen/internal/seed"
)

func runSeed(ctx context.Context, args []string) int {
	flags := flag.NewFlagSet("seed", flag.ContinueOnError)
	users := flags.Int("users", 100, "number of seeded users (at least 2)")
	auctions := flags.Int("auctions", 1000, "number of auctions to create")
	concurrency := flags.Int("concurrency", 16, "requests in flight")
	duration := flags.Duration("duration", 7*24*time.Hour, fmt.Sprintf("how long each auction stays open (%s to %s)", seed.MinDuration, seed.MaxDuration))
	currency := flags.String("currency", "USD", "auction currency")
	partitions := flags.Int("partitions", 0, "partitions of the events topic; 0 asks the brokers")
	out := flags.String("out", "tools/loadgen/out/manifest.json", "manifest path")
	databaseURL := databaseURLFlag(flags)
	apiURL := flags.String("api-url", environmentOr("AUCTION_API_URL", "http://localhost:3002"), "auction-service URL (AUCTION_API_URL)")
	brokers := brokersFlag(flags)
	topic := topicFlag(flags)
	auth := registerAuthFlags(flags)
	err := flags.Parse(args)
	if err != nil {
		return 2
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	minter, err := auth.minter()
	if err != nil {
		logger.Error("load signing key", "error", err)
		return 1
	}
	partitionCount := *partitions
	if partitionCount == 0 {
		partitionCount, err = topicPartitions(ctx, splitList(*brokers), *topic)
		if err != nil {
			logger.Error("find the partition count (or pass --partitions)", "error", err)
			return 1
		}
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
		seed.Options{Partitions: partitionCount, Concurrency: *concurrency, Logger: logger},
	)

	started := time.Now()
	manifest, runErr := seeder.Run(ctx, seed.Spec{
		Users:           *users,
		Auctions:        *auctions,
		AuctionDuration: *duration,
		CurrencyCode:    *currency,
	})
	var interrupted *seed.InterruptedError
	if runErr != nil && !errors.As(runErr, &interrupted) {
		logger.Error("seed", "error", runErr)
		return 1
	}

	writeErr := seed.WriteManifest(*out, manifest)
	if writeErr != nil {
		logger.Error("write manifest", "error", writeErr)
		return 1
	}
	printSeedSummary(manifest, *out, time.Since(started))

	if interrupted != nil {
		logger.Error("seed ended early", "error", interrupted)
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
	for partition, count := range manifest.PartitionCounts() {
		fmt.Printf("  %2d  %d\n", partition, count)
	}
}

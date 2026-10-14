package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/GeorgeCPO/AuctionBonanza/tools/loadgen/internal/sample"
)

func runSample(ctx context.Context, args []string) int {
	flags := flag.NewFlagSet("sample", flag.ContinueOnError)
	databaseURL := flags.String("database-url", environmentOr("DATABASE_URL", "postgresql://auction:auction_local@localhost:5432/auction_db"), "PostgreSQL URL (DATABASE_URL)")
	brokers := flags.String("brokers", environmentOr("KAFKA_BROKERS", "localhost:9092"), "comma-separated Kafka brokers (KAFKA_BROKERS)")
	topic := flags.String("topic", environmentOr("AUCTION_EVENTS_TOPIC", "auction.events"), "topic to watch (AUCTION_EVENTS_TOPIC)")
	groups := flags.String("groups", "realtime-relay,settlement-worker", "consumer groups whose lag to track")
	interval := flags.Duration("interval", time.Second, "time between samples")
	duration := flags.Duration("duration", 0, "stop after this long (0 runs until interrupted)")
	outDir := flags.String("out-dir", filepath.Join("tools/loadgen/out", "samples-"+time.Now().Format("20060102-150405")), "directory for samples.csv, partitions.csv and summary.txt")
	err := flags.Parse(args)
	if err != nil {
		return 2
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	groupNames := splitList(*groups)

	poolConfig, err := pgxpool.ParseConfig(*databaseURL)
	if err != nil {
		logger.Error("parse database url", "error", err)
		return 1
	}
	poolConfig.MaxConns = 2
	// A sampler that waits on a locked or overloaded database would hide the
	// very condition it exists to report.
	poolConfig.ConnConfig.RuntimeParams["statement_timeout"] = "2000"
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		logger.Error("connect to PostgreSQL", "error", err)
		return 1
	}
	defer pool.Close()

	kafkaClient, err := kgo.NewClient(kgo.SeedBrokers(splitList(*brokers)...), kgo.ClientID("loadgen-sampler"))
	if err != nil {
		logger.Error("create Kafka client", "error", err)
		return 1
	}
	defer kafkaClient.Close()

	err = os.MkdirAll(*outDir, 0o755)
	if err != nil {
		logger.Error("create output directory", "error", err)
		return 1
	}
	samplesFile, err := os.Create(filepath.Join(*outDir, "samples.csv"))
	if err != nil {
		logger.Error("create samples.csv", "error", err)
		return 1
	}
	defer samplesFile.Close()
	partitionsFile, err := os.Create(filepath.Join(*outDir, "partitions.csv"))
	if err != nil {
		logger.Error("create partitions.csv", "error", err)
		return 1
	}
	defer partitionsFile.Close()

	recorder, err := sample.NewCSVRecorder(samplesFile, partitionsFile, groupNames)
	if err != nil {
		logger.Error("write CSV headers", "error", err)
		return 1
	}
	sampler := sample.NewSampler(
		sample.NewPostgresCollector(pool),
		sample.NewKafkaCollector(kadm.NewClient(kafkaClient), *topic, groupNames),
		recorder,
		sample.SamplerOptions{Interval: *interval, Logger: logger},
	)

	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}
	fmt.Printf("sampling every %s into %s (Ctrl-C to stop)\n", *interval, *outDir)
	summary, runErr := sampler.Run(ctx)

	summaryFile, err := os.Create(filepath.Join(*outDir, "summary.txt"))
	if err == nil {
		summary.Write(summaryFile)
		summaryFile.Close()
	}
	fmt.Println()
	summary.Write(os.Stdout)

	if runErr != nil {
		logger.Error("sampling stopped", "error", runErr)
		return 1
	}
	return 0
}

func splitList(value string) []string {
	var items []string
	for _, item := range strings.Split(value, ",") {
		trimmed := strings.TrimSpace(item)
		if trimmed != "" {
			items = append(items, trimmed)
		}
	}
	return items
}

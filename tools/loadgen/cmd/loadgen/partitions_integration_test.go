//go:build integration

package main

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/GeorgeCPO/AuctionBonanza/tools/loadgen/internal/testinfra"
)

func TestMain(m *testing.M) {
	testinfra.RequireEnvironment("TEST_KAFKA_BROKERS")
	os.Exit(m.Run())
}

func TestTopicPartitionsReadsTheRealCount(t *testing.T) {
	client := testinfra.NewKafka(t)
	topic := testinfra.NewTopic(t, client, 7)
	brokers := strings.Split(os.Getenv("TEST_KAFKA_BROKERS"), ",")

	count, err := topicPartitions(context.Background(), brokers, topic)

	if err != nil || count != 7 {
		t.Fatalf("got %d partitions (err %v), want 7", count, err)
	}
}

func TestTopicPartitionsReportsAMissingTopic(t *testing.T) {
	brokers := strings.Split(os.Getenv("TEST_KAFKA_BROKERS"), ",")

	_, err := topicPartitions(context.Background(), brokers, "loadgen-test-no-such-topic")

	if err == nil {
		t.Fatal("a missing topic was reported as having partitions")
	}
}

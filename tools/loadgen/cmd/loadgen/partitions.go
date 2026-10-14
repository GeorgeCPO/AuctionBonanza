package main

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// topicPartitions asks the brokers how many partitions a topic has, so the
// partitions in a manifest cannot silently disagree with the topic they claim to
// describe.
func topicPartitions(ctx context.Context, brokers []string, topic string) (int, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ClientID("loadgen-seeder"))
	if err != nil {
		return 0, fmt.Errorf("create Kafka client: %w", err)
	}
	defer client.Close()

	details, err := kadm.NewClient(client).ListTopics(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("list topic %s: %w", topic, err)
	}
	detail, found := details[topic]
	if !found || detail.Err != nil || len(detail.Partitions) == 0 {
		return 0, fmt.Errorf("topic %s not found or unreadable (run `docker compose up kafka-init`): %v", topic, detail.Err)
	}
	return len(detail.Partitions), nil
}

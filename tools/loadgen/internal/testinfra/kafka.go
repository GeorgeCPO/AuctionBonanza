package testinfra

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// NewKafka returns a client on the brokers TEST_KAFKA_BROKERS names, whose keyed
// records are placed the way Kafka's default partitioner places them, and closes
// it when the test ends. Call RequireEnvironment from TestMain first.
func NewKafka(t testing.TB) *kgo.Client {
	t.Helper()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(os.Getenv("TEST_KAFKA_BROKERS"), ",")...),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	)
	if err != nil {
		t.Fatalf("create Kafka client: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

// NewTopic creates a topic with a random name and deletes it when the test ends.
func NewTopic(t testing.TB, client *kgo.Client, partitions int32) string {
	t.Helper()
	ctx := context.Background()

	name := uniqueName(t, "loadgen-test-")

	admin := kadm.NewClient(client)
	_, err := admin.CreateTopic(ctx, partitions, 1, nil, name)
	if err != nil {
		t.Fatalf("create topic %s: %v", name, err)
	}
	t.Cleanup(func() {
		_, deleteErr := admin.DeleteTopics(ctx, name)
		if deleteErr != nil {
			t.Errorf("delete topic %s: %v", name, deleteErr)
		}
	})
	waitUntilVisible(t, admin, name, int(partitions))
	return name
}

// waitUntilVisible returns once the brokers report the topic with all its
// partitions led. A created topic is not visible at once: asking right away can
// answer UNKNOWN_TOPIC_OR_PARTITION, and the busier the machine the likelier it
// is.
func waitUntilVisible(t testing.TB, admin *kadm.Client, name string, partitions int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		details, err := admin.ListTopics(context.Background(), name)
		if err == nil {
			detail := details[name]
			if detail.Err == nil && len(detail.Partitions) == partitions && allLed(detail.Partitions) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("topic %s did not become visible with %d partitions", name, partitions)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func allLed(partitions kadm.PartitionDetails) bool {
	for _, partition := range partitions {
		if partition.Err != nil || partition.Leader < 0 {
			return false
		}
	}
	return true
}

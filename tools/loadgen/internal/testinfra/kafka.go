package testinfra

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// NewKafka returns a client whose keyed records are placed the way Kafka's
// default partitioner places them, and closes it when the test ends.
func NewKafka(t testing.TB, brokers string) *kgo.Client {
	t.Helper()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
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

	suffix := make([]byte, 6)
	_, err := rand.Read(suffix)
	if err != nil {
		t.Fatal(err)
	}
	name := "loadgen-test-" + hex.EncodeToString(suffix)

	admin := kadm.NewClient(client)
	_, err = admin.CreateTopic(ctx, partitions, 1, nil, name)
	if err != nil {
		t.Fatalf("create topic %s: %v", name, err)
	}
	t.Cleanup(func() {
		_, deleteErr := admin.DeleteTopics(ctx, name)
		if deleteErr != nil {
			t.Errorf("delete topic %s: %v", name, deleteErr)
		}
	})
	return name
}

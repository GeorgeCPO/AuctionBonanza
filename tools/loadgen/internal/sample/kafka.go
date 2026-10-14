package sample

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/twmb/franz-go/pkg/kadm"
)

// KafkaCollector reads the watched topic's offsets and the committed offsets of
// the named consumer groups. It only reads: no group is joined, so sampling
// cannot trigger a rebalance.
type KafkaCollector struct {
	admin  *kadm.Client
	topic  string
	groups []string
}

func NewKafkaCollector(admin *kadm.Client, topic string, groups []string) *KafkaCollector {
	return &KafkaCollector{admin: admin, topic: topic, groups: groups}
}

func (c *KafkaCollector) Collect(ctx context.Context) (KafkaSnapshot, error) {
	starts, err := c.admin.ListStartOffsets(ctx, c.topic)
	if err != nil {
		return KafkaSnapshot{}, fmt.Errorf("list start offsets: %w", err)
	}
	err = starts.Error()
	if err != nil {
		return KafkaSnapshot{}, fmt.Errorf("list start offsets: %w", err)
	}
	ends, err := c.admin.ListEndOffsets(ctx, c.topic)
	if err != nil {
		return KafkaSnapshot{}, fmt.Errorf("list end offsets: %w", err)
	}
	err = ends.Error()
	if err != nil {
		return KafkaSnapshot{}, fmt.Errorf("list end offsets: %w", err)
	}
	endsByPartition, found := ends[c.topic]
	if !found || len(endsByPartition) == 0 {
		return KafkaSnapshot{}, fmt.Errorf("topic %s not found", c.topic)
	}

	partitions := make([]PartitionSnapshot, 0, len(endsByPartition))
	for number, end := range endsByPartition {
		start, _ := starts.Lookup(c.topic, number)
		partitions = append(partitions, PartitionSnapshot{
			Partition:   number,
			StartOffset: max(start.Offset, 0),
			EndOffset:   end.Offset,
			Groups:      map[string]GroupProgress{},
		})
	}
	slices.SortFunc(partitions, func(a, b PartitionSnapshot) int {
		return cmp.Compare(a.Partition, b.Partition)
	})

	snapshot := KafkaSnapshot{Partitions: partitions, GroupLag: map[string]int64{}}
	if len(c.groups) == 0 {
		return snapshot, nil
	}

	// One request for all groups, not one per group.
	fetched := c.admin.FetchManyOffsets(ctx, c.groups...)
	for _, group := range c.groups {
		response := fetched[group]
		if response.Err != nil {
			return KafkaSnapshot{}, fmt.Errorf("fetch offsets of group %s: %w", group, response.Err)
		}
		addGroup(&snapshot, group, committedOffsets(response, c.topic))
	}
	return snapshot, nil
}

// committedOffsets are a group's valid commits on the topic.
func committedOffsets(response kadm.FetchOffsetsResponse, topic string) map[int32]int64 {
	committed := map[int32]int64{}
	for _, offset := range response.Fetched[topic] {
		if offset.Err == nil && offset.At >= 0 {
			committed[offset.Partition] = offset.At
		}
	}
	return committed
}

// addGroup records a group's progress if it has committed anything on the
// topic. A group that does not exist, or has never committed, is left out
// entirely, so it appears as unknown rather than as zero lag.
func addGroup(snapshot *KafkaSnapshot, group string, committed map[int32]int64) {
	if len(committed) == 0 {
		return
	}

	var total int64
	for i := range snapshot.Partitions {
		partition := &snapshot.Partitions[i]
		position, hasCommit := committed[partition.Partition]
		progress := GroupProgress{Committed: -1}
		if hasCommit {
			progress.Committed = position
			progress.Lag = max(partition.EndOffset-position, 0)
		} else {
			progress.Lag = max(partition.EndOffset-partition.StartOffset, 0)
		}
		partition.Groups[group] = progress
		total += progress.Lag
	}
	snapshot.GroupLag[group] = total
}

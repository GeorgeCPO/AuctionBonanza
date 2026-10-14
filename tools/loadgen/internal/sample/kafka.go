package sample

import (
	"context"
	"fmt"
	"sort"

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
	return &KafkaCollector{admin: admin, topic: topic, groups: append([]string(nil), groups...)}
}

func (c *KafkaCollector) Collect(ctx context.Context) (KafkaSnapshot, error) {
	starts, err := c.admin.ListStartOffsets(ctx, c.topic)
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
	sort.Slice(partitions, func(a, b int) bool { return partitions[a].Partition < partitions[b].Partition })

	snapshot := KafkaSnapshot{Partitions: partitions, GroupLag: map[string]int64{}}
	for _, group := range c.groups {
		err = c.addGroup(ctx, &snapshot, group)
		if err != nil {
			return KafkaSnapshot{}, err
		}
	}
	return snapshot, nil
}

// addGroup records a group's progress if it has committed anything on the
// topic. A group that does not exist, or has never committed, is left out
// entirely, so it appears as unknown rather than as zero lag.
func (c *KafkaCollector) addGroup(ctx context.Context, snapshot *KafkaSnapshot, group string) error {
	responses, err := c.admin.FetchOffsets(ctx, group)
	if err != nil {
		return fmt.Errorf("fetch offsets of group %s: %w", group, err)
	}

	committed := map[int32]int64{}
	for _, response := range responses[c.topic] {
		if response.Err == nil && response.At >= 0 {
			committed[response.Partition] = response.At
		}
	}
	if len(committed) == 0 {
		return nil
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
	return nil
}

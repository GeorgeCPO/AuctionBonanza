package sample

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
)

var errTest = errors.New("offset fetch failed")

func snapshotWithEnds(ends ...int64) KafkaSnapshot {
	snapshot := KafkaSnapshot{GroupLag: map[string]int64{}}
	for partition, end := range ends {
		snapshot.Partitions = append(snapshot.Partitions, PartitionSnapshot{
			Partition: int32(partition),
			EndOffset: end,
			Groups:    map[string]GroupProgress{},
		})
	}
	return snapshot
}

func response(offsets ...kadm.OffsetResponse) kadm.FetchOffsetsResponse {
	fetched := kadm.OffsetResponses{}
	for _, offset := range offsets {
		fetched.Add(offset)
	}
	return kadm.FetchOffsetsResponse{Fetched: fetched}
}

func commit(topic string, partition int32, at int64) kadm.OffsetResponse {
	return kadm.OffsetResponse{Offset: kadm.Offset{Topic: topic, Partition: partition, At: at}}
}

func failedFetch(topic string, partition int32) kadm.OffsetResponse {
	return kadm.OffsetResponse{Offset: kadm.Offset{Topic: topic, Partition: partition, At: -1}, Err: errTest}
}

func TestCommittedOffsetsSeparatesCommitsFromFailuresAndNoCommit(t *testing.T) {
	r := response(commit("t", 0, 40), failedFetch("t", 1), commit("t", 2, -1), commit("t", 3, 0))

	committed, failed := committedOffsets(r, "t")

	if len(committed) != 2 || committed[0] != 40 || committed[3] != 0 {
		t.Errorf("committed %v, want partitions 0 and 3 (an offset of 0 is a commit, -1 is none)", committed)
	}
	if len(failed) != 1 || !failed[1] {
		t.Errorf("failed %v, want only partition 1", failed)
	}
}

func TestAGroupWithAFailedPartitionHasNoTotalAndNoMadeUpLag(t *testing.T) {
	snapshot := snapshotWithEnds(100, 100, 100)
	committed := map[int32]int64{0: 90, 2: 100}
	failed := map[int32]bool{1: true}

	addGroup(&snapshot, "relay", committed, failed)

	if _, known := snapshot.Partitions[1].Groups["relay"]; known {
		t.Errorf("partition 1 could not be read, yet it reports %+v: its whole contents must not become lag", snapshot.Partitions[1].Groups["relay"])
	}
	if got := snapshot.Partitions[0].Groups["relay"]; got.Lag != 10 || got.Committed != 90 {
		t.Errorf("partition 0 progress %+v, want committed 90 and lag 10", got)
	}
	if got := snapshot.Partitions[2].Groups["relay"]; got.Lag != 0 {
		t.Errorf("partition 2 progress %+v, want lag 0", got)
	}
	if total, known := snapshot.GroupLag["relay"]; known {
		t.Errorf("group total %d reported although a partition is missing; it would look complete", total)
	}
}

func TestAGroupWithOnlyFailedPartitionsIsUnknown(t *testing.T) {
	snapshot := snapshotWithEnds(100, 100)

	addGroup(&snapshot, "relay", map[int32]int64{}, map[int32]bool{0: true, 1: true})

	if len(snapshot.GroupLag) != 0 || len(snapshot.Partitions[0].Groups) != 0 {
		t.Errorf("a group nobody could read left data behind: %+v", snapshot)
	}
}

func TestAnUncommittedPartitionOfAHealthyGroupCountsItsWholeContents(t *testing.T) {
	snapshot := snapshotWithEnds(100, 70)
	snapshot.Partitions[1].StartOffset = 20

	addGroup(&snapshot, "relay", map[int32]int64{0: 100}, map[int32]bool{})

	if got := snapshot.Partitions[1].Groups["relay"]; got.Committed != -1 || got.Lag != 50 {
		t.Errorf("partition 1 progress %+v, want no commit and lag 50 (70 minus the start offset of 20)", got)
	}
	if snapshot.GroupLag["relay"] != 50 {
		t.Errorf("group lag %d, want 50", snapshot.GroupLag["relay"])
	}
}

func TestAGroupThatNeverCommittedIsLeftOut(t *testing.T) {
	snapshot := snapshotWithEnds(100)

	addGroup(&snapshot, "relay", map[int32]int64{}, map[int32]bool{})

	if len(snapshot.GroupLag) != 0 {
		t.Errorf("a group with no commits reported lag: %v", snapshot.GroupLag)
	}
}

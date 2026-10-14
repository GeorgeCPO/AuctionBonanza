package sample

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestRecorderWritesHeadersWithGroupColumns(t *testing.T) {
	var samples, partitions bytes.Buffer

	_, err := NewCSVRecorder(&samples, &partitions, []string{"realtime-relay", "settlement-worker"})
	if err != nil {
		t.Fatal(err)
	}

	wantSamples := "time,bids_issued,bids_per_sec,orders_issued,orders_per_sec,outbox_unpublished,outbox_oldest_age_s,outbox_retrying,lock_waiters,idle_in_transaction,active,events_per_sec,lag_realtime-relay,lag_settlement-worker\n"
	wantPartitions := "time,partition,start_offset,end_offset,produce_per_sec,committed_realtime-relay,lag_realtime-relay,committed_settlement-worker,lag_settlement-worker\n"
	if samples.String() != wantSamples {
		t.Errorf("samples header\n got %q\nwant %q", samples.String(), wantSamples)
	}
	if partitions.String() != wantPartitions {
		t.Errorf("partitions header\n got %q\nwant %q", partitions.String(), wantPartitions)
	}
}

func TestRecorderWritesAFullObservation(t *testing.T) {
	var samples, partitions bytes.Buffer
	recorder, err := NewCSVRecorder(&samples, &partitions, []string{"relay", "settlement"})
	if err != nil {
		t.Fatal(err)
	}

	err = recorder.Record(Observation{
		At: epoch.Add(1500 * time.Millisecond),
		Postgres: &PostgresSnapshot{
			BidsIssued: 1200, OrdersIssued: 7, OutboxUnpublished: 40, OutboxOldestAgeSeconds: 1.25,
			OutboxRetrying: 2, LockWaiters: 5, IdleInTransaction: 1, Active: 9,
		},
		BidRate:   ptr(250.5),
		OrderRate: ptr(0),
		EventRate: ptr(300),
		Kafka: &KafkaSnapshot{
			GroupLag: map[string]int64{"relay": 15},
			Partitions: []PartitionSnapshot{
				{Partition: 0, StartOffset: 0, EndOffset: 100, Groups: map[string]GroupProgress{"relay": {Committed: 85, Lag: 15}}},
				{Partition: 1, StartOffset: 10, EndOffset: 50, Groups: map[string]GroupProgress{}},
			},
		},
		ProduceRates: map[int32]float64{0: 120.5},
	})
	if err != nil {
		t.Fatal(err)
	}

	wantSamples := "2030-01-01T00:00:01.5Z,1200,250.500,7,0.000,40,1.250,2,5,1,9,300.000,15,\n"
	if !strings.HasSuffix(samples.String(), wantSamples) {
		t.Errorf("samples row\n got %q\nwant suffix %q", samples.String(), wantSamples)
	}
	rows := strings.Split(strings.TrimSpace(partitions.String()), "\n")
	if len(rows) != 3 {
		t.Fatalf("partitions file has %d lines, want header and 2 rows", len(rows))
	}
	if rows[1] != "2030-01-01T00:00:01.5Z,0,0,100,120.500,85,15,," {
		t.Errorf("partition 0 row %q", rows[1])
	}
	if rows[2] != "2030-01-01T00:00:01.5Z,1,10,50,,,,," {
		t.Errorf("partition 1 row %q: no rate and no group progress must be empty cells", rows[2])
	}
}

func TestRecorderWritesEmptyCellsForAFailedSource(t *testing.T) {
	var samples, partitions bytes.Buffer
	recorder, err := NewCSVRecorder(&samples, &partitions, []string{"relay"})
	if err != nil {
		t.Fatal(err)
	}

	err = recorder.Record(Observation{At: epoch})
	if err != nil {
		t.Fatal(err)
	}

	if !strings.HasSuffix(samples.String(), "2030-01-01T00:00:00Z,,,,,,,,,,,,\n") {
		t.Errorf("a failed tick must keep its timestamp and leave every value empty, got %q", samples.String())
	}
	if lines := strings.Count(partitions.String(), "\n"); lines != 1 {
		t.Errorf("partitions file has %d lines, want only the header when Kafka failed", lines)
	}
}

func TestRecorderLeavesAnUncommittedGroupsCommitEmptyButKeepsItsLag(t *testing.T) {
	var samples, partitions bytes.Buffer
	recorder, err := NewCSVRecorder(&samples, &partitions, []string{"relay"})
	if err != nil {
		t.Fatal(err)
	}

	err = recorder.Record(Observation{
		At: epoch,
		Kafka: &KafkaSnapshot{
			GroupLag: map[string]int64{"relay": 40},
			Partitions: []PartitionSnapshot{
				{Partition: 2, StartOffset: 0, EndOffset: 40, Groups: map[string]GroupProgress{"relay": {Committed: -1, Lag: 40}}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(partitions.String(), "2030-01-01T00:00:00Z,2,0,40,,,40\n") {
		t.Errorf("got %q", partitions.String())
	}
}

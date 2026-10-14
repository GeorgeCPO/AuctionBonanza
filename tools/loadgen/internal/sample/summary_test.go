package sample

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func feed(builder *SummaryBuilder, observations ...Observation) {
	for _, observation := range observations {
		builder.Add(observation)
	}
}

func TestSummaryReportsPeaksTotalsAndTheHottestPartition(t *testing.T) {
	builder := NewSummaryBuilder()
	feed(builder,
		Observation{
			At:       epoch,
			Postgres: &PostgresSnapshot{BidsIssued: 0, OrdersIssued: 0, OutboxUnpublished: 2, LockWaiters: 1},
			Kafka:    &KafkaSnapshot{Partitions: []PartitionSnapshot{{Partition: 0, EndOffset: 0}, {Partition: 1, EndOffset: 0}}, GroupLag: map[string]int64{"relay": 0}},
		},
		Observation{
			At:       epoch.Add(time.Second),
			Postgres: &PostgresSnapshot{BidsIssued: 500, OrdersIssued: 2, OutboxUnpublished: 90, OutboxOldestAgeSeconds: 4.5, OutboxRetrying: 3, LockWaiters: 7, IdleInTransaction: 2},
			BidRate:  ptr(500),
			Kafka:    &KafkaSnapshot{Partitions: []PartitionSnapshot{{Partition: 0, EndOffset: 100}, {Partition: 1, EndOffset: 300}}, GroupLag: map[string]int64{"relay": 250}},
		},
		Observation{
			At:       epoch.Add(2 * time.Second),
			Postgres: &PostgresSnapshot{BidsIssued: 1000, OrdersIssued: 5, OutboxUnpublished: 10, LockWaiters: 0},
			BidRate:  ptr(500),
			Kafka:    &KafkaSnapshot{Partitions: []PartitionSnapshot{{Partition: 0, EndOffset: 200}, {Partition: 1, EndOffset: 600}}, GroupLag: map[string]int64{"relay": 40}},
		},
	)

	summary := builder.Build()

	if summary.Ticks != 3 || summary.Elapsed != 2*time.Second {
		t.Errorf("ticks %d over %s", summary.Ticks, summary.Elapsed)
	}
	if summary.BidsIssued != 1000 || summary.MeanBidRate != 500 || summary.PeakBidRate != 500 || summary.OrdersIssued != 5 {
		t.Errorf("totals wrong: %+v", summary)
	}
	if summary.PeakOutboxUnpublished != 90 || summary.PeakOutboxOldestAgeSeconds != 4.5 || summary.PeakOutboxRetrying != 3 {
		t.Errorf("outbox peaks wrong: %+v", summary)
	}
	if summary.PeakLockWaiters != 7 || summary.PeakIdleInTransaction != 2 {
		t.Errorf("connection peaks wrong: %+v", summary)
	}
	if summary.PeakGroupLag["relay"] != 250 || summary.FinalGroupLag["relay"] != 40 {
		t.Errorf("group lag peak %d final %d, want 250 and 40", summary.PeakGroupLag["relay"], summary.FinalGroupLag["relay"])
	}
	if summary.Hottest == nil || summary.Hottest.Partition != 1 || summary.Hottest.Produced != 600 || summary.Hottest.Share != 0.75 {
		t.Errorf("hottest partition wrong: %+v", summary.Hottest)
	}
}

func TestSummaryCountsFailedSourcesAndStillSummarisesTheRest(t *testing.T) {
	builder := NewSummaryBuilder()
	feed(builder,
		Observation{At: epoch, Postgres: &PostgresSnapshot{BidsIssued: 10}},
		Observation{At: epoch.Add(time.Second)},
		Observation{At: epoch.Add(2 * time.Second), Postgres: &PostgresSnapshot{BidsIssued: 30}},
	)

	summary := builder.Build()

	if summary.PostgresFailures != 1 || summary.KafkaFailures != 3 {
		t.Errorf("failures: postgres %d kafka %d, want 1 and 3", summary.PostgresFailures, summary.KafkaFailures)
	}
	if summary.BidsIssued != 20 || summary.MeanBidRate != 10 {
		t.Errorf("bids %d mean %.1f; a gap must not distort the totals", summary.BidsIssued, summary.MeanBidRate)
	}
	if summary.Hottest != nil {
		t.Error("a hottest partition was named without any Kafka reading")
	}
}

func TestSummaryNamesNoHottestPartitionWhenNothingWasProduced(t *testing.T) {
	builder := NewSummaryBuilder()
	snapshot := &KafkaSnapshot{Partitions: []PartitionSnapshot{{Partition: 0, EndOffset: 5}}}
	feed(builder, Observation{At: epoch, Kafka: snapshot}, Observation{At: epoch.Add(time.Second), Kafka: snapshot})

	summary := builder.Build()
	if summary.Hottest != nil {
		t.Error("a quiet topic produced a hottest partition")
	}
}

func TestSummaryWriteMentionsTheKeyNumbers(t *testing.T) {
	builder := NewSummaryBuilder()
	feed(builder,
		Observation{At: epoch, Postgres: &PostgresSnapshot{}, Kafka: &KafkaSnapshot{Partitions: []PartitionSnapshot{{Partition: 4, EndOffset: 0}}, GroupLag: map[string]int64{"settlement": 3}}},
		Observation{At: epoch.Add(time.Second), Postgres: &PostgresSnapshot{LockWaiters: 9}, Kafka: &KafkaSnapshot{Partitions: []PartitionSnapshot{{Partition: 4, EndOffset: 8}}, GroupLag: map[string]int64{"settlement": 1}}},
	)

	var out bytes.Buffer
	builder.Build().Write(&out)

	for _, want := range []string{"2 over 1s", "9 waiting on a lock", "group settlement: peak lag 3, final lag 1", "hottest partition: 4 with 8 records (100% of 8)"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("summary lacks %q:\n%s", want, out.String())
		}
	}
}

func TestSummaryIgnoresTheOutboxOfTicksWhereItCouldNotBeRead(t *testing.T) {
	builder := NewSummaryBuilder()
	feed(builder,
		Observation{At: epoch, Postgres: &PostgresSnapshot{OutboxUnpublished: 40, LockWaiters: 2}},
		Observation{At: epoch.Add(time.Second), Postgres: &PostgresSnapshot{OutboxUnavailable: "timeout", LockWaiters: 9}},
	)

	summary := builder.Build()

	if summary.OutboxFailures != 1 || summary.PeakOutboxUnpublished != 40 {
		t.Errorf("outbox failures %d, peak %d, want 1 and 40", summary.OutboxFailures, summary.PeakOutboxUnpublished)
	}
	if summary.PeakLockWaiters != 9 {
		t.Errorf("peak lock waiters %d, want 9: only the outbox is missing on that tick", summary.PeakLockWaiters)
	}
	var out bytes.Buffer
	summary.Write(&out)
	if !strings.Contains(out.String(), "outbox could not be read on 1 ticks") {
		t.Errorf("the summary does not warn that its outbox peaks may be too low:\n%s", out.String())
	}
}

func pgReading(at time.Time, bids, orders int64) Observation {
	return Observation{At: at, Postgres: &PostgresSnapshot{BidsIssued: bids, OrdersIssued: orders}}
}

func TestSummaryTotalsSurviveACounterReset(t *testing.T) {
	builder := NewSummaryBuilder()
	// 100 -> 200 is +100; 200 -> 5 is a reset (nothing counted); 5 -> 15 is +10.
	feed(builder,
		pgReading(epoch, 100, 10),
		pgReading(epoch.Add(time.Second), 200, 12),
		pgReading(epoch.Add(2*time.Second), 5, 0),
		pgReading(epoch.Add(3*time.Second), 15, 1),
	)

	summary := builder.Build()

	if summary.BidsIssued != 110 {
		t.Errorf("bids %d, want 110 from the intervals either side of the reset", summary.BidsIssued)
	}
	if summary.OrdersIssued != 3 {
		t.Errorf("orders %d, want 3 (+2, reset, +1)", summary.OrdersIssued)
	}
	if summary.CounterResets != 2 {
		t.Errorf("resets %d, want 2: the bids counter and the orders counter each fell once", summary.CounterResets)
	}
	if summary.MeanBidRate <= 0 {
		t.Errorf("mean bid rate %.2f, want a positive average over the whole run", summary.MeanBidRate)
	}

	var out bytes.Buffer
	summary.Write(&out)
	if !strings.Contains(out.String(), "counter went backwards 2 times") {
		t.Errorf("the summary does not warn about the reset:\n%s", out.String())
	}
}

func TestSummaryNeverReportsANegativeTotalWhateverTheCounterDoes(t *testing.T) {
	builder := NewSummaryBuilder()
	feed(builder, pgReading(epoch, 900, 0), pgReading(epoch.Add(time.Second), 3, 0))

	summary := builder.Build()

	if summary.BidsIssued < 0 || summary.MeanBidRate < 0 {
		t.Errorf("a reset produced bids %d, mean %.2f", summary.BidsIssued, summary.MeanBidRate)
	}
}

func kafkaReading(at time.Time, ends ...int64) Observation {
	return Observation{At: at, Kafka: kafkaAt(ends...)}
}

func TestSummaryCountsProductionAcrossATopicRecreation(t *testing.T) {
	builder := NewSummaryBuilder()
	// Partition 0: 0 -> 100 (+100), recreated to 10 (nothing counted), 10 -> 60 (+50).
	// Partition 1: 0 -> 30 -> 60 -> 90, no reset (+90).
	feed(builder,
		kafkaReading(epoch, 0, 0),
		kafkaReading(epoch.Add(time.Second), 100, 30),
		kafkaReading(epoch.Add(2*time.Second), 10, 60),
		kafkaReading(epoch.Add(3*time.Second), 60, 90),
	)

	summary := builder.Build()

	if len(summary.Partitions) != 2 || summary.Partitions[0].Produced != 150 || summary.Partitions[1].Produced != 90 {
		t.Fatalf("produced per partition %+v, want 150 and 90", summary.Partitions)
	}
	if summary.CounterResets != 1 {
		t.Errorf("resets %d, want 1", summary.CounterResets)
	}
	if summary.Hottest == nil || summary.Hottest.Partition != 0 {
		t.Errorf("hottest %+v, want partition 0", summary.Hottest)
	}
}

func TestSummaryListsAPartitionNothingWasProducedTo(t *testing.T) {
	builder := NewSummaryBuilder()
	feed(builder, kafkaReading(epoch, 5, 5, 5), kafkaReading(epoch.Add(time.Second), 5, 9, 5))

	summary := builder.Build()

	if len(summary.Partitions) != 3 || summary.Partitions[0].Produced != 0 || summary.Partitions[1].Produced != 4 {
		t.Errorf("partitions %+v, want all three listed with 0, 4, 0 produced", summary.Partitions)
	}
}

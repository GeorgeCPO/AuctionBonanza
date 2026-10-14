package sample

import (
	"cmp"
	"fmt"
	"io"
	"maps"
	"slices"
	"time"
)

// PartitionSummary is how much one partition received during the run.
type PartitionSummary struct {
	Partition int32
	Produced  int64
	// Share is the fraction of everything produced during the run.
	Share float64
}

// Summary condenses a run: the peaks that name a bottleneck and the totals
// that give the average rates.
type Summary struct {
	Ticks            int
	PostgresFailures int
	KafkaFailures    int
	// OutboxFailures counts ticks where PostgreSQL answered but the outbox could
	// not be read; the outbox peaks cover only the other ticks.
	OutboxFailures int
	// CounterResets counts times a counter went backwards between two readings (a
	// database or topic recreated mid-run). The totals leave out those intervals,
	// so after a reset they can understate what happened, never overstate it.
	CounterResets int
	Elapsed       time.Duration

	BidsIssued   int64
	MeanBidRate  float64
	PeakBidRate  float64
	OrdersIssued int64

	PeakOutboxUnpublished      int64
	PeakOutboxOldestAgeSeconds float64
	PeakOutboxRetrying         int64
	PeakLockWaiters            int64
	PeakIdleInTransaction      int64

	PeakGroupLag  map[string]int64
	FinalGroupLag map[string]int64

	// Partitions is ordered by partition number.
	Partitions []PartitionSummary
	// Hottest is the partition that received the most, if anything was produced.
	Hottest *PartitionSummary
}

// SummaryBuilder accumulates observations into a Summary. It owns its state
// privately; callers feed it observations and ask for the result.
type SummaryBuilder struct {
	ticks            int
	postgresFailures int
	kafkaFailures    int
	outboxFailures   int
	first, last      time.Time

	firstPostgres, lastPostgres *Timed[PostgresSnapshot]
	peakBidRate                 float64
	peaks                       PostgresSnapshot
	// Totals add up the intervals between consecutive good readings, so a counter
	// that fell contributes nothing for that interval instead of a negative one.
	bidsIssued, ordersIssued int64
	counterResets            int

	lastKafka    *Timed[KafkaSnapshot]
	peakGroupLag map[string]int64
	producedBy   map[int32]int64
}

func NewSummaryBuilder() *SummaryBuilder {
	return &SummaryBuilder{peakGroupLag: map[string]int64{}, producedBy: map[int32]int64{}}
}

func (b *SummaryBuilder) Add(o Observation) {
	if b.ticks == 0 {
		b.first = o.At
	}
	b.ticks++
	b.last = o.At

	if o.Postgres == nil {
		b.postgresFailures++
	} else {
		b.addPostgres(o)
	}
	if o.Kafka == nil {
		b.kafkaFailures++
	} else {
		b.addKafka(o)
	}
}

func (b *SummaryBuilder) Build() Summary {
	summary := Summary{
		Ticks:            b.ticks,
		PostgresFailures: b.postgresFailures,
		KafkaFailures:    b.kafkaFailures,
		OutboxFailures:   b.outboxFailures,
		Elapsed:          b.last.Sub(b.first),
		PeakBidRate:      b.peakBidRate,

		PeakOutboxUnpublished:      b.peaks.OutboxUnpublished,
		PeakOutboxOldestAgeSeconds: b.peaks.OutboxOldestAgeSeconds,
		PeakOutboxRetrying:         b.peaks.OutboxRetrying,
		PeakLockWaiters:            b.peaks.LockWaiters,
		PeakIdleInTransaction:      b.peaks.IdleInTransaction,

		PeakGroupLag:  b.peakGroupLag,
		FinalGroupLag: map[string]int64{},
		CounterResets: b.counterResets,
		BidsIssued:    b.bidsIssued,
		OrdersIssued:  b.ordersIssued,
	}

	if b.firstPostgres != nil && b.lastPostgres != nil {
		span := b.lastPostgres.At.Sub(b.firstPostgres.At).Seconds()
		if span > 0 {
			summary.MeanBidRate = float64(summary.BidsIssued) / span
		}
	}

	if b.lastKafka != nil {
		summary.FinalGroupLag = b.lastKafka.Value.GroupLag
		summary.Partitions = b.producedSummaries()
		summary.Hottest = hottest(summary.Partitions)
	}
	return summary
}

func (b *SummaryBuilder) addPostgres(o Observation) {
	reading := Timed[PostgresSnapshot]{At: o.At, Value: *o.Postgres}
	if b.firstPostgres == nil {
		b.firstPostgres = &reading
	}
	if b.lastPostgres != nil {
		b.bidsIssued += b.interval(b.lastPostgres.Value.BidsIssued, reading.Value.BidsIssued)
		b.ordersIssued += b.interval(b.lastPostgres.Value.OrdersIssued, reading.Value.OrdersIssued)
	}
	b.lastPostgres = &reading

	if o.BidRate != nil && *o.BidRate > b.peakBidRate {
		b.peakBidRate = *o.BidRate
	}
	if o.Postgres.OutboxUnavailable == "" {
		b.peaks.OutboxUnpublished = max(b.peaks.OutboxUnpublished, o.Postgres.OutboxUnpublished)
		b.peaks.OutboxOldestAgeSeconds = max(b.peaks.OutboxOldestAgeSeconds, o.Postgres.OutboxOldestAgeSeconds)
		b.peaks.OutboxRetrying = max(b.peaks.OutboxRetrying, o.Postgres.OutboxRetrying)
	} else {
		b.outboxFailures++
	}
	b.peaks.LockWaiters = max(b.peaks.LockWaiters, o.Postgres.LockWaiters)
	b.peaks.IdleInTransaction = max(b.peaks.IdleInTransaction, o.Postgres.IdleInTransaction)
}

func (b *SummaryBuilder) addKafka(o Observation) {
	reading := Timed[KafkaSnapshot]{At: o.At, Value: *o.Kafka}
	before := map[int32]int64{}
	if b.lastKafka != nil {
		for _, partition := range b.lastKafka.Value.Partitions {
			before[partition.Partition] = partition.EndOffset
		}
	}
	for _, partition := range reading.Value.Partitions {
		previousEnd, known := before[partition.Partition]
		if !known {
			// A partition seen for the first time counts from here, and appears in
			// the summary even if nothing is ever produced to it.
			if b.lastKafka == nil {
				b.producedBy[partition.Partition] = 0
			}
			continue
		}
		b.producedBy[partition.Partition] += b.interval(previousEnd, partition.EndOffset)
	}
	b.lastKafka = &reading

	for group, lag := range o.Kafka.GroupLag {
		b.peakGroupLag[group] = max(b.peakGroupLag[group], lag)
	}
}

// Write prints the summary as plain text.
func (s Summary) Write(w io.Writer) {
	fmt.Fprintf(w, "ticks: %d over %s (postgres failed %d, kafka failed %d)\n",
		s.Ticks, s.Elapsed.Round(time.Millisecond), s.PostgresFailures, s.KafkaFailures)
	fmt.Fprintf(w, "bid ids issued: %d (mean %.1f/s, peak %.1f/s), order ids issued: %d\n",
		s.BidsIssued, s.MeanBidRate, s.PeakBidRate, s.OrdersIssued)
	if s.CounterResets > 0 {
		fmt.Fprintf(w, "WARNING: a counter went backwards %d times (database or topic recreated?); totals leave out those intervals\n", s.CounterResets)
	}
	fmt.Fprintf(w, "outbox: peak %d unpublished, oldest %.1fs, peak %d retrying\n",
		s.PeakOutboxUnpublished, s.PeakOutboxOldestAgeSeconds, s.PeakOutboxRetrying)
	if s.OutboxFailures > 0 {
		fmt.Fprintf(w, "WARNING: the outbox could not be read on %d ticks, so these peaks may be too low\n", s.OutboxFailures)
	}
	fmt.Fprintf(w, "connections: peak %d waiting on a lock, peak %d idle in transaction\n",
		s.PeakLockWaiters, s.PeakIdleInTransaction)

	for _, group := range slices.Sorted(maps.Keys(s.PeakGroupLag)) {
		fmt.Fprintf(w, "group %s: peak lag %d, final lag %d\n", group, s.PeakGroupLag[group], s.FinalGroupLag[group])
	}

	if s.Hottest != nil {
		fmt.Fprintf(w, "hottest partition: %d with %d records (%.0f%% of %d)\n",
			s.Hottest.Partition, s.Hottest.Produced, s.Hottest.Share*100, totalProduced(s.Partitions))
	}
}

// interval is how much a counter grew between two readings. A counter that went
// down was reset, and contributes nothing for that interval.
func (b *SummaryBuilder) interval(before, after int64) int64 {
	if after < before {
		b.counterResets++
		return 0
	}
	return after - before
}

func (b *SummaryBuilder) producedSummaries() []PartitionSummary {
	summaries := make([]PartitionSummary, 0, len(b.producedBy))
	var total int64
	for partition, count := range b.producedBy {
		total += count
		summaries = append(summaries, PartitionSummary{Partition: partition, Produced: count})
	}
	for i := range summaries {
		if total > 0 {
			summaries[i].Share = float64(summaries[i].Produced) / float64(total)
		}
	}
	slices.SortFunc(summaries, func(a, b PartitionSummary) int {
		return cmp.Compare(a.Partition, b.Partition)
	})
	return summaries
}

func hottest(partitions []PartitionSummary) *PartitionSummary {
	var best *PartitionSummary
	for i := range partitions {
		if partitions[i].Produced > 0 && (best == nil || partitions[i].Produced > best.Produced) {
			best = &partitions[i]
		}
	}
	return best
}

func totalProduced(partitions []PartitionSummary) int64 {
	var total int64
	for _, partition := range partitions {
		total += partition.Produced
	}
	return total
}

package sample

import (
	"fmt"
	"io"
	"sort"
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
	Elapsed          time.Duration

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
	first, last      time.Time

	firstPostgres, lastPostgres *Timed[PostgresSnapshot]
	peakBidRate                 float64
	peaks                       PostgresSnapshot

	firstKafka, lastKafka *Timed[KafkaSnapshot]
	peakGroupLag          map[string]int64
}

func NewSummaryBuilder() *SummaryBuilder {
	return &SummaryBuilder{peakGroupLag: map[string]int64{}}
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
		Elapsed:          b.last.Sub(b.first),
		PeakBidRate:      b.peakBidRate,

		PeakOutboxUnpublished:      b.peaks.OutboxUnpublished,
		PeakOutboxOldestAgeSeconds: b.peaks.OutboxOldestAgeSeconds,
		PeakOutboxRetrying:         b.peaks.OutboxRetrying,
		PeakLockWaiters:            b.peaks.LockWaiters,
		PeakIdleInTransaction:      b.peaks.IdleInTransaction,

		PeakGroupLag:  b.peakGroupLag,
		FinalGroupLag: map[string]int64{},
	}

	if b.firstPostgres != nil && b.lastPostgres != nil {
		span := b.lastPostgres.At.Sub(b.firstPostgres.At).Seconds()
		summary.BidsIssued = b.lastPostgres.Value.BidsIssued - b.firstPostgres.Value.BidsIssued
		summary.OrdersIssued = b.lastPostgres.Value.OrdersIssued - b.firstPostgres.Value.OrdersIssued
		if span > 0 && summary.BidsIssued >= 0 {
			summary.MeanBidRate = float64(summary.BidsIssued) / span
		}
	}

	if b.firstKafka != nil && b.lastKafka != nil {
		summary.FinalGroupLag = b.lastKafka.Value.GroupLag
		summary.Partitions = produced(b.firstKafka.Value, b.lastKafka.Value)
		summary.Hottest = hottest(summary.Partitions)
	}
	return summary
}

func (b *SummaryBuilder) addPostgres(o Observation) {
	reading := Timed[PostgresSnapshot]{At: o.At, Value: *o.Postgres}
	if b.firstPostgres == nil {
		b.firstPostgres = &reading
	}
	b.lastPostgres = &reading

	if o.BidRate != nil && *o.BidRate > b.peakBidRate {
		b.peakBidRate = *o.BidRate
	}
	b.peaks.OutboxUnpublished = max(b.peaks.OutboxUnpublished, o.Postgres.OutboxUnpublished)
	b.peaks.OutboxOldestAgeSeconds = max(b.peaks.OutboxOldestAgeSeconds, o.Postgres.OutboxOldestAgeSeconds)
	b.peaks.OutboxRetrying = max(b.peaks.OutboxRetrying, o.Postgres.OutboxRetrying)
	b.peaks.LockWaiters = max(b.peaks.LockWaiters, o.Postgres.LockWaiters)
	b.peaks.IdleInTransaction = max(b.peaks.IdleInTransaction, o.Postgres.IdleInTransaction)
}

func (b *SummaryBuilder) addKafka(o Observation) {
	reading := Timed[KafkaSnapshot]{At: o.At, Value: *o.Kafka}
	if b.firstKafka == nil {
		b.firstKafka = &reading
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
	fmt.Fprintf(w, "outbox: peak %d unpublished, oldest %.1fs, peak %d retrying\n",
		s.PeakOutboxUnpublished, s.PeakOutboxOldestAgeSeconds, s.PeakOutboxRetrying)
	fmt.Fprintf(w, "connections: peak %d waiting on a lock, peak %d idle in transaction\n",
		s.PeakLockWaiters, s.PeakIdleInTransaction)

	groups := make([]string, 0, len(s.PeakGroupLag))
	for group := range s.PeakGroupLag {
		groups = append(groups, group)
	}
	sort.Strings(groups)
	for _, group := range groups {
		fmt.Fprintf(w, "group %s: peak lag %d, final lag %d\n", group, s.PeakGroupLag[group], s.FinalGroupLag[group])
	}

	if s.Hottest != nil {
		fmt.Fprintf(w, "hottest partition: %d with %d records (%.0f%% of %d)\n",
			s.Hottest.Partition, s.Hottest.Produced, s.Hottest.Share*100, totalProduced(s.Partitions))
	}
}

func produced(first, last KafkaSnapshot) []PartitionSummary {
	before := make(map[int32]int64, len(first.Partitions))
	for _, partition := range first.Partitions {
		before[partition.Partition] = partition.EndOffset
	}

	var summaries []PartitionSummary
	var total int64
	for _, partition := range last.Partitions {
		startEnd, known := before[partition.Partition]
		if !known {
			continue
		}
		count := max(partition.EndOffset-startEnd, 0)
		total += count
		summaries = append(summaries, PartitionSummary{Partition: partition.Partition, Produced: count})
	}
	for i := range summaries {
		if total > 0 {
			summaries[i].Share = float64(summaries[i].Produced) / float64(total)
		}
	}
	sort.Slice(summaries, func(a, b int) bool { return summaries[a].Partition < summaries[b].Partition })
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

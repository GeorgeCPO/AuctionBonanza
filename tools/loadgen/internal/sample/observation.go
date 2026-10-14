// Package sample polls PostgreSQL and Kafka at a fixed interval, turns the
// readings into rates, and records them as CSV, so an experiment leaves numbers
// behind instead of impressions.
package sample

import "time"

// PostgresSnapshot is one reading of the database. Counters that come from
// sequences are ids issued, so they also count ids burned by transactions that
// rolled back; that makes them an upper bound on accepted bids, not a count.
type PostgresSnapshot struct {
	BidsIssued   int64
	OrdersIssued int64
	// OutboxUnpublished saturates at the collector's cap, so a value equal to it
	// means "at least that many".
	OutboxUnpublished      int64
	OutboxOldestAgeSeconds float64
	OutboxRetrying         int64
	LockWaiters            int64
	IdleInTransaction      int64
	Active                 int64

	// OutboxUnavailable is why the outbox fields could not be read, or empty when
	// they are valid. The outbox read is the one that grows with the backlog, so
	// it is the one most likely to time out, and it must not take the rest of
	// the snapshot with it.
	OutboxUnavailable string
}

// GroupProgress is one consumer group's position in one partition.
type GroupProgress struct {
	// Committed is the committed offset, or -1 when the group has committed
	// nothing in this partition.
	Committed int64
	// Lag is records the group has yet to consume. Without a commit it is the
	// whole partition, which assumes the group starts at the earliest offset.
	Lag int64
}

// PartitionSnapshot is one partition of the watched topic.
type PartitionSnapshot struct {
	Partition   int32
	StartOffset int64
	EndOffset   int64
	// Groups holds only groups that have committed somewhere in the topic.
	Groups map[string]GroupProgress
}

// KafkaSnapshot is one reading of the watched topic and its groups.
type KafkaSnapshot struct {
	Partitions []PartitionSnapshot
	// GroupLag totals each known group's lag across partitions. A group with no
	// commits at all is absent, not zero.
	GroupLag map[string]int64
}

// Timed pairs a reading with when it was taken.
type Timed[T any] struct {
	At    time.Time
	Value T
}

// Observation is one tick: what was read and the rates derived from the
// previous successful reading of each source. A nil snapshot or rate means
// unknown, and is written as an empty CSV cell.
type Observation struct {
	At       time.Time
	Postgres *PostgresSnapshot
	Kafka    *KafkaSnapshot

	BidRate      *float64
	OrderRate    *float64
	EventRate    *float64
	ProduceRates map[int32]float64
}

// Observe derives rates by comparing each fresh reading with the last good
// reading of the same source, each divided by the time between the moments the
// two were actually read. Using the last good one, not the last tick, keeps a
// rate honest across a gap, and using each reading's own time keeps it honest
// when a slow read delayed one source more than the other. A counter that went
// down (for example a database that was reset) gives no rate for that tick.
func Observe(at time.Time, postgres *Timed[PostgresSnapshot], kafka *Timed[KafkaSnapshot], previousPostgres *Timed[PostgresSnapshot], previousKafka *Timed[KafkaSnapshot]) Observation {
	observation := Observation{At: at}

	if postgres != nil {
		observation.Postgres = &postgres.Value
		if previousPostgres != nil {
			elapsed := postgres.At.Sub(previousPostgres.At).Seconds()
			observation.BidRate = rate(previousPostgres.Value.BidsIssued, postgres.Value.BidsIssued, elapsed)
			observation.OrderRate = rate(previousPostgres.Value.OrdersIssued, postgres.Value.OrdersIssued, elapsed)
		}
	}

	if kafka != nil {
		observation.Kafka = &kafka.Value
		if previousKafka != nil {
			observation.ProduceRates, observation.EventRate = produceRates(*previousKafka, *kafka)
		}
	}
	return observation
}

// produceRates are per-partition rates, and their sum when any partition has one.
func produceRates(previous, current Timed[KafkaSnapshot]) (map[int32]float64, *float64) {
	elapsed := current.At.Sub(previous.At).Seconds()
	before := make(map[int32]int64, len(previous.Value.Partitions))
	for _, partition := range previous.Value.Partitions {
		before[partition.Partition] = partition.EndOffset
	}

	rates := make(map[int32]float64, len(current.Value.Partitions))
	var total float64
	for _, partition := range current.Value.Partitions {
		previousEnd, known := before[partition.Partition]
		if !known {
			continue
		}
		partitionRate := rate(previousEnd, partition.EndOffset, elapsed)
		if partitionRate == nil {
			continue
		}
		rates[partition.Partition] = *partitionRate
		total += *partitionRate
	}
	if len(rates) == 0 {
		return rates, nil
	}
	return rates, &total
}

func rate(before, after int64, elapsedSeconds float64) *float64 {
	if elapsedSeconds <= 0 || after < before {
		return nil
	}
	perSecond := float64(after-before) / elapsedSeconds
	return &perSecond
}

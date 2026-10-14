// Package sample polls PostgreSQL and Kafka at a fixed interval, turns the
// readings into rates, and records them as CSV, so an experiment leaves numbers
// behind instead of impressions.
package sample

import "time"

// PostgresSnapshot is one reading of the database. Counters that come from
// sequences are ids issued, so they also count ids burned by transactions that
// rolled back; that makes them an upper bound on accepted bids, not a count.
type PostgresSnapshot struct {
	BidsIssued             int64
	OrdersIssued           int64
	OutboxUnpublished      int64
	OutboxOldestAgeSeconds float64
	OutboxRetrying         int64
	LockWaiters            int64
	IdleInTransaction      int64
	Active                 int64
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
// reading of the same source. Using the last good one, not the last tick, keeps
// a rate honest across a gap: the delta is divided by the time it really spans.
// A counter that went down (for example a database that was reset) gives no
// rate for that tick.
func Observe(at time.Time, postgres *PostgresSnapshot, kafka *KafkaSnapshot, previousPostgres *Timed[PostgresSnapshot], previousKafka *Timed[KafkaSnapshot]) Observation {
	observation := Observation{At: at, Postgres: postgres, Kafka: kafka}

	if postgres != nil && previousPostgres != nil {
		elapsed := at.Sub(previousPostgres.At).Seconds()
		observation.BidRate = rate(previousPostgres.Value.BidsIssued, postgres.BidsIssued, elapsed)
		observation.OrderRate = rate(previousPostgres.Value.OrdersIssued, postgres.OrdersIssued, elapsed)
	}

	if kafka != nil && previousKafka != nil {
		elapsed := at.Sub(previousKafka.At).Seconds()
		before := make(map[int32]int64, len(previousKafka.Value.Partitions))
		for _, partition := range previousKafka.Value.Partitions {
			before[partition.Partition] = partition.EndOffset
		}

		observation.ProduceRates = make(map[int32]float64, len(kafka.Partitions))
		var total float64
		var counted bool
		for _, partition := range kafka.Partitions {
			previousEnd, known := before[partition.Partition]
			if !known {
				continue
			}
			partitionRate := rate(previousEnd, partition.EndOffset, elapsed)
			if partitionRate == nil {
				continue
			}
			observation.ProduceRates[partition.Partition] = *partitionRate
			total += *partitionRate
			counted = true
		}
		if counted {
			observation.EventRate = &total
		}
	}
	return observation
}

func rate(before, after int64, elapsedSeconds float64) *float64 {
	if elapsedSeconds <= 0 || after < before {
		return nil
	}
	perSecond := float64(after-before) / elapsedSeconds
	return &perSecond
}

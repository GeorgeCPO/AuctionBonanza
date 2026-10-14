package sample

import (
	"math"
	"testing"
	"time"
)

var epoch = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

func kafkaAt(ends ...int64) *KafkaSnapshot {
	snapshot := &KafkaSnapshot{GroupLag: map[string]int64{}}
	for partition, end := range ends {
		snapshot.Partitions = append(snapshot.Partitions, PartitionSnapshot{Partition: int32(partition), EndOffset: end})
	}
	return snapshot
}

func postgresAt(at time.Time, snapshot PostgresSnapshot) *Timed[PostgresSnapshot] {
	return &Timed[PostgresSnapshot]{At: at, Value: snapshot}
}

func timedKafka(at time.Time, ends ...int64) *Timed[KafkaSnapshot] {
	return &Timed[KafkaSnapshot]{At: at, Value: *kafkaAt(ends...)}
}

func near(t *testing.T, name string, got *float64, want float64) {
	t.Helper()
	if got == nil {
		t.Errorf("%s is unknown, want %.3f", name, want)
		return
	}
	difference := math.Abs(*got - want)
	if difference > 1e-9 {
		t.Errorf("%s = %.3f, want %.3f", name, *got, want)
	}
}

func TestObserveDerivesRatesFromTheLastGoodReadings(t *testing.T) {
	previousPostgres := &Timed[PostgresSnapshot]{At: epoch, Value: PostgresSnapshot{BidsIssued: 100, OrdersIssued: 10}}
	previousKafka := &Timed[KafkaSnapshot]{At: epoch, Value: *kafkaAt(1000, 2000)}

	observation := Observe(epoch.Add(2*time.Second),
		postgresAt(epoch.Add(2*time.Second), PostgresSnapshot{BidsIssued: 300, OrdersIssued: 14}),
		timedKafka(epoch.Add(2*time.Second), 1100, 2500),
		previousPostgres, previousKafka)

	near(t, "bid rate", observation.BidRate, 100)
	near(t, "order rate", observation.OrderRate, 2)
	near(t, "partition 0", ptr(observation.ProduceRates[0]), 50)
	near(t, "partition 1", ptr(observation.ProduceRates[1]), 250)
	near(t, "event rate", observation.EventRate, 300)
}

func TestObserveHasNoRatesOnTheFirstReading(t *testing.T) {
	observation := Observe(epoch, postgresAt(epoch, PostgresSnapshot{BidsIssued: 5}), timedKafka(epoch, 10), nil, nil)

	if observation.BidRate != nil || observation.OrderRate != nil || observation.EventRate != nil {
		t.Errorf("first reading produced rates: %+v", observation)
	}
	if len(observation.ProduceRates) != 0 {
		t.Errorf("first reading produced partition rates: %v", observation.ProduceRates)
	}
}

func TestObserveSpansAGapWithTheRealElapsedTime(t *testing.T) {
	// Postgres failed for three ticks, so its last good reading is 4 s old.
	previous := &Timed[PostgresSnapshot]{At: epoch, Value: PostgresSnapshot{BidsIssued: 0}}

	observation := Observe(epoch.Add(4*time.Second), postgresAt(epoch.Add(4*time.Second), PostgresSnapshot{BidsIssued: 400}), nil, previous, nil)

	near(t, "bid rate", observation.BidRate, 100)
}

func TestObserveLeavesAFailedSourceUnknown(t *testing.T) {
	previous := &Timed[PostgresSnapshot]{At: epoch, Value: PostgresSnapshot{BidsIssued: 1}}

	observation := Observe(epoch.Add(time.Second), nil, nil, previous, nil)

	if observation.Postgres != nil || observation.Kafka != nil || observation.BidRate != nil {
		t.Errorf("a failed read left values behind: %+v", observation)
	}
}

func TestObserveGivesNoRateWhenACounterGoesDown(t *testing.T) {
	previousPostgres := &Timed[PostgresSnapshot]{At: epoch, Value: PostgresSnapshot{BidsIssued: 500}}
	previousKafka := &Timed[KafkaSnapshot]{At: epoch, Value: *kafkaAt(900, 100)}

	at := epoch.Add(time.Second)
	observation := Observe(at, postgresAt(at, PostgresSnapshot{BidsIssued: 3}), timedKafka(at, 10, 150), previousPostgres, previousKafka)

	if observation.BidRate != nil {
		t.Error("a database reset produced a negative bid rate")
	}
	if _, reset := observation.ProduceRates[0]; reset {
		t.Error("a topic recreated with lower offsets produced a partition rate")
	}
	near(t, "partition 1 still counts", ptr(observation.ProduceRates[1]), 50)
	near(t, "event rate counts only valid partitions", observation.EventRate, 50)
}

func TestObserveIgnoresPartitionsItHasNotSeenBefore(t *testing.T) {
	previousKafka := &Timed[KafkaSnapshot]{At: epoch, Value: *kafkaAt(10)}

	at := epoch.Add(time.Second)
	observation := Observe(at, nil, timedKafka(at, 20, 99), nil, previousKafka)

	if len(observation.ProduceRates) != 1 {
		t.Errorf("rates for %v, want only the partition seen before", observation.ProduceRates)
	}
}

func TestObserveUsesTheTimeEachSourceWasActuallyRead(t *testing.T) {
	// The tick started at 1 s, but Postgres answered at 2 s and Kafka at 1.5 s.
	previousPostgres := &Timed[PostgresSnapshot]{At: epoch, Value: PostgresSnapshot{BidsIssued: 0}}
	previousKafka := &Timed[KafkaSnapshot]{At: epoch, Value: *kafkaAt(0)}

	observation := Observe(epoch.Add(time.Second),
		postgresAt(epoch.Add(2*time.Second), PostgresSnapshot{BidsIssued: 200}),
		timedKafka(epoch.Add(1500*time.Millisecond), 300),
		previousPostgres, previousKafka)

	near(t, "bid rate over Postgres's own 2 s", observation.BidRate, 100)
	near(t, "event rate over Kafka's own 1.5 s", observation.EventRate, 200)
	if !observation.At.Equal(epoch.Add(time.Second)) {
		t.Errorf("the row is stamped %s, want the tick start", observation.At)
	}
}

func TestObserveHandlesNoElapsedTime(t *testing.T) {
	previous := &Timed[PostgresSnapshot]{At: epoch, Value: PostgresSnapshot{BidsIssued: 1}}

	observation := Observe(epoch, postgresAt(epoch, PostgresSnapshot{BidsIssued: 9}), nil, previous, nil)

	if observation.BidRate != nil {
		t.Error("a zero interval produced a rate")
	}
}

func ptr(value float64) *float64 {
	return &value
}

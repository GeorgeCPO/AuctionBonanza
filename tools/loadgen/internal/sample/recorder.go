package sample

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"time"
)

// CSVRecorder writes one row per tick to the samples file and one row per
// partition per tick to the partitions file. An unknown value is an empty cell,
// so a gap in the data (a broker down, a query timed out) shows up as a gap.
// Every row is flushed, so `tail -f` follows a live run.
type CSVRecorder struct {
	samples    *csv.Writer
	partitions *csv.Writer
	groups     []string
}

// NewCSVRecorder writes both headers. Group columns follow the order given.
func NewCSVRecorder(samples, partitions io.Writer, groups []string) (*CSVRecorder, error) {
	recorder := &CSVRecorder{
		samples:    csv.NewWriter(samples),
		partitions: csv.NewWriter(partitions),
		groups:     groups,
	}

	samplesHeader := []string{
		"time", "bids_issued", "bids_per_sec", "orders_issued", "orders_per_sec",
		"outbox_unpublished", "outbox_oldest_age_s", "outbox_retrying",
		"lock_waiters", "idle_in_transaction", "active",
		"events_per_sec",
	}
	partitionsHeader := []string{"time", "partition", "start_offset", "end_offset", "produce_per_sec"}
	for _, group := range groups {
		samplesHeader = append(samplesHeader, "lag_"+group)
		partitionsHeader = append(partitionsHeader, "committed_"+group, "lag_"+group)
	}

	err := recorder.samples.Write(samplesHeader)
	if err != nil {
		return nil, err
	}
	err = recorder.partitions.Write(partitionsHeader)
	if err != nil {
		return nil, err
	}
	return recorder, recorder.flush()
}

func (r *CSVRecorder) Record(o Observation) error {
	at := o.At.UTC().Format(time.RFC3339Nano)

	err := r.samples.Write(r.samplesRow(at, o))
	if err != nil {
		return err
	}
	if o.Kafka != nil {
		for _, partition := range o.Kafka.Partitions {
			err = r.partitions.Write(r.partitionRow(at, o, partition))
			if err != nil {
				return err
			}
		}
	}
	return r.flush()
}

func (r *CSVRecorder) samplesRow(at string, o Observation) []string {
	row := []string{at}
	if o.Postgres == nil {
		// bids, bids rate, orders, orders rate, 3 outbox, 3 connection counts
		row = append(row, make([]string, 10)...)
	} else {
		p := o.Postgres
		row = append(row,
			integer(p.BidsIssued), optional(o.BidRate),
			integer(p.OrdersIssued), optional(o.OrderRate),
			integer(p.OutboxUnpublished), decimal(p.OutboxOldestAgeSeconds), integer(p.OutboxRetrying),
			integer(p.LockWaiters), integer(p.IdleInTransaction), integer(p.Active),
		)
	}
	row = append(row, optional(o.EventRate))

	for _, group := range r.groups {
		row = append(row, groupLag(o, group))
	}
	return row
}

func (r *CSVRecorder) partitionRow(at string, o Observation, partition PartitionSnapshot) []string {
	row := []string{
		at,
		strconv.Itoa(int(partition.Partition)),
		integer(partition.StartOffset),
		integer(partition.EndOffset),
		"",
	}
	produceRate, known := o.ProduceRates[partition.Partition]
	if known {
		row[4] = decimal(produceRate)
	}

	for _, group := range r.groups {
		progress, committed := partition.Groups[group]
		if !committed {
			row = append(row, "", "")
			continue
		}
		commit := ""
		if progress.Committed >= 0 {
			commit = integer(progress.Committed)
		}
		row = append(row, commit, integer(progress.Lag))
	}
	return row
}

func groupLag(o Observation, group string) string {
	if o.Kafka == nil {
		return ""
	}
	lag, known := o.Kafka.GroupLag[group]
	if !known {
		return ""
	}
	return integer(lag)
}

func (r *CSVRecorder) flush() error {
	r.samples.Flush()
	r.partitions.Flush()
	err := r.samples.Error()
	if err != nil {
		return fmt.Errorf("write samples: %w", err)
	}
	err = r.partitions.Error()
	if err != nil {
		return fmt.Errorf("write partitions: %w", err)
	}
	return nil
}

func integer(value int64) string {
	return strconv.FormatInt(value, 10)
}

func decimal(value float64) string {
	return strconv.FormatFloat(value, 'f', 3, 64)
}

func optional(value *float64) string {
	if value == nil {
		return ""
	}
	return decimal(*value)
}

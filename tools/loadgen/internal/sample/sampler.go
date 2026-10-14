package sample

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// PostgresSource and KafkaSource are what the Sampler reads from.
type PostgresSource interface {
	Collect(ctx context.Context) (PostgresSnapshot, error)
}

type KafkaSource interface {
	Collect(ctx context.Context) (KafkaSnapshot, error)
}

// Recorder receives every observation.
type Recorder interface {
	Record(o Observation) error
}

// SamplerOptions tune a Sampler. Interval and ReadTimeout are required.
type SamplerOptions struct {
	// Interval is the time between ticks. A tick that overruns it delays the
	// next one rather than overlapping it.
	Interval time.Duration
	// ReadTimeout bounds one tick's reads. It is independent of Interval and
	// should be generous: a loaded system answers slowly exactly when the data
	// matters most, and a timed-out read leaves a gap.
	ReadTimeout time.Duration
	// Now is the clock. Default time.Now.
	Now    func() time.Time
	Logger *slog.Logger
}

// Sampler polls both sources every interval. It keeps the last good reading of
// each so rates can be derived, and it emits a row every tick even when a source
// fails: an experiment that takes a broker down must still show the gap.
type Sampler struct {
	postgres PostgresSource
	kafka    KafkaSource
	recorder Recorder
	summary  *SummaryBuilder
	options  SamplerOptions

	lastPostgres *Timed[PostgresSnapshot]
	lastKafka    *Timed[KafkaSnapshot]
}

func NewSampler(postgres PostgresSource, kafka KafkaSource, recorder Recorder, options SamplerOptions) *Sampler {
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Logger == nil {
		options.Logger = slog.New(slog.DiscardHandler)
	}
	return &Sampler{
		postgres: postgres,
		kafka:    kafka,
		recorder: recorder,
		summary:  NewSummaryBuilder(),
		options:  options,
	}
}

// Run samples until the context ends and returns the summary of the run. A
// failed read is logged and leaves its cells empty; a failed write stops the
// run, because data that cannot be recorded is not worth collecting.
func (s *Sampler) Run(ctx context.Context) (Summary, error) {
	if s.options.Interval <= 0 || s.options.ReadTimeout <= 0 {
		return Summary{}, errors.New("interval and read timeout must be positive")
	}

	ticker := time.NewTicker(s.options.Interval)
	defer ticker.Stop()

	for {
		err := s.tick(ctx)
		if err != nil {
			return s.summary.Build(), err
		}
		select {
		case <-ctx.Done():
			return s.summary.Build(), nil
		case <-ticker.C:
		}
	}
}

func (s *Sampler) tick(ctx context.Context) error {
	stopped := ctx.Err()
	if stopped != nil {
		return nil
	}
	at := s.options.Now()
	postgres, kafka := s.collect(ctx)
	stopped = ctx.Err()
	if stopped != nil {
		// The run ended while reading; what came back is incomplete.
		return nil
	}

	observation := Observe(at, postgres, kafka, s.lastPostgres, s.lastKafka)
	if postgres != nil {
		s.lastPostgres = postgres
	}
	if kafka != nil {
		s.lastKafka = kafka
	}

	s.summary.Add(observation)
	return s.recorder.Record(observation)
}

// collect reads both sources at the same time. Each reading is stamped when its
// own read finishes, so a slow source does not skew the other's rates. A nil
// result means that source failed this tick.
func (s *Sampler) collect(ctx context.Context) (*Timed[PostgresSnapshot], *Timed[KafkaSnapshot]) {
	readCtx, cancel := context.WithTimeout(ctx, s.options.ReadTimeout)
	defer cancel()

	var postgres *Timed[PostgresSnapshot]
	var kafka *Timed[KafkaSnapshot]
	var readers sync.WaitGroup
	readers.Add(2)

	go func() {
		defer readers.Done()
		snapshot, err := s.postgres.Collect(readCtx)
		if err != nil {
			s.logFailure(ctx, "postgres", err)
			return
		}
		postgres = &Timed[PostgresSnapshot]{At: s.options.Now(), Value: snapshot}
		if snapshot.OutboxUnavailable != "" {
			s.logFailure(ctx, "postgres outbox", errors.New(snapshot.OutboxUnavailable))
		}
	}()
	go func() {
		defer readers.Done()
		snapshot, err := s.kafka.Collect(readCtx)
		if err != nil {
			s.logFailure(ctx, "kafka", err)
			return
		}
		kafka = &Timed[KafkaSnapshot]{At: s.options.Now(), Value: snapshot}
	}()

	readers.Wait()
	return postgres, kafka
}

func (s *Sampler) logFailure(ctx context.Context, source string, err error) {
	// A read cut short by the user stopping the run is not a failure.
	stopped := ctx.Err()
	if stopped != nil {
		return
	}
	s.options.Logger.Warn("sample read failed", "source", source, "error", err)
}

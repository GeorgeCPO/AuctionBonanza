package sample

import (
	"context"
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

// SamplerOptions tune a Sampler.
type SamplerOptions struct {
	Interval time.Duration
	// ReadTimeout bounds one collection. Default: the interval, at most 5 s.
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
	if options.Interval <= 0 {
		options.Interval = time.Second
	}
	if options.ReadTimeout <= 0 {
		options.ReadTimeout = min(options.Interval, 5*time.Second)
	}
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
		s.lastPostgres = &Timed[PostgresSnapshot]{At: at, Value: *postgres}
	}
	if kafka != nil {
		s.lastKafka = &Timed[KafkaSnapshot]{At: at, Value: *kafka}
	}

	s.summary.Add(observation)
	return s.recorder.Record(observation)
}

// collect reads both sources at the same time, so a slow one does not delay the
// other's timestamp. A nil result means that source failed this tick.
func (s *Sampler) collect(ctx context.Context) (*PostgresSnapshot, *KafkaSnapshot) {
	readCtx, cancel := context.WithTimeout(ctx, s.options.ReadTimeout)
	defer cancel()

	var postgres *PostgresSnapshot
	var kafka *KafkaSnapshot
	var readers sync.WaitGroup
	readers.Add(2)

	go func() {
		defer readers.Done()
		snapshot, err := s.postgres.Collect(readCtx)
		if err != nil {
			s.logFailure(ctx, "postgres", err)
			return
		}
		postgres = &snapshot
	}()
	go func() {
		defer readers.Done()
		snapshot, err := s.kafka.Collect(readCtx)
		if err != nil {
			s.logFailure(ctx, "kafka", err)
			return
		}
		kafka = &snapshot
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

package sample

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type scriptedPostgres struct {
	mu      sync.Mutex
	calls   int
	bids    int64
	failAt  map[int]bool
	onCall  func(call int)
	perCall int64
}

func (s *scriptedPostgres) Collect(context.Context) (PostgresSnapshot, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()
	if s.onCall != nil {
		s.onCall(call)
	}
	if s.failAt[call] {
		return PostgresSnapshot{}, errors.New("connection refused")
	}
	return PostgresSnapshot{BidsIssued: int64(call) * s.perCall}, nil
}

type fixedKafka struct {
	err error
}

func (f fixedKafka) Collect(context.Context) (KafkaSnapshot, error) {
	if f.err != nil {
		return KafkaSnapshot{}, f.err
	}
	return *kafkaAt(10), nil
}

type collectingRecorder struct {
	observations []Observation
	recorded     atomic.Int64
	failAfter    int
}

func (c *collectingRecorder) Record(o Observation) error {
	c.observations = append(c.observations, o)
	c.recorded.Add(1)
	if c.failAfter > 0 && len(c.observations) >= c.failAfter {
		return errors.New("disk full")
	}
	return nil
}

// tickClock reads one second later after every recorded tick and the same
// within a tick, so a rate equals the counter's change per tick however many
// times the sampler asks for the time.
func tickClock(recorder *collectingRecorder) func() time.Time {
	return func() time.Time {
		return epoch.Add(time.Duration(recorder.recorded.Load()+1) * time.Second)
	}
}

func options(recorder *collectingRecorder) SamplerOptions {
	return SamplerOptions{Interval: time.Millisecond, ReadTimeout: time.Second, Now: tickClock(recorder)}
}

func TestSamplerRecordsATickPerReadingAndDerivesRates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	postgres := &scriptedPostgres{perCall: 100, onCall: func(call int) {
		if call == 4 {
			cancel()
		}
	}}
	recorder := &collectingRecorder{}
	sampler := NewSampler(postgres, fixedKafka{}, recorder, options(recorder))

	summary, err := sampler.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// The fourth reading is cut off by the shutdown and must not be recorded.
	if len(recorder.observations) != 3 || summary.Ticks != 3 {
		t.Fatalf("recorded %d ticks (summary %d), want 3", len(recorder.observations), summary.Ticks)
	}
	if recorder.observations[0].BidRate != nil {
		t.Error("the first tick has nothing to compare with")
	}
	near(t, "second tick bid rate", recorder.observations[1].BidRate, 100)
	if summary.PostgresFailures != 0 {
		t.Errorf("shutdown counted as %d failures", summary.PostgresFailures)
	}
}

func TestSamplerKeepsGoingWhenASourceFailsAndRatesSpanTheGap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	postgres := &scriptedPostgres{perCall: 100, failAt: map[int]bool{2: true, 3: true}, onCall: func(call int) {
		if call == 5 {
			cancel()
		}
	}}
	recorder := &collectingRecorder{}
	sampler := NewSampler(postgres, fixedKafka{}, recorder, options(recorder))

	summary, err := sampler.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(recorder.observations) != 4 {
		t.Fatalf("recorded %d ticks, want 4 (a failed read still gets a row)", len(recorder.observations))
	}
	if recorder.observations[1].Postgres != nil || recorder.observations[2].Postgres != nil {
		t.Error("failed reads left values behind")
	}
	if recorder.observations[1].Kafka == nil {
		t.Error("a Postgres failure blanked the Kafka side")
	}
	// Call 4 returns 400; the last good reading was call 1 (100), three seconds earlier.
	near(t, "rate across the gap", recorder.observations[3].BidRate, 100)
	if summary.PostgresFailures != 2 || summary.KafkaFailures != 0 {
		t.Errorf("failures: postgres %d kafka %d, want 2 and 0", summary.PostgresFailures, summary.KafkaFailures)
	}
}

func TestSamplerStopsWhenTheRecorderFails(t *testing.T) {
	recorder := &collectingRecorder{failAfter: 2}
	sampler := NewSampler(&scriptedPostgres{perCall: 1}, fixedKafka{}, recorder, options(recorder))

	summary, err := sampler.Run(context.Background())

	if err == nil {
		t.Fatal("expected the write error to stop the run")
	}
	if summary.Ticks != 2 {
		t.Errorf("summary covers %d ticks, want the 2 recorded before the failure", summary.Ticks)
	}
}

func TestSamplerReturnsImmediatelyForAnEndedContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	recorder := &collectingRecorder{}
	sampler := NewSampler(&scriptedPostgres{}, fixedKafka{}, recorder, options(recorder))

	summary, err := sampler.Run(ctx)

	if err != nil || summary.Ticks != 0 || len(recorder.observations) != 0 {
		t.Fatalf("an ended context still sampled: err=%v ticks=%d", err, summary.Ticks)
	}
}

func TestSamplerRejectsMissingTimings(t *testing.T) {
	recorder := &collectingRecorder{}
	cases := map[string]SamplerOptions{
		"no interval":     {ReadTimeout: time.Second},
		"no read timeout": {Interval: time.Second},
	}

	for name, timings := range cases {
		sampler := NewSampler(&scriptedPostgres{}, fixedKafka{}, recorder, timings)

		_, err := sampler.Run(context.Background())

		if err == nil || len(recorder.observations) != 0 {
			t.Errorf("%s: ran anyway (err %v); a zero interval would panic the ticker", name, err)
		}
	}
}

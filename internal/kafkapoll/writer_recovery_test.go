package kafkapoll

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"gigaquizz/internal/poll"
	"gigaquizz/internal/votelog"
)

type recoveryWriter struct {
	seal    func(context.Context) (votelog.Manifest, error)
	close   func()
	metrics map[string]uint64
	submits atomic.Uint64
}

func (w *recoveryWriter) Submit(context.Context, [16]byte, uint32) (votelog.Receipt, error) {
	w.submits.Add(1)
	return votelog.Receipt{}, errors.New("recovery must never resubmit an attempt")
}
func (w *recoveryWriter) Seal(ctx context.Context) (votelog.Manifest, error) { return w.seal(ctx) }
func (w *recoveryWriter) Close() {
	if w.close != nil {
		w.close()
	}
}
func (w *recoveryWriter) Metrics() map[string]uint64 { return w.metrics }

func TestFinalizationRecoveryDrainsThenClosesBeforeFencedReplacement(t *testing.T) {
	var events []string
	manifest := votelog.Manifest{Partitions: []votelog.PartitionEnd{{Partition: 0, Offset: 9}, {Partition: 1, Offset: 17}}}
	old := &recoveryWriter{metrics: map[string]uint64{"failed_writers": 1, "committed_attempts": 12, "transactions": 4}}
	old.seal = func(context.Context) (votelog.Manifest, error) {
		events = append(events, "drain healthy partitions")
		return votelog.Manifest{}, votelog.ErrUnknown
	}
	old.close = func() { events = append(events, "join old producers") }
	next := &recoveryWriter{metrics: map[string]uint64{}}
	next.seal = func(context.Context) (votelog.Manifest, error) {
		events = append(events, "seal recovered journal")
		return manifest, nil
	}
	e := checkpointEntry()
	e.writer = old
	e.admissionClosed.Store(true)
	s := &Store{polls: map[string]*entry{e.poll.ID: e}}
	originalHash := e.config.DefinitionHash()
	owner := func(context.Context) error { events = append(events, "check owner"); return nil }
	prepare := func(_ context.Context, e *entry) error {
		events = append(events, "fenced prepare")
		if e.writer != nil || !e.admissionClosed.Load() || e.config.DefinitionHash() != originalHash {
			t.Fatal("replacement changed definition/admission or overlapped an old writer")
		}
		s.mu.Lock()
		e.writer = next
		s.mu.Unlock()
		return nil
	}
	got, err := sealWithRecovery(context.Background(), old, func(ctx context.Context, failed journalWriter) (journalWriter, error) {
		return s.replaceFailedWriter(ctx, e, failed, owner, prepare)
	})
	if err != nil || !reflect.DeepEqual(got, manifest) {
		t.Fatal("finalization did not recover", got, err)
	}
	want := []string{"drain healthy partitions", "check owner", "join old producers", "check owner", "fenced prepare", "seal recovered journal"}
	if !reflect.DeepEqual(events, want) {
		t.Fatal("unsafe epoch transition", events)
	}
	if old.submits.Load()+next.submits.Load() != 0 {
		t.Fatal("recovery fabricated a fresh admission")
	}
	metrics := s.Diagnostics()
	if metrics["storage_durable_votes"] != 12 || metrics["storage_batches"] != 4 || metrics["storage_failed_writers"] != 0 {
		t.Fatal("retired counters lost or failed gauge persisted", metrics)
	}
}

func TestFinalizationRecoveryDoesNotReplaceHealthyOrCancelledEpoch(t *testing.T) {
	for _, test := range []string{"healthy", "cancelled", "nonterminal-error"} {
		t.Run(test, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			w := &recoveryWriter{metrics: map[string]uint64{"failed_writers": 1}}
			w.seal = func(context.Context) (votelog.Manifest, error) {
				switch test {
				case "healthy":
					return votelog.Manifest{}, nil
				case "cancelled":
					cancel()
					return votelog.Manifest{}, context.Canceled
				default:
					return votelog.Manifest{}, context.DeadlineExceeded
				}
			}
			if test == "nonterminal-error" {
				w.metrics["failed_writers"] = 0
			}
			_, err := sealWithRecovery(ctx, w, func(context.Context, journalWriter) (journalWriter, error) {
				t.Fatal("unexpected epoch replacement")
				return nil, nil
			})
			if (test == "healthy") != (err == nil) {
				t.Fatal("original outcome changed", err)
			}
		})
	}
}

func TestFinalizationRecoveryIsBoundedWhenReplacementAlsoFails(t *testing.T) {
	w := &recoveryWriter{metrics: map[string]uint64{"failed_writers": 1}, seal: func(context.Context) (votelog.Manifest, error) { return votelog.Manifest{}, votelog.ErrUnknown }}
	next := &recoveryWriter{metrics: map[string]uint64{"failed_writers": 1}, seal: w.seal}
	calls := 0
	_, err := sealWithRecovery(context.Background(), w, func(context.Context, journalWriter) (journalWriter, error) { calls++; return next, nil })
	if !errors.Is(err, votelog.ErrUnknown) || calls != 1 {
		t.Fatal("maintenance retried without a bound", calls, err)
	}
}

func TestRepeatedEpochReplacementRetainsCountersExactlyOnce(t *testing.T) {
	e := checkpointEntry()
	e.admissionClosed.Store(true)
	s := &Store{polls: map[string]*entry{e.poll.ID: e}}
	owner := func(context.Context) error { return nil }
	for epoch, attempts := range []uint64{12, 3} {
		failed := &recoveryWriter{metrics: map[string]uint64{"failed_writers": 1, "committed_attempts": attempts, "writer_failures_network": 1}}
		e.writer = failed
		next := &recoveryWriter{metrics: map[string]uint64{}}
		if _, err := s.replaceFailedWriter(context.Background(), e, failed, owner, func(context.Context, *entry) error {
			s.mu.Lock()
			e.writer = next
			s.mu.Unlock()
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.replaceFailedWriter(context.Background(), e, failed, owner, func(context.Context, *entry) error {
			t.Fatal("retired epoch replaced twice")
			return nil
		}); err == nil {
			t.Fatal("stale old epoch was retired twice")
		}
		if s.Diagnostics()["storage_writer_failures_network"] != uint64(epoch+1) {
			t.Fatal("retired failure causes were lost or counted twice")
		}
	}
	if s.Diagnostics()["storage_durable_votes"] != 15 || s.Diagnostics()["storage_failed_writers"] != 0 {
		t.Fatal("ACK counters changed across multiple replacements")
	}
}

func TestFailedWriterReplacementNeverReacquiresAfterOwnershipLossOrCancellation(t *testing.T) {
	for _, test := range []string{"open", "owner-before-close", "owner-during-close", "cancel-during-close", "prepare-fails"} {
		t.Run(test, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			e := checkpointEntry()
			e.admissionClosed.Store(test != "open")
			s := &Store{polls: map[string]*entry{e.poll.ID: e}}
			closed, ownerCalls, prepared := 0, 0, 0
			w := &recoveryWriter{metrics: map[string]uint64{"failed_writers": 1, "committed_attempts": 7}}
			w.close = func() {
				closed++
				if test == "cancel-during-close" {
					cancel()
				}
			}
			e.writer = w
			owner := func(context.Context) error {
				ownerCalls++
				if test == "owner-before-close" || (test == "owner-during-close" && closed > 0) {
					return ErrOwnership
				}
				return nil
			}
			prepare := func(context.Context, *entry) error { prepared++; return errors.New("temporary coordinator failure") }
			if _, err := s.replaceFailedWriter(ctx, e, w, owner, prepare); err == nil {
				t.Fatal("failed transition became successful")
			}
			if test == "open" || test == "owner-before-close" {
				if closed != 0 || prepared != 0 || e.writer != w {
					t.Fatal("changed writer before safe close")
				}
			} else {
				if closed != 1 || e.writer != nil || s.Diagnostics()["storage_durable_votes"] != 7 {
					t.Fatal("did not retire closed epoch exactly once")
				}
				if (prepared == 1) != (test == "prepare-fails") {
					t.Fatal("reacquired after ownership loss/cancellation")
				}
			}
		})
	}
}

func TestReplacementKeepsAdmissionClosedWhileWaitingForOldProducer(t *testing.T) {
	id := "10000000-0000-4000-8000-000000000001"
	now := time.Now()
	e := &entry{poll: poll.Poll{ID: id, Type: "single", StartsAt: now.Add(-time.Minute), EndsAt: now, Options: []poll.Option{{ID: 1}}}}
	e.admissionClosed.Store(true)
	s := &Store{ctx: context.Background(), polls: map[string]*entry{id: e}, clock: func() time.Time { return now.Add(-time.Second) }}
	entered, release := make(chan struct{}), make(chan struct{})
	w := &recoveryWriter{metrics: map[string]uint64{"failed_writers": 1}, close: func() { close(entered); <-release }}
	e.writer = w
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := s.replaceFailedWriter(ctx, e, w, func(context.Context) error { return nil }, func(context.Context, *entry) error { return errors.New("do not reopen") })
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("old writer did not start closing")
	}
	_, err := s.Get(context.Background(), id)
	r, voteErr := s.Vote(context.Background(), id, "20000000000040008000000000000001", []int{1})
	_ = s.Diagnostics()
	cancel()
	close(release)
	if got := <-done; !errors.Is(got, context.Canceled) {
		t.Fatal("cancelled replacement resumed", got)
	}
	if err != nil || voteErr != nil || r.Status != "closed" || w.submits.Load() != 0 {
		t.Fatal("rolled-back clock reopened admission while closing", r, err, voteErr)
	}
}

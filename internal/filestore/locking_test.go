package filestore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"gigaquizz/internal/filelog"
	"gigaquizz/internal/poll"
)

func finishWithoutRecovery[T any](t *testing.T, done <-chan T) T {
	t.Helper()
	select {
	case value := <-done:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("operation waited for unrelated historical recovery/diagnostics")
		var zero T
		return zero
	}
}

func TestHistoricalRecoveryDoesNotHoldEntryOrRegistryLocks(t *testing.T) {
	s := openTest(t, configForTest(t))
	start := time.Now().Add(-2 * time.Minute)
	old, err := s.Create(context.Background(), poll.CreateInput{Question: "history", Type: "ab", Options: []string{"A", "B"}, StartsAt: &start})
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	s.recoverFinalization = func(ctx context.Context, e *entry) (journalWriter, error) {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return e.recoverWriter(ctx)
	}
	finalized := make(chan error, 1)
	go func() { _, err := s.FinalizeDue(context.Background()); finalized <- err }()
	finishWithoutRecovery(t, entered)
	// The recovery call is still stopped. This read needs that same entry mu.
	read := make(chan error, 1)
	go func() {
		r, err := s.Results(context.Background(), old.ID)
		if err == nil && !r.Pending {
			err = errors.New("unverified history exposed final")
		}
		read <- err
	}()
	if err := finishWithoutRecovery(t, read); err != nil {
		t.Fatal(err)
	}
	diagnostics := make(chan map[string]uint64, 1)
	go func() { diagnostics <- s.Diagnostics() }()
	finishWithoutRecovery(t, diagnostics)
	created := make(chan error, 1)
	go func() {
		p, err := s.Create(context.Background(), poll.CreateInput{Question: "current", Type: "ab", Options: []string{"A", "B"}})
		if err == nil {
			var r poll.Receipt
			r, err = s.Vote(context.Background(), p.ID, tokenA, []int{1})
			if err == nil && r.Status != "recorded" {
				err = errors.New("healthy current vote was not recorded")
			}
		}
		if err == nil {
			err = s.Ping(context.Background())
		}
		created <- err
	}()
	if err := finishWithoutRecovery(t, created); err != nil {
		t.Fatal(err)
	}
	// Once finalization observed expiry, a later clock rollback cannot race a
	// second recovery/Vote against the finalizer's ownership acquisition.
	s.now = func() time.Time { return old.StartsAt.Add(time.Second) }
	if r, err := s.Vote(context.Background(), old.ID, tokenB, []int{1}); err != nil || r.Status != "closed" {
		t.Fatalf("finalizing poll reopened: %+v %v", r, err)
	}
	s.now = time.Now
	unblock()
	if err := finishWithoutRecovery(t, finalized); err != nil {
		t.Fatal(err)
	}
	result, err := s.Results(context.Background(), old.ID)
	if err != nil || result.Pending || result.TotalVotes != 0 {
		t.Fatalf("recovery did not finish correctly: %+v %v", result, err)
	}
}

type heldMetrics struct{ entered, release chan struct{} }

func (w *heldMetrics) Submit(context.Context, filelog.Input) (filelog.Receipt, error) {
	return filelog.Receipt{}, filelog.ErrClosed
}
func (w *heldMetrics) Seal(context.Context) (filelog.Manifest, error) {
	return filelog.Manifest{}, filelog.ErrClosed
}
func (w *heldMetrics) Close() {}
func (w *heldMetrics) Metrics() map[string]uint64 {
	close(w.entered)
	<-w.release
	return map[string]uint64{"durable_votes": 7}
}

func TestSlowDiagnosticsNeverPinsRegistryForCreateOrClose(t *testing.T) {
	for _, operation := range []string{"create", "close"} {
		t.Run(operation, func(t *testing.T) {
			s := openTest(t, configForTest(t))
			start := time.Now().Add(-2 * time.Minute)
			p, err := s.Create(context.Background(), poll.CreateInput{Question: "old", Type: "ab", Options: []string{"A", "B"}, StartsAt: &start})
			if err != nil {
				t.Fatal(err)
			}
			w := &heldMetrics{make(chan struct{}), make(chan struct{})}
			var once sync.Once
			unblock := func() { once.Do(func() { close(w.release) }) }
			defer unblock()
			s.lookup(p.ID).writer = w // configured before the diagnostic goroutine
			diagnostics := make(chan map[string]uint64, 1)
			go func() { diagnostics <- s.Diagnostics() }()
			finishWithoutRecovery(t, w.entered) // the real Metrics call is now blocked
			done := make(chan error, 1)
			go func() {
				if operation == "close" {
					s.Close()
					done <- nil
					return
				}
				_, err := s.Create(context.Background(), poll.CreateInput{Question: "fresh", Type: "ab", Options: []string{"A", "B"}})
				if err == nil {
					err = s.Ping(context.Background())
				}
				done <- err
			}()
			if err := finishWithoutRecovery(t, done); err != nil {
				t.Fatal(err)
			}
			unblock()
			if got := finishWithoutRecovery(t, diagnostics)["storage_durable_votes"]; got != 7 {
				t.Fatalf("diagnostic snapshot lost counter: %d", got)
			}
		})
	}
}

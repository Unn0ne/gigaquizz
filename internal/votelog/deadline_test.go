package votelog

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestObservedDeadlineClosesEveryLocalPartitionAndFrame(t *testing.T) {
	for _, viaFrame := range []bool{false, true} {
		s, w, _ := batchTestWriter()
		s.cfg.Partitions = 2
		other := &writer{s: s, partition: 1, jobs: make(chan *pending, s.cfg.QueuePerPartition)}
		s.writers = []*writer{w, other}
		s.byPartition = s.writers
		tokenFor := func(p int32) [16]byte {
			for i := 1; i < 256; i++ {
				v := [16]byte{byte(i)}
				if s.cfg.Partition(v) == p {
					return v
				}
			}
			t.Fatal("missing fixture partition")
			return [16]byte{}
		}
		first, next := tokenFor(0), tokenFor(1)
		now := s.cfg.EndsAt
		s.now = func() time.Time { return now }
		var err error
		if viaFrame {
			_, err = s.SubmitFrame(context.Background(), []Input{{Token: first, Choice: 1}})
		} else {
			_, err = s.Submit(context.Background(), first, 1)
		}
		if !errors.Is(err, ErrClosed) {
			t.Fatal("deadline was not closed", err)
		}
		now = s.cfg.StartsAt
		if _, err := s.Submit(context.Background(), next, 1); !errors.Is(err, ErrClosed) {
			t.Fatal("rollback reopened another partition", err)
		}
		if _, err := s.SubmitFrame(context.Background(), []Input{{Token: next, Choice: 1}}); !errors.Is(err, ErrClosed) {
			t.Fatal("rollback reopened frame admission", err)
		}
		if s.admitted.Load() != 0 || len(w.jobs) != 0 || len(other.jobs) != 0 {
			t.Fatal("closed attempt entered a queue")
		}
	}
}

func TestClosedLatchDoesNotBypassSealCancellation(t *testing.T) {
	s, w, _ := batchTestWriter()
	s.admissionClosed.Store(true)
	w.sealed = true
	w.end = 9
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Seal(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("cached CLOSED bypassed caller cancellation", err)
	}
}

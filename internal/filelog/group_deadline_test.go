package filelog

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestGroupObservedDeadlineClosesEveryPartitionAfterRollback(t *testing.T) {
	c := groupTestConfig(t)
	g, err := NewGroup(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	clock := freezeGroup(g)
	clock.Store(c.EndsAt.UnixNano())
	if _, err := g.Submit(context.Background(), groupTokens(c, 0, 1)[0]); !errors.Is(err, ErrClosed) {
		t.Fatalf("first shard did not observe deadline: %v", err)
	}
	clock.Store(c.StartsAt.Add(time.Second).UnixNano())
	if _, err := g.Submit(context.Background(), groupTokens(c, 1, 1)[0]); !errors.Is(err, ErrClosed) {
		t.Fatalf("another shard reopened after rollback: %v", err)
	}
	if g.Metrics()["durable_votes"] != 0 {
		t.Fatal("closed group wrote a vote")
	}
}

func TestGroupCancelledSealBeforeActionLeavesAdmissionOpen(t *testing.T) {
	for _, when := range []string{"already-cancelled", "waiting-for-deadline"} {
		t.Run(when, func(t *testing.T) {
			c := groupTestConfig(t)
			g, err := NewGroup(context.Background(), c)
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			clock := freezeGroup(g)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if when == "already-cancelled" {
				// Cancellation wins even when the unobserved test clock is at
				// the deadline: this call must not begin a sealing action.
				clock.Store(c.EndsAt.UnixNano())
				cancel()
				if _, err := g.Seal(ctx); !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled seal: %v", err)
				}
			} else {
				waiting := make(chan struct{})
				var once sync.Once
				g.now = func() time.Time {
					once.Do(func() { close(waiting) })
					return time.Unix(0, clock.Load()).UTC()
				}
				done := make(chan error, 1)
				go func() { _, err := g.Seal(ctx); done <- err }()
				awaitValue(t, waiting)
				cancel()
				if err := awaitValue(t, done); !errors.Is(err, context.Canceled) {
					t.Fatalf("waiting seal: %v", err)
				}
			}
			clock.Store(c.StartsAt.Add(time.Second).UnixNano())
			for p := 0; p < c.Partitions; p++ {
				if _, err := g.Submit(context.Background(), groupTokens(c, p, 1)[0]); err != nil {
					t.Fatalf("cancelled seal closed partition %d: %v", p, err)
				}
			}
		})
	}
}

func TestGroupSealLatchesBeforeFirstShardSyncCompletes(t *testing.T) {
	c := groupTestConfig(t)
	g, err := NewGroup(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	clock := freezeGroup(g)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	syncFile := g.writers[0].syncFile
	g.writers[0].syncFile = func() error {
		close(entered)
		<-release
		return syncFile()
	}
	clock.Store(c.EndsAt.UnixNano())
	done := make(chan error, 1)
	go func() { _, err := g.Seal(context.Background()); done <- err }()
	awaitValue(t, entered)
	clock.Store(c.StartsAt.Add(time.Second).UnixNano())
	if _, err := g.Submit(context.Background(), groupTokens(c, 1, 1)[0]); !errors.Is(err, ErrClosed) {
		t.Fatalf("another shard admitted during CLOSED sync after rollback: %v", err)
	}
	clock.Store(c.EndsAt.UnixNano())
	once.Do(func() { close(release) })
	if err := awaitValue(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestGroupRecoveryOfPartialSealDoesNotReopenAnotherPartition(t *testing.T) {
	c := groupTestConfig(t)
	g, err := NewGroup(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	clock := freezeGroup(g)
	in := groupTokens(c, 1, 1)[0]
	receipt, err := g.Submit(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	// Finish shard 0's real durable CLOSED, then cancel while shard 1's
	// clock is already back inside the minute. Its WAL must remain open.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g.writers[1].now = func() time.Time {
		cancel()
		return c.StartsAt.Add(time.Second)
	}
	clock.Store(c.EndsAt.UnixNano())
	if _, err := g.Seal(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("partial seal did not stop at second shard: %v", err)
	}
	g.Close()
	for p, wantClosed := range []bool{true, false} {
		scan, err := Scan(context.Background(), c.child(p), nil)
		if err != nil || scan.Closed != wantClosed {
			t.Fatalf("partial on-disk barrier at shard %d: closed=%v err=%v", p, scan.Closed, err)
		}
	}
	if _, err := GroupReplay(context.Background(), c, nil); err == nil {
		t.Fatal("partial group passed complete audit")
	}
	// Recover a new owner solely from the files, as after process exit. The
	// real clock and the new owner's clock are inside the original minute;
	// no in-memory latch from the previous owner survives this boundary.
	recovered, err := RecoverGroup(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	recoveredClock := freezeGroup(recovered)
	in.Choice = 2
	if _, err := recovered.Submit(context.Background(), in); !errors.Is(err, ErrClosed) {
		t.Fatalf("new owner reopened unsealed shard after clock rollback: %v", err)
	}
	recoveredClock.Store(c.EndsAt.UnixNano())
	if _, err := recovered.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	recovered.Close()
	count := 0
	_, err = GroupReplay(context.Background(), c, func(pos Position, v Vote) error {
		count++
		if pos != (Position{receipt.Partition, receipt.Offset, receipt.Index}) || v.Token != in.Token || v.Choice != 1 || !v.AdmittedAt.Equal(receipt.AdmittedAt) {
			return errors.New("original acknowledged record changed")
		}
		return nil
	})
	if err != nil || count != 1 {
		t.Fatalf("completed exact replay: count=%d err=%v", count, err)
	}
}

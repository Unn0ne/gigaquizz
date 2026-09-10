package filelog

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	start := time.Now().UTC().Add(-time.Second)
	return Config{Directory: filepath.Join(t.TempDir(), "journal"), PollID: [16]byte{1, 2, 3}, StartsAt: start, EndsAt: start.Add(time.Minute), AllowedMask: 3, BatchSize: 16, QueuePerPartition: 32, Linger: time.Millisecond, Partitions: 1}
}

func testInput(id byte, choice uint32) Input { return Input{Token: [16]byte{id}, Choice: choice} }

func testStore(t *testing.T, c Config) (*Store, *atomic.Int64) {
	t.Helper()
	s, err := New(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	var clock atomic.Int64
	clock.Store(c.StartsAt.Add(time.Second).UnixNano())
	s.now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	t.Cleanup(s.Close)
	return s, &clock
}

func sealTest(t *testing.T, s *Store, clock *atomic.Int64) Manifest {
	t.Helper()
	clock.Store(s.c.EndsAt.UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	m, err := s.Seal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func awaitValue[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("operation did not finish")
		var zero T
		return zero
	}
}

func waitQueued(t *testing.T, s *Store, votes uint64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s.Metrics()["queued_votes"] == votes {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("queue did not reach expected size")
}

func TestDurableFramesReplayFullIDsAndFirstChoice(t *testing.T) {
	c := testConfig(t)
	s, clock := testStore(t, c)
	a := testInput(7, 1)
	b := a
	b.Token[15] = 9
	b.Choice = 2
	r1, err := s.SubmitFrame(context.Background(), []Input{a, b, {Token: a.Token, Choice: 2}})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := s.SubmitFrame(context.Background(), []Input{b})
	if err != nil {
		t.Fatal(err)
	}
	m := sealTest(t, s, clock)
	if r1.Offset != 0 || r1.Count != 3 || r2.Offset != 1 || m.Partitions[0].Offset != 2 {
		t.Fatalf("receipts/manifest: %+v %+v %+v", r1, r2, m)
	}
	canonical := make(map[[16]byte]uint32)
	var positions []Position
	got, err := Replay(context.Background(), c, func(p Position, v Vote) error {
		positions = append(positions, p)
		if _, exists := canonical[v.Token]; !exists {
			canonical[v.Token] = v.Choice
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(positions) != 4 || positions[2] != (Position{Offset: 0, Index: 2}) || positions[3] != (Position{Offset: 1}) || len(canonical) != 2 || canonical[a.Token] != 1 || canonical[b.Token] != 2 || got.Partitions[0] != m.Partitions[0] {
		t.Fatalf("incorrect replay: %v %v", positions, canonical)
	}
	clock.Store(c.StartsAt.Add(time.Second).UnixNano())
	if _, err := s.SubmitFrame(context.Background(), []Input{a}); !errors.Is(err, ErrClosed) {
		t.Fatalf("reopened CLOSED: %v", err)
	}
}

func TestSyncBlocksACKAndLogicalQueueBounds(t *testing.T) {
	c := testConfig(t)
	c.BatchSize = 2
	c.QueuePerPartition = 2
	s, clock := testStore(t, c)
	entered, release := make(chan struct{}, 1), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	s.syncFile = func() error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		return durableSync(s.f)
	}
	first, second := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := s.SubmitFrame(context.Background(), []Input{testInput(1, 1), testInput(2, 1)})
		first <- err
	}()
	awaitValue(t, entered)
	select {
	case err := <-first:
		t.Fatalf("ACK before sync: %v", err)
	default:
	}
	go func() {
		_, err := s.SubmitFrame(context.Background(), []Input{testInput(3, 1), testInput(4, 1)})
		second <- err
	}()
	waitQueued(t, s, 2)
	if m := s.Metrics(); m["active_votes"] != 2 || m["queued_votes"] != 2 || m["durable_votes"] != 0 {
		t.Fatalf("bounds: %v", m)
	}
	if _, err := s.SubmitFrame(context.Background(), []Input{testInput(5, 1)}); !errors.Is(err, ErrBusy) {
		t.Fatalf("wanted Busy: %v", err)
	}
	// Even when full, observing the deadline must latch admission closed.
	clock.Store(c.EndsAt.UnixNano())
	if _, err := s.SubmitFrame(context.Background(), []Input{testInput(5, 1)}); !errors.Is(err, ErrClosed) {
		t.Fatalf("deadline during saturation: %v", err)
	}
	clock.Store(c.StartsAt.Add(time.Second).UnixNano())
	if _, err := s.SubmitFrame(context.Background(), []Input{testInput(5, 1)}); !errors.Is(err, ErrClosed) {
		t.Fatalf("rollback reopened: %v", err)
	}
	close(release)
	if err := awaitValue(t, first); err != nil {
		t.Fatal(err)
	}
	if err := awaitValue(t, second); err != nil {
		t.Fatal(err)
	}
	sealTest(t, s, clock)
	result, err := Scan(context.Background(), c, nil)
	if err != nil || result.Votes != 4 {
		t.Fatalf("scan: %+v %v", result, err)
	}
}

func TestAdmissionBeforeDeadlineMaySyncAfterDeadline(t *testing.T) {
	c := testConfig(t)
	s, clock := testStore(t, c)
	clock.Store(c.EndsAt.Add(-100 * time.Millisecond).UnixNano())
	entered, release := make(chan struct{}, 1), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	s.syncFile = func() error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		return durableSync(s.f)
	}
	result := make(chan answer, 1)
	go func() {
		r, err := s.SubmitFrame(context.Background(), []Input{testInput(1, 1)})
		result <- answer{r, err}
	}()
	awaitValue(t, entered)
	clock.Store(c.EndsAt.Add(200 * time.Millisecond).UnixNano())
	if _, err := s.SubmitFrame(context.Background(), []Input{testInput(2, 1)}); !errors.Is(err, ErrClosed) {
		t.Fatalf("after deadline: %v", err)
	}
	close(release)
	a := awaitValue(t, result)
	if a.err != nil || !a.receipt.AdmittedAt.Equal(c.EndsAt.Add(-100*time.Millisecond)) {
		t.Fatalf("59.9 admission: %+v", a)
	}
	sealTest(t, s, clock)
}

func TestCanceledWaitMayStillPersist(t *testing.T) {
	c := testConfig(t)
	s, _ := testStore(t, c)
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	s.syncFile = func() error { close(entered); <-release; return durableSync(s.f) }
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := s.SubmitFrame(ctx, []Input{testInput(1, 1)}); result <- err }()
	awaitValue(t, entered)
	cancel()
	if err := awaitValue(t, result); !errors.Is(err, ErrUnknown) {
		t.Fatalf("canceled admitted vote: %v", err)
	}
	close(release)
	s.Close()
	r, err := Scan(context.Background(), c, nil)
	if err != nil || r.Votes != 1 || r.Closed {
		t.Fatalf("unknown must remain replayable: %+v %v", r, err)
	}
	if _, err := Replay(context.Background(), c, nil); err == nil {
		t.Fatal("unsealed journal returned final result")
	}
}

func TestShortWritesAreCompletedBeforeSync(t *testing.T) {
	c := testConfig(t)
	s, clock := testStore(t, c)
	var calls atomic.Int64
	s.write = func(b []byte) (int, error) { calls.Add(1); return s.f.Write(b[:min(len(b), 7)]) }
	if _, err := s.SubmitFrame(context.Background(), []Input{testInput(1, 1)}); err != nil {
		t.Fatal(err)
	}
	sealTest(t, s, clock)
	if calls.Load() < 10 {
		t.Fatal("short writes were not exercised")
	}
	if _, err := Replay(context.Background(), c, nil); err != nil {
		t.Fatal(err)
	}
	if err := writeAll(func([]byte) (int, error) { return 0, nil }, []byte{1}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("zero write: %v", err)
	}
}

func TestIOFailurePoisonsWriter(t *testing.T) {
	for _, stage := range []string{"write", "sync"} {
		t.Run(stage, func(t *testing.T) {
			c := testConfig(t)
			s, _ := testStore(t, c)
			var calls atomic.Int64
			if stage == "write" {
				s.write = func(b []byte) (int, error) {
					calls.Add(1)
					n, err := s.f.Write(b[:min(len(b), 17)])
					if err != nil {
						return n, err
					}
					return n, syscall.ENOSPC
				}
			} else {
				s.syncFile = func() error { calls.Add(1); return syscall.EIO }
			}
			if _, err := s.SubmitFrame(context.Background(), []Input{testInput(1, 1)}); !errors.Is(err, ErrUnknown) {
				t.Fatalf("failed %s acknowledged: %v", stage, err)
			}
			if _, err := s.SubmitFrame(context.Background(), []Input{testInput(2, 1)}); !errors.Is(err, ErrUnknown) {
				t.Fatalf("poisoned writer accepted: %v", err)
			}
			s.Close()
			if calls.Load() != 1 || s.Metrics()["durable_votes"] != 0 {
				t.Fatalf("continued after error: calls %d metrics %v", calls.Load(), s.Metrics())
			}
			r, err := Scan(context.Background(), c, nil)
			if err != nil {
				t.Fatal(err)
			}
			if stage == "write" && (!r.IncompleteTail || r.Votes != 0) {
				t.Fatalf("short failed write: %+v", r)
			}
			if stage == "sync" && (r.IncompleteTail || r.Votes != 1) {
				t.Fatalf("written-but-unknown prefix: %+v", r)
			}
		})
	}
}

func TestNewPreservesExistingStateAndScanRequiresOwnership(t *testing.T) {
	c := testConfig(t)
	s, _ := testStore(t, c)
	if _, err := Scan(context.Background(), c, nil); err == nil {
		t.Fatal("scan ignored live writer flock")
	}
	if _, err := New(context.Background(), c); !errors.Is(err, os.ErrExist) {
		t.Fatalf("reopened existing directory: %v", err)
	}
	s.Close()
	if _, err := Scan(context.Background(), c, nil); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(c.Directory, walName))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(context.Background(), c); !errors.Is(err, os.ErrExist) {
		t.Fatalf("reopened closed directory: %v", err)
	}
	after, err := os.ReadFile(filepath.Join(c.Directory, walName))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("existing WAL changed")
	}
	bad := c
	bad.PollID[15]++
	if _, err := Scan(context.Background(), bad, nil); err == nil {
		t.Fatal("metadata mismatch accepted")
	}
}

func TestValidationAndNotOpen(t *testing.T) {
	c := testConfig(t)
	s, clock := testStore(t, c)
	clock.Store(c.StartsAt.Add(-time.Nanosecond).UnixNano())
	if _, err := s.SubmitFrame(context.Background(), []Input{testInput(1, 1)}); !errors.Is(err, ErrNotOpen) {
		t.Fatalf("not open: %v", err)
	}
	clock.Store(c.StartsAt.UnixNano())
	for _, in := range [][]Input{nil, {{Choice: 1}}, {testInput(1, 0)}, {testInput(1, 3)}, {testInput(1, 4)}} {
		if _, err := s.SubmitFrame(context.Background(), in); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid admitted: %v %v", in, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.SubmitFrame(ctx, []Input{testInput(1, 1)}); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-enqueue context: %v", err)
	}
	if s.Metrics()["queued_votes"] != 0 || s.Metrics()["durable_votes"] != 0 {
		t.Fatal("invalid votes entered writer")
	}
}

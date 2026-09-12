package filelog

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type singleAnswer struct {
	receipt Receipt
	err     error
}

func waitActive(t *testing.T, s *Store, votes uint64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s.Metrics()["active_votes"] == votes {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("active batch did not reach expected size")
}

func submitSingle(s *Store, in Input) <-chan singleAnswer {
	result := make(chan singleAnswer, 1)
	go func() {
		r, err := s.Submit(context.Background(), in)
		result <- singleAnswer{r, err}
	}()
	return result
}

func blockSync(t *testing.T, s *Store) (<-chan struct{}, func()) {
	t.Helper()
	entered, released := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(released) }) }
	t.Cleanup(release) // Release before the Store's registered Close cleanup.
	s.syncFile = func() error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-released
		return durableSync(s.f)
	}
	return entered, release
}

func TestPackedSinglesMixedFramesPreserveAdmissionOrderAndDeadline(t *testing.T) {
	c := testConfig(t)
	c.BatchSize, c.Linger = 7, time.Second
	s, clock := testStore(t, c)
	entered, release := blockSync(t, s)
	a, b, d := testInput(1, 1), testInput(2, 2), testInput(4, 2)
	inputs := []Input{a, {Token: a.Token, Choice: 2}, b, testInput(3, 1), {Token: b.Token, Choice: 1}, d, {Token: d.Token, Choice: 1}}
	times := []time.Time{c.StartsAt.Add(time.Second), c.StartsAt.Add(2 * time.Second), c.StartsAt.Add(3 * time.Second), c.StartsAt.Add(3 * time.Second), c.EndsAt.Add(-300 * time.Millisecond), c.EndsAt.Add(-200 * time.Millisecond), c.EndsAt.Add(-100 * time.Millisecond)}
	wantPositions := []Position{{Offset: 0, Index: 0}, {Offset: 0, Index: 1}, {Offset: 1, Index: 0}, {Offset: 1, Index: 1}, {Offset: 2, Index: 0}, {Offset: 2, Index: 1}, {Offset: 2, Index: 2}}
	responses := make(map[int]<-chan singleAnswer)
	var frameResponse <-chan answer
	for i := 0; i < len(inputs); i++ {
		clock.Store(times[i].UnixNano())
		if i == 2 {
			out := make(chan answer, 1)
			frameResponse = out
			go func() {
				r, err := s.SubmitFrame(context.Background(), inputs[2:4])
				out <- answer{r, err}
			}()
			i++
		} else {
			responses[i] = submitSingle(s, inputs[i])
		}
		waitActive(t, s, uint64(i+1))
	}
	awaitValue(t, entered)
	for _, response := range responses {
		select {
		case r := <-response:
			t.Fatalf("single returned before sync: %+v", r)
		default:
		}
	}
	select {
	case r := <-frameResponse:
		t.Fatalf("explicit frame returned before sync: %+v", r)
	default:
	}
	// All seven attempts were admitted, including 59.9; neither form may
	// enter at/after the deadline even though the earlier sync is unfinished.
	clock.Store(c.EndsAt.UnixNano())
	if _, err := s.Submit(context.Background(), testInput(9, 1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("late single: %v", err)
	}
	if _, err := s.SubmitFrame(context.Background(), []Input{testInput(9, 1)}); !errors.Is(err, ErrClosed) {
		t.Fatalf("late explicit frame: %v", err)
	}
	clock.Store(c.StartsAt.Add(time.Second).UnixNano())
	if _, err := s.Submit(context.Background(), testInput(9, 1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("clock rollback reopened admission: %v", err)
	}
	release()
	for i, response := range responses {
		r := awaitValue(t, response)
		if r.err != nil || r.receipt.Partition != 0 || r.receipt.Offset != wantPositions[i].Offset || r.receipt.Index != wantPositions[i].Index || !r.receipt.AdmittedAt.Equal(times[i]) {
			t.Fatalf("single %d lost position or admission: %+v", i, r)
		}
	}
	f := awaitValue(t, frameResponse)
	if f.err != nil || f.receipt.Offset != 1 || f.receipt.Count != 2 || !f.receipt.AdmittedAt.Equal(times[2]) {
		t.Fatalf("explicit frame contract changed: %+v", f)
	}
	m := sealTest(t, s, clock)
	if m.Partitions[0].Offset != 3 {
		t.Fatalf("physical frame count: %+v", m)
	}
	var positions []Position
	canonical := make(map[[16]byte]uint32)
	_, err := Replay(context.Background(), c, func(p Position, v Vote) error {
		i := len(positions)
		if i >= len(inputs) || v.Token != inputs[i].Token || v.Choice != inputs[i].Choice || !v.AdmittedAt.Equal(times[i]) {
			t.Errorf("entry %d lost full key, choice or individual timestamp: %+v", i, v)
		}
		positions = append(positions, p)
		if _, ok := canonical[v.Token]; !ok {
			canonical[v.Token] = v.Choice
		}
		return nil
	})
	if err != nil || !reflect.DeepEqual(positions, wantPositions) || len(canonical) != 4 || canonical[a.Token] != 1 || canonical[b.Token] != 2 || canonical[d.Token] != 2 {
		t.Fatalf("mixed replay/first choice: positions=%v canonical=%v err=%v", positions, canonical, err)
	}
	metrics := s.Metrics()
	if metrics["durable_frames"] != 3 || metrics["durable_votes"] != 7 || metrics["acknowledged_votes"] != 7 || metrics["wal_bytes"] != fileHeaderBytes+4*frameHeaderBytes+7*entryBytes {
		t.Fatalf("physical storage diagnostics: %v", metrics)
	}
}

func TestPackedSinglesBoundedQueueDuringSync(t *testing.T) {
	c := testConfig(t)
	c.BatchSize, c.QueuePerPartition, c.Linger = 2, 2, time.Second
	s, clock := testStore(t, c)
	entered, release := blockSync(t, s)
	first := submitSingle(s, testInput(1, 1))
	waitActive(t, s, 1)
	second := submitSingle(s, testInput(2, 1))
	awaitValue(t, entered)
	third := submitSingle(s, testInput(3, 1))
	fourth := submitSingle(s, testInput(4, 1))
	waitQueued(t, s, 2)
	if _, err := s.Submit(context.Background(), testInput(5, 1)); !errors.Is(err, ErrBusy) {
		t.Fatalf("full queue accepted a single: %v", err)
	}
	if metrics := s.Metrics(); metrics["active_votes"] != 2 || metrics["durable_votes"] != 0 || metrics["busy_votes"] != 1 {
		t.Fatalf("logical vote memory bound: %v", metrics)
	}
	release()
	for _, response := range []<-chan singleAnswer{first, second, third, fourth} {
		if r := awaitValue(t, response); r.err != nil {
			t.Fatal(r.err)
		}
	}
	sealTest(t, s, clock)
	scan, err := Scan(context.Background(), c, nil)
	if err != nil || scan.Votes != 4 || scan.Frames != 3 {
		t.Fatalf("busy attempt persisted or singles were not packed: %+v %v", scan, err)
	}
}

func TestPackedSingleFailuresNeverACKAndRecoverWrittenUnknown(t *testing.T) {
	for _, stage := range []string{"write", "sync"} {
		t.Run(stage, func(t *testing.T) {
			c := testConfig(t)
			c.BatchSize, c.Linger = 2, time.Second
			s, _ := testStore(t, c)
			var calls atomic.Int64
			if stage == "write" {
				s.write = func(b []byte) (int, error) {
					calls.Add(1)
					n, err := s.f.Write(b[:frameHeaderBytes+entryBytes+3])
					if err != nil {
						return n, err
					}
					return n, syscall.ENOSPC
				}
			} else {
				s.syncFile = func() error { calls.Add(1); return syscall.EIO }
			}
			first := submitSingle(s, testInput(1, 1))
			waitActive(t, s, 1)
			second := submitSingle(s, testInput(2, 2))
			for _, response := range []<-chan singleAnswer{first, second} {
				if r := awaitValue(t, response); !errors.Is(r.err, ErrUnknown) || r.receipt != (Receipt{}) {
					t.Fatalf("failed group produced an ACK: %+v", r)
				}
			}
			if _, err := s.Submit(context.Background(), testInput(3, 1)); !errors.Is(err, ErrUnknown) {
				t.Fatalf("poisoned writer continued: %v", err)
			}
			s.Close()
			if m := s.Metrics(); calls.Load() != 1 || m["durable_votes"] != 0 || m["acknowledged_votes"] != 0 || m["unknown_votes"] != 2 {
				t.Fatalf("failed sync metrics: calls=%d metrics=%v", calls.Load(), m)
			}
			recovered, err := Recover(context.Background(), c)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(recovered.Close)
			recovered.now = func() time.Time { return c.EndsAt }
			if _, err := recovered.Seal(context.Background()); err != nil {
				t.Fatal(err)
			}
			var positions []Position
			_, err = Replay(context.Background(), c, func(p Position, v Vote) error {
				positions = append(positions, p)
				if v.Token != testInput(byte(p.Index+1), 1).Token || v.Choice != p.Index+1 {
					t.Errorf("written unknown changed: %+v %+v", p, v)
				}
				return nil
			})
			want := 0
			if stage == "sync" {
				want = 2 // Complete writes may survive an unknown outcome.
			}
			if err != nil || len(positions) != want || (want == 2 && positions[1] != (Position{Index: 1})) {
				t.Fatalf("recover failed packed group: %v %v", positions, err)
			}
		})
	}
}

func TestPackedSingleCancellationLeavesAdmittedAttempt(t *testing.T) {
	c := testConfig(t)
	c.BatchSize, c.Linger = 2, time.Second
	s, _ := testStore(t, c)
	entered, release := blockSync(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan singleAnswer, 1)
	go func() { r, err := s.Submit(ctx, testInput(1, 1)); first <- singleAnswer{r, err} }()
	waitActive(t, s, 1)
	second := submitSingle(s, testInput(2, 2))
	awaitValue(t, entered)
	cancel()
	if r := awaitValue(t, first); !errors.Is(r.err, ErrUnknown) || r.receipt != (Receipt{}) {
		t.Fatalf("canceled admitted single: %+v", r)
	}
	release()
	if r := awaitValue(t, second); r.err != nil || r.receipt.Offset != 0 || r.receipt.Index != 1 {
		t.Fatalf("neighboring single lost its receipt: %+v", r)
	}
	s.Close()
	scan, err := Scan(context.Background(), c, nil)
	if err != nil || scan.Votes != 2 || scan.Frames != 1 || scan.Closed {
		t.Fatalf("canceled attempt was removed: %+v %v", scan, err)
	}
}

func TestPackedSingleValidationPreAdmission(t *testing.T) {
	c := testConfig(t)
	s, clock := testStore(t, c)
	clock.Store(c.StartsAt.Add(-time.Nanosecond).UnixNano())
	if _, err := s.Submit(context.Background(), testInput(1, 1)); !errors.Is(err, ErrNotOpen) {
		t.Fatalf("not open: %v", err)
	}
	clock.Store(c.StartsAt.UnixNano())
	for _, in := range []Input{{Choice: 1}, testInput(1, 0), testInput(1, 3), testInput(1, 4)} {
		if _, err := s.Submit(context.Background(), in); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid single: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Submit(ctx, testInput(1, 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled before admission: %v", err)
	}
	s.Close()
	if scan, err := Scan(context.Background(), c, nil); err != nil || scan.Votes != 0 {
		t.Fatalf("rejected attempt entered WAL: %+v %v", scan, err)
	}
}

func TestPackedFrameLimitRetainsEverySingleReceiptIndex(t *testing.T) {
	c := testConfig(t)
	admitted := c.StartsAt.Add(time.Second)
	batch := make([]*job, maxFrameVotes+1)
	for i := range batch {
		in := Input{Choice: 1}
		binary.BigEndian.PutUint64(in.Token[8:], uint64(i+1))
		j := &job{count: 1, admitted: admitted}
		encodeEntry(j.single[:], in, admitted)
		batch[i] = j
	}
	data, frames, err := encodeBatch(nil, batch, 0)
	if err != nil || frames != 2 {
		t.Fatalf("4096 boundary: frames=%d err=%v", frames, err)
	}
	if len(data) != 2*frameHeaderBytes+len(batch)*entryBytes {
		t.Fatal("single frame header per attempt was retained")
	}
	// The unmodified legacy reader is the compatibility oracle, not the
	// packer's own offsets or a second implementation of its chunking loop.
	closed := make([]byte, frameHeaderBytes)
	finishFrame(closed, frames, kindClosed, 0)
	if err := os.Mkdir(c.Directory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(c.Directory, pollName), pollBytes(c), 0600); err != nil {
		t.Fatal(err)
	}
	wal := append(fileHeader(c), data...)
	wal = append(wal, closed...)
	if err := os.WriteFile(filepath.Join(c.Directory, walName), wal, 0600); err != nil {
		t.Fatal(err)
	}
	seen := 0
	_, err = Replay(context.Background(), c, func(p Position, v Vote) error {
		id := binary.BigEndian.Uint64(v.Token[8:])
		if id != uint64(seen+1) || v.Choice != 1 || !v.AdmittedAt.Equal(admitted) || batch[seen].offset != p.Offset || batch[seen].index != p.Index {
			t.Fatalf("single receipt cannot identify its replayed key: row=%d position=%+v", seen, p)
		}
		if seen == maxFrameVotes-1 && p != (Position{Index: maxFrameVotes - 1}) {
			t.Fatal("first packed frame did not reach its limit")
		}
		if seen == maxFrameVotes && p != (Position{Offset: 1}) {
			t.Fatal("overflow entry did not start a new frame")
		}
		seen++
		return nil
	})
	if err != nil || seen != len(batch) {
		t.Fatalf("bounded packed replay: %d %v", seen, err)
	}
	if _, _, err := encodeBatch(nil, batch, math.MaxInt64-1); err == nil {
		t.Fatal("frame sequence overflow was accepted")
	}
}

func TestRecoverMixedOldFramesAndPackedSingles(t *testing.T) {
	c := testConfig(t)
	c.BatchSize, c.Linger = 2, time.Second
	s, _ := testStore(t, c)
	old, err := s.SubmitFrame(context.Background(), []Input{testInput(1, 1), testInput(2, 2)})
	if err != nil || old.Offset != 0 || old.Count != 2 {
		t.Fatalf("old frame: %+v %v", old, err)
	}
	s.Close()
	r, err := Recover(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	first := submitSingle(r, testInput(1, 2))
	waitActive(t, r, 1)
	second := submitSingle(r, testInput(3, 1))
	for i, response := range []<-chan singleAnswer{first, second} {
		got := awaitValue(t, response)
		if got.err != nil || got.receipt.Offset != 1 || got.receipt.Index != uint32(i) {
			t.Fatalf("recovered single: %+v", got)
		}
	}
	r.Close()
	r, err = Recover(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	last, err := r.SubmitFrame(context.Background(), []Input{testInput(3, 2), testInput(4, 2)})
	if err != nil || last.Offset != 2 || last.Count != 2 {
		t.Fatalf("explicit frame after packed restart: %+v %v", last, err)
	}
	r.now = func() time.Time { return c.EndsAt }
	manifest, err := r.Seal(context.Background())
	if err != nil || manifest.Partitions[0].Offset != 3 {
		t.Fatalf("mixed restart CLOSED: %+v %v", manifest, err)
	}
	canonical := make(map[[16]byte]uint32)
	count := 0
	_, err = Replay(context.Background(), c, func(_ Position, v Vote) error {
		count++
		if _, ok := canonical[v.Token]; !ok {
			canonical[v.Token] = v.Choice
		}
		return nil
	})
	if err != nil || count != 6 || len(canonical) != 4 || canonical[testInput(1, 1).Token] != 1 || canonical[testInput(3, 1).Token] != 1 {
		t.Fatalf("mixed restart lost first choices: count=%d canonical=%v err=%v", count, canonical, err)
	}
	closed, err := Recover(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closed.Close)
	closed.now = func() time.Time { return c.StartsAt }
	if _, err := closed.Submit(context.Background(), testInput(5, 1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("recovered CLOSED accepted a single: %v", err)
	}
}

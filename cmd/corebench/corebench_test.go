package main

import (
	"context"
	"crypto/aes"
	"encoding/binary"
	"math"
	"sync"
	"testing"
	"time"

	"gigaquizz/internal/votecore"
)

func TestBoundsIncludeRepeatsAndPreserveFullMinute(t *testing.T) {
	c := defaults()
	c.rate = maximumRate
	c.allowLarge = true
	if err := c.validate(); err != nil || c.keys() != 180000000 || c.capacity() != 189000000 {
		t.Fatalf("maximum configuration: keys%d capacity%d err%v", c.keys(), c.capacity(), err)
	}
	c.repeatEvery = 20
	if err := c.validate(); err == nil {
		t.Fatal("additional repeat attempts escaped180M limit")
	}
	c = defaults()
	c.rate = 1000
	c.repeatEvery = 5
	c.allowLarge = true
	if c.keys() != 60000 || c.attempts() != 72000 || c.capacity() != 262144 {
		t.Fatal("smoke capacity/count calculation wrong")
	}
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
	c.allowLarge = false
	if err := c.validate(); err == nil {
		t.Fatal("opt-in bypassed")
	}
	for _, mutate := range []func(*config){func(c *config) { c.rate = 3000001 }, func(c *config) { c.workers = 17 }, func(c *config) { c.bufferSize = 1028 }, func(c *config) { c.bufferSize = 255 }, func(c *config) { c.maxLag = 101 * time.Millisecond }, func(c *config) { c.coalesce = 2 * time.Millisecond }} {
		c = defaults()
		c.allowLarge = true
		mutate(&c)
		if err := c.validate(); err == nil {
			t.Fatal("invalid bound accepted")
		}
	}
}

func TestCoalescingGroupFitsRateAndDoesNotDelaySlowDefault(t *testing.T) {
	c := defaults()
	if c.groupSize() != 1 {
		t.Fatal("slow rate must not wait for entire256-key buffer")
	}
	c.rate = 1700000
	if c.groupSize() != 256 || scheduledOffset(uint64(c.groupSize()-1), c.rate) > c.coalesce {
		t.Fatal("coalescing exceeds configured bound")
	}
	c.bufferSize = 1024
	if c.groupSize() != 425 {
		t.Fatal("coalescing must adapt to rate below buffer size")
	}
}

func TestBitmapWorkerOwnershipAndFullChoiceValues(t *testing.T) {
	const keys = 8192
	bitmap := make([]byte, keys/4)
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Go(func() {
			for base := worker * 256; base < keys; base += 8 * 256 {
				for key := base; key < base+256; key++ {
					setChoice(bitmap, uint64(key), uint32(key%2+1))
				}
			}
		})
	}
	wg.Wait()
	for key := 0; key < keys; key++ {
		if getChoice(bitmap, uint64(key)) != uint32(key%2+1) {
			t.Fatalf("packed outcome changed at%d", key)
		}
	}
	setChoice(bitmap, 1, 0)
	if getChoice(bitmap, 0) != 1 || getChoice(bitmap, 1) != 0 || getChoice(bitmap, 2) != 1 {
		t.Fatal("bitmap overwrite corrupted neighbor")
	}
}

func TestHistogramIsBoundedApproximationAndCountsEverySample(t *testing.T) {
	for _, n := range []uint64{0, 1, 15, 16, 31, 32, 1000, 1000000, 10000000000, math.MaxUint64} {
		index := histogramIndex(n)
		if index >= 1024 || histogramUpper(index) < n {
			t.Fatalf("sample%d not covered", n)
		}
		if n >= 32 && n < math.MaxUint64/2 && float64(histogramUpper(index)) > float64(n)*1.0625 {
			t.Fatalf("histogram error too large for%d", n)
		}
	}
	var h histogram
	for i := uint64(1); i <= 100; i++ {
		h.add(i * 1000)
	}
	r := h.report()
	if r.Samples != 100 || r.P99UpperUS < 99 || r.P99UpperUS > 99*1.0625 || r.MeanUS != 50.5 {
		t.Fatalf("wrong histogram report%+v", r)
	}
}

func TestAESSamplingSpansWorkersAndPositions(t *testing.T) {
	var seed [16]byte
	cipher, _ := aes.NewCipher(seed[:])
	var counter, token [16]byte
	var selected [8]int
	positions := map[int]bool{}
	for key := uint64(0); key < 262144; key++ {
		binary.BigEndian.PutUint64(counter[8:], key+1)
		cipher.Encrypt(token[:], counter[:])
		if binary.BigEndian.Uint16(token[14:])&1023 == 0 {
			selected[(key/256)%8]++
			positions[int(key%256)] = true
		}
	}
	for worker, n := range selected {
		if n == 0 {
			t.Fatalf("AES sampling missed worker%d", worker)
		}
	}
	if len(positions) < 32 {
		t.Fatal("sampling biased toward a few buffer positions")
	}
}

func TestCancelledGeneratorCountsEveryUnvisitedAttempt(t *testing.T) {
	c := defaults()
	c.rate = 1
	c.repeatEvery = 5
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := runWorkload(ctx, c, time.Now(), [16]byte{}, make([]byte, (c.keys()+3)/4), func(votecore.Input) votecore.Result {
		t.Error("cancelled generator dispatched")
		return votecore.Result{Status: votecore.Accepted}
	})
	if r.Counts.Planned != 72 || r.Counts.Dispatched != 0 || r.Counts.Skipped != 72 || r.Counts.CancelledRemaining != 72 {
		t.Fatalf("cancelled slots disappeared:%+v", r.Counts)
	}
	var buckets uint64
	for _, b := range r.Buckets {
		buckets += b.Skipped
	}
	if buckets != 72 {
		t.Fatal("per-second skip totals differ")
	}
}

func TestWaitUntilNeverReturnsBeforeDueTime(t *testing.T) {
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	at := time.Now().Add(time.Millisecond)
	if !waitUntil(context.Background(), timer, at) || time.Now().Before(at) {
		t.Fatal("early scheduler wakeup was not rechecked")
	}
}

func TestSealWaitUsesWallDeadlineAndRechecksCore(t *testing.T) {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	end := time.Now().Add(time.Millisecond)
	called := 0
	snapshot, err := sealAfterDeadline(context.Background(), timer, end, func() (votecore.Snapshot, error) {
		if time.Now().UnixNano() < end.UnixNano() {
			t.Error("seal attempted before authoritative wall deadline")
		}
		called++
		if called == 1 {
			return votecore.Snapshot{}, votecore.ErrStillOpen
		}
		return votecore.Snapshot{Closed: true}, nil
	})
	if err != nil || !snapshot.Closed || called != 2 {
		t.Fatalf("seal boundary not rechecked: %+v %v calls=%d", snapshot, err, called)
	}
}

func TestRegeneratedAuditChecksAllFullKeysAndFirstChoices(t *testing.T) {
	c := defaults()
	c.rate = 1
	bitmap := make([]byte, (c.keys()+3)/4)
	setChoice(bitmap, 3, 1)
	setChoice(bitmap, 59, 2)
	var seed [16]byte
	seed[0] = 7
	cipher, _ := aes.NewCipher(seed[:])
	var counter, token [16]byte
	expected := map[[16]byte]uint32{}
	for _, key := range []uint64{3, 59} {
		binary.BigEndian.PutUint64(counter[8:], key+1)
		cipher.Encrypt(token[:], counter[:])
		expected[token] = getChoice(bitmap, key)
	}
	snapshot := votecore.Snapshot{Total: 2, Counts: [32]uint64{1, 1}, Closed: true}
	lookup := func(token [16]byte) (uint32, bool) { v, ok := expected[token]; return v, ok }
	r := reconcile(context.Background(), c, seed, bitmap, 2, snapshot, lookup)
	if !r.Correct || r.CheckedKeys != 2 {
		t.Fatalf("correct full-key replay rejected:%+v", r)
	}
	for token := range expected {
		delete(expected, token)
		break
	}
	r = reconcile(context.Background(), c, seed, bitmap, 2, snapshot, lookup)
	if r.Correct || r.Missing != 1 {
		t.Fatal("missing accepted key escaped audit")
	}
}

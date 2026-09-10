package votecore

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fixture(t *testing.T, capacity uint64) (*Engine, *atomic.Int64) {
	t.Helper()
	start := time.Unix(1700000000, 0).UTC()
	e, err := New(Config{PollID: [16]byte{1}, StartsAt: start, EndsAt: start.Add(time.Minute), AllowedMask: 7, Shards: 8, Capacity: capacity})
	if err != nil {
		t.Fatal(err)
	}
	clock := &atomic.Int64{}
	clock.Store(start.Add(time.Second).UnixNano())
	e.now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	return e, clock
}

func TestConcurrentConflictingVotesHaveOneFirstChoice(t *testing.T) {
	e, clock := fixture(t, 128)
	var accepted atomic.Int64
	var winner atomic.Uint32
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Go(func() {
			choice := uint32(1) << (i % 2)
			r := e.Submit(Input{Token: [16]byte{7}, Choice: choice})
			if r.Status == Accepted {
				accepted.Add(1)
				winner.Store(r.Choice)
			} else if r.Status != Duplicate {
				t.Errorf("unexpected status %v", r.Status)
			}
		})
	}
	wg.Wait()
	if accepted.Load() != 1 {
		t.Fatal("first choice was not unique")
	}
	choice, ok := e.Lookup([16]byte{7})
	if !ok || choice != winner.Load() {
		t.Fatal("canonical choice changed")
	}
	clock.Store(e.endNS)
	s, err := e.Seal()
	if err != nil || s.Total != 1 || s.Counts[0]+s.Counts[1] != 1 {
		t.Fatalf("incorrect final result %+v %v", s, err)
	}
}

func TestDeadlineAfterLockAndClosedSurvivesClockRollback(t *testing.T) {
	e, clock := fixture(t, 128)
	in := Input{Token: [16]byte{1}, Choice: 1}
	clock.Store(e.startNS - 1)
	if e.Submit(in).Status != NotOpen {
		t.Fatal("early vote admitted")
	}
	clock.Store(e.startNS)
	if _, err := e.Seal(); !errors.Is(err, ErrStillOpen) {
		t.Fatal("premature seal allowed")
	}
	if e.Submit(in).Status != Accepted {
		t.Fatal("start boundary rejected")
	}
	// The caller waits for admission lock across the deadline.
	s := &e.shards[e.Partition(in.Token)]
	s.mu.Lock()
	result := make(chan Result, 1)
	go func() { result <- e.Submit(in) }()
	clock.Store(e.endNS)
	s.mu.Unlock()
	if (<-result).Status != Closed {
		t.Fatal("duplicate after deadline admitted")
	}
	clock.Store(e.startNS)
	if e.Submit(Input{Token: [16]byte{2}, Choice: 1}).Status != Closed {
		t.Fatal("clock rollback reopened poll")
	}
	final, err := e.Seal()
	if err != nil || final.Total != 1 {
		t.Fatal(final, err)
	}
}

func TestSealWaitsForAdmittedWriteAcrossDeadline(t *testing.T) {
	e, clock := fixture(t, 128)
	clock.Store(e.endNS - int64(100*time.Millisecond))
	entered, release := make(chan struct{}), make(chan struct{})
	e.beforeStore = func() { close(entered); <-release }
	result := make(chan Result, 1)
	go func() { result <- e.Submit(Input{Token: [16]byte{5}, Choice: 2}) }()
	<-entered
	clock.Store(e.endNS + int64(200*time.Millisecond))
	sealed := make(chan Snapshot, 1)
	go func() {
		s, err := e.Seal()
		if err != nil {
			t.Error(err)
		}
		sealed <- s
	}()
	select {
	case <-sealed:
		t.Fatal("seal skipped admitted write")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	if (<-result).Status != Accepted || (<-sealed).Total != 1 {
		t.Fatal("predeadline admitted vote lost")
	}
}

func TestFullTokenRoutingAndShardCapacity(t *testing.T) {
	e, _ := fixture(t, 16)
	var tokens [][16]byte
	for i := uint64(1); len(tokens) < 3; i++ {
		var token [16]byte
		token[0] = byte(i)
		token[1] = byte(i >> 8)
		if e.Partition(token) == 0 {
			tokens = append(tokens, token)
		}
	}
	if e.Submit(Input{tokens[0], 1}).Status != Accepted || e.Submit(Input{tokens[1], 2}).Status != Accepted || e.Submit(Input{tokens[2], 1}).Status != CapacityExceeded {
		t.Fatal("shard capacity violated")
	}
	if r := e.Submit(Input{tokens[0], 2}); r.Status != Duplicate || r.Choice != 1 {
		t.Fatal("capacity altered first choice")
	}
}

func TestChoiceMaskAndConfiguration(t *testing.T) {
	e, _ := fixture(t, 128)
	for _, in := range []Input{{Token: [16]byte{}, Choice: 1}, {Token: [16]byte{1}, Choice: 0}, {Token: [16]byte{1}, Choice: 8}, {Token: [16]byte{1}, Choice: 3}} {
		if e.Submit(in).Status != Invalid {
			t.Fatal("invalid input admitted")
		}
	}
	c := e.cfg
	c.Multiple = true
	m, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	m.now = e.now
	if m.Submit(Input{Token: [16]byte{1}, Choice: 5}).Status != Accepted {
		t.Fatal("multiple choice rejected")
	}
	c.Shards = 3
	if _, err := New(c); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal("invalid shard count accepted")
	}
	c = e.cfg
	c.Capacity = 200_000_001
	if _, err := New(c); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal("unbounded capacity accepted")
	}
}

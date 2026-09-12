package votelog

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func kafkaWaitAdmitted(t *testing.T, ctx context.Context, s *Store, count uint64) {
	t.Helper()
	for s.admitted.Load() < count && ctx.Err() == nil {
		time.Sleep(time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("timed out waiting for isolated test admission")
	}
}

// The first real transaction holds the writer while the following admitted
// calls fill exactly one batch. This avoids relying on goroutine scheduling
// to observe mixed single-call/explicit-frame packing in the real journal.
func TestKafkaPackedSinglesMixedReceiptsSurviveRecovery(t *testing.T) {
	ctx, cfg := kafkaTestConfig(t, 1)
	cfg.BatchSize, cfg.Linger = 8, time.Second
	s, clock := kafkaTestStore(t, ctx, cfg)
	a, b, c, d, e, f, g := kafkaTestToken(t), kafkaTestToken(t), kafkaTestToken(t), kafkaTestToken(t), kafkaTestToken(t), kafkaTestToken(t), kafkaTestToken(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var hookOnce sync.Once
	s.beforeCommit = func(int32) error {
		hookOnce.Do(func() {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
		})
		return ctx.Err()
	}
	primer := make(chan outcome, 1)
	go func() { receipt, err := s.Submit(ctx, a, 1); primer <- outcome{receipt, err} }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("primer transaction did not reach the controlled commit barrier")
	}
	type batchCall struct {
		inputs []Input
		frame  bool
	}
	calls := []batchCall{
		{inputs: []Input{{Token: a, Choice: 2}}},
		{inputs: []Input{{Token: b, Choice: 2}}},
		{inputs: []Input{{Token: c, Choice: 4}, {Token: a, Choice: 4}}, frame: true},
		{inputs: []Input{{Token: d, Choice: 1}}},
		{inputs: []Input{{Token: e, Choice: 2}}, frame: true},
		{inputs: []Input{{Token: f, Choice: 4}}},
		{inputs: []Input{{Token: g, Choice: 1}}},
	}
	results := make([]chan outcome, len(calls))
	expectedTimes := make([]time.Time, len(calls))
	admitted := uint64(1)
	for i, call := range calls {
		admission := cfg.StartsAt.Add(time.Duration(i+2) * time.Second)
		if i == len(calls)-1 {
			admission = cfg.EndsAt.Add(-time.Nanosecond)
		}
		expectedTimes[i] = admission
		clock.Store(admission.UnixNano())
		results[i] = make(chan outcome, 1)
		go func(i int, call batchCall) {
			if call.frame {
				r, err := s.SubmitFrame(ctx, call.inputs)
				if err == nil && r.Count != uint32(len(call.inputs)) {
					err = errors.New("explicit frame receipt count changed")
				}
				results[i] <- outcome{Receipt{Partition: r.Partition, Offset: r.Offset, AdmittedAt: r.AdmittedAt}, err}
			} else {
				r, err := s.Submit(ctx, call.inputs[0].Token, call.inputs[0].Choice)
				results[i] <- outcome{r, err}
			}
		}(i, call)
		admitted += uint64(len(call.inputs))
		kafkaWaitAdmitted(t, ctx, s, admitted)
	}
	clock.Store(cfg.EndsAt.UnixNano())
	if _, err := s.Submit(ctx, a, 4); !errors.Is(err, ErrClosed) {
		t.Fatalf("known token re-admitted after deadline: %v", err)
	}
	releaseOnce.Do(func() { close(release) })
	first := <-primer
	if first.err != nil {
		t.Fatal(first.err)
	}
	receipts := make([]Receipt, len(calls))
	for i := range results {
		got := <-results[i]
		if got.err != nil || !got.receipt.AdmittedAt.Equal(expectedTimes[i]) {
			t.Fatalf("admitted call %d changed across late commit: %+v", i, got)
		}
		receipts[i] = got.receipt
	}
	for _, pair := range [][2]int{{0, 1}, {5, 6}} {
		a, b := receipts[pair[0]], receipts[pair[1]]
		if a.Offset != b.Offset || a.Index != 0 || b.Index != 1 {
			t.Fatalf("adjacent singles did not share an ordered frame: %+v %+v", a, b)
		}
	}
	if receipts[2].Index != 0 || receipts[2].Offset == receipts[0].Offset || receipts[3].Offset == receipts[2].Offset || receipts[4].Offset == receipts[5].Offset {
		t.Fatal("explicit frame boundaries no longer match their receipts")
	}
	s.Close()
	// Recovery occurs before the real deadline. The saved per-poll config and
	// legacy codec still apply even though most previous calls are now frames.
	recovered, recoveredClock := kafkaTestStore(t, ctx, cfg)
	legacy, err := recovered.Submit(ctx, a, 4)
	if err != nil {
		t.Fatal(err)
	}
	explicit, err := recovered.SubmitFrame(ctx, []Input{{Token: b, Choice: 1}})
	if err != nil {
		t.Fatal(err)
	}
	audit := kafkaTestSeal(t, ctx, recovered, recoveredClock)
	if audit.TotalAttempts != 11 || len(audit.Canonical) != 7 || audit.Canonical[a].Choice != 1 || audit.ChoiceCounts != ([32]uint64{3, 2, 2}) {
		t.Fatalf("recovery changed first full-ID counts: attempts=%d unique=%d counts=%v", audit.TotalAttempts, len(audit.Canonical), audit.ChoiceCounts)
	}
	kafkaAssertReceipt(t, audit, first.receipt, a, 1)
	kafkaAssertReceipt(t, audit, legacy, a, 4)
	kafkaAssertReceipt(t, audit, Receipt{Partition: explicit.Partition, Offset: explicit.Offset, AdmittedAt: explicit.AdmittedAt}, b, 1)
	for i, call := range calls {
		for j, input := range call.inputs {
			receipt := receipts[i]
			if call.frame {
				receipt.Index = uint32(j)
			}
			kafkaAssertReceipt(t, audit, receipt, input.Token, input.Choice)
		}
	}
	recovered.Close()
	again, againClock := kafkaTestStore(t, ctx, cfg)
	if _, err := again.Submit(ctx, a, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("sealed journal reopened on second recovery: %v", err)
	}
	final := kafkaTestSeal(t, ctx, again, againClock)
	if final.TotalAttempts != audit.TotalAttempts || final.ChoiceCounts != audit.ChoiceCounts {
		t.Fatal("second recovery changed the final aggregate")
	}
	visited := 0
	if _, err := Replay(ctx, cfg, func(pos Position, vote Vote) error {
		if audit.Records[pos] != vote {
			return errors.New("streaming replay changed an exact offset/index/admitted vote")
		}
		visited++
		return nil
	}); err != nil || visited != 11 {
		t.Fatalf("complete independent replay: visited=%d err=%v", visited, err)
	}
}

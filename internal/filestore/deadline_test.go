package filestore

import (
	"context"
	"testing"
	"time"

	"gigaquizz/internal/filelog"
	"gigaquizz/internal/poll"
)

func TestObservedDeadlineCannotReopenAnotherPartition(t *testing.T) {
	for _, opened := range []bool{false, true} {
		t.Run(map[bool]string{false: "lazy", true: "live-group"}[opened], func(t *testing.T) {
			c := configForTest(t)
			c.Partitions = 2
			s := openTest(t, c)
			p, err := s.Create(context.Background(), poll.CreateInput{Question: "deadline", Type: "ab", Options: []string{"A", "B"}})
			if err != nil {
				t.Fatal(err)
			}
			e := s.lookup(p.ID)
			a, b := routedTokens(e, 0, 1)[0], routedTokens(e, 1, 1)[0]
			if opened {
				recorded(t, s, p, a, 1)
			}
			// Only the repository clock advances: a wrongly reopened real WAL
			// would still consider this original minute open and would ACK b.
			s.now = func() time.Time { return p.EndsAt }
			if r, err := s.Vote(context.Background(), p.ID, a, []int{1}); err != nil || r.Status != "closed" {
				t.Fatalf("deadline not observed: %+v %v", r, err)
			}
			s.now = func() time.Time { return p.StartsAt.Add(time.Second) }
			if r, err := s.Vote(context.Background(), p.ID, b, []int{2}); err != nil || r.Status != "closed" {
				t.Fatalf("different partition reopened after rollback: %+v %v", r, err)
			}
			if !opened && e.writer != nil {
				t.Fatal("closed lazy poll acquired a writer")
			}
		})
	}
}

type partlyClosedWriter struct {
	config filelog.GroupConfig
	now    func() time.Time
	calls  []int32
}

func (w *partlyClosedWriter) Submit(_ context.Context, in filelog.Input) (filelog.Receipt, error) {
	p := w.config.Partition(in.Token)
	w.calls = append(w.calls, p)
	if p == 0 {
		return filelog.Receipt{}, filelog.ErrClosed
	}
	return filelog.Receipt{Partition: p, AdmittedAt: w.now()}, nil
}
func (*partlyClosedWriter) Seal(context.Context) (filelog.Manifest, error) {
	return filelog.Manifest{}, filelog.ErrClosed
}
func (*partlyClosedWriter) Close()                     {}
func (*partlyClosedWriter) Metrics() map[string]uint64 { return nil }

func TestChildClosedMakesTheWholePollIrreversible(t *testing.T) {
	c := configForTest(t)
	c.Partitions = 2
	s := openTest(t, c)
	p, err := s.Create(context.Background(), poll.CreateInput{Question: "child barrier", Type: "ab", Options: []string{"A", "B"}})
	if err != nil {
		t.Fatal(err)
	}
	e := s.lookup(p.ID)
	a, b := routedTokens(e, 0, 1)[0], routedTokens(e, 1, 1)[0]
	// A child's irreversible CLOSED barrier wins even if the repository's wall
	// clock has already moved back inside the original admission window.
	s.now = func() time.Time { return p.EndsAt.Add(-time.Millisecond) }
	w := &partlyClosedWriter{config: e.groupConfig(), now: s.now}
	e.writer = w
	if r, err := s.Vote(context.Background(), p.ID, a, []int{1}); err != nil || r.Status != "closed" {
		t.Fatalf("child CLOSED was not returned: %+v %v", r, err)
	}
	s.now = func() time.Time { return p.StartsAt.Add(time.Second) }
	if r, err := s.Vote(context.Background(), p.ID, b, []int{2}); err != nil || r.Status != "closed" {
		t.Fatalf("another child accepted after CLOSED: %+v %v", r, err)
	}
	if len(w.calls) != 1 || w.calls[0] != 0 {
		t.Fatalf("late vote reached another child: %v", w.calls)
	}
}

// Keep the real journal and sync barrier; inject only a wall-clock rollback
// after Seal and a known diagnostic counter unavailable from replayed votes.
type rollbackAfterSeal struct {
	journalWriter
	rollback func()
}

func (w rollbackAfterSeal) Seal(ctx context.Context) (filelog.Manifest, error) {
	m, err := w.journalWriter.Seal(ctx)
	if err == nil {
		w.rollback()
	}
	return m, err
}
func (w rollbackAfterSeal) Metrics() map[string]uint64 {
	m := w.journalWriter.Metrics()
	m["busy_votes"] = 9
	return m
}

func TestFinalClockRollbackRetainsCountersAndSurvivesStrictRestart(t *testing.T) {
	c := configForTest(t)
	s := openTest(t, c)
	p := createEndingSoon(t, s, "ab", []string{"A", "B"})
	recorded(t, s, p, tokenA, 1)
	e := s.lookup(p.ID)
	e.writer = rollbackAfterSeal{e.writer, func() { s.now = func() time.Time { return p.EndsAt.Add(-time.Second) } }}
	before := s.Diagnostics()
	if before["storage_durable_votes"] != 1 || before["storage_busy_votes"] != 9 {
		t.Fatal("test did not observe live writer metrics")
	}
	finalizeTest(t, s, p)
	result, err := s.Results(context.Background(), p.ID)
	if err != nil || result.Pending || !result.CalculatedAt.Equal(p.EndsAt) {
		t.Fatalf("rollback published a self-invalid final: %+v %v", result, err)
	}
	if e.writer != nil {
		t.Fatal("finalized writer was not released")
	}
	for range 2 {
		after := s.Diagnostics()
		if after["storage_durable_votes"] != 1 || after["storage_durable_frames"] != 1 || after["storage_busy_votes"] != 9 || after["storage_batches"] != before["storage_batches"]+1 || after["storage_batch_ns"] < before["storage_batch_ns"] {
			t.Fatalf("final counters disappeared or doubled: before=%v after=%v", before, after)
		}
	}
	s.Close()
	s = openTest(t, c)
	stored, err := s.Results(context.Background(), p.ID)
	if err != nil || stored.Pending || stored.TotalVotes != 1 || !stored.CalculatedAt.Equal(result.CalculatedAt) {
		t.Fatalf("strict restart rejected final after clock rollback: %+v %v", stored, err)
	}
	if s.Diagnostics()["storage_busy_votes"] != 0 {
		t.Fatal("old runtime-only busy count invented on restart")
	}
}

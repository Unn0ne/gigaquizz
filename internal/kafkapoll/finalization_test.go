package kafkapoll

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"gigaquizz/internal/poll"
	"gigaquizz/internal/votelog"
)

func TestAdministrativeLockCancellationReleasesWaitingHandler(t *testing.T) {
	var m contextMutex
	m.Lock()
	defer m.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.LockContext(ctx) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled handler still waiting for administration lock")
	}
}
func checkpointEntry() *entry {
	now := time.Now().UTC()
	return &entry{poll: poll.Poll{ID: "unit", StartsAt: now, EndsAt: now.Add(time.Minute), Options: []poll.Option{{ID: 1, Label: "A"}, {ID: 2, Label: "B"}}}, config: votelog.Config{Topic: "gqlog_unit_fixture", PollID: [16]byte{1}, Partitions: 2, StartsAt: now, EndsAt: now.Add(time.Minute), AllowedMask: 3}}
}
func TestPartitionAggregationExactDuplicatesBoundAndIncompleteRead(t *testing.T) {
	e := checkpointEntry()
	a := [16]byte{1}
	b := a
	b[15] = 1
	input := []votelog.Vote{{Token: a, Choice: 1}, {Token: a, Choice: 2}, {Token: b, Choice: 2}}
	read := func(ctx context.Context, c votelog.Config, p int32, visit func(votelog.Position, votelog.Vote) error) (votelog.PartitionEnd, error) {
		for i, v := range input {
			if err := visit(votelog.Position{Partition: p, Offset: int64(i)}, v); err != nil {
				return votelog.PartitionEnd{}, err
			}
		}
		return votelog.PartitionEnd{Partition: p, Offset: 9}, nil
	}
	got, err := aggregatePartitionFrom(context.Background(), e, 0, 2, read)
	if err != nil || got.Total != 2 || !reflect.DeepEqual(got.Counts, []int64{1, 1}) || got.Closed != 9 {
		t.Fatalf("full-ID first choice mismatch: %+v %v", got, err)
	}
	if _, err := aggregatePartitionFrom(context.Background(), e, 0, 1, read); err == nil {
		t.Fatal("partial map published after cardinality bound")
	}
	broken := func(ctx context.Context, c votelog.Config, p int32, visit func(votelog.Position, votelog.Vote) error) (votelog.PartitionEnd, error) {
		_, _ = read(ctx, c, p, visit)
		return votelog.PartitionEnd{}, context.DeadlineExceeded
	}
	if result, err := aggregatePartitionFrom(context.Background(), e, 0, 2, broken); err == nil || result.Total != 0 {
		t.Fatal("partial reader result was published")
	}
}
func TestCheckpointRejectsChangedDefinitionAndInvalidCounts(t *testing.T) {
	e := checkpointEntry()
	p := partitionResult{Partition: 0, Definition: e.config.DefinitionHash(), Closed: 9, Total: 2, Counts: []int64{1, 1}}
	if err := validatePartition(e, p, 2); err != nil {
		t.Fatal(err)
	}
	sum := p.checksum()
	p.Counts = []int64{0, 2}
	if p.checksum() == sum {
		t.Fatal("count mutation retained checksum")
	}
	for _, change := range []func(*partitionResult){func(v *partitionResult) { v.Definition[0]++ }, func(v *partitionResult) { v.Closed = -1 }, func(v *partitionResult) { v.Partition = 2 }, func(v *partitionResult) { v.Total = 3 }, func(v *partitionResult) { v.Counts = []int64{3, 0} }, func(v *partitionResult) { v.Counts = []int64{0, 1} }} {
		next := p
		change(&next)
		if validatePartition(e, next, 2) == nil {
			t.Fatal("invalid checkpoint accepted")
		}
	}
}

func TestPersistedFinalRejectsCorruptCountsAndTimestamp(t *testing.T) {
	e := checkpointEntry()
	final := e.poll.EndsAt.Add(time.Second).UTC().Truncate(time.Microsecond)
	e.poll.FinalizedAt = &final
	baseline := poll.Results{PollID: e.poll.ID, State: "final", TotalVotes: 2, CalculatedAt: final.Add(321 * time.Nanosecond), Options: []poll.OptionCount{{ID: 1, Label: "A", Votes: 1}, {ID: 2, Label: "B", Votes: 1}}}
	data, _ := json.Marshal(baseline)
	if err := validateFinal(e, data); err != nil {
		t.Fatal("valid PG-microsecond timestamp rejected", err)
	}
	for _, change := range []func(*poll.Results){
		func(r *poll.Results) { r.Options[0].Votes = -1 }, func(r *poll.Results) { r.Options[0].Votes = 3 }, func(r *poll.Results) { r.Options[0].ID = 9 }, func(r *poll.Results) { r.Options[0].Label = "wrong" }, func(r *poll.Results) { r.TotalVotes = 3 }, func(r *poll.Results) { r.Pending = true }, func(r *poll.Results) { r.CalculatedAt = e.poll.EndsAt.Add(-time.Second) }, func(r *poll.Results) { r.CalculatedAt = final.Add(time.Second) },
	} {
		r := baseline
		r.Options = append([]poll.OptionCount(nil), baseline.Options...)
		change(&r)
		data, _ := json.Marshal(r)
		if validateFinal(e, data) == nil {
			t.Fatal("corrupt stored final was accepted")
		}
	}
}

func TestFinalTimeAfterClockRollbackStillValidatesOnRestart(t *testing.T) {
	e := checkpointEntry()
	e.poll.EndsAt = e.poll.EndsAt.UTC().Truncate(time.Microsecond)
	calculated := finalResultTime(e.poll.EndsAt.Add(-time.Minute), e.poll.EndsAt)
	persisted := calculated.Truncate(time.Microsecond)
	e.poll.FinalizedAt = &persisted
	r := poll.Results{PollID: e.poll.ID, State: "final", TotalVotes: 1, CalculatedAt: calculated, Options: []poll.OptionCount{{ID: 1, Label: "A", Votes: 1}, {ID: 2, Label: "B", Votes: 0}}}
	encoded, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if calculated.Before(e.poll.EndsAt) || validateFinal(e, encoded) != nil {
		t.Fatal("clock rollback produced an unreadable persisted final")
	}
}
func TestFinalizationRetainsOnlyKnownWriterLifetimeCounters(t *testing.T) {
	s := &Store{}
	s.retainMetrics(map[string]uint64{"committed_attempts": 12, "committed_frames": 3, "transactions": 4, "transaction_ns": 100, "busy_votes": 2, "queued_votes": 8, "active_votes": 9, "failed_writers": 1, "unexpected_private_label": 999})
	got := s.Diagnostics()
	if got["storage_durable_votes"] != 12 || got["storage_durable_frames"] != 3 || got["storage_batches"] != 4 || got["storage_batch_ns"] != 100 || got["storage_busy_votes"] != 2 {
		t.Fatal("released writer counters disappeared")
	}
	if got["storage_queued_votes"] != 0 || got["storage_active_votes"] != 0 || got["storage_failed_writers"] != 0 || len(s.retiredMetrics) != 5 {
		t.Fatal("retired gauges/private labels leaked into metrics")
	}
	if (&Store{}).Diagnostics()["storage_durable_votes"] != 0 {
		t.Fatal("a fresh Store invented historical vote counts")
	}
}

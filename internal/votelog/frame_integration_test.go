package votelog

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestKafkaFrameLegacyMixAndExactEntryPositionsAfterRecovery(t *testing.T) {
	ctx, cfg := kafkaTestConfig(t, 1)
	s, clock := kafkaTestStore(t, ctx, cfg)
	a, b := kafkaTestToken(t), kafkaTestToken(t)
	inputs := []Input{{Token: a, Choice: 2}, {Token: a, Choice: 1}, {Token: b, Choice: 4}}
	frame, err := s.SubmitFrame(ctx, inputs)
	if err != nil || frame.Count != 3 {
		t.Fatalf("frame result %+v %v", frame, err)
	}
	legacy, err := s.Submit(ctx, a, 4)
	if err != nil {
		t.Fatal(err)
	}
	audit := kafkaTestSeal(t, ctx, s, clock)
	if audit.TotalAttempts != 4 || len(audit.Canonical) != 2 || audit.Canonical[a].Choice != 2 || audit.ChoiceCounts != ([32]uint64{0, 1, 1}) {
		t.Fatalf("frame order/legacy counts changed: %+v", audit.ChoiceCounts)
	}
	for index, input := range inputs {
		kafkaAssertReceipt(t, audit, Receipt{Partition: frame.Partition, Offset: frame.Offset, Index: uint32(index), AdmittedAt: frame.AdmittedAt}, input.Token, input.Choice)
	}
	kafkaAssertReceipt(t, audit, legacy, a, 4)
	s.Close()
	recovered, recoveredClock := kafkaTestStore(t, ctx, cfg)
	if _, err := recovered.SubmitFrame(ctx, inputs); !errors.Is(err, ErrClosed) {
		t.Fatalf("reopened frame admission: %v", err)
	}
	again := kafkaTestSeal(t, ctx, recovered, recoveredClock)
	if again.TotalAttempts != 4 || again.Canonical[a].Choice != 2 {
		t.Fatal("recovery lost frame or first index choice")
	}
	var visited int
	manifest, err := Replay(ctx, cfg, func(pos Position, v Vote) error {
		if expected, ok := again.Records[pos]; !ok || expected != v {
			t.Error("streaming replay differs from exact audit")
		}
		visited++
		return nil
	})
	if err != nil || visited != 4 || len(manifest.Partitions) != 1 {
		t.Fatalf("streaming replay: %v count=%d", err, visited)
	}
}

func TestKafkaFrameAdmissionBeforeDeadlineMayCommitAfter(t *testing.T) {
	ctx, cfg := kafkaTestConfig(t, 1)
	s, clock := kafkaTestStore(t, ctx, cfg)
	clock.Store(cfg.EndsAt.Add(-100 * time.Millisecond).UnixNano())
	s.beforeCommit = func(int32) error { clock.Store(cfg.EndsAt.Add(200 * time.Millisecond).UnixNano()); return nil }
	inputs := []Input{{Token: kafkaTestToken(t), Choice: 1}, {Token: kafkaTestToken(t), Choice: 2}}
	frame, err := s.SubmitFrame(ctx, inputs)
	if err != nil || !frame.AdmittedAt.Equal(cfg.EndsAt.Add(-100*time.Millisecond)) {
		t.Fatalf("frame deadline: %+v %v", frame, err)
	}
	if _, err := s.SubmitFrame(ctx, inputs); !errors.Is(err, ErrClosed) {
		t.Fatalf("late frame accepted: %v", err)
	}
	audit := kafkaTestSeal(t, ctx, s, clock)
	for i, input := range inputs {
		kafkaAssertReceipt(t, audit, Receipt{Partition: frame.Partition, Offset: frame.Offset, Index: uint32(i), AdmittedAt: frame.AdmittedAt}, input.Token, input.Choice)
	}
}

func TestKafkaFramePostCommitReplyLossCannotProduceFalseSuccess(t *testing.T) {
	ctx, cfg := kafkaTestConfig(t, 1)
	s, _ := kafkaTestStore(t, ctx, cfg)
	s.afterCommit = func(int32) error { return errors.New("test reply loss after real frame commit") }
	token := kafkaTestToken(t)
	if _, err := s.SubmitFrame(ctx, []Input{{Token: token, Choice: 2}}); !errors.Is(err, ErrUnknown) {
		t.Fatalf("false known frame result: %v", err)
	}
	s.Close()
	recovered, clock := kafkaTestStore(t, ctx, cfg)
	if _, err := recovered.SubmitFrame(ctx, []Input{{Token: token, Choice: 1}}); err != nil {
		t.Fatal(err)
	}
	audit := kafkaTestSeal(t, ctx, recovered, clock)
	if audit.TotalAttempts != 2 || audit.Canonical[token].Choice != 2 {
		t.Fatal("unknown committed frame lost canonical first choice")
	}
}

func TestFrameQueueBoundsLogicalVotesNotNumberOfFrames(t *testing.T) {
	start := time.Now().Add(-time.Second)
	s := &Store{cfg: Config{Partitions: 1, BatchSize: 4, QueuePerPartition: 4, AllowedMask: 3, StartsAt: start, EndsAt: start.Add(time.Minute)}, ctx: context.Background(), now: time.Now}
	w := &writer{s: s, jobs: make(chan *pending, 4), queuedVotes: 3}
	s.writers = []*writer{w}
	w.jobs <- &pending{frame: make([]Vote, 3)}
	token := [16]byte{1}
	if _, err := s.SubmitFrame(context.Background(), []Input{{Token: token, Choice: 1}, {Token: token, Choice: 2}}); !errors.Is(err, ErrBusy) {
		t.Fatalf("frame count bypassed logical queue bound: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := s.Submit(ctx, token, 1); !errors.Is(err, ErrUnknown) {
		t.Fatal("queued legacy cancellation is unknown")
	}
	if w.queuedVotes != 4 || len(w.jobs) != 2 {
		t.Fatalf("mixed queue accounting: votes=%d jobs=%d", w.queuedVotes, len(w.jobs))
	}
	if _, err := s.SubmitFrame(context.Background(), []Input{{Token: token, Choice: 1}}); !errors.Is(err, ErrBusy) {
		t.Fatalf("full queue accepted more votes: %v", err)
	}
}

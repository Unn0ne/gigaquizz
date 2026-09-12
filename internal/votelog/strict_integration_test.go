package votelog

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaStrictReplayRejectsCommittedTailButValidatesRecoveryBoot(t *testing.T) {
	ctx, cfg := kafkaTestConfig(t, 1)
	s, clock := kafkaTestStore(t, ctx, cfg)
	token := kafkaTestToken(t)
	if _, err := s.Submit(ctx, token, 1); err != nil {
		t.Fatal(err)
	}
	kafkaTestSeal(t, ctx, s, clock)
	s.Close()
	initial, err := ReplayStrict(ctx, cfg, nil)
	if err != nil || initial.RecoveryBootRecords != 0 {
		t.Fatalf("initial stable CLOSED snapshot failed: recovery_boots=%d err=%v", initial.RecoveryBootRecords, err)
	}
	// New ownership is allowed to append a validated recovery BOOT after
	// CLOSED; that control record must be scanned, validated and counted.
	recovered, _ := kafkaTestStore(t, ctx, cfg)
	recovered.Close()
	withBoot, err := ReplayStrict(ctx, cfg, nil)
	if err != nil || withBoot.RecoveryBootRecords != 1 || withBoot.Manifest.Partitions[0].Offset != initial.Manifest.Partitions[0].Offset {
		t.Fatalf("valid post-CLOSED recovery BOOT was skipped/rejected: count=%d err=%v", withBoot.RecoveryBootRecords, err)
	}
	frame, err := encodeFrame(cfg, []Vote{{Token: token, Choice: 2, AdmittedAt: cfg.StartsAt.Add(time.Second)}})
	if err != nil {
		t.Fatal(err)
	}
	kafkaCommitCraftedFrame(t, ctx, cfg, 0, frame)
	wait, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		result, err := ReplayStrict(wait, cfg, nil)
		if err == nil || len(result.Manifest.Partitions) != 0 {
			t.Fatal("strict audit missed a real committed frame after CLOSED")
		}
		if strings.Contains(err.Error(), "strict journal frame outside open partition") {
			break // Prove the committed tail was seen, not merely an open LSO.
		}
		if wait.Err() != nil {
			t.Fatal("post-CLOSED test never observed the committed frame itself")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The production CLOSED-prefix contract remains intentionally unchanged.
	if _, err := Replay(ctx, cfg, nil); err != nil {
		t.Fatal("strict audit changed the production prefix reader")
	}
}

func TestKafkaStrictReplayOpenTransactionIsIncompleteAndAbortedTailIsTraversed(t *testing.T) {
	ctx, cfg := kafkaTestConfig(t, 1)
	s, clock := kafkaTestStore(t, ctx, cfg)
	kafkaTestSeal(t, ctx, s, clock)
	s.Close()
	frame, err := encodeFrame(cfg, []Vote{{Token: kafkaTestToken(t), Choice: 1, AdmittedAt: cfg.StartsAt.Add(time.Second)}})
	if err != nil {
		t.Fatal(err)
	}
	options := append(clientOptions(cfg), kgo.TransactionalID(cfg.Topic+"-strict-open-tail"), kgo.TransactionTimeout(10*time.Second))
	producer, err := kgo.NewClient(options...)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, &kgo.Record{Topic: cfg.Topic, Partition: 0, Value: frame}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if result, err := ReplayStrict(ctx, cfg, nil); err == nil || len(result.Manifest.Partitions) != 0 {
		t.Fatal("open transaction beyond CLOSED produced a false strict PASS")
	}
	if err := producer.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	// The coordinator response can precede marker visibility. Retry only this
	// explicitly incomplete snapshot while waiting for the real abort marker.
	wait, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		visits := 0
		result, err := ReplayStrict(wait, cfg, func(Position, Vote) error { visits++; return nil })
		if err == nil {
			if visits != 0 || result.SnapshotEndOffsets[0].Offset <= result.Manifest.Partitions[0].Offset+2 {
				t.Fatal("aborted physical tail was counted or left untraversed")
			}
			break
		}
		if wait.Err() != nil {
			t.Fatal("resolved aborted tail did not become strictly readable")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

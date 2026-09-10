package votelog

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaAbortedFrameDoesNotCountAfterOwnerRecovery(t *testing.T) {
	ctx, cfg := kafkaTestConfig(t, 1)
	s, _ := kafkaTestStore(t, ctx, cfg)
	s.beforeCommit = func(int32) error { return errors.New("test: stop after frame produce, before commit") }
	a, b := kafkaTestToken(t), kafkaTestToken(t)
	if _, err := s.SubmitFrame(ctx, []Input{{Token: a, Choice: 2}, {Token: a, Choice: 4}, {Token: b, Choice: 4}}); !errors.Is(err, ErrUnknown) {
		t.Fatalf("uncommitted packed frame received a known outcome: %v", err)
	}
	s.Close()
	// New ownership resolves the real open Kafka transaction. No entry from
	// the old frame may appear through read_committed, including its repeats.
	recovered, clock := kafkaTestStore(t, ctx, cfg)
	inputs := []Input{{Token: a, Choice: 1}, {Token: b, Choice: 2}}
	receipt, err := recovered.SubmitFrame(ctx, inputs)
	if err != nil {
		t.Fatal(err)
	}
	audit := kafkaTestSeal(t, ctx, recovered, clock)
	if audit.TotalAttempts != 2 || len(audit.Canonical) != 2 || audit.Canonical[a].Choice != 1 || audit.Canonical[b].Choice != 2 || audit.ChoiceCounts != ([32]uint64{1, 1}) {
		t.Fatalf("aborted packed entries affected final result: attempts=%d counts=%v", audit.TotalAttempts, audit.ChoiceCounts)
	}
	for i, input := range inputs {
		kafkaAssertReceipt(t, audit, Receipt{Partition: receipt.Partition, Offset: receipt.Offset, Index: uint32(i), AdmittedAt: receipt.AdmittedAt}, input.Token, input.Choice)
	}
}

// This helper deliberately bypasses application admission for protocol tests.
// Kafka still commits an actual RF3 transactional record; only its application
// content is invalid. It touches a newly created topic belonging to this test.
func kafkaCommitCraftedFrame(t *testing.T, parent context.Context, cfg Config, partition int32, value []byte) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	opts := append(clientOptions(cfg), kgo.TransactionalID(cfg.Topic+"-test-crafted-frame"), kgo.TransactionTimeout(cfg.TransactionTimeout))
	client, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	stop := context.AfterFunc(ctx, client.Close)
	defer stop()
	if err := client.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	r := &kgo.Record{Topic: cfg.Topic, Partition: partition, Value: value}
	if err := client.ProduceSync(ctx, r).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := client.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
	return r.Offset
}

func kafkaFrameTokenForPartition(t *testing.T, cfg Config, partition int32) [16]byte {
	t.Helper()
	for candidate := uint64(1); candidate <= 4096; candidate++ {
		var token [16]byte
		token[0] = 1
		binary.BigEndian.PutUint64(token[8:], candidate)
		if cfg.Partition(token) == partition {
			return token
		}
	}
	t.Fatal("could not construct bounded synthetic routing fixture")
	return [16]byte{}
}

func TestKafkaCommittedFrameWithWrongEntryRoutingFailsReplay(t *testing.T) {
	ctx, cfg := kafkaTestConfig(t, 2)
	s, clock := kafkaTestStore(t, ctx, cfg)
	admitted := cfg.StartsAt.Add(time.Second)
	value, err := encodeFrame(cfg, []Vote{
		{Token: kafkaFrameTokenForPartition(t, cfg, 0), Choice: 1, AdmittedAt: admitted},
		{Token: kafkaFrameTokenForPartition(t, cfg, 1), Choice: 2, AdmittedAt: admitted},
	})
	if err != nil {
		t.Fatal(err)
	}
	kafkaCommitCraftedFrame(t, ctx, cfg, 0, value)
	clock.Store(cfg.EndsAt.UnixNano())
	if _, err := s.Seal(ctx); err != nil {
		t.Fatal(err)
	}
	manifest, err := Replay(ctx, cfg, nil)
	if err == nil || !strings.Contains(err.Error(), "routing mismatch") || len(manifest.Partitions) != 0 {
		t.Fatalf("committed malformed routing yielded a complete result: err=%v manifest=%+v", err, manifest)
	}
}

func TestKafkaCommittedFrameAfterClosedPreventsOwnerRecovery(t *testing.T) {
	ctx, cfg := kafkaTestConfig(t, 1)
	s, clock := kafkaTestStore(t, ctx, cfg)
	audit := kafkaTestSeal(t, ctx, s, clock)
	closedOffset := audit.Manifest.Partitions[0].Offset
	s.Close()
	// A valid historical admission timestamp must not hide the fact that
	// this record was appended beyond the irreversible CLOSED boundary.
	value, err := encodeFrame(cfg, []Vote{{Token: kafkaTestToken(t), Choice: 1, AdmittedAt: cfg.StartsAt.Add(time.Second)}})
	if err != nil {
		t.Fatal(err)
	}
	if offset := kafkaCommitCraftedFrame(t, ctx, cfg, 0, value); offset <= closedOffset {
		t.Fatal("crafted frame did not follow the actual committed CLOSED")
	}
	unexpected, err := New(ctx, cfg)
	if unexpected != nil {
		unexpected.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "journal frame outside open partition") {
		t.Fatalf("recovery accepted a frame after CLOSED: %v", err)
	}
}

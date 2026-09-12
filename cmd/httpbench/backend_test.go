package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gigaquizz/internal/votelog"
)

// Metadata validation is local: this test never connects to a broker. The
// separate-process actual Kafka read is performed by the controlled harness.
func TestKafkaReaderRejectsMismatchedHTTPPollBeforeConnecting(t *testing.T) {
	m, _, _, _ := fixture(t)
	id, _ := parseID(m.Config.PollID)
	c := votelog.Config{PollID: id, StartsAt: m.Poll.StartsAt, EndsAt: m.Poll.EndsAt, AllowedMask: 3, Partitions: 4, Topic: "gqlog_app_test_only", Brokers: []string{"127.0.0.1:1"}, BatchSize: 256, QueuePerPartition: 2048, Linger: 2 * time.Millisecond, TransactionTimeout: 10 * time.Second}
	path := filepath.Join(t.TempDir(), "private-config.json")
	write := func() {
		b, _ := json.Marshal(c)
		if err := os.WriteFile(path, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write()
	if _, err := backend(m, "", path); err != nil {
		t.Fatal(err)
	}
	c.EndsAt = c.EndsAt.Add(time.Nanosecond)
	write()
	if _, err := backend(m, "", path); err == nil {
		t.Fatal("accepted changed immutable deadline")
	}
	c.EndsAt = m.Poll.EndsAt
	c.PollID[0] ^= 1
	write()
	if _, err := backend(m, "", path); err == nil {
		t.Fatal("accepted different poll")
	}
	if _, err := backend(m, "wrong-branch", path); err == nil {
		t.Fatal("accepted wrong backend input")
	}
}

func strictFixture(t *testing.T) (votelog.Config, votelog.StrictReplayResult) {
	t.Helper()
	m, _, _, _ := fixture(t)
	id, _ := parseID(m.Config.PollID)
	c := votelog.Config{PollID: id, StartsAt: m.Poll.StartsAt, EndsAt: m.Poll.EndsAt, AllowedMask: 3, Partitions: 2, Topic: "gqlog_app_test_only"}
	r := votelog.StrictReplayResult{Manifest: votelog.Manifest{Topic: c.Topic, StartsAt: c.StartsAt, EndsAt: c.EndsAt, Partitions: []votelog.PartitionEnd{{Partition: 0, Offset: 3}, {Partition: 1, Offset: 2}}}, SnapshotEndOffsets: []votelog.PartitionEnd{{Partition: 0, Offset: 7}, {Partition: 1, Offset: 3}}, RecoveryBootRecords: 1}
	return c, r
}

func TestStrictReaderPublishesEvidenceOnlyForCompleteSnapshot(t *testing.T) {
	c, result := strictFixture(t)
	var calls int
	tailErr := errors.New("synthetic changing snapshot")
	reader := kafkaReader(c, func(ctx context.Context, got votelog.Config, visit func(votelog.Position, votelog.Vote) error) (votelog.StrictReplayResult, error) {
		calls++
		if got.Topic != c.Topic {
			t.Fatal("wrong strict reader config")
		}
		if err := visit(votelog.Position{Partition: 0, Offset: 1}, votelog.Vote{Token: [16]byte{1}, Choice: 1, AdmittedAt: c.StartsAt.Add(time.Second)}); err != nil {
			return votelog.StrictReplayResult{}, err
		}
		if calls > 1 {
			return votelog.StrictReplayResult{}, tailErr
		}
		return result, nil
	})
	var visited int
	visit := func(v journalVote) error {
		visited++
		if v.Token != [16]byte{1} || v.Choice != 1 || !v.AdmittedAt.Equal(c.StartsAt.Add(time.Second)) {
			t.Fatal("strict vote mapping changed")
		}
		return nil
	}
	if reader.Inspection.StrictSnapshotComplete {
		t.Fatal("inspection complete before reading")
	}
	if err := reader.Replay(context.Background(), visit); err != nil {
		t.Fatal(err)
	}
	if !reader.Inspection.StrictSnapshotComplete || reader.Inspection.RecoveryBootRecords != 1 || reader.Inspection.SnapshotPartitions != 2 || visited != 1 {
		t.Fatalf("wrong successful evidence: %+v", reader.Inspection)
	}
	if err := reader.Replay(context.Background(), visit); !errors.Is(err, tailErr) {
		t.Fatalf("strict error was hidden: %v", err)
	}
	if reader.Inspection.StrictSnapshotComplete || reader.Inspection.RecoveryBootRecords != 0 || reader.Inspection.SnapshotPartitions != 0 || visited != 2 {
		t.Fatalf("partial visits retained complete evidence: %+v", reader.Inspection)
	}
}

func TestStrictReaderRejectsIncompleteSnapshotMapping(t *testing.T) {
	for _, kind := range []string{"missing_partition", "wrong_partition", "end_at_closed", "missing_closed", "wrong_topic", "cancelled_after_visit"} {
		t.Run(kind, func(t *testing.T) {
			c, result := strictFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch kind {
			case "missing_partition":
				result.SnapshotEndOffsets = result.SnapshotEndOffsets[:1]
			case "wrong_partition":
				result.SnapshotEndOffsets[1].Partition = 0
			case "end_at_closed":
				result.SnapshotEndOffsets[1].Offset = result.Manifest.Partitions[1].Offset
			case "missing_closed":
				result.Manifest.Partitions = result.Manifest.Partitions[:1]
			case "wrong_topic":
				result.Manifest.Topic = "gqlog_wrong_snapshot"
			}
			reader := kafkaReader(c, func(context.Context, votelog.Config, func(votelog.Position, votelog.Vote) error) (votelog.StrictReplayResult, error) {
				if kind == "cancelled_after_visit" {
					cancel()
				}
				return result, nil
			})
			if err := reader.Replay(ctx, func(journalVote) error { return nil }); err == nil {
				t.Fatal("accepted incomplete strict snapshot")
			}
			if reader.Inspection.StrictSnapshotComplete || reader.Inspection.SnapshotPartitions != 0 {
				t.Fatalf("published partial snapshot: %+v", reader.Inspection)
			}
		})
	}
}

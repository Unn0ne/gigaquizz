package kafkapoll

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"gigaquizz/internal/poll"
	"gigaquizz/internal/votelog"
)

// Real fencing injects a terminal producer failure in exactly one partition of
// a fresh isolated poll. The same application controller must publish the final
// result on its next post-deadline maintenance call, without an app restart.
func TestTerminalWriterFailureFinalizesWithoutApplicationRestart(t *testing.T) {
	if os.Getenv("GIGAQUIZZ_APP_KAFKA_TEST") != "1" {
		t.Skip("requires isolated PostgreSQL/Kafka integration gate")
	}
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("TEST_DATABASE_URL required")
	}
	id, _, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 115*time.Second)
	defer cancel()
	s, err := New(ctx, Options{DatabaseURL: dsn, Schema: "gq_app_test_" + strings.ReplaceAll(id, "-", ""), Brokers: strings.Split(brokers, ","), Partitions: 2, PreparationLead: 5 * time.Second, MaxUnique: 100, MaxPolls: 10})
	if err != nil {
		t.Fatal("isolated controller initialization failed", err)
	}
	defer s.Close()
	p, err := s.Create(ctx, poll.CreateInput{Question: "Terminal writer automatic recovery", Type: "single", Options: []string{"A", "B"}})
	if err != nil {
		t.Fatal("create isolated poll", err)
	}
	e := s.polls[p.ID]
	old := e.writer
	definition := e.config.DefinitionHash()
	tokenFor := func(partition int32) ([16]byte, string) {
		t.Helper()
		for i := 0; i < 1024; i++ {
			_, token, err := newID()
			if err != nil {
				t.Fatal(err)
			}
			if e.config.Partition(token) == partition {
				return token, hex.EncodeToString(token[:])
			}
		}
		t.Fatal("could not create bounded partition fixture")
		return [16]byte{}, ""
	}
	a, aText := tokenFor(0)
	b, bText := tokenFor(1)
	wait := func(deadline time.Time) {
		t.Helper()
		timer := time.NewTimer(max(time.Until(deadline), 0))
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			t.Fatal("isolated test deadline exceeded")
		}
	}
	wait(p.StartsAt)
	ackA, err := s.Vote(ctx, p.ID, aText, []int{1})
	if err != nil || ackA.Status != "recorded" || ackA.AcceptedAt == nil {
		t.Fatal("initial attempt was not acknowledged", err)
	}
	fenceConfig := e.config
	fenceConfig.OwnedPartitions = []int32{0}
	fencer, err := votelog.New(ctx, fenceConfig)
	if err != nil {
		t.Fatal("isolated producer fencing failed", err)
	}
	fencer.Close()
	if r, err := s.Vote(ctx, p.ID, aText, []int{2}); err == nil || r.Status == "recorded" {
		t.Fatal("fenced old epoch claimed a known receipt")
	}
	if old.Metrics()["failed_writers"] != 1 || old.Metrics()["writer_failures_fenced"] != 1 {
		t.Fatal("real fencing did not produce exactly one classified terminal writer")
	}
	ackB, err := s.Vote(ctx, p.ID, bText, []int{2})
	if err != nil || ackB.Status != "recorded" || ackB.AcceptedAt == nil {
		t.Fatal("healthy partition stopped admitting", err)
	}
	if n, err := s.FinalizeDue(ctx); err != nil || n != 0 || e.writer != old {
		t.Fatal("terminal epoch was automatically reacquired while the window was open", n, err)
	}
	wait(p.EndsAt)
	if n, err := s.FinalizeDue(ctx); err != nil || n != 1 {
		t.Fatal("maintenance did not finalize in the same controller", n, err)
	}
	result, err := s.Results(ctx, p.ID)
	if err != nil || result.Pending || result.State != "final" || result.TotalVotes != 2 || result.Options[0].Votes != 1 || result.Options[1].Votes != 1 {
		t.Fatal("recovery changed exact first choices", err)
	}
	if e.config.DefinitionHash() != definition || e.writer != nil || !e.admissionClosed.Load() {
		t.Fatal("recovery changed journal identity or left admission open")
	}
	metrics := s.Diagnostics()
	if metrics["storage_durable_votes"] != 2 || metrics["storage_failed_writers"] != 0 || metrics["storage_writer_failures_fenced"] != 1 {
		t.Fatal("recovery lost/doubled ACK counters or concealed the retired failure", metrics)
	}
	if r, err := s.Vote(ctx, p.ID, aText, []int{2}); err != nil || r.Status != "closed" {
		t.Fatal("recovery reopened an ended poll")
	}
	seen := make(map[[16]byte]votelog.Vote)
	strict, err := votelog.ReplayStrict(ctx, e.config, func(_ votelog.Position, vote votelog.Vote) error {
		if _, exists := seen[vote.Token]; exists {
			return errors.New("unexpected duplicate attempt after fencing")
		}
		seen[vote.Token] = vote
		return nil
	})
	// This counter includes only BOOT after CLOSED: the healthy partition was
	// already sealed before recovery; the failed partition receives its first
	// CLOSED afterwards. Earlier BOOT records are validated but not counted here.
	if err != nil || len(seen) != 2 || len(strict.Manifest.Partitions) != 2 || len(strict.SnapshotEndOffsets) != 2 || strict.RecoveryBootRecords != 1 {
		t.Fatalf("complete independent recovery audit failed: votes=%d closed=%d snapshots=%d post_closed_boots=%d err=%v", len(seen), len(strict.Manifest.Partitions), len(strict.SnapshotEndOffsets), strict.RecoveryBootRecords, err)
	}
	if seen[a].Choice != 1 || !seen[a].AdmittedAt.Equal(*ackA.AcceptedAt) || seen[b].Choice != 2 || !seen[b].AdmittedAt.Equal(*ackB.AcceptedAt) {
		t.Fatal("recovery modified original admitted vote data")
	}
	if n, err := s.FinalizeDue(ctx); err != nil || n != 0 || s.Diagnostics()["storage_durable_votes"] != 2 {
		t.Fatal("completed maintenance recounted or reopened an epoch", n, err)
	}
	t.Log("same controller: 2 durable ACKs preserved, 1 fenced unknown excluded, 2 exact first choices, 2 strict snapshot partitions; no application restart")
}

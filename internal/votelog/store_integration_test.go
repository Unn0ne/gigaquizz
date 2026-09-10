package votelog

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These tests create small, isolated topics in the owned native lab. They do
// not stop or reconfigure brokers, and must run separately from load profiles.
func kafkaTestConfig(t *testing.T, partitions int) (context.Context, Config) {
	t.Helper()
	if os.Getenv("GIGAQUIZZ_KAFKA_TEST") != "1" {
		t.Skip("requires GIGAQUIZZ_KAFKA_TEST=1 and the owned three-broker Kafka lab")
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate owned Kafka lab")
	}
	lab := filepath.Join(filepath.Dir(source), "../../.local/kafka-lab")
	marker, err := os.ReadFile(filepath.Join(lab, ".gigaquizz-kafka-lab"))
	if err != nil || string(marker) != "gigaquizz native Kafka lab v1\n" {
		t.Fatal("run the owned Kafka lab before enabling integration tests")
	}
	var metadata struct {
		Bootstrap string `json:"bootstrap_servers"`
	}
	data, err := os.ReadFile(filepath.Join(lab, "metadata.json"))
	if err != nil || json.Unmarshal(data, &metadata) != nil || metadata.Bootstrap == "" {
		t.Fatal("invalid owned Kafka lab metadata")
	}
	id := kafkaTestToken(t)
	start := time.Now().UTC().Add(-time.Second)
	cfg := Config{
		Brokers: strings.Split(metadata.Bootstrap, ","), Topic: "gqlog_test_" + hex.EncodeToString(id[:]),
		PollID: id, Partitions: partitions, StartsAt: start, EndsAt: start.Add(time.Minute),
		AllowedMask: 7, BatchSize: 16, Linger: 2 * time.Millisecond,
		QueuePerPartition: 128, TransactionTimeout: 10 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	if err := CreateTopic(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	// Retain these synthetic topics for inspection; the lab's bounded retention
	// applies. No user topic or externally configured bootstrap server is used.
	t.Logf("owned test topic: %s", cfg.Topic)
	return ctx, cfg
}

func kafkaTestToken(t *testing.T) [16]byte {
	t.Helper()
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		t.Fatal(err)
	}
	return token
}

func kafkaTestStore(t *testing.T, ctx context.Context, cfg Config) (*Store, *atomic.Int64) {
	t.Helper()
	s, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	clock := new(atomic.Int64)
	clock.Store(cfg.StartsAt.Add(time.Second).UnixNano())
	s.now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	return s, clock
}

func kafkaTestSeal(t *testing.T, ctx context.Context, s *Store, clock *atomic.Int64) AuditResult {
	t.Helper()
	clock.Store(s.cfg.EndsAt.UnixNano())
	manifest, err := s.Seal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Partitions) != s.cfg.Partitions {
		t.Fatalf("incomplete CLOSED manifest: %+v", manifest)
	}
	audit, err := Audit(ctx, s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	return audit
}

func kafkaAssertReceipt(t *testing.T, audit AuditResult, receipt Receipt, token [16]byte, choice uint32) {
	t.Helper()
	vote, ok := audit.Records[Position{Partition: receipt.Partition, Offset: receipt.Offset, Index: receipt.Index}]
	if !ok || vote.Token != token || vote.Choice != choice || !vote.AdmittedAt.Equal(receipt.AdmittedAt) {
		t.Fatalf("confirmed receipt absent or changed at partition=%d offset=%d", receipt.Partition, receipt.Offset)
	}
}

func TestKafkaFirstCommittedChoiceAndConcurrentDuplicates(t *testing.T) {
	ctx, cfg := kafkaTestConfig(t, 2)
	s, clock := kafkaTestStore(t, ctx, cfg)
	token := kafkaTestToken(t)
	first, err := s.Submit(ctx, token, 1)
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		receipt Receipt
		choice  uint32
		err     error
	}
	const repeats = 32
	results := make(chan result, repeats)
	var wg sync.WaitGroup
	for i := 0; i < repeats; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			choice := uint32(1 << (i % 2))
			receipt, err := s.Submit(ctx, token, choice)
			results <- result{receipt, choice, err}
		}(i)
	}
	wg.Wait()
	close(results)
	if _, err := s.Submit(ctx, kafkaTestToken(t), 3); !errors.Is(err, ErrInvalid) {
		t.Fatalf("single-choice poll accepted multi-choice mask: %v", err)
	}
	audit := kafkaTestSeal(t, ctx, s, clock)
	if audit.TotalAttempts != repeats+1 || len(audit.Canonical) != 1 || audit.Canonical[token].Choice != 1 {
		t.Fatalf("first committed choice did not remain canonical: attempts=%d unique=%d choice=%d", audit.TotalAttempts, len(audit.Canonical), audit.Canonical[token].Choice)
	}
	kafkaAssertReceipt(t, audit, first, token, 1)
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		kafkaAssertReceipt(t, audit, result.receipt, token, result.choice)
	}
	if audit.ChoiceCounts[0] != 1 || audit.ChoiceCounts[1] != 0 {
		t.Fatalf("duplicate attempts increased final counters: %v", audit.ChoiceCounts)
	}
}

func TestKafkaAdmissionBeforeDeadlineMayCommitAfter(t *testing.T) {
	ctx, cfg := kafkaTestConfig(t, 1)
	s, clock := kafkaTestStore(t, ctx, cfg)
	clock.Store(cfg.EndsAt.Add(-100 * time.Millisecond).UnixNano())
	s.beforeCommit = func(int32) error {
		clock.Store(cfg.EndsAt.Add(200 * time.Millisecond).UnixNano())
		return nil
	}
	token := kafkaTestToken(t)
	receipt, err := s.Submit(ctx, token, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !receipt.AdmittedAt.Equal(cfg.EndsAt.Add(-100 * time.Millisecond)) {
		t.Fatalf("admission timestamp was reassigned during batch commit: %s", receipt.AdmittedAt)
	}
	if _, err := s.Submit(ctx, kafkaTestToken(t), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("new attempt admitted after deadline: %v", err)
	}
	audit := kafkaTestSeal(t, ctx, s, clock)
	kafkaAssertReceipt(t, audit, receipt, token, 2)
	if len(audit.Canonical) != 1 {
		t.Fatalf("late new attempt reached final result: unique=%d", len(audit.Canonical))
	}
}

func TestKafkaClosedSurvivesOwnerTransferAndClockRollback(t *testing.T) {
	ctx, cfg := kafkaTestConfig(t, 2)
	s, clock := kafkaTestStore(t, ctx, cfg)
	token := kafkaTestToken(t)
	receipt, err := s.Submit(ctx, token, 1)
	if err != nil {
		t.Fatal(err)
	}
	kafkaTestSeal(t, ctx, s, clock)
	s.Close()
	recovered, recoveredClock := kafkaTestStore(t, ctx, cfg)
	// The injected clock is back inside the original voting window. The
	// committed close barrier, rather than a volatile timer, must prevail.
	if _, err := recovered.Submit(ctx, kafkaTestToken(t), 2); !errors.Is(err, ErrClosed) {
		t.Fatalf("recovery reopened CLOSED partition after clock rollback: %v", err)
	}
	audit := kafkaTestSeal(t, ctx, recovered, recoveredClock)
	kafkaAssertReceipt(t, audit, receipt, token, 1)
	changed := cfg
	changed.AllowedMask = 3
	if unexpected, err := New(ctx, changed); err == nil {
		unexpected.Close()
		t.Fatal("existing committed journal was rebound to a different immutable poll definition")
	}
}

func TestKafkaAbortedAttemptCannotOverrideLaterCommittedChoice(t *testing.T) {
	ctx, cfg := kafkaTestConfig(t, 1)
	s, _ := kafkaTestStore(t, ctx, cfg)
	s.beforeCommit = func(int32) error { return errors.New("test: stop after ProduceSync, before commit") }
	token := kafkaTestToken(t)
	if _, err := s.Submit(ctx, token, 1); !errors.Is(err, ErrUnknown) {
		t.Fatalf("uncommitted transaction received success: %v", err)
	}
	s.Close()
	// A real new producer with the same transactional ID resolves the old
	// open transaction. read_committed replay must cross its hidden offsets.
	recovered, clock := kafkaTestStore(t, ctx, cfg)
	receipt, err := recovered.Submit(ctx, token, 2)
	if err != nil {
		t.Fatal(err)
	}
	audit := kafkaTestSeal(t, ctx, recovered, clock)
	kafkaAssertReceipt(t, audit, receipt, token, 2)
	if audit.TotalAttempts != 1 || audit.Canonical[token].Choice != 2 {
		t.Fatalf("aborted first choice leaked through read_committed: attempts=%d choice=%d", audit.TotalAttempts, audit.Canonical[token].Choice)
	}
}

func TestKafkaPostCommitReplyLossRemainsUnknownAndRecoverable(t *testing.T) {
	ctx, cfg := kafkaTestConfig(t, 1)
	s, _ := kafkaTestStore(t, ctx, cfg)
	s.afterCommit = func(int32) error { return errors.New("test: withhold response after real commit") }
	token := kafkaTestToken(t)
	if _, err := s.Submit(ctx, token, 1); !errors.Is(err, ErrUnknown) {
		t.Fatalf("lost commit response was reported as known: %v", err)
	}
	if _, err := s.Submit(ctx, kafkaTestToken(t), 2); !errors.Is(err, ErrUnknown) {
		t.Fatalf("uncertain writer admitted a later confirmed transaction: %v", err)
	}
	s.Close()
	recovered, clock := kafkaTestStore(t, ctx, cfg)
	receipt, err := recovered.Submit(ctx, token, 2)
	if err != nil {
		t.Fatal(err)
	}
	audit := kafkaTestSeal(t, ctx, recovered, clock)
	kafkaAssertReceipt(t, audit, receipt, token, 2)
	if audit.TotalAttempts != 2 || audit.Canonical[token].Choice != 1 {
		t.Fatalf("unknown-but-committed first choice was lost: attempts=%d choice=%d", audit.TotalAttempts, audit.Canonical[token].Choice)
	}
}

func TestKafkaFencedOwnerCannotReinitializeItself(t *testing.T) {
	ctx, cfg := kafkaTestConfig(t, 1)
	old, _ := kafkaTestStore(t, ctx, cfg)
	firstToken := kafkaTestToken(t)
	first, err := old.Submit(ctx, firstToken, 1)
	if err != nil {
		t.Fatal(err)
	}
	// This explicit, test-controlled transfer uses Kafka's real producer
	// fencing while deliberately leaving the old application object alive.
	recovered, clock := kafkaTestStore(t, ctx, cfg)
	rejectedToken := kafkaTestToken(t)
	for i := 0; i < 3; i++ {
		if _, err := old.Submit(ctx, rejectedToken, 2); !errors.Is(err, ErrUnknown) {
			t.Fatalf("fenced owner produced a positive receipt on attempt %d: %v", i, err)
		}
	}
	secondToken := kafkaTestToken(t)
	second, err := recovered.Submit(ctx, secondToken, 2)
	if err != nil {
		t.Fatalf("old owner re-fenced its replacement: %v", err)
	}
	audit := kafkaTestSeal(t, ctx, recovered, clock)
	kafkaAssertReceipt(t, audit, first, firstToken, 1)
	kafkaAssertReceipt(t, audit, second, secondToken, 2)
	if _, ok := audit.Canonical[rejectedToken]; ok || audit.TotalAttempts != 2 {
		t.Fatalf("fenced write reached committed replay: attempts=%d", audit.TotalAttempts)
	}
}

func TestKafkaCallerCancellationDoesNotCancelAdmittedCommit(t *testing.T) {
	ctx, cfg := kafkaTestConfig(t, 1)
	s, clock := kafkaTestStore(t, ctx, cfg)
	entered, release, committed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	s.beforeCommit = func(int32) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.afterCommit = func(int32) error { close(committed); return nil }
	request, cancel := context.WithCancel(ctx)
	defer cancel()
	token := kafkaTestToken(t)
	result := make(chan error, 1)
	go func() { _, err := s.Submit(request, token, 1); result <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("transaction never reached real pre-commit barrier")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, ErrUnknown) {
			t.Fatalf("cancelled caller received a known outcome: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("cancelled caller did not return")
	}
	clock.Store(cfg.EndsAt.UnixNano())
	releaseOnce.Do(func() { close(release) })
	select {
	case <-committed:
	case <-ctx.Done():
		t.Fatal("caller cancellation prevented detached transaction commit")
	}
	audit := kafkaTestSeal(t, ctx, s, clock)
	if audit.TotalAttempts != 1 || audit.Canonical[token].Choice != 1 {
		t.Fatalf("admitted operation disappeared after caller cancellation: attempts=%d", audit.TotalAttempts)
	}
}

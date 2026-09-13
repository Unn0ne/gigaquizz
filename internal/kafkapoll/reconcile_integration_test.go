package kafkapoll

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"gigaquizz/internal/poll"
	"github.com/jackc/pgx/v5"
)

func TestCreateCommitReplyLossReconcilesWithoutRestart(t *testing.T) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	s, err := New(ctx, Options{DatabaseURL: dsn, Schema: "gq_app_test_" + strings.ReplaceAll(id, "-", ""), Brokers: strings.Split(brokers, ","), Partitions: 2, MaxUnique: 100, MaxPolls: 10})
	if err != nil {
		t.Fatal("isolated repository initialization failed")
	}
	defer s.Close()
	request, abort := context.WithCancel(ctx)
	s.afterCreateCommit = func(err error) error {
		if err != nil {
			return err
		}
		abort()
		return errors.New("simulated COMMIT reply loss")
	}
	start := time.Now().UTC().Add(time.Minute)
	if _, err := s.Create(request, poll.CreateInput{Question: "Reconcile lost creation reply", Type: "single", Options: []string{"A", "B"}, StartsAt: &start}); err == nil {
		t.Fatal("lost reply became known successful creation")
	}
	list, err := s.List(ctx)
	if err != nil || len(list) != 1 {
		t.Fatal("committed poll is invisible without restart", err)
	}
	s.afterCreateCommit = nil
	if _, err := s.FinalizeDue(ctx); err != nil {
		t.Fatal("maintenance did not prepare reconciled poll", err)
	}
	s.mu.RLock()
	ready := s.polls[list[0].ID].writer != nil
	s.mu.RUnlock()
	if !ready {
		t.Fatal("reconciled poll remained unprepared")
	}
}

func TestScheduleTransactionsSerializeAcrossIndependentConnections(t *testing.T) {
	if os.Getenv("GIGAQUIZZ_APP_KAFKA_TEST") != "1" {
		t.Skip("requires isolated PostgreSQL integration gate")
	}
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("TEST_DATABASE_URL required")
	}
	idA, _, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	idB, _, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	schema := "gq_app_test_" + strings.ReplaceAll(idA, "-", "")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := Migrate(ctx, dsn, schema); err != nil {
		t.Fatal("isolated migration failed")
	}
	config, err := poolConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	a, err := pgx.ConnectConfig(ctx, config.ConnConfig.Copy())
	if err != nil {
		t.Fatal("connection A unavailable")
	}
	defer a.Close(ctx)
	b, err := pgx.ConnectConfig(ctx, config.ConnConfig.Copy())
	if err != nil {
		t.Fatal("connection B unavailable")
	}
	defer b.Close(ctx)
	txA, err := a.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(txA)
	txB, err := b.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(txB)
	s := &Store{opts: Options{Schema: schema}}
	now := time.Now().UTC().Truncate(time.Microsecond)
	pA := poll.Poll{ID: idA, StartsAt: now, EndsAt: now.Add(time.Minute)}
	pB := pA
	pB.ID = idB
	if err := s.insertDefinition(ctx, txA, pA, []byte(`{}`), []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.insertDefinition(ctx, txB, pB, []byte(`{}`), []byte(`{}`)) }()
	select {
	case err := <-done:
		t.Fatalf("second transaction bypassed pending schedule mutation: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := txA.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, poll.ErrOverlap) {
			t.Fatal("overlap remained possible after first commit", err)
		}
	case <-ctx.Done():
		t.Fatal("schedule lock did not release")
	}
}

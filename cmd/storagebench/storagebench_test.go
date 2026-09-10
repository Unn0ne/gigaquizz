package main

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gigaquizz/internal/poll"
	"github.com/jackc/pgx/v5"
)

func TestConfigurationBounds(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*config)
		bad  bool
	}{
		{"default", func(c *config) {}, false},
		{"hard key limit even with opt-in", func(c *config) { c.total = maximumKeys + 1; c.rate = 200000; c.allowLarge = true }, true},
		{"window too long", func(c *config) { c.total = 56; c.rate = 1 }, true},
		{"55 second window", func(c *config) { c.total = 55; c.rate = 1 }, false},
		{"maximum explicit key count", func(c *config) { c.total = maximumKeys; c.rate = 10000; c.allowLarge = true }, false},
		{"maximum keys require opt in", func(c *config) { c.total = maximumKeys; c.rate = 10000 }, true},
		{"warm vote connections", func(c *config) { c.warmConnections = true }, false},
		{"warm blind rejected", func(c *config) { c.mode = "blind"; c.warmConnections = true }, true},
		{"duplicate load counts", func(c *config) { c.rate = 600; c.repeatEvery = 1 }, true},
		{"large repeat interval does not overflow", func(c *config) { c.repeatEvery = math.MaxInt; c.rate = 1001 }, true},
		{"control disallows duplicate interpretation", func(c *config) { c.mode = "blind"; c.repeatEvery = 1 }, true},
		{"quorum without candidates", func(c *config) { c.requiredStandbys = 1 }, true},
		{"standby names without policy", func(c *config) { c.standbyNames = "gigaquizz_ha_a" }, true},
		{"explicit quorum", func(c *config) { c.requiredStandbys = 1; c.standbyNames = "gigaquizz_ha_a,gigaquizz_ha_b" }, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := defaults()
			tt.edit(&c)
			if (c.validate() != nil) != tt.bad {
				t.Fatal("unexpected validation outcome")
			}
		})
	}
}

func TestSearchPathPinnedForURLAndKeywordDSN(t *testing.T) {
	const schema = "gqbench_0011223344556677"
	for _, dsn := range []string{"postgres://test@localhost/test?search_path=public&sslmode=disable", "host=localhost user=test dbname=test search_path=public sslmode=disable"} {
		pinned, err := pinSchema(dsn, schema)
		if err != nil {
			t.Fatal("pin failed")
		}
		parsed, err := pgx.ParseConfig(pinned)
		if err != nil {
			t.Fatal("pinned DSN is invalid")
		}
		if parsed.RuntimeParams["search_path"] != schema+",pg_catalog" {
			t.Fatal("search path escaped fixture")
		}
	}
	if _, err := pinSchema("postgres://test@localhost/test", "public"); err == nil {
		t.Fatal("arbitrary schema accepted")
	}
}

func TestBoundedSchedulerAndBulkLagAccounting(t *testing.T) {
	c := defaults()
	c.total = 10
	c.rate = 100
	c.repeatEvery = 2
	c.maxLag = time.Second
	m, err := newObservations(c.total)
	if err != nil {
		t.Fatal(err)
	}
	jobs := make(chan attemptJob, 2)
	schedule(context.Background(), c, time.Now().Add(-200*time.Millisecond), jobs, m)
	if m.scheduled != 10 || m.queued != 2 || m.skips["queue_full"] != 13 {
		t.Fatal("bounded queue accounting is incorrect")
	}
	c.total = maximumKeys
	c.rate = 200000
	c.maxLag = time.Millisecond
	m, err = newObservations(c.total)
	if err != nil {
		t.Fatal(err)
	}
	schedule(context.Background(), c, time.Now().Add(-4*time.Second), make(chan attemptJob, 1), m)
	if m.scheduled != maximumKeys || m.skips["scheduler_lag"] != maximumKeys+maximumKeys/2 {
		t.Fatal("bulk skip lost original or repeat slots")
	}
}

func TestConcurrentDifferentChoicesKeepOneLogicalKey(t *testing.T) {
	c := defaults()
	c.total = 1
	c.rate = 1000
	c.workers = 2
	c.queue = 4
	c.repeatEvery = 1
	c.different = true
	c.maxLag = time.Second
	m, err := newObservations(c.total)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan int, 2)
	release := make(chan struct{})
	done := make(chan workloadReport, 1)
	var mu sync.Mutex
	winner := 0
	fake := func(ctx context.Context, _ string, _ string, choices []int) (poll.Receipt, error) {
		entered <- choices[0]
		select {
		case <-release:
		case <-ctx.Done():
			return poll.Receipt{}, ctx.Err()
		}
		mu.Lock()
		defer mu.Unlock()
		status := "conflict"
		if winner == 0 {
			winner = choices[0]
			status = "accepted"
		}
		at := time.Now()
		return poll.Receipt{Status: status, Choices: []int{winner}, AcceptedAt: &at}, nil
	}
	go func() { done <- runWorkload(context.Background(), c, "unused", m, fake) }()
	var seen uint8
	for range 2 {
		select {
		case choice := <-entered:
			seen |= 1 << uint(choice-1)
		case <-time.After(2 * time.Second):
			t.Fatal("repeat did not execute concurrently")
		}
	}
	close(release)
	r := <-done
	if seen != 3 || r.LogicalSent != 1 || r.LogicalConfirmed != 1 || r.AttemptSent != 2 || r.Outcomes["accepted"] != 1 || r.Outcomes["conflict"] != 1 || r.InvalidReceipts != 0 {
		t.Fatal("concurrent logical accounting is incorrect")
	}
	if r.AttemptSent+r.AttemptSkipped+r.AttemptNotScheduled != r.AttemptPlanned {
		t.Fatal("attempt accounting does not balance")
	}
	if r.StartedAt.IsZero() || r.FinishedAt.Before(r.StartedAt) || r.StartedAt.Location() != time.UTC || r.FinishedAt.Location() != time.UTC {
		t.Fatal("workload timestamps are not UTC observation boundaries")
	}
}

func TestExactLatencyQuantiles(t *testing.T) {
	values := make([]time.Duration, 100)
	for i := range values {
		values[i] = time.Duration(100-i) * time.Millisecond
	}
	r := latency(values)
	if r.MeanMS != 50.5 || r.P50MS != 50 || r.P95MS != 95 || r.P99MS != 99 || values[0] != 100*time.Millisecond {
		t.Fatal("quantile computation is incorrect or mutates observations")
	}
}

func TestPrivateLedgerRoundTripAndNoOverwrite(t *testing.T) {
	l := keyLedger{Version: 1, Schema: "gqbench_0011223344556677", PollID: "00112233-4455-6677-8899-aabbccddeeff", Mode: "vote", Keys: []keyState{{Token: "00112233445566778899aabbccddeeff", SentChoices: 1, ConfirmedChoices: 1, ReceiptChoices: 1, Accepted: 1}}}
	path := filepath.Join(t.TempDir(), "ledger.json")
	if _, err := saveLedger(path, l); err != nil {
		t.Fatal("ledger save failed")
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("ledger is not private")
	}
	got, err := readLedger(path)
	if err != nil || len(got.Keys) != 1 || got.Keys[0].ConfirmedChoices != 1 {
		t.Fatal("ledger round trip failed")
	}
	if _, err := saveLedger(path, l); err == nil {
		t.Fatal("existing ledger was overwritten")
	}
}

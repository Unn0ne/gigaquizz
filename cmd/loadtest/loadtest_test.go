package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func testConfig() config {
	return config{target: "http://127.0.0.1:8080", poll: "00000000-0000-4000-8000-000000000001", rate: 10, duration: time.Second,
		workers: 1, queue: 2, choice: 1, timeout: time.Second, drainTimeout: time.Second, maxLag: time.Second}
}

func TestLoadGuard(t *testing.T) {
	for _, tt := range []struct {
		name      string
		change    func(*config)
		wantError bool
	}{
		{"local defaults", func(c *config) {}, false},
		{"remote", func(c *config) { c.target = "https://example.com" }, true},
		{"DNS name needs explicit opt in", func(c *config) { c.target = "http://localhost:8080" }, true},
		{"remote explicit", func(c *config) { c.target = "https://example.com"; c.allowHighLoad = true }, false},
		{"high rate", func(c *config) { c.rate = 1001 }, true},
		{"retries counted", func(c *config) { c.rate = 600; c.duplicateEvery = 1 }, true},
		{"long low rate", func(c *config) { c.duration = 24 * time.Hour }, true},
		{"credentials", func(c *config) { c.target = "http://admin:secret@127.0.0.1" }, true},
		{"invalid UUID", func(c *config) { c.poll = "invalid" }, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := testConfig()
			tt.change(&c)
			_, err := c.validate()
			if (err != nil) != tt.wantError {
				t.Fatalf("validate error=%v, want error=%v", err, tt.wantError)
			}
		})
	}
}

func TestSchedulerReportsFullQueueWithoutWaitingForConsumer(t *testing.T) {
	c := testConfig()
	c.rate = 100
	c.duration = 100 * time.Millisecond
	m := newMetrics(10)
	jobs := make(chan job, 2)
	schedule(context.Background(), time.Now().Add(-200*time.Millisecond), c, jobs, m)
	if m.scheduled != 10 || m.enqueued != 2 || m.skipped["queue_full"] != 8 || len(jobs) != 2 {
		t.Fatalf("scheduled=%d enqueued=%d skipped=%v queued=%d", m.scheduled, m.enqueued, m.skipped, len(jobs))
	}
	first, second := <-jobs, <-jobs
	if second.scheduled.Sub(first.scheduled) != 10*time.Millisecond {
		t.Fatal("schedule was not preserved")
	}
}

func TestSchedulerDiscardsStaleSlotsAndHonorsCancellation(t *testing.T) {
	c := testConfig()
	c.rate = 1_000_000
	c.maxLag = time.Millisecond
	m := newMetrics(c.rate)
	jobs := make(chan job, 2)
	schedule(context.Background(), time.Now().Add(-2*time.Second), c, jobs, m)
	if m.scheduled != c.rate || m.skipped["scheduler_lag"] != c.rate || len(jobs) != 0 {
		t.Fatalf("stale slots not accounted for: %+v", m)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m = newMetrics(c.rate)
	schedule(ctx, time.Now(), c, jobs, m)
	if m.scheduled != 0 {
		t.Fatal("cancelled scheduler offered slots")
	}
}

func TestScheduleArithmeticAvoidsOverflow(t *testing.T) {
	const rate = 10_000_000
	if got := slotsBefore(24*time.Hour, rate); got != 864_000_000_000 {
		t.Fatalf("slots=%d", got)
	}
	if got := slotOffset(864_000_000_000, rate); got != 24*time.Hour {
		t.Fatalf("offset=%v", got)
	}
	if got := slotsBefore(time.Nanosecond, rate); got != 1 {
		t.Fatalf("fractional window slots=%d", got)
	}
}

func TestHistogramBounds(t *testing.T) {
	var h histogram
	for i := 1; i <= 1000; i++ {
		h.add(time.Duration(i) * time.Millisecond)
	}
	r := h.report()
	if r.Count != 1000 || r.MeanMS != 500.5 || r.MaxMS != 1000 {
		t.Fatalf("unexpected summary: %+v", r)
	}
	for _, p := range []struct{ got, want float64 }{{r.P50MS, 500}, {r.P95MS, 950}, {r.P99MS, 990}} {
		if p.got < p.want || p.got > p.want*1.101 {
			t.Fatalf("quantile bound=%f for exact=%f", p.got, p.want)
		}
	}
	if empty := (histogram{}).report(); empty.Count != 0 || empty.P99MS != 0 {
		t.Fatal("empty histogram is not zero")
	}
}

func TestRetryResolvesUnknownWithoutCounting201AsOnlyAcceptedVote(t *testing.T) {
	var previous string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/polls/00000000-0000-4000-8000-000000000001/votes" {
			t.Error("unexpected request")
		}
		var body struct {
			Token   string `json:"token"`
			Choices []int  `json:"choices"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		raw, err := hex.DecodeString(body.Token)
		if err != nil || len(raw) != 16 || hex.EncodeToString(raw) != body.Token || len(body.Choices) != 1 || body.Choices[0] != 1 {
			t.Error("invalid vote body")
		}
		if previous == "" {
			previous = body.Token // Simulates durable acceptance with an uncertain HTTP result.
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if previous != body.Token {
			t.Error("retry changed token")
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	c := testConfig()
	c.target = server.URL
	c.duration = time.Millisecond
	c.duplicateEvery = 1
	endpoint, err := c.validate()
	if err != nil {
		t.Fatal(err)
	}
	r := run(context.Background(), c, endpoint)
	if r.HTTPAttempts != 2 || r.Accepted201 != 0 || r.Duplicate200 != 1 || r.Unknown503 != 1 || r.LogicalConfirmed != 1 || r.LogicalUnknown != 0 {
		t.Fatalf("ambiguous result was not reconciled: %+v", r)
	}
	if r.LogicalScheduled != r.LogicalDispatched+r.LogicalSkipped || r.LogicalDispatched != r.LogicalConfirmed+r.LogicalUnknown+r.LogicalRejected {
		t.Fatal("logical accounting does not balance")
	}
	if r.LogicalEndToEnd.Count != 1 || r.AttemptEndToEnd.Count != 2 {
		t.Fatal("latency populations are incorrect")
	}
}

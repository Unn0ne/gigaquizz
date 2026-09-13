package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestResponseClassificationNeverFabricatesACK(t *testing.T) {
	m, _, _, _ := fixture(t)
	c := m.Config
	ack := func(choice int, at time.Time) []byte {
		b, _ := json.Marshal(map[string]any{"status": "recorded", "choices": []int{choice}, "accepted_at": at})
		return b
	}
	tests := []struct {
		name    string
		status  int
		body    []byte
		state   byte
		invalid bool
	}{
		{"valid", 202, ack(1, c.Start.Add(time.Nanosecond)), stateACK, false},
		{"admitted_59_9", 202, ack(1, c.Start.Add(59900*time.Millisecond)), stateACK, false},
		{"at_close", 202, ack(1, c.Start.Add(time.Minute)), stateUnknown, true},
		{"wrong_choice", 202, ack(2, c.Start.Add(time.Second)), stateUnknown, true},
		{"large_choice_cannot_wrap", 202, []byte(`{"status":"recorded","choices":[4294967297],"accepted_at":"2026-09-12T12:00:01Z"}`), stateUnknown, true},
		{"negative_choice_cannot_wrap", 202, []byte(`{"status":"recorded","choices":[-4294967295],"accepted_at":"2026-09-12T12:00:01Z"}`), stateUnknown, true},
		{"missing_time", 202, []byte(`{"status":"recorded","choices":[1]}`), stateUnknown, true},
		{"legacy_status_is_not_recorded", 201, ack(1, c.Start.Add(time.Second)), stateUnknown, true},
		{"malformed_positive", 202, []byte(`{"status":`), stateUnknown, true},
		{"closed", 410, []byte(`{"status":"closed"}`), stateClosed, false},
		{"not_open", 425, []byte(`{"status":"not_open"}`), stateNotOpen, false},
		{"busy", 503, []byte(`{"outcome":"not_admitted"}`), stateBusy, false},
		{"unknown", 503, []byte(`{"outcome":"unknown"}`), stateUnknown, false},
		{"bad_error_body", 503, []byte(`truncated`), stateUnknown, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := attempt{Choice: 1}
			classifyResponse(c, &a, tt.status, tt.body)
			if a.State != tt.state || (a.Flags&flagInvalidPositive != 0) != tt.invalid {
				t.Fatalf("got %+v", a)
			}
		})
	}
}

func TestCancelledHTTPRunKeepsEveryAttemptAndExactACK(t *testing.T) {
	m, _, _, _ := fixture(t)
	c := m.Config
	c.Rate = 20
	c.RepeatEvery = 0
	c.Start = time.Now().Add(1500 * time.Millisecond)
	c.Directory = filepath.Join(t.TempDir(), "private-run")
	c.Workers = 2
	c.Queue = 2
	var mu sync.Mutex
	var votes []journalVote
	first := make(chan struct{})
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Token   string `json:"token"`
			Choices []int  `json:"choices"`
		}
		if r.Method != "POST" || json.NewDecoder(r.Body).Decode(&in) != nil || len(in.Token) != 32 || len(in.Choices) != 1 {
			t.Error("invalid request")
			w.WriteHeader(422)
			return
		}
		at := time.Now().UTC()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(202)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "recorded", "choices": in.Choices, "accepted_at": at})
		mu.Lock()
		votes = append(votes, journalVote{AdmittedAt: at})
		mu.Unlock()
		once.Do(func() { close(first) })
	}))
	defer srv.Close()
	c.URL = srv.URL
	m.Poll.StartsAt = c.Start
	m.Poll.EndsAt = c.Start.Add(time.Minute)
	var err error
	m, err = createManifest(c, m)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-first:
			time.Sleep(20 * time.Millisecond)
			cancel()
		case <-ctx.Done():
		}
	}()
	client := newClient(c.Workers, c.Timeout)
	defer client.CloseIdleConnections()
	r, err := executeWorkload(ctx, m, client)
	if err == nil || !r.Complete || !r.Cancelled || r.ACK == 0 || r.Skipped == 0 || r.Attempts != c.attempts() || r.ACK+r.Unknown+r.Skipped != r.Attempts {
		t.Fatalf("run %+v, %v", r, err)
	}
	_, s, err := loadLedger(filepath.Join(c.Directory, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var count uint64
	for i, state := range s.Status {
		if state == stateACK {
			count++
			if s.Admitted[i] < c.Start.UnixNano() {
				t.Fatal("wrong admission")
			}
		}
	}
	if count != r.ACK {
		t.Fatalf("ledger ACK count %d differs from report %d", count, r.ACK)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(votes) == 0 {
		t.Fatal("test never sent real HTTP")
	}
}

func TestNoRedirectRetryOrMalformedSuccessACK(t *testing.T) {
	m, _, _, _ := fixture(t)
	c := m.Config
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Location", "/followed")
		w.WriteHeader(307)
	}))
	defer srv.Close()
	c.URL = srv.URL
	client := newClient(1, time.Second)
	defer client.CloseIdleConnections()
	a := attempt{Choice: 1}
	var s workerStats
	sendAttempt(context.Background(), c, client, &a, &s, time.Now())
	if calls != 1 || a.State != stateUnknown || s.Sent != 1 {
		t.Fatalf("calls %d attempt %+v", calls, a)
	}
}

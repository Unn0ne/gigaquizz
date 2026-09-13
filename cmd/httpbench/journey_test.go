package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func activeJourneyFixture(t *testing.T) (privateManifest, []attempt) {
	t.Helper()
	m, a, _, _ := fixture(t)
	m.Config.Journey = true
	m.Config.MaxLag = time.Second
	m.Config.Start = time.Now().Add(-time.Second).UTC()
	m.Poll.StartsAt = m.Config.Start
	m.Poll.EndsAt = m.Config.Start.Add(time.Minute)
	return m, a
}

func TestJourneyFetchesFiveResourcesThenOriginalAndRepeatPosts(t *testing.T) {
	m, records := activeJourneyFixture(t)
	question, _ := json.Marshal(m.Poll)
	paths := []string{"/p/" + m.Poll.ID, "/static/app.css", "/static/common.js", "/static/poll.js", "/api/polls/" + m.Poll.ID}
	types := []string{"text/html; charset=utf-8", "text/css", "text/javascript", "application/javascript", "application/json"}
	bodies := []string{"<html>synthetic fixture</html>", "body { color: black; }", "/* common */", "/* poll */", string(question)}
	var mu sync.Mutex
	var requests []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.Method+" "+r.URL.Path)
		mu.Unlock()
		if r.Method == http.MethodPost {
			var in struct {
				Choices []int `json:"choices"`
			}
			if json.NewDecoder(r.Body).Decode(&in) != nil || len(in.Choices) != 1 {
				w.WriteHeader(422)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(202)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "recorded", "choices": in.Choices, "accepted_at": time.Now().UTC()})
			return
		}
		for i, p := range paths {
			if r.URL.Path == p {
				w.Header().Set("Content-Type", types[i])
				_, _ = w.Write([]byte(bodies[i]))
				return
			}
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	m.Config.URL = srv.URL
	client := newClient(1, time.Second)
	defer client.CloseIdleConnections()
	var stats workerStats
	a := attempt{Token: records[1].Token, Key: 1, Choice: 1}
	dispatchAttempt(context.Background(), m.Config, client, &a, &stats, time.Now())
	if a.State != stateACK {
		t.Fatalf("original state=%d", a.State)
	}
	stats.Counts[a.State]++
	repeat := attempt{Token: records[1].Token, Key: 1, Choice: 2}
	dispatchAttempt(context.Background(), m.Config, client, &repeat, &stats, time.Now())
	if repeat.State != stateACK {
		t.Fatalf("repeat state=%d", repeat.State)
	}
	stats.Counts[repeat.State]++
	r := summarize(m.Config, []workerStats{stats}, time.Second, 2*ledgerBytes)
	var wantBytes uint64
	for _, b := range bodies {
		wantBytes += uint64(len(b))
	}
	if r.Mode != "http-journey" || r.GETSent != 5 || r.GETFailures != 0 || r.GETBytes != wantBytes || r.Sent != 2 || r.ACK != 2 || r.JourneyStarted != 1 || r.JourneyCompleted != 1 || r.JourneyFailed != 0 || len(r.GETStages) != 5 {
		t.Fatalf("journey stats=%+v", r)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 7 {
		t.Fatalf("request count %d", len(requests))
	}
	for i, p := range paths {
		if requests[i] != "GET "+p {
			t.Fatalf("step %d: %q", i, requests[i])
		}
	}
	for _, got := range requests[5:] {
		if got != "POST /api/polls/"+m.Poll.ID+"/votes" {
			t.Fatalf("unexpected POST step %q", got)
		}
	}
}

func TestJourneyFailureStopsBeforePOSTAndPreservesOutcome(t *testing.T) {
	for _, kind := range []string{"read_gate_busy", "redirect", "asset_body_bound", "wrong_content_type", "wrong_question", "short_body", "timeout"} {
		t.Run(kind, func(t *testing.T) {
			m, records := activeJourneyFixture(t)
			question, _ := json.Marshal(m.Poll)
			var mu sync.Mutex
			var methods []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				methods = append(methods, r.Method)
				mu.Unlock()
				if r.Method == http.MethodPost {
					w.WriteHeader(500)
					return
				}
				switch {
				case kind == "redirect":
					w.Header().Set("Location", "/must-not-follow")
					w.WriteHeader(307)
					return
				case kind == "asset_body_bound":
					w.Header().Set("Content-Type", "text/html")
					_, _ = w.Write([]byte(strings.Repeat("x", maxAssetBody+100)))
					return
				case kind == "wrong_content_type":
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{}`))
					return
				case kind == "short_body":
					w.Header().Set("Content-Type", "text/html")
					w.Header().Set("Content-Length", "100")
					_, _ = w.Write([]byte("short"))
					return
				case kind == "timeout":
					select {
					case <-r.Context().Done():
					case <-time.After(time.Second):
					}
					return
				}
				switch r.URL.Path {
				case "/p/" + m.Poll.ID:
					w.Header().Set("Content-Type", "text/html")
					_, _ = w.Write([]byte("<html>fixture</html>"))
				case "/static/app.css":
					w.Header().Set("Content-Type", "text/css")
					_, _ = w.Write([]byte("body {}"))
				case "/static/common.js", "/static/poll.js":
					w.Header().Set("Content-Type", "text/javascript")
					_, _ = w.Write([]byte("/* fixture */"))
				case "/api/polls/" + m.Poll.ID:
					w.Header().Set("Content-Type", "application/json")
					if kind == "read_gate_busy" {
						w.WriteHeader(503)
						_, _ = w.Write([]byte(`{"error":"synthetic busy"}`))
					} else if kind == "wrong_question" {
						wrong := m.Poll
						wrong.EndsAt = wrong.EndsAt.Add(time.Nanosecond)
						_ = json.NewEncoder(w).Encode(wrong)
					} else {
						_, _ = w.Write(question)
					}
				default:
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()
			m.Config.URL = srv.URL
			if kind == "timeout" {
				m.Config.MaxLag = 25 * time.Millisecond
			}
			client := newClient(1, time.Second)
			defer client.CloseIdleConnections()
			var stats workerStats
			a := attempt{Token: records[0].Token, Choice: 1}
			dispatchAttempt(context.Background(), m.Config, client, &a, &stats, time.Now())
			stats.Counts[a.State]++
			r := summarize(m.Config, []workerStats{stats}, time.Second, ledgerBytes)
			if a.State != stateJourneyFailed || a.HTTP != 0 || a.AdmittedNS != 0 || a.Flags != 0 || r.JourneyFailed != 1 || r.Sent != 0 || r.Unknown != 0 || r.Skipped != 0 || r.GETFailures != 1 || r.JourneyStarted != 1 || r.JourneyCompleted != 0 {
				t.Fatalf("failure outcome=%+v stats=%+v", a, r)
			}
			var classified uint64
			for _, stage := range r.GETStages {
				classified += stage.Timeouts + stage.TransportFailures + stage.StatusFailures + stage.ValidationFailures
			}
			if classified != r.GETFailures {
				t.Fatalf("failure categories overlap or miss: %+v", r)
			}
			if kind == "timeout" && r.GETStages[0].Timeouts != 1 {
				t.Fatalf("timeout missing: %+v", r.GETStages)
			}
			if kind == "read_gate_busy" && (r.GETSent != 5 || r.GETStages[4].Failures != 1) {
				t.Fatalf("read-gate failure not separate: %+v", r.GETStages)
			}
			if kind == "asset_body_bound" && r.GETBytes != maxAssetBody+1 {
				t.Fatalf("body bound violated: %d", r.GETBytes)
			}
			mu.Lock()
			defer mu.Unlock()
			for _, method := range methods {
				if method != http.MethodGet {
					t.Fatalf("POST after failed journey: %v", methods)
				}
			}
			if kind == "redirect" && len(methods) != 1 {
				t.Fatalf("redirect followed: %v", methods)
			}
		})
	}
}

func TestJourneyFailureLedgerAndAuditRemainExact(t *testing.T) {
	m, a, v, res := fixture(t)
	m.Config.Journey = true
	// This original made GETs but never POST. It remains one of the planned
	// attempts, distinct from an ordinary scheduler skip and an uncertain POST.
	a[2].State = stateJourneyFailed
	path := writeFixture(t, m, a)
	loaded, s, err := loadLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	r, err := reconcile(context.Background(), loaded, s, res, replayFixture(v, nil))
	if err != nil || !r.Correct || r.JourneyFailed != 1 || r.Planned != m.Config.attempts() {
		t.Fatalf("audit=%+v err=%v", r, err)
	}
	_, s, err = loadLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	v = append(v, journalVote{Token: a[2].Token, Choice: 1, AdmittedAt: m.Poll.StartsAt.Add(time.Second)})
	r, err = reconcile(context.Background(), loaded, s, res, replayFixture(v, nil))
	if err == nil || r.Correct || r.Failure != "rejected_or_skipped_attempt_committed" {
		t.Fatalf("journey failure explained committed data: %+v %v", r, err)
	}
}

func TestJourneyStateRequiresJourneyOriginalWithoutPOST(t *testing.T) {
	for _, kind := range []string{"baseline_mode", "repeat", "post_response"} {
		t.Run(kind, func(t *testing.T) {
			m, a, _, _ := fixture(t)
			m.Config.Journey = true
			i := uint64(2)
			if kind == "baseline_mode" {
				m.Config.Journey = false
			}
			if kind == "repeat" {
				i, _ = sequence(m.Config, 3, 2)
			}
			a[i].State = stateJourneyFailed
			if kind == "post_response" {
				a[i].HTTP = 503
			}
			path := writeFixture(t, m, a)
			if _, _, err := loadLedger(path); err == nil {
				t.Fatalf("accepted invalid journey state: %s", kind)
			}
		})
	}
}

func TestCompressedDefinitionJourneyAndByteAccounting(t *testing.T) {
	m, _ := activeJourneyFixture(t)
	m.Config.Definition = true
	question, _ := json.Marshal(m.Poll.Definition())
	paths := []string{"/p/" + m.Poll.ID, "/static/app.css", "/static/common.js", "/static/poll.js", "/api/polls/" + m.Poll.ID + "/definition"}
	types := []string{"text/html", "text/css", "text/javascript", "text/javascript", "application/json"}
	bodies := [][]byte{[]byte(strings.Repeat("<html>fixture</html>", 100)), []byte(strings.Repeat("body {}", 100)), []byte(strings.Repeat("/* fixture */", 100)), []byte(strings.Repeat("/* fixture */", 100)), question}
	var compressed [journeyStages][]byte
	var wantEncoded, wantDecoded uint64
	for i, b := range bodies {
		var out bytes.Buffer
		z := gzip.NewWriter(&out)
		_, _ = z.Write(b)
		_ = z.Close()
		compressed[i] = out.Bytes()
		wantEncoded += uint64(out.Len())
		wantDecoded += uint64(len(b))
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for i, p := range paths {
			if r.URL.Path == p && r.Header.Get("Accept-Encoding") == "gzip" {
				w.Header().Set("Content-Type", types[i])
				w.Header().Set("Content-Encoding", "gzip")
				_, _ = w.Write(compressed[i])
				return
			}
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	m.Config.URL = srv.URL
	client := newClient(1, time.Second)
	defer client.CloseIdleConnections()
	var s workerStats
	if !runJourney(context.Background(), m.Config, client, &s, time.Now().Add(time.Second)) {
		t.Fatal("compressed journey failed")
	}
	r := summarize(m.Config, []workerStats{s}, time.Second, 0)
	if r.Mode != "http-journey-definition-gzip" || r.GETFailures != 0 || r.GETBytes != wantEncoded || r.GETDecodedBytes != wantDecoded || wantEncoded >= wantDecoded {
		t.Fatalf("accounting: %+v", r)
	}
}

func TestCompressedResourceBoundsAndCorruption(t *testing.T) {
	for _, kind := range []string{"truncated", "expansion", "unsupported"} {
		t.Run(kind, func(t *testing.T) {
			var out bytes.Buffer
			z := gzip.NewWriter(&out)
			body := "<html>fixture</html>"
			if kind == "expansion" {
				body = strings.Repeat("x", maxAssetBody+100)
			}
			_, _ = z.Write([]byte(body))
			_ = z.Close()
			compressed := out.Bytes()
			if kind == "truncated" {
				compressed = compressed[:len(compressed)-4]
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/html")
				w.Header().Set("Content-Encoding", "gzip")
				_, _ = w.Write(compressed)
			}))
			defer srv.Close()
			m, _ := activeJourneyFixture(t)
			m.Config.URL = srv.URL
			m.Config.Definition = kind != "unsupported"
			client := newClient(1, time.Second)
			defer client.CloseIdleConnections()
			var s getStats
			if getResource(context.Background(), m.Config, client, "/", 0, &s, &journeyDecoder{}) || s.Failures != 1 || s.ValidationFailures != 1 || s.DecodedBytes > maxAssetBody+1 || s.Bytes > maxAssetBody+1 {
				t.Fatalf("accepted invalid encoding/bound: %+v", s)
			}
		})
	}
}

func TestWorkerDecoderCanRecoverAfterCorruptResponse(t *testing.T) {
	var out bytes.Buffer
	z := gzip.NewWriter(&out)
	_, _ = z.Write([]byte("<html>fixture</html>"))
	_ = z.Close()
	valid := out.Bytes()
	var decoder journeyDecoder
	for _, body := range [][]byte{valid[:len(valid)-4], valid, []byte("bad header"), valid} {
		reader, err := decoder.open(bytes.NewReader(body))
		if err == nil {
			_, err = io.Copy(io.Discard, reader)
			decoder.release()
		}
		if bytes.Equal(body, valid) && err != nil {
			t.Fatal("decoder failed after earlier malformed response", err)
		}
		if !bytes.Equal(body, valid) && err == nil {
			t.Fatal("corrupt gzip accepted")
		}
	}
}

type journeyRoundTrip func(*http.Request) (*http.Response, error)

func (f journeyRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type journeyFailingBody struct {
	io.Reader
	err error
}

func (b journeyFailingBody) Close() error { return b.err }

type journeyReadError struct{ err error }

func (r journeyReadError) Read([]byte) (int, error) { return 0, r.err }

func TestGzipHeaderAndCloseFailuresRetainTransportClassification(t *testing.T) {
	var compressed bytes.Buffer
	z := gzip.NewWriter(&compressed)
	_, _ = z.Write([]byte("<html>fixture</html>"))
	_ = z.Close()
	valid := compressed.Bytes()
	for _, test := range []struct {
		name       string
		reader     io.Reader
		closeErr   error
		timeouts   uint64
		transports uint64
	}{
		{"header_deadline", io.MultiReader(bytes.NewReader(valid[:2]), journeyReadError{context.DeadlineExceeded}), nil, 1, 0},
		{"header_cancel", io.MultiReader(bytes.NewReader(valid[:2]), journeyReadError{context.Canceled}), nil, 0, 1},
		{"header_corrupt", strings.NewReader("bad gzip header"), nil, 0, 0},
		{"close_deadline", bytes.NewReader(valid), context.DeadlineExceeded, 1, 0},
		{"close_transport", bytes.NewReader(valid), errors.New("connection close failed"), 0, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			m, _ := activeJourneyFixture(t)
			m.Config.Definition = true
			client := &http.Client{Transport: journeyRoundTrip(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/html"}, "Content-Encoding": {"gzip"}}, Body: journeyFailingBody{test.reader, test.closeErr}, Request: r}, nil
			})}
			var stats getStats
			var decoder journeyDecoder
			if getResource(context.Background(), m.Config, client, "/", 0, &stats, &decoder) || stats.Failures != 1 || stats.Timeouts != test.timeouts || stats.TransportFailures != test.transports || stats.ValidationFailures != 1-test.timeouts-test.transports {
				t.Fatalf("wrong compressed response failure classification: %+v", stats)
			}
			reader, err := decoder.open(bytes.NewReader(valid))
			if err != nil {
				t.Fatal("decoder initialization failed after prior response", err)
			}
			_, err = io.Copy(io.Discard, reader)
			decoder.release()
			if err != nil {
				t.Fatal("decoder retained prior response failure", err)
			}
		})
	}
}

func BenchmarkWorkerGzipDecode(b *testing.B) {
	var out bytes.Buffer
	z := gzip.NewWriter(&out)
	_, _ = z.Write([]byte(strings.Repeat("synthetic public asset;", 800)))
	_ = z.Close()
	body := out.Bytes()
	b.Run("fresh", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			r, err := gzip.NewReader(bytes.NewReader(body))
			if err != nil {
				b.Fatal(err)
			}
			_, err = io.Copy(io.Discard, r)
			_ = r.Close()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("worker_reuse", func(b *testing.B) {
		b.ReportAllocs()
		var d journeyDecoder
		for i := 0; i < b.N; i++ {
			r, err := d.open(bytes.NewReader(body))
			if err != nil {
				b.Fatal(err)
			}
			_, err = io.Copy(io.Discard, r)
			d.release()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

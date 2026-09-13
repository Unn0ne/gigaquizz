package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func writePlanForTest(t *testing.T, p distributedPlan) string {
	t.Helper()
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "plan.json")
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestClockIntervalsCoverAsymmetricRTTAndRejectSkewAndSteps(t *testing.T) {
	sent := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	s, step, err := evaluateClockSample(sent, sent.Add(10*time.Millisecond), sent.Add(7*time.Millisecond), 10*time.Millisecond)
	if err != nil || step || s.lower != -3*time.Millisecond || s.upper != 7*time.Millisecond || s.rtt != 10*time.Millisecond {
		t.Fatalf("asymmetric RTT interval: %+v step=%v err=%v", s, step, err)
	}
	c := config{MaxClockError: 10 * time.Millisecond}
	n := 0
	r, err := checkClockSamples(c, func() (clockSample, bool, error) {
		n++
		if n == 2 {
			return s, false, nil
		}
		return clockSample{rtt: 100 * time.Millisecond, lower: -50 * time.Millisecond, upper: 50 * time.Millisecond}, false, nil
	})
	if err != nil || !r.Verified || r.Samples != 3 || r.ValidSamples != 3 || r.RTTMS != 10 || r.OffsetLowerMS != -3 || r.OffsetUpperMS != 7 {
		t.Fatalf("best conservative sample: %+v %v", r, err)
	}
	for _, offset := range []time.Duration{-200 * time.Millisecond, 200 * time.Millisecond} {
		r, err := checkClockSamples(c, func() (clockSample, bool, error) {
			return clockSample{rtt: 2 * time.Millisecond, lower: offset - time.Millisecond, upper: offset + time.Millisecond}, false, nil
		})
		if err == nil || r.Verified || r.ClockStep {
			t.Fatalf("clock skew accepted: %+v %v", r, err)
		}
	}
	for _, wall := range []time.Duration{-time.Second, time.Second} {
		_, step, err := evaluateClockSample(sent, sent.Add(wall), sent, 10*time.Millisecond)
		if err == nil || !step {
			t.Fatal("wall-clock step was not detected")
		}
	}
	n = 0
	r, err = checkClockSamples(c, func() (clockSample, bool, error) {
		n++
		if n == 1 {
			return s, false, nil
		}
		return clockSample{rtt: time.Millisecond, lower: time.Second, upper: time.Second + time.Millisecond}, false, nil
	})
	if err == nil || r.Verified || !r.ClockStep {
		t.Fatalf("inconsistent time samples accepted: %+v %v", r, err)
	}
	for _, invalid := range []time.Time{{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)} {
		if _, _, err := evaluateClockSample(sent, sent.Add(time.Millisecond), invalid, time.Millisecond); err == nil {
			t.Fatal("invalid clock timestamp accepted")
		}
	}
}

func TestStartLeadIsBoundedAndNeverAdjusted(t *testing.T) {
	start := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	c := config{Start: start, PlanFile: "private-plan", MaxClockError: 10 * time.Millisecond}
	for _, lead := range []time.Duration{1010 * time.Millisecond, 15 * time.Minute} {
		if err := checkStartLead(c, start.Add(-lead)); err != nil {
			t.Fatalf("valid lead %v: %v", lead, err)
		}
	}
	for _, lead := range []time.Duration{-time.Second, 0, time.Second, 15*time.Minute + time.Nanosecond} {
		if err := checkStartLead(c, start.Add(-lead)); err == nil {
			t.Fatalf("unsafe lead %v accepted", lead)
		}
	}
	if !c.Start.Equal(start) {
		t.Fatal("readiness shifted the original window")
	}
}

func TestPlanPreflightChecksResourcesPollAndClockWithoutCreatingLedgerOrPost(t *testing.T) {
	m, _, _, _ := fixture(t)
	c := m.Config
	c.Start = time.Now().UTC().Add(30 * time.Second)
	m.Poll.StartsAt, m.Poll.EndsAt = c.Start, c.Start.Add(time.Minute)
	var gets, clocks, posts atomic.Int64
	var skew atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			posts.Add(1)
			w.WriteHeader(405)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if r.URL.Path == "/api/time" {
			clocks.Add(1)
			if !strings.Contains(r.Header.Get("Cache-Control"), "no-cache") {
				t.Error("time sample can use a cache")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"server_time": time.Now().UTC().Add(time.Duration(skew.Load()))})
			return
		}
		gets.Add(1)
		_ = json.NewEncoder(w).Encode(m.Poll)
	}))
	defer srv.Close()
	c.URL = srv.URL
	p, err := makeDistributedPlan(c, m.Poll, 12, 2)
	if err != nil {
		t.Fatal(err)
	}
	planPath := writePlanForTest(t, p)
	c, err = configFromPlan(config{PlanFile: planPath, Generator: 1, Directory: filepath.Join(t.TempDir(), "run"), MaxClockError: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var resources int
	checkResources := func(got config) error {
		resources++
		if !filepath.IsAbs(got.Directory) || got.Directory != c.Directory {
			t.Fatal("resource check received wrong output directory")
		}
		return nil // Real FD/disk bounds have independent tests; no CI disk-size dependency.
	}
	r, err := runPreflightWith(context.Background(), c, checkResources)
	if err != nil || !r.Complete || r.PlanSHA256 != p.digest() || r.Generator != 1 || r.UniqueKeys != 6 || r.Attempts != c.attempts() || r.Clock == nil || !r.Clock.Verified || r.Clock.Samples != 3 || resources != 1 || gets.Load() != 1 || clocks.Load() != 3 || posts.Load() != 0 {
		t.Fatalf("preflight: %+v %v", r, err)
	}
	if _, err := os.Lstat(c.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("preflight created a ledger directory")
	}
	skew.Store(int64(2 * time.Second))
	r, err = runPreflightWith(context.Background(), c, checkResources)
	if err == nil || r.Complete || r.Failure != "clock_check_failed" || r.Clock == nil || r.Clock.Verified || posts.Load() != 0 {
		t.Fatalf("skew did not prevent readiness: %+v %v", r, err)
	}
	beforeGET := gets.Load() + clocks.Load()
	r, err = runPreflightWith(context.Background(), c, func(config) error { return errors.New("insufficient resources") })
	if err == nil || r.Failure != "ledger_preflight_failed" || gets.Load()+clocks.Load() != beforeGET {
		t.Fatal("resource failure made an HTTP request")
	}
	if _, err := os.Lstat(c.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed preflight created files")
	}
}

func TestSetupTooLateAndClockFailureNeverPostOrCertifyCompleteLedger(t *testing.T) {
	for _, mode := range []string{"late-standalone", "skewed-plan"} {
		t.Run(mode, func(t *testing.T) {
			m, _, _, _ := fixture(t)
			c := m.Config
			c.Start = time.Now().UTC().Add(30 * time.Second)
			if mode == "late-standalone" {
				c.Start = time.Now().UTC().Add(50 * time.Millisecond)
			}
			c.Directory = filepath.Join(t.TempDir(), "run")
			m.Poll.StartsAt, m.Poll.EndsAt = c.Start, c.Start.Add(time.Minute)
			var posts, clocks atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					posts.Add(1)
					w.WriteHeader(500)
					return
				}
				clocks.Add(1)
				_ = json.NewEncoder(w).Encode(map[string]any{"server_time": time.Now().UTC().Add(2 * time.Second)})
			}))
			defer srv.Close()
			c.URL = srv.URL
			if mode == "skewed-plan" {
				p, err := makeDistributedPlan(c, m.Poll, 12, 1)
				if err != nil {
					t.Fatal(err)
				}
				c, err = configFromPlan(config{PlanFile: writePlanForTest(t, p), Directory: c.Directory, MaxClockError: 10 * time.Millisecond})
				if err != nil {
					t.Fatal(err)
				}
			}
			var err error
			m, err = createManifest(c, m)
			if err != nil {
				t.Fatal(err)
			}
			client := newClient(c.Workers, c.Timeout)
			defer client.CloseIdleConnections()
			r, err := executeWorkload(context.Background(), m, client)
			if err == nil || r.Complete || r.Sent != 0 || r.ACK != 0 || posts.Load() != 0 {
				t.Fatalf("unready setup sent votes: %+v %v", r, err)
			}
			if mode == "late-standalone" && (r.Failure != "original_start_outside_ready_interval" || clocks.Load() != 0 || r.Clock != nil) {
				t.Fatalf("standalone clock compatibility/late guard: %+v", r)
			}
			if mode == "skewed-plan" && (r.Failure != "clock_check_failed" || clocks.Load() != 3 || r.Clock == nil || r.Clock.Verified) {
				t.Fatalf("plan clock guard skipped: %+v", r)
			}
			var stored privateManifest
			if err := readPrivateJSON(filepath.Join(c.Directory, "manifest.json"), &stored, 2<<20); err != nil || stored.Complete {
				t.Fatalf("failed setup published complete manifest: %+v %v", stored, err)
			}
			if _, _, err := loadLedger(filepath.Join(c.Directory, "manifest.json")); err == nil {
				t.Fatal("incomplete setup passed client ledger verification")
			}
		})
	}
}

func TestExplicitTransportIgnoresEnvironmentProxyAndUsesHTTP1(t *testing.T) {
	var proxies, direct atomic.Int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { proxies.Add(1); w.WriteHeader(502) }))
	defer proxy.Close()
	t.Setenv("HTTP_PROXY", proxy.URL)
	t.Setenv("HTTPS_PROXY", proxy.URL)
	t.Setenv("NO_PROXY", "")
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		direct.Add(1)
		if r.ProtoMajor != 1 {
			t.Errorf("unexpected protocol %s", r.Proto)
		}
		w.WriteHeader(204)
	}))
	defer origin.Close()
	target, _ := url.Parse(origin.URL)
	client := newClient(1, time.Second)
	defer client.CloseIdleConnections()
	tr := client.Transport.(*http.Transport)
	if tr.Proxy != nil || tr.Protocols == nil || !tr.Protocols.HTTP1() || tr.Protocols.HTTP2() || (tr.TLSClientConfig != nil && tr.TLSClientConfig.InsecureSkipVerify) {
		t.Fatal("transport policy is not direct verified HTTP/1.1")
	}
	tr.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != "synthetic.invalid:80" {
			t.Errorf("unexpected proxy dial %s", address)
		}
		return (&net.Dialer{}).DialContext(ctx, network, target.Host)
	}
	response, err := client.Get("http://synthetic.invalid/")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if proxies.Load() != 0 || direct.Load() != 1 || response.StatusCode != 204 {
		t.Fatal("request used environmental proxy")
	}
	// Default TLS verification must still reject an untrusted test certificate.
	tls := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("untrusted TLS reached handler") }))
	defer tls.Close()
	secureClient := newClient(1, time.Second)
	defer secureClient.CloseIdleConnections()
	if resp, err := secureClient.Get(tls.URL); err == nil {
		resp.Body.Close()
		t.Fatal("TLS verification bypassed")
	}
}

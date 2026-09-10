// Command loadtest performs a bounded, open-loop POST workload against one poll.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type config struct {
	target, poll                  string
	rate                          int64
	duration                      time.Duration
	workers, queue, choice        int
	duplicateEvery                int64
	timeout, drainTimeout, maxLag time.Duration
	allowHighLoad                 bool
}

func main() {
	c := config{}
	flag.StringVar(&c.target, "url", "http://127.0.0.1:8080", "server base URL")
	flag.StringVar(&c.poll, "poll", "", "required poll UUID")
	flag.Int64Var(&c.rate, "rate", 10, "scheduled logical votes per second")
	flag.DurationVar(&c.duration, "duration", 10*time.Second, "arrival window (maximum 24h)")
	flag.IntVar(&c.workers, "workers", 16, "maximum concurrent HTTP requests")
	flag.IntVar(&c.queue, "queue", 64, "maximum waiting logical votes")
	flag.IntVar(&c.choice, "choice", 1, "selected option ID")
	flag.Int64Var(&c.duplicateEvery, "duplicate-every", 0, "repeat every Nth dispatched sequence with its original token and choice; 0 disables")
	flag.DurationVar(&c.timeout, "timeout", 5*time.Second, "timeout of each HTTP attempt")
	flag.DurationVar(&c.drainTimeout, "drain-timeout", 10*time.Second, "maximum extra time after the arrival window")
	flag.DurationVar(&c.maxLag, "max-lag", 100*time.Millisecond, "skip initial dispatches this far behind schedule")
	flag.BoolVar(&c.allowHighLoad, "allow-high-load", false, "explicitly enable non-loopback targets, more than 1000 maximum average HTTP attempts/s, or more than 100000 planned HTTP attempts")
	flag.Parse()
	if flag.NArg() != 0 {
		fatal("unexpected positional arguments")
	}
	endpoint, err := c.validate()
	if err != nil {
		fatal(err.Error())
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	report := run(ctx, c, endpoint)
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		fatal("cannot write JSON report")
	}
}

func fatal(message string) {
	fmt.Fprintln(os.Stderr, "loadtest:", message)
	os.Exit(2)
}

func (c config) validate() (string, error) {
	if c.rate < 1 || c.rate > 10_000_000 {
		return "", fmt.Errorf("rate must be between 1 and 10000000")
	}
	if c.duration <= 0 || c.duration > 24*time.Hour {
		return "", fmt.Errorf("duration must be positive and at most 24h")
	}
	if c.workers < 1 || c.workers > 10_000 || c.queue < 1 || c.queue > 100_000 {
		return "", fmt.Errorf("workers must be 1..10000 and queue must be 1..100000")
	}
	if c.choice < 1 || c.choice > 20 || c.duplicateEvery < 0 {
		return "", fmt.Errorf("choice must be 1..20 and duplicate-every must be nonnegative")
	}
	if c.timeout <= 0 || c.timeout > 5*time.Minute || c.drainTimeout <= 0 || c.drainTimeout > 5*time.Minute || c.maxLag <= 0 || c.maxLag > time.Minute {
		return "", fmt.Errorf("timeout and drain-timeout must be in (0,5m]; max-lag must be in (0,1m]")
	}
	if !validUUID(c.poll) {
		return "", fmt.Errorf("poll must be a UUID")
	}
	u, err := url.Parse(c.target)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return "", fmt.Errorf("url must be an HTTP(S) base URL without credentials, query, or fragment")
	}
	host := u.Hostname()
	ip := net.ParseIP(host)
	local := ip != nil && ip.IsLoopback()
	// Only loopback literals qualify: DNS names, including localhost, require
	// the explicit flag so name resolution cannot silently select a remote IP.
	planned := slotsBefore(c.duration, c.rate)
	maxAttempts := planned
	maxAverageRPS := c.rate
	if c.duplicateEvery > 0 {
		maxAttempts += planned / c.duplicateEvery
		maxAverageRPS += c.rate / c.duplicateEvery
		if c.rate%c.duplicateEvery != 0 {
			maxAverageRPS++
		}
	}
	if !c.allowHighLoad && (!local || maxAverageRPS > 1000 || maxAttempts > 100_000) {
		return "", fmt.Errorf("target or requested workload requires explicit -allow-high-load; defaults only allow loopback IPs, <=1000 maximum average HTTP attempts/s and <=100000 planned HTTP attempts")
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/api/polls/" + c.poll + "/votes"
	u.RawPath = ""
	return u.String(), nil
}

func validUUID(s string) bool {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	_, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	return err == nil
}

func run(parent context.Context, c config, endpoint string) report {
	start := time.Now()
	ctx, cancel := context.WithDeadline(parent, start.Add(c.duration+c.drainTimeout))
	defer cancel()
	transport := &http.Transport{
		// No environment proxy: a local default must remain local.
		DialContext:       (&net.Dialer{Timeout: c.timeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2: true,
		MaxIdleConns:      c.workers, MaxIdleConnsPerHost: c.workers,
		MaxConnsPerHost: c.workers, IdleConnTimeout: 30 * time.Second,
		TLSHandshakeTimeout: c.timeout, ResponseHeaderTimeout: c.timeout,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: c.timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	m := newMetrics(slotsBefore(c.duration, c.rate))
	jobs := make(chan job, c.queue)
	var workers sync.WaitGroup
	for range c.workers {
		workers.Go(func() {
			for j := range jobs {
				if ctx.Err() != nil {
					m.skip("cancelled", 1)
					continue
				}
				if time.Since(j.scheduled) > c.maxLag {
					m.skip("worker_lag", 1)
					continue
				}
				var token [16]byte
				if _, err := rand.Read(token[:]); err != nil {
					m.skip("token_generation", 1)
					continue
				}
				body, _ := json.Marshal(struct {
					Token   string `json:"token"`
					Choices []int  `json:"choices"`
				}{hex.EncodeToString(token[:]), []int{c.choice}})
				m.dispatch(time.Since(j.scheduled))
				status, unknown := attempt(ctx, client, endpoint, body, j, start, c.duration, m)
				confirmed := status == http.StatusCreated || status == http.StatusOK || status == http.StatusAccepted
				// Exactly one intentional retry, after the first attempt completes,
				// including its timeout. It never creates a new token.
				if c.duplicateEvery > 0 && (j.sequence+1)%c.duplicateEvery == 0 && ctx.Err() == nil {
					status2, unknown2 := attempt(ctx, client, endpoint, body, j, start, c.duration, m)
					confirmed = confirmed || status2 == http.StatusCreated || status2 == http.StatusOK || status2 == http.StatusAccepted
					unknown = unknown || unknown2
				}
				m.finishLogical(confirmed, unknown, time.Since(j.scheduled))
			}
		})
	}
	schedule(ctx, start, c, jobs, m)
	close(jobs)
	workers.Wait()
	return m.snapshot(c, time.Since(start), ctx.Err() != nil)
}

func attempt(ctx context.Context, client *http.Client, endpoint string, body []byte, j job, start time.Time, window time.Duration, m *metrics) (int, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		// Validation has already checked the endpoint. Keep this failure bounded
		// and avoid logging a request body or token even if it becomes reachable.
		m.localError()
		return 0, true
	}
	req.Header.Set("Content-Type", "application/json")
	// GetBody is unnecessary: redirect following and transparent retries of
	// this POST are disabled, keeping each counted attempt one explicit Do call.
	req.GetBody = nil
	sent := time.Now()
	m.beginAttempt(sent.Sub(start) < window)
	resp, err := client.Do(req)
	status := 0
	var bodyError bool
	if resp != nil {
		status = resp.StatusCode
		// Drain small responses for connection reuse; never allocate an
		// unbounded response body. Oversized responses close the connection.
		n, readErr := io.Copy(io.Discard, io.LimitReader(resp.Body, 64*1024+1))
		bodyError = readErr != nil || n > 64*1024
		resp.Body.Close()
	}
	done := time.Now()
	m.finishAttempt(status, err != nil, bodyError, done.Sub(sent), done.Sub(j.scheduled))
	// 201/200 confirm canonical acceptance; 202 confirms only a stored attempt.
	// Successful response headers acknowledge storage even if reading the body
	// fails. A transport error or 5xx leaves the durable outcome uncertain.
	return status, status == 0 || status >= 500
}

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"
)

const clockSamples = 3
const maxClockStep = 2 * time.Millisecond
const maxStartWait = 15 * time.Minute

type transportReport struct {
	Proxy           string `json:"proxy"`
	Protocol        string `json:"protocol"`
	TLSVerification bool   `json:"tls_verification"`
}

func transportPolicy() transportReport {
	return transportReport{Proxy: "disabled", Protocol: "http/1.1", TLSVerification: true}
}

type clockReport struct {
	BudgetMS      float64 `json:"max_clock_error_ms"`
	Samples       int     `json:"samples"`
	ValidSamples  int     `json:"valid_samples"`
	RTTMS         float64 `json:"rtt_ms"`
	OffsetLowerMS float64 `json:"offset_lower_ms"`
	OffsetUpperMS float64 `json:"offset_upper_ms"`
	ClockStep     bool    `json:"clock_step_detected"`
	Verified      bool    `json:"verified"`
}

type clockSample struct {
	rtt, lower, upper time.Duration
}

func clockBudget(c config) time.Duration {
	if c.MaxClockError == 0 {
		return 100 * time.Millisecond // Old private configs have no runtime policy.
	}
	return c.MaxClockError
}

// The timestamp is generated between send and complete body receipt. The
// interval covers arbitrary path asymmetry; its midpoint is never called exact.
// UTC removes Go's monotonic component only for the separate wall-step check.
func evaluateClockSample(sent, received, server time.Time, elapsed time.Duration) (clockSample, bool, error) {
	wallElapsed := received.UTC().Sub(sent.UTC())
	step := wallElapsed-elapsed > maxClockStep || elapsed-wallElapsed > maxClockStep
	if elapsed < 0 || step {
		return clockSample{}, step, errors.New("local clock changed during time sample")
	}
	if server.IsZero() || !time.Unix(0, server.UnixNano()).Equal(server) {
		return clockSample{}, false, errors.New("invalid server clock timestamp")
	}
	return clockSample{rtt: elapsed, lower: server.Sub(received.UTC()), upper: server.Sub(sent.UTC())}, false, nil
}

func sampleServerClock(ctx context.Context, c config, client *http.Client) (clockSample, bool, error) {
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.URL, "/")+"/api/time", nil)
	if err != nil {
		return clockSample{}, false, err
	}
	r.Header.Set("Cache-Control", "no-cache, no-store")
	r.Header.Set("Pragma", "no-cache")
	r.Header.Set("Accept", "application/json")
	sent := time.Now()
	resp, err := client.Do(r)
	if err != nil {
		return clockSample{}, false, err
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4097))
	closeErr := resp.Body.Close()
	received := time.Now()
	var payload struct {
		ServerTime time.Time `json:"server_time"`
	}
	if readErr != nil || closeErr != nil || len(body) > 4096 || resp.StatusCode != http.StatusOK || json.Unmarshal(body, &payload) != nil {
		return clockSample{}, false, errors.New("invalid fresh clock response")
	}
	// A conforming service sends no-store. An explicitly aged proxy response
	// cannot certify the current clock, even if its body parses correctly.
	if age := strings.TrimSpace(resp.Header.Get("Age")); age != "" && age != "0" {
		return clockSample{}, false, errors.New("aged clock response")
	}
	return evaluateClockSample(sent, received, payload.ServerTime, received.Sub(sent))
}

func checkServerClock(ctx context.Context, c config, client *http.Client) (clockReport, error) {
	started := time.Now()
	r, err := checkClockSamples(c, func() (clockSample, bool, error) { return sampleServerClock(ctx, c, client) })
	finished := time.Now()
	// Also cover a clock step between HTTP samples, not only during a read.
	_, step, stepErr := evaluateClockSample(started, finished, started.UTC(), finished.Sub(started))
	if step {
		r.ClockStep, r.Verified = true, false
		return r, stepErr
	}
	return r, err
}

func checkClockSamples(c config, sample func() (clockSample, bool, error)) (clockReport, error) {
	budget := clockBudget(c)
	r := clockReport{BudgetMS: float64(budget) / 1e6}
	if budget < time.Millisecond || budget > time.Second {
		return r, errors.New("invalid clock budget")
	}
	var best clockSample
	var overlapLower, overlapUpper time.Duration
	for i := 0; i < clockSamples; i++ {
		s, step, err := sample()
		r.Samples++
		r.ClockStep = r.ClockStep || step
		if err != nil {
			return r, err
		}
		r.ValidSamples++
		if i == 0 || s.rtt < best.rtt {
			best = s
		}
		if i == 0 {
			overlapLower, overlapUpper = s.lower, s.upper
		} else {
			overlapLower, overlapUpper = max(overlapLower, s.lower), min(overlapUpper, s.upper)
		}
	}
	r.RTTMS, r.OffsetLowerMS, r.OffsetUpperMS = float64(best.rtt)/1e6, float64(best.lower)/1e6, float64(best.upper)/1e6
	if overlapLower > overlapUpper {
		r.ClockStep = true
		return r, errors.New("clock samples are inconsistent")
	}
	if best.lower < -budget || best.upper > budget {
		return r, errors.New("clock offset interval exceeds budget")
	}
	r.Verified = true
	return r, nil
}

func checkStartLead(c config, now time.Time) error {
	minimum := time.Second
	if c.PlanFile != "" {
		minimum += clockBudget(c)
	}
	lead := c.Start.Sub(now)
	if lead < minimum {
		return errors.New("original start too close or past after preparation")
	}
	if lead > maxStartWait {
		return errors.New("original start exceeds bounded waiting interval")
	}
	return nil
}

type preflightReport struct {
	Mode              string          `json:"mode"`
	Complete          bool            `json:"complete"`
	Failure           string          `json:"failure,omitempty"`
	PlanSHA256        string          `json:"plan_sha256,omitempty"`
	Generator         int             `json:"generator"`
	UniqueKeys        uint64          `json:"planned_unique_keys"`
	Attempts          uint64          `json:"planned_attempts"`
	LedgerBytes       uint64          `json:"ledger_bytes"`
	WorkerBufferBytes uint64          `json:"worker_buffer_bytes"`
	RequiredOpenFiles uint64          `json:"required_open_files"`
	Clock             *clockReport    `json:"clock,omitempty"`
	Transport         transportReport `json:"transport"`
}

func runPreflight(ctx context.Context, c config) (preflightReport, error) {
	return runPreflightWith(ctx, c, preflight)
}

func runPreflightWith(ctx context.Context, c config, resources func(config) error) (preflightReport, error) {
	r := preflightReport{Mode: "distributed-generator-preflight", Generator: c.Generator, Transport: transportPolicy()}
	fail := func(reason string, err error) (preflightReport, error) { r.Failure = reason; return r, err }
	if err := c.validate(); err != nil || c.PlanFile == "" {
		return fail("invalid_plan_configuration", errors.New("preflight requires valid plan configuration"))
	}
	p, err := loadDistributedPlan(c.PlanFile)
	if err != nil || c.Generator < 0 || c.Generator >= len(p.Generators) || !sameConfig(c, p.Generators[c.Generator]) {
		return fail("invalid_distributed_plan", errors.New("preflight configuration differs from plan"))
	}
	r.PlanSHA256, r.UniqueKeys, r.Attempts = p.digest(), c.keys(), c.attempts()
	r.LedgerBytes, r.WorkerBufferBytes = c.attempts()*ledgerBytes, uint64(c.Workers+1)*64*1024
	r.RequiredOpenFiles = uint64(2*c.Workers + 64)
	if c.Directory, err = filepath.Abs(c.Directory); err != nil {
		return fail("invalid_ledger_directory", err)
	}
	if err := resources(c); err != nil {
		return fail("ledger_preflight_failed", err)
	}
	client := newClient(c.Workers, c.Timeout)
	defer client.CloseIdleConnections()
	actual, err := fetchPoll(ctx, c, client)
	if err != nil || !samePoll(actual, p.Poll) {
		return fail("http_poll_validation_failed", errors.New("HTTP poll differs from plan"))
	}
	clock, err := checkServerClock(ctx, c, client)
	r.Clock = &clock
	if err != nil {
		return fail("clock_check_failed", err)
	}
	if err := checkStartLead(c, time.Now()); err != nil {
		return fail("original_start_outside_ready_interval", err)
	}
	r.Complete = true
	return r, nil
}

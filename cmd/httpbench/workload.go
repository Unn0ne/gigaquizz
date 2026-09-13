package main

import (
	"bytes"
	"context"
	"crypto/aes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/bits"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gigaquizz/internal/poll"
)

type workerStats struct {
	Decoder          journeyDecoder
	Counts           [stateCount]uint64
	InvalidPositive  uint64
	Sent             uint64
	Latency          [64]uint64
	LatencyNS        uint64
	MaxLatencyNS     uint64
	PerSecondSent    [60]uint64
	PerSecondACK     [60]uint64
	JourneyStarted   uint64
	JourneyCompleted uint64
	JourneyNS        uint64
	JourneyMaxNS     uint64
	GET              [journeyStages]getStats
}
type workloadReport struct {
	PlanSHA256       string          `json:"plan_sha256,omitempty"`
	Generator        int             `json:"generator"`
	Clock            *clockReport    `json:"clock,omitempty"`
	Transport        transportReport `json:"transport"`
	Failure          string          `json:"failure,omitempty"`
	Mode             string          `json:"mode"`
	Complete         bool            `json:"complete"`
	Cancelled        bool            `json:"cancelled"`
	UniqueRate       float64         `json:"planned_unique_per_second"`
	Attempts         uint64          `json:"planned_attempts"`
	UniqueKeys       uint64          `json:"planned_unique_keys"`
	RepeatEvery      uint64          `json:"repeat_every"`
	Workers          int             `json:"workers"`
	Queue            int             `json:"queue"`
	MaxLagMS         float64         `json:"max_lag_ms"`
	DurationSeconds  float64         `json:"scheduled_seconds"`
	WallSeconds      float64         `json:"wall_seconds_including_drain"`
	Sent             uint64          `json:"http_post_sent"`
	ACK              uint64          `json:"valid_recorded_ack"`
	Unknown          uint64          `json:"unknown"`
	Closed           uint64          `json:"closed"`
	NotAdmitted      uint64          `json:"not_admitted"`
	NotOpen          uint64          `json:"not_open"`
	Rejected         uint64          `json:"rejected"`
	Skipped          uint64          `json:"generator_skipped"`
	JourneyFailed    uint64          `json:"journey_failed"`
	JourneyStarted   uint64          `json:"journey_started"`
	JourneyCompleted uint64          `json:"journey_completed_before_post"`
	GETSent          uint64          `json:"http_get_sent"`
	GETFailures      uint64          `json:"http_get_failures"`
	GETBytes         uint64          `json:"http_get_body_bytes"`
	GETDecodedBytes  uint64          `json:"http_get_decoded_body_bytes"`
	GETStages        []getReport     `json:"http_get_stages,omitempty"`
	JourneyMeanMS    float64         `json:"journey_mean_ms"`
	JourneyMaxMS     float64         `json:"journey_max_ms"`
	InvalidPositive  uint64          `json:"invalid_successful_responses"`
	LedgerBytes      uint64          `json:"ledger_bytes"`
	LatencyP50MS     float64         `json:"latency_p50_upper_bound_ms"`
	LatencyP95MS     float64         `json:"latency_p95_upper_bound_ms"`
	LatencyP99MS     float64         `json:"latency_p99_upper_bound_ms"`
	LatencyMaxMS     float64         `json:"latency_max_ms"`
	LatencyMeanMS    float64         `json:"latency_mean_ms"`
	SentPerSecond    [60]uint64      `json:"post_sent_per_scheduled_second"`
	ACKPerSecond     [60]uint64      `json:"ack_received_per_scheduled_second"`
	Method           string          `json:"method"`
	LatencyScope     string          `json:"post_latency_scope"`
}

func newClient(workers int, timeout time.Duration) *http.Client {
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	tr := &http.Transport{Protocols: protocols, DialContext: (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext, MaxConnsPerHost: workers, MaxIdleConns: workers, MaxIdleConnsPerHost: workers, IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: timeout, DisableCompression: true}
	return &http.Client{Transport: tr, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}
func validatePoll(c config, p poll.Poll) error {
	if p.ID != c.PollID || !p.StartsAt.Equal(c.Start) || !p.EndsAt.Equal(c.Start.Add(c.Duration)) || p.FinalizedAt != nil || len(p.Options) < 2 || len(p.Options) > 20 || (p.Type != "ab" && p.Type != "single" && p.Type != "multiple") {
		return errors.New("HTTP poll differs from original benchmark window or definition")
	}
	for i, o := range p.Options {
		if o.ID != i+1 {
			return errors.New("poll option IDs must be contiguous")
		}
	}
	return nil
}
func fetchPoll(ctx context.Context, c config, client *http.Client) (poll.Poll, error) {
	var p poll.Poll
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.URL, "/")+"/api/polls/"+c.PollID, nil)
	if err != nil {
		return p, err
	}
	resp, err := client.Do(r)
	if err != nil {
		return p, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 65537))
	if err != nil || len(b) > 65536 || resp.StatusCode != 200 || json.Unmarshal(b, &p) != nil {
		return p, errors.New("cannot validate original HTTP poll")
	}
	return p, validatePoll(c, p)
}

func runWorkload(ctx context.Context, c config) (workloadReport, error) {
	zero := workloadReport{Mode: "http-post", Generator: c.Generator, Transport: transportPolicy()}
	if c.Journey {
		zero.Mode = "http-journey"
	}
	c.URL = strings.TrimRight(c.URL, "/")
	var err error
	if c.Directory, err = filepath.Abs(c.Directory); err != nil {
		zero.Failure = "invalid_ledger_directory"
		return zero, err
	}
	if err = preflight(c); err != nil {
		zero.Failure = "ledger_preflight_failed"
		return zero, err
	}
	client := newClient(c.Workers, c.Timeout)
	defer client.CloseIdleConnections()
	p, err := fetchPoll(ctx, c, client)
	if err != nil {
		zero.Failure = "http_poll_validation_failed"
		return zero, err
	}
	if err := checkStartLead(c, time.Now()); err != nil {
		zero.Failure = "original_start_outside_ready_interval"
		return zero, err
	}
	m, err := createManifest(c, privateManifest{Poll: p})
	if err != nil {
		zero.Failure = "private_manifest_creation_failed"
		return zero, err
	}
	return executeWorkload(ctx, m, client)
}

func executeWorkload(ctx context.Context, m privateManifest, client *http.Client) (workloadReport, error) {
	c := m.Config
	writers := make([]*ledgerWriter, c.Workers+1)
	for i := range writers {
		w, err := newLedger(c.Directory, i)
		if err != nil {
			for _, w := range writers {
				if w != nil {
					_, _ = w.close()
				}
			}
			return workloadReport{}, err
		}
		writers[i] = w
	}
	var start time.Time // Set before any jobs are sent; the queue synchronizes it.
	jobs := make(chan attempt, c.Queue)
	stats := make([]workerStats, c.Workers+1)
	var workers sync.WaitGroup
	var ready sync.WaitGroup
	ready.Add(c.Workers)
	for i := 0; i < c.Workers; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			block, _ := aes.NewCipher(m.Seed[:])
			ready.Done()
			for a := range jobs {
				a.Token = tokenFor(block, m.Namespace, c.KeyOffset+a.Key)
				if ctx.Err() != nil || time.Since(start)-time.Duration(a.ScheduledNS) > c.MaxLag || writers[i].err != nil {
					a.State = stateSkipped
				} else {
					dispatchAttempt(ctx, c, client, &a, &stats[i], start)
				}
				stats[i].Counts[a.State]++
				if a.Flags&flagInvalidPositive != 0 {
					stats[i].InvalidPositive++
				}
				writers[i].append(a)
			}
		}(i)
	}
	ready.Wait()
	var clock *clockReport
	abortBeforeSchedule := func(reason string, cause error) (workloadReport, error) {
		close(jobs)
		workers.Wait()
		for _, w := range writers {
			_, _ = w.close()
		}
		// The initial manifest remains explicitly incomplete: no partial setup
		// or failed clock check can certify a completed client workload.
		r := summarize(c, stats, 0, 0)
		r.Failure, r.Cancelled = reason, ctx.Err() != nil
		r.PlanSHA256, r.Generator, r.Clock = m.PlanSHA256, c.Generator, clock
		return r, cause
	}
	if c.PlanFile != "" {
		checked, err := checkServerClock(ctx, c, client)
		clock = &checked
		if err != nil {
			return abortBeforeSchedule("clock_check_failed", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return abortBeforeSchedule("cancelled_before_schedule", err)
	}
	if err := checkStartLead(c, time.Now()); err != nil {
		return abortBeforeSchedule("original_start_outside_ready_interval", err)
	}
	// Keep a monotonic origin derived from the immutable start. The clock
	// guard certifies readiness; it never shifts the schedule to compensate.
	now := time.Now()
	start = now.Add(c.Start.Sub(now))
	block, _ := aes.NewCipher(m.Seed[:])
	scheduler := writers[c.Workers]
	for key := uint64(0); key < c.keys(); key++ {
		offset := c.scheduledOffset(key)
		// Wake in at most 1ms batches instead of allocating a timer per request.
		if ctx.Err() == nil {
			remaining := start.Add(offset).Sub(time.Now())
			if remaining > 0 {
				if remaining < time.Millisecond {
					remaining = time.Millisecond
				}
				t := time.NewTimer(remaining)
				select {
				case <-ctx.Done():
					t.Stop()
				case <-t.C:
				}
			}
		}
		emit := func(choice uint32) {
			seq, _ := sequence(c, key, choice)
			a := attempt{Seq: seq, Key: key, Choice: choice, ScheduledNS: int64(offset)}
			if ctx.Err() == nil && scheduler.err == nil && time.Since(start)-offset <= c.MaxLag {
				select {
				case jobs <- a:
					return
				default:
				}
			}
			a.Token = tokenFor(block, m.Namespace, c.KeyOffset+key)
			a.State = stateSkipped
			scheduler.append(a)
			stats[c.Workers].Counts[stateSkipped]++
		}
		emit(1)
		if c.RepeatEvery != 0 && (c.KeyOffset+key+1)%c.RepeatEvery == 0 {
			emit(2)
		}
	}
	close(jobs)
	workers.Wait()
	var firstErr error
	var totalRecords uint64
	for _, w := range writers {
		meta, err := w.close()
		m.Files = append(m.Files, meta)
		totalRecords += meta.Records
		if firstErr == nil {
			firstErr = err
		}
	}
	if totalRecords != c.attempts() && firstErr == nil {
		firstErr = errors.New("incomplete client ledger")
	}
	m.Complete = firstErr == nil
	if err := writeManifest(c.Directory, m); firstErr == nil {
		firstErr = err
	}
	r := summarize(c, stats, time.Since(start), totalRecords*ledgerBytes)
	r.PlanSHA256, r.Generator, r.Clock = m.PlanSHA256, c.Generator, clock
	r.Complete = firstErr == nil
	r.Cancelled = ctx.Err() != nil
	if firstErr != nil {
		r.Failure = "private_ledger_persistence_failed"
	}
	if firstErr == nil && ctx.Err() != nil {
		firstErr = ctx.Err()
	}
	return r, firstErr
}

func sendAttempt(ctx context.Context, c config, client *http.Client, a *attempt, s *workerStats, start time.Time) {
	var body [58]byte
	// Fixed single-choice request, no cookies or automatic application retries.
	b := append(body[:0], `{"token":"`...)
	b = hex.AppendEncode(b, a.Token[:])
	b = append(b, `","choices":[`...)
	if a.Choice == 1 {
		b = append(b, '1')
	} else {
		b = append(b, '2')
	}
	b = append(b, ']', '}')
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+"/api/polls/"+c.PollID+"/votes", bytes.NewReader(b))
	if err != nil {
		a.State = stateUnknown
		return
	}
	r.Header.Set("Content-Type", "application/json")
	begin := time.Now()
	s.Sent++
	if bucket := int(begin.Sub(start) / time.Second); bucket >= 0 && bucket < 60 {
		s.PerSecondSent[bucket]++
	}
	resp, err := client.Do(r)
	if err != nil {
		a.State = stateUnknown
	} else {
		a.HTTP = uint16(resp.StatusCode)
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, 4097))
		closeErr := resp.Body.Close()
		if readErr != nil || closeErr != nil || len(data) > 4096 {
			a.State = stateUnknown
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				a.Flags |= flagInvalidPositive
			}
		} else {
			classifyResponse(c, a, resp.StatusCode, data)
		}
	}
	elapsed := uint64(time.Since(begin))
	s.Latency[bits.Len64(elapsed)]++
	s.LatencyNS += elapsed
	if elapsed > s.MaxLatencyNS {
		s.MaxLatencyNS = elapsed
	}
	if a.State == stateACK {
		if bucket := int(time.Since(start) / time.Second); bucket >= 0 && bucket < 60 {
			s.PerSecondACK[bucket]++
		}
	}
}

func classifyResponse(c config, a *attempt, status int, data []byte) {
	a.State = stateUnknown
	var receipt struct {
		Status     string     `json:"status"`
		Choices    []int      `json:"choices"`
		AcceptedAt *time.Time `json:"accepted_at"`
		Outcome    string     `json:"outcome"`
	}
	valid := json.Unmarshal(data, &receipt) == nil
	if status >= 200 && status < 300 {
		if valid && status == 202 && receipt.Status == "recorded" && len(receipt.Choices) == 1 && receipt.Choices[0] == int(a.Choice) && receipt.AcceptedAt != nil && !receipt.AcceptedAt.Before(c.Start) && receipt.AcceptedAt.Before(c.Start.Add(c.Duration)) {
			a.State = stateACK
			a.AdmittedNS = receipt.AcceptedAt.UnixNano()
		} else {
			a.Flags |= flagInvalidPositive
		}
		return
	}
	if !valid {
		return
	}
	switch {
	case status == 410 && receipt.Status == "closed":
		a.State = stateClosed
	case status == 425 && receipt.Status == "not_open":
		a.State = stateNotOpen
	case status == 503 && receipt.Outcome == "not_admitted":
		a.State = stateBusy
	case status == 422 || status == 404 || status == 400 || status == 403 || status == 415:
		a.State = stateRejected
	}
}

func summarize(c config, workers []workerStats, elapsed time.Duration, bytes uint64) workloadReport {
	var all workerStats
	for _, s := range workers {
		for i, n := range s.Counts {
			all.Counts[i] += n
		}
		for i, n := range s.Latency {
			all.Latency[i] += n
		}
		for i := range s.PerSecondSent {
			all.PerSecondSent[i] += s.PerSecondSent[i]
			all.PerSecondACK[i] += s.PerSecondACK[i]
		}
		all.Sent += s.Sent
		all.JourneyStarted += s.JourneyStarted
		all.JourneyCompleted += s.JourneyCompleted
		all.JourneyNS += s.JourneyNS
		if s.JourneyMaxNS > all.JourneyMaxNS {
			all.JourneyMaxNS = s.JourneyMaxNS
		}
		for i, g := range s.GET {
			all.GET[i].Sent += g.Sent
			all.GET[i].Failures += g.Failures
			all.GET[i].Bytes += g.Bytes
			all.GET[i].DecodedBytes += g.DecodedBytes
			all.GET[i].Timeouts += g.Timeouts
			all.GET[i].TransportFailures += g.TransportFailures
			all.GET[i].StatusFailures += g.StatusFailures
			all.GET[i].ValidationFailures += g.ValidationFailures
			all.GET[i].LatencyNS += g.LatencyNS
			if g.MaxNS > all.GET[i].MaxNS {
				all.GET[i].MaxNS = g.MaxNS
			}
		}
		all.InvalidPositive += s.InvalidPositive
		all.LatencyNS += s.LatencyNS
		if s.MaxLatencyNS > all.MaxLatencyNS {
			all.MaxLatencyNS = s.MaxLatencyNS
		}
	}
	quantile := func(percent uint64) float64 {
		var n uint64
		for i, v := range all.Latency {
			n += v
			if all.Sent != 0 && n*100 >= all.Sent*percent {
				return float64(uint64(1)<<i) / 1e6
			}
		}
		return 0
	}
	r := workloadReport{Mode: "http-post", UniqueRate: float64(c.keys()) / c.Duration.Seconds(), Attempts: c.attempts(), UniqueKeys: c.keys(), RepeatEvery: c.RepeatEvery, Workers: c.Workers, Queue: c.Queue, MaxLagMS: float64(c.MaxLag) / 1e6, DurationSeconds: c.Duration.Seconds(), WallSeconds: elapsed.Seconds(), Sent: all.Sent, ACK: all.Counts[stateACK], Unknown: all.Counts[stateUnknown], Closed: all.Counts[stateClosed], NotAdmitted: all.Counts[stateBusy], NotOpen: all.Counts[stateNotOpen], Rejected: all.Counts[stateRejected], Skipped: all.Counts[stateSkipped], InvalidPositive: all.InvalidPositive, LedgerBytes: bytes, LatencyP50MS: quantile(50), LatencyP95MS: quantile(95), LatencyP99MS: quantile(99), LatencyMaxMS: float64(all.MaxLatencyNS) / 1e6, SentPerSecond: all.PerSecondSent, ACKPerSecond: all.PerSecondACK, Method: "Uniform scheduled unique IDs; one independent POST per attempt; bounded workers and queue; stale/full attempts are skips; no request retries. Each worker buffers its private 64-byte full-ID/choice/admission/outcome ledger. 202 recorded is a durable attempt, not canonical uniqueness. No journal reconciliation occurs in this process."}
	if all.Sent > 0 {
		r.LatencyMeanMS = float64(all.LatencyNS) / float64(all.Sent) / 1e6
	}
	r.LatencyScope = "POST dispatch through response body read; excludes scheduler and preceding GETs"
	r.Transport = transportPolicy()
	if c.Journey {
		r.Mode = "http-journey"
		if c.Definition {
			r.Mode = "http-journey-definition-gzip"
		}
		r.JourneyFailed = all.Counts[stateJourneyFailed]
		r.JourneyStarted = all.JourneyStarted
		r.JourneyCompleted = all.JourneyCompleted
		r.JourneyMaxMS = float64(all.JourneyMaxNS) / 1e6
		if all.JourneyStarted > 0 {
			r.JourneyMeanMS = float64(all.JourneyNS) / float64(all.JourneyStarted) / 1e6
		}
		for i, g := range all.GET {
			r.GETSent += g.Sent
			r.GETFailures += g.Failures
			r.GETBytes += g.Bytes
			r.GETDecodedBytes += g.DecodedBytes
			stage := getReport{Timeouts: g.Timeouts, TransportFailures: g.TransportFailures, StatusFailures: g.StatusFailures, ValidationFailures: g.ValidationFailures, DecodedBytes: g.DecodedBytes, Name: journeyNames[i], Sent: g.Sent, Failures: g.Failures, Bytes: g.Bytes, MaxMS: float64(g.MaxNS) / 1e6}
			if g.Sent > 0 {
				stage.MeanMS = float64(g.LatencyNS) / float64(g.Sent) / 1e6
			}
			r.GETStages = append(r.GETStages, stage)
		}
		if c.Definition {
			r.Method += " Uses immutable /definition and explicitly negotiated gzip with bounded decoding; encoded and decoded body bytes reported separately."
		}
		r.Method += " Each original attempt first fetches HTML, app.css, common.js, poll.js and the original question sequentially with no browser cache or JS execution. Repeats send only POST. The GET chain must finish before the original scheduled slot plus max-lag; a failed/expired chain is journey_failed and sends no POST. GET metrics count client.Do calls and actually read response-body bytes, excluding headers, transport/TLS overhead and the one setup validation GET; they do not include any transparent GET connection repair inside Go Transport."
	}
	return r
}

package main

import (
	"math"
	"strconv"
	"sync"
	"time"
)

// Fixed buckets have at most 10% relative width above 1us. Percentiles report
// bucket upper bounds, capped at the observed maximum; memory is independent
// of the request count. Quantiles are descriptive, not exact measurements.
var latencyBounds = func() [320]time.Duration {
	var bounds [320]time.Duration
	bounds[0] = time.Microsecond
	for i := 1; i < len(bounds); i++ {
		bounds[i] = time.Duration(math.Ceil(float64(bounds[i-1]) * 1.1))
	}
	return bounds
}()

type histogram struct {
	buckets [320]int64
	count   int64
	sum     float64
	max     time.Duration
}

func (h *histogram) add(d time.Duration) {
	d = max(0, d)
	lo, hi := 0, len(latencyBounds)-1
	for lo < hi {
		mid := (lo + hi) / 2
		if latencyBounds[mid] < d {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	h.buckets[lo]++
	h.count++
	h.sum += float64(d)
	h.max = max(h.max, d)
}

type latencyReport struct {
	Count  int64   `json:"count"`
	MeanMS float64 `json:"mean_ms"`
	P50MS  float64 `json:"p50_upper_bound_ms"`
	P95MS  float64 `json:"p95_upper_bound_ms"`
	P99MS  float64 `json:"p99_upper_bound_ms"`
	MaxMS  float64 `json:"max_ms"`
}

func (h histogram) percentile(q float64) float64 {
	if h.count == 0 {
		return 0
	}
	rank := int64(math.Ceil(float64(h.count) * q))
	var seen int64
	for i, n := range h.buckets {
		seen += n
		if seen >= rank {
			return float64(min(latencyBounds[i], h.max)) / float64(time.Millisecond)
		}
	}
	return float64(h.max) / float64(time.Millisecond)
}

func (h histogram) report() latencyReport {
	r := latencyReport{Count: h.count, P50MS: h.percentile(.5), P95MS: h.percentile(.95), P99MS: h.percentile(.99), MaxMS: float64(h.max) / float64(time.Millisecond)}
	if h.count > 0 {
		r.MeanMS = h.sum / float64(h.count) / float64(time.Millisecond)
	}
	return r
}

type metrics struct {
	mu                                                                    sync.Mutex
	planned, scheduled, enqueued, dispatched                              int64
	skipped                                                               map[string]int64
	attempts, duringWindow, responses, completeResponses                  int64
	transportErrors, bodyErrors, localErrors                              int64
	confirmed, unknown, rejected                                          int64
	statuses                                                              map[string]int64
	dispatchLag, attemptElapsed, attemptFromSchedule, logicalFromSchedule histogram
}

func newMetrics(planned int64) *metrics {
	return &metrics{planned: planned, skipped: map[string]int64{}, statuses: map[string]int64{}}
}
func (m *metrics) scheduledSlots(n int64) { m.mu.Lock(); defer m.mu.Unlock(); m.scheduled += n }
func (m *metrics) enqueuedSlot()          { m.mu.Lock(); defer m.mu.Unlock(); m.enqueued++ }
func (m *metrics) skip(reason string, n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.skipped[reason] += n
}
func (m *metrics) dispatch(lag time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dispatched++
	m.dispatchLag.add(lag)
}
func (m *metrics) localError() { m.mu.Lock(); defer m.mu.Unlock(); m.localErrors++ }
func (m *metrics) beginAttempt(inWindow bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.attempts++
	if inWindow {
		m.duringWindow++
	}
}
func (m *metrics) finishAttempt(status int, transportError, bodyError bool, elapsed, fromSchedule time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if status != 0 {
		m.statuses[strconv.Itoa(status)]++
		m.responses++
		if !transportError && !bodyError {
			m.completeResponses++
		}
	}
	if transportError {
		m.transportErrors++
	}
	if bodyError {
		m.bodyErrors++
	}
	m.attemptElapsed.add(elapsed)
	m.attemptFromSchedule.add(fromSchedule)
}
func (m *metrics) finishLogical(confirmed, unknown bool, elapsed time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if confirmed {
		m.confirmed++
	} else if unknown {
		m.unknown++
	} else {
		m.rejected++
	}
	m.logicalFromSchedule.add(elapsed)
}

type report struct {
	ConfiguredLogicalRate    int64            `json:"configured_logical_ops_per_second"`
	ConfiguredWindowSeconds  float64          `json:"configured_window_seconds"`
	ElapsedSeconds           float64          `json:"elapsed_seconds_including_drain"`
	Interrupted              bool             `json:"interrupted_or_drain_deadline_reached"`
	LogicalPlanned           int64            `json:"logical_planned"`
	LogicalScheduled         int64            `json:"logical_scheduled"`
	LogicalNotScheduled      int64            `json:"logical_not_scheduled"`
	LogicalEnqueued          int64            `json:"logical_enqueued"`
	LogicalDispatched        int64            `json:"logical_dispatched"`
	LogicalSkipped           int64            `json:"logical_skipped"`
	SkippedByReason          map[string]int64 `json:"skipped_by_reason"`
	HTTPAttempts             int64            `json:"http_attempts"`
	HTTPAttemptsDuringWindow int64            `json:"http_attempts_started_during_window"`
	HTTPAttemptsAfterWindow  int64            `json:"http_attempts_started_after_window"`
	AchievedSendRate         float64          `json:"http_attempts_during_window_per_configured_second"`
	OverallSendRate          float64          `json:"http_attempts_per_elapsed_second"`
	HTTPResponses            int64            `json:"http_response_headers_received"`
	HTTPCompleteResponses    int64            `json:"http_responses_fully_read"`
	Statuses                 map[string]int64 `json:"http_statuses"`
	Recorded202              int64            `json:"recorded_202_responses"`
	Accepted201              int64            `json:"accepted_201_responses"`
	Duplicate200             int64            `json:"duplicate_200_responses"`
	TransportErrors          int64            `json:"transport_errors_unknown_outcome"`
	BodyErrors               int64            `json:"response_body_errors"`
	LocalErrors              int64            `json:"local_request_errors"`
	Unknown503               int64            `json:"unknown_503_responses"`
	LogicalConfirmed         int64            `json:"logical_with_acceptance_confirmation"`
	LogicalUnknown           int64            `json:"logical_without_confirmation_unknown_outcome"`
	LogicalRejected          int64            `json:"logical_without_confirmation_only_other_responses"`
	DispatchLag              latencyReport    `json:"initial_dispatch_lag"`
	AttemptLatency           latencyReport    `json:"http_attempt_elapsed"`
	AttemptEndToEnd          latencyReport    `json:"http_attempt_end_to_end_from_logical_schedule"`
	LogicalEndToEnd          latencyReport    `json:"logical_end_to_end_including_optional_retry"`
	Limitations              []string         `json:"limitations"`
}

func (m *metrics) snapshot(c config, elapsed time.Duration, interrupted bool) report {
	m.mu.Lock()
	defer m.mu.Unlock()
	var skipped int64
	for _, n := range m.skipped {
		skipped += n
	}
	return report{
		ConfiguredLogicalRate: c.rate, ConfiguredWindowSeconds: c.duration.Seconds(), ElapsedSeconds: elapsed.Seconds(), Interrupted: interrupted,
		LogicalPlanned: m.planned, LogicalScheduled: m.scheduled, LogicalNotScheduled: m.planned - m.scheduled, LogicalEnqueued: m.enqueued,
		LogicalDispatched: m.dispatched, LogicalSkipped: skipped, SkippedByReason: m.skipped,
		HTTPAttempts: m.attempts, HTTPAttemptsDuringWindow: m.duringWindow, HTTPAttemptsAfterWindow: m.attempts - m.duringWindow,
		AchievedSendRate: float64(m.duringWindow) / c.duration.Seconds(), OverallSendRate: float64(m.attempts) / elapsed.Seconds(),
		HTTPResponses: m.responses, HTTPCompleteResponses: m.completeResponses, Statuses: m.statuses,
		Recorded202: m.statuses["202"], Accepted201: m.statuses["201"], Duplicate200: m.statuses["200"], TransportErrors: m.transportErrors, BodyErrors: m.bodyErrors,
		LocalErrors: m.localErrors, Unknown503: m.statuses["503"], LogicalConfirmed: m.confirmed, LogicalUnknown: m.unknown, LogicalRejected: m.rejected,
		DispatchLag: m.dispatchLag.report(), AttemptLatency: m.attemptElapsed.report(), AttemptEndToEnd: m.attemptFromSchedule.report(), LogicalEndToEnd: m.logicalFromSchedule.report(),
		Limitations: []string{
			"POST-only generator; page delivery, TLS population and full voter journey are not represented",
			"latency percentiles are fixed-bucket upper estimates (approximately 10% bucket width above 1us); skipped slots are reported separately",
			"201/200/202 confirm storage for a logical key at most once here; 202 confirms an attempt, not the final unique choice; timeout or 503 does not prove loss; reconcile server records and final results separately",
			"successful execution and local throughput do not certify 100 million voters in 60 seconds",
		},
	}
}

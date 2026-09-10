package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sort"
	"sync"
	"time"

	"gigaquizz/internal/poll"
)

type keyState struct {
	Token             string `json:"token"`
	SentChoices       uint8  `json:"sent_choices_mask"`
	ConfirmedChoices  uint8  `json:"confirmed_choices_mask"`
	ReceiptChoices    uint8  `json:"receipt_choices_mask"`
	Accepted          int    `json:"accepted_responses"`
	Unknown           bool   `json:"had_unknown_attempt"`
	firstConfirmation time.Duration
	hasConfirmation   bool
}

type attemptJob struct {
	index, choice int
	scheduled     time.Time
}
type voteFunc func(context.Context, string, string, []int) (poll.Receipt, error)

type observations struct {
	mu                                    sync.Mutex
	keys                                  []keyState
	scheduled, queued, sent, duringWindow int
	skips                                 map[string]int
	outcomes                              map[string]int
	invalidReceipts                       int
	lag, elapsed, fromSchedule            []time.Duration
	seconds                               [timelineSeconds]secondBucket
	lastSecond                            int
}

func newObservations(total int) (*observations, error) {
	m := &observations{keys: make([]keyState, total), skips: make(map[string]int), outcomes: make(map[string]int)}
	for i := range m.keys {
		var raw [16]byte
		if _, err := rand.Read(raw[:]); err != nil {
			return nil, err
		}
		m.keys[i].Token = hex.EncodeToString(raw[:])
	}
	return m, nil
}

func choiceMask(choices []int) uint8 {
	if len(choices) != 1 || choices[0] < 1 || choices[0] > 2 {
		return 0
	}
	return 1 << uint(choices[0]-1)
}

func (m *observations) skip(reason string, n int, scheduledOffset time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.skips[reason] += n
	b := m.bucket(scheduledOffset)
	if b.Skipped == nil {
		b.Skipped = make(map[string]int)
	}
	b.Skipped[reason] += n
}
func (m *observations) markStart(j attemptJob, start time.Time, window time.Duration, began time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent++
	m.keys[j.index].SentChoices |= 1 << uint(j.choice-1)
	m.lag = append(m.lag, began.Sub(j.scheduled))
	m.bucket(began.Sub(start)).Sent++
	if began.Sub(start) < window {
		m.duringWindow++
	}
}
func (m *observations) finish(j attemptJob, r poll.Receipt, err error, began, finished, start time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fromSchedule := finished.Sub(j.scheduled)
	m.elapsed = append(m.elapsed, finished.Sub(began))
	m.fromSchedule = append(m.fromSchedule, fromSchedule)
	k := &m.keys[j.index]
	status := r.Status
	if err != nil {
		status = "unknown_error"
	}
	if status == "" {
		status = "unknown_empty_receipt"
	}
	m.outcomes[status]++
	b := m.bucket(finished.Sub(start))
	if err != nil || status == "unknown" || status == "unknown_empty_receipt" {
		b.Unknown++
		k.Unknown = true
		return
	}
	if status == "accepted" || status == "duplicate" || status == "conflict" {
		mask := choiceMask(r.Choices)
		if mask == 0 || r.AcceptedAt == nil {
			m.invalidReceipts++
		}
		k.ReceiptChoices |= mask
		if status == "accepted" || status == "duplicate" {
			if mask != (1 << uint(j.choice-1)) {
				m.invalidReceipts++
			}
			if mask == (1<<uint(j.choice-1)) && r.AcceptedAt != nil {
				if !k.hasConfirmation || fromSchedule < k.firstConfirmation {
					k.firstConfirmation = fromSchedule
					k.hasConfirmation = true
				}
				k.ConfirmedChoices |= mask
			}
		}
		if status == "accepted" {
			b.Accepted++
			k.Accepted++
		}
	}
}

func slotsBefore(elapsed time.Duration, rate int) int {
	if elapsed <= 0 {
		return 0
	}
	return int(elapsed/time.Second)*rate + int((int64(elapsed%time.Second)*int64(rate)+int64(time.Second)-1)/int64(time.Second))
}
func offset(index, rate int) time.Duration {
	return time.Duration(index/rate)*time.Second + time.Duration(index%rate)*time.Second/time.Duration(rate)
}
func attemptsInRange(begin, end, repeatEvery int) int {
	n := end - begin
	if repeatEvery > 0 {
		n += end/repeatEvery - begin/repeatEvery
	}
	return n
}

func schedule(ctx context.Context, c config, start time.Time, jobs chan<- attemptJob, m *observations) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	<-timer.C
	for next := 0; next < c.total; {
		if ctx.Err() != nil {
			return
		}
		now := time.Now()
		obsolete := min(c.total, slotsBefore(now.Sub(start)-c.maxLag, c.rate))
		if obsolete > next {
			m.scheduledRange(c, next, obsolete)
			m.skipRange(c, next, obsolete, "scheduler_lag")
			next = obsolete
			continue
		}
		scheduled := start.Add(offset(next, c.rate))
		if delay := scheduled.Sub(now); delay > 0 {
			timer.Reset(delay)
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			continue
		}
		m.scheduledRange(c, next, next+1)
		offer := func(choice int) {
			select {
			case jobs <- attemptJob{next, choice, scheduled}:
				m.mu.Lock()
				m.queued++
				m.mu.Unlock()
			default:
				m.skip("queue_full", 1, scheduled.Sub(start))
			}
		}
		offer(1)
		if c.repeatEvery > 0 && (next+1)%c.repeatEvery == 0 {
			choice := 1
			if c.different {
				choice = 2
			}
			offer(choice)
		}
		next++
	}
}

func runWorkload(parent context.Context, c config, pollID string, m *observations, vote voteFunc) workloadReport {
	start := time.Now()
	ctx, cancel := context.WithDeadline(parent, start.Add(c.window()+c.drain))
	defer cancel()
	jobs := make(chan attemptJob, c.queue)
	var workers sync.WaitGroup
	for range c.workers {
		workers.Go(func() {
			for j := range jobs {
				if ctx.Err() != nil {
					m.skip("cancelled", 1, j.scheduled.Sub(start))
					continue
				}
				if time.Since(j.scheduled) > c.maxLag {
					m.skip("worker_lag", 1, j.scheduled.Sub(start))
					continue
				}
				operation, cancelOperation := context.WithTimeout(ctx, c.timeout)
				began := time.Now()
				m.markStart(j, start, c.window(), began)
				receipt, err := vote(operation, pollID, m.keys[j.index].Token, []int{j.choice})
				finished := time.Now()
				cancelOperation()
				m.finish(j, receipt, err, began, finished, start)
			}
		})
	}
	schedule(ctx, c, start, jobs, m)
	close(jobs)
	workers.Wait()
	finished := time.Now()
	r := m.report(c, finished.Sub(start), ctx.Err() != nil)
	r.StartedAt = start.UTC()
	r.FinishedAt = finished.UTC()
	return r
}

type latencyReport struct {
	Count  int     `json:"count"`
	MeanMS float64 `json:"mean_ms"`
	P50MS  float64 `json:"p50_ms"`
	P95MS  float64 `json:"p95_ms"`
	P99MS  float64 `json:"p99_ms"`
	MaxMS  float64 `json:"max_ms"`
}

func latency(values []time.Duration) latencyReport {
	if len(values) == 0 {
		return latencyReport{}
	}
	ordered := append([]time.Duration(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	var sum float64
	for _, v := range values {
		sum += float64(v)
	}
	quantile := func(percent int) float64 {
		index := (len(ordered)*percent+99)/100 - 1
		return float64(ordered[index]) / float64(time.Millisecond)
	}
	return latencyReport{Count: len(values), MeanMS: sum / float64(len(values)) / float64(time.Millisecond), P50MS: quantile(50), P95MS: quantile(95), P99MS: quantile(99), MaxMS: float64(ordered[len(ordered)-1]) / float64(time.Millisecond)}
}

type workloadReport struct {
	StartedAt              time.Time              `json:"started_at"`
	FinishedAt             time.Time              `json:"finished_at"`
	LogicalPlanned         int                    `json:"logical_planned"`
	LogicalScheduled       int                    `json:"logical_scheduled"`
	LogicalSent            int                    `json:"logical_sent"`
	LogicalNotSent         int                    `json:"logical_not_sent"`
	LogicalConfirmed       int                    `json:"logical_with_client_confirmation"`
	LogicalUnknown         int                    `json:"logical_unconfirmed_with_unknown_attempt"`
	AttemptPlanned         int                    `json:"attempts_planned"`
	AttemptQueued          int                    `json:"attempts_queued"`
	AttemptSent            int                    `json:"attempts_sent"`
	AttemptNotScheduled    int                    `json:"attempts_not_scheduled"`
	AttemptSkipped         int                    `json:"attempts_skipped"`
	Skipped                map[string]int         `json:"skipped_attempts_by_reason"`
	Outcomes               map[string]int         `json:"outcomes"`
	InvalidReceipts        int                    `json:"invalid_receipts"`
	WindowSeconds          float64                `json:"scheduled_window_seconds"`
	ExecutionSeconds       float64                `json:"workload_seconds_including_drain"`
	AttemptDuringWindow    int                    `json:"attempts_started_during_window"`
	AchievedRate           float64                `json:"attempts_started_during_window_per_scheduled_second"`
	OverallRate            float64                `json:"attempts_per_workload_second"`
	Interrupted            bool                   `json:"interrupted_or_drain_deadline"`
	InitialLag             latencyReport          `json:"dispatch_lag"`
	AttemptLatency         latencyReport          `json:"database_call_latency"`
	EndToEnd               latencyReport          `json:"attempt_end_to_end_from_schedule"`
	Confirmation           latencyReport          `json:"first_client_confirmation_from_schedule"`
	ConfirmationThresholds confirmationThresholds `json:"first_confirmation_deadline_counts_from_schedule"`
	Seconds                []secondBucket         `json:"one_second_buckets"`
	TimeBasis              string                 `json:"one_second_bucket_time_basis"`
}

func (m *observations) report(c config, elapsed time.Duration, interrupted bool) workloadReport {
	seconds := m.seconds
	confirmation := make([]time.Duration, 0)
	for i := 0; i <= m.lastSecond; i++ {
		seconds[i].Second = i
		seconds[i].Overflow = i == timelineSeconds-1
	}
	for i, k := range m.keys {
		if k.hasConfirmation {
			confirmation = append(confirmation, k.firstConfirmation)
			seconds[secondIndex(offset(i, c.rate)+k.firstConfirmation)].FirstConfirmations++
		}
	}
	r := workloadReport{LogicalPlanned: c.total, LogicalScheduled: m.scheduled, AttemptPlanned: attemptsInRange(0, c.total, c.repeatEvery), AttemptQueued: m.queued, AttemptSent: m.sent, Skipped: m.skips, Outcomes: m.outcomes, InvalidReceipts: m.invalidReceipts, WindowSeconds: c.window().Seconds(), ExecutionSeconds: elapsed.Seconds(), AttemptDuringWindow: m.duringWindow, AchievedRate: float64(m.duringWindow) / c.window().Seconds(), OverallRate: float64(m.sent) / elapsed.Seconds(), Interrupted: interrupted, InitialLag: latency(m.lag), AttemptLatency: latency(m.elapsed), EndToEnd: latency(m.fromSchedule), Confirmation: latency(confirmation), ConfirmationThresholds: thresholds(confirmation, c.total), Seconds: seconds[:m.lastSecond+1], TimeBasis: "All seconds are relative to the monotonic workload start. Scheduled and skipped counts use each key's original slot; sent uses the timestamp just before send accounting and the repository call; accepted/unknown use the timestamp captured after the call returns. First confirmations use the earliest accepted/duplicate completion for each key. Buckets are [s,s+1); bucket 127 includes all later times."}
	for _, k := range m.keys {
		if k.SentChoices != 0 {
			r.LogicalSent++
		}
		if k.ConfirmedChoices != 0 {
			r.LogicalConfirmed++
		} else if k.Unknown {
			r.LogicalUnknown++
		}
	}
	r.LogicalNotSent = c.total - r.LogicalSent
	for _, n := range m.skips {
		r.AttemptSkipped += n
	}
	r.AttemptNotScheduled = attemptsInRange(m.scheduled, c.total, c.repeatEvery)
	return r
}

package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"time"
)

type outcome uint8

const (
	unstarted outcome = iota
	recorded
	notOpen
	closed
	busy
	rejected
	unknown
	skipQueue
	skipLag
	skipCancelled
)

func (o outcome) String() string {
	switch o {
	case recorded:
		return "recorded"
	case notOpen:
		return "not_open"
	case closed:
		return "closed"
	case busy:
		return "busy"
	case rejected:
		return "rejected"
	case unknown:
		return "unknown"
	case skipQueue:
		return "generator_queue_full"
	case skipLag:
		return "generator_lag"
	case skipCancelled:
		return "generator_cancelled"
	default:
		return "unstarted"
	}
}

// Each entry has one writer during the workload. Aggregation and file I/O happen
// after workers stop, avoiding a central metrics/ledger mutex on the hot path.
type entry struct {
	Token      [16]byte
	Key        uint32
	Choice     uint32
	Outcome    outcome
	Partition  int32
	Offset     int64
	AdmittedNS int64
	DispatchNS int64
	CompleteNS int64
}

type attemptResult struct {
	Outcome    outcome
	Partition  int32
	Offset     int64
	AdmittedAt time.Time
}
type submitter func(context.Context, [16]byte, uint32) attemptResult

func makeEntries(c config, prefix [8]byte) []entry {
	es := make([]entry, 0, c.attempts())
	for key := 0; key < c.keys(); key++ {
		var token [16]byte
		copy(token[:8], prefix[:])
		binary.BigEndian.PutUint64(token[8:], uint64(key)+1)
		e := entry{Token: token, Key: uint32(key), Choice: 1, Partition: -1, Offset: -1}
		es = append(es, e)
		if c.repeatEvery > 0 && (key+1)%c.repeatEvery == 0 {
			if c.different {
				e.Choice = 2
			}
			es = append(es, e)
		}
	}
	return es
}

func scheduledAt(start time.Time, key uint32, rate int) time.Time {
	return start.Add(time.Duration(key) * time.Second / time.Duration(rate))
}

func runWorkload(parent context.Context, c config, start time.Time, entries []entry, submit submitter) workloadReport {
	ctx, cancel := context.WithDeadline(parent, start.Add(c.duration+c.drain))
	defer cancel()
	jobs := make(chan int, c.queue)
	var wg sync.WaitGroup
	for range c.workers {
		wg.Go(func() {
			for index := range jobs {
				e := &entries[index]
				if ctx.Err() != nil {
					e.Outcome = skipCancelled
					continue
				}
				if time.Since(scheduledAt(start, e.Key, c.rate)) > c.maxLag {
					e.Outcome = skipLag
					continue
				}
				callCtx, callCancel := context.WithTimeout(ctx, c.timeout)
				e.DispatchNS = time.Now().UnixNano()
				result := submit(callCtx, e.Token, e.Choice)
				e.CompleteNS = time.Now().UnixNano()
				callCancel()
				e.Outcome = result.Outcome
				e.Partition = result.Partition
				e.Offset = result.Offset
				if !result.AdmittedAt.IsZero() {
					e.AdmittedNS = result.AdmittedAt.UnixNano()
				}
			}
		})
	}
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	for index := range entries {
		e := &entries[index]
		at := scheduledAt(start, e.Key, c.rate)
		// The poll uses wall time. Recheck it after waking: a timer wakeup
		// alone does not establish that the absolute admission time arrived.
		for delay := time.Until(at); delay > 0 && ctx.Err() == nil; delay = time.Until(at) {
			timer.Reset(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			}
		}
		if ctx.Err() != nil {
			e.Outcome = skipCancelled
			continue
		}
		if time.Since(at) > c.maxLag {
			e.Outcome = skipLag
			continue
		}
		select {
		case jobs <- index:
		default:
			e.Outcome = skipQueue
		}
	}
	close(jobs)
	wg.Wait()
	return summarize(c, start, time.Now(), entries)
}

type counts struct {
	Planned    int `json:"planned_attempts"`
	Dispatched int `json:"dispatched_attempts"`
	Skipped    int `json:"generator_skipped_attempts"`
	Recorded   int `json:"recorded_attempts_202"`
	NotOpen    int `json:"not_open"`
	Closed     int `json:"closed"`
	Busy       int `json:"busy"`
	Rejected   int `json:"rejected"`
	Unknown    int `json:"unknown"`
}
type secondBucket struct {
	Second     int `json:"second"`
	Scheduled  int `json:"scheduled"`
	Skipped    int `json:"skipped_by_scheduled_second"`
	Dispatched int `json:"dispatched_by_actual_second"`
	Recorded   int `json:"recorded_by_completion_second"`
	Unknown    int `json:"unknown_by_completion_second"`
	Closed     int `json:"closed_by_completion_second"`
	Busy       int `json:"busy_by_completion_second"`
}
type latencyReport struct {
	Samples int     `json:"samples"`
	MeanMS  float64 `json:"mean_ms"`
	P95MS   float64 `json:"p95_ms"`
	P99MS   float64 `json:"p99_ms"`
	MaxMS   float64 `json:"max_ms"`
}
type latencyPair struct {
	FromSchedule latencyReport `json:"from_original_schedule"`
	FromDispatch latencyReport `json:"from_dispatch"`
}
type workloadReport struct {
	StartsAt           time.Time      `json:"starts_at"`
	FinishedAt         time.Time      `json:"finished_at"`
	Counts             counts         `json:"counts"`
	PlannedKeys        int            `json:"planned_unique_keys"`
	ConfirmedKeys      int            `json:"keys_with_at_least_one_202"`
	KeysWithoutReceipt int            `json:"keys_without_202"`
	SkippedReasons     map[string]int `json:"generator_skip_reasons"`
	Latency            latencyPair    `json:"latency_all_dispatched"`
	RecordedLatency    latencyPair    `json:"latency_recorded_attempts"`
	Buckets            []secondBucket `json:"one_second_buckets"`
	AfterPollDeadline  int            `json:"dispatched_at_or_after_poll_deadline"`
	LateCommit         int            `json:"recorded_after_deadline_admitted_before_deadline"`
	LatencyMethod      string         `json:"latency_method"`
}

func summarize(c config, start, finish time.Time, entries []entry) workloadReport {
	r := workloadReport{StartsAt: start.UTC(), FinishedAt: finish.UTC(), PlannedKeys: c.keys(), SkippedReasons: map[string]int{}, LatencyMethod: fmt.Sprintf("Exact nearest-rank percentiles of completed attempts; schedule includes generator queue, dispatch includes loopback HTTP or direct Submit. No censored/skipped attempts are latency samples. At most two %d-element int64 arrays are live for sorting.", maximumAttempts)}
	r.Counts.Planned = len(entries)
	n := int(c.duration/time.Second) + int(c.drain/time.Second) + 2
	r.Buckets = make([]secondBucket, n)
	for i := range r.Buckets {
		r.Buckets[i].Second = i
	}
	keyConfirmed := make([]bool, c.keys())
	bucket := func(ns int64) int {
		i := int((ns - start.UnixNano()) / int64(time.Second))
		if i < 0 {
			return 0
		}
		if i >= len(r.Buckets) {
			return len(r.Buckets) - 1
		}
		return i
	}
	for _, e := range entries {
		b := &r.Buckets[bucket(scheduledAt(start, e.Key, c.rate).UnixNano())]
		b.Scheduled++
		switch e.Outcome {
		case recorded:
			r.Counts.Recorded++
			keyConfirmed[e.Key] = true
			r.Buckets[bucket(e.CompleteNS)].Recorded++
			if e.CompleteNS >= start.Add(60*time.Second).UnixNano() && e.AdmittedNS < start.Add(60*time.Second).UnixNano() {
				r.LateCommit++
			}
		case notOpen:
			r.Counts.NotOpen++
		case closed:
			r.Counts.Closed++
			r.Buckets[bucket(e.CompleteNS)].Closed++
		case busy:
			r.Counts.Busy++
			r.Buckets[bucket(e.CompleteNS)].Busy++
		case rejected:
			r.Counts.Rejected++
		case unknown:
			r.Counts.Unknown++
			r.Buckets[bucket(e.CompleteNS)].Unknown++
		default:
			r.Counts.Skipped++
			b.Skipped++
			r.SkippedReasons[e.Outcome.String()]++
		}
		if e.DispatchNS > 0 {
			r.Counts.Dispatched++
			r.Buckets[bucket(e.DispatchNS)].Dispatched++
			if e.DispatchNS >= start.Add(60*time.Second).UnixNano() {
				r.AfterPollDeadline++
			}
		}
	}
	for _, v := range keyConfirmed {
		if v {
			r.ConfirmedKeys++
		}
	}
	r.KeysWithoutReceipt = r.PlannedKeys - r.ConfirmedKeys
	r.Latency = sampleLatencies(start, c.rate, entries, false)
	r.RecordedLatency = sampleLatencies(start, c.rate, entries, true)
	return r
}

func sampleLatencies(start time.Time, rate int, entries []entry, onlyRecorded bool) latencyPair {
	schedule := make([]int64, 0, len(entries))
	dispatch := make([]int64, 0, len(entries))
	for _, e := range entries {
		if e.DispatchNS == 0 || e.CompleteNS == 0 || onlyRecorded && e.Outcome != recorded {
			continue
		}
		schedule = append(schedule, e.CompleteNS-scheduledAt(start, e.Key, rate).UnixNano())
		dispatch = append(dispatch, e.CompleteNS-e.DispatchNS)
	}
	return latencyPair{FromSchedule: latencies(schedule), FromDispatch: latencies(dispatch)}
}
func latencies(samples []int64) latencyReport {
	r := latencyReport{Samples: len(samples)}
	if len(samples) == 0 {
		return r
	}
	var sum float64
	for _, n := range samples {
		sum += float64(n)
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	r.MeanMS = sum / float64(len(samples)) / 1e6
	r.P95MS = float64(samples[(len(samples)*95+99)/100-1]) / 1e6
	r.P99MS = float64(samples[(len(samples)*99+99)/100-1]) / 1e6
	r.MaxMS = float64(samples[len(samples)-1]) / 1e6
	return r
}

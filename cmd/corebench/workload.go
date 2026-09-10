package main

import (
	"context"
	"crypto/aes"
	"encoding/binary"
	"sync"
	"time"

	"gigaquizz/internal/votecore"
)

type workerStats struct {
	counts                                                     counts
	buckets                                                    [62]secondBucket
	schedule, dispatch, acceptedSchedule, acceptedDispatch     histogram
	wrongChoice, duplicateAccepted, unexpectedPrimaryDuplicate uint64
}
type workloadReport struct {
	StartsAt            time.Time      `json:"starts_at"`
	FinishedAt          time.Time      `json:"finished_at"`
	Counts              counts         `json:"counts"`
	PlannedKeys         uint64         `json:"planned_unique_keys"`
	Latency             latencyPair    `json:"sampled_latency_all_dispatched"`
	AcceptedLatency     latencyPair    `json:"sampled_latency_accepted"`
	LatencyMethod       string         `json:"latency_method"`
	SampleEvery         uint64         `json:"latency_sample_key_probability_denominator"`
	Coalescing          map[string]any `json:"generator_coalescing"`
	Buckets             []secondBucket `json:"one_second_buckets"`
	WrongChoice         uint64         `json:"responses_with_wrong_canonical_choice"`
	DoubleAccept        uint64         `json:"repeated_acceptance_of_same_key"`
	UnexpectedDuplicate uint64         `json:"duplicate_for_never_previously_submitted_unique_key"`
}

type submitter func(votecore.Input) votecore.Result

func scheduledOffset(key uint64, rate int) time.Duration {
	return time.Duration(key) * time.Second / time.Duration(rate)
}

func waitUntil(ctx context.Context, timer *time.Timer, at time.Time) bool {
	for {
		if ctx.Err() != nil {
			return false
		}
		delay := time.Until(at)
		if delay <= 0 {
			return true
		}
		timer.Reset(delay)
		select {
		case <-timer.C:
			// Recheck the absolute monotonic deadline after every wakeup.
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return false
		}
	}
}

func runWorkload(parent context.Context, c config, start time.Time, seed [16]byte, bitmap []byte, submit submitter) workloadReport {
	ctx, cancel := context.WithDeadline(parent, start.Add(pollWindow+c.maxLag+time.Second))
	defer cancel()
	stats := make([]workerStats, c.workers)
	var workers sync.WaitGroup
	for worker := range c.workers {
		workers.Go(func() { runWorker(ctx, c, start, seed, bitmap, worker, &stats[worker], submit) })
	}
	workers.Wait()
	r := workloadReport{StartsAt: start.UTC(), FinishedAt: time.Now().UTC(), PlannedKeys: c.keys(), SampleEvery: c.sampleEvery(), Buckets: make([]secondBucket, 62), LatencyMethod: "Approximate sampled latency, not exact population p95/p99. Select a key independently of outcome when low AES-token bits are zero; both attempts of a selected repeated key are sampled. Small default runs sample all attempts. Quantiles are histogram upper edges, <6.25% bin rounding above 32ns. Skipped attempts have no latency sample."}
	r.Coalescing = map[string]any{"buffer_inputs_per_worker": c.bufferSize, "group_inputs": c.groupSize(), "configured_interval_us": float64(c.coalesce) / 1000, "maximum_earliest_slot_delay_us": float64(scheduledOffset(uint64(c.groupSize()-1), c.rate)) / 1000, "method": "Each worker prepares a bounded AES buffer. A group waits until its last slot is due, rechecking monotonic time; no member is sent earlier than its own schedule. Waiting does not extend admission beyond 60 seconds."}
	for i := range r.Buckets {
		r.Buckets[i].Second = i
		if i < 60 {
			lo := uint64(i) * uint64(c.rate)
			hi := lo + uint64(c.rate)
			n := hi - lo
			if c.repeatEvery > 0 {
				n += hi/uint64(c.repeatEvery) - lo/uint64(c.repeatEvery)
			}
			r.Buckets[i].Planned = n
		}
	}
	var schedule, dispatch, acceptedSchedule, acceptedDispatch histogram
	for i := range stats {
		s := &stats[i]
		r.Counts.Dispatched += s.counts.Dispatched
		r.Counts.Accepted += s.counts.Accepted
		r.Counts.Duplicate += s.counts.Duplicate
		r.Counts.Closed += s.counts.Closed
		r.Counts.NotOpen += s.counts.NotOpen
		r.Counts.Invalid += s.counts.Invalid
		r.Counts.Capacity += s.counts.Capacity
		r.Counts.SkippedLag += s.counts.SkippedLag
		r.WrongChoice += s.wrongChoice
		r.DoubleAccept += s.duplicateAccepted
		r.UnexpectedDuplicate += s.unexpectedPrimaryDuplicate
		for j, b := range s.buckets {
			r.Buckets[j].Dispatched += b.Dispatched
			r.Buckets[j].Accepted += b.Accepted
			r.Buckets[j].Duplicate += b.Duplicate
			r.Buckets[j].Closed += b.Closed
			r.Buckets[j].ActualDispatched += b.ActualDispatched
		}
		schedule.merge(&s.schedule)
		dispatch.merge(&s.dispatch)
		acceptedSchedule.merge(&s.acceptedSchedule)
		acceptedDispatch.merge(&s.acceptedDispatch)
	}
	r.Counts.Planned = c.attempts()
	r.Counts.Skipped = r.Counts.Planned - r.Counts.Dispatched
	r.Counts.CancelledRemaining = r.Counts.Skipped - r.Counts.SkippedLag
	for i := range r.Buckets {
		if i < 60 {
			r.Buckets[i].Skipped = r.Buckets[i].Planned - r.Buckets[i].Dispatched
		}
	}
	r.Latency = latencyPair{schedule.report(), dispatch.report()}
	r.AcceptedLatency = latencyPair{acceptedSchedule.report(), acceptedDispatch.report()}
	return r
}

func runWorker(ctx context.Context, c config, start time.Time, seed [16]byte, bitmap []byte, worker int, s *workerStats, submit submitter) {
	cipher, _ := aes.NewCipher(seed[:])
	buffer := make([]votecore.Input, c.bufferSize)
	var counter [16]byte
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	keys := c.keys()
	sampleMask := c.sampleEvery() - 1
	for base := uint64(worker * c.bufferSize); base < keys; base += uint64(c.workers * c.bufferSize) {
		if ctx.Err() != nil {
			return
		}
		n := min(c.bufferSize, int(keys-base))
		for i := 0; i < n; i++ {
			binary.BigEndian.PutUint64(counter[8:], base+uint64(i)+1)
			cipher.Encrypt(buffer[i].Token[:], counter[:])
			buffer[i].Choice = 1
		}
		for first := 0; first < n; first += c.groupSize() {
			last := min(n, first+c.groupSize())
			if !waitUntil(ctx, timer, start.Add(scheduledOffset(base+uint64(last-1), c.rate))) {
				return
			}
			for i := first; i < last; i++ {
				key := base + uint64(i)
				at := start.Add(scheduledOffset(key, c.rate))
				sample := uint64(binary.BigEndian.Uint16(buffer[i].Token[14:]))&sampleMask == 0
				attempt := func(input votecore.Input, repeat bool) {
					before := time.Now()
					if before.Sub(at) > c.maxLag {
						s.counts.SkippedLag++
						return
					}
					if ctx.Err() != nil {
						return
					}
					s.counts.Dispatched++
					scheduledSecond := int(key / uint64(c.rate))
					s.buckets[scheduledSecond].Dispatched++
					actualSecond := min(61, max(0, int(before.Sub(start)/time.Second)))
					s.buckets[actualSecond].ActualDispatched++
					result := submit(input)
					var complete time.Time
					if sample {
						complete = time.Now()
						s.schedule.add(uint64(max(0, complete.Sub(at))))
						s.dispatch.add(uint64(max(0, complete.Sub(before))))
					}
					expected := getChoice(bitmap, key)
					switch result.Status {
					case votecore.Accepted:
						s.counts.Accepted++
						s.buckets[scheduledSecond].Accepted++
						if expected != 0 {
							s.duplicateAccepted++
						}
						setChoice(bitmap, key, input.Choice)
						if result.Choice != input.Choice {
							s.wrongChoice++
						}
						if sample {
							s.acceptedSchedule.add(uint64(max(0, complete.Sub(at))))
							s.acceptedDispatch.add(uint64(max(0, complete.Sub(before))))
						}
					case votecore.Duplicate:
						s.counts.Duplicate++
						s.buckets[scheduledSecond].Duplicate++
						if !repeat || expected == 0 {
							s.unexpectedPrimaryDuplicate++
						}
						if result.Choice != expected {
							s.wrongChoice++
						}
					case votecore.Closed:
						s.counts.Closed++
						s.buckets[scheduledSecond].Closed++
					case votecore.NotOpen:
						s.counts.NotOpen++
					case votecore.Invalid:
						s.counts.Invalid++
					case votecore.CapacityExceeded:
						s.counts.Capacity++
					default:
						s.counts.Invalid++
					}
				}
				attempt(buffer[i], false)
				if c.repeatEvery > 0 && (key+1)%uint64(c.repeatEvery) == 0 {
					repeat := buffer[i]
					repeat.Choice = 2
					attempt(repeat, true)
				}
			}
		}
	}
}

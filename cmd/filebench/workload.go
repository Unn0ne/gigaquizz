package main

import (
	"context"
	"crypto/aes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/bits"
	"sync"
	"time"

	"gigaquizz/internal/filelog"
)

const (
	stateNever byte = iota
	stateUnknown
	stateACK
	stateRejected
)

type states struct{ Original, Repeat []byte }

func (s states) get(key uint32, choice uint32) byte {
	if choice == 2 {
		return s.Repeat[key]
	}
	return s.Original[key]
}
func (s states) set(key uint32, choice uint32, state byte) {
	if choice == 2 {
		s.Repeat[key] = state
	} else {
		s.Original[key] = state
	}
}

type frame struct {
	Inputs    []filelog.Input
	Keys      []uint32
	First     time.Time
	Partition int32
}
type ackFrame struct {
	Partition  int32
	Count      uint32
	Offset     int64
	AdmittedNS int64
	Digest     [32]byte
}
type counts struct {
	Planned          uint64 `json:"planned_attempts"`
	Dispatched       uint64 `json:"dispatched_attempts"`
	Recorded         uint64 `json:"recorded_attempts"`
	RecordedOriginal uint64 `json:"recorded_original_attempts"`
	RecordedRepeat   uint64 `json:"recorded_repeat_attempts"`
	Unknown          uint64 `json:"unknown"`
	Closed           uint64 `json:"closed"`
	NotOpen          uint64 `json:"not_open"`
	Busy             uint64 `json:"busy"`
	Rejected         uint64 `json:"rejected"`
	Skipped          uint64 `json:"generator_skipped_attempts"`
	LagSkips         uint64 `json:"generator_lag_skips"`
	QueueSkips       uint64 `json:"generator_queue_skips"`
	Cancelled        uint64 `json:"unvisited_or_cancelled"`
	DispatchedFrames uint64 `json:"dispatched_frames"`
	RecordedFrames   uint64 `json:"recorded_frames"`
	InvalidReceipts  uint64 `json:"invalid_successful_receipts"`
	// Each malformed frame increments every applicable reason. These reason
	// counters can therefore sum to more than InvalidReceipts.
	InvalidReceiptPartition uint64 `json:"invalid_receipt_partition"`
	InvalidReceiptCount     uint64 `json:"invalid_receipt_count"`
	InvalidReceiptOffset    uint64 `json:"invalid_receipt_offset"`
	InvalidReceiptWindow    uint64 `json:"invalid_receipt_admission_window"`
}

type receiptViolation uint8

const (
	wrongPartition receiptViolation = 1 << iota
	wrongCount
	wrongOffset
	wrongWindow
)

func receiptViolations(receipt filelog.FrameReceipt, f frame, cfg filelog.Config) receiptViolation {
	var invalid receiptViolation
	if receipt.Partition != f.Partition {
		invalid |= wrongPartition
	}
	if int(receipt.Count) != len(f.Inputs) {
		invalid |= wrongCount
	}
	if receipt.Offset < 0 {
		invalid |= wrongOffset
	}
	// Admission is a persisted wall-clock contract. The generator's config
	// also carries monotonic readings for scheduling; those may drift against
	// wall time and must never change whether a server-admitted receipt is valid.
	admitted := receipt.AdmittedAt.UTC()
	if admitted.Before(cfg.StartsAt.UTC()) || !admitted.Before(cfg.EndsAt.UTC()) {
		invalid |= wrongWindow
	}
	return invalid
}

type bucket struct {
	Second           int    `json:"second"`
	Planned          uint64 `json:"planned_by_schedule"`
	Dispatched       uint64 `json:"dispatched_by_schedule"`
	Recorded         uint64 `json:"recorded_by_schedule"`
	Skipped          uint64 `json:"skipped_by_schedule"`
	ActualDispatched uint64 `json:"dispatched_by_actual_second"`
	ActualRecorded   uint64 `json:"recorded_by_completion_second"`
}
type histogram struct {
	bins  [1024]uint64
	count uint64
	sum   float64
	max   uint64
}

func (h *histogram) add(d time.Duration) {
	n := uint64(max(0, d))
	b := bits.Len64(n)
	index := int(n)
	if b > 4 {
		shift := b - 5
		index = shift*16 + int(n>>shift)
	}
	h.bins[index]++
	h.count++
	h.sum += float64(n)
	h.max = max(h.max, n)
}
func (h *histogram) merge(x histogram) {
	for i, n := range x.bins {
		h.bins[i] += n
	}
	h.count += x.count
	h.sum += x.sum
	h.max = max(h.max, x.max)
}

type latency struct {
	Samples    uint64  `json:"samples"`
	MeanMS     float64 `json:"sample_mean_ms"`
	P95UpperMS float64 `json:"sample_p95_upper_ms"`
	P99UpperMS float64 `json:"sample_p99_upper_ms"`
	MaxMS      float64 `json:"sample_max_ms"`
}

func (h histogram) report() latency {
	r := latency{Samples: h.count}
	if h.count == 0 {
		return r
	}
	r.MeanMS = h.sum / float64(h.count) / 1e6
	r.MaxMS = float64(h.max) / 1e6
	q := func(p uint64) float64 {
		target := (h.count*p + 99) / 100
		var seen uint64
		for i, n := range h.bins {
			seen += n
			if seen >= target {
				upper := uint64(i)
				if i >= 16 {
					shift := (i - 16) / 16
					mant := 16 + (i-16)%16
					upper = (uint64(mant+1) << shift) - 1
				}
				return float64(upper) / 1e6
			}
		}
		return 0
	}
	r.P95UpperMS = q(95)
	r.P99UpperMS = q(99)
	return r
}

type stat struct {
	counts             counts
	buckets            [82]bucket
	schedule, dispatch histogram
	acks               []ackFrame
}
type workloadReport struct {
	StartsAt          time.Time        `json:"starts_at"`
	FinishedAt        time.Time        `json:"finished_at"`
	Counts            counts           `json:"counts"`
	PlannedKeys       uint64           `json:"planned_unique_keys"`
	Buckets           []bucket         `json:"one_second_buckets"`
	ScheduleLatency   latency          `json:"sampled_recorded_latency_from_schedule"`
	DispatchLatency   latency          `json:"sampled_recorded_latency_from_frame_call"`
	LatencyMethod     string           `json:"latency_method"`
	FramePayloadBytes uint64           `json:"recorded_frame_payload_bytes"`
	MeanFrameRecords  float64          `json:"mean_recorded_frame_attempts"`
	Clock             clockObservation `json:"clock_observation"`
}

type clockObservation struct {
	WallSeconds      float64 `json:"wall_elapsed_from_start_seconds"`
	MonotonicSeconds float64 `json:"monotonic_elapsed_from_start_seconds"`
	DifferenceMS     float64 `json:"wall_minus_monotonic_ms"`
}
type sender func(context.Context, []filelog.Input) (filelog.FrameReceipt, error)

func at(start time.Time, key uint32, rate int) time.Time {
	return start.Add(time.Duration(key) * time.Second / time.Duration(rate))
}
func second(start, now time.Time) int { return min(81, max(0, int(now.Sub(start)/time.Second))) }
func waitUntil(ctx context.Context, t *time.Timer, until time.Time) bool {
	for {
		if ctx.Err() != nil {
			return false
		}
		delay := time.Until(until)
		if delay <= 0 {
			return true
		}
		t.Reset(delay)
		select {
		case <-t.C:
		case <-ctx.Done():
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
			return false
		}
	}
}
func digest(inputs []filelog.Input) [32]byte {
	h := sha256.New()
	var encoded [20]byte
	for _, input := range inputs {
		copy(encoded[:16], input.Token[:])
		binary.BigEndian.PutUint32(encoded[16:], input.Choice)
		_, _ = h.Write(encoded[:])
	}
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
}

func runWorkload(parent context.Context, c config, cfg filelog.Config, seed [16]byte, states states, send sender) (workloadReport, []ackFrame) {
	ctx, cancel := context.WithDeadline(parent, cfg.StartsAt.Add(time.Minute+c.drain))
	defer cancel()
	jobs := make(chan frame, c.queue)
	workerStats := make([]stat, c.workers)
	var wg sync.WaitGroup
	for worker := range c.workers {
		wg.Go(func() {
			s := &workerStats[worker]
			for f := range jobs {
				if ctx.Err() != nil {
					continue
				}
				if time.Since(at(cfg.StartsAt, f.Keys[0], c.rate)) > c.maxLag {
					s.counts.LagSkips += uint64(len(f.Inputs))
					continue
				}
				before := time.Now()
				callCtx, callCancel := context.WithTimeout(ctx, c.timeout)
				for i, input := range f.Inputs {
					states.set(f.Keys[i], input.Choice, stateUnknown)
					s.buckets[int(f.Keys[i])/c.rate].Dispatched++
				}
				s.counts.Dispatched += uint64(len(f.Inputs))
				s.counts.DispatchedFrames++
				s.buckets[second(cfg.StartsAt, before)].ActualDispatched += uint64(len(f.Inputs))
				receipt, err := send(callCtx, f.Inputs)
				after := time.Now()
				callCancel()
				state := stateUnknown
				if err == nil {
					if invalid := receiptViolations(receipt, f, cfg); invalid != 0 {
						s.counts.InvalidReceipts++
						if invalid&wrongPartition != 0 {
							s.counts.InvalidReceiptPartition++
						}
						if invalid&wrongCount != 0 {
							s.counts.InvalidReceiptCount++
						}
						if invalid&wrongOffset != 0 {
							s.counts.InvalidReceiptOffset++
						}
						if invalid&wrongWindow != 0 {
							s.counts.InvalidReceiptWindow++
						}
						err = filelog.ErrUnknown
					}
				}
				switch {
				case err == nil:
					state = stateACK
					s.counts.Recorded += uint64(len(f.Inputs))
					s.counts.RecordedFrames++
					s.buckets[second(cfg.StartsAt, after)].ActualRecorded += uint64(len(f.Inputs))
					s.acks = append(s.acks, ackFrame{receipt.Partition, receipt.Count, receipt.Offset, receipt.AdmittedAt.UnixNano(), digest(f.Inputs)})
				case errors.Is(err, filelog.ErrClosed):
					state = stateRejected
					s.counts.Closed += uint64(len(f.Inputs))
				case errors.Is(err, filelog.ErrNotOpen):
					state = stateRejected
					s.counts.NotOpen += uint64(len(f.Inputs))
				case errors.Is(err, filelog.ErrBusy):
					state = stateRejected
					s.counts.Busy += uint64(len(f.Inputs))
				case errors.Is(err, filelog.ErrInvalid):
					state = stateRejected
					s.counts.Rejected += uint64(len(f.Inputs))
				default:
					s.counts.Unknown += uint64(len(f.Inputs))
				}
				for i, input := range f.Inputs {
					states.set(f.Keys[i], input.Choice, state)
					if state == stateACK {
						s.buckets[int(f.Keys[i])/c.rate].Recorded++
						if input.Choice == 1 {
							s.counts.RecordedOriginal++
						} else {
							s.counts.RecordedRepeat++
						}
						if c.attempts() <= 10000 || binary.BigEndian.Uint16(input.Token[14:])&1023 == 0 {
							s.schedule.add(after.Sub(at(cfg.StartsAt, f.Keys[i], c.rate)))
							s.dispatch.add(after.Sub(before))
						}
					}
				}
			}
		})
	}
	gen := generate(ctx, c, cfg, seed, jobs)
	close(jobs)
	wg.Wait()
	finished := time.Now()
	wallElapsed := finished.UTC().Sub(cfg.StartsAt.UTC())
	monotonicElapsed := finished.Sub(cfg.StartsAt)
	r := workloadReport{StartsAt: cfg.StartsAt.UTC(), FinishedAt: finished.UTC(), PlannedKeys: c.keys(), Buckets: make([]bucket, 82), LatencyMethod: "Record ACK latency is sampled independently of outcome by low10 AES-token bits (all attempts if planned<=10000); both repeat choices share selection. Histogram quantiles are upper edges with <6.25% rounding. Skipped/unknown/rejected attempts do not enter ACK latency samples. Schedule, buckets and latency use monotonic time; admission validation uses the persisted wall-clock poll window. Clock observation is taken after workload/drain before ledger, Seal and replay. Frame dispatch includes direct filelog.Store.SubmitFrame with local disk synchronization, not external HTTP/TLS.", Clock: clockObservation{WallSeconds: wallElapsed.Seconds(), MonotonicSeconds: monotonicElapsed.Seconds(), DifferenceMS: float64(wallElapsed-monotonicElapsed) / 1e6}}
	all := append(workerStats, gen)
	var schedule, dispatch histogram
	var receipts []ackFrame
	for _, s := range all {
		r.Counts.Dispatched += s.counts.Dispatched
		r.Counts.Recorded += s.counts.Recorded
		r.Counts.RecordedOriginal += s.counts.RecordedOriginal
		r.Counts.RecordedRepeat += s.counts.RecordedRepeat
		r.Counts.Unknown += s.counts.Unknown
		r.Counts.Closed += s.counts.Closed
		r.Counts.NotOpen += s.counts.NotOpen
		r.Counts.Busy += s.counts.Busy
		r.Counts.Rejected += s.counts.Rejected
		r.Counts.LagSkips += s.counts.LagSkips
		r.Counts.QueueSkips += s.counts.QueueSkips
		r.Counts.DispatchedFrames += s.counts.DispatchedFrames
		r.Counts.RecordedFrames += s.counts.RecordedFrames
		for i, b := range s.buckets {
			r.Buckets[i].Dispatched += b.Dispatched
			r.Buckets[i].Recorded += b.Recorded
			r.Buckets[i].ActualDispatched += b.ActualDispatched
			r.Buckets[i].ActualRecorded += b.ActualRecorded
		}
		schedule.merge(s.schedule)
		dispatch.merge(s.dispatch)
		receipts = append(receipts, s.acks...)
	}
	for _, s := range all {
		r.Counts.InvalidReceipts += s.counts.InvalidReceipts
		r.Counts.InvalidReceiptPartition += s.counts.InvalidReceiptPartition
		r.Counts.InvalidReceiptCount += s.counts.InvalidReceiptCount
		r.Counts.InvalidReceiptOffset += s.counts.InvalidReceiptOffset
		r.Counts.InvalidReceiptWindow += s.counts.InvalidReceiptWindow
	}
	r.Counts.Planned = c.attempts()
	r.Counts.Skipped = r.Counts.Planned - r.Counts.Dispatched
	r.Counts.Cancelled = r.Counts.Skipped - r.Counts.LagSkips - r.Counts.QueueSkips
	for i := range r.Buckets {
		r.Buckets[i].Second = i
		if i < 60 {
			lo := uint64(i * c.rate)
			hi := lo + uint64(c.rate)
			n := hi - lo
			if c.repeatEvery > 0 {
				n += hi/uint64(c.repeatEvery) - lo/uint64(c.repeatEvery)
			}
			r.Buckets[i].Planned = n
			r.Buckets[i].Skipped = n - r.Buckets[i].Dispatched
		}
	}
	r.ScheduleLatency = schedule.report()
	r.DispatchLatency = dispatch.report()
	r.FramePayloadBytes = r.Counts.Recorded*28 + r.Counts.RecordedFrames*40
	if r.Counts.RecordedFrames > 0 {
		r.MeanFrameRecords = float64(r.Counts.Recorded) / float64(r.Counts.RecordedFrames)
	}
	return r, receipts
}

func generate(ctx context.Context, c config, cfg filelog.Config, seed [16]byte, jobs chan<- frame) stat {
	var s stat
	cipher, _ := aes.NewCipher(seed[:])
	var counter [16]byte
	var inputBuffer [256]filelog.Input
	buffers := make([]frame, c.partitions)
	for p := range buffers {
		buffers[p].Partition = int32(p)
	}
	newBuffer := func(p int) {
		buffers[p] = frame{Inputs: make([]filelog.Input, 0, c.frameSize), Keys: make([]uint32, 0, c.frameSize), Partition: int32(p)}
	}
	flush := func(p int) {
		f := buffers[p]
		if len(f.Inputs) == 0 {
			return
		}
		select {
		case jobs <- f:
		default:
			s.counts.QueueSkips += uint64(len(f.Inputs))
		}
		buffers[p] = frame{Partition: int32(p)}
	}
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	// Small rates must not wait for a full256-input block: groups span <=1ms.
	groupSize := max(1, min(256, c.rate/1000))
	for base := uint64(0); base < c.keys(); base += uint64(groupSize) {
		if ctx.Err() != nil {
			break
		}
		n := min(groupSize, int(c.keys()-base))
		for i := 0; i < n; i++ {
			binary.BigEndian.PutUint64(counter[8:], base+uint64(i)+1)
			cipher.Encrypt(inputBuffer[i].Token[:], counter[:])
			inputBuffer[i].Choice = 1
		}
		if !waitUntil(ctx, timer, at(cfg.StartsAt, uint32(base+uint64(n-1)), c.rate)) {
			break
		}
		now := time.Now()
		for p, f := range buffers {
			if len(f.Inputs) > 0 && now.Sub(f.First) >= c.frameAge {
				flush(p)
			}
		}
		for i := 0; i < n; i++ {
			key := uint32(base + uint64(i))
			repeated := c.repeatEvery > 0 && (uint64(key)+1)%uint64(c.repeatEvery) == 0
			if now.Sub(at(cfg.StartsAt, key, c.rate)) > c.maxLag {
				s.counts.LagSkips++
				if repeated {
					s.counts.LagSkips++
				}
				continue
			}
			p := int(cfg.Partition(inputBuffer[i].Token))
			appendInput := func(input filelog.Input) {
				if len(buffers[p].Inputs) == 0 {
					newBuffer(p)
					buffers[p].First = now
				}
				buffers[p].Inputs = append(buffers[p].Inputs, input)
				buffers[p].Keys = append(buffers[p].Keys, key)
				if len(buffers[p].Inputs) == c.frameSize {
					flush(p)
				}
			}
			appendInput(inputBuffer[i])
			if repeated {
				repeat := inputBuffer[i]
				repeat.Choice = 2
				appendInput(repeat)
			}
		}
	}
	for p := range buffers {
		flush(p)
	}
	return s
}

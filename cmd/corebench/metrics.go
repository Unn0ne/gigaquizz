package main

import (
	"math/bits"
)

// Sixteen sub-buckets per power of two. Quantiles report the upper edge:
// rounding is exact below 32ns and less than 6.25% above that range.
type histogram struct {
	bins    [1024]uint64
	count   uint64
	sum     float64
	maximum uint64
}

func (h *histogram) add(ns uint64) {
	h.bins[histogramIndex(ns)]++
	h.count++
	h.sum += float64(ns)
	h.maximum = max(h.maximum, ns)
}
func (h *histogram) merge(other *histogram) {
	for i, n := range other.bins {
		h.bins[i] += n
	}
	h.count += other.count
	h.sum += other.sum
	h.maximum = max(h.maximum, other.maximum)
}
func histogramIndex(ns uint64) int {
	b := bits.Len64(ns)
	if b <= 4 {
		return int(ns)
	}
	shift := b - 5
	return shift*16 + int(ns>>shift)
}
func histogramUpper(index int) uint64 {
	if index < 16 {
		return uint64(index)
	}
	shift := (index - 16) / 16
	mantissa := 16 + (index-16)%16
	return (uint64(mantissa+1) << shift) - 1
}

type latencyReport struct {
	Samples    uint64  `json:"samples"`
	MeanUS     float64 `json:"sample_mean_us"`
	P95UpperUS float64 `json:"sample_p95_upper_us"`
	P99UpperUS float64 `json:"sample_p99_upper_us"`
	MaxUS      float64 `json:"sample_max_us"`
}

func (h *histogram) report() latencyReport {
	r := latencyReport{Samples: h.count}
	if h.count == 0 {
		return r
	}
	r.MeanUS = h.sum / float64(h.count) / 1000
	r.MaxUS = float64(h.maximum) / 1000
	percentile := func(percent uint64) float64 {
		target := (h.count*percent + 99) / 100
		var seen uint64
		for i, n := range h.bins {
			seen += n
			if seen >= target {
				return float64(histogramUpper(i)) / 1000
			}
		}
		return 0
	}
	r.P95UpperUS = percentile(95)
	r.P99UpperUS = percentile(99)
	return r
}

type latencyPair struct {
	FromSchedule latencyReport `json:"from_original_schedule"`
	FromDispatch latencyReport `json:"from_submit_call"`
}
type counts struct {
	Planned            uint64 `json:"planned_attempts"`
	Dispatched         uint64 `json:"dispatched_attempts"`
	Accepted           uint64 `json:"accepted_unique_volatile"`
	Duplicate          uint64 `json:"duplicate_attempts"`
	Closed             uint64 `json:"closed"`
	NotOpen            uint64 `json:"not_open"`
	Invalid            uint64 `json:"invalid"`
	Capacity           uint64 `json:"capacity_exceeded"`
	Skipped            uint64 `json:"generator_skipped_attempts"`
	SkippedLag         uint64 `json:"generator_lag_skips"`
	CancelledRemaining uint64 `json:"generator_unvisited_or_cancelled"`
}
type secondBucket struct {
	Second           int    `json:"second"`
	Planned          uint64 `json:"planned_by_original_schedule"`
	Dispatched       uint64 `json:"dispatched_by_original_schedule"`
	Skipped          uint64 `json:"skipped_by_original_schedule"`
	Accepted         uint64 `json:"accepted_by_original_schedule"`
	Duplicate        uint64 `json:"duplicate_by_original_schedule"`
	Closed           uint64 `json:"closed_by_original_schedule"`
	ActualDispatched uint64 `json:"dispatched_by_actual_second"`
}

// Chunks have a multiple-of-four size and are owned by one worker, so writes
// to the two-bit expected-choice bitmap never share a byte across workers.
func setChoice(bitmap []byte, key uint64, choice uint32) {
	shift := (key % 4) * 2
	bitmap[key/4] = (bitmap[key/4] &^ (byte(3) << shift)) | byte(choice)<<shift
}
func getChoice(bitmap []byte, key uint64) uint32 {
	return uint32((bitmap[key/4] >> ((key % 4) * 2)) & 3)
}

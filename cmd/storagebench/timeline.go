package main

import "time"

// The schedule is limited to 55 seconds plus at most 15 seconds of draining.
// Keep an explicit overflow bucket if the process itself is paused longer.
const timelineSeconds = 128

type secondBucket struct {
	Second             int            `json:"start_second"`
	Overflow           bool           `json:"includes_later_seconds"`
	LogicalScheduled   int            `json:"logical_scheduled"`
	AttemptsScheduled  int            `json:"attempts_scheduled"`
	Sent               int            `json:"attempts_sent"`
	Accepted           int            `json:"accepted_responses"`
	Unknown            int            `json:"unknown_outcomes"`
	FirstConfirmations int            `json:"first_client_confirmations"`
	Skipped            map[string]int `json:"skipped_by_reason"`
}

func secondIndex(elapsed time.Duration) int {
	return min(timelineSeconds-1, max(0, int(elapsed/time.Second)))
}

// Caller holds m.mu while workers are active.
func (m *observations) bucket(elapsed time.Duration) *secondBucket {
	i := secondIndex(elapsed)
	m.lastSecond = max(m.lastSecond, i)
	b := &m.seconds[i]
	b.Second = i
	b.Overflow = i == timelineSeconds-1
	return b
}

func eachScheduleSecond(c config, begin, end int, visit func(time.Duration, int, int)) {
	for begin < end {
		second := begin / c.rate
		until := min(end, (second+1)*c.rate)
		visit(time.Duration(second)*time.Second, until-begin, attemptsInRange(begin, until, c.repeatEvery))
		begin = until
	}
}

func (m *observations) scheduledRange(c config, begin, end int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.scheduled += end - begin
	eachScheduleSecond(c, begin, end, func(second time.Duration, logical, attempts int) {
		b := m.bucket(second)
		b.LogicalScheduled += logical
		b.AttemptsScheduled += attempts
	})
}

func (m *observations) skipRange(c config, begin, end int, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.skips[reason] += attemptsInRange(begin, end, c.repeatEvery)
	eachScheduleSecond(c, begin, end, func(second time.Duration, _ int, attempts int) {
		b := m.bucket(second)
		if b.Skipped == nil {
			b.Skipped = make(map[string]int)
		}
		b.Skipped[reason] += attempts
	})
}

type confirmationThresholds struct {
	Confirmed           int `json:"logical_keys_with_first_confirmation"`
	WithoutConfirmation int `json:"planned_keys_without_confirmation"`
	After100MS          int `json:"after_100_ms"`
	After250MS          int `json:"after_250_ms"`
	After500MS          int `json:"after_500_ms"`
	After1000MS         int `json:"after_1000_ms"`
}

func thresholds(values []time.Duration, planned int) confirmationThresholds {
	t := confirmationThresholds{Confirmed: len(values), WithoutConfirmation: planned - len(values)}
	for _, d := range values {
		if d > 100*time.Millisecond {
			t.After100MS++
		}
		if d > 250*time.Millisecond {
			t.After250MS++
		}
		if d > 500*time.Millisecond {
			t.After500MS++
		}
		if d > time.Second {
			t.After1000MS++
		}
	}
	return t
}

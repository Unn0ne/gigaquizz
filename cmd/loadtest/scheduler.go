package main

import (
	"context"
	"time"
)

type job struct {
	sequence  int64
	scheduled time.Time
}

// slotsBefore counts timestamps i/rate strictly before elapsed. Splitting the
// seconds and remainder avoids overflowing duration*rate in large runs.
func slotsBefore(elapsed time.Duration, rate int64) int64 {
	if elapsed <= 0 {
		return 0
	}
	return int64(elapsed/time.Second)*rate + (int64(elapsed%time.Second)*rate+int64(time.Second)-1)/int64(time.Second)
}

func slotOffset(sequence, rate int64) time.Duration {
	return time.Duration(sequence/rate)*time.Second + time.Duration(sequence%rate)*time.Second/time.Duration(rate)
}

func schedule(ctx context.Context, start time.Time, c config, jobs chan<- job, m *metrics) {
	total := slotsBefore(c.duration, c.rate)
	timer := time.NewTimer(0)
	defer timer.Stop()
	<-timer.C
	for next := int64(0); next < total; {
		if ctx.Err() != nil {
			return
		}
		now := time.Now()
		// Discard missed slots in O(1), rather than flooding a recovering
		// generator with a catch-up burst or iterating over millions of misses.
		obsolete := min(total, slotsBefore(now.Sub(start)-c.maxLag, c.rate))
		if obsolete > next {
			m.scheduledSlots(obsolete - next)
			m.skip("scheduler_lag", obsolete-next)
			next = obsolete
			continue
		}
		j := job{sequence: next, scheduled: start.Add(slotOffset(next, c.rate))}
		if delay := j.scheduled.Sub(now); delay > 0 {
			timer.Reset(delay)
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			continue
		}
		m.scheduledSlots(1)
		select {
		case jobs <- j:
			m.enqueuedSlot()
		default:
			m.skip("queue_full", 1)
		}
		next++
	}
}

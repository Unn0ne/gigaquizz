package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"gigaquizz/internal/poll"
)

func TestTimelineUsesOriginalSlotsAndActualCompletionOrder(t *testing.T) {
	c := defaults()
	c.total, c.rate, c.repeatEvery = 2, 1, 1
	m, err := newObservations(c.total)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(1000, 0)
	m.scheduledRange(c, 0, 2)
	j := attemptJob{index: 0, choice: 1, scheduled: start}
	firstBegin, firstEnd := start.Add(10*time.Millisecond), start.Add(100*time.Millisecond)
	laterBegin, laterEnd := start.Add(950*time.Millisecond), start.Add(1250*time.Millisecond)
	m.markStart(j, start, c.window(), firstBegin)
	m.markStart(j, start, c.window(), laterBegin)
	// Metric-lock acquisition can invert the order of concurrent completions.
	m.finish(j, poll.Receipt{Status: "duplicate", Choices: []int{1}, AcceptedAt: &start}, nil, laterBegin, laterEnd, start)
	m.finish(j, poll.Receipt{Status: "accepted", Choices: []int{1}, AcceptedAt: &start}, nil, firstBegin, firstEnd, start)
	j = attemptJob{index: 1, choice: 1, scheduled: start.Add(time.Second)}
	unknownBegin, unknownEnd := start.Add(1050*time.Millisecond), start.Add(2010*time.Millisecond)
	m.markStart(j, start, c.window(), unknownBegin)
	m.finish(j, poll.Receipt{}, context.DeadlineExceeded, unknownBegin, unknownEnd, start)
	m.skip("queue_full", 1, time.Second)
	for range 2 {
		r := m.report(c, 3*time.Second, false)
		if len(r.Seconds) != 3 || r.AttemptSent != 3 || r.AttemptSkipped != 1 || r.AttemptNotScheduled != 0 {
			t.Fatal("timeline attempt accounting does not balance")
		}
		if r.Seconds[0].LogicalScheduled != 1 || r.Seconds[0].AttemptsScheduled != 2 || r.Seconds[1].LogicalScheduled != 1 || r.Seconds[1].AttemptsScheduled != 2 {
			t.Fatal("scheduled attempts do not use original slots")
		}
		if r.Seconds[0].Sent != 2 || r.Seconds[1].Sent != 1 || r.Seconds[0].Accepted != 1 || r.Seconds[2].Unknown != 1 || r.Seconds[1].Skipped["queue_full"] != 1 {
			t.Fatal("timeline events use the wrong time boundary")
		}
		if r.Confirmation.Count != 1 || r.Confirmation.MeanMS != 100 || r.Seconds[0].FirstConfirmations != 1 || r.Seconds[1].FirstConfirmations != 0 {
			t.Fatal("first confirmation follows lock order or report mutated counters")
		}
		if r.ConfirmationThresholds.After100MS != 0 || r.ConfirmationThresholds.WithoutConfirmation != 1 {
			t.Fatal("threshold counts include exact boundary or omit unconfirmed keys")
		}
	}
}

func TestConfirmationThresholdsAreCumulativeAndStrict(t *testing.T) {
	var values []time.Duration
	for _, ms := range []int{100, 101, 250, 251, 500, 501, 1000, 1001} {
		values = append(values, time.Duration(ms)*time.Millisecond)
	}
	r := thresholds(values, 10)
	if r.Confirmed != 8 || r.WithoutConfirmation != 2 || r.After100MS != 7 || r.After250MS != 5 || r.After500MS != 3 || r.After1000MS != 1 {
		t.Fatalf("incorrect deadline counts: %+v", r)
	}
}

func TestTimelineBulkSkipsAndBoundedOverflow(t *testing.T) {
	c := defaults()
	c.total, c.rate, c.repeatEvery = 6, 2, 2
	m, err := newObservations(c.total)
	if err != nil {
		t.Fatal(err)
	}
	m.scheduledRange(c, 0, c.total)
	m.skipRange(c, 0, c.total, "scheduler_lag")
	for i := range 3 {
		b := m.seconds[i]
		if b.LogicalScheduled != 2 || b.AttemptsScheduled != 3 || b.Skipped["scheduler_lag"] != 3 {
			t.Fatal("bulk skipped repeats do not match the original schedule second")
		}
	}
	m.skip("cancelled", 1, 130*time.Second)
	r := m.report(c, 131*time.Second, true)
	if len(r.Seconds) != timelineSeconds || !r.Seconds[timelineSeconds-1].Overflow || r.Seconds[timelineSeconds-1].Skipped["cancelled"] != 1 {
		t.Fatal("long process pause escaped bounded timeline storage")
	}
}

func TestLedgerReaderBoundSupportsMaximumKeys(t *testing.T) {
	key := keyState{Token: "00112233445566778899aabbccddeeff", SentChoices: 3, ConfirmedChoices: 3, ReceiptChoices: 3, Accepted: 2, Unknown: false, firstConfirmation: time.Second, hasConfirmation: true}
	encoded, err := json.Marshal(key)
	if err != nil {
		t.Fatal(err)
	}
	// One comma per key and ample fixed metadata margin, without allocating
	// a half-million-key ledger merely to check its maximum encoded size.
	if maximumKeys*(len(encoded)+1)+1024*1024 > maximumLedgerBytes {
		t.Fatal("a valid maximum-size ledger cannot be read back")
	}
	var decoded keyState
	if err := json.Unmarshal(encoded, &decoded); err != nil || decoded.hasConfirmation || decoded.firstConfirmation != 0 {
		t.Fatal("transient timing state leaked into the private ledger")
	}
}

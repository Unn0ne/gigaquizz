package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gigaquizz/internal/votelog"
)

func TestBoundsCountRepeatsAndFullMinute(t *testing.T) {
	c := defaults()
	c.rate = 200000
	c.allowLarge = true
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
	if c.keys() != 12000000 || c.attempts() != 12000000 {
		t.Fatal("full-minute bound is wrong")
	}
	c.repeatEvery = 5
	if err := c.validate(); err == nil {
		t.Fatal("repeats escaped total cap")
	}
	c.rate = 834
	c.duration = time.Second
	c.allowLarge = false
	if err := c.validate(); err == nil {
		t.Fatal("repeats escaped 1000/s opt-in gate")
	}
	c.rate = 10
	c.duration = 150 * time.Millisecond
	c.repeatEvery = 0
	if c.keys() != 2 {
		t.Fatal("fractional arrival window must include zero and 100ms slots")
	}
	for _, raw := range []string{"localhost:19092", "192.0.2.1:19092", "127.0.0.1:0", "http://127.0.0.1:19092"} {
		if _, err := localBrokers(raw); err == nil {
			t.Fatalf("accepted nonlocal/invalid broker %s", raw)
		}
	}
}

func TestMaximumRateAndAttemptBoundsApplyToBothModes(t *testing.T) {
	for _, mode := range []string{"http", "direct"} {
		t.Run(mode, func(t *testing.T) {
			c := defaults()
			c.mode, c.allowLarge = mode, true
			c.rate = 200000
			if err := c.validate(); err != nil || c.attempts() != 12000000 {
				t.Fatalf("full-minute maximum rejected: attempts=%d, err=%v", c.attempts(), err)
			}
			c.rate, c.duration = 200001, time.Second
			if err := c.validate(); err == nil {
				t.Fatal("rate above 200000 accepted despite otherwise bounded total")
			}
			c.rate, c.duration, c.repeatEvery = 200000, 50*time.Second, 5
			if err := c.validate(); err != nil || c.attempts() != 12000000 {
				t.Fatalf("12000000 attempts including repeats rejected: attempts=%d, err=%v", c.attempts(), err)
			}
			c.duration += 5 * time.Microsecond
			if c.attempts() != 12000001 {
				t.Fatalf("unexpected boundary count including repeats: %d", c.attempts())
			}
			if err := c.validate(); err == nil {
				t.Fatal("one attempt beyond 12000000 cap was accepted")
			}
			c.duration, c.repeatEvery, c.allowLarge = time.Minute, 0, false
			if err := c.validate(); err == nil {
				t.Fatal("higher ceiling bypassed allow-large opt-in")
			}
		})
	}
}

func TestPrivateLedgerRateBoundMatchesWorkloadBound(t *testing.T) {
	h, entries, _ := fixture()
	h.Rate = 200000
	path, err := writeLedger(t.TempDir(), h, entries)
	if err != nil {
		t.Fatal(err)
	}
	if restored, _, err := readLedger(path); err != nil || restored.Rate != 200000 {
		t.Fatalf("maximum-rate ledger cannot be audited: %v", err)
	}
	h.Rate = 200001
	path, err = writeLedger(t.TempDir(), h, entries)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := readLedger(path); err == nil {
		t.Fatal("audit accepted ledger with a rate above the hard workload bound")
	}
}

func TestSyntheticRepeatsShareFullKeyButHaveIndependentAttempts(t *testing.T) {
	c := defaults()
	c.rate = 5
	c.duration = time.Second
	c.repeatEvery = 2
	c.different = true
	es := makeEntries(c, [8]byte{1})
	if len(es) != 7 {
		t.Fatal(len(es))
	}
	if es[1].Token != es[2].Token || es[1].Choice != 1 || es[2].Choice != 2 || es[1].Key != es[2].Key {
		t.Fatal("repeat not a separate conflicting attempt of the same key")
	}
	if es[0].Token == es[1].Token {
		t.Fatal("different synthetic keys collided")
	}
}

func TestWorkerConcurrencyRemainsBoundedAt8192(t *testing.T) {
	c := defaults()
	c.workers = 8192
	if err := c.validate(); err != nil {
		t.Fatalf("8192 workers should be available for the bounded local comparison: %v", err)
	}
	for _, workers := range []int{0, 8193} {
		c.workers = workers
		if err := c.validate(); err == nil {
			t.Fatalf("accepted workers outside 1..8192: %d", workers)
		}
	}
}

func TestFaultWaitingBoundsPreserveDefaultsAndAdmissionWindow(t *testing.T) {
	c := defaults()
	if c.transactionTimeout != 10*time.Second || c.maxLag != time.Second || c.duration != time.Minute {
		t.Fatal("fault-test options changed default timing")
	}
	c.transactionTimeout, c.timeout, c.maxLag = 30*time.Second, 15*time.Second, 10*time.Second
	if err := c.validate(); err != nil {
		t.Fatalf("bounded long-wait profile rejected: %v", err)
	}
	for _, timeout := range []time.Duration{0, time.Second - 1, 30*time.Second + 1} {
		c.transactionTimeout = timeout
		if err := c.validate(); err == nil {
			t.Fatalf("invalid transaction timeout accepted: %s", timeout)
		}
	}
	c.transactionTimeout = time.Second
	if err := c.validate(); err != nil {
		t.Fatalf("minimum transaction timeout rejected: %v", err)
	}
	for _, lag := range []time.Duration{0, 10*time.Second + 1} {
		c.maxLag = lag
		if err := c.validate(); err == nil {
			t.Fatalf("invalid generator lag accepted: %s", lag)
		}
	}
}

func TestSummaryKeepsAdmissionAndCompletionAcrossMinuteBoundary(t *testing.T) {
	c := defaults()
	c.rate = 1000
	c.allowLarge = true
	start := time.Unix(1700000000, 0)
	es := []entry{
		{Key: 59999, Outcome: recorded, DispatchNS: start.Add(59999 * time.Millisecond).UnixNano(), AdmittedNS: start.Add(59999 * time.Millisecond).UnixNano(), CompleteNS: start.Add(60200 * time.Millisecond).UnixNano()},
		{Key: 59998, Outcome: skipQueue},
	}
	r := summarize(c, start, start.Add(61*time.Second), es)
	if r.Counts.Recorded != 1 || r.Counts.Skipped != 1 || r.LateCommit != 1 || r.AfterPollDeadline != 0 {
		t.Fatalf("wrong boundary accounting: %+v", r.Counts)
	}
	if r.Buckets[59].Scheduled != 2 || r.Buckets[59].Skipped != 1 || r.Buckets[60].Recorded != 1 {
		t.Fatal("buckets moved scheduled slots to completion time")
	}
	if r.Latency.FromSchedule.P99MS != 201 || r.Latency.FromDispatch.Samples != 1 {
		t.Fatal("latencies included skipped slot or dropped late completion")
	}
}

func TestExactLatencyNearestRankAndErrorPopulation(t *testing.T) {
	x := make([]int64, 100)
	for i := range x {
		x[i] = int64(99-i) * 1e6
	}
	r := latencies(x)
	if r.P95MS != 94 || r.P99MS != 98 || r.MeanMS != 49.5 {
		t.Fatalf("wrong exact percentile %+v", r)
	}
	start := time.Unix(1700000000, 0)
	es := []entry{{Key: 0, Outcome: unknown, DispatchNS: start.UnixNano(), CompleteNS: start.Add(time.Second).UnixNano()}}
	if sampleLatencies(start, 1, es, false).FromDispatch.Samples != 1 || sampleLatencies(start, 1, es, true).FromDispatch.Samples != 0 {
		t.Fatal("unknown population mixed into recorded latency")
	}
}

func TestCancelledSchedulerAccountsForEveryUnsentSlot(t *testing.T) {
	c := defaults()
	c.rate = 20
	c.duration = time.Second
	c.workers = 2
	c.queue = 1
	c.repeatEvery = 2
	es := makeEntries(c, [8]byte{3})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := runWorkload(ctx, c, time.Now(), es, func(context.Context, [16]byte, uint32) attemptResult {
		t.Error("cancelled job dispatched")
		return attemptResult{Outcome: recorded}
	})
	if r.Counts.Planned != 30 || r.Counts.Skipped != 30 || r.Counts.Dispatched != 0 {
		t.Fatalf("lost scheduler slot: %+v", r.Counts)
	}
}

func fixture() (ledgerHeader, []entry, votelog.AuditResult) {
	start := time.Unix(1700000000, 0).UTC()
	var token [16]byte
	token[0] = 1
	cfg := votelog.Config{Brokers: []string{"127.0.0.1:19092"}, Topic: "gigaquizz_logbench_0123456789abcdef", PollID: [16]byte{1}, StartsAt: start, EndsAt: start.Add(time.Minute), Partitions: 1}
	h := ledgerHeader{Version: 1, Config: cfg, Entries: 2, Keys: 1, Rate: 1, Mode: "http"}
	first := votelog.Vote{Token: token, Choice: 2, AdmittedAt: start.Add(time.Millisecond)}
	second := votelog.Vote{Token: token, Choice: 1, AdmittedAt: start.Add(2 * time.Millisecond)}
	es := []entry{{Token: token, Key: 0, Choice: 1, Outcome: recorded, Partition: 0, Offset: 5, AdmittedNS: second.AdmittedAt.UnixNano(), DispatchNS: start.UnixNano(), CompleteNS: start.Add(10 * time.Millisecond).UnixNano()}, {Token: token, Key: 0, Choice: 2, Outcome: unknown, DispatchNS: start.UnixNano(), CompleteNS: start.Add(10 * time.Millisecond).UnixNano()}}
	a := votelog.AuditResult{Manifest: votelog.Manifest{Partitions: []votelog.PartitionEnd{{Partition: 0, Offset: 6}}}, Records: map[votelog.Position]votelog.Vote{{Partition: 0, Offset: 4}: first, {Partition: 0, Offset: 5}: second}, Canonical: map[[16]byte]votelog.Vote{token: first}, TotalAttempts: 2, ChoiceCounts: [32]uint64{0, 1}}
	return h, es, a
}

func TestReconcileUnknownCommitAndConflictingCanonicalFirstOffset(t *testing.T) {
	h, es, a := fixture()
	r := reconcile(h, es, a)
	if !r.Correct || r.ConfirmedReceipts != 1 || r.UnknownKeysPresent != 1 || r.Duplicates != 1 || r.ChoiceCounts[1] != 1 {
		t.Fatalf("valid unknown/conflict incorrectly reconciled: %+v", r)
	}
	delete(a.Records, votelog.Position{Partition: 0, Offset: 5})
	a.TotalAttempts--
	r = reconcile(h, es, a)
	if r.Correct || r.MissingReceipts != 1 {
		t.Fatal("lost acknowledged offset was not detected")
	}
}

func TestReconcileDetectsWrongChoiceAndCanonical(t *testing.T) {
	h, es, a := fixture()
	v := a.Records[votelog.Position{Partition: 0, Offset: 5}]
	v.Choice = 2
	a.Records[votelog.Position{Partition: 0, Offset: 5}] = v
	if r := reconcile(h, es, a); r.Correct || r.WrongReceipts != 1 {
		t.Fatal("receipt choice mismatch was accepted")
	}
	h, es, a = fixture()
	a.Canonical[es[0].Token] = a.Records[votelog.Position{Partition: 0, Offset: 5}]
	if r := reconcile(h, es, a); r.Correct || r.CanonicalMismatches == 0 {
		t.Fatal("later conflicting choice became canonical")
	}
}

func TestPrivateLedgerIntegrityPermissionsAndBounds(t *testing.T) {
	h, es, _ := fixture()
	dir := t.TempDir()
	path, err := writeLedger(dir, h, es)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0600 {
		t.Fatal("ledger is not private")
	}
	got, restored, err := readLedger(path)
	if err != nil || got.Config.Topic != h.Config.Topic || len(restored) != 2 || restored[0] != es[0] || restored[1] != es[1] {
		t.Fatalf("ledger round trip failed: %v", err)
	}
	if _, err := writeLedger(dir, h, es); err == nil {
		t.Fatal("overwrote prior ledger")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 1
	corrupt := filepath.Join(dir, "corrupt.ledger")
	if err := os.WriteFile(corrupt, b, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readLedger(corrupt); err == nil {
		t.Fatal("corrupted ledger passed checksum")
	}
	if err := os.WriteFile(corrupt, b[:len(b)-1], 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readLedger(corrupt); err == nil {
		t.Fatal("truncated ledger accepted")
	}
}

func TestRecoveryRequiresExclusiveModeAndStoppedRecordedOwner(t *testing.T) {
	c := defaults()
	c.auditOnly, c.recoverAndSeal = "receipt.ledger", "receipt.ledger"
	if err := c.validate(); err == nil {
		t.Fatal("read-only and mutating recovery modes were combined")
	}
	if err := requireStoppedOwner(os.Getpid()); err == nil {
		t.Fatal("explicit recovery could fence a known live original owner")
	}
	if err := requireStoppedOwner(0); err == nil {
		t.Fatal("recovery accepted a ledger lacking original owner information")
	}
}

func TestRetainedLedgerBoundPreservesPriorReceipts(t *testing.T) {
	dir := t.TempDir()
	if err := ledgerGuard(dir, maxLedgerDirectoryBytes+1); err == nil {
		t.Fatal("cumulative ledger byte bound escaped")
	}
	path := filepath.Join(dir, "prior.ledger")
	if err := os.WriteFile(path, []byte("preserved"), 0600); err != nil {
		t.Fatal(err)
	}
	_ = ledgerGuard(dir, maxLedgerDirectoryBytes+1)
	if b, err := os.ReadFile(path); err != nil || string(b) != "preserved" {
		t.Fatal("disk guard changed retained receipts")
	}
}

func TestHTTPAcceptsOnlyWellFormed202Receipt(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		want   outcome
	}{
		{202, `{"partition":0,"offset":42,"admitted_at":"2026-09-10T12:00:00Z"}`, recorded},
		{201, `{"partition":0,"offset":42,"admitted_at":"2026-09-10T12:00:00Z"}`, unknown},
		{202, `{}`, unknown}, {425, `{}`, notOpen}, {410, `{}`, closed}, {429, `{}`, busy}, {503, `{}`, unknown},
	} {
		t.Run(tc.want.String()+time.Duration(tc.status).String(), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Token  string
					Choice uint32
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Token) != 32 || body.Choice != 2 {
					t.Error("bad request payload")
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			got := httpSubmitter(server.Client(), server.URL)(context.Background(), [16]byte{1}, 2)
			if got.Outcome != tc.want {
				t.Fatalf("status %d: got %s want %s", tc.status, got.Outcome, tc.want)
			}
		})
	}
}

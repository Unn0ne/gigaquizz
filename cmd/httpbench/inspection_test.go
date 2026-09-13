package main

import (
	"context"
	"crypto/aes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestOfflineInspectionCovers102MWith17BoundedGeneratorsWithoutIdentities(t *testing.T) {
	m, _, _, _ := fixture(t)
	m.Config.RepeatEvery = 0
	m.Config.Workers = 4096
	p, err := makeDistributedPlan(m.Config, m.Poll, 102000000, 17)
	if err != nil {
		t.Fatal(err)
	}
	path := writePlanForTest(t, p)
	r, err := inspectDistributedPlan(path)
	if err != nil || !r.Complete || r.UniqueKeys != 102000000 || r.Attempts != 102000000 || r.ScheduledSeconds != 60 || len(r.Generators) != 17 || r.PlanSHA256 != p.digest() {
		t.Fatalf("inspection: %+v %v", r, err)
	}
	for i, g := range r.Generators {
		if g.Index != i || g.UniqueKeys != 6000000 || g.KeyOffset != uint64(i)*6000000 || g.Attempts != 6000000 || g.LedgerBytes != 6000000*64 || g.WorkerBufferBytes != 4097*65536 {
			t.Fatalf("incorrect generator/resource range: %+v", g)
		}
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{m.Config.URL, m.Config.PollID, `"seed"`, `"namespace"`, `"url"`, `"poll_id"`, `"poll"`} {
		if strings.Contains(string(b), private) {
			t.Fatalf("inspection exposes private field %q", private)
		}
	}
	// The only private file is the small input plan. Inspection never prepares
	// a ledger, allocates per-attempt audit state, or contacts the fixture origin.
	items, err := os.ReadDir(filepath.Dir(path))
	if err != nil || len(items) != 1 || items[0].Name() != "plan.json" {
		t.Fatal("inspection created run artifacts")
	}
	p.Generators[1].URL = "http://another-origin.invalid"
	if p.validate() == nil {
		t.Fatal("mixed-origin plan accepted")
	}
}

func TestLegacyPlanDigestIgnoresNewRuntimeClockPolicy(t *testing.T) {
	// Frozen version-1 wire representation, before runtime clock policy existed.
	const legacy = `{"version":1,"unique_keys":60,"poll":{"id":"00000000-0000-4000-8000-000000000001","question":"fixture","type":"single","options":[{"id":1,"label":"A"},{"id":2,"label":"B"}],"starts_at":"2026-09-12T12:00:00Z","ends_at":"2026-09-12T12:01:00Z","created_at":"2026-09-12T11:59:00Z"},"seed":[1,2,3,0,0,0,0,0,0,0,0,0,0,0,0,0],"namespace":[9,8,7,0,0,0,0,0],"generators":[{"url":"http://127.0.0.1:1","poll_id":"00000000-0000-4000-8000-000000000001","unique_rate":1,"unique_count":60,"duration_ns":60000000000,"start_at":"2026-09-12T12:00:00Z","workers":2,"queue":4,"max_lag_ns":100000000,"request_timeout_ns":1000000000,"repeat_every":2}]}`
	path := filepath.Join(t.TempDir(), "plan.json")
	if err := os.WriteFile(path, []byte(legacy), 0600); err != nil {
		t.Fatal(err)
	}
	p, err := loadDistributedPlan(path)
	if err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256([]byte(legacy))
	if p.digest() != hex.EncodeToString(want[:]) {
		t.Fatal("legacy canonical plan digest changed")
	}
	c, err := configFromPlan(config{PlanFile: path, Directory: "private-run", Allow: true, MaxClockError: 10 * time.Millisecond})
	if err != nil || c.MaxClockError != 10*time.Millisecond || !sameConfig(c, p.Generators[0]) {
		t.Fatalf("runtime policy lost or persisted: %+v %v", c, err)
	}
	p.Generators[0].MaxClockError = time.Second
	if p.digest() != hex.EncodeToString(want[:]) {
		t.Fatal("runtime clock budget altered the private plan identity")
	}
}

func TestOfflineLedgerVerificationBindsPlanAndReportsEveryOutcome(t *testing.T) {
	m, records, _, _ := fixture(t)
	m.Config.Journey = true
	p, err := makeDistributedPlan(m.Config, m.Poll, m.Config.keys(), 1)
	if err != nil {
		t.Fatal(err)
	}
	c := p.Generators[0]
	c.Directory = t.TempDir()
	m.Config, m.Seed, m.Namespace, m.PlanSHA256 = c, p.Seed, p.Namespace, p.digest()
	block, _ := aes.NewCipher(p.Seed[:])
	for i := range records {
		records[i].Token = tokenFor(block, p.Namespace, records[i].Key)
	}
	for i, state := range []byte{stateClosed, stateBusy, stateNotOpen, stateRejected, stateJourneyFailed} {
		records[i+2].State = state
	}
	manifestPath := writeFixture(t, m, records)
	c.PlanFile = writePlanForTest(t, p)
	r, err := verifyPlanLedger(context.Background(), c)
	if err != nil || !r.Complete || !r.ClientLedgerValid || r.PlanSHA256 != p.digest() || r.Records != c.attempts() || r.ACK != 2 || r.Unknown != 1 || r.Closed != 1 || r.NotAdmitted != 1 || r.NotOpen != 1 || r.Rejected != 1 || r.JourneyFailed != 1 || r.Skipped != c.attempts()-8 {
		t.Fatalf("verification: %+v %v", r, err)
	}
	// A completed but differently configured manifest must fail before a
	// positive offline result; validation belongs to Go's canonical contract.
	var head privateManifest
	if err := readPrivateJSON(manifestPath, &head, 2<<20); err != nil {
		t.Fatal(err)
	}
	head.Config.URL = "http://wrong-target.invalid"
	if err := writeManifest(c.Directory, head); err != nil {
		t.Fatal(err)
	}
	r, err = verifyPlanLedger(context.Background(), c)
	if err == nil || r.Complete || r.ClientLedgerValid || r.Failure != "generator_manifest_differs_from_plan" {
		t.Fatalf("wrong private configuration passed: %+v %v", r, err)
	}
	head = m
	head.Complete = false
	if err := writeManifest(c.Directory, head); err != nil {
		t.Fatal(err)
	}
	r, err = verifyPlanLedger(context.Background(), c)
	if err == nil || r.Complete || r.ClientLedgerValid {
		t.Fatal("incomplete manifest passed offline verification")
	}
}

func requirePromptReadFailure(t *testing.T, path string, read func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- read() }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("FIFO accepted as a private regular file")
		}
	case <-time.After(time.Second):
		// Unblock an accidentally blocking old open so even a regression exits
		// cleanly instead of stranding a goroutine or hanging the test process.
		f, err := os.OpenFile(path, os.O_RDWR|syscall.O_NONBLOCK, 0)
		if err == nil {
			_ = f.Close()
		}
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		t.Fatal("private file validation blocked opening a FIFO")
	}
}

func TestPrivatePlanAndLedgerReadsRejectFIFOWithoutBlocking(t *testing.T) {
	fifo := filepath.Join(t.TempDir(), "plan.json")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	requirePromptReadFailure(t, fifo, func() error { _, err := loadDistributedPlan(fifo); return err })
	m, records, _, _ := fixture(t)
	manifest := writeFixture(t, m, records)
	ledger := filepath.Join(m.Config.Directory, "worker-0000.bin")
	if err := os.Remove(ledger); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(ledger, 0600); err != nil {
		t.Fatal(err)
	}
	requirePromptReadFailure(t, ledger, func() error { _, _, err := loadLedger(manifest); return err })
}

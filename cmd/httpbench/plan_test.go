package main

import (
	"context"
	"crypto/aes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gigaquizz/internal/poll"
)

func TestDistributedPlanCoversExactly100MAnd10PercentRepeats(t *testing.T) {
	m, _, _, _ := fixture(t)
	m.Config.RepeatEvery = 10
	p, err := makeDistributedPlan(m.Config, m.Poll, 100000000, 17)
	if err != nil {
		t.Fatal(err)
	}
	var keys, attempts uint64
	for _, c := range p.Generators {
		if c.KeyOffset != keys || c.keys() > 6000000 || c.scheduledOffset(c.keys()-1) >= time.Minute {
			t.Fatal("invalid coverage or schedule")
		}
		keys += c.keys()
		attempts += c.attempts()
	}
	if keys != 100000000 || attempts != 110000000 {
		t.Fatalf("population changed: %d/%d", keys, attempts)
	}
	for _, mutate := range []func(*distributedPlan){
		func(p *distributedPlan) { p.Generators[1].KeyOffset-- },
		func(p *distributedPlan) { p.Generators[1].KeyOffset++ },
		func(p *distributedPlan) { p.Generators = p.Generators[:len(p.Generators)-1] },
		func(p *distributedPlan) { p.Generators[1].Start = p.Generators[1].Start.Add(time.Second) },
	} {
		bad := p
		bad.Generators = append([]config(nil), p.Generators...)
		mutate(&bad)
		if bad.validate() == nil {
			t.Fatal("accepted incomplete or overlapping plan")
		}
	}
}

func TestGlobalRepeatSequenceAtUnalignedGeneratorBoundary(t *testing.T) {
	m, _, _, _ := fixture(t)
	c := m.Config
	c.Unique, c.KeyOffset, c.RepeatEvery = 7, 3, 5
	seen := make(map[uint64]bool)
	for k := uint64(0); k < c.keys(); k++ {
		for _, choice := range []uint32{1, 2} {
			seq, err := sequence(c, k, choice)
			if err != nil {
				continue
			}
			if seq >= c.attempts() || seen[seq] {
				t.Fatal("repeat collided or escaped range")
			}
			seen[seq] = true
		}
	}
	if len(seen) != 9 || uint64(len(seen)) != c.attempts() {
		t.Fatal("boundary lost a planned repeat")
	}
}

func distributedFixture(t *testing.T) ([]auditInput, []journalVote, poll.Results) {
	t.Helper()
	m, _, _, res := fixture(t)
	m.Config.RepeatEvery = 2
	p, err := makeDistributedPlan(m.Config, m.Poll, 13, 3)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := aes.NewCipher(p.Seed[:])
	var inputs []auditInput
	var journal []journalVote
	for _, c := range p.Generators {
		c.Directory = t.TempDir()
		part := privateManifest{Version: 1, Complete: true, Config: c, Poll: m.Poll, Seed: p.Seed, Namespace: p.Namespace, PlanSHA256: p.digest()}
		a := make([]attempt, c.attempts())
		for k := uint64(0); k < c.keys(); k++ {
			for _, choice := range []uint32{1, 2} {
				seq, err := sequence(c, k, choice)
				if err != nil {
					continue
				}
				admitted := c.Start.Add(time.Second + time.Duration(c.KeyOffset+k)*time.Millisecond)
				a[seq] = attempt{Seq: seq, Key: k, Token: tokenFor(block, p.Namespace, c.KeyOffset+k), Choice: choice, State: stateACK, HTTP: 202, AdmittedNS: admitted.UnixNano(), ScheduledNS: int64(c.scheduledOffset(k))}
				journal = append(journal, journalVote{a[seq].Token, choice, admitted})
			}
		}
		path := writeFixture(t, part, a)
		loaded, states, err := loadLedger(path)
		if err != nil {
			t.Fatal(err)
		}
		inputs = append(inputs, auditInput{loaded, states})
	}
	res.TotalVotes = 13
	res.Options[0].Votes = 13
	res.Options[1].Votes = 0
	return inputs, journal, res
}

func TestDistributedExactAuditAcrossDisjointRanges(t *testing.T) {
	inputs, journal, res := distributedFixture(t)
	r, err := reconcileMany(context.Background(), inputs, res, replayFixture(journal, nil))
	if err != nil || !r.Correct || r.Planned != 19 || r.Confirmed != 19 || r.Matched != 19 || r.Canonical != 13 || r.Duplicates != 6 || r.Generators != 3 || r.ACKCoverage != 1 {
		t.Fatalf("%+v %v", r, err)
	}
}

func TestDistributedAuditRejectsCrossGeneratorMissingCopyAndWrongIdentity(t *testing.T) {
	for _, mode := range []string{"missing", "copy", "identity", "overlap"} {
		t.Run(mode, func(t *testing.T) {
			inputs, journal, res := distributedFixture(t)
			switch mode {
			case "missing":
				journal = journal[:len(journal)-1]
			case "copy":
				journal = append(journal, journal[len(journal)-1])
			case "identity":
				inputs[1].Manifest.Seed[0] ^= 1
			case "overlap":
				inputs[1].Manifest.Config.KeyOffset = 0
			}
			r, err := reconcileMany(context.Background(), inputs, res, replayFixture(journal, nil))
			if err == nil || r.Correct {
				t.Fatalf("accepted %s", mode)
			}
		})
	}
}

func TestPlanMissingLedgerCannotProduceSuccessfulAudit(t *testing.T) {
	m, _, _, _ := fixture(t)
	p, err := makeDistributedPlan(m.Config, m.Poll, 13, 3)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "plan.json")
	b, _ := json.Marshal(p)
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	r, err := runPlanAudit(context.Background(), path, dir, "unused", "", "unused")
	if err == nil || r.Correct || r.JournalClosed || r.Failure != "missing_or_unsafe_generator_directory" {
		t.Fatalf("%+v %v", r, err)
	}
}

func TestStandaloneWorkerFileBudget(t *testing.T) {
	if checkFileBudget(128, 256) == nil {
		t.Fatal("default workers accepted insufficient limit")
	}
	if checkFileBudget(4096, 16384) != nil {
		t.Fatal("sufficient bounded limit rejected")
	}
}

func TestCancelledLedgerReadFailsBeforeScanning(t *testing.T) {
	m, a, _, _ := fixture(t)
	path := writeFixture(t, m, a)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := loadLedgerContext(ctx, path); err != context.Canceled {
		t.Fatalf("cancelled reader returned %v", err)
	}
}

func TestPlanNormalizesOriginBeforeManifestComparison(t *testing.T) {
	m, _, _, _ := fixture(t)
	m.Config.URL += "/"
	p, err := makeDistributedPlan(m.Config, m.Poll, 12, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range p.Generators {
		normalized := c
		normalized.URL = strings.TrimRight(c.URL, "/")
		if !sameConfig(c, normalized) {
			t.Fatal("plan differs from workload-normalized configuration")
		}
	}
	p.Generators[0].URL += "/"
	if p.validate() == nil {
		t.Fatal("accepted noncanonical external plan")
	}
}

package main

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"gigaquizz/internal/poll"
)

const maxPlanKeys uint64 = 120000000
const maxPlanAttempts uint64 = 200000000

// A plan assigns disjoint ranges of one full-width permutation before any
// generator starts. It is private: seed, namespace, poll and endpoints are
// never included in the aggregate workload/audit reports.
type distributedPlan struct {
	Version    int       `json:"version"`
	Unique     uint64    `json:"unique_keys"`
	Poll       poll.Poll `json:"poll"`
	Seed       [16]byte  `json:"seed"`
	Namespace  [8]byte   `json:"namespace"`
	Generators []config  `json:"generators"`
}

func sameConfig(a, b config) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return bytes.Equal(x, y)
}

func samePoll(a, b poll.Poll) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return bytes.Equal(x, y)
}

func (p distributedPlan) digest() string {
	b, _ := json.Marshal(p)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func (p distributedPlan) validate() error {
	if p.Version != 1 || p.Unique < 1 || p.Unique > maxPlanKeys || len(p.Generators) < 1 || len(p.Generators) > 128 {
		return errors.New("invalid distributed plan bounds")
	}
	var end, attempts uint64
	first := p.Generators[0]
	for _, c := range p.Generators {
		if c.URL != strings.TrimRight(c.URL, "/") || c.validate() != nil || validatePoll(c, p.Poll) != nil || c.KeyOffset != end || c.RepeatEvery != first.RepeatEvery || c.Journey != first.Journey || c.Definition != first.Definition {
			return errors.New("distributed plan ranges must completely and uniquely cover one immutable poll")
		}
		end += c.keys()
		attempts += c.attempts()
	}
	if end != p.Unique || attempts > maxPlanAttempts {
		return errors.New("distributed plan population or attempt cap mismatch")
	}
	b, _ := aes.NewCipher(p.Seed[:])
	if _, err := keyFor(b, p.Namespace, [16]byte{}, p.Unique); err == nil {
		return errors.New("distributed identity contains forbidden zero token")
	}
	return nil
}

func loadDistributedPlan(path string) (distributedPlan, error) {
	var p distributedPlan
	if err := readPrivateJSON(path, &p, 1<<20); err != nil {
		return p, err
	}
	return p, p.validate()
}

func makeDistributedPlan(c config, p poll.Poll, total uint64, generators int) (distributedPlan, error) {
	c.URL = strings.TrimRight(c.URL, "/")
	plan := distributedPlan{Version: 1, Unique: total, Poll: p}
	if total < 1 || total > maxPlanKeys || generators < 1 || generators > 128 || uint64(generators) > total {
		return plan, errors.New("invalid distributed plan size")
	}
	var offset uint64
	for i := 0; i < generators; i++ {
		x := c
		x.Unique = total / uint64(generators)
		if uint64(i) < total%uint64(generators) {
			x.Unique++
		}
		x.KeyOffset = offset
		x.Directory, x.PlanFile, x.Allow = "", "", false
		x.Generator = 0
		plan.Generators = append(plan.Generators, x)
		offset += x.Unique
	}
	if _, err := rand.Read(plan.Namespace[:]); err != nil {
		return plan, err
	}
	for {
		if _, err := rand.Read(plan.Seed[:]); err != nil {
			return plan, err
		}
		block, _ := aes.NewCipher(plan.Seed[:])
		if _, err := keyFor(block, plan.Namespace, [16]byte{}, total); err != nil {
			break
		}
	}
	return plan, plan.validate()
}

func prepareDistributedPlan(ctx context.Context, c config, path string, total uint64, generators int) (any, error) {
	// Validate per-generator resource bounds before making even a setup GET.
	probe := poll.Poll{ID: c.PollID, Type: "single", Options: []poll.Option{{ID: 1}, {ID: 2}}, StartsAt: c.Start, EndsAt: c.Start.Add(time.Minute)}
	if _, err := makeDistributedPlan(c, probe, total, generators); err != nil {
		return nil, err
	}
	client := newClient(1, c.Timeout)
	defer client.CloseIdleConnections()
	p, err := fetchPoll(ctx, c, client)
	if err != nil {
		return nil, err
	}
	plan, err := makeDistributedPlan(c, p, total, generators)
	if err != nil {
		return nil, err
	}
	data, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return nil, err
	}
	parent, err := os.Lstat(filepath.Dir(path))
	if err != nil || !parent.IsDir() || parent.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("plan parent must be an existing real directory")
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	_, err = f.Write(append(data, '\n'))
	if e := f.Sync(); err == nil {
		err = e
	}
	if e := f.Close(); err == nil {
		err = e
	}
	if err == nil {
		err = syncDirectory(filepath.Dir(path))
	}
	if err != nil {
		return nil, err
	}
	var attempts uint64
	for _, c := range plan.Generators {
		attempts += c.attempts()
	}
	return map[string]any{"mode": "distributed-plan", "complete": true, "generators": generators, "planned_unique_keys": total, "planned_attempts": attempts, "scheduled_seconds": 60, "http_votes_sent": 0}, nil
}

func configFromPlan(input config) (config, error) {
	p, err := loadDistributedPlan(input.PlanFile)
	if err != nil {
		return input, err
	}
	if input.Generator < 0 || input.Generator >= len(p.Generators) {
		return input, errors.New("generator index outside plan")
	}
	c := p.Generators[input.Generator]
	c.Directory, c.Allow, c.PlanFile, c.Generator = input.Directory, input.Allow, input.PlanFile, input.Generator
	return c, nil
}

func runPlanAudit(ctx context.Context, path, ledgerRoot, fileJournal, kafkaConfig, resultsPath string) (auditReport, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	start := time.Now()
	r := auditReport{Mode: "independent-distributed-http-audit"}
	p, err := loadDistributedPlan(path)
	if err != nil {
		r.Failure = "invalid_distributed_plan"
		return r, err
	}
	if ledgerRoot == "" {
		r.Failure = "missing_ledger_root"
		return r, errors.New(r.Failure)
	}
	inputs := make([]auditInput, 0, len(p.Generators))
	for i, expected := range p.Generators {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		dir := filepath.Join(ledgerRoot, fmt.Sprintf("generator-%03d", i))
		st, err := os.Lstat(dir)
		if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			r.Failure = "missing_or_unsafe_generator_directory"
			return r, errors.New(r.Failure)
		}
		manifestPath := filepath.Join(dir, "manifest.json")
		var head privateManifest
		if err := readPrivateJSON(manifestPath, &head, 2<<20); err != nil {
			r.Failure = "invalid_generator_manifest"
			return r, err
		}
		if !sameConfig(head.Config, expected) || !samePoll(head.Poll, p.Poll) || head.Seed != p.Seed || head.Namespace != p.Namespace || head.PlanSHA256 != p.digest() {
			r.Failure = "generator_manifest_differs_from_plan"
			return r, errors.New(r.Failure)
		}
		m, states, err := loadLedgerContext(ctx, manifestPath)
		if err != nil {
			r.Failure = "invalid_private_ledger"
			return r, err
		}
		inputs = append(inputs, auditInput{Manifest: m, States: states})
	}
	res, err := savedResults(resultsPath)
	if err != nil {
		r.Failure = "invalid_saved_results"
		return r, err
	}
	reader, err := backend(inputs[0].Manifest, fileJournal, kafkaConfig)
	if err != nil {
		r.Failure = "invalid_journal_configuration"
		return r, err
	}
	r, err = reconcileMany(ctx, inputs, res, reader.Replay)
	r.Mode = "independent-distributed-http-audit"
	r.JournalInspection = reader.Inspection
	r.WallSeconds = time.Since(start).Seconds()
	return r, err
}

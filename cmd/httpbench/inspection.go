package main

import (
	"context"
	"errors"
	"path/filepath"
	"time"
)

type generatorInspection struct {
	Index             int     `json:"index"`
	UniqueKeys        uint64  `json:"unique_keys"`
	Attempts          uint64  `json:"planned_attempts"`
	KeyOffset         uint64  `json:"key_offset"`
	Workers           int     `json:"workers"`
	Queue             int     `json:"queue"`
	RepeatEvery       uint64  `json:"repeat_every"`
	MaxLagMS          float64 `json:"max_lag_ms"`
	TimeoutMS         float64 `json:"request_timeout_ms"`
	Journey           bool    `json:"journey"`
	Definition        bool    `json:"definition"`
	LedgerBytes       uint64  `json:"ledger_bytes"`
	WorkerBufferBytes uint64  `json:"worker_buffer_bytes"`
}

type planInspection struct {
	Mode             string                `json:"mode"`
	Complete         bool                  `json:"complete"`
	Failure          string                `json:"failure,omitempty"`
	PlanSHA256       string                `json:"plan_sha256,omitempty"`
	UniqueKeys       uint64                `json:"planned_unique_keys"`
	Attempts         uint64                `json:"planned_attempts"`
	ScheduledSeconds int                   `json:"scheduled_seconds"`
	Start            time.Time             `json:"start_at"`
	End              time.Time             `json:"ends_at"`
	Generators       []generatorInspection `json:"generators"`
}

func inspectDistributedPlan(path string) (planInspection, error) {
	r := planInspection{Mode: "distributed-plan-inspection"}
	p, err := loadDistributedPlan(path)
	if err != nil {
		r.Failure = "invalid_distributed_plan"
		return r, err
	}
	r.PlanSHA256, r.UniqueKeys, r.ScheduledSeconds = p.digest(), p.Unique, 60
	r.Start, r.End = p.Poll.StartsAt, p.Poll.EndsAt
	for i, c := range p.Generators {
		r.Attempts += c.attempts()
		r.Generators = append(r.Generators, generatorInspection{Index: i, UniqueKeys: c.keys(), Attempts: c.attempts(), KeyOffset: c.KeyOffset, Workers: c.Workers, Queue: c.Queue, RepeatEvery: c.RepeatEvery, MaxLagMS: float64(c.MaxLag) / 1e6, TimeoutMS: float64(c.Timeout) / 1e6, Journey: c.Journey, Definition: c.Definition, LedgerBytes: c.attempts() * ledgerBytes, WorkerBufferBytes: uint64(c.Workers+1) * 64 * 1024})
	}
	r.Complete = true
	return r, nil
}

type ledgerVerification struct {
	Mode              string `json:"mode"`
	Complete          bool   `json:"complete"`
	ClientLedgerValid bool   `json:"client_ledger_valid"`
	Failure           string `json:"failure,omitempty"`
	PlanSHA256        string `json:"plan_sha256,omitempty"`
	Generator         int    `json:"generator"`
	UniqueKeys        uint64 `json:"planned_unique_keys"`
	Attempts          uint64 `json:"planned_attempts"`
	Records           uint64 `json:"ledger_records"`
	ACK               uint64 `json:"valid_recorded_ack"`
	Unknown           uint64 `json:"unknown"`
	Closed            uint64 `json:"closed"`
	NotAdmitted       uint64 `json:"not_admitted"`
	NotOpen           uint64 `json:"not_open"`
	Rejected          uint64 `json:"rejected"`
	Skipped           uint64 `json:"generator_skipped"`
	JourneyFailed     uint64 `json:"journey_failed"`
	InvalidPositive   uint64 `json:"invalid_successful_responses"`
}

func verifyPlanLedger(ctx context.Context, c config) (ledgerVerification, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	r := ledgerVerification{Mode: "distributed-generator-ledger-verification", Generator: c.Generator}
	fail := func(reason string, err error) (ledgerVerification, error) { r.Failure = reason; return r, err }
	p, err := loadDistributedPlan(c.PlanFile)
	if err != nil || c.Generator < 0 || c.Generator >= len(p.Generators) || !sameConfig(c, p.Generators[c.Generator]) {
		return fail("invalid_distributed_plan", errors.New("verification configuration differs from plan"))
	}
	r.PlanSHA256, r.UniqueKeys, r.Attempts = p.digest(), c.keys(), c.attempts()
	path := filepath.Join(c.Directory, "manifest.json")
	var head privateManifest
	if err := readPrivateJSON(path, &head, 2<<20); err != nil {
		return fail("invalid_generator_manifest", err)
	}
	if err := validatePlanManifest(p, c.Generator, head); err != nil {
		return fail("generator_manifest_differs_from_plan", err)
	}
	m, states, err := loadLedgerContext(ctx, path)
	if err != nil {
		return fail("invalid_private_ledger", err)
	}
	// Bind the actual manifest returned by the complete reader too, so replacing
	// a manifest between these reads cannot bypass plan identity validation.
	if err := validatePlanManifest(p, c.Generator, m); err != nil {
		return fail("generator_manifest_differs_from_plan", err)
	}
	for _, state := range states.Status {
		switch state & 0x3f {
		case stateACK:
			r.ACK++
		case stateUnknown:
			r.Unknown++
		case stateClosed:
			r.Closed++
		case stateBusy:
			r.NotAdmitted++
		case stateNotOpen:
			r.NotOpen++
		case stateRejected:
			r.Rejected++
		case stateSkipped:
			r.Skipped++
		case stateJourneyFailed:
			r.JourneyFailed++
		}
		if state&0x40 != 0 {
			r.InvalidPositive++
		}
	}
	r.Records = uint64(len(states.Status))
	r.Complete, r.ClientLedgerValid = true, true
	return r, nil
}

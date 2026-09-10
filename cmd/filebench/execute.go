package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"gigaquizz/internal/filelog"
)

func execute(parent context.Context, c config) (r report) {
	start := time.Now()
	defer func() { r.WallSeconds = time.Since(start).Seconds() }()
	r.Mode = "direct local-file journal with synchronized group ACK"
	r.Errors = []string{}
	r.Limitations = []string{
		"A successful frame receipt confirms attempts after the writer's platform disk synchronization call, not canonical unique choices. This is a direct Go-to-local-file API test without public HTTP, TLS, browser identity or external routing.",
		"One host and one local storage device are used. There is no replication or failover. This workload and a new-process offline replay do not simulate power loss, controller failure or loss of the storage device.",
		"Synthetic complete 128-bit keys are an AES permutation of counters. Public output omits the private seed, poll identifier and per-key state. Deduplication uses exact full-key inversion, never probabilistic hashes.",
		"Generator and admission queues are bounded. Planned but unsubmitted attempts, definitive refusals, unknown outcomes and missing acknowledged attempts are separate categories.",
		"Private manifest, states and ACK frames are persisted after workload completion before Seal. Killing the generator during load is not covered by this client-receipt persistence method.",
		"The writer is closed before offline replay. Reopening in a new process does not clear operating-system caches or prove recovery after a power interruption.",
	}
	ctx, cancel := context.WithTimeout(parent, 6*time.Minute)
	defer cancel()
	if c.auditOnly != "" {
		r.Mode = "read-only offline local-file audit"
		m, s, acks, err := readLedger(c.auditOnly)
		if err != nil {
			r.Errors = append(r.Errors, err.Error())
			return r
		}
		r.Manifest = c.auditOnly
		r.Configuration = map[string]any{"original_writer_sync_mode": m.SyncMode, "audit_performs_write_or_sync": false}
		a, err := reconcile(ctx, m, s, acks, filelog.Replay)
		r.Audit = &a
		if err != nil {
			r.Errors = append(r.Errors, err.Error())
		}
		return r
	}
	disk, err := diskGuard(ownedLedgerRoot, c, true)
	if err != nil {
		r.Configuration = map[string]any{"disk_preflight": disk}
		r.Errors = append(r.Errors, err.Error())
		return r
	}
	s := states{Original: make([]byte, c.keys())}
	if c.repeatEvery > 0 {
		s.Repeat = make([]byte, c.keys())
	}
	var seed, pollID [16]byte
	var runID [8]byte
	for _, dst := range [][]byte{seed[:], pollID[:], runID[:]} {
		if _, err := rand.Read(dst); err != nil {
			r.Errors = append(r.Errors, "cannot generate private benchmark identifiers")
			return r
		}
	}
	root, err := filepath.Abs(ownedLogsRoot)
	if err != nil {
		r.Errors = append(r.Errors, "cannot resolve owned journal root")
		return r
	}
	scheduleStart := time.Now().Add(15 * time.Second)
	cfg := filelog.Config{Directory: filepath.Join(root, "filebench_"+hex.EncodeToString(runID[:])), PollID: pollID, Partitions: 1, StartsAt: scheduleStart.UTC(), EndsAt: scheduleStart.Add(time.Minute).UTC(), AllowedMask: 3, Multiple: false, BatchSize: c.batchSize, Linger: c.linger, QueuePerPartition: c.storeQueue}
	r.Configuration = map[string]any{
		"rate_unique_keys_per_second": c.rate, "planned_unique_keys": c.keys(), "planned_attempts": c.attempts(), "duration_seconds": 60,
		"workers": c.workers, "generator_queue_frames": c.queue, "frame_size": c.frameSize, "frame_age_ms": float64(c.frameAge) / 1e6, "partitions": 1,
		"group_batch_votes": c.batchSize, "group_linger_ms": float64(c.linger) / 1e6, "queue_votes": c.storeQueue,
		"max_lag_ms": float64(c.maxLag) / 1e6, "client_timeout_seconds": c.timeout.Seconds(), "drain_seconds": c.drain.Seconds(), "repeat_every": c.repeatEvery,
		"starts_at": cfg.StartsAt, "ends_at": cfg.EndsAt, "acks": "after group disk synchronization succeeds", "sync_mode": filelog.SyncMode(),
		"frame_header_bytes": 40, "record_entry_bytes": 28, "wal_header_bytes": 80, "closed_record_bytes": 40, "maximum_metadata_frames": c.frameBound(), "disk_preflight": disk,
	}
	setupCtx, setupCancel := context.WithTimeout(ctx, 90*time.Second)
	store, err := filelog.New(setupCtx, cfg)
	setupCancel()
	if err != nil {
		r.Errors = append(r.Errors, fmt.Sprintf("prepare file journal: %v", err))
		return r
	}
	defer store.Close()
	if time.Until(scheduleStart) < 250*time.Millisecond {
		r.Errors = append(r.Errors, "writer preparation consumed 15-second start lead; no workload scheduled")
		return r
	}
	fmt.Fprintf(os.Stderr, "filebench: ready; %d attempts from %s for 60s; sync=%s\n", c.attempts(), cfg.StartsAt.Format(time.RFC3339Nano), filelog.SyncMode())
	// Preserve monotonic scheduling independently of the writer's wall deadline.
	workConfig := cfg
	workConfig.StartsAt = scheduleStart
	workConfig.EndsAt = scheduleStart.Add(time.Minute)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	w, acks := runWorkload(ctx, c, workConfig, seed, s, store.SubmitFrame)
	runtime.ReadMemStats(&after)
	r.Runtime = map[string]any{"scope": "Generator and local journal writer after setup through workload and drain, excluding offline replay", "gomaxprocs": runtime.GOMAXPROCS(0), "heap_alloc_before_bytes": before.HeapAlloc, "heap_alloc_after_bytes": after.HeapAlloc, "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "allocations": after.Mallocs - before.Mallocs, "gc_cycles": after.NumGC - before.NumGC, "gc_stop_the_world_pause_ns": after.PauseTotalNs - before.PauseTotalNs}
	r.Workload = &w
	if w.Counts.InvalidReceipts > 0 {
		r.Errors = append(r.Errors, fmt.Sprintf("successful frame response violated receipt contract: partition=%d count=%d offset=%d admission_window=%d", w.Counts.InvalidReceiptPartition, w.Counts.InvalidReceiptCount, w.Counts.InvalidReceiptOffset, w.Counts.InvalidReceiptWindow))
	}
	fmt.Fprintf(os.Stderr, "filebench: workload complete; recorded=%d unknown=%d busy=%d skipped=%d closed=%d; saving client ledger\n", w.Counts.Recorded, w.Counts.Unknown, w.Counts.Busy, w.Counts.Skipped, w.Counts.Closed)
	m := manifest{Config: cfg, SyncMode: filelog.SyncMode(), Seed: seed, Rate: c.rate, Keys: c.keys(), RepeatEvery: c.repeatEvery, FrameSize: c.frameSize, FrameAge: c.frameAge, Counts: w.Counts}
	path, err := saveLedger(ownedLedgerRoot, m, s, acks, c)
	r.Manifest = path
	if err != nil {
		r.Errors = append(r.Errors, err.Error())
	}
	sealCtx, sealCancel := context.WithTimeout(ctx, max(0, time.Until(cfg.EndsAt))+45*time.Second)
	_, err = store.Seal(sealCtx)
	sealCancel()
	r.StoreMetrics = store.Metrics()
	store.Close()
	if info, statErr := os.Stat(filepath.Join(cfg.Directory, "votes.wal")); statErr == nil {
		r.Configuration["actual_wal_bytes"] = info.Size()
	} else {
		r.Errors = append(r.Errors, "cannot inspect completed WAL size")
	}
	if used, _, sizeErr := retainedBytes(cfg.Directory); sizeErr == nil {
		r.Configuration["actual_journal_directory_bytes"] = used
		if used > maximumWALBytes {
			r.Errors = append(r.Errors, "journal directory exceeded per-run 4 GiB bound")
		}
	} else {
		r.Errors = append(r.Errors, "cannot inspect completed journal size")
	}
	if err != nil {
		r.Errors = append(r.Errors, fmt.Sprintf("seal file journal: %v", err))
		return r
	}
	a, err := reconcile(ctx, m, s, acks, filelog.Replay)
	r.Audit = &a
	if err != nil {
		r.Errors = append(r.Errors, err.Error())
	}
	fmt.Fprintf(os.Stderr, "filebench: offline audit complete; records=%d canonical=%d correct=%t\n", a.RecordedAttempts, a.CanonicalKeys, a.Correct)
	return r
}

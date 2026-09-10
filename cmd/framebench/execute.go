package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"runtime"
	"time"

	"gigaquizz/internal/votelog"
)

func execute(parent context.Context, c config) (r report) {
	start := time.Now()
	defer func() { r.WallSeconds = time.Since(start).Seconds() }()
	r.Mode = "direct compact-frame replicated journal"
	r.Errors = []string{}
	r.Limitations = []string{
		"A successful frame receipt acknowledges committed attempts, not canonical unique choices. It is a direct Go-to-Kafka API test without public HTTP, TLS, CDN, browser identity or external routing.",
		"Generator, Kafka clients and three broker JVMs share one host. Replicated Kafka ACK is not a remote physical-flush proof and does not establish survival of simultaneous all-copy power loss.",
		"Synthetic full128-bit keys are an AES permutation of counters. The ordinary report omits the private seed and per-key state. Streaming reconciliation uses the complete keys and exact counters, never probabilistic deduplication.",
		"Generator and admission queues are bounded. Unsubmitted slots, definitive refusals, unknown outcomes and missing acknowledged attempts are separate categories.",
		"Private manifests/states/ACK frames are written after workload completion before Seal. Killing the generator during load is not covered by this receipt persistence method.",
		"Single static writer ownership and prepared poll configuration are experimental; production ownership assignment and fencing orchestration remain separate work.",
	}
	ctx, cancel := context.WithTimeout(parent, 6*time.Minute)
	defer cancel()
	if c.auditOnly != "" {
		r.Mode = "read-only compact-frame audit"
		m, s, acks, err := readLedger(c.auditOnly)
		if err != nil {
			r.Errors = append(r.Errors, err.Error())
			return r
		}
		m.Config.Brokers, _ = localBrokers(c.brokers)
		r.Manifest = c.auditOnly
		a, err := reconcile(ctx, m, s, acks, votelog.Replay)
		r.Audit = &a
		if err != nil {
			r.Errors = append(r.Errors, err.Error())
		}
		return r
	}
	disk, err := diskGuard(".local/framebench", c, true)
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
	if _, err := rand.Read(seed[:]); err != nil {
		r.Errors = append(r.Errors, "cannot generate AES seed")
		return r
	}
	if _, err := rand.Read(pollID[:]); err != nil {
		r.Errors = append(r.Errors, "cannot generate poll ID")
		return r
	}
	brokers, _ := localBrokers(c.brokers)
	scheduleStart := time.Now().Add(15 * time.Second)
	cfg := votelog.Config{Brokers: brokers, Topic: "gqlog_framebench_" + hex.EncodeToString(pollID[:8]), PollID: pollID, Partitions: c.partitions, StartsAt: scheduleStart.UTC(), EndsAt: scheduleStart.Add(time.Minute).UTC(), AllowedMask: 3, Multiple: false, BatchSize: 4096, Linger: 20 * time.Millisecond, QueuePerPartition: 8192, TransactionTimeout: 30 * time.Second}
	r.Configuration = map[string]any{"rate_unique_keys_per_second": c.rate, "planned_unique_keys": c.keys(), "planned_attempts": c.attempts(), "duration_seconds": 60, "workers": c.workers, "generator_queue_frames": c.queue, "frame_size": c.frameSize, "frame_age_ms": float64(c.frameAge) / 1e6, "partitions": c.partitions, "max_lag_ms": float64(c.maxLag) / 1e6, "client_timeout_seconds": c.timeout.Seconds(), "drain_seconds": c.drain.Seconds(), "repeat_every": c.repeatEvery, "starts_at": cfg.StartsAt, "ends_at": cfg.EndsAt, "transaction_timeout_seconds": 30, "writer_linger_ms": 20, "queue_votes_per_partition": 8192, "replication_factor": 3, "min_insync_replicas": 2, "acks": "all, then transaction commit", "record_header_bytes": 80, "record_entry_bytes": 28, "maximum_metadata_frames": c.frameBound(), "disk_preflight": disk}
	setupCtx, setupCancel := context.WithTimeout(ctx, 90*time.Second)
	if err := votelog.CreateTopic(setupCtx, cfg); err != nil {
		setupCancel()
		r.Errors = append(r.Errors, fmt.Sprintf("create compact topic: %v", err))
		return r
	}
	store, err := votelog.New(setupCtx, cfg)
	setupCancel()
	if err != nil {
		r.Errors = append(r.Errors, fmt.Sprintf("prepare compact writers: %v", err))
		return r
	}
	defer store.Close()
	if time.Until(scheduleStart) < 250*time.Millisecond {
		r.Errors = append(r.Errors, "writer preparation consumed15-second start lead; no workload scheduled")
		return r
	}
	fmt.Fprintf(os.Stderr, "framebench: ready; %d attempts from %s for60s\n", c.attempts(), cfg.StartsAt.Format(time.RFC3339Nano))
	// Keep monotonic schedule separately from the server's wall-clock deadline.
	workConfig := cfg
	workConfig.StartsAt = scheduleStart
	workConfig.EndsAt = scheduleStart.Add(time.Minute)
	var memoryBefore, memoryAfter runtime.MemStats
	runtime.ReadMemStats(&memoryBefore)
	w, acks := runWorkload(ctx, c, workConfig, seed, s, store.SubmitFrame)
	runtime.ReadMemStats(&memoryAfter)
	r.Runtime = map[string]any{"scope": "Generator and journal client after setup through workload and drain, excluding replay", "gomaxprocs": runtime.GOMAXPROCS(0), "heap_alloc_before_bytes": memoryBefore.HeapAlloc, "heap_alloc_after_bytes": memoryAfter.HeapAlloc, "allocated_bytes": memoryAfter.TotalAlloc - memoryBefore.TotalAlloc, "allocations": memoryAfter.Mallocs - memoryBefore.Mallocs, "gc_cycles": memoryAfter.NumGC - memoryBefore.NumGC, "gc_stop_the_world_pause_ns": memoryAfter.PauseTotalNs - memoryBefore.PauseTotalNs}
	r.Workload = &w
	if w.Counts.InvalidReceipts > 0 {
		r.Errors = append(r.Errors, "successful frame response violated receipt contract")
	}
	fmt.Fprintf(os.Stderr, "framebench: workload complete; recorded=%d unknown=%d skipped=%d closed=%d; saving client ledger\n", w.Counts.Recorded, w.Counts.Unknown, w.Counts.Skipped, w.Counts.Closed)
	m := manifest{Config: cfg, Seed: seed, Rate: c.rate, Keys: c.keys(), RepeatEvery: c.repeatEvery, FrameSize: c.frameSize, FrameAge: c.frameAge, Counts: w.Counts}
	path, err := saveLedger(".local/framebench", m, s, acks, c)
	r.Manifest = path
	if err != nil {
		r.Errors = append(r.Errors, err.Error())
	}
	sealCtx, sealCancel := context.WithTimeout(ctx, max(0, time.Until(cfg.EndsAt))+45*time.Second)
	_, err = store.Seal(sealCtx)
	sealCancel()
	r.StoreMetrics = store.Metrics()
	if err != nil {
		r.Errors = append(r.Errors, fmt.Sprintf("seal compact journal: %v", err))
		return r
	}
	a, err := reconcile(ctx, m, s, acks, votelog.Replay)
	r.Audit = &a
	if err != nil {
		r.Errors = append(r.Errors, err.Error())
	}
	fmt.Fprintf(os.Stderr, "framebench: streaming audit complete; records=%d canonical=%d correct=%t\n", a.RecordedAttempts, a.CanonicalKeys, a.Correct)
	return r
}

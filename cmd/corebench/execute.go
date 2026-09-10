package main

import (
	"context"
	"crypto/aes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"time"

	"gigaquizz/internal/votecore"
)

func execute(ctx context.Context, c config) (r report) {
	started := time.Now()
	defer func() { r.WallSeconds = time.Since(started).Seconds() }()
	r.Mode = "NON-DURABLE CPU-only"
	r.Durable = false
	r.Errors = []string{}
	r.Limitations = []string{
		"Accepted means exact deduplication and counting in this Go process's RAM only. This is NOT an HTTP, Kafka, disk or durable acknowledgement benchmark.",
		"Process or machine failure loses all core votes. Private replay metadata is a synthetic test artifact, not a durable voting log or recovery mechanism.",
		"The generator, AES token creation, timing, shard routing and vote core share this process and computer; their costs are included together.",
		"A uniform monotonic schedule is served with bounded coalescing; slots are never intentionally dispatched early. Generator omissions and closed requests remain unserved planned attempts.",
		"Latency is sampled by random AES-token bits and represented by bounded approximate histograms; reported percentiles are not exact population percentiles.",
		"Per-shard capacity can reject under skew even if total capacity remains available; capacity_exceeded is reported separately.",
	}
	var seed, pollID [16]byte
	if _, err := rand.Read(seed[:]); err != nil {
		r.Errors = append(r.Errors, "cannot generate private AES seed")
		return r
	}
	if _, err := rand.Read(pollID[:]); err != nil {
		r.Errors = append(r.Errors, "cannot generate poll ID")
		return r
	}
	bitmap := make([]byte, (c.keys()+3)/4)
	if err := artifactGuard(".local/corebench", int64(len(bitmap))); err != nil {
		r.Errors = append(r.Errors, err.Error())
		return r
	}
	startsAt := time.Now().Add(10 * time.Second)
	cfg := votecore.Config{PollID: pollID, StartsAt: startsAt, EndsAt: startsAt.Add(pollWindow), AllowedMask: 3, Multiple: false, Shards: c.shards, Capacity: c.capacity()}
	r.Configuration = map[string]any{"unique_key_rate_per_second": c.rate, "planned_unique_keys": c.keys(), "planned_attempts": c.attempts(), "poll_window_seconds": 60, "workers": c.workers, "shards": c.shards, "capacity": cfg.Capacity, "capacity_rule": "max(ceil(planned_unique_keys * 1.05), shards * 1024)", "max_lag_ms": float64(c.maxLag) / 1e6, "buffer_inputs_per_worker": c.bufferSize, "coalescing_us": float64(c.coalesce) / 1000, "repeat_every": c.repeatEvery, "repeat_choice": 2, "sampling_denominator": c.sampleEvery(), "expected_choice_bitmap_bytes": len(bitmap), "starts_at": cfg.StartsAt.UTC(), "ends_at": cfg.EndsAt.UTC(), "token_generation": "AES-128 permutation of distinct 128-bit counters with a random private key; full 128-bit tokens"}
	engine, err := votecore.New(cfg)
	if err != nil {
		r.Errors = append(r.Errors, fmt.Sprintf("initialize volatile core: %v", err))
		return r
	}
	if time.Until(startsAt) < 250*time.Millisecond {
		r.Errors = append(r.Errors, "core allocation consumed the 10-second start lead; no workload scheduled")
		return r
	}
	fmt.Fprintf(os.Stderr, "corebench: ready; %d unique keys scheduled from %s for 60s (volatile RAM only)\n", c.keys(), startsAt.UTC().Format(time.RFC3339Nano))
	var memoryBefore, memoryAfter runtime.MemStats
	runtime.ReadMemStats(&memoryBefore)
	cipher, _ := aes.NewCipher(seed[:])
	var probe [16]byte
	var zero [16]byte
	cipher.Encrypt(probe[:], zero[:])
	r.BoundaryChecks = map[string]string{}
	before := engine.Submit(votecore.Input{Token: probe, Choice: 1})
	r.BoundaryChecks["before_start"] = statusName(before.Status)
	if before.Status != votecore.NotOpen {
		r.Errors = append(r.Errors, "before-start core probe did not return not_open")
	}
	work := runWorkload(ctx, c, startsAt, seed, bitmap, engine.Submit)
	runtime.ReadMemStats(&memoryAfter)
	r.Runtime = map[string]any{"scope": "Combined generator and core; counters from after map allocation until workload completion, including start wait and excluding audit", "gomaxprocs": runtime.GOMAXPROCS(0), "heap_alloc_before_bytes": memoryBefore.HeapAlloc, "heap_alloc_after_bytes": memoryAfter.HeapAlloc, "heap_inuse_after_bytes": memoryAfter.HeapInuse, "allocated_bytes": memoryAfter.TotalAlloc - memoryBefore.TotalAlloc, "allocations": memoryAfter.Mallocs - memoryBefore.Mallocs, "gc_cycles": memoryAfter.NumGC - memoryBefore.NumGC, "gc_stop_the_world_pause_ns": memoryAfter.PauseTotalNs - memoryBefore.PauseTotalNs}
	fmt.Fprintf(os.Stderr, "corebench: workload complete; accepted=%d duplicate=%d skipped=%d closed=%d; sealing and auditing full keys\n", work.Counts.Accepted, work.Counts.Duplicate, work.Counts.Skipped, work.Counts.Closed)
	r.Workload = &work
	if work.WrongChoice != 0 || work.DoubleAccept != 0 || work.UnexpectedDuplicate != 0 {
		r.Errors = append(r.Errors, "volatile submission response invariant failed")
	}
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	snapshot, err := sealAfterDeadline(ctx, timer, cfg.EndsAt, engine.Seal)
	if err != nil {
		r.Errors = append(r.Errors, fmt.Sprintf("seal volatile core: %v", err))
		return r
	}
	after := engine.Submit(votecore.Input{Token: probe, Choice: 1})
	r.BoundaryChecks["after_closed_new_token"] = statusName(after.Status)
	if after.Status != votecore.Closed {
		r.Errors = append(r.Errors, "after-close new-token probe did not return closed")
	}
	var counter [16]byte
	binary.BigEndian.PutUint64(counter[8:], 1)
	cipher.Encrypt(probe[:], counter[:])
	after = engine.Submit(votecore.Input{Token: probe, Choice: 2})
	r.BoundaryChecks["after_closed_previously_scheduled_token"] = statusName(after.Status)
	if after.Status != votecore.Closed {
		r.Errors = append(r.Errors, "after-close scheduled-token probe did not return closed")
	}
	audit := reconcile(ctx, c, seed, bitmap, work.Counts.Accepted, snapshot, engine.Lookup)
	r.Audit = &audit
	fmt.Fprintf(os.Stderr, "corebench: audit complete; checked=%d correct=%t duration=%.3fs\n", audit.CheckedKeys, audit.Correct, audit.WallSeconds)
	if !audit.Correct {
		r.Errors = append(r.Errors, "full regenerated-key reconciliation failed")
	}
	path, err := saveArtifacts(".local/corebench", seed, cfg, bitmap, c, work, snapshot)
	r.PrivateManifest = path
	if err != nil {
		r.Errors = append(r.Errors, err.Error())
	}
	return r
}

func sealAfterDeadline(ctx context.Context, timer *time.Timer, end time.Time, seal func() (votecore.Snapshot, error)) (votecore.Snapshot, error) {
	for {
		// The core's authoritative deadline is wall time. Strip the monotonic
		// component here: the load schedule and wall clock can drift apart.
		if !waitUntil(ctx, timer, end.Round(0)) {
			return votecore.Snapshot{}, ctx.Err()
		}
		snapshot, err := seal()
		if !errors.Is(err, votecore.ErrStillOpen) {
			return snapshot, err
		}
		// A wall-clock correction between waiting and Seal is also possible.
	}
}

func statusName(s votecore.Status) string {
	switch s {
	case votecore.Accepted:
		return "accepted_volatile"
	case votecore.Duplicate:
		return "duplicate"
	case votecore.Closed:
		return "closed"
	case votecore.NotOpen:
		return "not_open"
	case votecore.Invalid:
		return "invalid"
	case votecore.CapacityExceeded:
		return "capacity_exceeded"
	default:
		return "unknown_status"
	}
}

type auditReport struct {
	Correct           bool       `json:"correct"`
	Closed            bool       `json:"closed"`
	AcceptedResponses uint64     `json:"accepted_responses"`
	CheckedKeys       uint64     `json:"full_keys_regenerated_and_checked"`
	Missing           uint64     `json:"missing_expected_keys"`
	Wrong             uint64     `json:"wrong_choices"`
	ExpectedCounts    [32]uint64 `json:"expected_choice_counts"`
	SnapshotTotal     uint64     `json:"snapshot_total"`
	SnapshotCounts    [32]uint64 `json:"snapshot_choice_counts"`
	WallSeconds       float64    `json:"wall_seconds"`
	Method            string     `json:"method"`
}
type lookup func([16]byte) (uint32, bool)

func reconcile(ctx context.Context, c config, seed [16]byte, bitmap []byte, accepted uint64, snapshot votecore.Snapshot, lookup lookup) auditReport {
	start := time.Now()
	parts := make([]auditReport, c.workers)
	var wg sync.WaitGroup
	for worker := range c.workers {
		wg.Go(func() {
			cipher, _ := aes.NewCipher(seed[:])
			var counter, token [16]byte
			r := &parts[worker]
			for base := uint64(worker * c.bufferSize); base < c.keys(); base += uint64(c.workers * c.bufferSize) {
				if ctx.Err() != nil {
					return
				}
				end := min(c.keys(), base+uint64(c.bufferSize))
				for key := base; key < end; key++ {
					expected := getChoice(bitmap, key)
					if expected == 0 {
						continue
					}
					binary.BigEndian.PutUint64(counter[8:], key+1)
					cipher.Encrypt(token[:], counter[:])
					choice, exists := lookup(token)
					r.CheckedKeys++
					if !exists {
						r.Missing++
					} else if choice != expected {
						r.Wrong++
					}
					if expected == 1 {
						r.ExpectedCounts[0]++
					} else if expected == 2 {
						r.ExpectedCounts[1]++
					} else {
						r.Wrong++
					}
				}
			}
		})
	}
	wg.Wait()
	r := auditReport{AcceptedResponses: accepted, Closed: snapshot.Closed, SnapshotTotal: snapshot.Total, SnapshotCounts: snapshot.Counts, Method: "After irreversible Seal, regenerate every key marked accepted in a two-bit expected-choice bitmap using the private AES seed; compare the full 128-bit token via Lookup and exact first choice. Independently compare all 32 snapshot counts and total. No probabilistic deduplication and no token sample is used for this audit."}
	for _, part := range parts {
		r.CheckedKeys += part.CheckedKeys
		r.Missing += part.Missing
		r.Wrong += part.Wrong
		for bit, count := range part.ExpectedCounts {
			r.ExpectedCounts[bit] += count
		}
	}
	r.Correct = ctx.Err() == nil && r.Closed && r.Missing == 0 && r.Wrong == 0 && r.CheckedKeys == accepted && r.SnapshotTotal == accepted && r.ExpectedCounts == r.SnapshotCounts
	r.WallSeconds = time.Since(start).Seconds()
	return r
}

func artifactGuard(directory string, expectedBytes int64) error {
	if err := os.MkdirAll(directory, 0700); err != nil {
		return fmt.Errorf("cannot create private replay directory")
	}
	files, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("cannot inspect private replay directory")
	}
	var bytes int64
	for _, file := range files {
		info, err := file.Info()
		if err != nil {
			return fmt.Errorf("cannot inspect private replay artifact")
		}
		if info.Mode().IsRegular() {
			bytes += info.Size()
		}
	}
	if len(files)+2 > 64 || bytes+expectedBytes+65536 > 512<<20 {
		return fmt.Errorf("private replay artifact bound reached: 64 files/512MiB; prior artifacts preserved")
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(directory, &fs); err != nil {
		return fmt.Errorf("cannot inspect replay artifact disk space")
	}
	if uint64(fs.Bavail)*uint64(fs.Bsize) < uint64(expectedBytes)+(2<<30) {
		return fmt.Errorf("need bitmap size plus 2GiB free for private replay artifacts")
	}
	return nil
}

func saveArtifacts(directory string, seed [16]byte, cfg votecore.Config, bitmap []byte, c config, w workloadReport, snapshot votecore.Snapshot) (string, error) {
	if err := artifactGuard(directory, int64(len(bitmap))); err != nil {
		return "", err
	}
	prefix := "corebench_" + hex.EncodeToString(cfg.PollID[:8])
	bitsPath := filepath.Join(directory, prefix+".outcomes")
	manifestPath := filepath.Join(directory, prefix+".json")
	write := func(path string, b []byte) error {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return err
		}
		_, err = f.Write(b)
		if err == nil {
			err = f.Sync()
		}
		closeErr := f.Close()
		if err != nil {
			return err
		}
		return closeErr
	}
	if err := write(bitsPath, bitmap); err != nil {
		return "", fmt.Errorf("cannot write private expected-choice bitmap")
	}
	hash := sha256.Sum256(bitmap)
	manifest := map[string]any{"version": 1, "non_durable_cpu_only": true, "poll_id_hex": hex.EncodeToString(cfg.PollID[:]), "aes_seed_hex": hex.EncodeToString(seed[:]), "starts_at": cfg.StartsAt.UTC(), "ends_at": cfg.EndsAt.UTC(), "rate": c.rate, "keys": c.keys(), "attempts": c.attempts(), "repeat_every": c.repeatEvery, "bitmap_file": bitsPath, "bitmap_bytes": len(bitmap), "bitmap_sha256": hex.EncodeToString(hash[:]), "bitmap_encoding": "two bits per zero-based logical key, low bits first: 0 no accepted response, 1 expected choice1, 2 expected choice2", "counts": w.Counts, "snapshot": snapshot, "warning": "Contains synthetic generation seed only; does not persist or recover the volatile engine and is not a durable voting log."}
	b, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", fmt.Errorf("cannot encode replay manifest")
	}
	if err := write(manifestPath, b); err != nil {
		return "", fmt.Errorf("cannot write private replay manifest")
	}
	return manifestPath, nil
}

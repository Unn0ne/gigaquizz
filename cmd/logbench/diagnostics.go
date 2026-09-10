package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"
)

// These counters cover the combined generator and service process. The sampled
// interval includes waiting for the scheduled start, drain and latency summary,
// but excludes journal replay/reconciliation.
type workloadDiagnostics struct {
	CPUProfile       string    `json:"cpu_profile,omitempty"`
	StartedAt        time.Time `json:"started_at"`
	FinishedAt       time.Time `json:"finished_at"`
	WallSeconds      float64   `json:"wall_seconds"`
	AllocatedBytes   uint64    `json:"allocated_bytes"`
	Allocations      uint64    `json:"allocations"`
	GCCycles         uint32    `json:"gc_cycles"`
	GCPauseNS        uint64    `json:"gc_stop_the_world_pause_ns"`
	HeapAllocBefore  uint64    `json:"heap_alloc_before_bytes"`
	HeapAllocAfter   uint64    `json:"heap_alloc_after_bytes"`
	HeapInuseAfter   uint64    `json:"heap_inuse_after_bytes"`
	GoroutinesBefore int       `json:"goroutines_before"`
	GoroutinesAfter  int       `json:"goroutines_after"`
}

func startWorkloadDiagnostics() (func() workloadDiagnostics, error) {
	path := os.Getenv("LOGBENCH_CPU_PROFILE")
	var file *os.File
	if path != "" {
		// A diagnostic run may create one fresh local artifact. Never overwrite
		// another profile or write to a supplied external/parent directory.
		if filepath.Clean(path) != path || filepath.Dir(path) != ".local/kafka-lab/profiles" || filepath.Ext(path) != ".pprof" {
			return nil, fmt.Errorf("CPU profile must be a fresh .pprof file in .local/kafka-lab/profiles")
		}
		var err error
		file, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return nil, fmt.Errorf("create CPU profile: %w", err)
		}
		if err := pprof.StartCPUProfile(file); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("start CPU profile: %w", err)
		}
	}
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	started := time.Now()
	gr := runtime.NumGoroutine()
	return func() workloadDiagnostics {
		finished := time.Now()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		if file != nil {
			pprof.StopCPUProfile()
			_ = file.Close()
		}
		return workloadDiagnostics{CPUProfile: path, StartedAt: started.UTC(), FinishedAt: finished.UTC(), WallSeconds: finished.Sub(started).Seconds(), AllocatedBytes: after.TotalAlloc - before.TotalAlloc, Allocations: after.Mallocs - before.Mallocs, GCCycles: after.NumGC - before.NumGC, GCPauseNS: after.PauseTotalNs - before.PauseTotalNs, HeapAllocBefore: before.HeapAlloc, HeapAllocAfter: after.HeapAlloc, HeapInuseAfter: after.HeapInuse, GoroutinesBefore: gr, GoroutinesAfter: runtime.NumGoroutine()}
	}, nil
}

// Package runtimeconfig applies runtime settings loaded from an application env
// file. Go reads its own environment before main, so os.Setenv alone is too late.
package runtimeconfig

import (
	"errors"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
	"strings"
)

type Config struct {
	maxProcs    int
	memoryLimit *int64
	gcPercent   *int
}

// Load validates every requested setting before Apply changes any runtime state.
// Empty settings preserve Go's current defaults, including cgroup CPU updates.
func Load() (Config, error) {
	var c Config
	if raw := os.Getenv("GOMAXPROCS"); raw != "" {
		n, err := strconv.Atoi(raw)
		// A mistyped override must not allocate millions of runtime processors.
		if err != nil || n < 1 || n > 1024 {
			return c, errors.New("GOMAXPROCS must be an integer from 1 to 1024, or omitted for automatic CPU selection")
		}
		c.maxProcs = n
	}
	if raw := os.Getenv("GOMEMLIMIT"); raw != "" {
		n, err := memoryBytes(raw)
		if err != nil {
			return c, errors.New("GOMEMLIMIT must be nonnegative bytes with optional B/KiB/MiB/GiB/TiB suffix, or off")
		}
		c.memoryLimit = &n
	}
	if raw := os.Getenv("GOGC"); raw != "" {
		n := -1
		if raw != "off" {
			parsed, err := strconv.ParseInt(raw, 10, 32)
			if err != nil || parsed < 0 {
				return c, errors.New("GOGC must be a nonnegative integer or off")
			}
			n = int(parsed)
		}
		c.gcPercent = &n
	}
	return c, nil
}

func memoryBytes(raw string) (int64, error) {
	if raw == "off" {
		return math.MaxInt64, nil
	}
	multiplier := int64(1)
	for _, unit := range []struct {
		suffix string
		scale  int64
	}{{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"TiB", 1 << 40}, {"B", 1}} {
		if strings.HasSuffix(raw, unit.suffix) {
			raw = strings.TrimSuffix(raw, unit.suffix)
			multiplier = unit.scale
			break
		}
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 || n > math.MaxInt64/multiplier {
		return 0, errors.New("invalid memory limit")
	}
	return n * multiplier, nil
}

// Apply changes only explicitly configured settings. In particular it never
// calls GOMAXPROCS when the operator wants Go to follow the host/cgroup defaults.
func (c Config) Apply() {
	if c.maxProcs != 0 {
		runtime.GOMAXPROCS(c.maxProcs)
	}
	if c.memoryLimit != nil {
		debug.SetMemoryLimit(*c.memoryLimit)
	}
	if c.gcPercent != nil {
		debug.SetGCPercent(*c.gcPercent)
	}
}

// State reports effective settings, not a claim about host capacity or RSS.
type State struct {
	GoVersion        string `json:"go_version"`
	OS               string `json:"os"`
	Arch             string `json:"arch"`
	MaxProcs         int    `json:"gomaxprocs"`
	MemoryLimitBytes int64  `json:"gomemlimit_bytes"`
	GCPercent        string `json:"gogc"`
}

func Current() State {
	samples := []metrics.Sample{{Name: "/gc/gogc:percent"}}
	metrics.Read(samples)
	gc := samples[0].Value.Uint64()
	gcText := strconv.FormatUint(gc, 10)
	if gc == math.MaxUint64 {
		gcText = "off"
	}
	return State{GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH,
		MaxProcs: runtime.GOMAXPROCS(0), MemoryLimitBytes: debug.SetMemoryLimit(-1), GCPercent: gcText}
}

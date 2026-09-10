// Command corebench exercises the volatile vote core without HTTP or Kafka.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	maximumRate     = 3_000_000
	maximumAttempts = 180_000_000
	pollWindow      = time.Minute
)

type config struct {
	rate, workers, shards, bufferSize, repeatEvery int
	allowLarge                                     bool
	maxLag, coalesce                               time.Duration
}

func defaults() config {
	return config{rate: 100, workers: 8, shards: 256, bufferSize: 256, maxLag: 20 * time.Millisecond, coalesce: 250 * time.Microsecond}
}

func main() {
	c := defaults()
	flag.IntVar(&c.rate, "rate", c.rate, "uniformly scheduled unique keys/s; maximum 3000000; poll is always 60 seconds")
	flag.IntVar(&c.workers, "workers", c.workers, "independent streaming generators, 1..16")
	flag.IntVar(&c.shards, "shards", c.shards, "exact volatile deduplication shards: 256 or 512")
	flag.IntVar(&c.bufferSize, "buffer-size", c.bufferSize, "AES input buffer per worker, multiple of 4 in 4..1024")
	flag.IntVar(&c.repeatEvery, "repeat-every", 0, "one sequential extra choice-2 attempt each Nth key; 0 disables")
	flag.DurationVar(&c.maxLag, "max-lag", c.maxLag, "skip before dispatch when this far behind original schedule; maximum 100ms")
	flag.DurationVar(&c.coalesce, "coalesce", c.coalesce, "group already due slots for timer efficiency; 100us..1ms; no slot is sent early")
	flag.BoolVar(&c.allowLarge, "allow-large", false, "explicitly allow >1000 average attempts/s or >10000 planned attempts")
	flag.Parse()
	if flag.NArg() != 0 {
		fatal("unexpected positional arguments")
	}
	if err := c.validate(); err != nil {
		fatal(err.Error())
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	r := execute(ctx, c)
	e := json.NewEncoder(os.Stdout)
	e.SetIndent("", "  ")
	if err := e.Encode(r); err != nil {
		fatal("cannot write aggregate report")
	}
	if len(r.Errors) > 0 {
		os.Exit(1)
	}
}

func fatal(message string) { fmt.Fprintln(os.Stderr, "corebench:", message); os.Exit(2) }

func (c config) keys() uint64 { return uint64(c.rate) * 60 }
func (c config) attempts() uint64 {
	n := c.keys()
	if c.repeatEvery > 0 {
		n += n / uint64(c.repeatEvery)
	}
	return n
}
func (c config) capacity() uint64 { return max((c.keys()*105+99)/100, uint64(c.shards)*1024) }
func (c config) sampleEvery() uint64 {
	if c.attempts() <= 10000 {
		return 1
	}
	return 1024
}
func (c config) groupSize() int {
	return min(c.bufferSize, max(1, int(int64(c.rate)*int64(c.coalesce)/int64(time.Second))))
}

func (c config) validate() error {
	if c.rate < 1 || c.rate > maximumRate {
		return fmt.Errorf("rate must be 1..%d", maximumRate)
	}
	if c.workers < 1 || c.workers > 16 || c.shards != 256 && c.shards != 512 {
		return fmt.Errorf("workers must be 1..16; shards must be 256 or 512")
	}
	if c.bufferSize < 4 || c.bufferSize > 1024 || c.bufferSize%4 != 0 {
		return fmt.Errorf("buffer-size must be a multiple of 4 in 4..1024")
	}
	if c.repeatEvery < 0 || c.attempts() > maximumAttempts {
		return fmt.Errorf("repeat-every must be nonnegative; maximum %d total attempts includes repeats", maximumAttempts)
	}
	if c.maxLag <= 0 || c.maxLag > 100*time.Millisecond || c.coalesce < 100*time.Microsecond || c.coalesce > time.Millisecond {
		return fmt.Errorf("max-lag must be (0,100ms]; coalesce must be [100us,1ms]")
	}
	averageAttempts := uint64(c.rate)
	if c.repeatEvery > 0 {
		averageAttempts += (uint64(c.rate) + uint64(c.repeatEvery) - 1) / uint64(c.repeatEvery)
	}
	if !c.allowLarge && (averageAttempts > 1000 || c.attempts() > 10000) {
		return fmt.Errorf("requested load requires -allow-large; hard bounds remain in force")
	}
	return nil
}

type report struct {
	Mode            string            `json:"mode"`
	Durable         bool              `json:"durable"`
	Configuration   map[string]any    `json:"configuration,omitempty"`
	Workload        *workloadReport   `json:"workload,omitempty"`
	Runtime         map[string]any    `json:"runtime_diagnostics,omitempty"`
	Audit           *auditReport      `json:"reconciliation,omitempty"`
	BoundaryChecks  map[string]string `json:"boundary_checks,omitempty"`
	PrivateManifest string            `json:"private_replay_manifest,omitempty"`
	WallSeconds     float64           `json:"wall_seconds"`
	Errors          []string          `json:"errors"`
	Limitations     []string          `json:"limitations"`
}

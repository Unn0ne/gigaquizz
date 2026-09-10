// Command framebench measures a bounded replicated compact-frame workload.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const maximumRate = 1_800_000
const maximumAttempts uint64 = 120_000_000
const maximumFrames uint64 = 1_000_000

type config struct {
	brokers, auditOnly                                       string
	rate, workers, queue, frameSize, partitions, repeatEvery int
	allowLarge                                               bool
	maxLag, frameAge, timeout, drain                         time.Duration
}

func defaults() config {
	brokers := os.Getenv("FRAMEBENCH_BROKERS")
	if brokers == "" {
		brokers = "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094"
	}
	return config{brokers: brokers, rate: 1000, workers: 64, queue: 128, frameSize: 4096, partitions: 8, maxLag: 100 * time.Millisecond, frameAge: 20 * time.Millisecond, timeout: 15 * time.Second, drain: 20 * time.Second}
}
func (c config) keys() uint64 { return uint64(c.rate) * 60 }
func (c config) attempts() uint64 {
	n := c.keys()
	if c.repeatEvery > 0 {
		n += n / uint64(c.repeatEvery)
	}
	return n
}
func (c config) frameBound() uint64 {
	return min(c.attempts(), (c.attempts()+uint64(c.frameSize)-1)/uint64(c.frameSize)+uint64(c.partitions)*(uint64((60*time.Second+c.maxLag)/c.frameAge)+2))
}
func (c config) validate() error {
	if _, err := localBrokers(c.brokers); err != nil {
		return err
	}
	if c.rate < 1 || c.rate > maximumRate || c.repeatEvery < 0 {
		return fmt.Errorf("rate must be 1..1800000 and repeat-every nonnegative")
	}
	if c.attempts() > maximumAttempts {
		return fmt.Errorf("hard cap120000000 attempts includes repeats")
	}
	if c.workers < 1 || c.workers > 256 || c.queue < 1 || c.queue > 512 || c.frameSize < 1 || c.frameSize > 4096 || c.partitions < 1 || c.partitions > 32 {
		return fmt.Errorf("workers1..256, queue1..512 frames, frame-size1..4096, partitions1..32")
	}
	if c.maxLag <= 0 || c.maxLag > time.Second || c.frameAge < time.Millisecond || c.frameAge > 100*time.Millisecond || c.timeout <= 0 || c.timeout > 15*time.Second || c.drain <= 0 || c.drain > 20*time.Second {
		return fmt.Errorf("max-lag (0,1s], frame-age[1ms,100ms], timeout(0,15s], drain(0,20s]")
	}
	if c.frameBound() > maximumFrames {
		return fmt.Errorf("configuration can exceed bounded1000000-frame client metadata; increase frame size/age")
	}
	rps := uint64(c.rate)
	if c.repeatEvery > 0 {
		rps += (uint64(c.rate) + uint64(c.repeatEvery) - 1) / uint64(c.repeatEvery)
	}
	if !c.allowLarge && c.auditOnly == "" && (rps > 1000 || c.attempts() > 10000) {
		return fmt.Errorf("requested full-minute workload requires -allow-large")
	}
	return nil
}
func localBrokers(raw string) ([]string, error) {
	x := strings.Split(raw, ",")
	if len(x) < 1 || len(x) > 3 {
		return nil, fmt.Errorf("provide1..3 numeric loopback brokers")
	}
	for i, a := range x {
		a = strings.TrimSpace(a)
		host, port, err := net.SplitHostPort(a)
		if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
			return nil, fmt.Errorf("only numeric loopback broker addresses are supported")
		}
		if n, err := net.LookupPort("tcp", port); err != nil || n < 1 {
			return nil, fmt.Errorf("invalid broker port")
		}
		x[i] = a
	}
	return x, nil
}
func main() {
	c := defaults()
	flag.StringVar(&c.brokers, "brokers", c.brokers, "numeric loopback brokers; FRAMEBENCH_BROKERS")
	flag.IntVar(&c.rate, "rate", c.rate, "uniform unique keys/s for60s; repeats are additional attempts; maximum1800000")
	flag.IntVar(&c.workers, "workers", c.workers, "bounded concurrent frame calls, maximum256")
	flag.IntVar(&c.queue, "queue", c.queue, "bounded waiting frames, maximum512")
	flag.IntVar(&c.frameSize, "frame-size", c.frameSize, "maximum attempts per same-partition frame, maximum4096")
	flag.IntVar(&c.partitions, "partitions", c.partitions, "fresh topic partitions,1..32")
	flag.IntVar(&c.repeatEvery, "repeat-every", 0, "extra same-token choice2 attempt each Nth logical key")
	flag.DurationVar(&c.maxLag, "max-lag", c.maxLag, "skip dispatch this far behind original schedule, maximum1s")
	flag.DurationVar(&c.frameAge, "frame-age", c.frameAge, "flush incomplete frame after this age,1..100ms")
	flag.DurationVar(&c.timeout, "timeout", c.timeout, "client frame timeout, maximum15s; server transaction budget30s")
	flag.DurationVar(&c.drain, "drain-timeout", c.drain, "extra workload drain time, maximum20s")
	flag.BoolVar(&c.allowLarge, "allow-large", false, "opt in above1000 average attempts/s or10000 planned attempts")
	flag.StringVar(&c.auditOnly, "audit-only", "", "read-only streaming reconciliation from a saved private manifest")
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
func fatal(s string) { fmt.Fprintln(os.Stderr, "framebench:", s); os.Exit(2) }

type report struct {
	Mode          string            `json:"mode"`
	Configuration map[string]any    `json:"configuration,omitempty"`
	Workload      *workloadReport   `json:"workload,omitempty"`
	Audit         *auditReport      `json:"reconciliation,omitempty"`
	StoreMetrics  map[string]uint64 `json:"store_metrics,omitempty"`
	Runtime       map[string]any    `json:"runtime_diagnostics,omitempty"`
	Manifest      string            `json:"private_manifest,omitempty"`
	WallSeconds   float64           `json:"wall_seconds"`
	Errors        []string          `json:"errors"`
	Limitations   []string          `json:"limitations"`
}

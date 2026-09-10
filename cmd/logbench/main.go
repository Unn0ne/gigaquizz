// Command logbench measures the experimental replicated vote journal locally.
// Its 202 receipts acknowledge attempts, not canonical unique votes.
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

const (
	maximumAttempts = 12_000_000
	maximumRate     = 200_000
)

type config struct {
	mode, brokers, auditOnly, recoverAndSeal string
	rate, workers, queue, repeatEvery        int
	different, allowLarge                    bool
	duration, timeout, drain, maxLag, linger time.Duration
	transactionTimeout                       time.Duration
	partitions, batchSize, partitionQueue    int
}

func defaults() config {
	brokers := os.Getenv("LOGBENCH_BROKERS")
	if brokers == "" {
		brokers = "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094"
	}
	return config{mode: "http", brokers: brokers, rate: 100, workers: 256, queue: 8192, duration: 60 * time.Second, timeout: 10 * time.Second, drain: 15 * time.Second, maxLag: time.Second, linger: 5 * time.Millisecond, transactionTimeout: 10 * time.Second, partitions: 8, batchSize: 1000, partitionQueue: 8192}
}

func main() {
	c := defaults()
	flag.StringVar(&c.mode, "mode", c.mode, "http (real loopback handler, no TLS/CDN) or direct (Store.Submit)")
	flag.StringVar(&c.brokers, "brokers", c.brokers, "numeric loopback broker addresses; also LOGBENCH_BROKERS")
	flag.IntVar(&c.rate, "rate", c.rate, fmt.Sprintf("scheduled unique keys/s, maximum %d; repeats add separate attempts", maximumRate))
	flag.DurationVar(&c.duration, "duration", c.duration, "arrival window <=60s; poll always remains open exactly 60s")
	flag.IntVar(&c.workers, "workers", c.workers, "bounded in-flight attempts, maximum 8192")
	flag.IntVar(&c.queue, "queue", c.queue, "bounded generator waiting queue, maximum 100000")
	flag.IntVar(&c.repeatEvery, "repeat-every", 0, "one extra attempt for each Nth scheduled key; 0 disables")
	flag.BoolVar(&c.different, "different-choice", false, "repeat selects bit 2 instead of bit 1")
	flag.BoolVar(&c.allowLarge, "allow-large", false, "allow >1000 maximum average attempts/s or >10000 total attempts")
	flag.DurationVar(&c.timeout, "timeout", c.timeout, "per-attempt timeout, maximum 15s")
	flag.DurationVar(&c.transactionTimeout, "transaction-timeout", c.transactionTimeout, "writer transaction timeout, 1..30s; independent from client timeout and poll admission deadline")
	flag.DurationVar(&c.drain, "drain-timeout", c.drain, "extra workload drain time, maximum 20s")
	flag.DurationVar(&c.maxLag, "max-lag", c.maxLag, "skip before dispatch once this far behind schedule, maximum 10s; poll admission still closes after 60s")
	flag.DurationVar(&c.linger, "linger", c.linger, "journal writer batch wait, maximum 100ms")
	flag.IntVar(&c.partitions, "partitions", c.partitions, "fresh topic partitions, maximum 64")
	flag.IntVar(&c.batchSize, "batch-size", c.batchSize, "maximum records per writer transaction, maximum 4096")
	flag.IntVar(&c.partitionQueue, "partition-queue", c.partitionQueue, "bounded queue per journal partition, maximum 8192")
	flag.StringVar(&c.auditOnly, "audit-only", "", "read-only re-audit of a private binary ledger after broker restart")
	flag.StringVar(&c.recoverAndSeal, "recover-and-seal", "", "explicit ownership transfer: previous owner must be stopped; recover owned ledger, seal and audit (mutates local journal)")
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
		fatal("cannot write JSON report")
	}
	if len(r.Errors) > 0 {
		os.Exit(1)
	}
}

func fatal(s string) { fmt.Fprintln(os.Stderr, "logbench:", s); os.Exit(2) }

func (c config) validate() error {
	if c.auditOnly != "" && c.recoverAndSeal != "" {
		return fmt.Errorf("audit-only and recover-and-seal are mutually exclusive")
	}
	if c.mode != "http" && c.mode != "direct" {
		return fmt.Errorf("mode must be http or direct")
	}
	if _, err := localBrokers(c.brokers); err != nil {
		return err
	}
	if c.rate < 1 || c.rate > maximumRate || c.duration <= 0 || c.duration > 60*time.Second {
		return fmt.Errorf("rate must be 1..%d and duration (0,60s]", maximumRate)
	}
	if c.repeatEvery < 0 || c.different && c.repeatEvery == 0 {
		return fmt.Errorf("different-choice requires a positive repeat-every")
	}
	if c.attempts() > maximumAttempts {
		return fmt.Errorf("hard cap is %d total attempts, including repeats", maximumAttempts)
	}
	if c.workers < 1 || c.workers > 8192 || c.queue < 1 || c.queue > 100000 {
		return fmt.Errorf("workers must be 1..8192 and queue 1..100000")
	}
	if c.timeout <= 0 || c.timeout > 15*time.Second || c.drain <= 0 || c.drain > 20*time.Second || c.maxLag <= 0 || c.maxLag > 10*time.Second {
		return fmt.Errorf("timeout must be (0,15s], drain (0,20s], max-lag (0,10s]")
	}
	if c.transactionTimeout < time.Second || c.transactionTimeout > 30*time.Second {
		return fmt.Errorf("transaction-timeout must be [1s,30s]")
	}
	if c.partitions < 1 || c.partitions > 64 || c.batchSize < 1 || c.batchSize > 4096 || c.partitionQueue < 1 || c.partitionQueue > 8192 {
		return fmt.Errorf("invalid writer bounds: partitions 1..64, batch 1..4096, queue 1..8192")
	}
	if c.linger < time.Millisecond || c.linger > 100*time.Millisecond {
		return fmt.Errorf("linger must be [1ms,100ms]")
	}
	rps := c.rate
	if c.repeatEvery > 0 {
		rps += (c.rate + c.repeatEvery - 1) / c.repeatEvery
	}
	if !c.allowLarge && c.auditOnly == "" && c.recoverAndSeal == "" && (rps > 1000 || c.attempts() > 10000) {
		return fmt.Errorf("requested load requires -allow-large; hard bounds still apply")
	}
	return nil
}

func localBrokers(raw string) ([]string, error) {
	items := strings.Split(raw, ",")
	if len(items) < 1 || len(items) > 3 {
		return nil, fmt.Errorf("provide 1..3 loopback brokers")
	}
	for i, item := range items {
		item = strings.TrimSpace(item)
		host, port, err := net.SplitHostPort(item)
		if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() || port == "" {
			return nil, fmt.Errorf("brokers must be numeric loopback addresses with ports")
		}
		if n, err := net.LookupPort("tcp", port); err != nil || n < 1 {
			return nil, fmt.Errorf("invalid broker port")
		}
		items[i] = item
	}
	return items, nil
}

func (c config) keys() int {
	return int((int64(c.duration)*int64(c.rate) + int64(time.Second) - 1) / int64(time.Second))
}
func (c config) attempts() int {
	n := c.keys()
	if c.repeatEvery > 0 {
		n += n / c.repeatEvery
	}
	return n
}

type report struct {
	Mode           string               `json:"mode"`
	Topic          string               `json:"topic,omitempty"`
	Configuration  map[string]any       `json:"configuration,omitempty"`
	Workload       *workloadReport      `json:"workload,omitempty"`
	Audit          *auditReport         `json:"reconciliation,omitempty"`
	BoundaryChecks map[string]string    `json:"boundary_checks,omitempty"`
	StoreMetrics   map[string]uint64    `json:"store_metrics,omitempty"`
	Diagnostics    *workloadDiagnostics `json:"workload_diagnostics,omitempty"`
	Ledger         string               `json:"private_receipt_ledger,omitempty"`
	WallSeconds    float64              `json:"wall_seconds"`
	Errors         []string             `json:"errors"`
	Limitations    []string             `json:"limitations"`
}

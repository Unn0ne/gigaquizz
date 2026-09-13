// httpbench exercises the real, single-attempt public HTTP API. Its private
// ledgers permit a separate process to reconcile durable receipts without
// requiring journal positions in that API. It never manages application state.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"gigaquizz/internal/poll"
)

const (
	maxAttempts = 10000000
	maxRate     = 100000
	maxWorkers  = 4096
	ledgerBytes = 64
)

type config struct {
	URL         string        `json:"url"`
	PollID      string        `json:"poll_id"`
	Rate        uint64        `json:"unique_rate"`
	Unique      uint64        `json:"unique_count,omitempty"`
	KeyOffset   uint64        `json:"key_offset,omitempty"`
	Duration    time.Duration `json:"duration_ns"`
	Start       time.Time     `json:"start_at"`
	Workers     int           `json:"workers"`
	Queue       int           `json:"queue"`
	MaxLag      time.Duration `json:"max_lag_ns"`
	Timeout     time.Duration `json:"request_timeout_ns"`
	RepeatEvery uint64        `json:"repeat_every"`
	Journey     bool          `json:"journey,omitempty"`
	Definition  bool          `json:"definition,omitempty"`
	PlanFile    string        `json:"-"`
	Generator   int           `json:"-"`
	Directory   string        `json:"-"`
	Allow       bool          `json:"-"`
}

func (c config) keys() uint64 {
	if c.Unique != 0 {
		return c.Unique
	}
	return c.Rate * uint64(c.Duration/time.Second)
}
func (c config) scheduledOffset(key uint64) time.Duration {
	return time.Duration(key * uint64(c.Duration) / c.keys())
}
func (c config) attempts() uint64 {
	n := c.keys()
	if c.RepeatEvery != 0 {
		n += (c.KeyOffset+n)/c.RepeatEvery - c.KeyOffset/c.RepeatEvery
	}
	return n
}
func (c config) validate() error {
	if c.Definition && !c.Journey {
		return errors.New("definition requires journey mode")
	}
	if c.Duration != time.Minute || c.Rate < 1 || c.Rate > maxRate || c.keys() < 1 || c.keys() > maxRate*60 || c.KeyOffset > maxPlanKeys || c.keys() > maxPlanKeys-c.KeyOffset {
		return errors.New("invalid minute workload or global key range")
	}
	u, err := url.Parse(c.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return errors.New("url must be an HTTP(S) origin")
	}
	if _, err := parseID(c.PollID); err != nil {
		return err
	}
	if c.Rate < 1 || c.Rate > maxRate || c.Duration != time.Minute || c.attempts() > maxAttempts || c.Workers < 1 || c.Workers > maxWorkers || c.Queue < 1 || c.Queue > 1048576 || c.MaxLag < time.Millisecond || c.MaxLag > time.Second || c.Timeout < time.Millisecond || c.Timeout > 30*time.Second || c.Start.IsZero() || c.Start.Year() < 2020 || c.Start.Year() > 2200 {
		return errors.New("invalid workload bounds: minute duration, <=100k unique/s, <=10M attempts, <=4096 workers")
	}
	return nil
}

func parseID(s string) ([16]byte, error) {
	var id [16]byte
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' || s != strings.ToLower(s) {
		return id, errors.New("poll must be a lowercase UUID")
	}
	b, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	if err != nil || len(b) != 16 {
		return id, errors.New("invalid poll UUID")
	}
	copy(id[:], b)
	if id == [16]byte{} {
		return id, errors.New("zero poll UUID")
	}
	return id, nil
}

type privateManifest struct {
	Version    int          `json:"version"`
	Complete   bool         `json:"complete"`
	Config     config       `json:"config"`
	Poll       poll.Poll    `json:"poll"`
	Seed       [16]byte     `json:"seed"`
	Namespace  [8]byte      `json:"namespace"`
	PlanSHA256 string       `json:"plan_sha256,omitempty"`
	Files      []ledgerFile `json:"files"`
}

type ledgerFile struct {
	Name    string `json:"name"`
	Records uint64 `json:"records"`
	SHA256  string `json:"sha256"`
}

func main() {
	var c config
	var start, audit, fileJournal, kafkaConfig, results, preparePlan, auditPlan, ledgerRoot string
	var totalUnique uint64
	var generators int
	flag.StringVar(&c.URL, "url", "", "HTTP(S) origin; no credentials")
	flag.StringVar(&c.PollID, "poll", "", "existing isolated poll UUID")
	flag.Uint64Var(&c.Rate, "rate", 1000, "unique IDs per second (additional attempts use -repeat-every)")
	flag.Uint64Var(&c.Unique, "unique", 0, "exact unique population over the original minute; overrides rate, at most 6M per generator")
	flag.StringVar(&preparePlan, "prepare-plan", "", "write a new private distributed workload plan; never sends votes")
	flag.Uint64Var(&totalUnique, "total-unique", 100000000, "exact population for -prepare-plan, at most 120M")
	flag.IntVar(&generators, "generators", 20, "generator ranges in -prepare-plan, 1..128")
	flag.StringVar(&c.PlanFile, "plan", "", "private workload plan shared by all generators")
	flag.IntVar(&c.Generator, "generator", 0, "zero-based generator range in -plan")
	flag.StringVar(&auditPlan, "audit-plan", "", "audit all ranges of this private plan together; never sends HTTP")
	flag.StringVar(&ledgerRoot, "ledgers", "", "collected generator-000/manifest.json etc. for -audit-plan")
	flag.DurationVar(&c.Duration, "duration", time.Minute, "fixed original poll duration")
	flag.StringVar(&start, "start-at", "", "original poll starts_at, RFC3339Nano")
	flag.IntVar(&c.Workers, "workers", 128, "bounded concurrent HTTP workers (1..4096)")
	flag.IntVar(&c.Queue, "queue", 8192, "bounded scheduled attempt queue")
	flag.DurationVar(&c.MaxLag, "max-lag", 100*time.Millisecond, "skip rather than send a more stale scheduled attempt")
	flag.DurationVar(&c.Timeout, "request-timeout", 12*time.Second, "per-request deadline; errors remain unknown")
	flag.Uint64Var(&c.RepeatEvery, "repeat-every", 5, "one choice-2 attempt after each Nth choice-1 ID; 0 disables")
	flag.BoolVar(&c.Journey, "journey", false, "before each original POST, fetch HTML, CSS, two scripts and question sequentially; repeats remain POST-only")
	flag.BoolVar(&c.Definition, "definition", false, "journey uses the cacheable /definition endpoint; baseline journey remains unchanged")
	flag.StringVar(&c.Directory, "ledger-dir", "", "new private ledger directory; parent must already exist")
	flag.BoolVar(&c.Allow, "allow-high-load", false, "explicitly allow this bounded HTTP workload")
	flag.StringVar(&audit, "audit-only", "", "private completed manifest.json; never sends HTTP")
	flag.StringVar(&fileJournal, "file-journal", "", "files branch: closed journal directory")
	flag.StringVar(&kafkaConfig, "kafka-config", "", "Kafka branch: private JSON votelog.Config")
	flag.StringVar(&results, "results", "", "private saved service results JSON (raw or results envelope)")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var report any
	var err error
	modes := 0
	for _, value := range []string{audit, preparePlan, auditPlan, c.PlanFile} {
		if value != "" {
			modes++
		}
	}
	if flag.NArg() != 0 || modes > 1 {
		err = errors.New("unexpected arguments")
	} else if preparePlan != "" {
		c.Start, err = time.Parse(time.RFC3339Nano, start)
		if err == nil {
			report, err = prepareDistributedPlan(ctx, c, preparePlan, totalUnique, generators)
		}
	} else if auditPlan != "" {
		report, err = runPlanAudit(ctx, auditPlan, ledgerRoot, fileJournal, kafkaConfig, results)
	} else if audit != "" {
		report, err = runAudit(ctx, audit, fileJournal, kafkaConfig, results)
	} else {
		if c.PlanFile != "" {
			c, err = configFromPlan(c)
		} else {
			c.Start, err = time.Parse(time.RFC3339Nano, start)
		}
		if err == nil {
			err = c.validate()
		}
		if err == nil && (!c.Allow || c.Directory == "") {
			err = errors.New("workload requires -allow-high-load and a new -ledger-dir")
		}
		if err == nil {
			report, err = runWorkload(ctx, c)
		}
	}
	if report == nil {
		report = map[string]any{"complete": false, "error": "configuration or private I/O validation failed"}
	}
	if e := json.NewEncoder(os.Stdout).Encode(report); e != nil {
		os.Exit(1)
	}
	if err != nil {
		// All errors deliberately omit URLs, generated IDs, private paths and
		// storage error strings. Aggregated counters describe reconciliation.
		fmt.Fprintln(os.Stderr, "httpbench failed; inspect aggregate report and private run inputs")
		os.Exit(1)
	}
}

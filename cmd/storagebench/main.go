// Command storagebench runs a bounded workload in a disposable PostgreSQL schema.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const maximumKeys = 500_000

type config struct {
	total, rate, workers, queue int
	repeatEvery                 int
	different                   bool
	mode                        string
	timeout, drain, maxLag      time.Duration
	keepSchema, allowLarge      bool
	warmConnections             bool
	auditFile, auditOnly        string
	requiredStandbys            int
	standbyNames                string
}

func defaults() config {
	return config{total: 100, rate: 100, workers: 16, queue: 64, mode: "vote", timeout: 3 * time.Second, drain: 10 * time.Second, maxLag: 100 * time.Millisecond}
}

func main() {
	c := defaults()
	flag.IntVar(&c.total, "total", c.total, "logical keys (hard maximum 500000, large runs need -allow-large)")
	flag.IntVar(&c.rate, "rate", c.rate, "scheduled logical keys per second; total/rate must be <=55s")
	flag.IntVar(&c.workers, "workers", c.workers, "bounded concurrent database attempts")
	flag.IntVar(&c.queue, "queue", c.queue, "bounded waiting attempt queue")
	flag.IntVar(&c.repeatEvery, "repeat-every", 0, "schedule one concurrent repeat for every Nth key; 0 disables")
	flag.BoolVar(&c.different, "different-choice", false, "concurrent repeat chooses option 2 instead of option 1")
	flag.StringVar(&c.mode, "mode", "vote", "vote (real repository) or blind (lean INSERT control, not deduplication overhead comparison)")
	flag.DurationVar(&c.timeout, "timeout", c.timeout, "timeout of one database attempt, maximum 10s")
	flag.DurationVar(&c.drain, "drain-timeout", c.drain, "extra time to finish workload attempts, maximum 15s")
	flag.DurationVar(&c.maxLag, "max-lag", c.maxLag, "drop attempts whose initial dispatch is this late")
	flag.BoolVar(&c.keepSchema, "retain-schema", false, "retain fixture and a private local key ledger for later failover audit")
	flag.BoolVar(&c.allowLarge, "allow-large", false, "explicitly allow more than 10000 keys or 1000 scheduled database attempts/s")
	flag.BoolVar(&c.warmConnections, "warm-connections", false, "open and validate every repository connection before creating the poll; vote mode only")
	flag.StringVar(&c.auditFile, "audit-file", "", "optional new private JSON key ledger path; never printed to stdout as contents")
	flag.StringVar(&c.auditOnly, "audit-only", "", "read-only reconciliation of a previously retained fixture using its key ledger")
	standbysDefault := 0
	if value := os.Getenv("STORAGEBENCH_REQUIRED_STANDBYS"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil {
			die("invalid STORAGEBENCH_REQUIRED_STANDBYS")
		}
		standbysDefault = n
	}
	flag.IntVar(&c.requiredStandbys, "required-standbys", standbysDefault, "required durable standbys; also STORAGEBENCH_REQUIRED_STANDBYS")
	flag.StringVar(&c.standbyNames, "standby-names", os.Getenv("STORAGEBENCH_STANDBY_NAMES"), "comma-separated expected standby application names; also STORAGEBENCH_STANDBY_NAMES")
	flag.Parse()
	if flag.NArg() != 0 {
		die("unexpected positional arguments")
	}
	if err := c.validate(); err != nil {
		die(err.Error())
	}
	dsn := os.Getenv("STORAGEBENCH_DATABASE_URL")
	if dsn == "" {
		die("set STORAGEBENCH_DATABASE_URL; database URLs are accepted only through the environment")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	r := execute(ctx, c, dsn)
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(r); err != nil {
		die("cannot write JSON report")
	}
	if len(r.Errors) > 0 {
		os.Exit(1)
	}
}

func die(message string) { fmt.Fprintln(os.Stderr, "storagebench:", message); os.Exit(2) }

func (c config) validate() error {
	if c.total < 1 || c.total > maximumKeys || c.rate < 1 || c.rate > 200_000 || c.total > c.rate*55 {
		return fmt.Errorf("total must be 1..500000, rate 1..200000 and total/rate <=55 seconds")
	}
	if c.workers < 1 || c.workers > 256 || c.queue < 1 || c.queue > 8192 {
		return fmt.Errorf("workers must be 1..256 and queue 1..8192")
	}
	if c.timeout <= 0 || c.timeout > 10*time.Second || c.drain <= 0 || c.drain > 15*time.Second || c.maxLag <= 0 || c.maxLag > time.Second {
		return fmt.Errorf("timeout must be (0,10s], drain-timeout (0,15s], max-lag (0,1s]")
	}
	if c.repeatEvery < 0 || (c.different && c.repeatEvery == 0) {
		return fmt.Errorf("repeat-every must be nonnegative; different-choice requires repeats")
	}
	if c.mode != "vote" && c.mode != "blind" {
		return fmt.Errorf("mode must be vote or blind")
	}
	if c.mode == "blind" && c.repeatEvery != 0 {
		return fmt.Errorf("blind control only supports unique keys; use vote mode for repeat correctness")
	}
	if c.warmConnections && (c.mode == "blind" || c.auditOnly != "") {
		return fmt.Errorf("warm-connections is supported only for vote workloads, not blind control or audit-only")
	}
	if c.requiredStandbys < 0 || c.requiredStandbys > 8 {
		return fmt.Errorf("required-standbys must be 0..8")
	}
	if c.requiredStandbys == 0 && c.standbyNames != "" {
		return fmt.Errorf("standby-names requires a positive required-standbys policy")
	}
	if c.requiredStandbys > 0 {
		names := strings.Split(c.standbyNames, ",")
		seen := make(map[string]bool)
		for _, name := range names {
			name = strings.TrimSpace(name)
			if name == "" || seen[name] {
				return fmt.Errorf("standby-names must contain distinct nonempty names")
			}
			seen[name] = true
		}
		if c.requiredStandbys > len(seen) {
			return fmt.Errorf("not enough standby-names for required-standbys")
		}
	}
	maxRate := c.rate
	if c.repeatEvery > 0 {
		maxRate += c.rate / c.repeatEvery
		if c.rate%c.repeatEvery != 0 {
			maxRate++
		}
	}
	if !c.allowLarge && (c.total > 10_000 || maxRate > 1000) {
		return fmt.Errorf("this workload requires -allow-large; hard key cap still applies")
	}
	if c.auditOnly != "" && (c.keepSchema || c.auditFile != "") {
		return fmt.Errorf("audit-only cannot be combined with retain-schema or audit-file")
	}
	return nil
}

func (c config) window() time.Duration {
	return time.Duration(c.total) * time.Second / time.Duration(c.rate)
}

type report struct {
	Mode              string          `json:"mode"`
	Schema            string          `json:"isolated_schema,omitempty"`
	PollID            string          `json:"poll_id,omitempty"`
	Configuration     map[string]any  `json:"configuration,omitempty"`
	Database          *databaseInfo   `json:"database,omitempty"`
	Workload          *workloadReport `json:"workload,omitempty"`
	WAL               *walReport      `json:"wal,omitempty"`
	PhysicalSizes     *sizeReport     `json:"physical_sizes,omitempty"`
	Audit             *auditReport    `json:"reconciliation,omitempty"`
	DurabilityChecked bool            `json:"explicit_replication_durability_checked"`
	KeyLedgerFile     string          `json:"private_key_ledger_file,omitempty"`
	Cleanup           string          `json:"schema_cleanup,omitempty"`
	WallSeconds       float64         `json:"total_wall_seconds"`
	Errors            []string        `json:"errors"`
	Limitations       []string        `json:"limitations"`
}

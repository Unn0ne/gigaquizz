package main

import (
	"encoding/json"
	"flag"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"

	"gigaquizz/internal/runtimeconfig"
)

func startupFixture(t *testing.T, overrides map[string]string) (string, string) {
	t.Helper()
	cpu, memory := runtime.GOMAXPROCS(0), debug.SetMemoryLimit(-1)
	gc := debug.SetGCPercent(100)
	t.Cleanup(func() { runtime.GOMAXPROCS(cpu); debug.SetMemoryLimit(memory); debug.SetGCPercent(gc) })
	for _, key := range []string{
		"HTTP_ADDR", "PUBLIC_URL", "ADMIN_PASSWORD", "DATA_DIR", "MAX_INFLIGHT", "MAX_UNIQUE_VOTERS", "MAX_PARTITION_UNIQUE_VOTERS",
		"FILE_PARTITIONS", "FILE_BATCH_VOTES", "FILE_QUEUE_VOTES", "FILE_GROUP_LINGER", "GOMAXPROCS", "GOMEMLIMIT", "GOGC",
		"DATABASE_URL", "GIGAQUIZZ_SCHEMA", "KAFKA_BROKERS", "KAFKA_PARTITIONS", "KAFKA_BATCH_VOTES", "KAFKA_QUEUE_VOTES", "KAFKA_LINGER_MS",
		"KAFKA_ALLOW_REMOTE_BROKERS", "KAFKA_TLS", "KAFKA_TLS_CA_FILE", "KAFKA_TLS_CERT_FILE", "KAFKA_TLS_KEY_FILE", "KAFKA_TLS_SERVER_NAME",
		"KAFKA_SASL_MECHANISM", "KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD", "DURABILITY_REQUIRED_STANDBYS", "DURABILITY_STANDBY_NAMES",
		"MAX_STORED_POLLS", "POLL_PREPARATION_SECONDS",
	} {
		// Setenv records the original value for cleanup; absence lets LoadEnv
		// populate it, exactly as on a server configured only through -env.
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data := filepath.Join(directory, "must-not-be-created")
	values := map[string]string{
		"ADMIN_PASSWORD": "fixture-only-configuration-password", "HTTP_ADDR": "127.0.0.1:0", "PUBLIC_URL": "http://127.0.0.1:8091",
		"DATA_DIR": data, "DATABASE_URL": "postgres://fixture:fixture-only-database-password@127.0.0.1:1/unused?sslmode=disable",
		"KAFKA_BROKERS": "127.0.0.1:1", "GIGAQUIZZ_SCHEMA": "gq_config_fixture",
		"GOMAXPROCS": "1", "GOMEMLIMIT": "256MiB", "GOGC": "75",
		"MAX_UNIQUE_VOTERS": "100", "FILE_PARTITIONS": "4", "KAFKA_PARTITIONS": "4",
	}
	for key, value := range overrides {
		values[key] = value
	}
	var contents strings.Builder
	for key, value := range values {
		contents.WriteString(key + "=" + value + "\n")
	}
	env := filepath.Join(directory, "service.env")
	if err := os.WriteFile(env, []byte(contents.String()), 0600); err != nil {
		t.Fatal(err)
	}
	return env, data
}

func runStartup(args ...string) (string, error) {
	previousFlags, previousArgs, previousOut := flag.CommandLine, os.Args, os.Stdout
	flag.CommandLine = flag.NewFlagSet("startup-fixture", flag.ContinueOnError)
	os.Args = append([]string{"gigaquizz"}, args...)
	defer func() { flag.CommandLine, os.Args, os.Stdout = previousFlags, previousArgs, previousOut }()
	r, w, err := os.Pipe()
	if err != nil {
		return "", err
	}
	defer r.Close()
	defer w.Close()
	os.Stdout = w
	runErr := run()
	_ = w.Close()
	body, readErr := io.ReadAll(r)
	if readErr != nil {
		return "", readErr
	}
	return string(body), runErr
}

func requireUntouchedStore(t *testing.T, data string) {
	t.Helper()
	if _, err := os.Lstat(data); !os.IsNotExist(err) {
		t.Fatalf("startup validation touched storage: %v", err)
	}
}

func TestCheckConfigReportsEffectiveFileRuntimeWithoutOpeningService(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	env, data := startupFixture(t, map[string]string{"HTTP_ADDR": listener.Addr().String()})
	body, err := runStartup("-env", env, "-check-config")
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		Mode               string              `json:"mode"`
		Runtime            runtimeconfig.State `json:"runtime"`
		MaxUnique          int                 `json:"max_unique_voters"`
		MaxPartitionUnique int                 `json:"max_partition_unique_voters"`
	}
	if err := json.Unmarshal([]byte(body), &report); err != nil {
		t.Fatal(err)
	}
	if report.Mode != "configuration-check" || report.Runtime.MaxProcs != 1 || report.Runtime.MemoryLimitBytes != 256<<20 || report.Runtime.GCPercent != "75" || report.MaxUnique != 100 || report.MaxPartitionUnique != 100 {
		t.Fatalf("incorrect effective configuration report: %s", body)
	}
	if runtimeconfig.Current() != report.Runtime {
		t.Fatal("report differs from real runtime settings")
	}
	for _, private := range []string{"fixture-only", "postgres://", "gq_config_fixture", data} {
		if strings.Contains(body, private) {
			t.Fatal("configuration report discloses private settings")
		}
	}
	requireUntouchedStore(t, data)
}

func TestInvalidHTTPAndRuntimeFailBeforeStorage(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"PUBLIC_URL", "https://example.invalid/path"}, {"HTTP_ADDR", "localhost:bad"}, {"GOMEMLIMIT", "5GB"}, {"GOMAXPROCS", "0"}, {"GOGC", "bad"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			env, data := startupFixture(t, map[string]string{tc.key: tc.value})
			if _, err := runStartup("-env", env); err == nil || !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("wrong startup rejection: %v", err)
			}
			requireUntouchedStore(t, data)
		})
	}
}

func TestOccupiedListenerFailsBeforeStorage(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	env, data := startupFixture(t, map[string]string{"HTTP_ADDR": listener.Addr().String()})
	if _, err := runStartup("-env", env); err == nil || !strings.Contains(err.Error(), "cannot listen") {
		t.Fatalf("wrong startup rejection: %v", err)
	}
	requireUntouchedStore(t, data)
}

func TestDryConfigurationCannotWriteCPUProfile(t *testing.T) {
	env, data := startupFixture(t, nil)
	profile := filepath.Join(filepath.Dir(env), "must-not-exist.pprof")
	if _, err := runStartup("-env", env, "-check-config", "-cpu-profile", profile); err == nil {
		t.Fatal("dry configuration allowed profiling")
	}
	if _, err := os.Lstat(profile); !os.IsNotExist(err) {
		t.Fatal("dry configuration created profile")
	}
	requireUntouchedStore(t, data)
}

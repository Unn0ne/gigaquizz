package runtimeconfig

import (
	"math"
	"runtime"
	"runtime/debug"
	"testing"
)

func resetEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{"GOMAXPROCS", "GOMEMLIMIT", "GOGC"} {
		t.Setenv(name, "")
	}
}

func preserveRuntime(t *testing.T) {
	t.Helper()
	cpu := runtime.GOMAXPROCS(0)
	mem := debug.SetMemoryLimit(-1)
	gc := debug.SetGCPercent(100)
	t.Cleanup(func() { runtime.GOMAXPROCS(cpu); debug.SetMemoryLimit(mem); debug.SetGCPercent(gc) })
}

func TestSettingsAddedAfterRuntimeStartupAreApplied(t *testing.T) {
	resetEnvironment(t)
	preserveRuntime(t)
	runtime.GOMAXPROCS(2)
	debug.SetMemoryLimit(2 << 30)
	t.Setenv("GOMAXPROCS", "1")
	t.Setenv("GOMEMLIMIT", "512MiB")
	t.Setenv("GOGC", "75")
	before := Current()
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if Current() != before {
		t.Fatal("validation changed the runtime")
	}
	c.Apply()
	after := Current()
	if after.MaxProcs != 1 || after.MemoryLimitBytes != 512<<20 || after.GCPercent != "75" {
		t.Fatalf("env-file settings not applied: %+v", after)
	}
}

func TestEmptySettingsPreserveExistingRuntime(t *testing.T) {
	resetEnvironment(t)
	preserveRuntime(t)
	runtime.GOMAXPROCS(2)
	debug.SetMemoryLimit(3 << 30)
	debug.SetGCPercent(83)
	before := Current()
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	c.Apply()
	if Current() != before {
		t.Fatal("omitted settings replaced runtime defaults")
	}
}

func TestInvalidSettingsFailWithoutChangingRuntime(t *testing.T) {
	for _, setting := range []struct{ key, value string }{
		{"GOMAXPROCS", "0"}, {"GOMAXPROCS", "1025"}, {"GOMAXPROCS", "many"},
		{"GOMEMLIMIT", "2GB"}, {"GOMEMLIMIT", "-1"}, {"GOMEMLIMIT", "8388608TiB"},
		{"GOGC", "no"}, {"GOGC", "2147483648"}, {"GOGC", "-1"},
	} {
		t.Run(setting.key+"/"+setting.value, func(t *testing.T) {
			resetEnvironment(t)
			t.Setenv("GOMAXPROCS", "1")
			t.Setenv(setting.key, setting.value)
			before := Current()
			if _, err := Load(); err == nil {
				t.Fatal("invalid setting accepted")
			}
			if Current() != before {
				t.Fatal("partial settings applied before validation failed")
			}
		})
	}
}

func TestMemoryUnitsAndOff(t *testing.T) {
	for raw, expected := range map[string]int64{"0": 0, "42": 42, "42B": 42, "1KiB": 1 << 10, "2MiB": 2 << 20, "96GiB": 96 << 30, "1TiB": 1 << 40, "off": math.MaxInt64} {
		got, err := memoryBytes(raw)
		if err != nil || got != expected {
			t.Fatalf("%q: %d, %v", raw, got, err)
		}
	}
	resetEnvironment(t)
	preserveRuntime(t)
	t.Setenv("GOGC", "off")
	t.Setenv("GOMEMLIMIT", "off")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	c.Apply()
	if got := Current(); got.MemoryLimitBytes != math.MaxInt64 || got.GCPercent != "off" {
		t.Fatalf("off not applied: %+v", got)
	}
}

package main

import (
	"compress/gzip"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestCPUProfileRunStartupFailureFinishesTheProfile(t *testing.T) {
	directory := t.TempDir()
	profilePath := filepath.Join(directory, "startup-error.pprof")
	previousFlags, previousArgs := flag.CommandLine, os.Args
	flag.CommandLine = flag.NewFlagSet("profile-startup-error", flag.ContinueOnError)
	os.Args = []string{"gigaquizz", "-cpu-profile", profilePath, "-env", directory}
	t.Cleanup(func() { flag.CommandLine, os.Args = previousFlags, previousArgs })
	// A directory cannot be parsed as an env file. The app must return before
	// opening any store or listener, but still finish the real runtime profile.
	if err := run(); err == nil || err.Error() != "cannot load environment file" {
		t.Fatalf("expected isolated startup configuration error, got %v", err)
	}
	file, err := os.Open(profilePath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("profile did not finish writing its gzip header: %v", err)
	}
	defer reader.Close()
	data, err := io.ReadAll(io.LimitReader(reader, 8<<20))
	if err != nil || len(data) == 0 || len(data) == 8<<20 {
		t.Fatalf("profile is empty, unfinished or unexpectedly large: bytes=%d err=%v", len(data), err)
	}
	// The startup error's defer must release the process-wide profiler too.
	cleanup, err := startCPUProfile(filepath.Join(directory, "next.pprof"))
	if err != nil {
		t.Fatalf("startup failure left CPU profiling active: %v", err)
	}
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
}

func TestCPUProfileDisabledHasNoSideEffects(t *testing.T) {
	cleanup, err := startCPUProfileWith("", func(io.Writer) error {
		t.Fatal("disabled profile started the runtime profiler")
		return nil
	}, func() { t.Fatal("disabled profile stopped the runtime profiler") })
	if err != nil || cleanup == nil {
		t.Fatalf("disabled profile: %v", err)
	}
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
}

func TestCPUProfileDoesNotOverwriteExistingFileOrSymlink(t *testing.T) {
	directory := t.TempDir()
	original := filepath.Join(directory, "existing.pprof")
	const previous = "retained private profile"
	if err := os.WriteFile(original, []byte(previous), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "profile-link")
	if err := os.Symlink(original, link); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{original, link} {
		cleanup, err := startCPUProfileWith(path, func(io.Writer) error {
			t.Fatal("existing output path reached the runtime profiler")
			return nil
		}, func() { t.Fatal("failed output creation stopped an unrelated profile") })
		if !errors.Is(err, os.ErrExist) || cleanup != nil {
			t.Fatalf("existing profile accepted: cleanup=%v err=%v", cleanup != nil, err)
		}
		data, err := os.ReadFile(original)
		if err != nil || string(data) != previous {
			t.Fatalf("previous profile changed: err=%v", err)
		}
	}
}

func TestCPUProfileCleanupFlushesBeforeClosingAndIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "new.pprof")
	var output *os.File
	var stops int
	cleanup, err := startCPUProfileWith(path, func(writer io.Writer) error {
		var ok bool
		output, ok = writer.(*os.File)
		if !ok {
			t.Fatal("CPU profile output is not its owned file")
		}
		_, err := io.WriteString(writer, "header\n")
		return err
	}, func() {
		stops++
		if _, err := io.WriteString(output, "footer\n"); err != nil {
			t.Errorf("profile output closed before StopCPUProfile flushed: %v", err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cleanup() })
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("profile is not a private 0600 file: info=%v err=%v", info, err)
	}
	for range 2 {
		if err := cleanup(); err != nil {
			t.Fatal(err)
		}
	}
	if stops != 1 {
		t.Fatalf("runtime profiler stopped %d times", stops)
	}
	if _, err := output.Write([]byte("after close")); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("profile descriptor remained open: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "header\nfooter\n" {
		t.Fatalf("final profile data was not retained: err=%v", err)
	}
}

func TestCPUProfileStartFailureClosesItsFileWithoutStoppingAnotherProfile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "failed.pprof")
	failure := errors.New("another CPU profile is active")
	var output *os.File
	cleanup, err := startCPUProfileWith(path, func(writer io.Writer) error {
		output = writer.(*os.File)
		return failure
	}, func() { t.Fatal("failed start stopped an unrelated CPU profile") })
	if !errors.Is(err, failure) || cleanup != nil {
		t.Fatalf("profile start failure hidden: cleanup=%v err=%v", cleanup != nil, err)
	}
	if _, err := output.Write([]byte("after close")); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("failed start leaked its file descriptor: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("failed output was unexpectedly removed: %v", err)
	}
}

func TestCPUProfileCleanupReportsCloseFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "close-failure.pprof")
	cleanup, err := startCPUProfileWith(path, func(writer io.Writer) error {
		// Force the close error without relying on a real full or failed disk.
		return writer.(*os.File).Close()
	}, func() {})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := cleanup(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("profile close failure hidden: %v", err)
		}
	}
}

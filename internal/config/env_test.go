package config

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestEnvironmentFilePreservesPrecedenceAndSupportsSecretSymlinks(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "service.env")
	if err := os.WriteFile(path, []byte("GIGAQUIZZ_ENV_TEST=from-file\n"), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "secret.env")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GIGAQUIZZ_ENV_TEST", "from-environment")
	if err := LoadEnv(link); err != nil || os.Getenv("GIGAQUIZZ_ENV_TEST") != "from-environment" {
		t.Fatalf("existing environment or symlinked secret changed: %v", err)
	}
	if err := os.Unsetenv("GIGAQUIZZ_ENV_TEST"); err != nil {
		t.Fatal(err)
	}
	if err := LoadEnv(link); err != nil || os.Getenv("GIGAQUIZZ_ENV_TEST") != "from-file" {
		t.Fatalf("private file configuration was not loaded: %v", err)
	}
}

func TestEnvironmentFileRejectsMissingDirectoryAndFIFO(t *testing.T) {
	directory := t.TempDir()
	if err := LoadEnv(filepath.Join(directory, "missing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing required file was ignored: %v", err)
	}
	if err := LoadEnv(directory); err == nil {
		t.Fatal("directory accepted as env file")
	}
	fifo := filepath.Join(directory, "fifo")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- LoadEnv(fifo) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("FIFO accepted as env file")
		}
	case <-time.After(time.Second):
		// Unblock a regressed blocking open so the failing test can exit.
		f, err := os.OpenFile(fifo, os.O_RDWR|syscall.O_NONBLOCK, 0)
		if err == nil {
			_ = f.Close()
		}
		t.Fatal("env validation blocked opening a FIFO")
	}
}

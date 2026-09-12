package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime/pprof"
	"sync"
)

// CPU profiles are an explicit local diagnostic. They cover startup through
// finalization and shutdown, not only an externally measured voting interval.
// The empty default leaves the runtime profiler and filesystem untouched.
func startCPUProfile(path string) (func() error, error) {
	return startCPUProfileWith(path, pprof.StartCPUProfile, pprof.StopCPUProfile)
}

func startCPUProfileWith(path string, start func(io.Writer) error, stop func()) (func() error, error) {
	if path == "" {
		return func() error { return nil }, nil
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, fmt.Errorf("create CPU profile: %w", err)
	}
	if err := start(file); err != nil {
		// StartCPUProfile can fail if another profile is active. Close only
		// this new file, leaving that profile and all existing files alone.
		return nil, errors.Join(fmt.Errorf("start CPU profile: %w", err), file.Close())
	}
	var once sync.Once
	var closeErr error
	return func() error {
		once.Do(func() {
			// Stop waits for the runtime to write its final profile data.
			stop()
			if err := file.Close(); err != nil {
				closeErr = fmt.Errorf("close CPU profile: %w", err)
			}
		})
		return closeErr
	}, nil
}

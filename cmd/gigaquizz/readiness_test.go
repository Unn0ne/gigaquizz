package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type testHealth struct {
	failed atomic.Bool
	checks atomic.Int64
}

func (h *testHealth) Ping(context.Context) error {
	h.checks.Add(1)
	if h.failed.Load() {
		return errors.New("current writer failed")
	}
	return nil
}

type testReady struct{ changes chan bool }

func (r testReady) SetReady(v bool) { r.changes <- v }

type delayedPing struct{ entered, release chan struct{} }

func (h delayedPing) Ping(context.Context) error { close(h.entered); <-h.release; return nil }

type notifyingGate struct {
	gate      *readinessGate
	attempted chan struct{}
}

func (n notifyingGate) SetReady(v bool) { n.gate.SetReady(v); close(n.attempted) }

func TestShutdownCannotBeReopenedByEarlierHealthCheck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := testReady{make(chan bool, 4)}
	gate := &readinessGate{target: ready}
	h := delayedPing{make(chan struct{}), make(chan struct{})}
	done := make(chan struct{})
	attempted := make(chan struct{})
	go func() { defer close(done); readiness(ctx, notifyingGate{gate, attempted}, h, time.Hour) }()
	<-h.entered
	gate.stop()
	if <-ready.changes {
		t.Fatal("shutdown remained ready")
	}
	close(h.release)
	// The completed Ping must attempt its update before cancellation so the
	// assertion actually exercises an old successful check arriving too late.
	<-attempted
	cancel()
	<-done
	select {
	case v := <-ready.changes:
		t.Fatalf("late readiness update: %v", v)
	default:
	}
}

func TestReadinessKeepsCheckingIndependentOfFinalization(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	health := new(testHealth)
	ready := testReady{make(chan bool, 32)}
	done := make(chan struct{})
	// The caller may remain blocked calculating history; readiness owns its
	// own loop and must observe a current writer failure without that loop.
	go func() { defer close(done); readiness(ctx, ready, health, time.Millisecond) }()
	select {
	case ok := <-ready.changes:
		if !ok {
			t.Fatal("initial healthy rejected")
		}
	case <-time.After(time.Second):
		t.Fatal("no initial readiness")
	}
	health.failed.Store(true)
	timeout := time.After(time.Second)
	for {
		select {
		case ok := <-ready.changes:
			if !ok {
				cancel()
				<-done
				if health.checks.Load() < 2 {
					t.Fatal("no recheck")
				}
				return
			}
		case <-timeout:
			t.Fatal("readiness waited for unrelated work")
		}
	}
}

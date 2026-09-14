package main

import (
	"context"
	"sync"
	"time"
)

// Health checks have their own goroutine; a historical replay must not suspend
// readiness updates for the currently admitting writer.
func readiness(ctx context.Context, api interface{ SetReady(bool) }, store interface{ Ping(context.Context) error }, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		ping, cancel := context.WithTimeout(ctx, time.Second)
		err := store.Ping(ping)
		cancel()
		if ctx.Err() != nil {
			return
		}
		api.SetReady(err == nil)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// An in-flight health check cannot re-enable readiness after shutdown starts.
// Stopping this gate does not wait for a blocked Ping or finalization syscall.
type readinessGate struct {
	mu      sync.Mutex
	stopped bool
	target  interface{ SetReady(bool) }
}

func (g *readinessGate) SetReady(ready bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.stopped {
		g.target.SetReady(ready)
	}
}

func (g *readinessGate) stop() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.stopped = true
	g.target.SetReady(false)
}

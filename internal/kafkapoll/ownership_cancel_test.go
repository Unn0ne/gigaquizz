package kafkapoll

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type observedOwnerContext struct {
	context.Context
	checked  chan struct{}
	observed atomic.Bool
}

func (c *observedOwnerContext) Err() error {
	err := c.Context.Err()
	if c.observed.CompareAndSwap(false, true) {
		close(c.checked)
	}
	return err
}

func TestCanceledOwnerCheckWaitingForMutexDoesNotRetireController(t *testing.T) {
	ownerCtx, ownerCancel := context.WithCancel(context.Background())
	defer ownerCancel()
	s := &Store{ctx: ownerCtx, cancel: ownerCancel}
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	caller := &observedOwnerContext{Context: base, checked: make(chan struct{})}
	s.ownerMu.Lock()
	locked := true
	t.Cleanup(func() {
		if locked {
			s.ownerMu.Unlock()
		}
	})
	done := make(chan error, 1)
	go func() {
		// A nil owner proves that canceled-before-query work touches no PG
		// connection. Recover makes the regression fail at the assertion.
		defer func() {
			if recover() != nil {
				done <- errors.New("canceled owner check attempted SQL")
			}
		}()
		done <- s.checkOwner(caller)
	}()
	select {
	case <-caller.checked:
	case <-time.After(time.Second):
		t.Fatal("owner check did not reach pre-lock validation")
	}
	// Err already returned nil to checkOwner; cancellation therefore happens
	// after its first guard and before the lock can be acquired.
	cancel()
	s.ownerMu.Unlock()
	locked = false
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("caller cancellation was treated as ownership loss", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled owner check did not return after lock release")
	}
	if s.ctx.Err() != nil || s.closed.Load() {
		t.Fatal("caller cancellation retired the controller")
	}
}

func TestOwnerLossWhileWaitingDoesNotUseOldConnection(t *testing.T) {
	ownerCtx, ownerCancel := context.WithCancel(context.Background())
	defer ownerCancel()
	s := &Store{ctx: ownerCtx, cancel: ownerCancel}
	caller := &observedOwnerContext{Context: context.Background(), checked: make(chan struct{})}
	s.ownerMu.Lock()
	locked := true
	t.Cleanup(func() {
		if locked {
			s.ownerMu.Unlock()
		}
	})
	done := make(chan error, 1)
	go func() {
		defer func() {
			if recover() != nil {
				done <- errors.New("retired owner check attempted SQL")
			}
		}()
		done <- s.checkOwner(caller)
	}()
	select {
	case <-caller.checked:
	case <-time.After(time.Second):
		t.Fatal("owner check did not reach pre-lock validation")
	}
	ownerCancel()
	s.ownerMu.Unlock()
	locked = false
	select {
	case err := <-done:
		if !errors.Is(err, ErrOwnership) {
			t.Fatal("known ownership loss was ignored", err)
		}
	case <-time.After(time.Second):
		t.Fatal("retired owner check did not return")
	}
}

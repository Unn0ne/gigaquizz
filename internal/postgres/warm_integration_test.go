package postgres

import (
	"context"
	"testing"
	"time"
)

func TestWarmCreatesAllConnectionsAndReleasesOnCancellation(t *testing.T) {
	_, dsn := integrationStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := New(ctx, dsn, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Warm(ctx); err != nil {
		t.Fatal(err)
	}
	if stat := s.pool.Stat(); stat.TotalConns() != 4 || stat.IdleConns() != 4 || stat.AcquiredConns() != 0 {
		t.Fatalf("warmup did not release four ready connections: total=%d idle=%d acquired=%d", stat.TotalConns(), stat.IdleConns(), stat.AcquiredConns())
	}
	// A concurrent borrower makes warmup wait after acquiring the other three.
	borrowed, err := s.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer borrowed.Release()
	short, stop := context.WithTimeout(ctx, 25*time.Millisecond)
	defer stop()
	if err := s.Warm(short); err == nil {
		t.Fatal("warmup should fail while the last connection is unavailable")
	}
	if stat := s.pool.Stat(); stat.AcquiredConns() != 1 || stat.IdleConns() != 3 {
		t.Fatalf("cancelled warmup leaked connections: acquired=%d idle=%d", stat.AcquiredConns(), stat.IdleConns())
	}
}

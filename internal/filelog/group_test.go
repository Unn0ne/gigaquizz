package filelog

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func groupTestConfig(t *testing.T) GroupConfig {
	c := testConfig(t)
	return GroupConfig{Directory: c.Directory, PollID: c.PollID, StartsAt: c.StartsAt, EndsAt: c.EndsAt, AllowedMask: 3, Partitions: 4, BatchSize: 16, QueuePerPartition: 4, Linger: 0}
}
func groupTokens(c GroupConfig, p int, n int) []Input {
	var result []Input
	for id := uint64(1); len(result) < n; id++ {
		var token [16]byte
		binary.BigEndian.PutUint64(token[8:], id)
		if c.Partition(token) == int32(p) {
			result = append(result, Input{Token: token, Choice: 1})
		}
	}
	return result
}
func freezeGroup(g *Group) *atomic.Int64 {
	clock := new(atomic.Int64)
	clock.Store(g.c.StartsAt.Add(time.Second).UnixNano())
	g.now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	for _, w := range g.writers {
		w.now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	}
	return clock
}

func TestGroupRecoveryExactRoutingReceiptsAndBarrier(t *testing.T) {
	c := groupTestConfig(t)
	g, err := NewGroup(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	want := make(map[Position]Vote)
	for p := 0; p < c.Partitions; p++ {
		for _, in := range groupTokens(c, p, 3) {
			r, err := g.Submit(context.Background(), in)
			if err != nil || r.Partition != int32(p) {
				t.Fatalf("route: %+v %v", r, err)
			}
			want[Position{r.Partition, r.Offset, r.Index}] = Vote{in.Token, in.Choice, r.AdmittedAt.UTC()}
		}
	}
	g.Close()
	g, err = RecoverGroup(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	clock := freezeGroup(g)
	in := groupTokens(c, 1, 1)[0]
	in.Choice = 2
	r, err := g.Submit(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	want[Position{r.Partition, r.Offset, r.Index}] = Vote{in.Token, in.Choice, r.AdmittedAt.UTC()}
	clock.Store(c.EndsAt.UnixNano())
	m, err := g.Seal(context.Background())
	if err != nil || len(m.Partitions) != 4 {
		t.Fatalf("seal: %+v %v", m, err)
	}
	if _, err := g.Submit(context.Background(), in); !errors.Is(err, ErrClosed) {
		t.Fatalf("late duplicate admitted: %v", err)
	}
	g.Close()
	got := make(map[Position]Vote)
	replayed, err := GroupReplay(context.Background(), c, func(p Position, v Vote) error { got[p] = v; return nil })
	if err != nil || !reflect.DeepEqual(m, replayed) || !reflect.DeepEqual(want, got) {
		t.Fatalf("exact replay differs: %v", err)
	}
	changed := c
	changed.Partitions = 3
	if _, err := RecoverGroup(context.Background(), changed); err == nil {
		t.Fatal("changed topology adopted")
	}
}

func TestGroupIndependentQueueAndSyncFailure(t *testing.T) {
	c := groupTestConfig(t)
	g, err := NewGroup(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	clock := freezeGroup(g)
	clock.Store(c.EndsAt.Add(-time.Millisecond).UnixNano())
	entered, release := make(chan struct{}), make(chan struct{})
	g.writers[0].syncFile = func() error { close(entered); <-release; return syscall.EIO }
	done := make(chan error, 1)
	go func() { _, err := g.Submit(context.Background(), groupTokens(c, 0, 1)[0]); done <- err }()
	awaitValue(t, entered)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := g.Submit(ctx, groupTokens(c, 1, 1)[0])
	if err != nil || !r.AdmittedAt.Equal(c.EndsAt.Add(-time.Millisecond)) {
		t.Fatalf("healthy shard blocked: %+v %v", r, err)
	}
	clock.Store(c.EndsAt.UnixNano())
	close(release)
	if err := awaitValue(t, done); !errors.Is(err, ErrUnknown) {
		t.Fatalf("sync failure ACKed: %v", err)
	}
	if _, err := g.Seal(ctx); err == nil {
		t.Fatal("partial failed group produced complete manifest")
	}
	if g.Metrics()["poisoned"] != 1 {
		t.Fatal("failed writer aggregate missing")
	}
	g.Close()
	if _, err := GroupReplay(context.Background(), c, nil); err == nil {
		t.Fatal("incomplete group passed read audit")
	}
}

func TestGroupRejectsMissingExtraSwappedAndMisroutedWAL(t *testing.T) {
	for _, kind := range []string{"missing", "extra", "swapped", "misrouted"} {
		t.Run(kind, func(t *testing.T) {
			c := groupTestConfig(t)
			g, err := NewGroup(context.Background(), c)
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			clock := freezeGroup(g)
			if kind == "misrouted" {
				if _, err := g.writers[0].Submit(context.Background(), groupTokens(c, 1, 1)[0]); err != nil {
					t.Fatal(err)
				}
			}
			clock.Store(c.EndsAt.UnixNano())
			if _, err := g.Seal(context.Background()); err != nil {
				t.Fatal(err)
			}
			g.Close()
			switch kind {
			case "missing":
				if err := os.Remove(filepath.Join(c.child(0).Directory, walName)); err != nil {
					t.Fatal(err)
				}
			case "extra":
				if err := os.WriteFile(filepath.Join(c.Directory, "foreign"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "swapped":
				a, b := c.child(0).Directory, c.child(1).Directory
				tmp := filepath.Join(c.Directory, "swap")
				for _, paths := range [][2]string{{a, tmp}, {b, a}, {tmp, b}} {
					if err := os.Rename(paths[0], paths[1]); err != nil {
						t.Fatal(err)
					}
				}
			}
			if _, err := GroupReplay(context.Background(), c, nil); err == nil {
				t.Fatal("malformed group passed replay")
			}
			if w, err := RecoverGroup(context.Background(), c); err == nil {
				w.Close()
				t.Fatal("malformed group recovered")
			}
		})
	}
}

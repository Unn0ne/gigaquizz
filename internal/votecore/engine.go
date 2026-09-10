// Package votecore provides an exact, bounded in-memory voting core for CPU
// experiments. Accepted means stored in this process's RAM, never a durable ACK.
// It is not wired to the public API or used as proof of Kafka throughput.
package votecore

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrInvalidConfig = errors.New("invalid in-memory voting configuration")
	ErrStillOpen     = errors.New("poll admission window is still open")
)

type Config struct {
	PollID           [16]byte
	StartsAt, EndsAt time.Time
	AllowedMask      uint32
	Multiple         bool
	Shards           int
	// Capacity is an exact total bound split between shards. A full shard can
	// reject before the whole engine fills; callers must budget for imbalance.
	Capacity uint64
}

type Input struct {
	Token  [16]byte
	Choice uint32
}
type Status uint8

const (
	Accepted Status = iota
	Duplicate
	Closed
	NotOpen
	Invalid
	CapacityExceeded
)

type Result struct {
	Status Status
	Choice uint32
}
type Snapshot struct {
	Total  uint64
	Counts [32]uint64
	Closed bool
}

type shard struct {
	mu       sync.Mutex
	votes    map[[16]byte]uint32
	counts   [32]uint64
	capacity int
}

type Engine struct {
	cfg            Config
	startNS, endNS int64
	shards         []shard
	sealed         atomic.Bool
	now            func() time.Time
	// Tests configure this hook before concurrent use; production never does.
	beforeStore func()
}

func New(c Config) (*Engine, error) {
	if c.PollID == [16]byte{} || c.StartsAt.IsZero() || c.EndsAt.Sub(c.StartsAt) != time.Minute || c.AllowedMask == 0 || c.Shards < 1 || c.Shards > 4096 || c.Shards&(c.Shards-1) != 0 || c.Capacity < uint64(c.Shards) || c.Capacity > 200_000_000 {
		return nil, ErrInvalidConfig
	}
	e := &Engine{cfg: c, startNS: c.StartsAt.UnixNano(), endNS: c.EndsAt.UnixNano(), shards: make([]shard, c.Shards), now: time.Now}
	base, extra := c.Capacity/uint64(c.Shards), c.Capacity%uint64(c.Shards)
	for i := range e.shards {
		limit := base
		if uint64(i) < extra {
			limit++
		}
		e.shards[i].capacity = int(limit)
		e.shards[i].votes = make(map[[16]byte]uint32, int(limit))
	}
	return e, nil
}

// Partition matches the existing journal's SHA-256(poll ID || full token)
// routing for the same shard count. Deduplication always compares the full ID.
func (e *Engine) Partition(token [16]byte) int {
	var key [32]byte
	copy(key[:16], e.cfg.PollID[:])
	copy(key[16:], token[:])
	h := sha256.Sum256(key[:])
	return int(binary.BigEndian.Uint64(h[:8]) & uint64(len(e.shards)-1))
}

func (e *Engine) Submit(input Input) Result {
	if input.Token == [16]byte{} || input.Choice == 0 || input.Choice & ^e.cfg.AllowedMask != 0 || !e.cfg.Multiple && bits.OnesCount32(input.Choice) != 1 {
		return Result{Status: Invalid}
	}
	s := &e.shards[e.Partition(input.Token)]
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.sealed.Load() {
		return Result{Status: Closed}
	}
	// Admission is checked after obtaining the shard lock, never at scheduling
	// or arrival outside the critical section.
	now := e.now().UnixNano()
	if now < e.startNS {
		return Result{Status: NotOpen}
	}
	if now >= e.endNS {
		e.sealed.Store(true)
		return Result{Status: Closed}
	}
	if previous := s.votes[input.Token]; previous != 0 {
		return Result{Status: Duplicate, Choice: previous}
	}
	if len(s.votes) >= s.capacity {
		return Result{Status: CapacityExceeded}
	}
	if e.beforeStore != nil {
		e.beforeStore()
	}
	s.votes[input.Token] = input.Choice
	for mask := input.Choice; mask != 0; mask &= mask - 1 {
		s.counts[bits.TrailingZeros32(mask)]++
	}
	return Result{Status: Accepted, Choice: input.Choice}
}

// Seal is irreversible and waits for all already-admitted critical sections.
// It returns ErrStillOpen before the deadline rather than advancing the clock.
func (e *Engine) Seal() (Snapshot, error) {
	if !e.sealed.Load() && e.now().UnixNano() < e.endNS {
		return Snapshot{}, ErrStillOpen
	}
	e.sealed.Store(true)
	result := Snapshot{Closed: true}
	for i := range e.shards {
		s := &e.shards[i]
		s.mu.Lock()
		result.Total += uint64(len(s.votes))
		for bit, n := range s.counts {
			result.Counts[bit] += n
		}
		s.mu.Unlock()
	}
	return result, nil
}

// Lookup recovers the exact first choice held in RAM, including after closing.
// No outcome survives process exit or crash; this function does not read a log.
func (e *Engine) Lookup(token [16]byte) (uint32, bool) {
	s := &e.shards[e.Partition(token)]
	s.mu.Lock()
	choice, ok := s.votes[token]
	s.mu.Unlock()
	return choice, ok
}

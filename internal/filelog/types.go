// Package filelog is an isolated single-disk durable voting journal prototype.
// A receipt confirms a persisted attempt, not a unique or final vote.
package filelog

import (
	"context"
	"errors"
	"math/bits"
	"time"
)

var (
	ErrNotOpen = errors.New("poll has not started")
	ErrClosed  = errors.New("poll admission closed")
	ErrBusy    = errors.New("bounded admission queue full")
	ErrUnknown = errors.New("attempt outcome unknown")
	ErrInvalid = errors.New("invalid vote or configuration")
)

type Config struct {
	Directory         string
	PollID            [16]byte
	StartsAt, EndsAt  time.Time
	AllowedMask       uint32
	Multiple          bool
	BatchSize         int
	QueuePerPartition int
	Linger            time.Duration
	Partitions        int
}

func (c Config) Partition([16]byte) int32 { return 0 }

func (c Config) validate() error {
	if c.Directory == "" || c.PollID == [16]byte{} || c.Partitions != 1 ||
		c.StartsAt.Year() < 1678 || c.EndsAt.Year() > 2261 || c.EndsAt.Sub(c.StartsAt) != time.Minute ||
		c.AllowedMask == 0 || c.BatchSize < 1 || c.BatchSize > 131072 ||
		c.QueuePerPartition < 1 || c.QueuePerPartition > 1048576 ||
		c.Linger < 0 || c.Linger > time.Second {
		return ErrInvalid
	}
	return nil
}

func (c Config) validChoice(choice uint32) bool {
	return choice != 0 && choice & ^c.AllowedMask == 0 && (c.Multiple || bits.OnesCount32(choice) == 1)
}

type Input struct {
	Token  [16]byte
	Choice uint32
}
type Vote struct {
	Token      [16]byte
	Choice     uint32
	AdmittedAt time.Time
}
type FrameReceipt struct {
	Partition  int32     `json:"partition"`
	Offset     int64     `json:"offset"`
	Count      uint32    `json:"count"`
	AdmittedAt time.Time `json:"admitted_at"`
}
type Position struct {
	Partition int32
	Offset    int64
	Index     uint32
}
type PartitionEnd struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}
type Manifest struct {
	Partitions []PartitionEnd `json:"partitions"`
}

// ScanResult describes only structurally complete frames. A valid prefix may
// contain unacknowledged attempts; scanning never certifies prior sync or ACK.
type ScanResult struct {
	Manifest       Manifest
	Closed         bool
	IncompleteTail bool
	Frames         uint64
	Votes          uint64
	ValidBytes     int64
}

func waitUntil(ctx context.Context, until time.Time, now func() time.Time) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		delay := until.Sub(now())
		if delay <= 0 {
			return nil
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

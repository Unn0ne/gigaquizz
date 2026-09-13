package votelog

import (
	"context"
	"crypto/sha256"
	"errors"
	"sort"
)

func (c Config) ownedPartitions() ([]int32, error) {
	if c.Partitions < 1 || c.Partitions > 256 {
		return nil, ErrInvalid
	}
	if c.OwnedPartitions == nil {
		p := make([]int32, c.Partitions)
		for i := range p {
			p[i] = int32(i)
		}
		return p, nil
	}
	if len(c.OwnedPartitions) == 0 || len(c.OwnedPartitions) > c.Partitions {
		return nil, ErrInvalid
	}
	p := append([]int32(nil), c.OwnedPartitions...)
	sort.Slice(p, func(i, j int) bool { return p[i] < p[j] })
	for i, v := range p {
		if v < 0 || int(v) >= c.Partitions || (i > 0 && p[i-1] == v) {
			return nil, ErrInvalid
		}
	}
	return p, nil
}

func (s *Store) writerFor(partition int32) *writer {
	if s.byPartition != nil {
		return s.byPartition[partition]
	}
	// Small deterministic unit fixtures construct writers directly.
	for _, w := range s.writers {
		if w.partition == partition {
			return w
		}
	}
	return nil
}

func (c Config) manifest() Manifest {
	owned, _ := c.ownedPartitions()
	return Manifest{Topic: c.Topic, StartsAt: c.StartsAt, EndsAt: c.EndsAt,
		Partial: len(owned) != c.Partitions, TotalPartitions: c.Partitions, OwnedPartitions: owned}
}

// DefinitionHash binds checkpoints to the immutable topic and poll definition.
// Runtime credentials, ownership and batching settings are deliberately excluded.
func (c Config) DefinitionHash() [32]byte {
	data := encodeRecord(c, recordBoot, Vote{})
	data = append(data, c.Topic...)
	return sha256.Sum256(data)
}

// ReplayPartition validates exactly one CLOSED prefix. Routing still uses the
// complete immutable partition count, so full-token deduplication is local to p.
// It opens no producer and never transfers ownership.
func ReplayPartition(ctx context.Context, c Config, p int32, visit func(Position, Vote) error) (PartitionEnd, error) {
	if p < 0 || int(p) >= c.Partitions {
		return PartitionEnd{}, ErrInvalid
	}
	c.OwnedPartitions = []int32{p}
	if err := c.validate(); err != nil {
		return PartitionEnd{}, err
	}
	closed, err := replay(ctx, c, nil, visit)
	if err != nil {
		return PartitionEnd{}, err
	}
	if closed[p] < 0 {
		return PartitionEnd{}, errors.New("partition replay lacks CLOSED")
	}
	return PartitionEnd{Partition: p, Offset: closed[p]}, nil
}

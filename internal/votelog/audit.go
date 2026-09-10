package votelog

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// scan stops only after observing a caller-selected committed record in every
// partition. High watermarks/LSOs and empty fetches are not consumed positions:
// aborted data and hidden transaction markers make those shortcuts incorrect.
func scan(ctx context.Context, c Config, visit func(*kgo.Record) (bool, error)) error {
	partitions := map[int32]kgo.Offset{}
	for i := 0; i < c.Partitions; i++ {
		partitions[int32(i)] = kgo.NewOffset().AtStart()
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.Brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{c.Topic: partitions}),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxBytes(32<<20), kgo.FetchMaxPartitionBytes(4<<20))
	if err != nil {
		return err
	}
	defer cl.Close()
	done := make([]bool, c.Partitions)
	remaining := c.Partitions
	for remaining > 0 {
		fetches := cl.PollRecords(ctx, 10000)
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, e := range fetches.Errors() {
			return e.Err
		}
		var failure error
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if failure != nil {
				return
			}
			if p.Partition < 0 || int(p.Partition) >= c.Partitions {
				failure = errors.New("unexpected partition")
				return
			}
			if p.LogStartOffset > 0 {
				failure = errors.New("journal prefix expired; exact reconciliation impossible")
				return
			}
			for _, r := range p.Records {
				if done[r.Partition] {
					continue
				}
				if !r.Attrs.IsTransactional() {
					failure = errors.New("journal record bypassed transactional writer")
					return
				}
				finish, err := visit(r)
				if err != nil {
					failure = err
					return
				}
				if finish {
					done[r.Partition] = true
					remaining--
					cl.PauseFetchPartitions(map[string][]int32{c.Topic: {r.Partition}})
				}
			}
		})
		if failure != nil {
			return failure
		}
	}
	return nil
}

func verifyFirstRecords(ctx context.Context, c Config) error {
	return scan(ctx, c, func(r *kgo.Record) (bool, error) {
		kind, _, err := decodeRecord(c, r.Value)
		if err != nil {
			return false, err
		}
		if kind != recordBoot || len(r.Key) != 0 {
			return false, errors.New("journal lacks initial configuration barrier")
		}
		return true, nil
	})
}

// A recovery BOOT after CLOSED is harmless control data. Votes after CLOSED
// are invalid. BOOT does not move the terminal vote/result boundary.
func replay(ctx context.Context, c Config, barriers []int64, visit func(Position, Vote) error) ([]int64, error) {
	closed := make([]int64, c.Partitions)
	seenBoot := make([]bool, c.Partitions)
	for i := range closed {
		closed[i] = -1
	}
	err := scan(ctx, c, func(r *kgo.Record) (bool, error) {
		p := int(r.Partition)
		if len(r.Value) > 0 && r.Value[0] == frameVersion {
			if !seenBoot[p] || closed[p] >= 0 || len(r.Key) != 0 {
				return false, errors.New("journal frame outside open partition or carrying Kafka key")
			}
			if barriers != nil && r.Offset >= barriers[p] {
				return false, errors.New("journal frame cannot replace or follow recovery barrier")
			}
			err := decodeFrame(c, r.Value, func(index uint32, v Vote) error {
				if c.Partition(v.Token) != r.Partition {
					return errors.New("journal frame vote routing mismatch")
				}
				if visit != nil {
					return visit(Position{Partition: r.Partition, Offset: r.Offset, Index: index}, v)
				}
				return nil
			})
			return false, err
		}
		kind, v, err := decodeRecord(c, r.Value)
		if err != nil {
			return false, err
		}
		if !seenBoot[p] && kind != recordBoot {
			return false, errors.New("missing initial barrier")
		}
		switch kind {
		case recordBoot:
			if len(r.Key) != 0 {
				return false, ErrInvalid
			}
			seenBoot[p] = true
		case recordVote:
			if closed[p] >= 0 {
				return false, errors.New("vote appended after CLOSED")
			}
			if c.Partition(v.Token) != r.Partition || string(r.Key) != string(v.Token[:]) {
				return false, errors.New("vote routing/key mismatch")
			}
			if visit != nil {
				if err := visit(Position{Partition: r.Partition, Offset: r.Offset}, v); err != nil {
					return false, err
				}
			}
		case recordClosed:
			if len(r.Key) != 0 || closed[p] >= 0 {
				return false, errors.New("invalid repeated CLOSED")
			}
			closed[p] = r.Offset
		}
		if barriers == nil {
			return kind == recordClosed, nil
		}
		if r.Offset > barriers[p] {
			return false, errors.New("recovery barrier skipped")
		}
		if r.Offset == barriers[p] {
			if kind != recordBoot {
				return false, errors.New("recovery target is not a committed BOOT")
			}
			return true, nil
		}
		return false, nil
	})
	return closed, err
}

// Replay validates the committed journal through each partition's CLOSED
// barrier and streams every vote in (offset, index) order within a partition.
// It does not retain a vote map. Callers publish only after a successful return;
// a visitor error or malformed later record leaves the result incomplete.
func Replay(ctx context.Context, c Config, visit func(Position, Vote) error) (Manifest, error) {
	if err := c.validate(); err != nil {
		return Manifest{}, err
	}
	closed, err := replay(ctx, c, nil, visit)
	if err != nil {
		return Manifest{}, err
	}
	m := Manifest{Topic: c.Topic, StartsAt: c.StartsAt, EndsAt: c.EndsAt}
	for p, offset := range closed {
		m.Partitions = append(m.Partitions, PartitionEnd{Partition: int32(p), Offset: offset})
	}
	return m, nil
}

func Audit(ctx context.Context, c Config) (AuditResult, error) {
	if err := c.validate(); err != nil {
		return AuditResult{}, err
	}
	a := AuditResult{Canonical: make(map[[16]byte]Vote), Records: make(map[Position]Vote), Manifest: Manifest{Topic: c.Topic, StartsAt: c.StartsAt, EndsAt: c.EndsAt}}
	closed, err := replay(ctx, c, nil, func(pos Position, v Vote) error {
		if a.TotalAttempts >= 12_000_000 {
			return fmt.Errorf("prototype audit record limit exceeded")
		}
		if _, ok := a.Records[pos]; ok {
			return errors.New("consumer delivered position twice")
		}
		a.Records[pos] = v
		a.TotalAttempts++
		if _, ok := a.Canonical[v.Token]; !ok {
			a.Canonical[v.Token] = v
			for bit := 0; bit < 32; bit++ {
				if v.Choice&(1<<bit) != 0 {
					a.ChoiceCounts[bit]++
				}
			}
		}
		return nil
	})
	if err != nil {
		return AuditResult{}, err
	}
	for p, offset := range closed {
		a.Manifest.Partitions = append(a.Manifest.Partitions, PartitionEnd{int32(p), offset})
	}
	return a, nil
}

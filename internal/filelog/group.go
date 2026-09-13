package filelog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

// GroupConfig fixes a poll's global topology. Queue and batch bounds apply to
// each partition independently. All partitions belong to this local disk owner.
type GroupConfig struct {
	Directory                                string
	PollID                                   [16]byte
	StartsAt, EndsAt                         time.Time
	AllowedMask                              uint32
	Multiple                                 bool
	Partitions, BatchSize, QueuePerPartition int
	Linger                                   time.Duration
}

func (c GroupConfig) Partition(token [16]byte) int32 {
	var key [32]byte
	copy(key[:16], c.PollID[:])
	copy(key[16:], token[:])
	hash := sha256.Sum256(key[:])
	return int32(binary.BigEndian.Uint64(hash[:8]) % uint64(c.Partitions))
}

func (c GroupConfig) validate() error {
	if c.PollID == [16]byte{} || c.Partitions < 1 || c.Partitions > 256 {
		return ErrInvalid
	}

	return c.child(0).validate()
}

func (c GroupConfig) child(p int) Config {
	// Bind even an empty WAL to its partition. The routing hash still uses the
	// original full poll ID; the derived ID only binds physical WAL metadata.
	var identity [40]byte
	copy(identity[:20], "gigaquizz-file-shard")
	copy(identity[20:36], c.PollID[:])
	binary.BigEndian.PutUint32(identity[36:], uint32(p))
	h := sha256.Sum256(identity[:])
	var id [16]byte
	copy(id[:], h[:16])
	return Config{Directory: filepath.Join(c.Directory, fmt.Sprintf("partition-%04d", p)), PollID: id, StartsAt: c.StartsAt, EndsAt: c.EndsAt, AllowedMask: c.AllowedMask, Multiple: c.Multiple, Partitions: 1, BatchSize: c.BatchSize, QueuePerPartition: c.QueuePerPartition, Linger: c.Linger}
}

func (c GroupConfig) metadata() []byte {
	c.Directory = ""
	c.StartsAt, c.EndsAt = c.StartsAt.UTC(), c.EndsAt.UTC()
	b, _ := json.Marshal(struct {
		Version int
		Routing string
		Config  GroupConfig
	}{1, "sha256-poll-token-first8be-mod", c})
	return append(b, '\n')
}

type Group struct {
	c       GroupConfig
	writers []*Store
	closed  atomic.Bool
	now     func() time.Time
}

func NewGroup(ctx context.Context, c GroupConfig) (_ *Group, err error) {
	if err = c.validate(); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if err = os.Mkdir(c.Directory, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(c.Directory, "group.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	err = writeAll(f.Write, c.metadata())
	if err == nil {
		err = durableSync(f)
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	g := &Group{c: c, now: time.Now}
	defer func() {
		if err != nil {
			g.Close()
		}
	}()
	for p := 0; p < c.Partitions; p++ {
		w, createErr := New(ctx, c.child(p))
		if createErr != nil {
			return nil, createErr
		}
		g.writers = append(g.writers, w)
	}
	for _, path := range []string{c.Directory, filepath.Dir(c.Directory)} {
		f, openErr := os.Open(path)
		if openErr != nil {
			return nil, openErr
		}
		err = f.Sync()
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return nil, err
		}
	}
	return g, nil
}

// CheckGroup verifies the exact directory inventory and immutable topology.
// WAL contents are validated by RecoverGroup/GroupReplay, not by this check.
func CheckGroup(c GroupConfig) error {
	if err := c.validate(); err != nil {
		return err
	}
	if err := groupPath(c.Directory, true); err != nil {
		return err
	}
	items, err := os.ReadDir(c.Directory)
	if err != nil {
		return err
	}
	if len(items) != c.Partitions+1 {
		return errors.New("group partition inventory mismatch")
	}
	path := filepath.Join(c.Directory, "group.json")
	if err := groupPath(path, false); err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	b, readErr := io.ReadAll(io.LimitReader(f, 16385))
	closeErr := f.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if !bytes.Equal(b, c.metadata()) {
		return errors.New("immutable group metadata mismatch")
	}
	for p := 0; p < c.Partitions; p++ {
		d := c.child(p).Directory
		if err := groupPath(d, true); err != nil {
			return err
		}
		files, err := os.ReadDir(d)
		if err != nil {
			return err
		}
		if len(files) != 2 {
			return errors.New("partition file inventory mismatch")
		}
		for _, name := range []string{pollName, walName} {
			if err := groupPath(filepath.Join(d, name), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func groupPath(path string, directory bool) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || (directory && !info.IsDir()) || (!directory && !info.Mode().IsRegular()) {
		return errors.New("group path must be an owned regular file/directory")
	}
	return nil
}

func groupVisitor(c GroupConfig, p int, visit func(Position, Vote) error) func(Position, Vote) error {
	return func(pos Position, vote Vote) error {
		if c.Partition(vote.Token) != int32(p) {
			return errors.New("vote is in the wrong partition")
		}
		pos.Partition = int32(p)
		if visit != nil {
			return visit(pos, vote)
		}
		return nil
	}
}

func RecoverGroup(ctx context.Context, c GroupConfig) (_ *Group, err error) {
	if err = CheckGroup(c); err != nil {
		return nil, err
	}
	g := &Group{c: c, now: time.Now}
	defer func() {
		if err != nil {
			g.Close()
		}
	}()
	for p := 0; p < c.Partitions; p++ {
		w, recoveryErr := recoverWithVisitor(ctx, c.child(p), groupVisitor(c, p, nil))
		if recoveryErr != nil {
			return nil, recoveryErr
		}
		g.writers = append(g.writers, w)
		// A persisted CLOSED barrier in any shard is irreversible for the
		// whole poll, including after a restart with a rolled-back wall clock.
		w.mu.Lock()
		closed := w.admissionClosed
		w.mu.Unlock()
		if closed {
			g.closed.Store(true)
		}
	}
	return g, nil
}

func (g *Group) Submit(ctx context.Context, in Input) (Receipt, error) {
	if g.closed.Load() {
		return Receipt{}, ErrClosed
	}
	p := g.c.Partition(in.Token)
	r, err := g.writers[p].Submit(ctx, in)
	if errors.Is(err, ErrClosed) {
		g.closed.Store(true)
	}
	r.Partition = p
	return r, err
}

func (g *Group) Seal(ctx context.Context) (Manifest, error) {
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	if !g.closed.Load() {
		// Waiting is cancellable and does not close an otherwise open poll.
		// Latch before the first shard's potentially slow disk sync so clock
		// rollback during that sync cannot admit into another shard.
		if err := waitUntil(ctx, g.c.EndsAt.UTC(), g.now); err != nil {
			return Manifest{}, err
		}
		g.closed.Store(true)
	}
	var m Manifest
	for p, w := range g.writers {
		part, err := w.Seal(ctx)
		if err != nil {
			return Manifest{}, err
		}
		m.Partitions = append(m.Partitions, PartitionEnd{Partition: int32(p), Offset: part.Partitions[0].Offset})
	}
	return m, nil
}

func (g *Group) Close() {
	g.closed.Store(true)
	for _, w := range g.writers {
		w.Close()
	}
}
func (g *Group) Metrics() map[string]uint64 {
	out := make(map[string]uint64)
	for _, w := range g.writers {
		for k, v := range w.Metrics() {
			if k == "sync_max_ns" {
				if v > out[k] {
					out[k] = v
				}
			} else {
				out[k] += v
			}
		}
	}
	out["partitions"] = uint64(g.c.Partitions)
	return out
}

// ReplayGroupPartition verifies the entire group inventory, then streams one
// closed partition. Its caller can release an exact dedup map between shards.
func ReplayGroupPartition(ctx context.Context, c GroupConfig, partition int, visit func(Position, Vote) error) (PartitionEnd, error) {
	if err := CheckGroup(c); err != nil {
		return PartitionEnd{}, err
	}
	if partition < 0 || partition >= c.Partitions {
		return PartitionEnd{}, ErrInvalid
	}
	m, err := Replay(ctx, c.child(partition), groupVisitor(c, partition, visit))
	if err != nil {
		return PartitionEnd{}, err
	}
	return PartitionEnd{Partition: int32(partition), Offset: m.Partitions[0].Offset}, nil
}

// GroupReplay requires every shard's CLOSED barrier and rejects missing,
// extra, swapped or misrouted data. Discard all visitor results on any error.
func GroupReplay(ctx context.Context, c GroupConfig, visit func(Position, Vote) error) (Manifest, error) {
	if err := CheckGroup(c); err != nil {
		return Manifest{}, err
	}
	var m Manifest
	for p := 0; p < c.Partitions; p++ {
		part, err := ReplayGroupPartition(ctx, c, p, visit)
		if err != nil {
			return Manifest{}, err
		}
		m.Partitions = append(m.Partitions, part)
	}
	return m, nil
}

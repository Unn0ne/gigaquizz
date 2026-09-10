package filelog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type answer struct {
	receipt FrameReceipt
	err     error
}
type job struct {
	data     []byte
	count    int
	admitted time.Time
	done     chan answer
}

type Store struct {
	c                                           Config
	f                                           *os.File
	mu                                          sync.Mutex
	admissionClosed, queueClosed, sealRequested bool
	poison                                      error
	manifest                                    Manifest
	queuedVotes                                 int
	queue                                       chan *job
	done                                        chan struct{}
	now                                         func() time.Time
	// Hooks are installed before submissions, never concurrently with work.
	write                                                    func([]byte) (int, error)
	syncFile                                                 func() error
	activeVotes                                              atomic.Uint64
	durableVotes, durableFrames, groups, walBytes            atomic.Uint64
	syncNS, maxSyncNS, acknowledged, unknownVotes, busyVotes atomic.Uint64
}

// New only creates a new final directory; it never removes or reopens existing
// state. Its parent directory must already exist. Failed initialization leaves
// diagnostic files in place and cannot start admission.
func New(ctx context.Context, c Config) (_ *Store, err error) {
	if err = c.validate(); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	c.StartsAt, c.EndsAt = c.StartsAt.UTC(), c.EndsAt.UTC()
	if err = os.Mkdir(c.Directory, 0700); err != nil {
		return nil, err
	}
	dir, err := os.Open(c.Directory)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	parent, err := os.Open(filepath.Dir(filepath.Clean(c.Directory)))
	if err != nil {
		return nil, err
	}
	defer parent.Close()
	f, err := os.OpenFile(filepath.Join(c.Directory, walName), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, err
	}
	poll, err := os.OpenFile(filepath.Join(c.Directory, pollName), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	err = writeAll(poll.Write, pollBytes(c))
	if err == nil {
		err = durableSync(poll)
	}
	if closeErr := poll.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	if err = writeAll(f.Write, fileHeader(c)); err != nil {
		return nil, err
	}
	if err = durableSync(f); err != nil {
		return nil, err
	}
	if err = dir.Sync(); err != nil {
		return nil, err
	}
	if err = parent.Sync(); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	s := &Store{c: c, f: f, queue: make(chan *job, c.QueuePerPartition), done: make(chan struct{}), now: time.Now}
	s.write, s.syncFile = f.Write, func() error { return durableSync(f) }
	s.walBytes.Store(fileHeaderBytes)
	go s.run()
	return s, nil
}

// SubmitFrame copies its inputs before returning. The caller must not mutate
// them concurrently with this call. One frame must fit BatchSize and contain
// 1..4096 votes. Admission is sampled under the queue lock after capacity is
// reserved; context expiry after enqueue means an unknown outcome.
func (s *Store) SubmitFrame(ctx context.Context, inputs []Input) (FrameReceipt, error) {
	if len(inputs) == 0 || len(inputs) > maxFrameVotes || len(inputs) > s.c.BatchSize {
		return FrameReceipt{}, ErrInvalid
	}
	for _, in := range inputs {
		if in.Token == [16]byte{} || !s.c.validChoice(in.Choice) {
			return FrameReceipt{}, ErrInvalid
		}
	}
	if err := ctx.Err(); err != nil {
		return FrameReceipt{}, err
	}
	s.mu.Lock()
	if s.poison != nil {
		err := s.poison
		s.mu.Unlock()
		return FrameReceipt{}, err
	}
	if s.admissionClosed {
		s.mu.Unlock()
		return FrameReceipt{}, ErrClosed
	}
	// Observe the deadline even under saturation, so a later clock rollback
	// cannot reopen a poll whose deadline this owner already observed.
	observed := s.now()
	if !observed.Before(s.c.EndsAt) {
		s.admissionClosed = true
		s.mu.Unlock()
		return FrameReceipt{}, ErrClosed
	}
	if observed.Before(s.c.StartsAt) {
		s.mu.Unlock()
		return FrameReceipt{}, ErrNotOpen
	}
	if s.queuedVotes+len(inputs) > s.c.QueuePerPartition {
		s.mu.Unlock()
		s.busyVotes.Add(uint64(len(inputs)))
		return FrameReceipt{}, ErrBusy
	}
	admitted := s.now()
	if admitted.Before(s.c.StartsAt) {
		s.mu.Unlock()
		return FrameReceipt{}, ErrNotOpen
	}
	if !admitted.Before(s.c.EndsAt) {
		s.admissionClosed = true
		s.mu.Unlock()
		return FrameReceipt{}, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return FrameReceipt{}, err
	}
	j := &job{data: encodeInputs(inputs, admitted), count: len(inputs), admitted: admitted, done: make(chan answer, 1)}
	s.queuedVotes += len(inputs)
	s.queue <- j // The logical-vote capacity also bounds the number of frames.
	s.mu.Unlock()
	select {
	case a := <-j.done:
		if a.err == nil {
			s.acknowledged.Add(uint64(j.count))
		} else {
			s.unknownVotes.Add(uint64(j.count))
		}
		return a.receipt, a.err
	case <-ctx.Done():
		s.unknownVotes.Add(uint64(j.count))
		return FrameReceipt{}, fmt.Errorf("%w: %v", ErrUnknown, ctx.Err())
	}
}

func (s *Store) stopQueueLocked() {
	s.admissionClosed = true
	if !s.queueClosed {
		s.queueClosed = true
		close(s.queue)
	}
}

// Seal waits for the original deadline, closes admission, drains admitted
// frames, and durably writes CLOSED. Its context cannot cancel a disk syscall.
func (s *Store) Seal(ctx context.Context) (Manifest, error) {
	s.mu.Lock()
	alreadyStopping := s.queueClosed
	s.mu.Unlock()
	if !alreadyStopping {
		if err := waitUntil(ctx, s.c.EndsAt, s.now); err != nil {
			return Manifest{}, err
		}
		s.mu.Lock()
		if !s.queueClosed {
			s.sealRequested = true
			s.stopQueueLocked()
		}
		s.mu.Unlock()
	}
	select {
	case <-ctx.Done():
		return Manifest{}, ctx.Err()
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.poison != nil {
			return Manifest{}, s.poison
		}
		if len(s.manifest.Partitions) == 0 {
			return Manifest{}, ErrClosed
		}
		return Manifest{Partitions: append([]PartitionEnd(nil), s.manifest.Partitions...)}, nil
	}
}

// Close drains pending attempts and joins the writer. It does not manufacture
// CLOSED before the deadline. It may wait on an in-flight uninterruptible sync.
func (s *Store) Close() {
	s.mu.Lock()
	s.stopQueueLocked()
	s.mu.Unlock()
	<-s.done
}

func (s *Store) fail(err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poison == nil {
		s.poison = fmt.Errorf("%w: %v", ErrUnknown, err)
	}
	s.stopQueueLocked()
	return s.poison
}

func (s *Store) release(j *job) {
	s.mu.Lock()
	s.queuedVotes -= j.count
	s.mu.Unlock()
}

func (s *Store) syncGroup(data []byte) error {
	if err := writeAll(s.write, data); err != nil {
		return err
	}
	s.walBytes.Add(uint64(len(data)))
	started := time.Now()
	err := s.syncFile()
	elapsed := uint64(time.Since(started))
	s.syncNS.Add(elapsed)
	for old := s.maxSyncNS.Load(); elapsed > old && !s.maxSyncNS.CompareAndSwap(old, elapsed); old = s.maxSyncNS.Load() {
	}
	if err == nil {
		s.groups.Add(1)
	}
	return err
}

func (s *Store) run() {
	defer close(s.done)
	defer func() {
		if err := s.f.Close(); err != nil {
			s.fail(err)
		}
	}()
	var sequence int64
	var batch []*job
	var votes int
	var timer *time.Timer
	var tick <-chan time.Time
	data := make([]byte, 0, min(s.c.BatchSize*entryBytes, 1<<20))
	stopTimer := func() {
		if timer != nil {
			timer.Stop()
			timer = nil
			tick = nil
		}
	}
	defer stopTimer()
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		stopTimer()
		data = data[:0]
		var err error
		if int64(len(batch)) > math.MaxInt64-sequence {
			err = errors.New("frame sequence exhausted")
		}
		if err == nil {
			for i, j := range batch {
				finishFrame(j.data, sequence+int64(i), kindVotes, uint32(j.count))
				data = append(data, j.data...)
			}
			err = s.syncGroup(data)
		}
		if err != nil {
			err = s.fail(err)
		} else {
			s.durableVotes.Add(uint64(votes))
			s.durableFrames.Add(uint64(len(batch)))
		}
		for i, j := range batch {
			j.done <- answer{receipt: FrameReceipt{Partition: 0, Offset: sequence + int64(i), Count: uint32(j.count), AdmittedAt: j.admitted}, err: err}
			batch[i] = nil
		}
		sequence += int64(len(batch))
		batch = batch[:0]
		votes = 0
		s.activeVotes.Store(0)
		return err
	}
	drainFailure := func(err error, pending *job) {
		if pending != nil {
			s.release(pending)
			pending.done <- answer{err: err}
		}
		for j := range s.queue {
			s.release(j)
			j.done <- answer{err: err}
		}
	}
	for {
		select {
		case j, ok := <-s.queue:
			if !ok {
				if err := flush(); err != nil {
					return
				}
				s.mu.Lock()
				seal := s.sealRequested
				s.mu.Unlock()
				if seal {
					closed := make([]byte, frameHeaderBytes)
					finishFrame(closed, sequence, kindClosed, 0)
					if err := s.syncGroup(closed); err != nil {
						s.fail(err)
						return
					}
					s.mu.Lock()
					s.manifest = Manifest{Partitions: []PartitionEnd{{Partition: 0, Offset: sequence}}}
					s.mu.Unlock()
				}
				return
			}
			if votes+j.count > s.c.BatchSize {
				// Keep this dequeued job charged to the queue while the previous
				// batch syncs; queue + one active batch is the memory bound.
				if err := flush(); err != nil {
					drainFailure(err, j)
					return
				}
			}
			s.release(j)
			batch = append(batch, j)
			votes += j.count
			s.activeVotes.Store(uint64(votes))
			if votes == j.count && s.c.Linger > 0 {
				timer = time.NewTimer(s.c.Linger)
				tick = timer.C
			}
			if votes >= s.c.BatchSize || s.c.Linger == 0 {
				if err := flush(); err != nil {
					drainFailure(err, nil)
					return
				}
			}
		case <-tick:
			if err := flush(); err != nil {
				drainFailure(err, nil)
				return
			}
		}
	}
}

func writeAll(write func([]byte) (int, error), b []byte) error {
	for len(b) != 0 {
		n, err := write(b)
		if n < 0 || n > len(b) {
			return io.ErrShortWrite
		}
		b = b[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func (s *Store) Metrics() map[string]uint64 {
	s.mu.Lock()
	queued, poisoned := s.queuedVotes, s.poison != nil
	s.mu.Unlock()
	m := map[string]uint64{
		"queued_votes": uint64(queued), "active_votes": s.activeVotes.Load(),
		"durable_votes": s.durableVotes.Load(), "durable_frames": s.durableFrames.Load(),
		"group_syncs": s.groups.Load(), "wal_bytes": s.walBytes.Load(),
		"sync_total_ns": s.syncNS.Load(), "sync_max_ns": s.maxSyncNS.Load(),
		"acknowledged_votes": s.acknowledged.Load(), "unknown_votes": s.unknownVotes.Load(), "busy_votes": s.busyVotes.Load(),
	}
	if poisoned {
		m["poisoned"] = 1
	}
	return m
}

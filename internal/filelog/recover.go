package filelog

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// DurableSync exposes the same platform-specific durability boundary used by
// vote ACKs, so repository metadata/results do not silently use weaker sync.
func DurableSync(f *os.File) error { return durableSync(f) }

// Recover exclusively owns and validates an existing journal. Only a short
// final frame is truncated; any complete CRC/config/sequence failure leaves
// the file unchanged. Surviving unacknowledged complete frames remain votes.
// The original deadline is retained. CLOSED is irreversible across restarts.
func Recover(ctx context.Context, c Config) (_ *Store, err error) {
	if err = c.validate(); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	for _, name := range []string{"", walName, pollName} {
		info, statErr := os.Lstat(filepath.Join(c.Directory, name))
		if statErr != nil {
			return nil, statErr
		}
		if info.Mode()&os.ModeSymlink != 0 || (name == "" && !info.IsDir()) || (name != "" && !info.Mode().IsRegular()) {
			return nil, fmt.Errorf("journal path is not a regular owned file/directory")
		}
	}
	f, err := os.OpenFile(filepath.Join(c.Directory, walName), os.O_RDWR|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, fmt.Errorf("journal already owned: %w", err)
	}
	r, err := scanFile(ctx, c, f, nil)
	if err != nil {
		return nil, err
	}
	if r.IncompleteTail {
		if err = f.Truncate(r.ValidBytes); err != nil {
			return nil, err
		}
	}
	// Before serving new ACKs, persist both a repaired tail and any complete
	// records that survived an earlier unknown outcome.
	if err = durableSync(f); err != nil {
		return nil, err
	}
	// An interrupted initial New may leave readable metadata whose file or
	// directory sync never completed. Re-establish the whole dependency chain
	// before this owner can return another durable attempt receipt.
	metadata, openErr := os.OpenFile(filepath.Join(c.Directory, pollName), os.O_RDWR|syscall.O_NOFOLLOW, 0)
	if openErr != nil {
		return nil, openErr
	}
	err = durableSync(metadata)
	if closeErr := metadata.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	for _, path := range []string{c.Directory, filepath.Dir(filepath.Clean(c.Directory))} {
		directory, openErr := os.Open(path)
		if openErr != nil {
			return nil, openErr
		}
		err = directory.Sync()
		if closeErr := directory.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return nil, err
		}
	}
	if _, err = f.Seek(r.ValidBytes, io.SeekStart); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	c.StartsAt, c.EndsAt = c.StartsAt.UTC(), c.EndsAt.UTC()
	capacity := c.QueuePerPartition
	if r.Closed {
		capacity = 0
	}
	s := &Store{c: c, f: f, queue: make(chan *job, capacity), done: make(chan struct{}), now: time.Now, initialSequence: int64(r.Frames)}
	s.write, s.syncFile = f.Write, func() error { return durableSync(f) }
	s.walBytes.Store(uint64(r.ValidBytes))
	s.durableVotes.Store(r.Votes)
	if r.Closed {
		s.manifest = r.Manifest
		s.admissionClosed, s.queueClosed = true, true
		close(s.queue)
		if err = f.Close(); err != nil {
			return nil, err
		}
		close(s.done)
		return s, nil
	}
	s.durableFrames.Store(r.Frames)
	if !time.Now().Before(c.EndsAt) {
		s.sealRequested = true
		s.stopQueueLocked()
	}
	go s.run()
	return s, nil
}

// Package filestore implements the full poll API on one owned local disk.
// Acknowledgements depend on that disk; unique first-token choices are counted
// after closing, without retaining a per-token receipt cache during admission.
package filestore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"gigaquizz/internal/filelog"
	"gigaquizz/internal/poll"
)

type Config struct {
	Directory             string
	MaxUnique             uint64
	MaxPartitionUnique    uint64
	Partitions            int
	BatchSize, QueueVotes int
	Linger                time.Duration
}

type rootMarker struct {
	Format  string `json:"format"`
	Version int    `json:"version"`
}

type definition struct {
	Version    int           `json:"version"`
	Partitions int           `json:"partitions,omitempty"`
	Poll       poll.Poll     `json:"poll"`
	MaxUnique  uint64        `json:"max_unique"`
	BatchSize  int           `json:"batch_size"`
	QueueVotes int           `json:"queue_votes"`
	Linger     time.Duration `json:"linger_ns"`
}

type journalWriter interface {
	Submit(context.Context, filelog.Input) (filelog.Receipt, error)
	Seal(context.Context) (filelog.Manifest, error)
	Close()
	Metrics() map[string]uint64
}

type entry struct {
	def             definition
	directory       string
	mu              sync.Mutex
	finalizeMu      sync.Mutex
	writer          journalWriter
	retainedMetrics map[string]uint64 // Immutable snapshot for this startup only; never persisted.
	result          *poll.Results
	finalError      error // A failed exact calculation needs operator intervention.
	validationError error // A historical journal/result failed verification.
	needsValidation bool
	closing         bool // An observed deadline/finalization cannot reopen on clock rollback.
	sealed          *filelog.Manifest
}

type Store struct {
	c             Config
	owner         *os.File
	mu            sync.RWMutex
	createMu      sync.Mutex
	entries       map[string]*entry
	closed        bool
	creationError error
	ops           sync.WaitGroup
	closeOnce     sync.Once
	now           func() time.Time
	// Test-only recovery barrier, configured before concurrent operations.
	recoverFinalization func(context.Context, *entry) (journalWriter, error)
}

var _ poll.Repository = (*Store)(nil)
var errStopped = errors.New("file repository is closed")

func normalizeConfig(c Config) (Config, error) {
	if c.Directory == "" {
		return c, errors.New("data directory is required")
	}
	var err error
	c.Directory, err = filepath.Abs(c.Directory)
	if err != nil {
		return c, err
	}
	if c.MaxUnique == 0 {
		c.MaxUnique = 120000000
	}
	if c.MaxPartitionUnique == 0 {
		c.MaxPartitionUnique = c.MaxUnique
	}
	if c.Partitions == 0 {
		c.Partitions = 1
	}
	if c.BatchSize == 0 {
		c.BatchSize = 4096
	}
	if c.QueueVotes == 0 {
		c.QueueVotes = 65536
	}
	// Zero explicitly requests immediate flush. Configuration-loading callers
	// supply the ordinary 2 ms default; do not silently replace an explicit zero.
	if c.MaxUnique > 200000000 || c.MaxPartitionUnique > 200000000 || c.Partitions < 1 || c.Partitions > 256 || c.BatchSize < 1 || c.BatchSize > 131072 || c.QueueVotes < 1 || c.QueueVotes > 1048576 || c.Linger < 0 || c.Linger > time.Second {
		return c, errors.New("invalid file repository bounds")
	}
	return c, nil
}

func New(ctx context.Context, config Config) (*Store, error) {
	return newWithClock(ctx, config, time.Now)
}

// The clock is fixed before recovery and admission begin. This private seam
// lets restart tests exercise clock rollback without changing the host clock.
func newWithClock(ctx context.Context, config Config, now func() time.Time) (_ *Store, err error) {
	c, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := bootstrapRoot(c.Directory, nil); err != nil {
		return nil, err
	}
	items, err := os.ReadDir(c.Directory)
	if err != nil {
		return nil, err
	}
	if len(items) != 3 {
		return nil, errors.New("incomplete or foreign data directory inventory")
	}
	for _, item := range items {
		if item.Type()&os.ModeSymlink != 0 || (item.Name() != "marker.json" && item.Name() != ".owner" && item.Name() != "polls") {
			return nil, errors.New("foreign entry in data directory")
		}
	}
	markerPath := filepath.Join(c.Directory, "marker.json")
	var marker rootMarker
	if err := readJSON(markerPath, &marker); err != nil {
		return nil, err
	}
	if marker != (rootMarker{"gigaquizz-filestore", 1}) {
		return nil, errors.New("unknown data directory format")
	}
	if err := regular(filepath.Join(c.Directory, ".owner")); err != nil {
		return nil, err
	}
	info, err := os.Lstat(filepath.Join(c.Directory, "polls"))
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("invalid polls directory")
	}
	flags := os.O_RDWR | syscall.O_NOFOLLOW
	owner, err := os.OpenFile(filepath.Join(c.Directory, ".owner"), flags, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(owner.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		owner.Close()
		return nil, errors.New("data directory already has an owner")
	}
	s := &Store{c: c, owner: owner, entries: make(map[string]*entry), now: now}
	defer func() {
		if err != nil {
			s.Close()
		}
	}()
	if err = syncDir(c.Directory); err != nil {
		return nil, err
	}
	if err = syncDir(filepath.Dir(c.Directory)); err != nil {
		return nil, err
	}
	if err = syncMetadata(markerPath); err != nil {
		return nil, err
	}
	if err = s.load(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) begin(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return errStopped
	}
	s.ops.Add(1)
	return nil
}

func (s *Store) Close() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		s.ops.Wait()
		for _, e := range s.entries {
			if e.writer != nil {
				e.writer.Close()
			}
		}
		_ = s.owner.Close()
	})
}

func (s *Store) Ping(ctx context.Context) error {
	if err := s.begin(ctx); err != nil {
		return err
	}
	defer s.ops.Done()
	s.mu.RLock()
	creationError := s.creationError
	entries := make([]*entry, 0, len(s.entries))
	now := s.now()
	for _, e := range s.entries {
		// Historical failures belong to that poll and do not block readiness.
		if now.Before(e.def.Poll.EndsAt) {
			entries = append(entries, e)
		}
	}
	s.mu.RUnlock()
	if creationError != nil {
		return creationError
	}
	for _, e := range entries {
		e.mu.Lock()
		failed, w := e.validationError != nil || e.finalError != nil, e.writer
		e.mu.Unlock()
		if failed || (w != nil && w.Metrics()["poisoned"] != 0) {
			return errors.New("file writer or finalization failed")
		}
	}
	return nil
}

func (e *entry) logConfig() filelog.Config {
	id, _ := parseUUID(e.def.Poll.ID)
	return filelog.Config{Directory: filepath.Join(e.directory, "journal"), PollID: id, StartsAt: e.def.Poll.StartsAt, EndsAt: e.def.Poll.EndsAt, AllowedMask: uint32(1)<<len(e.def.Poll.Options) - 1, Multiple: e.def.Poll.Type == "multiple", Partitions: 1, BatchSize: e.def.BatchSize, QueuePerPartition: e.def.QueueVotes, Linger: e.def.Linger}
}

func clonePoll(p poll.Poll) poll.Poll {
	p.Options = append([]poll.Option(nil), p.Options...)
	if p.FinalizedAt != nil {
		final := *p.FinalizedAt
		p.FinalizedAt = &final
	}
	return p
}

// The WAL stores both schedule endpoints as signed 64-bit nanoseconds. Check
// the round trip before publishing metadata, including when a minute crosses
// the upper boundary; calendar years alone cannot express that exact range.
func validJournalWindow(starts, ends time.Time) bool {
	return ends.Sub(starts) == time.Minute &&
		time.Unix(0, starts.UnixNano()).Equal(starts) &&
		time.Unix(0, ends.UnixNano()).Equal(ends)
}

func (e *entry) publicPoll() poll.Poll {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := clonePoll(e.def.Poll)
	if e.result != nil {
		final := e.result.CalculatedAt
		p.FinalizedAt = &final
	}
	return p
}

func (s *Store) lookup(id string) *entry {
	if _, err := parseUUID(id); err != nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.entries[strings.ToLower(id)]
}

func (s *Store) Create(ctx context.Context, in poll.CreateInput) (poll.Poll, error) {
	if err := s.begin(ctx); err != nil {
		return poll.Poll{}, err
	}
	defer s.ops.Done()
	in.Options = append([]string(nil), in.Options...)
	if err := in.Validate(); err != nil {
		return poll.Poll{}, err
	}
	s.createMu.Lock()
	defer s.createMu.Unlock()
	s.mu.RLock()
	creationError := s.creationError
	s.mu.RUnlock()
	if creationError != nil {
		return poll.Poll{}, creationError
	}
	starts := s.now().UTC()
	if in.StartsAt != nil {
		starts = in.StartsAt.UTC()
	}
	ends := starts.Add(time.Minute)
	if !validJournalWindow(starts, ends) {
		return poll.Poll{}, errors.New("invalid poll start date")
	}
	s.mu.RLock()
	for _, e := range s.entries {
		if e.def.Poll.StartsAt.Before(ends) && e.def.Poll.EndsAt.After(starts) {
			s.mu.RUnlock()
			return poll.Poll{}, poll.ErrOverlap
		}
	}
	s.mu.RUnlock()
	id, err := uuid()
	if err != nil {
		return poll.Poll{}, err
	}
	p := poll.Poll{ID: id, Question: in.Question, Type: in.Type, StartsAt: starts, EndsAt: ends, CreatedAt: s.now().UTC()}
	for i, label := range in.Options {
		p.Options = append(p.Options, poll.Option{ID: i + 1, Label: label})
	}
	// Prepared directories are never public polls. A crash at any preparation
	// step leaves an ignored, inspectable orphan, rather than an incomplete
	// published poll that could prevent other polls from recovering.
	finalDirectory := filepath.Join(s.c.Directory, "polls", id)
	e := &entry{def: definition{Version: 1, Poll: p, MaxUnique: s.c.MaxUnique, BatchSize: s.c.BatchSize, QueueVotes: s.c.QueueVotes, Linger: s.c.Linger}, directory: filepath.Join(s.c.Directory, "polls", ".creating-"+id)}
	if s.c.Partitions > 1 {
		e.def.Version = 2
		e.def.Partitions = s.c.Partitions
	}
	if err := os.Mkdir(e.directory, 0700); err != nil {
		return poll.Poll{}, err
	}
	if err := syncDir(filepath.Dir(e.directory)); err != nil {
		return poll.Poll{}, err
	}
	if err := createJSON(filepath.Join(e.directory, "definition.json"), e.def); err != nil {
		return poll.Poll{}, err
	}
	w, err := e.newWriter(ctx)
	if err != nil {
		return poll.Poll{}, err
	}
	w.Close()
	if _, err := os.Lstat(finalDirectory); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return poll.Poll{}, errors.New("poll target already exists")
		}
		return poll.Poll{}, err
	}
	if err := os.Rename(e.directory, finalDirectory); err != nil {
		return poll.Poll{}, err
	}
	e.directory = finalDirectory
	if err := syncDir(filepath.Dir(finalDirectory)); err != nil {
		// Rename may already be visible, so do not permit an overlapping
		// replacement after an uncertain publication. Restart revalidates it.
		s.mu.Lock()
		s.creationError = err
		s.mu.Unlock()
		return poll.Poll{}, err
	}
	s.mu.Lock()
	s.entries[id] = e
	s.mu.Unlock()
	return clonePoll(p), nil
}

func (s *Store) Get(ctx context.Context, id string) (poll.Poll, error) {
	if err := s.begin(ctx); err != nil {
		return poll.Poll{}, err
	}
	defer s.ops.Done()
	e := s.lookup(id)
	if e == nil {
		return poll.Poll{}, poll.ErrNotFound
	}
	e.mu.Lock()
	err := e.validationError
	e.mu.Unlock()
	if err != nil {
		return poll.Poll{}, err
	}
	return e.publicPoll(), nil
}

func (s *Store) List(ctx context.Context) ([]poll.Poll, error) {
	if err := s.begin(ctx); err != nil {
		return nil, err
	}
	defer s.ops.Done()
	s.mu.RLock()
	entries := make([]*entry, 0, len(s.entries))
	for _, e := range s.entries {
		entries = append(entries, e)
	}
	s.mu.RUnlock()
	result := make([]poll.Poll, 0, len(entries))
	for _, e := range entries {
		result = append(result, e.publicPoll())
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].StartsAt.Equal(result[j].StartsAt) {
			return result[i].ID < result[j].ID
		}
		return result[i].StartsAt.After(result[j].StartsAt)
	})
	if len(result) > 100 {
		result = result[:100]
	}
	return result, nil
}

func (s *Store) Vote(ctx context.Context, id, token string, choices []int) (poll.Receipt, error) {
	if err := s.begin(ctx); err != nil {
		return poll.Receipt{}, err
	}
	defer s.ops.Done()
	e := s.lookup(id)
	if e == nil {
		return poll.Receipt{Status: "not_found"}, nil
	}
	key, err := parseToken(token)
	if err != nil {
		return poll.Receipt{Status: "invalid"}, nil
	}
	choices, err = poll.NormalizeChoices(choices)
	if err != nil || (e.def.Poll.Type != "multiple" && len(choices) != 1) {
		return poll.Receipt{Status: "invalid"}, nil
	}
	var mask uint32
	for _, choice := range choices {
		if choice > len(e.def.Poll.Options) {
			return poll.Receipt{Status: "invalid"}, nil
		}
		mask |= 1 << (choice - 1)
	}
	e.mu.Lock()
	if e.validationError != nil {
		err := e.validationError
		e.mu.Unlock()
		return poll.Receipt{}, err
	}
	if e.result != nil || e.closing {
		e.mu.Unlock()
		return poll.Receipt{Status: "closed"}, nil
	}
	now := s.now()
	if !now.Before(e.def.Poll.EndsAt) {
		// This is a poll-wide irreversible observation, even when no lazy
		// writer has been opened or a different partition sees the next vote.
		e.closing = true
		e.mu.Unlock()
		return poll.Receipt{Status: "closed"}, nil
	}
	if now.Before(e.def.Poll.StartsAt) {
		e.mu.Unlock()
		return poll.Receipt{Status: "not_open"}, nil
	}
	if e.writer == nil {
		e.writer, err = e.recoverWriter(ctx)
		if err != nil {
			e.mu.Unlock()
			return poll.Receipt{}, err
		}
	}
	w := e.writer
	e.mu.Unlock()
	r, err := w.Submit(ctx, filelog.Input{Token: key, Choice: mask})
	if err != nil {
		switch {
		case errors.Is(err, filelog.ErrNotOpen):
			return poll.Receipt{Status: "not_open"}, nil
		case errors.Is(err, filelog.ErrClosed):
			// One partition can already have observed the deadline or CLOSED.
			// Persist that decision in the common in-memory admission gate.
			e.mu.Lock()
			e.closing = true
			e.mu.Unlock()
			return poll.Receipt{Status: "closed"}, nil
		case errors.Is(err, filelog.ErrInvalid):
			return poll.Receipt{Status: "invalid"}, nil
		case errors.Is(err, filelog.ErrBusy):
			return poll.Receipt{Status: "busy"}, nil
		default:
			return poll.Receipt{}, err
		}
	}
	accepted := r.AdmittedAt
	return poll.Receipt{Status: "recorded", Choices: choices, AcceptedAt: &accepted}, nil
}

func (s *Store) Results(ctx context.Context, id string) (poll.Results, error) {
	if err := s.begin(ctx); err != nil {
		return poll.Results{}, err
	}
	defer s.ops.Done()
	e := s.lookup(id)
	if e == nil {
		return poll.Results{}, poll.ErrNotFound
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.validationError != nil {
		return poll.Results{}, e.validationError
	}
	if e.result != nil {
		r := *e.result
		r.Options = append([]poll.OptionCount(nil), r.Options...)
		return r, nil
	}
	r := poll.Results{PollID: e.def.Poll.ID, State: e.def.Poll.State(s.now()), Pending: true, CalculatedAt: s.now().UTC()}
	for _, option := range e.def.Poll.Options {
		r.Options = append(r.Options, poll.OptionCount{ID: option.ID, Label: option.Label})
	}
	return r, nil
}

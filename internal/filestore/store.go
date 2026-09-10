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
	BatchSize, QueueVotes int
	Linger                time.Duration
}

type rootMarker struct {
	Format  string `json:"format"`
	Version int    `json:"version"`
}

type definition struct {
	Version    int           `json:"version"`
	Poll       poll.Poll     `json:"poll"`
	MaxUnique  uint64        `json:"max_unique"`
	BatchSize  int           `json:"batch_size"`
	QueueVotes int           `json:"queue_votes"`
	Linger     time.Duration `json:"linger_ns"`
}

type entry struct {
	def        definition
	directory  string
	mu         sync.Mutex
	finalizeMu sync.Mutex
	writer     *filelog.Store
	result     *poll.Results
	finalError error // A failed exact calculation needs operator intervention.
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
	if c.BatchSize == 0 {
		c.BatchSize = 4096
	}
	if c.QueueVotes == 0 {
		c.QueueVotes = 65536
	}
	if c.Linger == 0 {
		c.Linger = 2 * time.Millisecond
	}
	if c.MaxUnique > 200000000 || c.BatchSize < 1 || c.BatchSize > 131072 || c.QueueVotes < 1 || c.QueueVotes > 1048576 || c.Linger < 0 || c.Linger > time.Second {
		return c, errors.New("invalid file repository bounds")
	}
	return c, nil
}

func New(ctx context.Context, config Config) (_ *Store, err error) {
	c, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ensureDirectory(c.Directory); err != nil {
		return nil, err
	}
	items, err := os.ReadDir(c.Directory)
	if err != nil {
		return nil, err
	}
	markerPath := filepath.Join(c.Directory, "marker.json")
	_, markerErr := os.Lstat(markerPath)
	if errors.Is(markerErr, os.ErrNotExist) && len(items) != 0 {
		return nil, errors.New("refusing nonempty unowned data directory")
	}
	if markerErr != nil && !errors.Is(markerErr, os.ErrNotExist) {
		return nil, markerErr
	}
	for _, item := range items {
		if item.Type()&os.ModeSymlink != 0 || (item.Name() != "marker.json" && item.Name() != ".owner" && item.Name() != "polls") {
			return nil, errors.New("foreign entry in data directory")
		}
	}
	marker := rootMarker{"gigaquizz-filestore", 1}
	flags := os.O_RDWR | syscall.O_NOFOLLOW
	if markerErr == nil {
		var got rootMarker
		if err := readJSON(markerPath, &got); err != nil {
			return nil, err
		}
		if got != marker {
			return nil, errors.New("unknown data directory format")
		}
		if err := regular(filepath.Join(c.Directory, ".owner")); err != nil {
			return nil, err
		}
	} else {
		flags |= os.O_CREATE | os.O_EXCL
	}
	owner, err := os.OpenFile(filepath.Join(c.Directory, ".owner"), flags, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(owner.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		owner.Close()
		return nil, errors.New("data directory already has an owner")
	}
	s := &Store{c: c, owner: owner, entries: make(map[string]*entry), now: time.Now}
	defer func() {
		if err != nil {
			s.Close()
		}
	}()
	if errors.Is(markerErr, os.ErrNotExist) {
		if err = createJSON(markerPath, marker); err != nil {
			return nil, err
		}
	} else {
		var got rootMarker
		if err = readJSON(markerPath, &got); err != nil {
			return nil, err
		}
		if got != marker {
			return nil, errors.New("unknown data directory format")
		}
	}
	if err = ensureDirectory(filepath.Join(c.Directory, "polls")); err != nil {
		return nil, err
	}
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
	if _, err = s.FinalizeDue(ctx); err != nil {
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
	defer s.mu.RUnlock()
	if s.creationError != nil {
		return s.creationError
	}
	for _, e := range s.entries {
		e.mu.Lock()
		poisoned := e.finalError != nil || (e.writer != nil && e.writer.Metrics()["poisoned"] != 0)
		e.mu.Unlock()
		if poisoned {
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
	if starts.Year() < 1678 || starts.Year() > 2260 {
		return poll.Poll{}, errors.New("invalid poll start date")
	}
	ends := starts.Add(time.Minute)
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
	if err := os.Mkdir(e.directory, 0700); err != nil {
		return poll.Poll{}, err
	}
	if err := syncDir(filepath.Dir(e.directory)); err != nil {
		return poll.Poll{}, err
	}
	if err := createJSON(filepath.Join(e.directory, "definition.json"), e.def); err != nil {
		return poll.Poll{}, err
	}
	w, err := filelog.New(ctx, e.logConfig())
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
	if e.result != nil {
		e.mu.Unlock()
		return poll.Receipt{Status: "closed"}, nil
	}
	if e.writer == nil {
		now := s.now()
		if now.Before(e.def.Poll.StartsAt) {
			e.mu.Unlock()
			return poll.Receipt{Status: "not_open"}, nil
		}
		if !now.Before(e.def.Poll.EndsAt) {
			e.mu.Unlock()
			return poll.Receipt{Status: "closed"}, nil
		}
		e.writer, err = filelog.Recover(ctx, e.logConfig())
		if err != nil {
			e.mu.Unlock()
			return poll.Receipt{}, err
		}
	}
	w := e.writer
	e.mu.Unlock()
	r, err := w.SubmitFrame(ctx, []filelog.Input{{Token: key, Choice: mask}})
	if err != nil {
		switch {
		case errors.Is(err, filelog.ErrNotOpen):
			return poll.Receipt{Status: "not_open"}, nil
		case errors.Is(err, filelog.ErrClosed):
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

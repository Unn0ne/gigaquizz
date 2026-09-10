package filestore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"

	"gigaquizz/internal/filelog"
	"gigaquizz/internal/poll"
)

type diskResult struct {
	Version        int              `json:"version"`
	DefinitionHash string           `json:"definition_hash"`
	Results        poll.Results     `json:"results"`
	Manifest       filelog.Manifest `json:"manifest"`
	Checksum       string           `json:"checksum"`
}

func hashJSON(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func (s *Store) FinalizeDue(ctx context.Context) (int, error) {
	if err := s.begin(ctx); err != nil {
		return 0, err
	}
	defer s.ops.Done()
	s.mu.RLock()
	var due []*entry
	now := s.now()
	for _, e := range s.entries {
		if !now.Before(e.def.Poll.EndsAt) {
			due = append(due, e)
		}
	}
	s.mu.RUnlock()
	sort.Slice(due, func(i, j int) bool { return due[i].def.Poll.EndsAt.Before(due[j].def.Poll.EndsAt) })
	n := 0
	var failures []error
	for _, e := range due {
		changed, err := s.finalize(ctx, e)
		if err != nil {
			failures = append(failures, err)
		}
		if changed {
			n++
		}
		if ctx.Err() != nil {
			break
		}
	}
	return n, errors.Join(failures...)
}

func (s *Store) finalize(ctx context.Context, e *entry) (changed bool, err error) {
	if !e.finalizeMu.TryLock() {
		return false, nil
	}
	defer e.finalizeMu.Unlock()
	e.mu.Lock()
	if e.result != nil {
		e.mu.Unlock()
		return false, nil
	}
	if e.finalError != nil {
		err := e.finalError
		e.mu.Unlock()
		return false, err
	}
	defer func() {
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			e.mu.Lock()
			e.finalError = err
			e.mu.Unlock()
		}
	}()
	if e.writer == nil {
		e.writer, err = filelog.Recover(ctx, e.logConfig())
	}
	w := e.writer
	e.mu.Unlock()
	if err != nil {
		return false, err
	}
	manifest, err := w.Seal(ctx)
	if err != nil {
		return false, err
	}
	w.Close()
	e.mu.Lock()
	e.writer = nil
	e.mu.Unlock()
	r := poll.Results{PollID: e.def.Poll.ID, State: "final"}
	for _, option := range e.def.Poll.Options {
		r.Options = append(r.Options, poll.OptionCount{ID: option.ID, Label: option.Label})
	}
	seen := make(map[[16]byte]struct{})
	replayed, err := filelog.Replay(ctx, e.logConfig(), func(_ filelog.Position, v filelog.Vote) error {
		if _, found := seen[v.Token]; found {
			return nil
		}
		// This is an operator RAM budget, not part of vote semantics. Raising
		// Config.MaxUnique on restart can finish an earlier bounded failure.
		if uint64(len(seen)) >= s.c.MaxUnique {
			return errors.New("exact result exceeds configured unique-voter bound; raise MAX_UNIQUE and restart")
		}
		seen[v.Token] = struct{}{}
		r.TotalVotes++
		for i := range r.Options {
			if v.Choice&(1<<i) != 0 {
				r.Options[i].Votes++
			}
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	if len(replayed.Partitions) != 1 || len(manifest.Partitions) != 1 || replayed.Partitions[0] != manifest.Partitions[0] {
		return false, errors.New("CLOSED manifest changed during finalization")
	}
	r.CalculatedAt = s.now().UTC()
	disk := diskResult{Version: 1, DefinitionHash: hashJSON(e.def), Results: r, Manifest: manifest}
	disk.Checksum = hashJSON(disk)
	if err := publishResult(e.directory, disk); err != nil {
		return false, err
	}
	e.mu.Lock()
	e.result = &r
	e.mu.Unlock()
	return true, nil
}

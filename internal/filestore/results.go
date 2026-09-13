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
	if e.validationError != nil {
		err := e.validationError
		e.mu.Unlock()
		return false, err
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
	wasUnverified := e.needsValidation
	e.closing = true
	w, sealed := e.writer, e.sealed
	e.mu.Unlock()
	// finalizeMu serializes finalizers; closing prevents Vote from acquiring a
	// replacement writer, including after a wall-clock rollback. Historical
	// CRC/routing scans and disk syncs must never hold an entry or registry lock.
	if w == nil && sealed == nil {
		if err = validateInventory(e); err == nil {
			if s.recoverFinalization != nil {
				w, err = s.recoverFinalization(ctx, e)
			} else {
				w, err = e.recoverWriter(ctx)
			}
		}
		if err == nil {
			e.mu.Lock()
			e.writer = w
			e.mu.Unlock()
		}
	}
	markValidationFailure := func(failure error) {
		if failure != nil && !errors.Is(failure, context.Canceled) && !errors.Is(failure, context.DeadlineExceeded) {
			e.mu.Lock()
			e.validationError = failure
			e.mu.Unlock()
		}
	}
	if err != nil {
		markValidationFailure(err)
		return false, err
	}
	var manifest filelog.Manifest
	if sealed == nil {
		manifest, err = w.Seal(ctx)
		if err != nil {
			markValidationFailure(err)
			return false, err
		}
		w.Close()
		metrics := w.Metrics()
		e.mu.Lock()
		e.retainedMetrics = metrics
		e.writer = nil
		e.sealed = &manifest
		e.mu.Unlock()
	} else {
		manifest = *sealed
	}
	if wasUnverified {
		var stored *poll.Results
		stored, err = readVerifiedResult(e, manifest)
		if err != nil {
			markValidationFailure(err)
			return false, err
		}
		e.mu.Lock()
		e.needsValidation = false
		e.result = stored
		e.mu.Unlock()
		if stored != nil {
			return false, nil
		}
	}
	r, err := s.calculate(ctx, e, manifest)
	if err != nil {
		return false, err
	}
	r.CalculatedAt = s.now().UTC()
	if r.CalculatedAt.Before(e.def.Poll.EndsAt) {
		r.CalculatedAt = e.def.Poll.EndsAt.UTC()
	}
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

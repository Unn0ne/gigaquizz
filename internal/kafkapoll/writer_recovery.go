package kafkapoll

import (
	"context"
	"errors"
	"fmt"

	"gigaquizz/internal/votelog"
)

// Seal the old epoch first: healthy partitions must get the opportunity to
// drain their already admitted attempts, even when another writer has failed.
// A terminal error cannot be repaired within that epoch. Recover at most once
// per maintenance call; a failed replacement is retried on a later tick.
func sealWithRecovery(ctx context.Context, w journalWriter, recover func(context.Context, journalWriter) (journalWriter, error)) (votelog.Manifest, error) {
	manifest, err := w.Seal(ctx)
	if err == nil || w.Metrics()["failed_writers"] == 0 {
		return manifest, err
	}
	if err := ctx.Err(); err != nil {
		return votelog.Manifest{}, err
	}
	next, err := recover(ctx, w)
	if err != nil {
		return votelog.Manifest{}, fmt.Errorf("recover closed poll journal: %w", err)
	}
	return next.Seal(ctx)
}

// Called under opMu with admission irreversibly closed. Close joins all old
// producers before prepare can acquire their stable transactional IDs. The
// owner check is repeated after Close: shutdown or PG ownership loss must never
// trigger automatic reacquisition. Callbacks exercise these ordering boundaries
// in unit tests without connecting to Kafka or PostgreSQL.
func (s *Store) replaceFailedWriter(ctx context.Context, e *entry, failed journalWriter, owner func(context.Context) error, prepare func(context.Context, *entry) error) (journalWriter, error) {
	if !e.admissionClosed.Load() {
		return nil, errors.New("cannot recover a failed writer while poll admission is open")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := owner(ctx); err != nil {
		return nil, err
	}
	s.mu.RLock()
	current := e.writer
	s.mu.RUnlock()
	if current != failed || failed == nil || failed.Metrics()["failed_writers"] == 0 {
		return nil, errors.New("failed journal ownership changed before recovery")
	}
	failed.Close()
	s.mu.Lock()
	s.retainMetrics(failed.Metrics())
	e.writer = nil
	s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := owner(ctx); err != nil {
		return nil, err
	}
	// prepare reloads the durable prepared definition, never rotates its topic,
	// verifies the original BOOT, fences old epochs and resolves open transactions
	// through read_committed before publishing a replacement writer.
	if err := prepare(ctx, e); err != nil {
		return nil, err
	}
	s.mu.RLock()
	next := e.writer
	s.mu.RUnlock()
	if next == nil {
		return nil, errors.New("recovered journal was not published")
	}
	return next, nil
}

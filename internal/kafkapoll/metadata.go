package kafkapoll

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"gigaquizz/internal/poll"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// contextMutex bounds waiting callers as well as the work they perform.
type contextMutex struct {
	once  sync.Once
	token chan struct{}
}

func (m *contextMutex) LockContext(ctx context.Context) error {
	m.once.Do(func() { m.token = make(chan struct{}, 1) })
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case m.token <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-m.token
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (m *contextMutex) Lock()   { _ = m.LockContext(context.Background()) }
func (m *contextMutex) Unlock() { <-m.token }

func (s *Store) waitDurable(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	return s.guard.WaitDurableConn(ctx, conn.Conn())
}
func (s *Store) execDurable(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	defer conn.Release()
	tag, err := conn.Exec(ctx, query, args...)
	if err != nil {
		return tag, err
	}
	return tag, s.guard.WaitDurableConn(ctx, conn.Conn())
}

// Called under opMu. Refresh only missing unfinished rows, including a COMMIT
// whose reply was lost. Never replace a published writer or its definition.
func (s *Store) reloadPending(ctx context.Context) error {
	rows, err := s.pool.Query(ctx, "SELECT id::text,description,journal,prepared FROM "+s.table()+" WHERE finalized_at IS NULL ORDER BY starts_at LIMIT $1", maxPendingPolls+1)
	if err != nil {
		return err
	}
	defer rows.Close()
	pending := make([]*entry, 0)
	for rows.Next() {
		var id string
		var desc, journal []byte
		e := new(entry)
		if err := rows.Scan(&id, &desc, &journal, &e.prepared); err != nil {
			return err
		}
		if json.Unmarshal(desc, &e.poll) != nil || json.Unmarshal(journal, &e.config) != nil {
			return errors.New("invalid pending metadata")
		}
		token, err := parseID(id)
		if err != nil || e.poll.ID != id || e.config.PollID != token || !validTopic(e.config.Topic, token) || !e.poll.StartsAt.Equal(e.config.StartsAt) || !e.poll.EndsAt.Equal(e.config.EndsAt) || e.config.AllowedMask != (uint32(1)<<len(e.poll.Options))-1 || e.config.Multiple != (e.poll.Type == "multiple") {
			return errors.New("pending metadata definitions disagree")
		}
		e.config.Brokers = append([]string(nil), s.opts.Brokers...)
		e.config.AllowRemoteBrokers = s.opts.AllowRemoteBrokers
		e.config.Security = s.opts.Security
		pending = append(pending, e)
		if len(pending) > maxPendingPolls {
			return errors.New("unfinished poll inventory bound exceeded")
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range pending {
		if s.polls[e.poll.ID] == nil {
			if len(s.polls) >= s.opts.MaxPolls {
				return errors.New("metadata poll inventory bound exceeded")
			}
			s.polls[e.poll.ID] = e
		}
	}
	return nil
}

// A previous final UPDATE can have committed despite a timeout. Reuse it before
// opening producers, sealing, or rebuilding an exact map.
func (s *Store) restoreFinal(ctx context.Context, e *entry) (bool, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Release()
	var encoded []byte
	var finalized *time.Time
	err = conn.QueryRow(ctx, "SELECT result,finalized_at FROM "+s.table()+" WHERE id=$1", e.poll.ID).Scan(&encoded, &finalized)
	if err != nil {
		return false, err
	}
	if finalized == nil {
		return false, nil
	}
	finalEntry := entry{poll: clonePoll(e.poll), config: e.config}
	finalEntry.poll.FinalizedAt = finalized
	if err := validateFinal(&finalEntry, encoded); err != nil {
		return false, err
	}
	if err := s.guard.WaitDurableConn(ctx, conn.Conn()); err != nil {
		return false, err
	}
	s.mu.RLock()
	w := e.writer
	s.mu.RUnlock()
	if w != nil {
		w.Close()
	}
	s.mu.Lock()
	if w != nil {
		s.retainMetrics(w.Metrics())
	}
	e.writer = nil
	e.poll.FinalizedAt = finalized
	e.admissionClosed.Store(true)
	s.mu.Unlock()
	return true, nil
}

func (s *Store) progressTable() string {
	return pgx.Identifier{s.opts.Schema, "partition_results"}.Sanitize()
}

// The schedule lock belongs to the same transaction as overlap checking and
// insertion, independently of the controller's dedicated ownership connection.
func (s *Store) insertDefinition(ctx context.Context, tx pgx.Tx, p poll.Poll, description, journal []byte) error {
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", lockID(s.opts.Schema+"/schedule")); err != nil {
		return err
	}
	var overlap bool
	if err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM "+s.table()+" WHERE starts_at < $2 AND ends_at > $1)", p.StartsAt, p.EndsAt).Scan(&overlap); err != nil {
		return err
	}
	if overlap {
		return poll.ErrOverlap
	}
	_, err := tx.Exec(ctx, "INSERT INTO "+s.table()+"(id,description,journal,starts_at,ends_at) VALUES($1,$2,$3,$4,$5)", p.ID, description, journal, p.StartsAt, p.EndsAt)
	return err
}

func (s *Store) now() time.Time {
	if s.clock != nil {
		return s.clock()
	}
	return time.Now()
}

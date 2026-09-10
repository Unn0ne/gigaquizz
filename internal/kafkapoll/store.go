// Package kafkapoll connects the public application to Kafka vote journals and
// PostgreSQL poll metadata/results. PostgreSQL is never called by Vote.
package kafkapoll

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gigaquizz/internal/poll"
	"gigaquizz/internal/postgres"
	"gigaquizz/internal/votelog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrOwnership = errors.New("application controller ownership lost; restart required")
var schemaPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

const maxPendingPolls = 32 // bound prepared producers/queues as well as metadata

type Options struct {
	DatabaseURL        string
	Schema             string
	Brokers            []string
	AllowRemoteBrokers bool
	Partitions         int
	BatchSize          int
	QueuePerPartition  int
	Linger             time.Duration
	PreparationLead    time.Duration
	MaxUnique          int
	MaxPolls           int
	Durability         postgres.DurabilityOptions
}

func (o *Options) defaults() error {
	if o.Schema == "" {
		o.Schema = "gigaquizz_kafka"
	}
	if !schemaPattern.MatchString(o.Schema) {
		return errors.New("invalid metadata schema")
	}
	if o.Partitions == 0 {
		o.Partitions = 4
	}
	if o.BatchSize == 0 {
		o.BatchSize = 256
	}
	if o.QueuePerPartition == 0 {
		o.QueuePerPartition = 2048
	}
	if o.Linger == 0 {
		o.Linger = 2 * time.Millisecond
	}
	if o.PreparationLead == 0 {
		o.PreparationLead = 20 * time.Second
	}
	if o.MaxUnique == 0 {
		o.MaxUnique = 120_000_000
	}
	if o.MaxPolls == 0 {
		o.MaxPolls = 10_000
	}
	if o.DatabaseURL == "" || len(o.Brokers) == 0 || o.Partitions < 1 || o.Partitions > 32 || o.BatchSize < 1 || o.BatchSize > 4096 || o.QueuePerPartition < 1 || o.QueuePerPartition > 8192 || o.Linger < time.Millisecond || o.Linger > time.Second || o.PreparationLead < 5*time.Second || o.PreparationLead > time.Minute || o.MaxUnique < 1 || o.MaxUnique > 120_000_000 || o.MaxPolls < 1 || o.MaxPolls > 100_000 {
		return errors.New("invalid Kafka application configuration")
	}
	o.Brokers = append([]string(nil), o.Brokers...)
	return nil
}

type entry struct {
	poll     poll.Poll
	config   votelog.Config
	writer   *votelog.Store
	prepared bool
}

type Store struct {
	opts          Options
	pool          *pgxpool.Pool
	guard         *postgres.Store
	owner         *pgx.Conn
	ownerMu       sync.Mutex
	ownerStarted  time.Time
	ctx           context.Context
	cancel        context.CancelFunc
	closed        atomic.Bool
	closeOnce     sync.Once
	opMu          sync.Mutex // administrative create/recover/finalize; never used by Vote
	mu            sync.RWMutex
	polls         map[string]*entry
	heartbeatDone chan struct{}
}

var _ poll.Repository = (*Store)(nil)

func (s *Store) table() string { return pgx.Identifier{s.opts.Schema, "polls"}.Sanitize() }
func lockID(schema string) int64 {
	h := sha256.Sum256([]byte("gigaquizz-kafka-controller-v1/" + schema))
	return int64(binary.BigEndian.Uint64(h[:8]) & 0x7fffffffffffffff)
}

func poolConfig(databaseURL string) (*pgxpool.Config, error) {
	c, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, errors.New("invalid metadata database configuration")
	}
	c.MaxConns = 8
	c.ConnConfig.ConnectTimeout = 5 * time.Second
	c.ConnConfig.RuntimeParams["application_name"] = "gigaquizz_kafka_metadata"
	c.ConnConfig.RuntimeParams["synchronous_commit"] = "on"
	c.ConnConfig.RuntimeParams["statement_timeout"] = "10000"
	c.ConnConfig.RuntimeParams["lock_timeout"] = "5000"
	c.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "10000"
	return c, nil
}

func migrate(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", lockID(schema+"/migration")); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgx.Identifier{schema}.Sanitize()); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+pgx.Identifier{schema, "polls"}.Sanitize()+` (
		id uuid PRIMARY KEY, description jsonb NOT NULL, journal jsonb NOT NULL,
		starts_at timestamptz NOT NULL, ends_at timestamptz NOT NULL,
		result jsonb, finalized_at timestamptz,
		prepared boolean NOT NULL DEFAULT false,
		CHECK (ends_at = starts_at + interval '60 seconds'),
		CHECK ((result IS NULL) = (finalized_at IS NULL))
	)`)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "ALTER TABLE "+pgx.Identifier{schema, "polls"}.Sanitize()+" ADD COLUMN IF NOT EXISTS prepared boolean NOT NULL DEFAULT false"); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Migrate creates only this branch's schema, without Kafka ownership transfer.
func Migrate(ctx context.Context, databaseURL, schema string) error {
	if !schemaPattern.MatchString(schema) {
		return errors.New("invalid metadata schema")
	}
	c, err := poolConfig(databaseURL)
	if err != nil {
		return err
	}
	p, err := pgxpool.NewWithConfig(ctx, c)
	if err != nil {
		return errors.New("cannot initialize metadata database")
	}
	defer p.Close()
	return migrate(ctx, p, schema)
}

func New(ctx context.Context, options Options) (_ *Store, err error) {
	if err = options.defaults(); err != nil {
		return nil, err
	}
	s := &Store{opts: options, polls: make(map[string]*entry), heartbeatDone: make(chan struct{})}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	heartbeatStarted := false
	defer func() {
		if err != nil {
			if !heartbeatStarted {
				close(s.heartbeatDone)
			}
			s.Close()
		}
	}()
	s.guard, err = postgres.NewWithOptions(ctx, options.DatabaseURL, 1, options.Durability)
	if err != nil {
		return nil, errors.New("metadata database durability check failed")
	}
	c, err := poolConfig(options.DatabaseURL)
	if err != nil {
		return nil, err
	}
	s.pool, err = pgxpool.NewWithConfig(ctx, c)
	if err != nil {
		return nil, errors.New("cannot initialize metadata database")
	}
	s.owner, err = pgx.ConnectConfig(ctx, c.ConnConfig.Copy())
	if err != nil {
		return nil, errors.New("cannot open controller ownership session")
	}
	var owned bool
	err = s.owner.QueryRow(ctx, "SELECT pg_try_advisory_lock($1), pg_postmaster_start_time()", lockID(options.Schema)).Scan(&owned, &s.ownerStarted)
	if err != nil || !owned {
		return nil, errors.New("another controller owns this metadata schema")
	}
	if err = migrate(ctx, s.pool, options.Schema); err != nil {
		return nil, errors.New("cannot migrate metadata schema")
	}
	if err = s.guard.WaitDurable(ctx); err != nil {
		return nil, err
	}
	if err = s.load(ctx); err != nil {
		return nil, err
	}
	heartbeatStarted = true
	go s.heartbeat()
	// A restarted owner fences the preceding producer epochs before serving.
	for _, e := range s.polls {
		if e.poll.FinalizedAt != nil {
			continue
		}
		if err = s.prepare(ctx, e); err != nil {
			return nil, fmt.Errorf("recover pending poll journal: %w", err)
		}
	}
	return s, nil
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func (s *Store) checkOwner(ctx context.Context) error {
	if s.closed.Load() || s.ctx.Err() != nil {
		return ErrOwnership
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.ownerMu.Lock()
	defer s.ownerMu.Unlock()
	var started time.Time
	var recovery bool
	err := s.owner.QueryRow(ctx, "SELECT pg_postmaster_start_time(), pg_is_in_recovery()").Scan(&started, &recovery)
	if err != nil || recovery || !started.Equal(s.ownerStarted) {
		s.cancel()
		return ErrOwnership
	}
	return nil
}

func (s *Store) heartbeat() {
	defer close(s.heartbeatDone)
	defer func() { s.opMu.Lock(); s.closeWriters(); s.opMu.Unlock() }()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-tick.C:
		}
		ctx, cancel := context.WithTimeout(s.ctx, 2*time.Second)
		err := s.checkOwner(ctx)
		cancel()
		if err != nil {
			s.cancel() // irreversible: no automatic reacquisition/reinitialization
			return
		}
	}
}

func (s *Store) closeWriters() {
	s.mu.RLock()
	writers := make([]*votelog.Store, 0, len(s.polls))
	for _, e := range s.polls {
		if e.writer != nil {
			writers = append(writers, e.writer)
		}
	}
	s.mu.RUnlock()
	for _, w := range writers {
		w.Close()
	}
}

// Close stops all producer epochs before releasing the PG ownership session.
func (s *Store) Close() {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		<-s.heartbeatDone
		s.opMu.Lock()
		s.closeWriters()
		s.opMu.Unlock()
		if s.owner != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_ = s.owner.Close(ctx)
			cancel()
		}
		if s.pool != nil {
			s.pool.Close()
		}
		if s.guard != nil {
			s.guard.Close()
		}
	})
}

func (s *Store) Ping(ctx context.Context) error {
	if err := s.checkOwner(ctx); err != nil {
		return err
	}
	if err := s.guard.Ping(ctx); err != nil {
		return err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, e := range s.polls {
		if e.poll.FinalizedAt == nil && (e.writer == nil || e.writer.Metrics()["failed_writers"] > 0) {
			return errors.New("a pending poll journal is unavailable")
		}
	}
	return nil
}

func parseID(id string) ([16]byte, error) {
	var token [16]byte
	if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		return token, poll.ErrNotFound
	}
	return parseToken(strings.ReplaceAll(id, "-", ""))
}

func parseToken(value string) ([16]byte, error) {
	var token [16]byte
	if len(value) != 32 || value != strings.ToLower(value) {
		return token, poll.ErrNotFound
	}
	data, err := hex.DecodeString(value)
	if err != nil || len(data) != 16 {
		return token, poll.ErrNotFound
	}
	copy(token[:], data)
	if token == [16]byte{} {
		return token, poll.ErrNotFound
	}
	return token, nil
}

func newID() (string, [16]byte, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", b, err
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:], b, nil
}

func clonePoll(p poll.Poll) poll.Poll {
	p.Options = append([]poll.Option(nil), p.Options...)
	if p.FinalizedAt != nil {
		t := *p.FinalizedAt
		p.FinalizedAt = &t
	}
	return p
}

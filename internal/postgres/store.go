// Package postgres implements the durable vote repository. Durability uses the
// connected PostgreSQL server's WAL and replication configuration; this package
// alone does not make a single server highly available.
package postgres

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gigaquizz/internal/poll"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/001_initial.sql
var initialMigration string

const (
	migrationLock int64 = 714466920202601
	creationLock  int64 = 714466920202602
	pollColumns         = "id::text, question, kind, options, starts_at, ends_at, created_at, finalized_at"
)

type Store struct {
	pool            *pgxpool.Pool
	durability      DurabilityOptions
	durabilityState durabilityState
	diagnostics     storeDiagnostics
}

var _ poll.Repository = (*Store)(nil)

func New(ctx context.Context, databaseURL string, maxConns int32) (*Store, error) {
	return NewWithOptions(ctx, databaseURL, maxConns, DurabilityOptions{})
}

func NewWithOptions(ctx context.Context, databaseURL string, maxConns int32, options DurabilityOptions) (*Store, error) {
	if err := options.validate(); err != nil {
		return nil, err
	}
	options.StandbyNames = append([]string(nil), options.StandbyNames...)
	s := &Store{durability: options}
	if maxConns < 1 {
		return nil, errors.New("database max connections must be positive")
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		// Parse errors can contain credentials from the supplied URL.
		return nil, errors.New("invalid database configuration")
	}
	config.MaxConns = maxConns
	config.ConnConfig.ConnectTimeout = 5 * time.Second
	config.ConnConfig.RuntimeParams["application_name"] = "gigaquizz"
	config.ConnConfig.RuntimeParams["synchronous_commit"] = "on"
	config.ConnConfig.RuntimeParams["default_transaction_isolation"] = "read committed"
	config.ConnConfig.RuntimeParams["statement_timeout"] = "30000"
	config.ConnConfig.RuntimeParams["lock_timeout"] = "5000"
	config.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "30000"
	config.AfterConnect = s.checkConnection
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, errors.New("cannot initialize database pool")
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errors.New("cannot connect to database")
	}
	var version int
	if err := pool.QueryRow(ctx, "SELECT current_setting('server_version_num')::integer").Scan(&version); err != nil {
		pool.Close()
		return nil, errors.New("cannot check database version")
	}
	if version < 160000 {
		pool.Close()
		return nil, errors.New("PostgreSQL 16 or newer is required")
	}
	s.pool = pool
	return s, nil
}

func (s *Store) Close() { s.pool.Close() }

// Warm establishes every configured connection before the event starts.
// Call before serving traffic: retaining the acquired connections forces the
// pool to create distinct backends. It does not warm table/index caches or
// promise that a backend will never need to be replaced during the event.
func (s *Store) Warm(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	connections := make([]*pgxpool.Conn, 0, s.pool.Config().MaxConns)
	defer func() {
		for _, conn := range connections {
			conn.Release()
		}
	}()
	for range cap(connections) {
		conn, err := s.pool.Acquire(ctx)
		if err != nil {
			return errors.New("database connection warmup failed")
		}
		connections = append(connections, conn)
		if err := conn.Ping(ctx); err != nil {
			return errors.New("database connection warmup failed")
		}
	}
	// Readiness in replicated mode must still prove the required WAL prefix.
	return s.waitDurableConn(ctx, connections[0].Conn())
}

func (s *Store) Ping(ctx context.Context) error {
	if s.durability.RequiredStandbys > 0 {
		return s.WaitDurable(ctx)
	}
	return s.pool.Ping(ctx)
}

func (s *Store) Migrate(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", migrationLock); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS gigaquizz_migrations (
		version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
	)`); err != nil {
		return err
	}
	var applied bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM gigaquizz_migrations WHERE version = 1)").Scan(&applied); err != nil {
		return err
	}
	if !applied {
		if _, err = tx.Exec(ctx, initialMigration); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, "INSERT INTO gigaquizz_migrations (version) VALUES (1)"); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return s.waitDurableConn(ctx, conn.Conn())
}

// A cancelled request context must not prevent rollback and returning the
// connection. pgx closes the connection if rollback cannot safely finish.
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

type rowScanner interface{ Scan(...any) error }

func scanPoll(row rowScanner) (poll.Poll, error) {
	var p poll.Poll
	var options []byte
	err := row.Scan(&p.ID, &p.Question, &p.Type, &options, &p.StartsAt, &p.EndsAt, &p.CreatedAt, &p.FinalizedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, poll.ErrNotFound
	}
	if err != nil {
		return p, err
	}
	err = json.Unmarshal(options, &p.Options)
	return p, err
}

func parseUUID(value string) (pgtype.UUID, error) {
	var id pgtype.UUID
	err := id.Scan(value)
	return id, err
}

func (s *Store) Create(ctx context.Context, input poll.CreateInput) (poll.Poll, error) {
	if err := input.Validate(); err != nil {
		return poll.Poll{}, err
	}
	options := make([]poll.Option, len(input.Options))
	for i, label := range input.Options {
		options[i] = poll.Option{ID: i + 1, Label: label}
	}
	encoded, err := json.Marshal(options)
	if err != nil {
		return poll.Poll{}, err
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return poll.Poll{}, err
	}
	defer conn.Release()
	tx, err := conn.Begin(ctx)
	if err != nil {
		return poll.Poll{}, err
	}
	defer rollback(tx)
	// Only administrative creation serializes here; voting never takes this lock.
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", creationLock); err != nil {
		return poll.Poll{}, err
	}
	var startsAt time.Time
	if err = tx.QueryRow(ctx, "SELECT coalesce($1::timestamptz, clock_timestamp())", input.StartsAt).Scan(&startsAt); err != nil {
		return poll.Poll{}, err
	}
	endsAt := startsAt.Add(time.Minute)
	var overlap bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM polls WHERE starts_at < $2 AND ends_at > $1)", startsAt, endsAt).Scan(&overlap); err != nil {
		return poll.Poll{}, err
	}
	if overlap {
		return poll.Poll{}, poll.ErrOverlap
	}
	p, err := scanPoll(tx.QueryRow(ctx, `INSERT INTO polls (question, kind, options, starts_at, ends_at)
		VALUES ($1, $2, $3, $4, $5) RETURNING `+pollColumns, input.Question, input.Type, encoded, startsAt, endsAt))
	if err != nil {
		return poll.Poll{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return poll.Poll{}, err
	}
	if err = s.waitDurableConn(ctx, conn.Conn()); err != nil {
		return poll.Poll{}, err
	}
	return p, nil
}

func (s *Store) Get(ctx context.Context, id string) (poll.Poll, error) {
	uuid, err := parseUUID(id)
	if err != nil {
		return poll.Poll{}, poll.ErrNotFound
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return poll.Poll{}, err
	}
	defer conn.Release()
	p, err := scanPoll(conn.QueryRow(ctx, "SELECT "+pollColumns+" FROM polls WHERE id = $1", uuid))
	if err != nil {
		return poll.Poll{}, err
	}
	if err = s.waitDurableConn(ctx, conn.Conn()); err != nil {
		return poll.Poll{}, err
	}
	return p, nil
}

func (s *Store) List(ctx context.Context) ([]poll.Poll, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	rows, err := conn.Query(ctx, "SELECT "+pollColumns+" FROM polls ORDER BY starts_at DESC, id LIMIT 100")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]poll.Poll, 0)
	for rows.Next() {
		p, err := scanPoll(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, p)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if err = s.waitDurableConn(ctx, conn.Conn()); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Store) Vote(ctx context.Context, id, token string, choices []int) (poll.Receipt, error) {
	pollID, err := parseUUID(id)
	if err != nil {
		return poll.Receipt{Status: "not_found"}, nil
	}
	tokenID, err := parseUUID(token)
	if err != nil {
		return poll.Receipt{Status: "invalid"}, nil
	}
	normalized, err := poll.NormalizeChoices(choices)
	if err != nil {
		return poll.Receipt{Status: "invalid"}, nil
	}
	encoded := make([]int32, len(normalized))
	for i, choice := range normalized {
		encoded[i] = int32(choice)
	}
	var result poll.Receipt
	var recorded []int32
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return poll.Receipt{}, err
	}
	defer conn.Release()
	// One implicit transaction / one RPC. Scan closes/drains rows, so an
	// autocommit failure is returned as an error rather than an accepted receipt.
	queryStarted := time.Now()
	err = conn.QueryRow(ctx, "SELECT status, choices, accepted_at FROM record_vote($1, $2, $3)", pollID, tokenID, encoded).
		Scan(&result.Status, &recorded, &result.AcceptedAt)
	queryElapsed := time.Since(queryStarted)
	s.diagnostics.voteSQLCalls.Add(1)
	s.diagnostics.voteSQLNS.Add(uint64(queryElapsed))
	if err != nil {
		return poll.Receipt{}, err
	}
	if result.Status == "accepted" || result.Status == "duplicate" || result.Status == "conflict" {
		if err = s.waitDurableConn(ctx, conn.Conn()); err != nil {
			return poll.Receipt{}, err
		}
	}
	for _, choice := range recorded {
		result.Choices = append(result.Choices, int(choice))
	}
	return result, nil
}

func (s *Store) Results(ctx context.Context, id string) (poll.Results, error) {
	uuid, err := parseUUID(id)
	if err != nil {
		return poll.Results{}, poll.ErrNotFound
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return poll.Results{}, err
	}
	defer conn.Release()
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return poll.Results{}, err
	}
	defer rollback(tx)
	p, err := scanPoll(tx.QueryRow(ctx, "SELECT "+pollColumns+" FROM polls WHERE id = $1", uuid))
	if err != nil {
		return poll.Results{}, err
	}
	result := poll.Results{PollID: p.ID}
	var encoded []byte
	if p.FinalizedAt != nil {
		err = tx.QueryRow(ctx, "SELECT total_votes, options, calculated_at FROM poll_results WHERE poll_id = $1", uuid).
			Scan(&result.TotalVotes, &encoded, &result.CalculatedAt)
		result.State = "final"
	} else {
		// The repeatable-read snapshot makes the independent total and option
		// counts agree, including multiple-choice ballots counted only once.
		err = tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM votes WHERE poll_id = $1),
			calculate_option_counts($1, $2), transaction_timestamp()`, uuid, mustJSON(p.Options)).
			Scan(&result.TotalVotes, &encoded, &result.CalculatedAt)
		result.State = p.State(result.CalculatedAt)
	}
	if err != nil {
		return poll.Results{}, err
	}
	if err = json.Unmarshal(encoded, &result.Options); err != nil {
		return poll.Results{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return poll.Results{}, err
	}
	if err = s.waitDurableConn(ctx, conn.Conn()); err != nil {
		return poll.Results{}, err
	}
	return result, nil
}

func mustJSON(options []poll.Option) []byte {
	// This type contains only integers and strings, so encoding cannot fail.
	result, _ := json.Marshal(options)
	return result
}

func (s *Store) FinalizeDue(ctx context.Context) (int, error) {
	rows, err := s.pool.Query(ctx, "SELECT id::text FROM polls WHERE finalized_at IS NULL AND ends_at <= clock_timestamp() ORDER BY ends_at, id LIMIT 20")
	if err != nil {
		return 0, err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, err
	}
	finalized := 0
	for _, id := range ids {
		var changed bool
		conn, acquireErr := s.pool.Acquire(ctx)
		if acquireErr != nil {
			return finalized, acquireErr
		}
		err = conn.QueryRow(ctx, "SELECT finalize_poll($1::uuid)", id).Scan(&changed)
		if err == nil {
			err = s.waitDurableConn(ctx, conn.Conn())
		}
		conn.Release()
		if err != nil {
			return finalized, fmt.Errorf("finalize poll: %w", err)
		}
		if changed {
			finalized++
		}
	}
	return finalized, nil
}

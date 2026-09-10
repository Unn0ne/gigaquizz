package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/url"
	"strings"
	"time"

	"gigaquizz/internal/poll"
	"gigaquizz/internal/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type databaseInfo struct {
	Version           string `json:"server_version"`
	SynchronousCommit string `json:"synchronous_commit"`
	StandbySetting    string `json:"synchronous_standby_names"`
	FSync             string `json:"fsync"`
	FullPageWrites    string `json:"full_page_writes"`
	InRecovery        bool   `json:"in_recovery"`
}
type walPosition struct {
	Insert string `json:"insert_lsn"`
	Flush  string `json:"flush_lsn"`
}
type walReport struct {
	Before             walPosition `json:"before_workload"`
	AtDrain            walPosition `json:"at_client_drain"`
	AfterMarker        walPosition `json:"after_post_drain_marker_and_durability_check"`
	BytesAtDrain       int64       `json:"insert_lsn_delta_at_client_drain_bytes"`
	BytesThroughMarker int64       `json:"insert_lsn_delta_through_marker_bytes"`
	MarkerDurable      bool        `json:"post_drain_marker_durability_confirmed"`
	Method             string      `json:"method"`
}
type relationSizes struct {
	SchemaBytes int64 `json:"schema_tables_and_indexes_bytes"`
	VoteBytes   int64 `json:"vote_partitions_and_indexes_bytes"`
}
type sizeReport struct {
	Before            relationSizes `json:"before_workload"`
	AtDrain           relationSizes `json:"after_client_drain"`
	AfterFinalization relationSizes `json:"after_real_close_and_finalization"`
	VoteDeltaAtDrain  int64         `json:"vote_bytes_delta_at_client_drain"`
	VoteDeltaFinal    int64         `json:"vote_bytes_delta_after_finalization"`
	Method            string        `json:"method"`
}

func pinSchema(dsn, schema string) (string, error) {
	if !validSchema(schema) {
		return "", errors.New("invalid isolated schema")
	}
	if _, err := pgx.ParseConfig(dsn); err != nil {
		return "", errors.New("invalid database configuration")
	}
	path := schema + ",pg_catalog"
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", errors.New("invalid database configuration")
		}
		q := u.Query()
		q.Set("search_path", path)
		u.RawQuery = q.Encode()
		return u.String(), nil
	}
	return dsn + " search_path='" + path + "'", nil
}

func openConnection(ctx context.Context, dsn string) (*pgx.Conn, error) {
	c, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid database configuration")
	}
	c.ConnectTimeout = 5 * time.Second
	c.RuntimeParams["application_name"] = "gigaquizz_storagebench"
	c.RuntimeParams["synchronous_commit"] = "on"
	c.RuntimeParams["statement_timeout"] = "10000"
	c.RuntimeParams["lock_timeout"] = "5000"
	return pgx.ConnectConfig(ctx, c)
}

func readDatabaseInfo(ctx context.Context, conn *pgx.Conn) (*databaseInfo, error) {
	i := &databaseInfo{}
	err := conn.QueryRow(ctx, `SELECT current_setting('server_version'),current_setting('synchronous_commit'),
		current_setting('synchronous_standby_names'),current_setting('fsync'),current_setting('full_page_writes'),pg_is_in_recovery()`).Scan(&i.Version, &i.SynchronousCommit, &i.StandbySetting, &i.FSync, &i.FullPageWrites, &i.InRecovery)
	return i, err
}
func readWAL(ctx context.Context, conn *pgx.Conn) (walPosition, error) {
	var p walPosition
	err := conn.QueryRow(ctx, `SELECT pg_current_wal_insert_lsn()::text,pg_current_wal_flush_lsn()::text`).Scan(&p.Insert, &p.Flush)
	return p, err
}
func walDelta(ctx context.Context, conn *pgx.Conn, end, begin string) (int64, error) {
	var delta int64
	err := conn.QueryRow(ctx, `SELECT pg_wal_lsn_diff($1::pg_lsn,$2::pg_lsn)::bigint`, end, begin).Scan(&delta)
	return delta, err
}
func sizes(ctx context.Context, conn *pgx.Conn, schema string) (relationSizes, error) {
	var s relationSizes
	err := conn.QueryRow(ctx, `SELECT coalesce(sum(pg_total_relation_size(c.oid)),0)::bigint,
		coalesce(sum(pg_total_relation_size(c.oid)) FILTER (WHERE c.relname LIKE 'votes_p%'),0)::bigint
		FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		WHERE n.nspname=$1 AND c.relkind IN ('r','m')`, schema).Scan(&s.SchemaBytes, &s.VoteBytes)
	return s, err
}

func execute(parent context.Context, c config, dsn string) (r report) {
	start := time.Now()
	defer func() { r.WallSeconds = time.Since(start).Seconds() }()
	r.Mode = c.mode
	r.Errors = []string{}
	r.Limitations = []string{
		"Database-only experiment: HTTP parsing, authentication, browser delivery and external TLS are excluded.",
		"Latency quantiles are exact nearest-rank quantiles for bounded retained observations; do not average quantiles across runs.",
		"WAL LSNs are global to this PostgreSQL primary; concurrent activity and boundary markers contaminate attribution. Physical size deltas are allocated relation bytes, not bytes written or storage latency.",
		"Unknown responses are reconciled after the genuine close barrier, not classified as lost confirmations.",
		"One loopback replication lab does not model separate failure zones or establish production throughput.",
	}
	if c.mode == "blind" {
		r.Limitations = append(r.Limitations, "Blind mode preserves fields, unique indexes, the close barrier and requested durability but omits full vote validation and conflict/receipt handling. Its ratio to vote mode is not deduplication overhead; it uses a separate insert pool and the repository durability proof.")
	}
	if c.auditOnly != "" {
		return auditOnly(parent, c, dsn)
	}
	r.Configuration = map[string]any{"total_keys": c.total, "logical_rate": c.rate, "workers": c.workers, "queue": c.queue, "repeat_every": c.repeatEvery, "different_choice": c.different, "required_standbys": c.requiredStandbys, "HTTP_included": false, "max_lag_ms": float64(c.maxLag) / float64(time.Millisecond), "timeout_ms": float64(c.timeout) / float64(time.Millisecond), "drain_timeout_ms": float64(c.drain) / float64(time.Millisecond), "warm_connections": c.warmConnections, "pool_warmup": "one repository Ping; additional connection establishment may occur during workload"}
	if c.warmConnections {
		r.Configuration["pool_warmup"] = "Store.Warm opens and validates all configured repository connections before poll creation"
	}
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		r.Errors = append(r.Errors, "schema_name_generation_failed")
		return
	}
	r.Schema = "gqbench_" + hex.EncodeToString(nonce[:])
	pinned, err := pinSchema(dsn, r.Schema)
	if err != nil {
		r.Errors = append(r.Errors, "database_configuration_invalid")
		return
	}
	setup, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	conn, err := openConnection(setup, pinned)
	if err != nil {
		r.Errors = append(r.Errors, "database_connection_failed_details_suppressed")
		return
	}
	defer conn.Close(context.Background())
	r.Database, err = readDatabaseInfo(setup, conn)
	if err != nil || r.Database.InRecovery {
		r.Errors = append(r.Errors, "workload_requires_readable_primary_settings")
		return
	}
	if _, err = conn.Exec(setup, "CREATE SCHEMA "+pgx.Identifier{r.Schema}.Sanitize()); err != nil {
		r.Errors = append(r.Errors, "isolated_schema_creation_failed")
		return
	}
	defer func() {
		if c.keepSchema || (r.Workload != nil && len(r.Errors) > 0) {
			r.Cleanup = "retained"
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, err := conn.Exec(cleanup, "DROP SCHEMA "+pgx.Identifier{r.Schema}.Sanitize()+" CASCADE"); err != nil {
			r.Cleanup = "failed_schema_retained"
			r.Errors = append(r.Errors, "schema_cleanup_failed_details_suppressed")
		} else {
			r.Cleanup = "dropped"
		}
	}()
	options := postgres.DurabilityOptions{RequiredStandbys: c.requiredStandbys}
	if c.requiredStandbys > 0 {
		for _, name := range strings.Split(c.standbyNames, ",") {
			options.StandbyNames = append(options.StandbyNames, strings.TrimSpace(name))
		}
	}
	store, err := postgres.NewWithOptions(setup, pinned, int32(c.workers), options)
	if err != nil {
		r.Errors = append(r.Errors, "repository_or_durability_configuration_failed_details_suppressed")
		return
	}
	defer store.Close()
	if err = store.Migrate(setup); err != nil {
		r.Errors = append(r.Errors, "isolated_migration_failed_details_suppressed")
		return
	}
	if _, err = conn.Exec(setup, `CREATE TABLE bench_marker (label text PRIMARY KEY, recorded_at timestamptz NOT NULL DEFAULT clock_timestamp())`); err != nil {
		r.Errors = append(r.Errors, "marker_setup_failed")
		return
	}
	m, err := newObservations(c.total)
	if err != nil {
		r.Errors = append(r.Errors, "key_generation_failed")
		return
	}
	vote := voteFunc(store.Vote)
	if c.mode == "blind" {
		if _, err = conn.Exec(setup, blindFunction); err != nil {
			r.Errors = append(r.Errors, "control_function_setup_failed")
			return
		}
		pool, err := blindPool(setup, pinned, c.workers)
		if err != nil {
			r.Errors = append(r.Errors, "control_pool_setup_failed")
			return
		}
		defer pool.Close()
		vote = blindVote(pool, store)
	}
	if err = store.Ping(setup); err != nil {
		r.Errors = append(r.Errors, "repository_warmup_failed")
		return
	}
	if c.warmConnections {
		if err = store.Warm(setup); err != nil {
			r.Errors = append(r.Errors, "all_repository_connections_warmup_failed")
			return
		}
	}
	p, err := store.Create(setup, poll.CreateInput{Question: "Isolated storage benchmark", Type: "single", Options: []string{"A", "B"}})
	if err != nil {
		r.Errors = append(r.Errors, "test_poll_creation_failed")
		return
	}
	r.PollID = p.ID
	if _, err = conn.Exec(setup, `INSERT INTO bench_marker(label) VALUES ('before')`); err != nil {
		r.Errors = append(r.Errors, "initial_marker_failed")
		return
	}
	if err = store.WaitDurable(setup); err != nil {
		r.Errors = append(r.Errors, "initial_marker_durability_unconfirmed")
		return
	}
	r.WAL = &walReport{Method: "pg_current_wal_insert_lsn / pg_current_wal_flush_lsn; pg_wal_lsn_diff. Baseline follows setup and a durable marker. Drain snapshot precedes the second marker; that marker is explicitly included in the later boundary. Finalization and cleanup are excluded."}
	r.PhysicalSizes = &sizeReport{Method: "SUM(pg_total_relation_size) for ordinary tables/materialized views in the isolated schema; vote subtotal selects votes_p* leaf tables. Includes their indexes/TOAST; excludes WAL and other schemas. Empty schema allocation is subtracted."}
	r.WAL.Before, err = readWAL(setup, conn)
	if err != nil {
		r.Errors = append(r.Errors, "initial_wal_snapshot_failed")
		return
	}
	r.PhysicalSizes.Before, err = sizes(setup, conn, r.Schema)
	if err != nil {
		r.Errors = append(r.Errors, "initial_relation_sizes_failed")
		return
	}
	work := runWorkload(parent, c, p.ID, m, vote)
	r.Workload = &work
	if work.InvalidReceipts > 0 {
		r.Errors = append(r.Errors, "invalid_repository_receipts")
	}
	ledger := keyLedger{Version: 1, Schema: r.Schema, PollID: p.ID, Mode: c.mode, Keys: m.keys, PollEndsAt: p.EndsAt}
	// Save test keys outside the database before any fault-sensitive wait. An
	// acknowledged-key ledger stored only on the same primary cannot prove loss.
	if c.keepSchema || c.auditFile != "" {
		r.KeyLedgerFile, err = saveLedger(c.auditFile, ledger)
		if err != nil {
			r.Errors = append(r.Errors, "private_key_ledger_write_failed")
		}
	}
	boundary, stopBoundary := context.WithTimeout(parent, 15*time.Second)
	r.WAL.AtDrain, err = readWAL(boundary, conn)
	if err != nil {
		r.Errors = append(r.Errors, "drain_wal_snapshot_failed")
	} else {
		r.WAL.BytesAtDrain, err = walDelta(boundary, conn, r.WAL.AtDrain.Insert, r.WAL.Before.Insert)
		if err != nil {
			r.Errors = append(r.Errors, "drain_wal_delta_failed")
		}
	}
	r.PhysicalSizes.AtDrain, err = sizes(boundary, conn, r.Schema)
	if err != nil {
		r.Errors = append(r.Errors, "drain_relation_sizes_failed")
	} else {
		r.PhysicalSizes.VoteDeltaAtDrain = r.PhysicalSizes.AtDrain.VoteBytes - r.PhysicalSizes.Before.VoteBytes
	}
	if _, err = conn.Exec(boundary, `INSERT INTO bench_marker(label) VALUES ('after_drain')`); err != nil {
		r.Errors = append(r.Errors, "post_drain_marker_failed")
	} else if err = store.WaitDurable(boundary); err != nil {
		r.Errors = append(r.Errors, "post_drain_marker_durability_unconfirmed")
	} else {
		r.WAL.MarkerDurable = true
	}
	r.WAL.AfterMarker, err = readWAL(boundary, conn)
	if err != nil {
		r.Errors = append(r.Errors, "post_marker_wal_snapshot_failed")
	} else {
		r.WAL.BytesThroughMarker, err = walDelta(boundary, conn, r.WAL.AfterMarker.Insert, r.WAL.Before.Insert)
		if err != nil {
			r.Errors = append(r.Errors, "post_marker_wal_delta_failed")
		}
	}
	stopBoundary()
	// No timestamp is shortened for the experiment: wait for the actual poll
	// close, then use the real repository barrier before reconciling keys.
	finalCtx, stopFinal := context.WithTimeout(parent, max(0, time.Until(p.EndsAt))+30*time.Second)
	defer stopFinal()
	if err = waitForFinal(finalCtx, store, p.ID); err != nil {
		r.Errors = append(r.Errors, "real_close_finalization_not_established")
	}
	r.Audit, err = reconcile(finalCtx, conn, ledger)
	if err != nil {
		r.Errors = append(r.Errors, "per_key_reconciliation_unavailable")
	} else if !r.Audit.Stable || !r.Audit.Correct {
		r.Errors = append(r.Errors, "reconciliation_not_stable_or_correct")
	}
	if c.requiredStandbys > 0 && r.Audit != nil && r.Audit.Stable {
		if err = proveFinalAudit(finalCtx, store, p.ID, r.Audit); err != nil {
			r.Errors = append(r.Errors, "final_audit_replication_durability_unconfirmed")
		} else {
			r.DurabilityChecked = true
		}
	}
	r.PhysicalSizes.AfterFinalization, err = sizes(finalCtx, conn, r.Schema)
	if err != nil {
		r.Errors = append(r.Errors, "final_relation_sizes_failed")
	} else {
		r.PhysicalSizes.VoteDeltaFinal = r.PhysicalSizes.AfterFinalization.VoteBytes - r.PhysicalSizes.Before.VoteBytes
	}
	// Preserve a ledger for failed experiments even when retain-schema was not
	// requested. It contains only artificial test keys and has mode 0600.
	if len(r.Errors) > 0 && r.KeyLedgerFile == "" {
		r.KeyLedgerFile, err = saveLedger("", ledger)
		if err != nil {
			r.Errors = append(r.Errors, "failure_key_ledger_write_failed")
		}
	}
	return
}

func waitForFinal(ctx context.Context, store *postgres.Store, pollID string) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		p, err := store.Get(ctx, pollID)
		if err != nil {
			return err
		}
		if p.FinalizedAt != nil {
			return nil
		}
		if !time.Now().Before(p.EndsAt) {
			if _, err = store.FinalizeDue(ctx); err != nil {
				return err
			}
		}
		timer.Reset(200 * time.Millisecond)
	}
}

func blindPool(ctx context.Context, dsn string, workers int) (*pgxpool.Pool, error) {
	c, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	c.MaxConns = int32(workers)
	c.ConnConfig.ConnectTimeout = 5 * time.Second
	c.ConnConfig.RuntimeParams["synchronous_commit"] = "on"
	c.ConnConfig.RuntimeParams["statement_timeout"] = "10000"
	c.ConnConfig.RuntimeParams["lock_timeout"] = "5000"
	c.ConnConfig.RuntimeParams["application_name"] = "gigaquizz_storagebench_control"
	pool, err := pgxpool.NewWithConfig(ctx, c)
	if err != nil {
		return nil, err
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}
func blindVote(pool *pgxpool.Pool, store *postgres.Store) voteFunc {
	return func(ctx context.Context, pollID, token string, choices []int) (poll.Receipt, error) {
		var pid, tid pgtype.UUID
		if err := pid.Scan(pollID); err != nil {
			return poll.Receipt{}, err
		}
		if err := tid.Scan(token); err != nil {
			return poll.Receipt{}, err
		}
		var accepted *time.Time
		var status string
		err := pool.QueryRow(ctx, `SELECT status,accepted_at FROM bench_blind_vote($1,$2,$3)`, pid, tid, []int32{int32(choices[0])}).Scan(&status, &accepted)
		if err == nil && status == "accepted" {
			err = store.WaitDurable(ctx)
		}
		if err != nil {
			return poll.Receipt{}, err
		}
		return poll.Receipt{Status: status, Choices: choices, AcceptedAt: accepted}, nil
	}
}

// Keep the same finalization barrier even in the lean control: delayed control
// statements must never insert after the final audit. This is deliberately not
// marketed as a matched measurement of the cost of deduplication alone.
const blindFunction = `CREATE FUNCTION bench_blind_vote(p_poll uuid,p_token uuid,p_choices integer[])
RETURNS TABLE(status text,accepted_at timestamptz) LANGUAGE plpgsql VOLATILE AS $$
DECLARE config polls%ROWTYPE; admitted timestamptz;
BEGIN
 PERFORM pg_advisory_xact_lock_shared(hashtext(p_poll::text),vote_lock_shard(p_token));
 SELECT * INTO STRICT config FROM polls WHERE id=p_poll;
 admitted:=clock_timestamp();
 IF config.finalized_at IS NOT NULL OR admitted>=config.ends_at THEN
  RETURN QUERY SELECT 'closed'::text,NULL::timestamptz; RETURN;
 END IF;
 IF admitted<config.starts_at THEN
  RETURN QUERY SELECT 'not_open'::text,NULL::timestamptz; RETURN;
 END IF;
 INSERT INTO votes(poll_id,token,choices,accepted_at) VALUES(p_poll,p_token,p_choices,admitted);
 RETURN QUERY SELECT 'accepted'::text,admitted;
END $$`

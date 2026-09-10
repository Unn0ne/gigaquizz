package postgres

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// DurabilityOptions is an explicit physical-replication policy. Zero values
// select local durability. Synchronous mode supports a fixed ANY quorum only;
// it does not implement leader election, fencing, or automatic failover.
type DurabilityOptions struct {
	RequiredStandbys int
	StandbyNames     []string
}

var ErrDurability = errors.New("required database durability is not established")

var standbyNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

func (o DurabilityOptions) validate() error {
	if o.RequiredStandbys < 0 || o.RequiredStandbys > 16 || len(o.StandbyNames) > 16 {
		return errors.New("invalid durability quorum")
	}
	if o.RequiredStandbys == 0 {
		if len(o.StandbyNames) != 0 {
			return errors.New("standby names require a positive durability quorum")
		}
		return nil
	}
	if o.RequiredStandbys > len(o.StandbyNames) {
		return errors.New("durability quorum exceeds named standbys")
	}
	seen := make(map[string]bool)
	for _, name := range o.StandbyNames {
		if !standbyNamePattern.MatchString(name) || seen[name] || name == "any" || name == "first" {
			return errors.New("standby names must be distinct lowercase identifiers")
		}
		seen[name] = true
	}
	return nil
}

func (o DurabilityOptions) expectedSetting() string {
	return fmt.Sprintf("ANY %d (%s)", o.RequiredStandbys, strings.Join(o.StandbyNames, ","))
}

func compactSetting(s string) string { return strings.Join(strings.Fields(s), "") }

// An existing Store never follows a restart or timeline change implicitly.
// A controller must fence the previous primary and construct a new Store.
type primaryIdentity struct {
	SystemID string
	Timeline int64
	Started  time.Time
}

type durabilityState struct {
	mu       sync.Mutex
	identity primaryIdentity
	set      bool
}

func (d *durabilityState) check(id primaryIdentity) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.set {
		d.identity, d.set = id, true
		return true
	}
	return d.identity.SystemID == id.SystemID && d.identity.Timeline == id.Timeline && d.identity.Started.Equal(id.Started)
}

func (s *Store) checkConnection(ctx context.Context, conn *pgx.Conn) error {
	var recovery bool
	var fsync, commit, fullPages, setting string
	err := conn.QueryRow(ctx, `SELECT pg_catalog.pg_is_in_recovery(),
		current_setting('fsync'), current_setting('synchronous_commit'),
		current_setting('full_page_writes'), current_setting('synchronous_standby_names')`).
		Scan(&recovery, &fsync, &commit, &fullPages, &setting)
	if err != nil {
		return ErrDurability
	}
	if recovery || fsync != "on" || commit != "on" || fullPages != "on" {
		return ErrDurability
	}
	if s.durability.RequiredStandbys == 0 {
		if strings.TrimSpace(setting) != "" {
			return errors.New("synchronous replication requires an explicit durability policy")
		}
		return nil
	}
	if compactSetting(setting) != compactSetting(s.durability.expectedSetting()) {
		return ErrDurability
	}
	var id primaryIdentity
	err = conn.QueryRow(ctx, `SELECT (pg_catalog.pg_control_system()).system_identifier::text,
		('x' || left(pg_catalog.pg_walfile_name(pg_catalog.pg_current_wal_insert_lsn()),8))::bit(32)::bigint,
		pg_catalog.pg_postmaster_start_time()`).Scan(&id.SystemID, &id.Timeline, &id.Started)
	if err != nil || !s.durabilityState.check(id) {
		return ErrDurability
	}
	return nil
}

// WaitDurable covers all WAL inserted before this method captures its target.
// It is useful for a benchmark's raw control writes on the same fixed primary.
// Application operations use waitDurableConn on their own pinned connection.
func (s *Store) WaitDurable(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if s.durability.RequiredStandbys == 0 {
		return conn.Ping(ctx)
	}
	return s.waitDurableConn(ctx, conn.Conn())
}

// Called only after the preceding implicit/explicit transaction is fully
// drained. The first query captures one fixed LSN covering the previous commit,
// including a competing winner read by a duplicate operation. Subsequent
// polls never move that target. Each poll is a new transaction/statistics view.
// A successful SQL command alone (e.g. after a SyncRep WARNING) is insufficient.
// Physical standbys must also run with fsync=on; this is a deployment invariant.
func (s *Store) waitDurableConn(ctx context.Context, conn *pgx.Conn) error {
	if s.durability.RequiredStandbys == 0 {
		return nil
	}
	// Prevent callers without a deadline from waiting forever on lost replicas.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var target *string
	// Most healthy replica lag is short. Probe again promptly, then cap the
	// frequency at the original 5ms interval during a prolonged outage.
	retryDelay := time.Millisecond
	for {
		var id primaryIdentity
		var currentTarget, setting, fsync, commit, fullPages string
		var recovery, localFlush bool
		var replicas int
		queryStarted := time.Now()
		err := conn.QueryRow(ctx, `WITH target AS MATERIALIZED (
			SELECT coalesce($1::pg_lsn, pg_catalog.pg_current_wal_insert_lsn()) AS lsn
		)
		SELECT target.lsn::text, pg_catalog.pg_is_in_recovery(),
			current_setting('fsync'), current_setting('synchronous_commit'),
			current_setting('full_page_writes'), current_setting('synchronous_standby_names'),
			(pg_catalog.pg_control_system()).system_identifier::text,
			('x' || left(pg_catalog.pg_walfile_name(target.lsn),8))::bit(32)::bigint,
			pg_catalog.pg_postmaster_start_time(),
			pg_catalog.pg_current_wal_flush_lsn() >= target.lsn,
			(SELECT count(DISTINCT r.application_name)::integer
			 FROM pg_catalog.pg_stat_replication r
			 JOIN pg_catalog.pg_replication_slots p ON p.active_pid=r.pid AND p.slot_type='physical'
			 WHERE r.application_name=ANY($2::text[]) AND r.state='streaming'
			 AND r.sync_state='quorum' AND r.flush_lsn >= target.lsn)
		FROM target`, target, s.durability.StandbyNames).
			Scan(&currentTarget, &recovery, &fsync, &commit, &fullPages, &setting,
				&id.SystemID, &id.Timeline, &id.Started, &localFlush, &replicas)
		queryElapsed := time.Since(queryStarted)
		s.diagnostics.proofQueries.Add(1)
		s.diagnostics.proofQueryNS.Add(uint64(queryElapsed))
		if err != nil {
			return fmt.Errorf("%w: replication proof unavailable", ErrDurability)
		}
		if recovery || fsync != "on" || commit != "on" || fullPages != "on" ||
			compactSetting(setting) != compactSetting(s.durability.expectedSetting()) || !s.durabilityState.check(id) {
			return ErrDurability
		}
		if localFlush && replicas >= s.durability.RequiredStandbys {
			return nil
		}
		if target == nil {
			target = &currentTarget
		}
		s.diagnostics.proofRetries.Add(1)
		waitBegan := time.Now()
		timer := time.NewTimer(retryDelay)
		retryDelay = min(2*retryDelay, 5*time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			s.diagnostics.proofWaitNS.Add(uint64(time.Since(waitBegan)))
			return fmt.Errorf("%w: replication proof deadline", ErrDurability)
		case <-timer.C:
			s.diagnostics.proofWaitNS.Add(uint64(time.Since(waitBegan)))
		}
	}
}

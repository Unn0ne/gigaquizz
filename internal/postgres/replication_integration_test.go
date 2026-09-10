package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"gigaquizz/internal/poll"

	"github.com/jackc/pgx/v5"
)

type labMetadata struct {
	CurrentPrimary string   `json:"current_primary"`
	PrimaryURL     string   `json:"primary_url"`
	Retired        []string `json:"retired_nodes"`
	Nodes          map[string]struct {
		URL  string `json:"url"`
		Name string `json:"application_name"`
	} `json:"nodes"`
	Policy struct {
		Required int      `json:"minimum_remote_flush"`
		Names    []string `json:"expected_standby_names"`
	} `json:"durability_policy"`
}

func labRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the owned replication lab")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
}

func readLab(t *testing.T, root string) labMetadata {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, ".local/replication-lab/metadata.json"))
	if err != nil {
		t.Fatal("run make lab-up before replication tests")
	}
	var m labMetadata
	if json.Unmarshal(data, &m) != nil || m.CurrentPrimary == "" || m.Policy.Required != 1 || len(m.Nodes) != 3 {
		t.Fatal("unexpected owned replication lab metadata")
	}
	return m
}

func labCommand(root string, args ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "python3", append([]string{filepath.Join(root, "scripts/replication_lab.py")}, args...)...)
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("replication lab %v: %w: %s", args, err, output)
	}
	return nil
}

func schemaDSN(t *testing.T, dsn, schema string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("invalid test database URL")
	}
	q := u.Query()
	q.Set("search_path", schema)
	q.Set("application_name", "gigaquizz_replication_test")
	u.RawQuery = q.Encode()
	return u.String()
}

func eventuallyLab(t *testing.T, timeout time.Duration, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(description)
}

// This test intentionally stops/promotes ONLY nodes owned by our local lab.
// It must never run concurrently with benchmarks or other lab operators.
// No externally supplied DATABASE_URL is used for destructive fault actions.
func TestReplicatedDurability(t *testing.T) {
	if os.Getenv("GIGAQUIZZ_REPLICATION_TEST") != "1" {
		t.Skip("make lab-up, then make replication-test; exclusively owns the local lab during faults")
	}
	root := labRoot(t)
	m := readLab(t, root)
	oldPrimary := m.CurrentPrimary
	var replicaA, replicaB string
	for node, info := range m.Nodes {
		if info.Name == "gigaquizz_ha_a" {
			replicaA = node
		}
		if info.Name == "gigaquizz_ha_b" {
			replicaB = node
		}
	}
	if replicaA == "" || replicaB == "" || replicaA == oldPrimary || replicaB == oldPrimary {
		t.Fatal("restore all three lab nodes before the test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, m.PrimaryURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	schema := "gigaquizz_replication_test_" + randomToken(t)[:12]
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	dsn := schemaDSN(t, m.PrimaryURL, schema)
	opts := DurabilityOptions{RequiredStandbys: m.Policy.Required, StandbyNames: m.Policy.Names}
	s, err := NewWithOptions(ctx, dsn, 32, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer func() {
		// up restores the fixed settings and starts the eligible replicas even
		// if an assertion failed while replication was deliberately unavailable.
		if err := labCommand(root, "up"); err != nil {
			t.Error(err)
			return
		}
		latest := readLab(t, root)
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		c, err := pgx.Connect(cleanup, latest.PrimaryURL)
		if err != nil {
			t.Error("cannot reconnect for isolated schema cleanup")
			return
		}
		defer c.Close(cleanup)
		if _, err = c.Exec(cleanup, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Error(err)
		}
	}()
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if unexpected, err := New(ctx, dsn, 1); err == nil {
		unexpected.Close()
		t.Fatal("implicit local policy accepted a synchronously replicated server")
	}
	// Leave enough real open-window time for process-control fault operations.
	start := dbNow(t, s).Add(-30 * time.Second)
	p, err := s.Create(ctx, poll.CreateInput{Question: "Replicated fault test", Type: "single", Options: []string{"One", "Two"}, StartsAt: &start})
	if err != nil {
		t.Fatal(err)
	}
	baseToken, replayToken, lostToken, driftToken, nativeToken := randomToken(t), randomToken(t), randomToken(t), randomToken(t), randomToken(t)

	t.Log("concurrent repeats: exactly one durable accepted result")
	var wg sync.WaitGroup
	replies := make(chan voteReply, 24)
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); r, e := s.Vote(ctx, p.ID, baseToken, []int{1}); replies <- voteReply{r, e} }()
	}
	wg.Wait()
	close(replies)
	accepted, duplicates := 0, 0
	for reply := range replies {
		if reply.err != nil {
			t.Fatal(reply.err)
		}
		switch reply.receipt.Status {
		case "accepted":
			accepted++
		case "duplicate":
			duplicates++
		default:
			t.Fatal("unexpected concurrent receipt", reply.receipt.Status)
		}
	}
	if accepted != 1 || duplicates != 23 {
		t.Fatalf("accepted=%d duplicate=%d", accepted, duplicates)
	}

	t.Log("durable flush is sufficient even while standby replay is paused")
	if err = labCommand(root, "stop-node", replicaB); err != nil {
		t.Fatal(err)
	}
	a, err := pgx.Connect(ctx, schemaDSN(t, m.Nodes[replicaA].URL, schema))
	if err != nil {
		t.Fatal(err)
	}
	eventuallyLab(t, 5*time.Second, "replica did not apply the test fixture before pause", func() bool {
		var n int
		return a.QueryRow(ctx, "SELECT count(*) FROM votes WHERE poll_id=$1::uuid AND token=$2::uuid", p.ID, baseToken).Scan(&n) == nil && n == 1
	})
	if _, err = a.Exec(ctx, "SELECT pg_wal_replay_pause()"); err != nil {
		t.Fatal(err)
	}
	eventuallyLab(t, 3*time.Second, "replay did not pause", func() bool {
		var state string
		return a.QueryRow(ctx, "SELECT pg_get_wal_replay_pause_state()").Scan(&state) == nil && state == "paused"
	})
	r, err := s.Vote(ctx, p.ID, replayToken, []int{2})
	if err != nil || r.Status != "accepted" {
		t.Fatalf("flush without replay: status=%s error=%v", r.Status, err)
	}
	var visible int
	if err = a.QueryRow(ctx, "SELECT count(*) FROM votes WHERE poll_id=$1::uuid AND token=$2::uuid", p.ID, replayToken).Scan(&visible); err != nil || visible != 0 {
		t.Fatal("paused replica unexpectedly exposes the new vote", err)
	}
	if _, err = a.Exec(ctx, "SELECT pg_wal_replay_resume()"); err != nil {
		t.Fatal(err)
	}
	_ = a.Close(ctx)
	if err = labCommand(root, "start-node", replicaB); err != nil {
		t.Fatal(err)
	}

	t.Log("configuration drift cannot silently weaken the required quorum")
	if _, err = admin.Exec(ctx, "ALTER SYSTEM SET synchronous_standby_names = ''"); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, "SELECT pg_reload_conf()"); err != nil {
		t.Fatal(err)
	}
	eventuallyLab(t, 2*time.Second, "configuration did not reload", func() bool {
		var v string
		return admin.QueryRow(ctx, "SHOW synchronous_standby_names").Scan(&v) == nil && v == ""
	})
	short, done := context.WithTimeout(ctx, time.Second)
	r, err = s.Vote(short, p.ID, driftToken, []int{1})
	done()
	if err == nil {
		t.Fatal("policy drift returned an acknowledged receipt", r.Status)
	}
	if err = labCommand(root, "up"); err != nil {
		t.Fatal(err)
	}
	r, err = s.Vote(ctx, p.ID, driftToken, []int{1})
	if err != nil || r.Status != "duplicate" {
		t.Fatal("cannot recover drifted local commit", r.Status, err)
	}

	t.Log("cancel SyncRep: local commit must not become a false durable receipt")
	if err = labCommand(root, "stop-node", replicaA); err != nil {
		t.Fatal(err)
	}
	if err = labCommand(root, "stop-node", replicaB); err != nil {
		t.Fatal(err)
	}
	forceLocalCommit := func(query string, args ...any) {
		t.Helper()
		raw, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer raw.Close(context.Background())
		pid := raw.PgConn().PID()
		result := make(chan error, 1)
		go func() { var ignored any; result <- raw.QueryRow(ctx, query, args...).Scan(&ignored) }()
		eventuallyLab(t, 5*time.Second, "operation never reached SyncRep wait", func() bool {
			var waiting bool
			return admin.QueryRow(ctx, "SELECT coalesce((SELECT wait_event='SyncRep' FROM pg_stat_activity WHERE pid=$1),false)", pid).Scan(&waiting) == nil && waiting
		})
		if _, err = admin.Exec(ctx, "SELECT pg_cancel_backend($1)", pid); err != nil {
			t.Fatal(err)
		}
		select {
		case err = <-result:
			if err != nil {
				t.Fatal("expected local commit with PostgreSQL warning", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("cancelled SyncRep did not finish")
		}
	}
	forceLocalCommit("SELECT status FROM record_vote($1::uuid,$2::uuid,ARRAY[1])", p.ID, lostToken)
	if err = admin.QueryRow(ctx, "SELECT count(*) FROM "+quoted+".votes WHERE poll_id=$1::uuid AND token=$2::uuid", p.ID, lostToken).Scan(&visible); err != nil || visible != 1 {
		t.Fatal("cancelled SyncRep did not leave the intended local commit", err)
	}
	// Exercise the actual new-vote path too: native PostgreSQL cancellation
	// returns only a WARNING after local commit. The repository must still
	// withhold its accepted receipt until its separate proof succeeds.
	t.Log("new Store.Vote cannot turn a cancelled SyncRep WARNING into accepted")
	nativeCtx, nativeCancel := context.WithTimeout(ctx, 2*time.Second)
	defer nativeCancel()
	nativeReply := make(chan voteReply, 1)
	go func() {
		receipt, e := s.Vote(nativeCtx, p.ID, nativeToken, []int{1})
		nativeReply <- voteReply{receipt, e}
	}()
	var nativePID int
	eventuallyLab(t, time.Second, "repository vote did not enter SyncRep", func() bool {
		return admin.QueryRow(ctx, `SELECT coalesce(min(pid),0) FROM pg_stat_activity
			WHERE application_name='gigaquizz' AND wait_event='SyncRep' AND query LIKE '%record_vote%'`).Scan(&nativePID) == nil && nativePID != 0
	})
	if _, err = admin.Exec(ctx, "SELECT pg_cancel_backend($1)", nativePID); err != nil {
		t.Fatal(err)
	}
	select {
	case reply := <-nativeReply:
		if reply.err == nil {
			t.Fatal("new vote acknowledged after cancelled SyncRep", reply.receipt.Status)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("repository did not respect its proof deadline")
	}
	for _, choices := range [][]int{{1}, {2}} {
		short, done := context.WithTimeout(ctx, 300*time.Millisecond)
		r, err = s.Vote(short, p.ID, lostToken, choices)
		done()
		if err == nil {
			t.Fatal("unreplicated duplicate/conflict was acknowledged", r.Status)
		}
	}
	short, done = context.WithTimeout(ctx, 300*time.Millisecond)
	_, err = s.Results(short, p.ID)
	done()
	if err == nil {
		t.Fatal("unreplicated intermediate result was published")
	}

	t.Log("cancelled finalization is not published before its replica proof")
	eventuallyLab(t, 40*time.Second, "poll did not reach its actual deadline", func() bool {
		var closed bool
		return admin.QueryRow(ctx, "SELECT clock_timestamp()>=$1", p.EndsAt).Scan(&closed) == nil && closed
	})
	forceLocalCommit("SELECT finalize_poll($1::uuid)", p.ID)
	for _, read := range []func(context.Context) error{
		func(c context.Context) error { _, e := s.Results(c, p.ID); return e },
		func(c context.Context) error { _, e := s.Get(c, p.ID); return e },
		func(c context.Context) error { _, e := s.List(c); return e },
	} {
		short, done := context.WithTimeout(ctx, 300*time.Millisecond)
		e := read(short)
		done()
		if e == nil {
			t.Fatal("unreplicated final state was published")
		}
	}
	if err = labCommand(root, "start-node", replicaA); err != nil {
		t.Fatal(err)
	}
	if err = labCommand(root, "start-node", replicaB); err != nil {
		t.Fatal(err)
	}
	r, err = s.Vote(ctx, p.ID, lostToken, []int{1})
	if err != nil || r.Status != "duplicate" {
		t.Fatal("late duplicate failed after replication recovery", r.Status, err)
	}
	r, err = s.Vote(ctx, p.ID, lostToken, []int{2})
	if err != nil || r.Status != "conflict" {
		t.Fatal("late conflict failed after replication recovery", r.Status, err)
	}
	result, err := s.Results(ctx, p.ID)
	if err != nil || result.State != "final" || result.TotalVotes != 5 {
		t.Fatal("unexpected recovered final", result.State, result.TotalVotes, err)
	}
	r, err = s.Vote(ctx, p.ID, nativeToken, []int{1})
	if err != nil || r.Status != "duplicate" {
		t.Fatal("native cancelled vote was not recovered", r.Status, err)
	}

	t.Log("fence primary, promote a caught-up replica, reconcile every acknowledged key")
	var target string
	if err = admin.QueryRow(ctx, "SELECT pg_current_wal_insert_lsn()::text").Scan(&target); err != nil {
		t.Fatal(err)
	}
	a, err = pgx.Connect(ctx, m.Nodes[replicaA].URL)
	if err != nil {
		t.Fatal(err)
	}
	eventuallyLab(t, 5*time.Second, "promotion candidate did not catch up", func() bool {
		var caught bool
		return a.QueryRow(ctx, "SELECT coalesce(pg_last_wal_replay_lsn()>=$1::pg_lsn,false)", target).Scan(&caught) == nil && caught
	})
	_ = a.Close(ctx)
	if err = labCommand(root, "stop-node", oldPrimary, "--mode", "immediate"); err != nil {
		t.Fatal(err)
	}
	if err = labCommand(root, "promote", replicaA); err != nil {
		t.Fatal(err)
	}
	latest := readLab(t, root)
	next, err := NewWithOptions(ctx, schemaDSN(t, latest.PrimaryURL, schema), 8, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	for token, choice := range map[string]int{baseToken: 1, replayToken: 2, lostToken: 1, driftToken: 1, nativeToken: 1} {
		r, err = next.Vote(ctx, p.ID, token, []int{choice})
		if err != nil || r.Status != "duplicate" {
			t.Fatal("acknowledged key not preserved after promotion", r.Status, err)
		}
	}
	result, err = next.Results(ctx, p.ID)
	if err != nil || result.State != "final" || result.TotalVotes != 5 || result.Options[0].Votes != 4 || result.Options[1].Votes != 1 {
		t.Fatal("final result changed after promotion", err)
	}
	if err = labCommand(root, "start-node", oldPrimary); err == nil {
		t.Fatal("stale former primary was allowed to restart")
	}
	if err = labCommand(root, "rebuild-node", oldPrimary); err != nil {
		t.Fatal(err)
	}
	short, done = context.WithTimeout(ctx, 400*time.Millisecond)
	err = s.WaitDurable(short)
	done()
	if err == nil {
		t.Fatal("old Store followed a changed primary identity")
	}
	t.Log("restart of the same primary requires a new Store even on the same timeline")
	if err = labCommand(root, "stop-node", latest.CurrentPrimary, "--mode", "immediate"); err != nil {
		t.Fatal(err)
	}
	if err = labCommand(root, "start-node", latest.CurrentPrimary); err != nil {
		t.Fatal(err)
	}
	short, done = context.WithTimeout(ctx, 500*time.Millisecond)
	err = next.WaitDurable(short)
	done()
	if err == nil {
		t.Fatal("Store silently followed a restarted primary")
	}
	fresh, err := NewWithOptions(ctx, schemaDSN(t, latest.PrimaryURL, schema), 2, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	result, err = fresh.Results(ctx, p.ID)
	if err != nil || result.TotalVotes != 5 || result.State != "final" {
		t.Fatal("fresh Store failed after primary restart", err)
	}
	t.Log("all five acknowledged keys and final counts preserved; old branch fenced and rebuilt")
}

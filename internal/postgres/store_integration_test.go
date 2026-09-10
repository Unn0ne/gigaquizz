package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"gigaquizz/internal/poll"

	"github.com/jackc/pgx/v5"
)

// Every test owns a randomly named schema. We never drop or truncate a user's
// tables, even when TEST_DATABASE_URL points to a database containing data.
func integrationStore(t *testing.T) (*Store, string) {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	schema := "gigaquizz_test_" + randomToken(t)[:16]
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		_ = admin.Close(ctx)
		t.Fatal(err)
	}
	u, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	query := u.Query()
	query.Set("search_path", schema)
	u.RawQuery = query.Encode()
	store, err := New(ctx, u.String(), 40)
	if err != nil {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+quoted+" CASCADE")
		_ = admin.Close(ctx)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		store.Close()
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := admin.Exec(cleanup, "DROP SCHEMA "+quoted+" CASCADE")
		if err != nil {
			t.Error(err)
		}
		_ = admin.Close(cleanup)
	})
	if err = store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return store, u.String()
}

func randomToken(t *testing.T) string {
	t.Helper()
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(data[:])
}

func dbNow(t *testing.T, s *Store) time.Time {
	t.Helper()
	var now time.Time
	if err := s.pool.QueryRow(context.Background(), "SELECT clock_timestamp()").Scan(&now); err != nil {
		t.Fatal(err)
	}
	return now
}

func createPoll(t *testing.T, s *Store, kind string, start *time.Time) poll.Poll {
	t.Helper()
	options := []string{"One", "Two"}
	if kind == "multiple" {
		options = append(options, "Three")
	}
	p, err := s.Create(context.Background(), poll.CreateInput{Question: "Pick an option", Type: kind, Options: options, StartsAt: start})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func closingSoon(t *testing.T, s *Store) poll.Poll {
	t.Helper()
	start := dbNow(t, s).Add(-59 * time.Second)
	return createPoll(t, s, "single", &start)
}

func awaitClose(t *testing.T, s *Store, p poll.Poll) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if !dbNow(t, s).Before(p.EndsAt) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("poll did not close on database clock")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func awaitBlockedAdmission(t *testing.T, s *Store, p poll.Poll) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var blocked bool
		err := s.pool.QueryRow(context.Background(), `SELECT EXISTS (
			SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted
			AND classid = ((hashtext($1::text)::bigint & 4294967295)::oid)
		)`, p.ID).Scan(&blocked)
		if err != nil {
			t.Fatal(err)
		}
		if blocked {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("expected an admission/finalization advisory lock wait")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func uncommittedVote(t *testing.T, s *Store, p poll.Poll, token string) pgx.Tx {
	t.Helper()
	tx, err := s.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rollback(tx) })
	var status string
	err = tx.QueryRow(context.Background(), "SELECT status FROM record_vote($1::uuid, $2::uuid, ARRAY[1])", p.ID, token).Scan(&status)
	if err != nil {
		t.Fatal(err)
	}
	if status != "accepted" {
		t.Fatalf("uncommitted vote status = %s", status)
	}
	return tx
}

type voteReply struct {
	receipt poll.Receipt
	err     error
}

func asyncVote(s *Store, p poll.Poll, token string, choices []int) <-chan voteReply {
	ch := make(chan voteReply, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		r, err := s.Vote(ctx, p.ID, token, choices)
		ch <- voteReply{r, err}
	}()
	return ch
}

func receiveVote(t *testing.T, ch <-chan voteReply) poll.Receipt {
	t.Helper()
	select {
	case result := <-ch:
		if result.err != nil {
			t.Fatal(result.err)
		}
		return result.receipt
	case <-time.After(8 * time.Second):
		t.Fatal("vote did not finish")
		return poll.Receipt{}
	}
}

func TestMigrationAndSessionSettings(t *testing.T) {
	s, _ := integrationStore(t)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.pool.QueryRow(context.Background(), "SELECT count(*) FROM gigaquizz_migrations").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("migration rows = %d", count)
	}
	var durability, isolation, statementTimeout, lockTimeout string
	var version int
	err := s.pool.QueryRow(context.Background(), `SELECT current_setting('synchronous_commit'),
		current_setting('default_transaction_isolation'), current_setting('statement_timeout'),
		current_setting('lock_timeout'), current_setting('server_version_num')::integer`).
		Scan(&durability, &isolation, &statementTimeout, &lockTimeout, &version)
	if err != nil {
		t.Fatal(err)
	}
	if durability != "on" || isolation != "read committed" || statementTimeout == "0" || lockTimeout == "0" || version < 160000 {
		t.Fatalf("unexpected database settings: %q, %q, %q, %q, version %d", durability, isolation, statementTimeout, lockTimeout, version)
	}
}

func TestConcurrentCreationRejectsOverlapAndAllowsAdjacentWindows(t *testing.T) {
	s, _ := integrationStore(t)
	start := dbNow(t, s).Add(time.Hour)
	const writers = 12
	results := make(chan error, writers)
	var wg sync.WaitGroup
	for range writers {
		wg.Go(func() {
			_, err := s.Create(context.Background(), poll.CreateInput{Question: "Pick", Type: "single", Options: []string{"A", "B"}, StartsAt: &start})
			results <- err
		})
	}
	wg.Wait()
	close(results)
	created, overlaps := 0, 0
	for err := range results {
		switch {
		case err == nil:
			created++
		case errors.Is(err, poll.ErrOverlap):
			overlaps++
		default:
			t.Fatal(err)
		}
	}
	if created != 1 || overlaps != writers-1 {
		t.Fatalf("created %d, overlaps %d", created, overlaps)
	}
	list, err := s.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].EndsAt.Sub(list[0].StartsAt) != time.Minute {
		t.Fatalf("unexpected polls: %+v", list)
	}
	adjacent := createPoll(t, s, "ab", &list[0].EndsAt)
	got, err := s.Get(context.Background(), adjacent.ID)
	if err != nil || got.ID != adjacent.ID {
		t.Fatalf("Get = %+v, %v", got, err)
	}
}

func TestConcurrentDuplicatesAndConflictingChoices(t *testing.T) {
	for _, different := range []bool{false, true} {
		t.Run(fmt.Sprintf("different_choices_%t", different), func(t *testing.T) {
			s, _ := integrationStore(t)
			p := createPoll(t, s, "single", nil)
			token := randomToken(t)
			const writers = 32
			start := make(chan struct{})
			replies := make(chan voteReply, writers)
			var wg sync.WaitGroup
			for i := range writers {
				choice := 1
				if different {
					choice += i % 2
				}
				wg.Go(func() {
					<-start
					r, err := s.Vote(context.Background(), p.ID, token, []int{choice})
					replies <- voteReply{r, err}
				})
			}
			close(start)
			wg.Wait()
			close(replies)
			statuses := map[string]int{}
			var winningChoices []int
			var acceptedAt *time.Time
			for reply := range replies {
				if reply.err != nil {
					t.Fatal(reply.err)
				}
				statuses[reply.receipt.Status]++
				if winningChoices == nil {
					winningChoices = reply.receipt.Choices
					acceptedAt = reply.receipt.AcceptedAt
				}
				if !reflect.DeepEqual(reply.receipt.Choices, winningChoices) || reply.receipt.AcceptedAt == nil || !reply.receipt.AcceptedAt.Equal(*acceptedAt) {
					t.Fatalf("replies disagree about winner: %+v", reply.receipt)
				}
			}
			expectedDuplicates, expectedConflicts := writers-1, 0
			if different {
				expectedDuplicates, expectedConflicts = writers/2-1, writers/2
			}
			if statuses["accepted"] != 1 || statuses["duplicate"] != expectedDuplicates || statuses["conflict"] != expectedConflicts {
				t.Fatalf("statuses = %v", statuses)
			}
			counts, err := s.Results(context.Background(), p.ID)
			if err != nil {
				t.Fatal(err)
			}
			if counts.TotalVotes != 1 || counts.Options[winningChoices[0]-1].Votes != 1 {
				t.Fatalf("results = %+v", counts)
			}
		})
	}
}

func TestValidationMultipleChoiceAndNotOpen(t *testing.T) {
	s, _ := integrationStore(t)
	p := createPoll(t, s, "multiple", nil)
	token := randomToken(t)
	accepted, err := s.Vote(context.Background(), p.ID, token, []int{3, 1})
	if err != nil || accepted.Status != "accepted" || !reflect.DeepEqual(accepted.Choices, []int{1, 3}) {
		t.Fatalf("receipt = %+v, %v", accepted, err)
	}
	duplicate, err := s.Vote(context.Background(), p.ID, token, []int{1, 3})
	if err != nil || duplicate.Status != "duplicate" {
		t.Fatalf("duplicate = %+v, %v", duplicate, err)
	}
	for _, choices := range [][]int{nil, {}, {4}, {1, 1}, {0}, {21}} {
		r, err := s.Vote(context.Background(), p.ID, randomToken(t), choices)
		if err != nil || r.Status != "invalid" {
			t.Fatalf("choices %v: %+v, %v", choices, r, err)
		}
	}
	counts, err := s.Results(context.Background(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if counts.TotalVotes != 1 || counts.Options[0].Votes != 1 || counts.Options[1].Votes != 0 || counts.Options[2].Votes != 1 {
		t.Fatalf("multiple-choice results = %+v", counts)
	}
	start := p.EndsAt.Add(time.Minute)
	future := createPoll(t, s, "single", &start)
	r, err := s.Vote(context.Background(), future.ID, randomToken(t), []int{1})
	if err != nil || r.Status != "not_open" {
		t.Fatalf("future vote = %+v, %v", r, err)
	}
	r, err = s.Vote(context.Background(), "00000000-0000-0000-0000-000000000000", randomToken(t), []int{1})
	if err != nil || r.Status != "not_found" {
		t.Fatalf("missing poll = %+v, %v", r, err)
	}
}

func TestInsertConflictUsesFreshSnapshotAfterConcurrentCommit(t *testing.T) {
	for _, choice := range []int{1, 2} {
		t.Run(fmt.Sprintf("choice_%d", choice), func(t *testing.T) {
			s, _ := integrationStore(t)
			p := createPoll(t, s, "single", nil)
			token := randomToken(t)
			winner := uncommittedVote(t, s, p, token)
			var transactionID string
			if err := winner.QueryRow(context.Background(), "SELECT pg_current_xact_id()::text").Scan(&transactionID); err != nil {
				t.Fatal(err)
			}
			result := asyncVote(s, p, token, []int{choice})
			deadline := time.Now().Add(3 * time.Second)
			for {
				var blocked bool
				err := s.pool.QueryRow(context.Background(), `SELECT EXISTS (
					SELECT 1 FROM pg_locks WHERE locktype = 'transactionid'
					AND NOT granted AND transactionid::text = $1
				)`, transactionID).Scan(&blocked)
				if err != nil {
					t.Fatal(err)
				}
				if blocked {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("replay did not wait on the uncommitted unique-key winner")
				}
				time.Sleep(5 * time.Millisecond)
			}
			if err := winner.Commit(context.Background()); err != nil {
				t.Fatal(err)
			}
			receipt := receiveVote(t, result)
			want := "duplicate"
			if choice == 2 {
				want = "conflict"
			}
			if receipt.Status != want || !reflect.DeepEqual(receipt.Choices, []int{1}) {
				t.Fatalf("receipt after competing commit = %+v, want %s", receipt, want)
			}
		})
	}
}

func TestAdmittedVoteCommitsAfterCloseFinalizationDrainsAndReconnectReplays(t *testing.T) {
	s, databaseURL := integrationStore(t)
	p := closingSoon(t, s)
	token := randomToken(t)
	tx := uncommittedVote(t, s, p, token)
	awaitClose(t, s, p)
	type finalReply struct {
		count int
		err   error
	}
	finished := make(chan finalReply, 1)
	go func() {
		n, err := s.FinalizeDue(context.Background())
		finished <- finalReply{n, err}
	}()
	awaitBlockedAdmission(t, s, p)
	provisional, err := s.Results(context.Background(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if provisional.TotalVotes != 0 || provisional.State != "processing" {
		t.Fatalf("provisional snapshot = %+v", provisional)
	}
	select {
	case result := <-finished:
		t.Fatalf("finalized before admitted transaction completed: %+v", result)
	default:
	}
	if err = tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-finished:
		if result.err != nil || result.count != 1 {
			t.Fatalf("finalize = %+v", result)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("finalization did not finish")
	}
	final, err := s.Results(context.Background(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != "final" || final.TotalVotes != 1 || final.Options[0].Votes != 1 {
		t.Fatalf("final = %+v", final)
	}
	if n, err := s.FinalizeDue(context.Background()); err != nil || n != 0 {
		t.Fatalf("second finalization = %d, %v", n, err)
	}
	late, err := s.Vote(context.Background(), p.ID, randomToken(t), []int{2})
	if err != nil || late.Status != "closed" {
		t.Fatalf("new late vote = %+v, %v", late, err)
	}
	// Reopen the application's connections, without restarting PostgreSQL. The
	// original successful database transaction's response was never delivered
	// to an HTTP client; its same-token replay must still recover the receipt.
	s.Close()
	reopened, err := New(context.Background(), databaseURL, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	replay, err := reopened.Vote(context.Background(), p.ID, token, []int{1})
	if err != nil || replay.Status != "duplicate" {
		t.Fatalf("replay = %+v, %v", replay, err)
	}
	after, err := reopened.Results(context.Background(), p.ID)
	if err != nil || !reflect.DeepEqual(final, after) {
		t.Fatalf("final result changed: %+v, %v", after, err)
	}
}

func TestLateRepeatWaitsForCommitOrRollback(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		commit bool
		choice int
		status string
	}{
		{"commit_same", true, 1, "duplicate"},
		{"commit_conflict", true, 2, "conflict"},
		{"rollback", false, 1, "closed"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			s, _ := integrationStore(t)
			p := closingSoon(t, s)
			token := randomToken(t)
			tx := uncommittedVote(t, s, p, token)
			awaitClose(t, s, p)
			result := asyncVote(s, p, token, []int{scenario.choice})
			awaitBlockedAdmission(t, s, p)
			select {
			case reply := <-result:
				t.Fatalf("late replay failed to wait: %+v", reply)
			default:
			}
			var err error
			if scenario.commit {
				err = tx.Commit(context.Background())
			} else {
				err = tx.Rollback(context.Background())
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := receiveVote(t, result); got.Status != scenario.status {
				t.Fatalf("late replay = %+v, want %s", got, scenario.status)
			}
		})
	}
}

func TestDeadlineCrossedWhileWaitingForSharedLockIsUnknown(t *testing.T) {
	s, _ := integrationStore(t)
	p := closingSoon(t, s)
	token := randomToken(t)
	blocker, err := s.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(blocker)
	if _, err = blocker.Exec(context.Background(), "SELECT pg_advisory_xact_lock(hashtext($1::text), vote_lock_shard($2::uuid))", p.ID, token); err != nil {
		t.Fatal(err)
	}
	result := asyncVote(s, p, token, []int{1})
	awaitBlockedAdmission(t, s, p)
	awaitClose(t, s, p)
	if err = blocker.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := receiveVote(t, result); got.Status != "unknown" {
		t.Fatalf("boundary receipt = %+v", got)
	}
	resolved, err := s.Vote(context.Background(), p.ID, token, []int{1})
	if err != nil || resolved.Status != "closed" {
		t.Fatalf("resolved receipt = %+v, %v", resolved, err)
	}
	counts, err := s.Results(context.Background(), p.ID)
	if err != nil || counts.TotalVotes != 0 {
		t.Fatalf("late admission created a vote: %+v, %v", counts, err)
	}
}

func TestBackendDeathBeforeCommitDoesNotCreateConfirmedVote(t *testing.T) {
	s, _ := integrationStore(t)
	p := closingSoon(t, s)
	token := randomToken(t)
	tx := uncommittedVote(t, s, p, token)
	pid := tx.Conn().PgConn().PID()
	var killed bool
	if err := s.pool.QueryRow(context.Background(), "SELECT pg_terminate_backend($1)", pid).Scan(&killed); err != nil || !killed {
		t.Fatalf("terminate uncommitted backend = %t, %v", killed, err)
	}
	if err := tx.Commit(context.Background()); err == nil {
		t.Fatal("terminated transaction unexpectedly committed")
	}
	awaitClose(t, s, p)
	r, err := s.Vote(context.Background(), p.ID, token, []int{1})
	if err != nil || r.Status != "closed" {
		t.Fatalf("receipt after backend death = %+v, %v", r, err)
	}
	counts, err := s.Results(context.Background(), p.ID)
	if err != nil || counts.TotalVotes != 0 {
		t.Fatalf("uncommitted vote survived: %+v, %v", counts, err)
	}
}

func TestCancelledLockWaitDoesNotConsumeToken(t *testing.T) {
	s, _ := integrationStore(t)
	p := createPoll(t, s, "single", nil)
	token := randomToken(t)
	blocker, err := s.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(blocker)
	if _, err = blocker.Exec(context.Background(), "SELECT pg_advisory_xact_lock(hashtext($1::text), vote_lock_shard($2::uuid))", p.ID, token); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reply := make(chan error, 1)
	go func() { _, err := s.Vote(ctx, p.ID, token, []int{1}); reply <- err }()
	awaitBlockedAdmission(t, s, p)
	cancel()
	select {
	case err := <-reply:
		if err == nil {
			t.Fatal("cancelled vote returned no error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled query did not return")
	}
	if err = blocker.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	r, err := s.Vote(context.Background(), p.ID, token, []int{1})
	if err != nil || r.Status != "accepted" {
		t.Fatalf("retry after cancellation = %+v, %v", r, err)
	}
}

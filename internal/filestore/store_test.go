package filestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"gigaquizz/internal/filelog"
	"gigaquizz/internal/httpapi"
	"gigaquizz/internal/poll"
)

const tokenA = "10000000000000000000000000000001"
const tokenB = "10000000000000000000000000000002"

func configForTest(t *testing.T) Config {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return Config{Directory: filepath.Join(base, "data"), MaxUnique: 1000, BatchSize: 32, QueueVotes: 128, Linger: time.Millisecond}
}

func openTest(t *testing.T, c Config) *Store {
	t.Helper()
	s, err := New(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func createEndingSoon(t *testing.T, s *Store, kind string, options []string) poll.Poll {
	t.Helper()
	start := time.Now().UTC().Add(-58 * time.Second)
	p, err := s.Create(context.Background(), poll.CreateInput{Question: "Выбор?", Type: kind, Options: options, StartsAt: &start})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func finalizeTest(t *testing.T, s *Store, p poll.Poll) {
	t.Helper()
	delay := time.Until(p.EndsAt)
	if delay > 0 {
		time.Sleep(delay + time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if n, err := s.FinalizeDue(ctx); err != nil || n != 1 {
		t.Fatalf("finalize: n=%d err=%v", n, err)
	}
}

func recorded(t *testing.T, s *Store, p poll.Poll, token string, choices ...int) poll.Receipt {
	t.Helper()
	r, err := s.Vote(context.Background(), p.ID, token, choices)
	if err != nil || r.Status != "recorded" || r.AcceptedAt == nil || !r.AcceptedAt.Before(p.EndsAt) || r.AcceptedAt.Before(p.StartsAt) {
		t.Fatalf("durable attempt: %+v %v", r, err)
	}
	return r
}

func TestRepositoryActiveAndFinalRestartPreserveFirstFullTokenChoice(t *testing.T) {
	c := configForTest(t)
	s := openTest(t, c)
	p := createEndingSoon(t, s, "single", []string{"A", "B"})
	first := recorded(t, s, p, tokenA, 1)
	var wg sync.WaitGroup
	failures := make(chan error, 24)
	for range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := s.Vote(context.Background(), p.ID, tokenA, []int{2})
			if err != nil || r.Status != "recorded" || !reflect.DeepEqual(r.Choices, []int{2}) {
				failures <- fmt.Errorf("duplicate attempt: %+v %v", r, err)
			}
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	recorded(t, s, p, tokenB, 2)
	pending, err := s.Results(context.Background(), p.ID)
	if err != nil || !pending.Pending || pending.State != "open" || len(pending.Options) != 2 || pending.TotalVotes != 0 {
		t.Fatalf("pending results: %+v %v", pending, err)
	}
	s.Close()
	s = openTest(t, c)
	restarted, err := s.Get(context.Background(), p.ID)
	if err != nil || !restarted.StartsAt.Equal(p.StartsAt) || !restarted.EndsAt.Equal(p.EndsAt) {
		t.Fatalf("restart altered window: %+v %v", restarted, err)
	}
	recorded(t, s, p, tokenA, 2)
	finalizeTest(t, s, p)
	result, err := s.Results(context.Background(), p.ID)
	if err != nil || result.Pending || result.State != "final" || result.TotalVotes != 2 || result.Options[0].Votes != 1 || result.Options[1].Votes != 1 {
		t.Fatalf("canonical results: %+v %v", result, err)
	}
	finalPoll, err := s.Get(context.Background(), p.ID)
	if err != nil || finalPoll.FinalizedAt == nil || !finalPoll.FinalizedAt.Equal(result.CalculatedAt) {
		t.Fatalf("final publication metadata: %+v %v", finalPoll, err)
	}
	for _, token := range []string{tokenA, tokenB, "10000000000000000000000000000003"} {
		if r, err := s.Vote(context.Background(), p.ID, token, []int{1}); err != nil || r.Status != "closed" {
			t.Fatalf("late attempt: %+v %v", r, err)
		}
	}
	s.Close()
	s = openTest(t, c)
	again, err := s.Results(context.Background(), p.ID)
	if err != nil || !reflect.DeepEqual(result, again) {
		t.Fatalf("final restart changed result: %+v %+v %v", result, again, err)
	}
	if n, err := s.FinalizeDue(context.Background()); err != nil || n != 0 {
		t.Fatalf("finalized twice: %d %v", n, err)
	}
	if first.Choices[0] != 1 {
		t.Fatal("attempt receipt mutated")
	}
}

func TestMultipleChoiceAndFinalizationOnStartupAfterDeadline(t *testing.T) {
	c := configForTest(t)
	s := openTest(t, c)
	p := createEndingSoon(t, s, "multiple", []string{"A", "B", "C"})
	r := recorded(t, s, p, tokenA, 3, 1)
	if !reflect.DeepEqual(r.Choices, []int{1, 3}) {
		t.Fatalf("choices not normalized: %+v", r)
	}
	recorded(t, s, p, tokenA, 2)
	recorded(t, s, p, tokenB, 2)
	s.Close()
	time.Sleep(time.Until(p.EndsAt) + time.Millisecond)
	s = openTest(t, c)
	result, err := s.Results(context.Background(), p.ID)
	if err != nil || result.Pending || result.TotalVotes != 2 || len(result.Options) != 3 {
		t.Fatalf("startup result: %+v %v", result, err)
	}
	for _, option := range result.Options {
		if option.Votes != 1 {
			t.Fatalf("multiple counts: %+v", result)
		}
	}
}

func TestCreateValidationOverlapAndDefensiveCopies(t *testing.T) {
	s := openTest(t, configForTest(t))
	start := time.Now().Add(time.Hour)
	in := poll.CreateInput{Question: " Q ", Type: "ab", Options: []string{" A ", " B "}, StartsAt: &start}
	p, err := s.Create(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if in.Options[0] != " A " {
		t.Fatal("Create mutated caller options")
	}
	p.Options[0].Label = "changed"
	got, err := s.Get(context.Background(), p.ID)
	if err != nil || got.Options[0].Label != "A" || got.Question != "Q" || got.EndsAt.Sub(got.StartsAt) != time.Minute {
		t.Fatalf("immutable definition: %+v %v", got, err)
	}
	if r, err := s.Vote(context.Background(), p.ID, tokenA, []int{1}); err != nil || r.Status != "not_open" {
		t.Fatalf("scheduled admission: %+v %v", r, err)
	}
	if _, err := s.Create(context.Background(), in); !errors.Is(err, poll.ErrOverlap) {
		t.Fatalf("overlap admitted: %v", err)
	}
	adjacent := got.EndsAt
	in.StartsAt = &adjacent
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := s.Create(context.Background(), in); results <- err }()
	}
	wg.Wait()
	close(results)
	created := 0
	for err := range results {
		if err == nil {
			created++
		} else if !errors.Is(err, poll.ErrOverlap) {
			t.Fatal(err)
		}
	}
	if created != 1 {
		t.Fatalf("concurrent overlaps: %d created", created)
	}
	list, err := s.List(context.Background())
	if err != nil || len(list) != 2 {
		t.Fatalf("list: %v %v", list, err)
	}
	if _, err := s.Create(context.Background(), poll.CreateInput{Question: "q", Type: "ab", Options: []string{"same", " SAME "}}); err == nil {
		t.Fatal("invalid options created")
	}
	if _, err := s.Get(context.Background(), "../../foreign"); !errors.Is(err, poll.ErrNotFound) {
		t.Fatalf("invalid ID: %v", err)
	}
}

func TestHTTPTokenProtocolRecords32HexButRejectsUUIDToken(t *testing.T) {
	s := openTest(t, configForTest(t))
	p, err := s.Create(context.Background(), poll.CreateInput{Question: "q", Type: "ab", Options: []string{"A", "B"}})
	if err != nil {
		t.Fatal(err)
	}
	server, err := httpapi.New(s, s, fstest.MapFS{}, httpapi.Config{AdminPassword: "test-password-long-enough", PublicURL: "http://example.test", MaxInflight: 8, OperationTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	server.SetReady(true)
	for _, test := range []struct {
		token string
		code  int
	}{{tokenA, 202}, {"10000000-0000-0000-0000-000000000001", 422}, {strings.ToUpper("abcdef00000000000000000000000001"), 422}, {strings.Repeat("0", 32), 422}} {
		body, _ := json.Marshal(map[string]any{"token": test.token, "choices": []int{1}})
		request := httptest.NewRequest(http.MethodPost, "http://example.test/api/polls/"+p.ID+"/votes", strings.NewReader(string(body)))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Origin", "http://example.test")
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if response.Code != test.code {
			t.Fatalf("HTTP token %s: %d %s", test.token, response.Code, response.Body.String())
		}
		if test.code == 202 {
			var r poll.Receipt
			if err := json.Unmarshal(response.Body.Bytes(), &r); err != nil || r.Status != "recorded" || r.AcceptedAt == nil {
				t.Fatalf("HTTP durable receipt: %+v %v", r, err)
			}
		}
	}
	for _, choices := range [][]int{nil, {1, 1}, {0}, {3}, {1, 2}} {
		if r, err := s.Vote(context.Background(), p.ID, tokenA, choices); err != nil || r.Status != "invalid" {
			t.Fatalf("invalid choices: %+v %v", r, err)
		}
	}
}

func TestBoundedExactResultsRemainPendingUntilBudgetRaised(t *testing.T) {
	c := configForTest(t)
	c.MaxUnique = 1
	s := openTest(t, c)
	p := createEndingSoon(t, s, "ab", []string{"A", "B"})
	recorded(t, s, p, tokenA, 1)
	recorded(t, s, p, tokenB, 2)
	time.Sleep(time.Until(p.EndsAt) + time.Millisecond)
	if n, err := s.FinalizeDue(context.Background()); err == nil || n != 0 {
		t.Fatalf("limit ignored: %d %v", n, err)
	}
	e := s.lookup(p.ID)
	e.mu.Lock()
	firstError := e.finalError
	e.mu.Unlock()
	if firstError == nil {
		t.Fatal("failed calculation not latched")
	}
	if _, err := s.FinalizeDue(context.Background()); err == nil {
		t.Fatal("repeated failure hidden")
	}
	r, err := s.Results(context.Background(), p.ID)
	if err != nil || !r.Pending {
		t.Fatalf("partial result published: %+v %v", r, err)
	}
	if err := s.Ping(context.Background()); err == nil {
		t.Fatal("permanent finalization failure reported healthy")
	}
	if _, err := os.Stat(filepath.Join(e.directory, "result.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial result file: %v", err)
	}
	s.Close()
	c.MaxUnique = 2
	s = openTest(t, c)
	r, err = s.Results(context.Background(), p.ID)
	if err != nil || r.Pending || r.TotalVotes != 2 {
		t.Fatalf("larger budget could not finish saved attempts: %+v %v", r, err)
	}
}

func TestRepositoryRecoveryRepairsTailButRejectsCorruption(t *testing.T) {
	c := configForTest(t)
	s := openTest(t, c)
	p, err := s.Create(context.Background(), poll.CreateInput{Question: "q", Type: "ab", Options: []string{"A", "B"}})
	if err != nil {
		t.Fatal(err)
	}
	recorded(t, s, p, tokenA, 1)
	e := s.lookup(p.ID)
	s.Close()
	path := filepath.Join(e.directory, "journal", "votes.wal")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write([]byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	f.Close()
	s = openTest(t, c)
	recorded(t, s, p, tokenB, 2)
	s.Close()
	audit, err := filelog.Scan(context.Background(), e.logConfig(), nil)
	if err != nil || audit.IncompleteTail || audit.Votes != 2 {
		t.Fatalf("recovered prefix: %+v %v", audit, err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[80+40] ^= 1
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if recovered, err := New(context.Background(), c); err == nil {
			recovered.Close()
			t.Fatal("corrupt WAL reopened")
		}
	}
	after, err := os.ReadFile(path)
	if err != nil || string(b) != string(after) {
		t.Fatal("corrupted WAL was silently repaired")
	}
}

func TestExclusiveOwnershipAndForeignDataProtection(t *testing.T) {
	c := configForTest(t)
	s := openTest(t, c)
	if second, err := New(context.Background(), c); err == nil {
		second.Close()
		t.Fatal("two repository owners")
	}
	s.Close()
	s = openTest(t, c)
	if err := s.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if err := s.Ping(context.Background()); err == nil {
		t.Fatal("closed repository healthy")
	}
	foreign := configForTest(t)
	if err := os.MkdirAll(foreign.Directory, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(foreign.Directory, "important.txt")
	if err := os.WriteFile(path, []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	if bad, err := New(context.Background(), foreign); err == nil {
		bad.Close()
		t.Fatal("foreign directory adopted")
	}
	items, err := os.ReadDir(foreign.Directory)
	if err != nil || len(items) != 1 || items[0].Name() != "important.txt" {
		t.Fatal("foreign directory changed")
	}
	linked := configForTest(t)
	if err := os.Symlink(foreign.Directory, linked.Directory); err != nil {
		t.Fatal(err)
	}
	if bad, err := New(context.Background(), linked); err == nil {
		bad.Close()
		t.Fatal("symlink followed")
	}
}

func TestUnpublishedCreationOrphansDoNotBlockExistingPollRecovery(t *testing.T) {
	c := configForTest(t)
	s := openTest(t, c)
	p, err := s.Create(context.Background(), poll.CreateInput{Question: "live", Type: "ab", Options: []string{"A", "B"}})
	if err != nil {
		t.Fatal(err)
	}
	recorded(t, s, p, tokenA, 1)
	e := s.lookup(p.ID)
	s.Close()
	// Simulate crashes at two preparation boundaries: definition-only, and
	// fully initialized WAL before the publication rename. Both have no ACK.
	for _, complete := range []bool{false, true} {
		id, err := uuid()
		if err != nil {
			t.Fatal(err)
		}
		directory := filepath.Join(c.Directory, "polls", ".creating-"+id)
		if err := os.Mkdir(directory, 0700); err != nil {
			t.Fatal(err)
		}
		def := e.def
		def.Poll.ID = id
		if err := createJSON(filepath.Join(directory, "definition.json"), def); err != nil {
			t.Fatal(err)
		}
		if complete {
			prepared := &entry{directory: directory, def: def}
			w, err := filelog.New(context.Background(), prepared.logConfig())
			if err != nil {
				t.Fatal(err)
			}
			w.Close()
		}
	}
	s = openTest(t, c)
	list, err := s.List(context.Background())
	if err != nil || len(list) != 1 || list[0].ID != p.ID {
		t.Fatalf("orphan became public or blocked recovery: %+v %v", list, err)
	}
	recorded(t, s, p, tokenB, 2)
	s.Close()
	audit, err := filelog.Scan(context.Background(), e.logConfig(), nil)
	if err != nil || audit.Votes != 2 {
		t.Fatalf("existing ACK prefix damaged: %+v %v", audit, err)
	}
	items, err := os.ReadDir(filepath.Join(c.Directory, "polls"))
	if err != nil || len(items) != 3 {
		t.Fatal("recovery removed inspectable creation orphans")
	}
}

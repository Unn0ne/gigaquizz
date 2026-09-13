package filestore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"gigaquizz/internal/filelog"
	"gigaquizz/internal/poll"
)

func TestAtomicBootstrapSurvivesPrepublicationFailure(t *testing.T) {
	c := configForTest(t)
	fault := errors.New("interrupted before root publication")
	if err := bootstrapRoot(c.Directory, func() error { return fault }); !errors.Is(err, fault) {
		t.Fatal(err)
	}
	if _, err := os.Stat(c.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partially published root: %v", err)
	}
	s, err := New(context.Background(), c)
	if err != nil {
		t.Fatalf("retry wedged by interrupted bootstrap: %v", err)
	}
	s.Close()
	for _, name := range []string{".owner", "marker.json", "polls"} {
		if _, err := os.Stat(filepath.Join(c.Directory, name)); err != nil {
			t.Fatal(err)
		}
	}
	orphans, err := filepath.Glob(filepath.Join(filepath.Dir(c.Directory), ".gigaquizz-root-*"))
	if err != nil || len(orphans) != 1 {
		t.Fatalf("diagnostic staging lost: %v %v", orphans, err)
	}
}

func TestPublishedRootNeverRecreatesMissingHistory(t *testing.T) {
	c := configForTest(t)
	s := openTest(t, c)
	s.Close()
	if err := os.Remove(filepath.Join(c.Directory, "polls")); err != nil {
		t.Fatal(err)
	}
	if s, err := New(context.Background(), c); err == nil {
		s.Close()
		t.Fatal("missing published history adopted as empty")
	}
	if _, err := os.Stat(filepath.Join(c.Directory, "polls")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing history directory was silently recreated")
	}
}

func TestExplicitZeroLingerSurvivesPersistedDefinition(t *testing.T) {
	c := configForTest(t)
	c.Linger = 0
	s := openTest(t, c)
	start := time.Now().Add(time.Hour)
	p, err := s.Create(context.Background(), poll.CreateInput{Question: "q", Type: "ab", Options: []string{"A", "B"}, StartsAt: &start})
	if err != nil {
		t.Fatal(err)
	}
	if s.lookup(p.ID).def.Linger != 0 {
		t.Fatal("explicit zero replaced")
	}
	s.Close()
	c.Linger = time.Second
	s = openTest(t, c)
	if s.lookup(p.ID).logConfig().Linger != 0 {
		t.Fatal("restart changed persisted explicit zero")
	}
}

func TestBrokenHistoryIsolatedButScheduleStillReserved(t *testing.T) {
	c := configForTest(t)
	s := openTest(t, c)
	start := time.Now().Add(-2 * time.Minute)
	p, err := s.Create(context.Background(), poll.CreateInput{Question: "old", Type: "ab", Options: []string{"A", "B"}, StartsAt: &start})
	if err != nil {
		t.Fatal(err)
	}
	directory := s.lookup(p.ID).directory
	s.Close()
	if err := os.WriteFile(filepath.Join(directory, "journal", "votes.wal"), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	s, err = New(context.Background(), c)
	if err != nil {
		t.Fatalf("history blocked serving: %v", err)
	}
	defer s.Close()
	r, err := s.Results(context.Background(), p.ID)
	if err != nil || !r.Pending || r.State == "final" {
		t.Fatalf("unverified history exposed final: %+v %v", r, err)
	}
	if _, err := s.Create(context.Background(), poll.CreateInput{Question: "overlap", Type: "ab", Options: []string{"A", "B"}, StartsAt: &start}); !errors.Is(err, poll.ErrOverlap) {
		t.Fatalf("lost published schedule: %v", err)
	}
	if _, err := s.FinalizeDue(context.Background()); err == nil {
		t.Fatal("corrupt historical journal accepted")
	}
	if _, err := s.Get(context.Background(), p.ID); err == nil {
		t.Fatal("failed historical poll exposed successfully")
	}
	if _, err := s.Results(context.Background(), p.ID); err == nil {
		t.Fatal("failed historical result exposed successfully")
	}
	if err := s.Ping(context.Background()); err != nil {
		t.Fatalf("history poisoned current readiness: %v", err)
	}
	fresh, err := s.Create(context.Background(), poll.CreateInput{Question: "fresh", Type: "ab", Options: []string{"A", "B"}})
	if err != nil {
		t.Fatal(err)
	}
	recorded(t, s, fresh, tokenA, 1)
}

func routedTokens(e *entry, p, n int) []string {
	var out []string
	for i := 1; len(out) < n; i++ {
		token := fmt.Sprintf("%032x", i)
		key, _ := parseToken(token)
		if e.groupConfig().Partition(key) == int32(p) {
			out = append(out, token)
		}
	}
	return out
}

func TestGroupCheckpointRestartAndExactGlobalFinal(t *testing.T) {
	c := configForTest(t)
	c.Partitions = 4
	c.MaxPartitionUnique = 1
	s := openTest(t, c)
	p := createEndingSoon(t, s, "ab", []string{"A", "B"})
	e := s.lookup(p.ID)
	a := routedTokens(e, 0, 1)[0]
	b := routedTokens(e, 1, 2)
	recorded(t, s, p, a, 1)
	recorded(t, s, p, a, 2)
	recorded(t, s, p, b[0], 2)
	recorded(t, s, p, b[1], 1)
	time.Sleep(time.Until(p.EndsAt) + time.Millisecond)
	if n, err := s.FinalizeDue(context.Background()); n != 0 || err == nil {
		t.Fatalf("partition RAM bound ignored: %d %v", n, err)
	}
	path := filepath.Join(e.directory, "result-part-0000.json")
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal("completed partition progress was not persisted", err)
	}
	if _, err := os.Stat(filepath.Join(e.directory, "result.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("partial aggregate published")
	}
	s.Close()
	c.Partitions = 8
	c.MaxPartitionUnique = 2
	s = openTest(t, c)
	r, err := s.Results(context.Background(), p.ID)
	if err != nil || r.Pending || r.TotalVotes != 3 || r.Options[0].Votes != 2 || r.Options[1].Votes != 1 {
		t.Fatalf("wrong exact resumed result: %+v %v", r, err)
	}
	if s.lookup(p.ID).def.Partitions != 4 {
		t.Fatal("runtime topology replaced saved topology")
	}
	after, err := os.Stat(path)
	if err != nil || !before.ModTime().Equal(after.ModTime()) {
		t.Fatal("completed checkpoint was rewritten")
	}
	definition, err := ReadPollDefinition(context.Background(), e.directory)
	if err != nil || definition.ID != p.ID {
		t.Fatal("read-only definition failed", err)
	}
	var count int
	m, err := ReplayPoll(context.Background(), e.directory, func(pos filelog.Position, v filelog.Vote) error {
		count++
		if e.groupConfig().Partition(v.Token) != pos.Partition {
			return errors.New("wrong route")
		}
		return nil
	})
	if err != nil || len(m.Partitions) != 4 || count != 4 {
		t.Fatalf("full grouped audit: count=%d manifest=%+v err=%v", count, m, err)
	}
	s.Close()
	s = openTest(t, c)
	again, err := s.Results(context.Background(), p.ID)
	if err != nil || !reflect.DeepEqual(r, again) {
		t.Fatal("stored final changed after lazy validation", err)
	}
}

func TestHistoricalFinalRequiresWholeWALValidation(t *testing.T) {
	c := configForTest(t)
	c.Partitions = 2
	s := openTest(t, c)
	p := createEndingSoon(t, s, "ab", []string{"A", "B"})
	recorded(t, s, p, tokenA, 1)
	finalizeTest(t, s, p)
	e := s.lookup(p.ID)
	s.Close()
	path := filepath.Join(e.directory, "journal", "partition-0000", "votes.wal")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.Write([]byte{1})
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	s, err = New(context.Background(), c)
	if err != nil {
		t.Fatalf("historical scan still blocks startup: %v", err)
	}
	defer s.Close()
	if _, err := s.FinalizeDue(context.Background()); err == nil {
		t.Fatal("valid result hid bytes after CLOSED")
	}
	if _, err := s.Results(context.Background(), p.ID); err == nil {
		t.Fatal("corrupt final exposed")
	}
}

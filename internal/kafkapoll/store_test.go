package kafkapoll

import (
	"context"
	"testing"
	"time"

	"gigaquizz/internal/poll"
)

func TestParseFullIdentifiersAndOptionsBounds(t *testing.T) {
	a, err := parseID("10000000-0000-4000-8000-000000000001")
	if err != nil {
		t.Fatal(err)
	}
	b, err := parseID("10000000-0000-4000-8000-000000000002")
	if err != nil || a == b {
		t.Fatal("different complete IDs collapsed")
	}
	for _, id := range []string{"", "00000000-0000-0000-0000-000000000000", "10000000000040008000000000000001", "10000000-0000-4000-8000-00000000000x"} {
		if _, err := parseID(id); err == nil {
			t.Fatal("accepted malformed or zero token")
		}
	}
	for _, o := range []Options{{DatabaseURL: "x", Schema: "bad;schema", Brokers: []string{"127.0.0.1:19092"}}, {DatabaseURL: "x", Brokers: []string{"127.0.0.1:19092"}, MaxUnique: 120_000_001}, {DatabaseURL: "x", Brokers: []string{"127.0.0.1:19092"}, MaxPolls: 100_001}} {
		if o.defaults() == nil {
			t.Fatal("accepted configuration outside its inventory/memory bound")
		}
	}
}

func TestVoteNeverUsesMetadataDatabase(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	id := "10000000-0000-4000-8000-000000000001"
	token := "20000000000040008000000000000001"
	now := time.Now()
	// pool/owner/guard are nil: all definitive admission decisions use memory.
	s := &Store{ctx: ctx, polls: map[string]*entry{id: {poll: poll.Poll{ID: id, Type: "single", Options: []poll.Option{{ID: 1}, {ID: 2}}, StartsAt: now.Add(time.Minute), EndsAt: now.Add(2 * time.Minute)}}}}
	for _, tc := range []struct {
		choices []int
		status  string
	}{{[]int{1}, "not_open"}, {[]int{1, 2}, "invalid"}, {[]int{3}, "invalid"}} {
		r, err := s.Vote(ctx, id, token, tc.choices)
		if err != nil || r.Status != tc.status {
			t.Fatalf("got %q %v, want %s", r.Status, err, tc.status)
		}
	}
	s.polls[id].poll.StartsAt = now.Add(-time.Second)
	s.polls[id].poll.EndsAt = now.Add(time.Minute)
	if r, err := s.Vote(ctx, id, token, []int{1}); err != nil || r.Status != "busy" {
		t.Fatalf("unprepared journal: %+v %v", r, err)
	}
	s.polls[id].poll.EndsAt = now.Add(-time.Second)
	if r, err := s.Vote(ctx, id, token, []int{1}); err != nil || r.Status != "closed" {
		t.Fatalf("closed poll: %+v %v", r, err)
	}
	cancel()
	if _, err := s.Vote(context.Background(), id, token, []int{1}); err != ErrOwnership {
		t.Fatal("lost controller continued admission")
	}
}

func TestApplicationDeadlineLatchSurvivesWallClockRollback(t *testing.T) {
	id := "10000000-0000-4000-8000-000000000001"
	now := time.Now().UTC()
	ends := now
	e := &entry{poll: poll.Poll{ID: id, Type: "single", Options: []poll.Option{{ID: 1}, {ID: 2}}, StartsAt: ends.Add(-time.Minute), EndsAt: ends}}
	s := &Store{ctx: context.Background(), polls: map[string]*entry{id: e}, clock: func() time.Time { return now }}
	if r, err := s.Vote(context.Background(), id, "20000000000040008000000000000001", []int{1}); err != nil || r.Status != "closed" {
		t.Fatal("deadline did not close application", r, err)
	}
	now = ends.Add(-time.Second)
	// The writer is absent: reopening would incorrectly return busy, not closed.
	if r, err := s.Vote(context.Background(), id, "20000000000040008000000000000002", []int{2}); err != nil || r.Status != "closed" {
		t.Fatal("API reopened after observing closed", r, err)
	}
}

package filestore

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"gigaquizz/internal/filelog"
	"gigaquizz/internal/poll"
)

func TestSavedFinalSurvivesRestartWithClockBeforeDeadline(t *testing.T) {
	for _, partitions := range []int{1, 2} {
		t.Run(map[int]string{1: "single", 2: "group"}[partitions], func(t *testing.T) {
			c := configForTest(t)
			c.Partitions = partitions
			s := openTest(t, c)
			p := createEndingSoon(t, s, "ab", []string{"A", "B"})
			recorded(t, s, p, tokenA, 1)
			recorded(t, s, p, tokenA, 2)
			recorded(t, s, p, tokenB, 2)
			finalizeTest(t, s, p)
			want, err := s.Results(context.Background(), p.ID)
			if err != nil {
				t.Fatal(err)
			}
			resultPath := filepath.Join(s.lookup(p.ID).directory, "result.json")
			s.Close()
			for _, now := range []time.Time{p.StartsAt.Add(time.Second), p.StartsAt.Add(-time.Second)} {
				// Open a fresh repository owner through the complete startup path;
				// only its clock is supplied, never the files or recovery result.
				restarted, err := newWithClock(context.Background(), c, func() time.Time { return now })
				if err != nil {
					t.Fatalf("durable final rejected after clock rollback: %v", err)
				}
				t.Cleanup(restarted.Close)
				got, err := restarted.Results(context.Background(), p.ID)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("final changed after rollback: got=%+v want=%+v err=%v", got, want, err)
				}
				definition, err := restarted.Get(context.Background(), p.ID)
				if err != nil || definition.FinalizedAt == nil || !definition.FinalizedAt.Equal(want.CalculatedAt) {
					t.Fatalf("finalized poll fact disappeared: %+v %v", definition, err)
				}
				for _, token := range []string{tokenA, tokenB, "10000000000000000000000000000003"} {
					if r, err := restarted.Vote(context.Background(), p.ID, token, []int{1}); err != nil || r.Status != "closed" {
						t.Fatalf("saved CLOSED reopened after rollback: %+v %v", r, err)
					}
				}
				if err := restarted.Ping(context.Background()); err != nil {
					t.Fatalf("valid final blocks service readiness: %v", err)
				}
				if changed, err := restarted.FinalizeDue(context.Background()); err != nil || changed != 0 {
					t.Fatalf("already final poll recalculated: %d %v", changed, err)
				}
				restarted.Close()
			}
			// A CLOSED journal is necessary but cannot excuse a bad result.
			var disk diskResult
			if err := readJSON(resultPath, &disk); err != nil {
				t.Fatal(err)
			}
			disk.Results.TotalVotes++ // Retain the original checksum deliberately.
			bad, err := json.Marshal(disk)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(resultPath, bad, 0600); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if badOwner, err := newWithClock(context.Background(), c, func() time.Time { return p.StartsAt.Add(time.Second) }); err == nil {
					badOwner.Close()
					t.Fatal("rollback recovery trusted a tampered result")
				}
			}
			after, err := os.ReadFile(resultPath)
			if err != nil || string(after) != string(bad) {
				t.Fatal("failed recovery changed inspectable damaged result")
			}
		})
	}
}

func TestPrematureResultCannotManufactureClosedDuringRecovery(t *testing.T) {
	for _, partitions := range []int{1, 2} {
		t.Run(map[int]string{1: "single", 2: "group"}[partitions], func(t *testing.T) {
			c := configForTest(t)
			c.Partitions = partitions
			s := openTest(t, c)
			p, err := s.Create(context.Background(), poll.CreateInput{Question: "premature", Type: "ab", Options: []string{"A", "B"}})
			if err != nil {
				t.Fatal(err)
			}
			recorded(t, s, p, tokenA, 1)
			e := s.lookup(p.ID)
			s.Close()
			result := poll.Results{PollID: p.ID, State: "final", TotalVotes: 1, CalculatedAt: p.EndsAt, Options: []poll.OptionCount{{ID: 1, Label: "A", Votes: 1}, {ID: 2, Label: "B"}}}
			disk := diskResult{Version: 1, DefinitionHash: hashJSON(e.def), Results: result, Manifest: filelog.Manifest{}}
			for i := 0; i < partitions; i++ {
				disk.Manifest.Partitions = append(disk.Manifest.Partitions, filelog.PartitionEnd{Partition: int32(i), Offset: 1})
			}
			disk.Checksum = hashJSON(disk)
			if err := publishResult(e.directory, disk); err != nil {
				t.Fatal(err)
			}
			resultPath := filepath.Join(e.directory, "result.json")
			before, err := os.ReadFile(resultPath)
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if badOwner, err := newWithClock(context.Background(), c, func() time.Time { return p.StartsAt.Add(time.Second) }); err == nil {
					badOwner.Close()
					t.Fatal("premature result accepted without existing CLOSED")
				}
			}
			after, err := os.ReadFile(resultPath)
			if err != nil || string(after) != string(before) {
				t.Fatal("premature result was overwritten")
			}
			// A failed startup releases ownership and cannot seal the open WAL
			// to make its invented final appear valid on the following restart.
			var configs []filelog.Config
			if partitions == 1 {
				configs = append(configs, e.logConfig())
			} else {
				for i := 0; i < partitions; i++ {
					path := filepath.Join(e.directory, "journal", []string{"partition-0000", "partition-0001"}[i])
					var metadata struct {
						PollID [16]byte `json:"poll_id"`
					}
					b, err := os.ReadFile(filepath.Join(path, "poll.json"))
					if err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal(b, &metadata); err != nil {
						t.Fatal(err)
					}
					child := e.logConfig()
					child.Directory, child.PollID = path, metadata.PollID
					configs = append(configs, child)
				}
			}
			var attempts uint64
			for _, c := range configs {
				scan, err := filelog.Scan(context.Background(), c, nil)
				if err != nil || scan.Closed || scan.IncompleteTail {
					t.Fatalf("premature recovery changed the open journal: %+v %v", scan, err)
				}
				attempts += scan.Votes
			}
			if attempts != 1 {
				t.Fatalf("prior acknowledged attempt changed: %d", attempts)
			}
		})
	}
}

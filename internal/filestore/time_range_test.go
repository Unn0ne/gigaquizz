package filestore

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gigaquizz/internal/poll"
)

func TestCreateAndPersistedDefinitionRequireExactUnixNanoWindow(t *testing.T) {
	minimum := time.Unix(0, math.MinInt64).UTC()
	maximum := time.Unix(0, math.MaxInt64).UTC()
	for _, tc := range []struct {
		name  string
		start time.Time
		valid bool
	}{
		{"minimum", minimum, true},
		{"maximum-end", maximum.Add(-time.Minute), true},
		{"minimum-with-offset", minimum.In(time.FixedZone("offset", -4*60*60)), true},
		{"below-minimum", minimum.Add(-time.Nanosecond), false},
		{"end-above-maximum", maximum.Add(-time.Minute + time.Nanosecond), false},
		{"year-3000", time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := openTest(t, configForTest(t))
			p, err := s.Create(context.Background(), poll.CreateInput{Question: "window", Type: "ab", Options: []string{"A", "B"}, StartsAt: &tc.start})
			if (err == nil) != tc.valid {
				t.Fatalf("Create window validation: valid=%v err=%v", tc.valid, err)
			}
			items, err := os.ReadDir(filepath.Join(s.c.Directory, "polls"))
			if err != nil {
				t.Fatal(err)
			}
			if !tc.valid && (len(items) != 0 || len(s.entries) != 0) {
				t.Fatal("invalid schedule created a published poll or preparation files")
			}
			d := definition{Version: 1, Poll: poll.Poll{ID: "00000000-0000-0000-0000-000000000001", Question: "window", Type: "ab", Options: []poll.Option{{ID: 1, Label: "A"}, {ID: 2, Label: "B"}}, StartsAt: tc.start, EndsAt: tc.start.Add(time.Minute), CreatedAt: time.Now().UTC()}, MaxUnique: 100, BatchSize: 16, QueueVotes: 32}
			if err := validateDefinition(d, d.Poll.ID); (err == nil) != tc.valid {
				t.Fatalf("persisted definition validation: valid=%v err=%v", tc.valid, err)
			}
			if tc.valid {
				saved, err := ReadPollDefinition(context.Background(), s.lookup(p.ID).directory)
				if err != nil || !saved.StartsAt.Equal(tc.start) || !saved.EndsAt.Equal(tc.start.Add(time.Minute)) {
					t.Fatalf("valid boundary definition did not round trip: %+v %v", saved, err)
				}
			}
		})
	}
}

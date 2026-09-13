package votelog

import (
	"math"
	"testing"
	"time"
)

func TestJournalCalendarRangePreservesNanoseconds(t *testing.T) {
	minimum := time.Unix(0, math.MinInt64).UTC()
	maximum := time.Unix(0, math.MaxInt64).UTC()
	for _, tc := range []struct {
		name  string
		start time.Time
		valid bool
	}{
		{"first representable minute", minimum, true},
		{"last representable minute", maximum.Add(-time.Minute), true},
		{"one nanosecond before minimum", minimum.Add(-time.Nanosecond), false},
		{"end one nanosecond beyond maximum", maximum.Add(-time.Minute + time.Nanosecond), false},
		{"future calendar overflow", time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := codecConfig()
			c.StartsAt, c.EndsAt = tc.start, tc.start.Add(time.Minute)
			if err := c.validate(); (err == nil) != tc.valid {
				t.Fatalf("journal range accepted=%v, want %v", err == nil, tc.valid)
			}
			if !tc.valid {
				return
			}
			for _, admission := range []time.Time{c.StartsAt, c.EndsAt.Add(-time.Nanosecond)} {
				vote := Vote{Token: [16]byte{7}, Choice: 1, AdmittedAt: admission}
				_, got, err := decodeRecord(c, encodeRecord(c, recordVote, vote))
				if err != nil || !got.AdmittedAt.Equal(admission) {
					t.Fatal("accepted boundary did not roundtrip in legacy record", err)
				}
				frame, err := encodeFrame(c, []Vote{vote})
				if err != nil {
					t.Fatal(err)
				}
				visits := 0
				if err := decodeFrame(c, frame, func(_ uint32, got Vote) error {
					visits++
					if !got.AdmittedAt.Equal(admission) {
						t.Fatal("accepted boundary wrapped in compact frame")
					}
					return nil
				}); err != nil || visits != 1 {
					t.Fatal("accepted compact boundary failed", err)
				}
			}
		})
	}
}

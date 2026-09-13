package filelog

import (
	"context"
	"errors"
	"math"
	"os"
	"testing"
	"time"
)

func TestConfigRequiresExactUnixNanoWindow(t *testing.T) {
	minimum := time.Unix(0, math.MinInt64).UTC()
	maximum := time.Unix(0, math.MaxInt64).UTC()
	for _, tc := range []struct {
		name  string
		start time.Time
		valid bool
	}{
		{"minimum", minimum, true},
		{"maximum-end", maximum.Add(-time.Minute), true},
		{"below-minimum", minimum.Add(-time.Nanosecond), false},
		{"end-above-maximum", maximum.Add(-time.Minute + time.Nanosecond), false},
		{"above-maximum", maximum.Add(time.Nanosecond), false},
		{"year-3000", time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testConfig(t)
			c.StartsAt, c.EndsAt = tc.start, tc.start.Add(time.Minute)
			if err := c.validate(); (err == nil) != tc.valid {
				t.Fatalf("window validation: valid=%v err=%v", tc.valid, err)
			}
			if tc.valid {
				return
			}
			if _, err := New(context.Background(), c); !errors.Is(err, ErrInvalid) {
				t.Fatalf("invalid window reached journal creation: %v", err)
			}
			if _, err := os.Lstat(c.Directory); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid schedule created files: %v", err)
			}
			if _, err := Recover(context.Background(), c); !errors.Is(err, ErrInvalid) {
				t.Fatalf("invalid window reached recovery: %v", err)
			}
			if _, err := Scan(context.Background(), c, nil); !errors.Is(err, ErrInvalid) {
				t.Fatalf("invalid window reached scan: %v", err)
			}
		})
	}
}

func TestExactUnixNanoBoundaryVotesSurviveRealWALReplay(t *testing.T) {
	for _, start := range []time.Time{time.Unix(0, math.MinInt64).UTC(), time.Unix(0, math.MaxInt64).UTC().Add(-time.Minute)} {
		t.Run(start.Format(time.RFC3339Nano), func(t *testing.T) {
			c := testConfig(t)
			c.StartsAt, c.EndsAt = start, start.Add(time.Minute)
			s, clock := testStore(t, c)
			admissions := []time.Time{c.StartsAt, c.EndsAt.Add(-time.Nanosecond)}
			for i, admitted := range admissions {
				clock.Store(admitted.UnixNano())
				r, err := s.Submit(context.Background(), testInput(byte(i+1), 1))
				if err != nil || !r.AdmittedAt.Equal(admitted) {
					t.Fatalf("boundary ACK: %+v %v", r, err)
				}
			}
			sealTest(t, s, clock)
			var count int
			_, err := Replay(context.Background(), c, func(_ Position, v Vote) error {
				if count >= len(admissions) || !v.AdmittedAt.Equal(admissions[count]) {
					t.Fatalf("timestamp did not round trip: %v at entry %d", v.AdmittedAt, count)
				}
				count++
				return nil
			})
			if err != nil || count != len(admissions) {
				t.Fatalf("boundary replay: count=%d err=%v", count, err)
			}
			recovered, err := Recover(context.Background(), c)
			if err != nil {
				t.Fatal(err)
			}
			recovered.Close()
		})
	}
}

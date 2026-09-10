package poll

import (
	"testing"
	"time"
)

func TestChoicesAreCanonicalAndInputUnchanged(t *testing.T) {
	in := []int{3, 1, 2}
	got, err := NormalizeChoices(in)
	if err != nil || got[0] != 1 || got[2] != 3 || in[0] != 3 {
		t.Fatalf("normalization=%v input=%v err=%v", got, in, err)
	}
	for _, invalid := range [][]int{{}, {1, 1}, {0}, {21}} {
		if _, err := NormalizeChoices(invalid); err == nil {
			t.Errorf("accepted invalid choices %v", invalid)
		}
	}
}

func TestQuestionTypesAndLimits(t *testing.T) {
	for _, kind := range []string{"ab", "single", "multiple"} {
		in := CreateInput{Question: " Вопрос ", Type: kind, Options: []string{" A ", "B"}}
		if err := in.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	in := CreateInput{Question: "Вопрос", Type: "ab", Options: []string{"A", "B", "C"}}
	if in.Validate() == nil {
		t.Fatal("A/B accepted 3 options")
	}
	in = CreateInput{Question: "Вопрос", Type: "single", Options: []string{" A ", "a"}}
	if in.Validate() == nil {
		t.Fatal("duplicate labels accepted")
	}
}

func TestPollDeadlineBoundaries(t *testing.T) {
	start := time.Now()
	p := Poll{StartsAt: start, EndsAt: start.Add(time.Minute)}
	for _, tc := range []struct {
		at   time.Time
		want string
	}{{start.Add(-time.Nanosecond), "scheduled"}, {start, "open"}, {p.EndsAt.Add(-time.Nanosecond), "open"}, {p.EndsAt, "processing"}} {
		if got := p.State(tc.at); got != tc.want {
			t.Errorf("state %s want %s", got, tc.want)
		}
	}
	p.FinalizedAt = &start
	if p.State(p.EndsAt) != "final" {
		t.Fatal("final state missing")
	}
}

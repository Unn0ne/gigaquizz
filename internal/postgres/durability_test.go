package postgres

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDurabilityOptionsValidation(t *testing.T) {
	names := make([]string, 17)
	for i := range names {
		names[i] = fmt.Sprintf("standby_%d", i)
	}
	tests := []struct {
		name    string
		options DurabilityOptions
		valid   bool
	}{
		{"local_default", DurabilityOptions{}, true},
		{"single_standby", DurabilityOptions{1, []string{"standby_a"}}, true},
		{"quorum_subset", DurabilityOptions{2, []string{"standby_a", "standby_b", "standby_c"}}, true},
		{"maximum_quorum", DurabilityOptions{16, names[:16]}, true},
		{"identifier_boundary", DurabilityOptions{1, []string{strings.Repeat("a", 63)}}, true},
		{"negative_quorum", DurabilityOptions{-1, nil}, false},
		{"quorum_over_limit", DurabilityOptions{17, names}, false},
		{"names_over_limit", DurabilityOptions{1, names}, false},
		{"missing_names", DurabilityOptions{1, nil}, false},
		{"too_few_names", DurabilityOptions{2, []string{"standby_a"}}, false},
		{"local_with_names", DurabilityOptions{0, []string{"standby_a"}}, false},
		{"duplicate_name", DurabilityOptions{2, []string{"standby_a", "standby_a"}}, false},
		{"case_alias", DurabilityOptions{2, []string{"standby_a", "STANDBY_A"}}, false},
		{"wildcard", DurabilityOptions{1, []string{"*"}}, false},
		{"empty_name", DurabilityOptions{1, []string{""}}, false},
		{"leading_digit", DurabilityOptions{1, []string{"1standby"}}, false},
		{"overlong_identifier", DurabilityOptions{1, []string{strings.Repeat("a", 64)}}, false},
		{"quoted_identifier", DurabilityOptions{1, []string{"\"standby_a\""}}, false},
		{"embedded_quote", DurabilityOptions{1, []string{"standby\"a"}}, false},
		{"comma_in_name", DurabilityOptions{1, []string{"standby_a,standby_b"}}, false},
		{"whitespace_in_name", DurabilityOptions{1, []string{"standby a"}}, false},
		{"setting_injection", DurabilityOptions{1, []string{"standby_a) ANY 0 (*)"}}, false},
		// ANY and FIRST are tokens, not unquoted standby names, in PostgreSQL's
		// synchronous_standby_names grammar. The fixed policy emits no quotes.
		{"reserved_any", DurabilityOptions{1, []string{"any"}}, false},
		{"reserved_first", DurabilityOptions{1, []string{"first"}}, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.options.validate()
			if (err == nil) != test.valid {
				t.Fatalf("validate(%+v) = %v; want valid=%v", test.options, err, test.valid)
			}
		})
	}
}

func TestDurabilitySettingComparison(t *testing.T) {
	policy := DurabilityOptions{RequiredStandbys: 2, StandbyNames: []string{"standby_a", "standby_b", "standby_c"}}
	expected := compactSetting(policy.expectedSetting())
	for _, setting := range []string{
		"ANY 2 (standby_a,standby_b,standby_c)",
		" ANY\t2 ( standby_a, standby_b, standby_c )\n",
	} {
		if compactSetting(setting) != expected {
			t.Errorf("rejected equivalent policy formatting %q", setting)
		}
	}
	for _, setting := range []string{
		"",
		"ANY 1 (standby_a,standby_b,standby_c)",
		"FIRST 2 (standby_a,standby_b,standby_c)",
		"ANY 2 (*)",
		"ANY 2 (standby_a,standby_b,standby_d)",
		"ANY 2 (standby_a,standby_a,standby_c)",
		"ANY 2 (standby_a,standby_b,standby_c,standby_d)",
		"ANY 2 (\"standby_a\",standby_b,standby_c)",
	} {
		if compactSetting(setting) == expected {
			t.Errorf("accepted changed policy %q", setting)
		}
	}
}

func TestDurabilityStateEpochIsImmutable(t *testing.T) {
	initial := primaryIdentity{
		SystemID: "7654321098765432100",
		Timeline: 3,
		Started:  time.Date(2026, 9, 10, 12, 30, 0, 123000, time.UTC),
	}
	var state durabilityState
	if !state.check(initial) {
		t.Fatal("first primary identity was rejected")
	}
	equivalent := initial
	equivalent.Started = initial.Started.In(time.FixedZone("other_location", 3*60*60))
	if !state.check(equivalent) {
		t.Fatal("same start instant with another time zone was rejected")
	}
	for _, test := range []struct {
		name   string
		change func(primaryIdentity) primaryIdentity
	}{
		{"different_cluster", func(id primaryIdentity) primaryIdentity { id.SystemID = "7654321098765432101"; return id }},
		{"new_timeline", func(id primaryIdentity) primaryIdentity { id.Timeline++; return id }},
		{"older_timeline", func(id primaryIdentity) primaryIdentity { id.Timeline--; return id }},
		{"restart", func(id primaryIdentity) primaryIdentity { id.Started = id.Started.Add(time.Microsecond); return id }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if state.check(test.change(initial)) {
				t.Fatal("changed primary epoch was accepted")
			}
			if !state.check(initial) {
				t.Fatal("rejected epoch replaced the pinned primary identity")
			}
		})
	}
}

func TestDurabilityStateConcurrentInitialization(t *testing.T) {
	initial := primaryIdentity{SystemID: "cluster", Timeline: 1, Started: time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)}
	restarted := initial
	restarted.Started = initial.Started.Add(time.Second)
	identities := []primaryIdentity{initial, restarted}
	const perEpoch = 32
	type result struct {
		epoch    int
		accepted bool
	}
	var state durabilityState
	start := make(chan struct{})
	results := make(chan result, 2*perEpoch)
	var workers sync.WaitGroup
	for epoch, id := range identities {
		for range perEpoch {
			workers.Add(1)
			go func() {
				defer workers.Done()
				<-start
				results <- result{epoch, state.check(id)}
			}()
		}
	}
	close(start)
	workers.Wait()
	close(results)
	accepted := [2]int{}
	for result := range results {
		if result.accepted {
			accepted[result.epoch]++
		}
	}
	if accepted != [2]int{perEpoch, 0} && accepted != [2]int{0, perEpoch} {
		t.Fatalf("multiple concurrent connections did not pin exactly one epoch: accepted=%v", accepted)
	}
}

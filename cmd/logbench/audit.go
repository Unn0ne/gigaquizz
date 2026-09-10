package main

import (
	"context"
	"fmt"
	"time"

	"gigaquizz/internal/votelog"
)

type auditReport struct {
	ReadCommittedThroughClosed bool       `json:"read_committed_through_closed"`
	PerReceiptCorrect          bool       `json:"every_202_receipt_matches_partition_offset_token_choice_admission"`
	CanonicalCorrect           bool       `json:"canonical_matches_first_committed_offset_per_key"`
	Correct                    bool       `json:"correct"`
	ConfirmedReceipts          int        `json:"confirmed_receipts"`
	ConfirmedKeys              int        `json:"confirmed_unique_keys"`
	MissingReceipts            int        `json:"missing_confirmed_receipts"`
	WrongReceipts              int        `json:"mismatched_confirmed_receipts"`
	TotalJournalAttempts       uint64     `json:"committed_journal_attempts"`
	CanonicalKeys              int        `json:"canonical_unique_keys"`
	Duplicates                 uint64     `json:"duplicate_committed_attempts"`
	UnknownKeysPresent         int        `json:"keys_with_unknown_attempt_and_committed_record"`
	NoReceiptKeysPresent       int        `json:"keys_without_202_but_present_in_journal"`
	InvalidFixtureEntries      int        `json:"invalid_fixture_entries_or_logical_key_token_bindings"`
	UnaccountedRecords         int        `json:"records_not_explained_by_generated_keys_and_choices"`
	InvalidAdmissionTimes      int        `json:"records_outside_server_admission_window"`
	SplitKeys                  int        `json:"tokens_in_multiple_partitions"`
	CanonicalMismatches        int        `json:"canonical_mismatches"`
	ChoiceCounts               [32]uint64 `json:"canonical_choice_counts"`
	ClosedPartitions           int        `json:"closed_partitions"`
	WallSeconds                float64    `json:"wall_seconds"`
}

type expectedKey struct {
	logicalKey         uint32
	remaining          [2]uint32
	confirmed, unknown bool
}

func reconcile(h ledgerHeader, entries []entry, a votelog.AuditResult) auditReport {
	r := auditReport{ReadCommittedThroughClosed: true, TotalJournalAttempts: a.TotalAttempts, CanonicalKeys: len(a.Canonical), ChoiceCounts: a.ChoiceCounts, ClosedPartitions: len(a.Manifest.Partitions)}
	if h.Entries != len(entries) || h.Entries < 1 || h.Entries > maximumAttempts || h.Keys < 1 || h.Keys > h.Entries {
		r.InvalidFixtureEntries++
		return r
	}
	expected := make(map[[16]byte]expectedKey, h.Keys)
	// The token map binds token -> logical key. This bounded bitmap records
	// whether a new token has already claimed a logical key, giving the reverse
	// uniqueness check without another large map or a 16-byte-per-key array.
	seenKeys := make([]byte, (h.Keys+7)/8)
	boundKeys := 0
	for _, e := range entries {
		if uint64(e.Key) >= uint64(h.Keys) || e.Token == [16]byte{} || e.Choice != 1 && e.Choice != 2 || e.Outcome > skipCancelled {
			r.InvalidFixtureEntries++
			continue
		}
		key, exists := expected[e.Token]
		if !exists {
			mask := byte(1 << (e.Key % 8))
			if seenKeys[e.Key/8]&mask != 0 {
				r.InvalidFixtureEntries++
			} else {
				seenKeys[e.Key/8] |= mask
				boundKeys++
			}
			key.logicalKey = e.Key
		} else if key.logicalKey != e.Key {
			r.InvalidFixtureEntries++
		}
		// Only success or an unknown dispatched outcome can explain a commit.
		// Count attempts, not just a union of choices: unexplained extra copies
		// must fail reconciliation even if deduplication hides them in the total.
		if e.Outcome == recorded || e.Outcome == unknown {
			if e.DispatchNS > 0 {
				key.remaining[e.Choice-1]++
			} else {
				r.InvalidFixtureEntries++
			}
		}
		if e.Outcome == recorded {
			r.ConfirmedReceipts++
			key.confirmed = true
			v, ok := a.Records[votelog.Position{Partition: e.Partition, Offset: e.Offset}]
			if !ok {
				r.MissingReceipts++
			} else if v.Token != e.Token || v.Choice != e.Choice || v.AdmittedAt.UnixNano() != e.AdmittedNS {
				r.WrongReceipts++
			}
		}
		if e.Outcome == unknown {
			key.unknown = true
		}
		expected[e.Token] = key
	}
	if boundKeys != h.Keys || len(expected) != h.Keys {
		r.InvalidFixtureEntries++
	}
	first := make(map[[16]byte]votelog.Position, len(a.Canonical))
	for p, v := range a.Records {
		key, exists := expected[v.Token]
		if !exists || v.Choice != 1 && v.Choice != 2 || key.remaining[v.Choice-1] == 0 {
			r.UnaccountedRecords++
		} else {
			key.remaining[v.Choice-1]--
			expected[v.Token] = key
		}
		if v.AdmittedAt.Before(h.Config.StartsAt) || !v.AdmittedAt.Before(h.Config.EndsAt) {
			r.InvalidAdmissionTimes++
		}
		prev, ok := first[v.Token]
		if !ok {
			first[v.Token] = p
		} else if prev.Partition != p.Partition {
			r.SplitKeys++
		} else if p.Offset < prev.Offset {
			first[v.Token] = p
		}
	}
	var independentCounts [32]uint64
	for token, p := range first {
		v := a.Records[p]
		canonical, ok := a.Canonical[token]
		if !ok || canonical.Token != v.Token || canonical.Choice != v.Choice || !canonical.AdmittedAt.Equal(v.AdmittedAt) {
			r.CanonicalMismatches++
		}
		for bit := range 32 {
			if v.Choice&(uint32(1)<<bit) != 0 {
				independentCounts[bit]++
			}
		}
	}
	if len(first) != len(a.Canonical) || independentCounts != a.ChoiceCounts {
		r.CanonicalMismatches++
	}
	for token, key := range expected {
		if key.confirmed {
			r.ConfirmedKeys++
		}
		if _, exists := first[token]; exists {
			if key.unknown {
				r.UnknownKeysPresent++
			}
			if !key.confirmed {
				r.NoReceiptKeysPresent++
			}
		}
	}
	if a.TotalAttempts >= uint64(len(first)) {
		r.Duplicates = a.TotalAttempts - uint64(len(first))
	}
	r.PerReceiptCorrect = r.MissingReceipts == 0 && r.WrongReceipts == 0
	r.CanonicalCorrect = r.CanonicalMismatches == 0 && r.SplitKeys == 0
	r.Correct = r.PerReceiptCorrect && r.CanonicalCorrect && r.InvalidFixtureEntries == 0 && r.UnaccountedRecords == 0 && r.InvalidAdmissionTimes == 0 && len(a.Records) == int(a.TotalAttempts) && len(a.Manifest.Partitions) == h.Config.Partitions
	return r
}

func runAudit(ctx context.Context, h ledgerHeader, entries []entry) (auditReport, error) {
	start := time.Now()
	a, err := votelog.Audit(ctx, h.Config)
	if err != nil {
		return auditReport{}, fmt.Errorf("read-committed journal audit failed: %w", err)
	}
	r := reconcile(h, entries, a)
	r.WallSeconds = time.Since(start).Seconds()
	if !r.Correct {
		return r, fmt.Errorf("journal reconciliation mismatch; inspect aggregate counters")
	}
	return r, nil
}

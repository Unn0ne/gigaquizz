package main

import (
	"testing"
	"unsafe"

	"gigaquizz/internal/votelog"
)

func TestReconcileRejectsInvalidLogicalKeyTokenBindings(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*ledgerHeader, []entry, *votelog.AuditResult)
	}{
		{"one token used for two logical keys", func(h *ledgerHeader, es []entry, _ *votelog.AuditResult) {
			h.Keys = 2
			es[1].Key = 1
		}},
		{"two tokens used for one logical key", func(_ *ledgerHeader, es []entry, a *votelog.AuditResult) {
			old := es[0].Token
			es[1].Token[0]++
			first := a.Records[votelog.Position{Partition: 0, Offset: 4}]
			first.Token = es[1].Token
			a.Records[votelog.Position{Partition: 0, Offset: 4}] = first
			a.Canonical[es[1].Token] = first
			a.Canonical[old] = a.Records[votelog.Position{Partition: 0, Offset: 5}]
			a.ChoiceCounts = [32]uint64{1, 1}
		}},
		{"declared logical key missing", func(h *ledgerHeader, _ []entry, _ *votelog.AuditResult) {
			h.Keys = 2
		}},
		{"key outside declared range", func(h *ledgerHeader, es []entry, _ *votelog.AuditResult) {
			es[0].Key = uint32(h.Keys)
		}},
		{"incorrect entry count", func(h *ledgerHeader, _ []entry, _ *votelog.AuditResult) {
			h.Entries++
		}},
		{"unbounded declared keys", func(h *ledgerHeader, _ []entry, _ *votelog.AuditResult) {
			h.Keys = maximumAttempts + 1
		}},
		{"negative declared keys", func(h *ledgerHeader, _ []entry, _ *votelog.AuditResult) {
			h.Keys = -1
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, es, a := fixture()
			tc.edit(&h, es, &a)
			r := reconcile(h, es, a)
			if r.Correct || r.InvalidFixtureEntries == 0 {
				t.Fatalf("corrupt logical-key fixture passed reconciliation: %+v", r)
			}
		})
	}
}

func TestReconcileRejectsCommitExplainedOnlyByDefinitiveRejection(t *testing.T) {
	for _, status := range []outcome{closed, busy, notOpen, rejected} {
		t.Run(status.String(), func(t *testing.T) {
			h, es, a := fixture()
			// This choice-2 record originally came from an unknown outcome.
			// A definitive rejection cannot authorize that persisted choice.
			es[1].Outcome = status
			r := reconcile(h, es, a)
			if r.Correct || r.UnaccountedRecords != 1 {
				t.Fatalf("definitively rejected attempt explained a commit: %+v", r)
			}
		})
	}
}

func TestReconcileBoundsCopiesByDispatchedRecordedAndUnknownAttempts(t *testing.T) {
	h, es, a := fixture()
	position := votelog.Position{Partition: 0, Offset: 6}
	a.Records[position] = a.Records[votelog.Position{Partition: 0, Offset: 5}]
	a.TotalAttempts++
	a.Manifest.Partitions[0].Offset++
	if r := reconcile(h, es, a); r.Correct || r.UnaccountedRecords != 1 || !r.PerReceiptCorrect || !r.CanonicalCorrect {
		t.Fatalf("extra copy hidden by otherwise correct deduplication was accepted: %+v", r)
	}
	// An additional actually dispatched unknown attempt with that same choice
	// permits the additional record. Unknown does not promise absence.
	extra := es[0]
	extra.Outcome = unknown
	extra.Offset = -1
	extra.Partition = -1
	extra.AdmittedNS = 0
	es = append(es, extra)
	h.Entries++
	if r := reconcile(h, es, a); !r.Correct || r.UnaccountedRecords != 0 {
		t.Fatalf("real unknown attempt could not explain its committed copy: %+v", r)
	}
	es[2].DispatchNS = 0
	if r := reconcile(h, es, a); r.Correct || r.InvalidFixtureEntries == 0 || r.UnaccountedRecords != 1 {
		t.Fatalf("undispatched unknown attempt explained a commit: %+v", r)
	}
}

func TestReconcileAllowsUnknownOutcomeWithOrWithoutCommit(t *testing.T) {
	h, es, a := fixture()
	if r := reconcile(h, es, a); !r.Correct || r.UnknownKeysPresent != 1 {
		t.Fatalf("normal committed unknown failed: %+v", r)
	}
	delete(a.Records, votelog.Position{Partition: 0, Offset: 4})
	a.TotalAttempts--
	a.Canonical[es[0].Token] = a.Records[votelog.Position{Partition: 0, Offset: 5}]
	a.ChoiceCounts = [32]uint64{1}
	if r := reconcile(h, es, a); !r.Correct || r.UnaccountedRecords != 0 {
		t.Fatalf("unknown outcome was incorrectly required to commit: %+v", r)
	}
}

func TestExpectedKeyStorageRemainsBounded(t *testing.T) {
	// At the maximum audit size, adding an accidental pointer or wider field
	// to every map value would materially change the memory budget.
	if size := unsafe.Sizeof(expectedKey{}); size > 16 {
		t.Fatalf("per-key reconciliation state exceeds its 16-byte memory budget: %d", size)
	}
}

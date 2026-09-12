package main

import (
	"context"
	"crypto/aes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gigaquizz/internal/poll"
)

func fixture(t *testing.T) (privateManifest, []attempt, []journalVote, poll.Results) {
	t.Helper()
	c := config{URL: "http://127.0.0.1:1", PollID: "00000000-0000-4000-8000-000000000001", Rate: 1, Duration: time.Minute, Start: time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC), Workers: 2, Queue: 4, MaxLag: 100 * time.Millisecond, Timeout: time.Second, RepeatEvery: 2, Directory: t.TempDir()}
	p := poll.Poll{ID: c.PollID, Type: "single", Options: []poll.Option{{ID: 1, Label: "First"}, {ID: 2, Label: "Second"}}, StartsAt: c.Start, EndsAt: c.Start.Add(time.Minute)}
	m := privateManifest{Version: 1, Complete: true, Config: c, Poll: p, Seed: [16]byte{1, 2, 3}, Namespace: [8]byte{9, 8, 7}}
	block, _ := aes.NewCipher(m.Seed[:])
	records := make([]attempt, c.attempts())
	for key := uint64(0); key < c.keys(); key++ {
		for _, choice := range []uint32{1, 2} {
			seq, err := sequence(c, key, choice)
			if err != nil {
				continue
			}
			records[seq] = attempt{Seq: seq, Key: key, Token: tokenFor(block, m.Namespace, key), Choice: choice, State: stateSkipped, ScheduledNS: int64(key) * int64(time.Second)}
		}
	}
	// A changed-choice repeat can reach the server before its original. The
	// journal determines canonical order, never the planned sequence number.
	records[0].State = stateACK
	records[0].HTTP = 202
	records[0].AdmittedNS = c.Start.Add(time.Second).UnixNano()
	records[1].State = stateUnknown
	records[1].HTTP = 503
	repeat, _ := sequence(c, 1, 2)
	records[repeat].State = stateACK
	records[repeat].HTTP = 202
	records[repeat].AdmittedNS = c.Start.Add(2 * time.Second).UnixNano()
	votes := []journalVote{{records[0].Token, 1, time.Unix(0, records[0].AdmittedNS)}, {records[repeat].Token, 2, time.Unix(0, records[repeat].AdmittedNS)}, {records[1].Token, 1, c.Start.Add(3 * time.Second)}}
	res := poll.Results{PollID: c.PollID, State: "final", TotalVotes: 2, Options: []poll.OptionCount{{ID: 1, Label: "First", Votes: 1}, {ID: 2, Label: "Second", Votes: 1}}}
	return m, records, votes, res
}

func writeFixture(t *testing.T, m privateManifest, records []attempt) string {
	t.Helper()
	m.Files = nil
	for i := 0; i <= m.Config.Workers; i++ {
		w, err := newLedger(m.Config.Directory, i)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			for _, a := range records {
				w.append(a)
			}
		}
		meta, err := w.close()
		if err != nil {
			t.Fatal(err)
		}
		m.Files = append(m.Files, meta)
	}
	if err := writeManifest(m.Config.Directory, m); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(m.Config.Directory, "manifest.json")
}
func replayFixture(votes []journalVote, tailErr error) journalReplay {
	return func(ctx context.Context, visit func(journalVote) error) error {
		for _, v := range votes {
			if err := visit(v); err != nil {
				return err
			}
		}
		return tailErr
	}
}

func TestExactHTTPACKAndUnknownReconciliation(t *testing.T) {
	m, records, votes, res := fixture(t)
	path := writeFixture(t, m, records)
	m, s, err := loadLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	r, err := reconcile(context.Background(), m, s, res, replayFixture(votes, nil))
	if err != nil || !r.Correct || r.Matched != 2 || r.Resolved != 1 || r.Canonical != 2 || r.Duplicates != 1 || r.Choices != [2]uint64{1, 1} {
		t.Fatalf("reconcile = %+v, %v", r, err)
	}
}

func TestAuditRejectsBrokenReceiptRelationships(t *testing.T) {
	tests := []struct {
		name, want string
		mutate     func(*privateManifest, []attempt, *[]journalVote, *poll.Results)
	}{
		{"definitive_rejection_committed", "rejected_or_skipped_attempt_committed", func(_ *privateManifest, a []attempt, _ *[]journalVote, _ *poll.Results) { a[1].State = stateBusy }},
		{"extra_physical_copy", "extra_physical_copy", func(_ *privateManifest, _ []attempt, v *[]journalVote, _ *poll.Results) { *v = append(*v, (*v)[2]) }},
		{"different_full_ID", "unexpected_full_token", func(_ *privateManifest, _ []attempt, v *[]journalVote, _ *poll.Results) { (*v)[0].Token[0] ^= 0xff }},
		{"admission_time_changed", "ack_admission_mismatch", func(_ *privateManifest, _ []attempt, v *[]journalVote, _ *poll.Results) {
			(*v)[0].AdmittedAt = (*v)[0].AdmittedAt.Add(time.Nanosecond)
		}},
		{"ack_missing", "ack_missing_from_closed_journal", func(_ *privateManifest, _ []attempt, v *[]journalVote, _ *poll.Results) { *v = (*v)[1:] }},
		{"wrong_first_choice_results", "service_option_results_mismatch", func(_ *privateManifest, _ []attempt, _ *[]journalVote, r *poll.Results) {
			r.Options[0].Votes = 2
			r.Options[1].Votes = 0
		}},
		{"pending_results", "service_results_mismatch", func(_ *privateManifest, _ []attempt, _ *[]journalVote, r *poll.Results) { r.Pending = true }},
		{"malformed_success_cannot_pass", "invalid_successful_http_responses", func(_ *privateManifest, a []attempt, _ *[]journalVote, _ *poll.Results) {
			a[1].Flags = flagInvalidPositive
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, a, v, res := fixture(t)
			tt.mutate(&m, a, &v, &res)
			path := writeFixture(t, m, a)
			m, s, err := loadLedger(path)
			if err != nil {
				t.Fatal(err)
			}
			r, err := reconcile(context.Background(), m, s, res, replayFixture(v, nil))
			if err == nil || r.Correct || r.Failure != tt.want {
				t.Fatalf("got %+v, %v; want %s", r, err, tt.want)
			}
		})
	}
}

func TestIncompleteJournalDoesNotDeclareMissingACKLoss(t *testing.T) {
	m, a, v, res := fixture(t)
	path := writeFixture(t, m, a)
	m, s, err := loadLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	r, err := reconcile(context.Background(), m, s, res, replayFixture(v[:1], errors.New("missing CLOSED")))
	if err == nil || r.JournalClosed || r.Correct || r.Missing != 0 || r.Unverified != 1 {
		t.Fatalf("incorrect incomplete-journal report: %+v, %v", r, err)
	}
}

func TestLedgerRejectsMappingCoverageAndCorruption(t *testing.T) {
	for _, kind := range []string{"key", "token", "duplicate_sequence", "short", "crc", "sha", "incomplete_manifest", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			m, a, _, _ := fixture(t)
			switch kind {
			case "key":
				a[0].Key = 2
			case "token":
				a[0].Token[15] ^= 1
			case "duplicate_sequence":
				a[2] = a[0]
			case "short":
				a = a[:len(a)-1]
			case "incomplete_manifest":
				m.Complete = false
			}
			path := writeFixture(t, m, a)
			ledger := filepath.Join(m.Config.Directory, "worker-0000.bin")
			switch kind {
			case "crc", "sha":
				b, err := os.ReadFile(ledger)
				if err != nil {
					t.Fatal(err)
				}
				b[48] ^= 1
				if kind == "sha" {
					var raw [ledgerBytes]byte
					copy(raw[:], b[:ledgerBytes])
					valid, _ := decodeAttempt(b[ledgerBytes : 2*ledgerBytes])
					valid.Seq = 0
					encodeAttempt(valid, &raw)
					copy(b, raw[:])
				}
				if err = os.WriteFile(ledger, b, 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Rename(ledger, ledger+"-private"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(ledger+"-private", ledger); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := loadLedger(path); err == nil {
				t.Fatalf("accepted %s", kind)
			}
		})
	}
}

func TestFullPermutationChecksAll128Bits(t *testing.T) {
	b, _ := aes.NewCipher(make([]byte, 16))
	namespace := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	token := tokenFor(b, namespace, 12)
	if got, err := keyFor(b, namespace, token, 13); err != nil || got != 12 {
		t.Fatalf("decode: %d %v", got, err)
	}
	for i := 0; i < 16; i++ {
		bad := token
		bad[i] ^= 1
		if _, err := keyFor(b, namespace, bad, 13); err == nil {
			t.Fatalf("accepted changed byte %d", i)
		}
	}
	if _, err := keyFor(b, namespace, token, 12); err == nil {
		t.Fatal("accepted out-of-bound key")
	}
}

func TestSHARejectsValidCRCEdit(t *testing.T) {
	m, a, _, _ := fixture(t)
	path := writeFixture(t, m, a)
	ledger := filepath.Join(m.Config.Directory, "worker-0000.bin")
	data, err := os.ReadFile(ledger)
	if err != nil {
		t.Fatal(err)
	}
	var raw [ledgerBytes]byte
	changed := a[2]
	changed.State = stateUnknown
	encodeAttempt(changed, &raw)
	copy(data[2*ledgerBytes:3*ledgerBytes], raw[:])
	if err = os.WriteFile(ledger, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err = loadLedger(path); err == nil {
		t.Fatal("accepted valid-CRC edit without matching SHA")
	}
	// SHA is integrity of the artifact, not a secret authentication mechanism.
	var updated privateManifest
	if err = readPrivateJSON(path, &updated, 2<<20); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	updated.Files[0].SHA256 = hex.EncodeToString(sum[:])
	if err = writeManifest(m.Config.Directory, updated); err != nil {
		t.Fatal(err)
	}
	if _, _, err = loadLedger(path); err != nil {
		t.Fatal(err)
	}
}

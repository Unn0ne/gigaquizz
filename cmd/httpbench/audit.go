package main

import (
	"bufio"
	"context"
	"crypto/aes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"gigaquizz/internal/poll"
)

type journalVote struct {
	Token      [16]byte
	Choice     uint32
	AdmittedAt time.Time
}
type journalReplay func(context.Context, func(journalVote) error) error
type journalInspection struct {
	Method                 string `json:"method"`
	StrictSnapshotComplete bool   `json:"strict_snapshot_complete"`
	RecoveryBootRecords    uint64 `json:"recovery_boot_records"`
	SnapshotPartitions     int    `json:"snapshot_partitions"`
}
type journalReader struct {
	Replay     journalReplay
	Inspection *journalInspection
}
type auditStates struct {
	Status   []byte
	Admitted []int64
}
type auditInput struct {
	Manifest privateManifest
	States   auditStates
}
type auditReport struct {
	JournalInspection *journalInspection `json:"journal_inspection,omitempty"`
	Mode              string             `json:"mode"`
	Correct           bool               `json:"correct"`
	Generators        int                `json:"generators"`
	PlannedKeys       uint64             `json:"planned_unique_keys"`
	ACKCoverage       float64            `json:"acknowledged_attempt_fraction"`
	LedgerValid       bool               `json:"ledger_valid"`
	JournalClosed     bool               `json:"journal_closed"`
	ResultsMatched    bool               `json:"saved_service_results_matched"`
	Failure           string             `json:"failure,omitempty"`
	Planned           uint64             `json:"planned_attempts"`
	Confirmed         uint64             `json:"confirmed_attempts"`
	Matched           uint64             `json:"matched_ack_attempts"`
	Missing           uint64             `json:"missing_ack_attempts"`
	Unverified        uint64             `json:"unverified_ack_attempts"`
	Unknown           uint64             `json:"unknown_client_attempts"`
	JourneyFailed     uint64             `json:"journey_failed_client_attempts"`
	Resolved          uint64             `json:"unknown_attempts_found_committed"`
	Recorded          uint64             `json:"journal_attempts"`
	Canonical         uint64             `json:"canonical_unique_keys"`
	Duplicates        uint64             `json:"duplicate_logical_attempts"`
	Unexpected        uint64             `json:"unexpected_records"`
	Copies            uint64             `json:"unexpected_physical_copies"`
	Mismatch          uint64             `json:"receipt_mismatches"`
	Choices           [2]uint64          `json:"canonical_choice_counts"`
	InvalidPositive   uint64             `json:"invalid_successful_responses"`
	StateBytes        uint64             `json:"compact_state_bytes"`
	WallSeconds       float64            `json:"wall_seconds"`
	Method            string             `json:"method"`
}

func loadLedger(path string) (privateManifest, auditStates, error) {
	return loadLedgerContext(context.Background(), path)
}
func loadLedgerContext(ctx context.Context, path string) (privateManifest, auditStates, error) {
	var m privateManifest
	var s auditStates
	if err := readPrivateJSON(path, &m, 2<<20); err != nil {
		return m, s, err
	}
	if m.Version != 1 || !m.Complete || m.Config.validate() != nil || validatePoll(m.Config, m.Poll) != nil || len(m.Files) != m.Config.Workers+1 {
		return m, s, errors.New("invalid or incomplete private manifest")
	}
	block, _ := aes.NewCipher(m.Seed[:])
	s.Status = make([]byte, m.Config.attempts())
	s.Admitted = make([]int64, m.Config.attempts())
	var total uint64
	for i, meta := range m.Files {
		if meta.Name != fmt.Sprintf("worker-%04d.bin", i) || meta.Records > m.Config.attempts() || meta.Records > m.Config.attempts()-total {
			return m, s, errors.New("ledger inventory bound mismatch")
		}
		wantHash, err := hex.DecodeString(meta.SHA256)
		if err != nil || len(wantHash) != sha256.Size {
			return m, s, errors.New("invalid ledger hash")
		}
		f, err := os.OpenFile(filepath.Join(filepath.Dir(path), meta.Name), os.O_RDONLY|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return m, s, err
		}
		err = func() error {
			defer f.Close()
			i, err := f.Stat()
			if err != nil {
				return err
			}
			if !i.Mode().IsRegular() || i.Mode().Perm()&0077 != 0 || i.Size() != int64(meta.Records*ledgerBytes) {
				return errors.New("ledger size or private permissions mismatch")
			}
			h := sha256.New()
			reader := bufio.NewReaderSize(io.TeeReader(f, h), 256*1024)
			var raw [ledgerBytes]byte
			for n := uint64(0); n < meta.Records; n++ {
				if n%4096 == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				if _, err = io.ReadFull(reader, raw[:]); err != nil {
					return err
				}
				a, err := decodeAttempt(raw[:])
				if err != nil {
					return err
				}
				seq, err := sequence(m.Config, a.Key, a.Choice)
				if err != nil || seq != a.Seq || a.Seq >= uint64(len(s.Status)) || s.Status[a.Seq] != 0 || a.Token != tokenFor(block, m.Namespace, m.Config.KeyOffset+a.Key) || a.Token == [16]byte{} || a.ScheduledNS != int64(m.Config.scheduledOffset(a.Key)) {
					return errors.New("ledger sequence, full token, key or schedule mismatch")
				}
				if a.State == stateACK && (a.HTTP != 202 || a.AdmittedNS < m.Poll.StartsAt.UnixNano() || a.AdmittedNS >= m.Poll.EndsAt.UnixNano() || a.Flags != 0) {
					return errors.New("ledger ACK protocol mismatch")
				}
				if a.State == stateSkipped && (a.HTTP != 0 || a.Flags != 0) {
					return errors.New("skipped ledger attempt has HTTP response")
				}
				if a.State == stateJourneyFailed && (!m.Config.Journey || a.Choice != 1 || a.HTTP != 0 || a.Flags != 0) {
					return errors.New("journey failure must be an original without POST response")
				}
				s.Status[a.Seq] = a.State
				if a.Flags&flagInvalidPositive != 0 {
					s.Status[a.Seq] |= 0x40
				}
				s.Admitted[a.Seq] = a.AdmittedNS
			}
			if _, err := reader.ReadByte(); err != io.EOF {
				return errors.New("extra ledger bytes")
			}
			if hex.EncodeToString(h.Sum(nil)) != meta.SHA256 {
				return errors.New("ledger SHA256 mismatch")
			}
			return nil
		}()
		if err != nil {
			return m, s, err
		}
		total += meta.Records
	}
	if total != m.Config.attempts() {
		return m, s, errors.New("ledger does not cover every planned attempt")
	}
	return m, s, nil
}

func savedResults(path string) (poll.Results, error) {
	var raw json.RawMessage
	var result poll.Results
	if err := readPrivateJSON(path, &raw, 1<<20); err != nil {
		return result, err
	}
	var envelope struct {
		Results json.RawMessage `json:"results"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return result, err
	}
	if len(envelope.Results) > 0 {
		raw = envelope.Results
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return result, err
	}
	return result, nil
}

func runAudit(ctx context.Context, path, fileJournal, kafkaConfig, resultsPath string) (auditReport, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	start := time.Now()
	r := auditReport{Mode: "independent-http-audit"}
	m, s, err := loadLedgerContext(ctx, path)
	if err != nil {
		r.Failure = "invalid_private_ledger"
		return r, err
	}
	r.LedgerValid = true
	res, err := savedResults(resultsPath)
	if err != nil {
		r.Failure = "invalid_saved_results"
		return r, err
	}
	reader, err := backend(m, fileJournal, kafkaConfig)
	if err != nil {
		r.Failure = "invalid_journal_configuration"
		return r, err
	}
	r, err = reconcile(ctx, m, s, res, reader.Replay)
	r.JournalInspection = reader.Inspection
	if reader.Inspection != nil {
		r.Method += " Kafka inspection additionally scans the entire captured stable raw-offset snapshot beyond CLOSED, including transaction markers and aborted data; only valid recovery BOOT controls may follow CLOSED. Any open transaction, changing snapshot or unread tail prevents a successful audit. This certifies the inspected snapshot, not future writes."
	}
	r.WallSeconds = time.Since(start).Seconds()
	return r, err
}

func reconcile(ctx context.Context, m privateManifest, s auditStates, res poll.Results, replay journalReplay) (auditReport, error) {
	return reconcileMany(ctx, []auditInput{{Manifest: m, States: s}}, res, replay)
}

func reconcileMany(ctx context.Context, inputs []auditInput, res poll.Results, replay journalReplay) (auditReport, error) {
	r := auditReport{Mode: "independent-http-audit", LedgerValid: true, Generators: len(inputs), Method: "Separate process reads every CRC/SHA256-checked full-ID client record and the journal through CLOSED. AES inversion validates all 128 token bits and disjoint generator ranges; dense flags are exact, not a probabilistic hash. Each ACK matches token+choice+admitted_at with multiplicity one. Only unknown attempts can explain additional records. Canonical first journal occurrence is compared to saved final service counts. Complete range coverage is required by distributed-plan audits; correct does not mean every planned attempt was admitted."}
	fail := func(reason string) error { r.Failure = reason; return errors.New(reason) }
	if len(inputs) < 1 || len(inputs) > 128 {
		return r, fail("invalid_generator_inventory")
	}
	inputs = append([]auditInput(nil), inputs...)
	sort.Slice(inputs, func(i, j int) bool { return inputs[i].Manifest.Config.KeyOffset < inputs[j].Manifest.Config.KeyOffset })
	m := inputs[0].Manifest
	var end uint64
	for i, input := range inputs {
		c, s := input.Manifest.Config, input.States
		if c.validate() != nil || uint64(len(s.Status)) != c.attempts() || len(s.Status) != len(s.Admitted) {
			return r, fail("invalid_state_bounds")
		}
		if input.Manifest.Seed != m.Seed || input.Manifest.Namespace != m.Namespace || !samePoll(input.Manifest.Poll, m.Poll) || (i > 0 && c.KeyOffset < end) {
			return r, fail("incompatible_or_overlapping_generator_ranges")
		}
		end = c.KeyOffset + c.keys()
		r.Planned += c.attempts()
		r.PlannedKeys += c.keys()
		if r.Planned > maxPlanAttempts || r.PlannedKeys > maxPlanKeys {
			return r, fail("distributed_audit_capacity_exceeded")
		}
		for _, state := range s.Status {
			switch state & 0x3f {
			case stateACK:
				r.Confirmed++
			case stateUnknown:
				r.Unknown++
			case stateJourneyFailed:
				r.JourneyFailed++
			}
			if state&0x40 != 0 {
				r.InvalidPositive++
			}
		}
	}
	r.StateBytes = 9*r.Planned + r.PlannedKeys
	r.ACKCoverage = float64(r.Confirmed) / float64(r.Planned)
	block, _ := aes.NewCipher(m.Seed[:])
	seen := make([][]byte, len(inputs))
	for i, input := range inputs {
		seen[i] = make([]byte, input.Manifest.Config.keys())
	}
	err := replay(ctx, func(v journalVote) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if v.AdmittedAt.Before(m.Poll.StartsAt) || !v.AdmittedAt.Before(m.Poll.EndsAt) {
			r.Unexpected++
			return fail("journal_admission_outside_window")
		}
		globalKey, err := keyFor(block, m.Namespace, v.Token, end)
		if err != nil {
			r.Unexpected++
			return fail("unexpected_full_token")
		}
		i := sort.Search(len(inputs), func(i int) bool { c := inputs[i].Manifest.Config; return globalKey < c.KeyOffset+c.keys() })
		if i == len(inputs) || globalKey < inputs[i].Manifest.Config.KeyOffset {
			r.Unexpected++
			return fail("unexpected_full_token")
		}
		c, s := inputs[i].Manifest.Config, inputs[i].States
		key := globalKey - c.KeyOffset
		seq, err := sequence(c, key, v.Choice)
		if err != nil {
			r.Unexpected++
			return fail("unplanned_choice_or_repeat")
		}
		state := s.Status[seq] & 0x3f
		if state != stateACK && state != stateUnknown {
			r.Unexpected++
			return fail("rejected_or_skipped_attempt_committed")
		}
		if s.Status[seq]&0x80 != 0 {
			r.Copies++
			return fail("extra_physical_copy")
		}
		if state == stateACK {
			if s.Admitted[seq] != v.AdmittedAt.UnixNano() {
				r.Mismatch++
				return fail("ack_admission_mismatch")
			}
			r.Matched++
		} else {
			r.Resolved++
		}
		s.Status[seq] |= 0x80
		if seen[i][key] == 0 {
			r.Canonical++
			r.Choices[v.Choice-1]++
			seen[i][key] = 1
		} else {
			r.Duplicates++
		}
		r.Recorded++
		return nil
	})
	if err == nil && ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		r.Unverified = r.Confirmed - r.Matched
		if r.Failure == "" {
			r.Failure = "journal_replay_failed"
		}
		return r, err
	}
	r.JournalClosed = true
	r.Missing = r.Confirmed - r.Matched
	if r.Missing != 0 {
		return r, fail("ack_missing_from_closed_journal")
	}
	if res.PollID != m.Config.PollID || res.Pending || res.State != "final" || res.TotalVotes < 0 || uint64(res.TotalVotes) != r.Canonical || len(res.Options) != len(m.Poll.Options) {
		return r, fail("service_results_mismatch")
	}
	for i, o := range res.Options {
		var want uint64
		if i < 2 {
			want = r.Choices[i]
		}
		if o.ID != m.Poll.Options[i].ID || o.Label != m.Poll.Options[i].Label || o.Votes < 0 || uint64(o.Votes) != want {
			return r, fail("service_option_results_mismatch")
		}
	}
	r.ResultsMatched = true
	if r.InvalidPositive != 0 {
		return r, fail("invalid_successful_http_responses")
	}
	r.Correct = true
	return r, nil
}

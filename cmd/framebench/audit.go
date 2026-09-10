package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"time"

	"gigaquizz/internal/votelog"
)

type framePosition struct {
	Partition int32
	Offset    int64
}
type frameAccumulator struct {
	active     bool
	offset     int64
	next       uint32
	admittedNS int64
	digest     hash.Hash
}
type auditReport struct {
	Correct            bool       `json:"correct"`
	ClosedPartitions   int        `json:"closed_partitions"`
	RecordedAttempts   uint64     `json:"recorded_attempts"`
	RecordedFrames     uint64     `json:"recorded_frames"`
	ConfirmedAttempts  uint64     `json:"confirmed_client_attempts"`
	ConfirmedFrames    uint64     `json:"confirmed_client_frames"`
	MatchedACKFrames   uint64     `json:"matched_ack_frames"`
	MissingACKFrames   uint64     `json:"missing_ack_frames"`
	MatchedACKAttempts uint64     `json:"matched_ack_attempts"`
	MissingACKAttempts uint64     `json:"missing_ack_attempts"`
	UnknownResolved    uint64     `json:"unknown_attempts_found_committed"`
	CanonicalKeys      uint64     `json:"canonical_unique_keys"`
	Duplicates         uint64     `json:"duplicate_logical_attempts"`
	Choices            [32]uint64 `json:"canonical_choice_counts"`
	UnexpectedRecords  uint64     `json:"unexpected_records"`
	PhysicalCopies     uint64     `json:"unexpected_duplicate_physical_attempts"`
	ReceiptMismatches  uint64     `json:"receipt_mismatches"`
	PayloadBytes       uint64     `json:"recorded_frame_payload_bytes"`
	WallSeconds        float64    `json:"wall_seconds"`
	Method             string     `json:"method"`
}
type replayer func(context.Context, votelog.Config, func(votelog.Position, votelog.Vote) error) (votelog.Manifest, error)

func decodeKey(block cipher.Block, token []byte, keys uint64, counter []byte) (uint32, error) {
	block.Decrypt(counter, token)
	n := binary.BigEndian.Uint64(counter[8:])
	if binary.BigEndian.Uint64(counter[:8]) != 0 || n == 0 || n > keys {
		return 0, fmt.Errorf("record token is outside the generated AES permutation range")
	}
	return uint32(n - 1), nil
}

func reconcile(ctx context.Context, m manifest, s states, acks []ackFrame, replay replayer) (auditReport, error) {
	start := time.Now()
	r := auditReport{ConfirmedFrames: uint64(len(acks)), Method: "Read committed through every CLOSED. AES decrypt each complete128-bit token to its unique synthetic counter; validate original/repeat eligibility and exact per-attempt state. Match every client ACK frame by partition/offset, ordered index/count/admission and SHA256(token16+choice4) digest. Digest is receipt integrity only, never a deduplication key. Canonical first occurrence uses exact AES-bijection-indexed flags."}
	block, _ := aes.NewCipher(m.Seed[:])
	var counter [16]byte
	var encoded [20]byte
	receipts := make(map[framePosition]ackFrame, len(acks))
	for _, ack := range acks {
		p := framePosition{ack.Partition, ack.Offset}
		if _, exists := receipts[p]; exists {
			return r, fmt.Errorf("duplicate receipt position in client ledger")
		}
		receipts[p] = ack
		r.ConfirmedAttempts += uint64(ack.Count)
	}
	var stateACKCount uint64
	for _, state := range s.Original {
		if state == stateACK {
			stateACKCount++
		}
	}
	for _, state := range s.Repeat {
		if state == stateACK {
			stateACKCount++
		}
	}
	if stateACKCount != r.ConfirmedAttempts {
		return r, fmt.Errorf("ACK states and receipt counts differ")
	}
	seen := make([]byte, m.Keys)
	acc := make([]frameAccumulator, m.Config.Partitions)
	flush := func(partition int) error {
		a := &acc[partition]
		if !a.active {
			return nil
		}
		r.RecordedFrames++
		r.PayloadBytes += 80 + 28*uint64(a.next)
		if receipt, ok := receipts[framePosition{int32(partition), a.offset}]; ok {
			var hash [32]byte
			copy(hash[:], a.digest.Sum(nil))
			if a.next != receipt.Count || a.admittedNS != receipt.AdmittedNS || hash != receipt.Digest {
				r.ReceiptMismatches++
				return fmt.Errorf("ACK frame count/admission/ordered digest mismatch")
			}
			r.MatchedACKFrames++
		}
		a.active = false
		return nil
	}
	manifest, err := replay(ctx, m.Config, func(pos votelog.Position, vote votelog.Vote) error {
		if pos.Partition < 0 || int(pos.Partition) >= len(acc) || pos.Offset < 0 {
			r.UnexpectedRecords++
			return fmt.Errorf("invalid replay position")
		}
		a := &acc[pos.Partition]
		if !a.active || a.offset != pos.Offset {
			if a.active && pos.Offset < a.offset {
				r.ReceiptMismatches++
				return fmt.Errorf("replay offset moved backwards")
			}
			if err := flush(int(pos.Partition)); err != nil {
				return err
			}
			*a = frameAccumulator{active: true, offset: pos.Offset, admittedNS: vote.AdmittedAt.UnixNano(), digest: sha256.New()}
		}
		if pos.Index != a.next || vote.AdmittedAt.UnixNano() != a.admittedNS {
			r.ReceiptMismatches++
			return fmt.Errorf("frame index sequence or shared admission mismatch")
		}
		a.next++
		if vote.AdmittedAt.Before(m.Config.StartsAt) || !vote.AdmittedAt.Before(m.Config.EndsAt) {
			r.UnexpectedRecords++
			return fmt.Errorf("vote admitted outside immutable poll window")
		}
		copy(encoded[:16], vote.Token[:])
		key, err := decodeKey(block, encoded[:16], m.Keys, counter[:])
		if err != nil {
			r.UnexpectedRecords++
			return err
		}
		if vote.Choice != 1 && (vote.Choice != 2 || m.RepeatEvery == 0 || (uint64(key)+1)%uint64(m.RepeatEvery) != 0) {
			r.UnexpectedRecords++
			return fmt.Errorf("unexpected choice or repeat key")
		}
		state := s.get(key, vote.Choice)
		if state != stateACK && state != stateUnknown {
			r.UnexpectedRecords++
			return fmt.Errorf("committed attempt was skipped or definitively rejected by client")
		}
		if seen[key]&byte(vote.Choice) != 0 {
			r.PhysicalCopies++
			return fmt.Errorf("one generated attempt appeared more than once in committed journal")
		}
		if seen[key] == 0 {
			r.CanonicalKeys++
			r.Choices[vote.Choice-1]++
		} else {
			r.Duplicates++
		}
		seen[key] |= byte(vote.Choice)
		_, knownReceipt := receipts[framePosition{pos.Partition, pos.Offset}]
		if state == stateACK {
			if !knownReceipt {
				r.ReceiptMismatches++
				return fmt.Errorf("ACK attempt appeared at an unacknowledged frame position")
			}
			r.MatchedACKAttempts++
		} else {
			if knownReceipt {
				r.ReceiptMismatches++
				return fmt.Errorf("unknown client attempt appeared in an ACK frame")
			}
			r.UnknownResolved++
		}
		copy(encoded[:16], vote.Token[:])
		binary.BigEndian.PutUint32(encoded[16:], vote.Choice)
		_, _ = a.digest.Write(encoded[:])
		r.RecordedAttempts++
		return nil
	})
	if err == nil {
		for p := range acc {
			if e := flush(p); e != nil {
				err = e
				break
			}
		}
	}
	r.ClosedPartitions = len(manifest.Partitions)
	r.MissingACKFrames = r.ConfirmedFrames - r.MatchedACKFrames
	r.MissingACKAttempts = r.ConfirmedAttempts - r.MatchedACKAttempts
	r.Correct = err == nil && ctx.Err() == nil && r.ClosedPartitions == m.Config.Partitions && r.MissingACKFrames == 0 && r.MissingACKAttempts == 0 && r.UnexpectedRecords == 0 && r.PhysicalCopies == 0 && r.ReceiptMismatches == 0
	r.WallSeconds = time.Since(start).Seconds()
	if err == nil && !r.Correct {
		err = fmt.Errorf("streaming reconciliation failed; inspect aggregate counters")
	}
	return r, err
}

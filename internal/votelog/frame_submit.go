package votelog

import (
	"context"
	"time"
)

// FrameReceipt confirms one committed frame. Each entry's durable position is
// (Partition, Offset, Index), with Index in [0, Count). It acknowledges attempts,
// never the final canonical choice; duplicates are resolved during replay.
type FrameReceipt struct {
	Partition  int32     `json:"partition"`
	Offset     int64     `json:"offset"`
	Count      uint32    `json:"count"`
	AdmittedAt time.Time `json:"admitted_at"`
}

// SubmitFrame admits one bounded batch for exactly one partition. The caller
// may reuse inputs after return, including on unknown outcome: the store owns
// its copy. Client cancellation cannot revoke an already admitted frame.
func (s *Store) SubmitFrame(ctx context.Context, inputs []Input) (FrameReceipt, error) {
	if len(inputs) == 0 || len(inputs) > s.cfg.BatchSize || len(inputs) > 4096 {
		return FrameReceipt{}, ErrInvalid
	}
	partition := s.cfg.Partition(inputs[0].Token)
	votes := make([]Vote, len(inputs))
	for i, input := range inputs {
		if input.Token == [16]byte{} || !s.cfg.validChoice(input.Choice) || s.cfg.Partition(input.Token) != partition {
			return FrameReceipt{}, ErrInvalid
		}
		votes[i] = Vote{Token: input.Token, Choice: input.Choice}
	}
	if ctx.Err() != nil {
		return FrameReceipt{}, ErrUnknown
	}
	w := s.writers[partition]
	w.mu.Lock()
	if w.sealed || w.closing {
		w.mu.Unlock()
		return FrameReceipt{}, ErrClosed
	}
	if w.failed || s.ctx.Err() != nil {
		w.mu.Unlock()
		return FrameReceipt{}, ErrUnknown
	}
	if w.queuedVotes+len(votes) > s.cfg.QueuePerPartition || len(w.jobs) == cap(w.jobs) {
		w.mu.Unlock()
		return FrameReceipt{}, ErrBusy
	}
	now := s.now()
	if now.Before(s.cfg.StartsAt) {
		w.mu.Unlock()
		return FrameReceipt{}, ErrNotOpen
	}
	if !now.Before(s.cfg.EndsAt) {
		w.mu.Unlock()
		return FrameReceipt{}, ErrClosed
	}
	// All entries pass the same authoritative server admission instant under
	// the partition gate. The frame's commit may finish after the deadline.
	for i := range votes {
		votes[i].AdmittedAt = now.UTC()
	}
	p := &pending{frame: votes, result: make(chan outcome, 1)}
	w.queuedVotes += len(votes)
	w.jobs <- p
	s.admitted.Add(uint64(len(votes)))
	w.mu.Unlock()
	select {
	case result := <-p.result:
		if result.err != nil {
			return FrameReceipt{}, result.err
		}
		return FrameReceipt{Partition: result.receipt.Partition, Offset: result.receipt.Offset, Count: uint32(len(votes)), AdmittedAt: result.receipt.AdmittedAt}, nil
	case <-ctx.Done():
		return FrameReceipt{}, ErrUnknown
	case <-s.ctx.Done():
		return FrameReceipt{}, ErrUnknown
	}
}

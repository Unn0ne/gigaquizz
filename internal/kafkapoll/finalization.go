package kafkapoll

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gigaquizz/internal/poll"
	"gigaquizz/internal/votelog"
)

type partitionResult struct {
	Partition  int32    `json:"partition"`
	Definition [32]byte `json:"definition"`
	Closed     int64    `json:"closed"`
	Total      int64    `json:"total"`
	Counts     []int64  `json:"counts"`
}

func (p partitionResult) checksum() [32]byte { b, _ := json.Marshal(p); return sha256.Sum256(b) }

func validatePartition(e *entry, p partitionResult, maxUnique int) error {
	if p.Partition < 0 || int(p.Partition) >= e.config.Partitions || p.Definition != e.config.DefinitionHash() || p.Closed < 0 || p.Total < 0 || p.Total > int64(maxUnique) || len(p.Counts) != len(e.poll.Options) {
		return errors.New("invalid partition checkpoint definition or bounds")
	}
	var sum int64
	for _, n := range p.Counts {
		if n < 0 || n > p.Total {
			return errors.New("invalid partition checkpoint count")
		}
		sum += n
	}
	if (!e.config.Multiple && sum != p.Total) || (e.config.Multiple && sum < p.Total) {
		return errors.New("partition checkpoint totals disagree")
	}
	return nil
}
func validateFinal(e *entry, data []byte) error {
	var r poll.Results
	if json.Unmarshal(data, &r) != nil || r.Pending || r.State != "final" || r.PollID != e.poll.ID || r.CalculatedAt.IsZero() || r.TotalVotes < 0 || len(r.Options) != len(e.poll.Options) {
		return errors.New("invalid persisted final result")
	}
	if r.CalculatedAt.Before(e.poll.EndsAt) {
		return errors.New("persisted result was finalized before admission closed")
	}
	if e.poll.FinalizedAt != nil && !r.CalculatedAt.UTC().Truncate(time.Microsecond).Equal(e.poll.FinalizedAt.UTC().Truncate(time.Microsecond)) {
		return errors.New("persisted result timestamp disagrees with final metadata")
	}
	var sum int64
	for i, c := range r.Options {
		if c.ID != e.poll.Options[i].ID || c.Label != e.poll.Options[i].Label || c.Votes < 0 || c.Votes > r.TotalVotes {
			return errors.New("invalid persisted final option")
		}
		sum += c.Votes
	}
	if (!e.config.Multiple && sum != r.TotalVotes) || (e.config.Multiple && sum < r.TotalVotes) {
		return errors.New("invalid persisted final totals")
	}
	return nil
}

// Only one finalizer runs in this controller. The administration mutex protects
// preparation/inventory briefly; Kafka replay and exact maps run outside it.
func (s *Store) FinalizeDue(parent context.Context) (int, error) {
	ctx, cancel := s.operation(parent)
	defer cancel()
	if err := s.finalizeMu.LockContext(ctx); err != nil {
		return 0, err
	}
	defer s.finalizeMu.Unlock()
	if err := s.opMu.LockContext(ctx); err != nil {
		return 0, err
	}
	err := s.checkOwner(ctx)
	if err == nil {
		err = s.reloadPending(ctx)
	}
	s.opMu.Unlock()
	if err != nil {
		return 0, err
	}
	s.mu.RLock()
	pending := make([]*entry, 0)
	for _, e := range s.polls {
		if e.poll.FinalizedAt == nil {
			pending = append(pending, e)
		}
	}
	s.mu.RUnlock()
	completed := 0
	for _, e := range pending {
		if ok, err := s.restoreFinal(ctx, e); err != nil {
			return completed, err
		} else if ok {
			completed++
			continue
		}
		if err := s.opMu.LockContext(ctx); err != nil {
			return completed, err
		}
		s.mu.RLock()
		w := e.writer
		s.mu.RUnlock()
		if w == nil {
			err = s.prepare(ctx, e)
			s.mu.RLock()
			w = e.writer
			s.mu.RUnlock()
		}
		s.opMu.Unlock()
		if err != nil {
			return completed, err
		}
		if s.now().Before(e.poll.EndsAt) && !e.admissionClosed.Load() {
			continue
		}
		e.admissionClosed.Store(true)
		manifest, err := sealWithRecovery(ctx, w, func(ctx context.Context, failed journalWriter) (journalWriter, error) {
			if err := s.opMu.LockContext(ctx); err != nil {
				return nil, err
			}
			defer s.opMu.Unlock()
			return s.replaceFailedWriter(ctx, e, failed, s.checkOwner, s.prepare)
		})
		if err != nil {
			return completed, err
		}
		// A worker's partial seal is never sufficient to publish a full result.
		if manifest.Partial || len(manifest.Partitions) != e.config.Partitions {
			return completed, errors.New("full finalization requires every partition CLOSED")
		}
		result, err := s.aggregatePartitions(ctx, e, manifest)
		if err != nil {
			return completed, err
		}
		if err := s.checkOwner(ctx); err != nil {
			return completed, err
		}
		encoded, _ := json.Marshal(result)
		if _, err := s.execDurable(ctx, "UPDATE "+s.table()+" SET result=$2,finalized_at=$3 WHERE id=$1 AND finalized_at IS NULL", e.poll.ID, encoded, result.CalculatedAt); err != nil {
			return completed, err
		}
		if ok, err := s.restoreFinal(ctx, e); err != nil {
			return completed, err
		} else if !ok {
			return completed, errors.New("final result publication incomplete")
		}
		completed++
	}
	return completed, nil
}

func (s *Store) aggregatePartitions(ctx context.Context, e *entry, manifest votelog.Manifest) (poll.Results, error) {
	expected := make(map[int32]int64, e.config.Partitions)
	for _, p := range manifest.Partitions {
		if p.Partition < 0 || int(p.Partition) >= e.config.Partitions || p.Offset < 0 {
			return poll.Results{}, errors.New("invalid CLOSED inventory")
		}
		if _, ok := expected[p.Partition]; ok {
			return poll.Results{}, errors.New("duplicate CLOSED partition")
		}
		expected[p.Partition] = p.Offset
	}
	if len(expected) != e.config.Partitions {
		return poll.Results{}, errors.New("incomplete CLOSED inventory")
	}
	saved, err := s.loadProgress(ctx, e)
	if err != nil {
		return poll.Results{}, err
	}
	result := poll.Results{PollID: e.poll.ID, State: "final", Options: make([]poll.OptionCount, len(e.poll.Options))}
	for i, o := range e.poll.Options {
		result.Options[i] = poll.OptionCount{ID: o.ID, Label: o.Label}
	}
	for partition := 0; partition < e.config.Partitions; partition++ {
		p, ok := saved[int32(partition)]
		if ok && p.Closed != expected[int32(partition)] {
			return poll.Results{}, errors.New("checkpoint CLOSED offset changed")
		}
		if !ok {
			p, err = aggregatePartition(ctx, e, int32(partition), s.opts.MaxPartitionUnique)
			if err != nil {
				return poll.Results{}, err
			}
			if p.Closed != expected[p.Partition] {
				return poll.Results{}, errors.New("replay CLOSED differs from sealed manifest")
			}
			if result.TotalVotes+p.Total > int64(s.opts.MaxUnique) {
				return poll.Results{}, errors.New("total exact unique-ID bound exceeded")
			}
			if err := s.checkOwner(ctx); err != nil {
				return poll.Results{}, err
			}
			counts, _ := json.Marshal(p.Counts)
			checksum := p.checksum()
			tag, err := s.execDurable(ctx, "INSERT INTO "+s.progressTable()+"(poll_id,partition,definition_hash,closed_offset,total,counts,checksum) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING", e.poll.ID, p.Partition, p.Definition[:], p.Closed, p.Total, counts, checksum[:])
			if err != nil {
				return poll.Results{}, err
			}
			if tag.RowsAffected() != 1 {
				return poll.Results{}, errors.New("checkpoint appeared concurrently; reload before publishing")
			}
		}
		if err := validatePartition(e, p, s.opts.MaxUnique); err != nil {
			return poll.Results{}, err
		}
		result.TotalVotes += p.Total
		if result.TotalVotes > int64(s.opts.MaxUnique) {
			return poll.Results{}, errors.New("total exact unique-ID bound exceeded")
		}
		for i, n := range p.Counts {
			result.Options[i].Votes += n
		}
	}
	result.CalculatedAt = finalResultTime(s.now(), e.poll.EndsAt)
	return result, nil
}

func aggregatePartition(ctx context.Context, e *entry, partition int32, maxUnique int) (partitionResult, error) {
	return aggregatePartitionFrom(ctx, e, partition, maxUnique, votelog.ReplayPartition)
}

func aggregatePartitionFrom(ctx context.Context, e *entry, partition int32, maxUnique int, read func(context.Context, votelog.Config, int32, func(votelog.Position, votelog.Vote) error) (votelog.PartitionEnd, error)) (partitionResult, error) {
	p := partitionResult{Partition: partition, Definition: e.config.DefinitionHash(), Closed: -1, Counts: make([]int64, len(e.poll.Options))}
	seen := make(map[[16]byte]struct{})
	end, err := read(ctx, e.config, partition, func(_ votelog.Position, v votelog.Vote) error {
		if _, ok := seen[v.Token]; ok {
			return nil
		}
		if len(seen) >= maxUnique {
			return errors.New("partition exact-ID memory bound exceeded; increase partitions or explicit memory limit")
		}
		seen[v.Token] = struct{}{}
		p.Total++
		for i := range p.Counts {
			if v.Choice&(1<<uint(i)) != 0 {
				p.Counts[i]++
			}
		}
		return nil
	})
	if err != nil {
		return partitionResult{}, err
	}
	p.Closed = end.Offset
	return p, nil
}
func (s *Store) loadProgress(ctx context.Context, e *entry) (map[int32]partitionResult, error) {
	rows, err := s.pool.Query(ctx, "SELECT partition,definition_hash,closed_offset,total,counts,checksum FROM "+s.progressTable()+" WHERE poll_id=$1 ORDER BY partition LIMIT 257", e.poll.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[int32]partitionResult)
	for rows.Next() {
		var p partitionResult
		var hash, counts, checksum []byte
		if err := rows.Scan(&p.Partition, &hash, &p.Closed, &p.Total, &counts, &checksum); err != nil {
			return nil, err
		}
		if len(hash) != 32 || len(checksum) != 32 || json.Unmarshal(counts, &p.Counts) != nil {
			return nil, errors.New("malformed partition checkpoint")
		}
		copy(p.Definition[:], hash)
		expected := p.checksum()
		if string(checksum) != string(expected[:]) {
			return nil, errors.New("partition checkpoint checksum mismatch")
		}
		if err := validatePartition(e, p, s.opts.MaxUnique); err != nil {
			return nil, err
		}
		if _, exists := out[p.Partition]; exists {
			return nil, fmt.Errorf("duplicate checkpoint partition")
		}
		out[p.Partition] = p
	}
	return out, rows.Err()
}

func finalResultTime(now, end time.Time) time.Time {
	if now.Before(end) {
		return end.UTC()
	}
	return now.UTC()
}

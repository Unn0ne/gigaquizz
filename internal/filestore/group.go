package filestore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gigaquizz/internal/filelog"
	"gigaquizz/internal/poll"
)

func (e *entry) groupConfig() filelog.GroupConfig {
	c := e.logConfig()
	return filelog.GroupConfig{Directory: c.Directory, PollID: c.PollID, StartsAt: c.StartsAt, EndsAt: c.EndsAt, AllowedMask: c.AllowedMask, Multiple: c.Multiple, Partitions: e.def.Partitions, BatchSize: c.BatchSize, QueuePerPartition: c.QueuePerPartition, Linger: c.Linger}
}
func (e *entry) newWriter(ctx context.Context) (journalWriter, error) {
	if e.def.Version == 2 {
		return filelog.NewGroup(ctx, e.groupConfig())
	}
	return filelog.New(ctx, e.logConfig())
}
func (e *entry) recoverWriter(ctx context.Context) (journalWriter, error) {
	if e.def.Version == 2 {
		w, err := filelog.RecoverGroup(ctx, e.groupConfig())
		if err != nil {
			return nil, err
		}
		return w, nil
	}
	w, err := filelog.Recover(ctx, e.logConfig())
	if err != nil {
		return nil, err
	}
	return w, nil
}
func (e *entry) replay(ctx context.Context, visit func(filelog.Position, filelog.Vote) error) (filelog.Manifest, error) {
	if e.def.Version == 2 {
		return filelog.GroupReplay(ctx, e.groupConfig(), visit)
	}
	return filelog.Replay(ctx, e.logConfig(), visit)
}

func readDefinition(ctx context.Context, directory string) (*entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	directory = filepath.Clean(directory)
	id := filepath.Base(directory)
	if _, err := parseUUID(id); err != nil || strings.ToLower(id) != id {
		return nil, errors.New("invalid poll directory name")
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("invalid poll directory")
	}
	e := &entry{directory: directory}
	if err := readJSON(filepath.Join(directory, "definition.json"), &e.def); err != nil {
		return nil, err
	}
	if err := validateDefinition(e.def, id); err != nil {
		return nil, err
	}
	return e, nil
}

// ReadPollDefinition reads the published immutable schedule without opening or
// repairing journals. ReplayPoll validates the associated complete journals.
func ReadPollDefinition(ctx context.Context, pollDirectory string) (poll.Poll, error) {
	e, err := readDefinition(ctx, pollDirectory)
	if err != nil {
		return poll.Poll{}, err
	}
	return clonePoll(e.def.Poll), nil
}

// ReplayPoll supports legacy single WALs and fixed topology groups. It is
// read-only, excludes live writers, and requires all CLOSED barriers. Callers
// must discard the entire streamed result if any partition fails validation.
func ReplayPoll(ctx context.Context, pollDirectory string, visit func(filelog.Position, filelog.Vote) error) (filelog.Manifest, error) {
	e, err := readDefinition(ctx, pollDirectory)
	if err != nil {
		return filelog.Manifest{}, err
	}
	if err := validateInventory(e); err != nil {
		return filelog.Manifest{}, err
	}
	return e.replay(ctx, visit)
}

func (e *entry) isCheckpoint(name string) bool {
	if e.def.Version != 2 || !strings.HasPrefix(name, "result-part-") || !strings.HasSuffix(name, ".json") {
		return false
	}
	p, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(name, "result-part-"), ".json"))
	return err == nil && p >= 0 && p < e.def.Partitions && name == fmt.Sprintf("result-part-%04d.json", p)
}

type partitionResult struct {
	Version        int                  `json:"version"`
	DefinitionHash string               `json:"definition_hash"`
	End            filelog.PartitionEnd `json:"end"`
	Total          uint64               `json:"total"`
	Counts         [32]uint64           `json:"counts"`
	Checksum       string               `json:"checksum"`
}

func (s *Store) calculate(ctx context.Context, e *entry, manifest filelog.Manifest) (poll.Results, error) {
	r := poll.Results{PollID: e.def.Poll.ID, State: "final"}
	for _, option := range e.def.Poll.Options {
		r.Options = append(r.Options, poll.OptionCount{ID: option.ID, Label: option.Label})
	}
	parts := 1
	if e.def.Version == 2 {
		parts = e.def.Partitions
	}
	if len(manifest.Partitions) != parts {
		return r, errors.New("incomplete global CLOSED manifest")
	}
	for p := 0; p < parts; p++ {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		piece, err := s.calculatePartition(ctx, e, p, manifest.Partitions[p])
		if err != nil {
			return r, err
		}
		if uint64(r.TotalVotes)+piece.Total > s.c.MaxUnique {
			return r, errors.New("exact result exceeds MAX_UNIQUE_VOTERS; raise bound and restart")
		}
		r.TotalVotes += int64(piece.Total)
		for i := range r.Options {
			r.Options[i].Votes += int64(piece.Counts[i])
		}
	}
	return r, nil
}

func (s *Store) calculatePartition(ctx context.Context, e *entry, p int, closed filelog.PartitionEnd) (partitionResult, error) {
	path := filepath.Join(e.directory, fmt.Sprintf("result-part-%04d.json", p))
	var piece partitionResult
	if e.def.Version == 2 {
		if _, err := os.Lstat(path); err == nil {
			if err := readJSON(path, &piece); err != nil {
				return piece, err
			}
			checksum := piece.Checksum
			piece.Checksum = ""
			if piece.Version != 1 || checksum != hashJSON(piece) || piece.DefinitionHash != hashJSON(e.def) || piece.End != closed || piece.End.Partition != int32(p) || piece.Total > s.c.MaxPartitionUnique {
				return piece, errors.New("invalid partition result checkpoint or partition RAM bound")
			}
			for i, n := range piece.Counts {
				if n > piece.Total || (i >= len(e.def.Poll.Options) && n != 0) {
					return piece, errors.New("invalid partition counts")
				}
			}
			// Recover already fully scanned and CRC/routing-validated every WAL
			// before supplying this CLOSED manifest. Resume without rebuilding its map.
			return piece, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return piece, err
		}
	}
	piece = partitionResult{Version: 1, DefinitionHash: hashJSON(e.def), End: closed}
	seen := make(map[[16]byte]struct{})
	visit := func(_ filelog.Position, v filelog.Vote) error {
		if _, exists := seen[v.Token]; exists {
			return nil
		}
		if uint64(len(seen)) >= s.c.MaxPartitionUnique {
			return errors.New("exact partition exceeds MAX_PARTITION_UNIQUE_VOTERS; raise bound and restart")
		}
		seen[v.Token] = struct{}{}
		piece.Total++
		for i := range e.def.Poll.Options {
			if v.Choice&(1<<i) != 0 {
				piece.Counts[i]++
			}
		}
		return nil
	}
	var end filelog.PartitionEnd
	var err error
	if e.def.Version == 2 {
		end, err = filelog.ReplayGroupPartition(ctx, e.groupConfig(), p, visit)
	} else {
		var m filelog.Manifest
		m, err = filelog.Replay(ctx, e.logConfig(), visit)
		if err == nil {
			end = m.Partitions[0]
		}
	}
	if err != nil {
		return piece, err
	}
	if end != closed {
		return piece, errors.New("partition CLOSED changed during finalization")
	}
	if e.def.Version == 2 {
		piece.Checksum = hashJSON(piece)
		if err := publishJSON(e.directory, filepath.Base(path), piece); err != nil {
			return piece, err
		}
	}
	// The exact map is local to this call. The next partition does not retain it.
	return piece, nil
}

// Diagnostics contains only aggregate counters; no poll/token identifiers.
func (s *Store) Diagnostics() map[string]uint64 {
	out := make(map[string]uint64)
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return out
	}
	if s.creationError != nil {
		out["storage_preparation_failures"]++
	}
	entries := make([]*entry, 0, len(s.entries))
	for _, e := range s.entries {
		entries = append(entries, e)
	}
	s.mu.RUnlock()
	for _, e := range entries {
		e.mu.Lock()
		w, m := e.writer, e.retainedMetrics
		e.mu.Unlock()
		if w != nil {
			m = w.Metrics()
		}
		if m != nil {
			for source, target := range map[string]string{"queued_votes": "storage_queued_votes", "active_votes": "storage_active_votes", "poisoned": "storage_failed_writers", "durable_votes": "storage_durable_votes", "durable_frames": "storage_durable_frames", "group_syncs": "storage_batches", "sync_total_ns": "storage_batch_ns", "busy_votes": "storage_busy_votes"} {
				out[target] += m[source]
			}
		}
	}
	return out
}

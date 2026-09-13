package filestore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gigaquizz/internal/filelog"
	"gigaquizz/internal/poll"
)

func (s *Store) load(ctx context.Context) error {
	items, err := os.ReadDir(filepath.Join(s.c.Directory, "polls"))
	if err != nil {
		return err
	}
	for _, item := range items {
		if !item.IsDir() || item.Type()&os.ModeSymlink != 0 {
			return errors.New("foreign entry in polls directory")
		}
		if strings.HasPrefix(item.Name(), ".creating-") {
			id := strings.TrimPrefix(item.Name(), ".creating-")
			if _, err := parseUUID(id); err != nil || id != strings.ToLower(id) {
				return errors.New("invalid preparation directory")
			}
			continue // No Create or vote ACK can precede the final rename.
		}
		if _, err := parseUUID(item.Name()); err != nil || strings.ToLower(item.Name()) != item.Name() {
			return errors.New("invalid poll directory name")
		}
		e := &entry{directory: filepath.Join(s.c.Directory, "polls", item.Name())}
		if err := readJSON(filepath.Join(e.directory, "definition.json"), &e.def); err != nil {
			return err
		}
		if err := validateDefinition(e.def, item.Name()); err != nil {
			return err
		}
		if err := syncMetadata(filepath.Join(e.directory, "definition.json")); err != nil {
			return err
		}
		if err := syncDir(filepath.Dir(e.directory)); err != nil {
			return err
		}
		for _, old := range s.entries {
			if old.def.Poll.StartsAt.Before(e.def.Poll.EndsAt) && old.def.Poll.EndsAt.After(e.def.Poll.StartsAt) {
				return poll.ErrOverlap
			}
		}
		s.entries[item.Name()] = e
		if !s.now().Before(e.def.Poll.EndsAt) {
			// Definitions reserve the immutable schedule immediately. Historical
			// journals and results are verified independently after serving starts.
			e.needsValidation = true
			e.closing = true
			continue
		}
		if err := validateInventory(e); err != nil {
			return err
		}
		w, err := e.recoverWriter(ctx)
		if err != nil {
			return err
		}
		e.writer = w
		if s.now().Before(e.def.Poll.StartsAt) {
			w.Close()
			e.writer = nil
		}
		if _, err := os.Lstat(filepath.Join(e.directory, "result.json")); !errors.Is(err, os.ErrNotExist) {
			return errors.New("premature result for an unfinished poll")
		}
	}
	return nil
}

func validateDefinition(d definition, id string) error {
	p := d.Poll
	if (d.Version != 1 && d.Version != 2) || (d.Version == 1 && d.Partitions != 0) || (d.Version == 2 && (d.Partitions < 2 || d.Partitions > 256)) || p.ID != id || p.FinalizedAt != nil || p.CreatedAt.IsZero() || p.EndsAt.Sub(p.StartsAt) != time.Minute {
		return errors.New("invalid immutable poll definition")
	}
	input := poll.CreateInput{Question: p.Question, Type: p.Type}
	for i, option := range p.Options {
		if option.ID != i+1 {
			return errors.New("invalid option IDs")
		}
		input.Options = append(input.Options, option.Label)
	}
	if err := input.Validate(); err != nil {
		return err
	}
	if input.Question != p.Question {
		return errors.New("noncanonical poll question")
	}
	for i, option := range p.Options {
		if input.Options[i] != option.Label {
			return errors.New("noncanonical option")
		}
	}
	_, err := normalizeConfig(Config{Directory: ".", MaxUnique: d.MaxUnique, BatchSize: d.BatchSize, QueueVotes: d.QueueVotes, Linger: d.Linger})
	if d.MaxUnique == 0 || d.BatchSize == 0 || d.QueueVotes == 0 {
		return errors.New("missing persisted limits")
	}
	return err
}

func validateInventory(e *entry) error {
	files, err := os.ReadDir(e.directory)
	if err != nil {
		return err
	}
	for _, f := range files {
		if f.Type()&os.ModeSymlink != 0 || (f.Name() != "definition.json" && f.Name() != "journal" && f.Name() != "result.json" && !e.isCheckpoint(f.Name()) && !(strings.HasPrefix(f.Name(), ".result-") && strings.HasSuffix(f.Name(), ".tmp"))) {
			return errors.New("foreign file in poll directory")
		}
	}
	if e.def.Version == 2 {
		return filelog.CheckGroup(e.groupConfig())
	}
	journalFiles, err := os.ReadDir(filepath.Join(e.directory, "journal"))
	if err != nil {
		return err
	}
	for _, f := range journalFiles {
		if f.Type()&os.ModeSymlink != 0 || (f.Name() != "poll.json" && f.Name() != "votes.wal") {
			return errors.New("foreign file in vote journal")
		}
	}
	return nil
}

func readVerifiedResult(e *entry, manifest filelog.Manifest) (*poll.Results, error) {
	path := filepath.Join(e.directory, "result.json")
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	var disk diskResult
	if err := readJSON(path, &disk); err != nil {
		return nil, err
	}
	checksum := disk.Checksum
	disk.Checksum = ""
	if checksum != hashJSON(disk) || disk.Version != 1 || disk.DefinitionHash != hashJSON(e.def) || disk.Results.PollID != e.def.Poll.ID || disk.Results.State != "final" || disk.Results.Pending || disk.Results.CalculatedAt.Before(e.def.Poll.EndsAt) || len(disk.Results.Options) != len(e.def.Poll.Options) || disk.Results.TotalVotes < 0 {
		return nil, errors.New("invalid stored result")
	}
	for i, option := range disk.Results.Options {
		if option.ID != e.def.Poll.Options[i].ID || option.Label != e.def.Poll.Options[i].Label || option.Votes < 0 || option.Votes > disk.Results.TotalVotes {
			return nil, errors.New("invalid stored option counts")
		}
	}
	if !sameManifest(manifest, disk.Manifest) {
		return nil, errors.New("stored result CLOSED mismatch")
	}
	if err := syncMetadata(path); err != nil {
		return nil, err
	}
	if err := syncDir(e.directory); err != nil {
		return nil, err
	}
	return &disk.Results, nil
}

func sameManifest(a, b filelog.Manifest) bool {
	if len(a.Partitions) != len(b.Partitions) {
		return false
	}
	for i := range a.Partitions {
		if a.Partitions[i] != b.Partitions[i] {
			return false
		}
	}
	return true
}

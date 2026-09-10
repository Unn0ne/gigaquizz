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
		files, err := os.ReadDir(e.directory)
		if err != nil {
			return err
		}
		for _, f := range files {
			if f.Type()&os.ModeSymlink != 0 || (f.Name() != "definition.json" && f.Name() != "journal" && f.Name() != "result.json" && !(strings.HasPrefix(f.Name(), ".result-") && strings.HasSuffix(f.Name(), ".tmp"))) {
				return errors.New("foreign file in poll directory")
			}
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
		w, err := filelog.Recover(ctx, e.logConfig())
		if err != nil {
			return err
		}
		e.writer = w
		s.entries[item.Name()] = e
		if e.def.Poll.State(s.now()) != "open" {
			if !s.now().Before(e.def.Poll.EndsAt) {
				if _, err := w.Seal(ctx); err != nil {
					return err
				}
			}
			w.Close()
			e.writer = nil
		}
		if _, err := os.Lstat(filepath.Join(e.directory, "result.json")); err == nil {
			var disk diskResult
			if err := readJSON(filepath.Join(e.directory, "result.json"), &disk); err != nil {
				return err
			}
			checksum := disk.Checksum
			disk.Checksum = ""
			if checksum != hashJSON(disk) || disk.Version != 1 || disk.DefinitionHash != hashJSON(e.def) || disk.Results.PollID != e.def.Poll.ID || disk.Results.State != "final" || disk.Results.Pending || disk.Results.CalculatedAt.IsZero() || len(disk.Results.Options) != len(e.def.Poll.Options) || disk.Results.TotalVotes < 0 {
				return errors.New("invalid stored result")
			}
			for i, option := range disk.Results.Options {
				if option.ID != e.def.Poll.Options[i].ID || option.Label != e.def.Poll.Options[i].Label || option.Votes < 0 || option.Votes > disk.Results.TotalVotes {
					return errors.New("invalid stored option counts")
				}
			}
			if e.writer != nil {
				e.writer.Close()
				e.writer = nil
			}
			manifest, err := filelog.Replay(ctx, e.logConfig(), nil)
			if err != nil {
				return err
			}
			if len(manifest.Partitions) != 1 || len(disk.Manifest.Partitions) != 1 || manifest.Partitions[0] != disk.Manifest.Partitions[0] {
				return errors.New("stored result CLOSED mismatch")
			}
			e.result = &disk.Results
			if err := syncMetadata(filepath.Join(e.directory, "result.json")); err != nil {
				return err
			}
			if err := syncDir(e.directory); err != nil {
				return err
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func validateDefinition(d definition, id string) error {
	p := d.Poll
	if d.Version != 1 || p.ID != id || p.FinalizedAt != nil || p.CreatedAt.IsZero() || p.EndsAt.Sub(p.StartsAt) != time.Minute {
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
	if d.MaxUnique == 0 || d.BatchSize == 0 || d.QueueVotes == 0 || d.Linger == 0 {
		return errors.New("missing persisted limits")
	}
	return err
}

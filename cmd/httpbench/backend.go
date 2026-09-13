package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"gigaquizz/internal/filelog"
	"gigaquizz/internal/filestore"
)

func backend(m privateManifest, fileJournal, kafkaConfig string) (*journalReader, error) {
	if fileJournal == "" || kafkaConfig != "" {
		return nil, errors.New("files reader requires only -file-journal")
	}
	if _, err := os.Lstat(filepath.Join(fileJournal, "definition.json")); err == nil {
		return &journalReader{Replay: func(ctx context.Context, visit func(journalVote) error) error {
			p, err := filestore.ReadPollDefinition(ctx, fileJournal)
			if err != nil {
				return err
			}
			if !samePoll(p, m.Poll) {
				return errors.New("file poll definition differs from the client manifest")
			}
			manifest, err := filestore.ReplayPoll(ctx, fileJournal, func(_ filelog.Position, v filelog.Vote) error {
				return visit(journalVote{v.Token, v.Choice, v.AdmittedAt})
			})
			if err != nil {
				return err
			}
			if len(manifest.Partitions) < 1 || len(manifest.Partitions) > 256 {
				return errors.New("invalid CLOSED group inventory")
			}
			for i, p := range manifest.Partitions {
				if p.Partition != int32(i) || p.Offset < 0 {
					return errors.New("invalid CLOSED group partition")
				}
			}
			return nil
		}}, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	id, err := parseID(m.Config.PollID)
	if err != nil {
		return nil, err
	}
	c := filelog.Config{Directory: fileJournal, PollID: id, StartsAt: m.Poll.StartsAt, EndsAt: m.Poll.EndsAt, AllowedMask: (uint32(1) << len(m.Poll.Options)) - 1, Multiple: m.Poll.Type == "multiple", BatchSize: 4096, QueuePerPartition: 65536, Partitions: 1}
	return &journalReader{Replay: func(ctx context.Context, visit func(journalVote) error) error {
		manifest, err := filelog.Replay(ctx, c, func(_ filelog.Position, v filelog.Vote) error {
			return visit(journalVote{v.Token, v.Choice, v.AdmittedAt})
		})
		if err != nil {
			return err
		}
		if len(manifest.Partitions) != 1 || manifest.Partitions[0].Partition != 0 || manifest.Partitions[0].Offset < 0 {
			return errors.New("invalid CLOSED manifest")
		}
		return nil
	}}, nil
}

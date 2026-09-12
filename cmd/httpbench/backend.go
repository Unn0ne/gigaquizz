package main

import (
	"context"
	"errors"

	"gigaquizz/internal/filelog"
)

func backend(m privateManifest, fileJournal, kafkaConfig string) (*journalReader, error) {
	if fileJournal == "" || kafkaConfig != "" {
		return nil, errors.New("files reader requires only -file-journal")
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

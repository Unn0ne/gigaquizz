package main

import (
	"context"
	"errors"
	"os"
	"strconv"

	"gigaquizz/internal/votelog"
)

func backend(m privateManifest, fileJournal, kafkaConfig string) (*journalReader, error) {
	if fileJournal != "" || kafkaConfig == "" {
		return nil, errors.New("Kafka reader requires only -kafka-config")
	}
	var c votelog.Config
	if err := readPrivateJSON(kafkaConfig, &c, 65536); err != nil {
		return nil, err
	}
	id, err := parseID(m.Config.PollID)
	if err != nil || c.PollID != id || !c.StartsAt.Equal(m.Poll.StartsAt) || !c.EndsAt.Equal(m.Poll.EndsAt) || c.AllowedMask != (uint32(1)<<len(m.Poll.Options))-1 || c.Multiple != (m.Poll.Type == "multiple") {
		return nil, errors.New("Kafka configuration differs from HTTP poll")
	}
	// Journal JSON never stores credentials. Offline readers use the same
	// runtime environment as the application, independently of private ledgers.
	tlsEnabled := false
	if raw := os.Getenv("KAFKA_TLS"); raw != "" {
		tlsEnabled, err = strconv.ParseBool(raw)
		if err != nil {
			return nil, errors.New("invalid Kafka reader TLS setting")
		}
	}
	c.Security, err = votelog.BuildSecurity(votelog.SecurityOptions{
		TLS: tlsEnabled, CAFile: os.Getenv("KAFKA_TLS_CA_FILE"), CertFile: os.Getenv("KAFKA_TLS_CERT_FILE"),
		KeyFile: os.Getenv("KAFKA_TLS_KEY_FILE"), ServerName: os.Getenv("KAFKA_TLS_SERVER_NAME"),
		SASLMechanism: os.Getenv("KAFKA_SASL_MECHANISM"), Username: os.Getenv("KAFKA_SASL_USERNAME"), Password: os.Getenv("KAFKA_SASL_PASSWORD"),
	})
	if err != nil {
		return nil, err
	}
	return kafkaReader(c, votelog.ReplayStrict), nil
}

type strictReplayer func(context.Context, votelog.Config, func(votelog.Position, votelog.Vote) error) (votelog.StrictReplayResult, error)

func kafkaReader(c votelog.Config, replay strictReplayer) *journalReader {
	inspection := &journalInspection{Method: "kafka-stable-snapshot"}
	return &journalReader{Inspection: inspection, Replay: func(ctx context.Context, visit func(journalVote) error) error {
		*inspection = journalInspection{Method: "kafka-stable-snapshot"}
		result, err := replay(ctx, c, func(_ votelog.Position, v votelog.Vote) error {
			return visit(journalVote{v.Token, v.Choice, v.AdmittedAt})
		})
		if err != nil {
			return err
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		manifest := result.Manifest
		if len(manifest.Partitions) != c.Partitions || manifest.Topic != c.Topic || !manifest.StartsAt.Equal(c.StartsAt) || !manifest.EndsAt.Equal(c.EndsAt) {
			return errors.New("invalid CLOSED manifest")
		}
		for i, p := range manifest.Partitions {
			if p.Partition != int32(i) || p.Offset < 0 {
				return errors.New("invalid CLOSED partition")
			}
		}
		if len(result.SnapshotEndOffsets) != c.Partitions {
			return errors.New("incomplete strict snapshot inventory")
		}
		for i, end := range result.SnapshotEndOffsets {
			if end.Partition != int32(i) || end.Offset <= manifest.Partitions[i].Offset {
				return errors.New("strict snapshot does not include CLOSED")
			}
		}
		inspection.RecoveryBootRecords = result.RecoveryBootRecords
		inspection.SnapshotPartitions = len(result.SnapshotEndOffsets)
		inspection.StrictSnapshotComplete = true
		return nil
	}}
}

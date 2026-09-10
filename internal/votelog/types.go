// Package votelog is an isolated local Kafka architecture prototype. A receipt
// proves a committed attempt, not the final unique choice. It has static,
// explicitly transferred writer ownership; it is not a production controller.
package votelog

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"net"
	"regexp"
	"time"
)

var (
	ErrNotOpen = errors.New("poll has not started")
	ErrClosed  = errors.New("poll admission closed")
	ErrBusy    = errors.New("bounded admission queue full")
	ErrUnknown = errors.New("attempt outcome unknown")
	ErrInvalid = errors.New("invalid vote or configuration")
)

type Config struct {
	Brokers            []string
	Topic              string
	PollID             [16]byte
	Partitions         int
	StartsAt, EndsAt   time.Time
	AllowedMask        uint32
	Multiple           bool
	BatchSize          int
	Linger             time.Duration
	QueuePerPartition  int
	TransactionTimeout time.Duration
}

var ownedTopic = regexp.MustCompile(`^(gqlog_|gigaquizz_logbench_)[a-z0-9_]{8,100}$`)

func (c Config) validate() error {
	if !ownedTopic.MatchString(c.Topic) || c.PollID == [16]byte{} || c.Partitions < 1 || c.Partitions > 256 ||
		c.EndsAt.Sub(c.StartsAt) != time.Minute || c.StartsAt.IsZero() || c.AllowedMask == 0 ||
		c.BatchSize < 1 || c.BatchSize > 4096 || c.QueuePerPartition < 1 || c.QueuePerPartition > 8192 ||
		c.Linger < time.Millisecond || c.Linger > time.Second || c.TransactionTimeout < time.Second || c.TransactionTimeout > 30*time.Second {
		return ErrInvalid
	}
	if len(c.Brokers) < 1 || len(c.Brokers) > 3 {
		return ErrInvalid
	}
	for _, address := range c.Brokers {
		host, _, err := net.SplitHostPort(address)
		ip := net.ParseIP(host)
		if err != nil || ip == nil || !ip.IsLoopback() {
			return errors.New("prototype requires numeric loopback brokers")
		}
	}
	return nil
}

func (c Config) validChoice(choice uint32) bool {
	return choice != 0 && choice & ^c.AllowedMask == 0 && (c.Multiple || bits.OnesCount32(choice) == 1)
}

func (c Config) Partition(token [16]byte) int32 {
	var key [32]byte
	copy(key[:16], c.PollID[:])
	copy(key[16:], token[:])
	h := sha256.Sum256(key[:])
	return int32(binary.BigEndian.Uint64(h[:8]) % uint64(c.Partitions))
}

type Input struct {
	Token  [16]byte
	Choice uint32
}

type Receipt struct {
	Partition  int32     `json:"partition"`
	Offset     int64     `json:"offset"`
	Index      uint32    `json:"index,omitempty"`
	AdmittedAt time.Time `json:"admitted_at"`
}

type Position struct {
	Partition int32
	Offset    int64
	Index     uint32
}
type Vote struct {
	Token      [16]byte
	Choice     uint32
	AdmittedAt time.Time
}
type PartitionEnd struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}
type Manifest struct {
	Topic      string         `json:"topic"`
	Partitions []PartitionEnd `json:"partitions"`
	StartsAt   time.Time      `json:"starts_at"`
	EndsAt     time.Time      `json:"ends_at"`
}
type AuditResult struct {
	Manifest      Manifest
	Canonical     map[[16]byte]Vote
	Records       map[Position]Vote
	TotalAttempts uint64
	ChoiceCounts  [32]uint64
}

const (
	recordVote   byte = 1
	recordBoot   byte = 2
	recordClosed byte = 3
	recordBytes       = 80
)

// Every record binds the immutable poll definition. Control records have no
// voter key. The Kafka record key adds 16 bytes to an 80-byte vote value.
func encodeRecord(c Config, kind byte, vote Vote) []byte {
	b := make([]byte, recordBytes)
	b[0], b[1] = 1, kind
	if c.Multiple {
		b[2] = 1
	}
	binary.BigEndian.PutUint32(b[4:8], vote.Choice)
	copy(b[8:24], c.PollID[:])
	copy(b[24:40], vote.Token[:])
	if !vote.AdmittedAt.IsZero() {
		binary.BigEndian.PutUint64(b[40:48], uint64(vote.AdmittedAt.UnixNano()))
	}
	binary.BigEndian.PutUint64(b[48:56], uint64(c.StartsAt.UnixNano()))
	binary.BigEndian.PutUint64(b[56:64], uint64(c.EndsAt.UnixNano()))
	binary.BigEndian.PutUint32(b[64:68], uint32(c.Partitions))
	binary.BigEndian.PutUint32(b[68:72], c.AllowedMask)
	return b
}

func decodeRecord(c Config, b []byte) (byte, Vote, error) {
	var v Vote
	if len(b) != recordBytes || b[0] != 1 || b[1] < recordVote || b[1] > recordClosed || b[2] > 1 || b[3] != 0 ||
		(b[2] == 1) != c.Multiple || string(b[8:24]) != string(c.PollID[:]) ||
		int64(binary.BigEndian.Uint64(b[48:56])) != c.StartsAt.UnixNano() || int64(binary.BigEndian.Uint64(b[56:64])) != c.EndsAt.UnixNano() ||
		int(binary.BigEndian.Uint32(b[64:68])) != c.Partitions || binary.BigEndian.Uint32(b[68:72]) != c.AllowedMask || binary.BigEndian.Uint64(b[72:80]) != 0 {
		return 0, v, fmt.Errorf("journal record does not match immutable poll configuration")
	}
	v.Choice = binary.BigEndian.Uint32(b[4:8])
	copy(v.Token[:], b[24:40])
	if b[1] == recordVote {
		v.AdmittedAt = time.Unix(0, int64(binary.BigEndian.Uint64(b[40:48]))).UTC()
		if v.Token == [16]byte{} || !c.validChoice(v.Choice) || v.AdmittedAt.Before(c.StartsAt) || !v.AdmittedAt.Before(c.EndsAt) {
			return 0, Vote{}, errors.New("invalid admitted journal vote")
		}
	} else if v.Token != [16]byte{} || v.Choice != 0 || binary.BigEndian.Uint64(b[40:48]) != 0 {
		return 0, Vote{}, errors.New("invalid journal control record")
	}
	return b[1], v, nil
}

func unknown(err error) error {
	if err == nil {
		return nil
	}
	// Errors intentionally do not embed a record or its key.
	return fmt.Errorf("%w: %s", ErrUnknown, err.Error())
}

func waitUntil(ctx context.Context, until time.Time, now func() time.Time) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		delay := until.Sub(now())
		if delay <= 0 {
			return nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
			// A monotonic timer wake does not prove that the wall-clock
			// admission deadline has arrived. Recheck before sealing.
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

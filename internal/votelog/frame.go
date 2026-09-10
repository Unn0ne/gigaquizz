package votelog

import (
	"encoding/binary"
	"errors"
	"time"
)

const (
	frameVersion    byte = 2
	frameEntryBytes      = 28
	maxFrameVotes        = 4096
)

// A frame is one Kafka record containing a bounded, ordered sequence of votes.
// Its immutable 80-byte header uses the v1 configuration layout. Each vote
// retains its complete token, exact choice and server admission timestamp.
func encodeFrame(c Config, votes []Vote) ([]byte, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	if len(votes) < 1 || len(votes) > maxFrameVotes {
		return nil, errors.New("journal frame vote count outside bounds")
	}
	for _, v := range votes {
		if !validFrameVote(c, v) {
			return nil, errors.New("invalid admitted journal frame vote")
		}
	}
	b := make([]byte, recordBytes+len(votes)*frameEntryBytes)
	copy(b, encodeRecord(c, recordBoot, Vote{}))
	b[0], b[1] = frameVersion, recordVote
	binary.BigEndian.PutUint32(b[72:76], uint32(len(votes)))
	binary.BigEndian.PutUint32(b[76:80], frameEntryBytes)
	for i, v := range votes {
		entry := b[recordBytes+i*frameEntryBytes:][:frameEntryBytes]
		copy(entry[:16], v.Token[:])
		binary.BigEndian.PutUint32(entry[16:20], v.Choice)
		binary.BigEndian.PutUint64(entry[20:28], uint64(v.AdmittedAt.UnixNano()))
	}
	return b, nil
}

func validFrameVote(c Config, v Vote) bool {
	return v.Token != [16]byte{} && c.validChoice(v.Choice) &&
		!v.AdmittedAt.Before(c.StartsAt) && v.AdmittedAt.Before(c.EndsAt)
}

func frameVote(entry []byte) Vote {
	v := Vote{Choice: binary.BigEndian.Uint32(entry[16:20]),
		AdmittedAt: time.Unix(0, int64(binary.BigEndian.Uint64(entry[20:28]))).UTC()}
	copy(v.Token[:], entry[:16])
	return v
}

func decodeFrame(c Config, b []byte, visit func(index uint32, v Vote) error) error {
	if len(b) < recordBytes || b[0] != frameVersion || b[1] != recordVote {
		return errors.New("invalid journal frame format")
	}
	count := binary.BigEndian.Uint32(b[72:76])
	if count < 1 || count > maxFrameVotes || binary.BigEndian.Uint32(b[76:80]) != frameEntryBytes ||
		len(b) != recordBytes+int(count)*frameEntryBytes {
		return errors.New("invalid journal frame count or length")
	}
	// Reuse the complete immutable-configuration and zero-control-payload
	// validation of v1. The original frame bytes are never modified.
	var header [recordBytes]byte
	copy(header[:], b[:recordBytes])
	header[0], header[1] = 1, recordBoot
	clear(header[72:80])
	if _, _, err := decodeRecord(c, header[:]); err != nil {
		return err
	}
	// Validate every entry before invoking a visitor. A malformed last entry
	// must not partially apply an otherwise valid prefix of this frame.
	for i := 0; i < int(count); i++ {
		if !validFrameVote(c, frameVote(b[recordBytes+i*frameEntryBytes:][:frameEntryBytes])) {
			return errors.New("invalid admitted journal frame vote")
		}
	}
	if visit != nil {
		for i := 0; i < int(count); i++ {
			if err := visit(uint32(i), frameVote(b[recordBytes+i*frameEntryBytes:][:frameEntryBytes])); err != nil {
				return err
			}
		}
	}
	return nil
}

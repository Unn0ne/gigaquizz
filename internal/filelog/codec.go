package filelog

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"time"
)

const (
	walName          = "votes.wal"
	pollName         = "poll.json"
	fileHeaderBytes  = 80
	frameHeaderBytes = 40
	entryBytes       = 28
	maxFrameVotes    = 4096
	kindVotes        = 1
	kindClosed       = 2
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type diskPoll struct {
	Version     int       `json:"version"`
	PollID      [16]byte  `json:"poll_id"`
	StartsAt    time.Time `json:"starts_at"`
	EndsAt      time.Time `json:"ends_at"`
	AllowedMask uint32    `json:"allowed_mask"`
	Multiple    bool      `json:"multiple"`
}

func pollBytes(c Config) []byte {
	b, _ := json.Marshal(diskPoll{1, c.PollID, c.StartsAt.UTC(), c.EndsAt.UTC(), c.AllowedMask, c.Multiple})
	return append(b, '\n')
}

func fileHeader(c Config) []byte {
	b := make([]byte, fileHeaderBytes)
	copy(b[:8], "GQFILE01")
	copy(b[8:24], c.PollID[:])
	binary.BigEndian.PutUint64(b[24:32], uint64(c.StartsAt.UnixNano()))
	binary.BigEndian.PutUint64(b[32:40], uint64(c.EndsAt.UnixNano()))
	binary.BigEndian.PutUint32(b[40:44], c.AllowedMask)
	if c.Multiple {
		b[44] = 1
	}
	binary.BigEndian.PutUint32(b[76:80], crc32.Checksum(b[:76], crcTable))
	return b
}

func encodeInputs(inputs []Input, admitted time.Time) []byte {
	b := make([]byte, frameHeaderBytes+entryBytes*len(inputs))
	for i, v := range inputs {
		e := b[frameHeaderBytes+i*entryBytes : frameHeaderBytes+(i+1)*entryBytes]
		copy(e[:16], v.Token[:])
		binary.BigEndian.PutUint32(e[16:20], v.Choice)
		binary.BigEndian.PutUint64(e[20:28], uint64(admitted.UnixNano()))
	}
	return b
}

func finishFrame(b []byte, sequence int64, kind uint32, count uint32) {
	copy(b[:4], "GQF1")
	binary.BigEndian.PutUint32(b[4:8], uint32(len(b)))
	binary.BigEndian.PutUint64(b[8:16], uint64(sequence))
	binary.BigEndian.PutUint32(b[16:20], kind)
	binary.BigEndian.PutUint32(b[20:24], count)
	binary.BigEndian.PutUint32(b[32:36], crc32.Checksum(b[frameHeaderBytes:], crcTable))
	binary.BigEndian.PutUint32(b[36:40], crc32.Checksum(b[:36], crcTable))
}

func checkFrameHeader(b []byte, sequence int64) (kind uint32, count uint32, err error) {
	if len(b) != frameHeaderBytes || string(b[:4]) != "GQF1" ||
		binary.BigEndian.Uint32(b[36:40]) != crc32.Checksum(b[:36], crcTable) ||
		binary.BigEndian.Uint64(b[8:16]) != uint64(sequence) || !bytes.Equal(b[24:32], make([]byte, 8)) {
		return 0, 0, fmt.Errorf("invalid frame header at sequence %d", sequence)
	}
	kind, count = binary.BigEndian.Uint32(b[16:20]), binary.BigEndian.Uint32(b[20:24])
	if (kind != kindVotes && kind != kindClosed) || (kind == kindVotes && (count == 0 || count > maxFrameVotes)) ||
		(kind == kindClosed && count != 0) || binary.BigEndian.Uint32(b[4:8]) != frameHeaderBytes+count*entryBytes {
		return 0, 0, fmt.Errorf("invalid frame length or kind at sequence %d", sequence)
	}
	return kind, count, nil
}

func decodeEntry(e []byte) Vote {
	var v Vote
	copy(v.Token[:], e[:16])
	v.Choice = binary.BigEndian.Uint32(e[16:20])
	v.AdmittedAt = time.Unix(0, int64(binary.BigEndian.Uint64(e[20:28]))).UTC()
	return v
}

func validatePayload(c Config, header, payload []byte) error {
	if binary.BigEndian.Uint32(header[32:36]) != crc32.Checksum(payload, crcTable) {
		return fmt.Errorf("frame payload CRC mismatch")
	}
	for i := 0; i < len(payload); i += entryBytes {
		v := decodeEntry(payload[i : i+entryBytes])
		if v.Token == [16]byte{} || !c.validChoice(v.Choice) || v.AdmittedAt.Before(c.StartsAt) || !v.AdmittedAt.Before(c.EndsAt) {
			return fmt.Errorf("invalid vote in frame")
		}
	}
	return nil
}

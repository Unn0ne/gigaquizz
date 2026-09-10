package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"syscall"
	"time"

	"gigaquizz/internal/votelog"
)

const maximumLedgerBytes int64 = 512 << 20

var topicPattern = regexp.MustCompile(`^gqlog_framebench_[0-9a-f]{16}$`)

type manifest struct {
	Version     int               `json:"version"`
	Config      votelog.Config    `json:"journal_configuration"`
	Seed        [16]byte          `json:"private_aes_seed"`
	Rate        int               `json:"rate"`
	Keys        uint64            `json:"keys"`
	RepeatEvery int               `json:"repeat_every"`
	FrameSize   int               `json:"frame_size"`
	FrameAge    time.Duration     `json:"frame_age_ns"`
	ACKCount    uint64            `json:"ack_frame_count"`
	Files       map[string]string `json:"files"`
	Hashes      map[string]string `json:"sha256"`
	Counts      counts            `json:"client_counts"`
}

func ledgerBytes(c config) uint64 {
	size := c.keys()
	if c.repeatEvery > 0 {
		size += c.keys()
	}
	return size + c.frameBound()*64 + 65536
}
func diskGuard(directory string, c config, includeBroker bool) (map[string]uint64, error) {
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, fmt.Errorf("cannot create private frame ledger directory")
	}
	files, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("cannot inspect frame ledger directory")
	}
	var used uint64
	for _, f := range files {
		info, err := f.Info()
		if err != nil {
			return nil, fmt.Errorf("cannot inspect retained frame ledger")
		}
		if info.Mode().IsRegular() {
			used += uint64(info.Size())
		}
	}
	client := ledgerBytes(c)
	if client > uint64(maximumLedgerBytes) || used+client > 1<<30 || len(files)+4 > 256 {
		return nil, fmt.Errorf("private frame ledgers exceed512MiB/run,1GiB aggregate or256-file bound; prior artifacts preserved")
	}
	broker := uint64(0)
	if includeBroker {
		broker = max(c.attempts()*32, c.attempts()*28+c.frameBound()*(80+256))*3 + (64 << 20)
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(directory, &fs); err != nil {
		return nil, fmt.Errorf("cannot inspect frame benchmark disk space")
	}
	free := uint64(fs.Bavail) * uint64(fs.Bsize)
	need := client + broker + (8 << 30)
	report := map[string]uint64{"free_bytes": free, "estimated_client_ledger_bytes": client, "estimated_new_broker_bytes": broker, "assumed_protocol_reserve_bytes_per_frame": 256, "reserve_bytes": 8 << 30, "required_free_bytes": need, "retained_client_bytes": used}
	if free < need {
		return report, fmt.Errorf("insufficient free space for bounded RF3 records, client ledger and8GiB reserve")
	}
	return report, nil
}
func encodeACK(a ackFrame, b []byte) {
	clear(b)
	binary.LittleEndian.PutUint32(b[:4], uint32(a.Partition))
	binary.LittleEndian.PutUint32(b[4:8], a.Count)
	binary.LittleEndian.PutUint64(b[8:16], uint64(a.Offset))
	binary.LittleEndian.PutUint64(b[16:24], uint64(a.AdmittedNS))
	copy(b[24:56], a.Digest[:])
}
func decodeACK(b []byte) ackFrame {
	a := ackFrame{Partition: int32(binary.LittleEndian.Uint32(b[:4])), Count: binary.LittleEndian.Uint32(b[4:8]), Offset: int64(binary.LittleEndian.Uint64(b[8:16])), AdmittedNS: int64(binary.LittleEndian.Uint64(b[16:24]))}
	copy(a.Digest[:], b[24:56])
	return a
}
func saveLedger(directory string, m manifest, s states, acks []ackFrame, c config) (string, error) {
	if _, err := diskGuard(directory, c, false); err != nil {
		return "", err
	}
	if !topicPattern.MatchString(m.Config.Topic) || uint64(len(acks)) > maximumFrames {
		return "", fmt.Errorf("invalid owned frame ledger")
	}
	m.Version = 1
	m.ACKCount = uint64(len(acks))
	m.Files = map[string]string{}
	m.Hashes = map[string]string{}
	ackBytes := make([]byte, len(acks)*64)
	for i, a := range acks {
		encodeACK(a, ackBytes[i*64:(i+1)*64])
	}
	write := func(path string, b []byte) error {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return err
		}
		_, err = f.Write(b)
		if err == nil {
			err = f.Sync()
		}
		closeErr := f.Close()
		if err != nil {
			return err
		}
		return closeErr
	}
	for _, item := range []struct {
		name  string
		bytes []byte
	}{{"original", s.Original}, {"repeat", s.Repeat}, {"acks", ackBytes}} {
		if item.name == "repeat" && len(item.bytes) == 0 {
			continue
		}
		name := m.Config.Topic + "." + item.name
		if err := write(filepath.Join(directory, name), item.bytes); err != nil {
			return "", fmt.Errorf("cannot write new private frame ledger file")
		}
		hash := sha256.Sum256(item.bytes)
		m.Files[item.name] = name
		m.Hashes[item.name] = hex.EncodeToString(hash[:])
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil || len(b) > 65536 {
		return "", fmt.Errorf("invalid frame manifest size")
	}
	path := filepath.Join(directory, m.Config.Topic+".json")
	if err := write(path, b); err != nil {
		return "", fmt.Errorf("cannot write frame manifest")
	}
	return path, nil
}

func readLedger(path string) (manifest, states, []ackFrame, error) {
	var m manifest
	var s states
	read := func(path string, expected int64) ([]byte, error) {
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Size() > maximumLedgerBytes || expected >= 0 && info.Size() != expected {
			return nil, fmt.Errorf("invalid bounded frame ledger file")
		}
		return os.ReadFile(path)
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() > 65536 {
		return m, s, nil, fmt.Errorf("invalid bounded frame manifest")
	}
	b, err := read(path, -1)
	if err != nil {
		return m, s, nil, err
	}
	if err = json.Unmarshal(b, &m); err != nil {
		return m, s, nil, fmt.Errorf("cannot decode frame manifest")
	}
	if m.Version != 1 || !topicPattern.MatchString(m.Config.Topic) || m.Rate < 1 || m.Rate > maximumRate || m.Keys != uint64(m.Rate)*60 || m.RepeatEvery < 0 || m.ACKCount > maximumFrames || m.FrameSize < 1 || m.FrameSize > 4096 || m.Config.Partitions < 1 || m.Config.Partitions > 32 || m.Config.EndsAt.Sub(m.Config.StartsAt) != time.Minute {
		return m, s, nil, fmt.Errorf("invalid frame manifest bounds")
	}
	planned := m.Keys
	if m.RepeatEvery > 0 {
		planned += m.Keys / uint64(m.RepeatEvery)
	}
	if planned > maximumAttempts {
		return m, s, nil, fmt.Errorf("manifest attempts exceed hard bound")
	}
	readPart := func(part string, size int64) ([]byte, error) {
		name, ok := m.Files[part]
		if !ok || name != m.Config.Topic+"."+part {
			return nil, fmt.Errorf("invalid private frame file reference")
		}
		b, err := read(filepath.Join(filepath.Dir(path), name), size)
		if err != nil {
			return nil, err
		}
		hash := sha256.Sum256(b)
		if hex.EncodeToString(hash[:]) != m.Hashes[part] {
			return nil, fmt.Errorf("frame ledger checksum mismatch")
		}
		return b, nil
	}
	s.Original, err = readPart("original", int64(m.Keys))
	if err != nil {
		return m, s, nil, err
	}
	if m.RepeatEvery > 0 {
		s.Repeat, err = readPart("repeat", int64(m.Keys))
		if err != nil {
			return m, s, nil, err
		}
	}
	for _, x := range [][]byte{s.Original, s.Repeat} {
		for _, state := range x {
			if state > stateRejected {
				return m, s, nil, fmt.Errorf("invalid per-attempt state")
			}
		}
	}
	b, err = readPart("acks", int64(m.ACKCount*64))
	if err != nil {
		return m, s, nil, err
	}
	acks := make([]ackFrame, m.ACKCount)
	for i := range acks {
		a := decodeACK(b[i*64 : (i+1)*64])
		if a.Partition < 0 || int(a.Partition) >= m.Config.Partitions || a.Offset < 0 || a.Count < 1 || int(a.Count) > m.FrameSize || a.AdmittedNS < m.Config.StartsAt.UnixNano() || a.AdmittedNS >= m.Config.EndsAt.UnixNano() {
			return m, s, nil, fmt.Errorf("invalid ACK frame metadata")
		}
		acks[i] = a
	}
	return m, s, acks, nil
}

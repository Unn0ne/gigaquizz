package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"gigaquizz/internal/votelog"
)

const ledgerMagic = "GQLB0001"
const entryBytes = 72
const maxHeaderBytes = 65536
const maxLedgerBytes = int64(maximumAttempts*entryBytes + maxHeaderBytes + 44)
const maxLedgerDirectoryBytes int64 = 5 << 30
const maxLedgerFiles = 64

func prepareLedgerDirectory(directory string, entries int) error {
	if err := os.MkdirAll(directory, 0700); err != nil {
		return fmt.Errorf("cannot create private ledger directory")
	}
	return ledgerGuard(directory, int64(entries)*entryBytes+maxHeaderBytes+44)
}

type ledgerHeader struct {
	Version  int            `json:"version"`
	Config   votelog.Config `json:"journal_configuration"`
	Mode     string         `json:"mode"`
	Entries  int            `json:"entries"`
	Keys     int            `json:"keys"`
	Rate     int            `json:"rate"`
	OwnerPID int            `json:"original_owner_pid"`
}

func requireStoppedOwner(pid int) error {
	if pid <= 0 {
		return fmt.Errorf("recovery requires a ledger recording its original owner PID")
	}
	if err := syscall.Kill(pid, 0); err == nil || err == syscall.EPERM {
		return fmt.Errorf("original owner process is still present; stop it before explicit recovery (no automatic takeover)")
	} else if err != syscall.ESRCH {
		return fmt.Errorf("cannot establish that original owner process has stopped")
	}
	return nil
}

func lockRecoveryLedger(path string) (func(), error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cannot lock recovery ledger")
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("another explicit recovery holds this ledger; concurrent takeover refused")
	}
	return func() { _ = f.Close() }, nil
}

func encodeEntry(e entry, b []byte) {
	clear(b)
	copy(b[:16], e.Token[:])
	binary.LittleEndian.PutUint32(b[16:20], e.Key)
	binary.LittleEndian.PutUint32(b[20:24], e.Choice)
	b[24] = byte(e.Outcome)
	binary.LittleEndian.PutUint32(b[28:32], uint32(e.Partition))
	binary.LittleEndian.PutUint64(b[32:40], uint64(e.Offset))
	binary.LittleEndian.PutUint64(b[40:48], uint64(e.AdmittedNS))
	binary.LittleEndian.PutUint64(b[48:56], uint64(e.DispatchNS))
	binary.LittleEndian.PutUint64(b[56:64], uint64(e.CompleteNS))
}
func decodeEntry(b []byte) entry {
	e := entry{Key: binary.LittleEndian.Uint32(b[16:20]), Choice: binary.LittleEndian.Uint32(b[20:24]), Outcome: outcome(b[24]), Partition: int32(binary.LittleEndian.Uint32(b[28:32])), Offset: int64(binary.LittleEndian.Uint64(b[32:40])), AdmittedNS: int64(binary.LittleEndian.Uint64(b[40:48])), DispatchNS: int64(binary.LittleEndian.Uint64(b[48:56])), CompleteNS: int64(binary.LittleEndian.Uint64(b[56:64]))}
	copy(e.Token[:], b[:16])
	return e
}

func writeLedger(directory string, h ledgerHeader, entries []entry) (path string, err error) {
	if h.Entries != len(entries) || h.Entries < 1 || h.Entries > maximumAttempts {
		return "", fmt.Errorf("invalid ledger count")
	}
	if !ownedTopic(h.Config.Topic) {
		return "", fmt.Errorf("ledger topic is outside the owned namespace")
	}
	if err = os.MkdirAll(directory, 0700); err != nil {
		return "", fmt.Errorf("cannot create private ledger directory")
	}
	if err = ledgerGuard(directory, int64(len(entries))*entryBytes+maxHeaderBytes+44); err != nil {
		return "", err
	}
	path = filepath.Join(directory, h.Config.Topic+".ledger")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return "", fmt.Errorf("cannot create new private ledger")
	}
	defer func() {
		if closeErr := f.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("cannot close private ledger")
		}
	}()
	b, err := json.Marshal(h)
	if err != nil || len(b) > maxHeaderBytes {
		return path, fmt.Errorf("invalid ledger header")
	}
	w := bufio.NewWriterSize(f, 256*1024)
	hash := sha256.New()
	out := io.MultiWriter(w, hash)
	if _, err = io.WriteString(out, ledgerMagic); err != nil {
		return path, err
	}
	var n [4]byte
	binary.LittleEndian.PutUint32(n[:], uint32(len(b)))
	if _, err = out.Write(n[:]); err != nil {
		return path, err
	}
	if _, err = out.Write(b); err != nil {
		return path, err
	}
	var record [entryBytes]byte
	for _, e := range entries {
		encodeEntry(e, record[:])
		if _, err = out.Write(record[:]); err != nil {
			return path, err
		}
	}
	if _, err = w.Write(hash.Sum(nil)); err != nil {
		return path, err
	}
	if err = w.Flush(); err != nil {
		return path, err
	}
	if err = f.Sync(); err != nil {
		return path, err
	}
	return path, nil
}

// Each run is bounded independently, and retained ledgers also have a cumulative
// bound. Existing receipts are preserved when the bound is reached.
func ledgerGuard(directory string, expectedBytes int64) error {
	files, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("cannot inspect private ledger directory")
	}
	var bytes int64
	for _, file := range files {
		info, err := file.Info()
		if err != nil {
			return fmt.Errorf("cannot inspect retained ledger")
		}
		if info.Mode().IsRegular() {
			bytes += info.Size()
		}
	}
	if len(files) >= maxLedgerFiles || bytes+expectedBytes > maxLedgerDirectoryBytes {
		return fmt.Errorf("retained ledger limit reached (64 files or 4 GiB); existing receipts preserved")
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(directory, &fs); err != nil {
		return fmt.Errorf("cannot inspect ledger disk space")
	}
	if uint64(fs.Bavail)*uint64(fs.Bsize) < uint64(expectedBytes)+(2<<30) {
		return fmt.Errorf("ledger disk guard requires expected ledger size plus 2 GiB free")
	}
	return nil
}

func readLedger(path string) (ledgerHeader, []entry, error) {
	var h ledgerHeader
	f, err := os.Open(path)
	if err != nil {
		return h, nil, fmt.Errorf("cannot open private ledger")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxLedgerBytes {
		return h, nil, fmt.Errorf("ledger is not a bounded regular file")
	}
	r := bufio.NewReaderSize(f, 256*1024)
	hash := sha256.New()
	in := io.TeeReader(r, hash)
	var magic [8]byte
	if _, err = io.ReadFull(in, magic[:]); err != nil || string(magic[:]) != ledgerMagic {
		return h, nil, fmt.Errorf("invalid ledger magic")
	}
	var n [4]byte
	if _, err = io.ReadFull(in, n[:]); err != nil {
		return h, nil, fmt.Errorf("incomplete ledger header")
	}
	size := binary.LittleEndian.Uint32(n[:])
	if size < 1 || size > maxHeaderBytes {
		return h, nil, fmt.Errorf("invalid ledger header length")
	}
	b := make([]byte, size)
	if _, err = io.ReadFull(in, b); err != nil {
		return h, nil, fmt.Errorf("incomplete ledger header")
	}
	if err = json.Unmarshal(b, &h); err != nil || h.Version != 1 || h.Entries < 1 || h.Entries > maximumAttempts || h.Keys < 1 || h.Keys > h.Entries || h.Rate < 1 || h.Rate > maximumRate || !ownedTopic(h.Config.Topic) {
		return h, nil, fmt.Errorf("invalid ledger configuration")
	}
	if expected := int64(12+size) + int64(h.Entries)*entryBytes + sha256.Size; info.Size() != expected {
		return h, nil, fmt.Errorf("ledger size does not match its declared count")
	}
	entries := make([]entry, h.Entries)
	var record [entryBytes]byte
	for i := range entries {
		if _, err = io.ReadFull(in, record[:]); err != nil {
			return h, nil, fmt.Errorf("incomplete ledger record")
		}
		e := decodeEntry(record[:])
		if int(e.Key) >= h.Keys || e.Outcome > skipCancelled || e.Choice != 1 && e.Choice != 2 {
			return h, nil, fmt.Errorf("invalid ledger record")
		}
		entries[i] = e
	}
	var checksum [sha256.Size]byte
	if _, err = io.ReadFull(r, checksum[:]); err != nil {
		return h, nil, fmt.Errorf("missing ledger checksum")
	}
	if string(checksum[:]) != string(hash.Sum(nil)) {
		return h, nil, fmt.Errorf("ledger checksum mismatch")
	}
	return h, entries, nil
}

func ownedTopic(topic string) bool {
	const prefix = "gigaquizz_logbench_"
	if !strings.HasPrefix(topic, prefix) {
		return false
	}
	suffix := strings.TrimPrefix(topic, prefix)
	_, err := hex.DecodeString(suffix)
	return err == nil && len(suffix) == 16
}

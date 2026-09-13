package main

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const (
	stateACK byte = iota + 1
	stateUnknown
	stateClosed
	stateBusy
	stateNotOpen
	stateRejected
	stateSkipped
	// Append states only: baseline manifests and states 1..7 remain readable.
	stateJourneyFailed
	stateCount
	flagInvalidPositive byte = 1
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type attempt struct {
	Seq, Key     uint64
	Token        [16]byte
	Choice       uint32
	State, Flags byte
	HTTP         uint16
	AdmittedNS   int64
	ScheduledNS  int64
}

func encodeAttempt(a attempt, b *[ledgerBytes]byte) {
	clear(b[:])
	binary.BigEndian.PutUint64(b[0:8], a.Seq)
	binary.BigEndian.PutUint64(b[8:16], a.Key)
	copy(b[16:32], a.Token[:])
	binary.BigEndian.PutUint32(b[32:36], a.Choice)
	b[36], b[37] = a.State, a.Flags
	binary.BigEndian.PutUint16(b[38:40], a.HTTP)
	binary.BigEndian.PutUint64(b[40:48], uint64(a.AdmittedNS))
	binary.BigEndian.PutUint64(b[48:56], uint64(a.ScheduledNS))
	binary.BigEndian.PutUint32(b[60:64], crc32.Checksum(b[:60], crcTable))
}
func decodeAttempt(b []byte) (attempt, error) {
	var a attempt
	if len(b) != ledgerBytes || binary.BigEndian.Uint32(b[60:]) != crc32.Checksum(b[:60], crcTable) || binary.BigEndian.Uint32(b[56:60]) != 0 {
		return a, errors.New("ledger record CRC or format mismatch")
	}
	a.Seq = binary.BigEndian.Uint64(b[:8])
	a.Key = binary.BigEndian.Uint64(b[8:16])
	copy(a.Token[:], b[16:32])
	a.Choice = binary.BigEndian.Uint32(b[32:36])
	a.State = b[36]
	a.Flags = b[37]
	a.HTTP = binary.BigEndian.Uint16(b[38:40])
	a.AdmittedNS = int64(binary.BigEndian.Uint64(b[40:48]))
	a.ScheduledNS = int64(binary.BigEndian.Uint64(b[48:56]))
	if a.State == 0 || a.State >= stateCount || a.Flags & ^flagInvalidPositive != 0 || (a.Flags != 0 && a.State != stateUnknown) || (a.State != stateACK && a.AdmittedNS != 0) {
		return a, errors.New("invalid ledger state")
	}
	return a, nil
}

func tokenFor(block cipher.Block, namespace [8]byte, key uint64) [16]byte {
	var plain, token [16]byte
	copy(plain[:8], namespace[:])
	binary.BigEndian.PutUint64(plain[8:], key)
	block.Encrypt(token[:], plain[:])
	return token
}
func keyFor(block cipher.Block, namespace [8]byte, token [16]byte, keys uint64) (uint64, error) {
	var plain [16]byte
	block.Decrypt(plain[:], token[:])
	key := binary.BigEndian.Uint64(plain[8:])
	if !bytes.Equal(plain[:8], namespace[:]) || key >= keys {
		return 0, errors.New("full token outside exact synthetic permutation")
	}
	return key, nil
}
func sequence(c config, key uint64, choice uint32) (uint64, error) {
	if key >= c.keys() {
		return 0, errors.New("key out of range")
	}
	if choice == 1 {
		return key, nil
	}
	if choice == 2 && c.RepeatEvery != 0 && (c.KeyOffset+key+1)%c.RepeatEvery == 0 {
		return c.keys() + (c.KeyOffset+key+1)/c.RepeatEvery - c.KeyOffset/c.RepeatEvery - 1, nil
	}
	return 0, errors.New("unplanned key or choice")
}

type ledgerWriter struct {
	f    *os.File
	b    *bufio.Writer
	h    hash.Hash
	meta ledgerFile
	err  error
}

func newLedger(dir string, index int) (*ledgerWriter, error) {
	name := fmt.Sprintf("worker-%04d.bin", index)
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	return &ledgerWriter{f: f, b: bufio.NewWriterSize(io.MultiWriter(f, h), 64*1024), h: h, meta: ledgerFile{Name: name}}, nil
}
func (w *ledgerWriter) append(a attempt) {
	if w.err != nil {
		return
	}
	var b [ledgerBytes]byte
	encodeAttempt(a, &b)
	if _, w.err = w.b.Write(b[:]); w.err == nil {
		w.meta.Records++
	}
}
func (w *ledgerWriter) close() (ledgerFile, error) {
	err := w.err
	if e := w.b.Flush(); err == nil {
		err = e
	}
	if e := w.f.Sync(); err == nil {
		err = e
	}
	if e := w.f.Close(); err == nil {
		err = e
	}
	w.meta.SHA256 = hex.EncodeToString(w.h.Sum(nil))
	return w.meta, err
}

func createManifest(c config, pollData privateManifest) (privateManifest, error) {
	m := pollData
	m.Version = 1
	m.Complete, m.Files = false, nil
	m.Config = c
	if c.PlanFile != "" {
		p, err := loadDistributedPlan(c.PlanFile)
		if err != nil {
			return m, err
		}
		if c.Generator < 0 || c.Generator >= len(p.Generators) || !sameConfig(c, p.Generators[c.Generator]) || !samePoll(m.Poll, p.Poll) {
			return m, errors.New("generator configuration differs from distributed plan")
		}
		m.Seed, m.Namespace, m.PlanSHA256 = p.Seed, p.Namespace, p.digest()
	} else {
		if _, err := rand.Read(m.Seed[:]); err != nil {
			return m, err
		}
		if _, err := rand.Read(m.Namespace[:]); err != nil {
			return m, err
		}
		// The permutation can theoretically contain the protocol's forbidden zero
		// token. Reject that seed before transmitting any request.
		for {
			b, _ := aes.NewCipher(m.Seed[:])
			if _, err := keyFor(b, m.Namespace, [16]byte{}, c.KeyOffset+c.keys()); err != nil {
				break
			}
			if _, err := rand.Read(m.Seed[:]); err != nil {
				return m, err
			}
		}
	}
	if err := os.Mkdir(c.Directory, 0700); err != nil {
		return m, err
	}
	if err := writeManifest(c.Directory, m); err != nil {
		return m, err
	}
	if err := syncDirectory(filepath.Dir(c.Directory)); err != nil {
		return m, err
	}
	return m, nil
}
func writeManifest(dir string, m privateManifest) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, "manifest.tmp"), os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(append(b, '\n'))
	if e := f.Sync(); err == nil {
		err = e
	}
	if e := f.Close(); err == nil {
		err = e
	}
	if err != nil {
		return err
	}
	if err = os.Rename(filepath.Join(dir, "manifest.tmp"), filepath.Join(dir, "manifest.json")); err != nil {
		return err
	}
	return syncDirectory(dir)
}
func syncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func preflight(c config) error {
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		return err
	}
	if err := checkFileBudget(c.Workers, limit.Cur); err != nil {
		return err
	}
	parent := filepath.Dir(c.Directory)
	i, err := os.Lstat(parent)
	if err != nil || !i.IsDir() || i.Mode()&os.ModeSymlink != 0 {
		return errors.New("ledger parent must be an existing real directory")
	}
	if _, err = os.Lstat(c.Directory); !errors.Is(err, os.ErrNotExist) {
		return errors.New("ledger directory must be new")
	}
	var fs syscall.Statfs_t
	if err = syscall.Statfs(parent, &fs); err != nil {
		return err
	}
	free := uint64(fs.Bavail) * uint64(fs.Bsize)
	if free < (8<<30)+c.attempts()*ledgerBytes {
		return errors.New("ledger preflight requires 8GiB free reserve after planned ledger")
	}
	return nil
}

func checkFileBudget(workers int, soft uint64) error {
	// A private ledger and at most one connection per worker, plus scheduler,
	// DNS, standard descriptors and bounded transport overhead. Never change
	// the parent shell or system limit from this standalone generator.
	if workers < 1 || soft < uint64(2*workers+64) {
		return errors.New("open-file limit too small: require at least 2*workers+64; raise the child limit explicitly")
	}
	return nil
}

// Inputs are private local artifacts. Reject symlinks, broad permissions,
// unexpectedly large data and extra JSON values before allocating audit state.
func readPrivateJSON(path string, dst any, limit int64) error {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	i, err := f.Stat()
	if err != nil {
		return err
	}
	if !i.Mode().IsRegular() || i.Size() > limit || i.Mode().Perm()&0077 != 0 {
		return errors.New("private JSON bounds or permissions invalid")
	}
	d := json.NewDecoder(io.LimitReader(f, limit+1))
	if err = d.Decode(dst); err != nil {
		return err
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return errors.New("extra JSON input")
	}
	return nil
}

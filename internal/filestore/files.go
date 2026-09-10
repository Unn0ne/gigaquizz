package filestore

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"gigaquizz/internal/filelog"
)

func uuid() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = b[6]&15 | 64
	b[8] = b[8]&63 | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

func parseUUID(s string) ([16]byte, error) {
	var out [16]byte
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return out, errors.New("invalid UUID")
	}
	b, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	if err != nil || len(b) != 16 {
		return out, errors.New("invalid UUID")
	}
	copy(out[:], b)
	if out == [16]byte{} {
		return out, errors.New("zero UUID")
	}
	return out, nil
}

func parseToken(s string) ([16]byte, error) {
	var out [16]byte
	if len(s) != 32 || s != strings.ToLower(s) {
		return out, errors.New("invalid vote token")
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return out, err
	}
	copy(out[:], b)
	if out == [16]byte{} {
		return out, errors.New("zero vote token")
	}
	return out, nil
}

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	err = f.Sync()
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	return err
}

// No symlinks at any component. Newly created directory entries are synced.
func ensureDirectory(path string) error {
	info, err := os.Lstat(path)
	if err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("data path must be a directory without symlinks")
		}
		if parent := filepath.Dir(path); parent != path {
			return ensureDirectory(parent)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(path)
	if parent == path {
		return err
	}
	if err := ensureDirectory(parent); err != nil {
		return err
	}
	if err := os.Mkdir(path, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	return syncDir(parent)
}

func regular(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("expected regular owned file: %s", filepath.Base(path))
	}
	return nil
}

func syncMetadata(path string) error {
	if err := regular(path); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	err = filelog.DurableSync(f)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return err
}

func readJSON(path string, out any) error {
	if err := regular(path); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 65537))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("trailing or oversized JSON metadata")
	}
	return nil
}

func writeAll(f *os.File, b []byte) error {
	for len(b) > 0 {
		n, err := f.Write(b)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

func createJSON(path string, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	err = writeAll(f, b)
	if err == nil {
		err = filelog.DurableSync(f)
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// The sole owner and per-poll finalizer ensure there is no concurrent
// publisher. Failed publication leaves owned temporary data for inspection.
func publishResult(directory string, value any) error {
	path := filepath.Join(directory, "result.json")
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return errors.New("result already exists")
		}
		return err
	}
	id, err := uuid()
	if err != nil {
		return err
	}
	tmp := filepath.Join(directory, ".result-"+id+".tmp")
	if err := createJSON(tmp, value); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(directory)
}

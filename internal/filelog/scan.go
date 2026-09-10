package filelog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"syscall"
)

// Scan reads a quiescent journal without modifying it. A shared nonblocking
// flock excludes its live writer. Only EOF inside a frame is an incomplete
// tail; complete header/payload CRC failures always return an error.
func Scan(ctx context.Context, c Config, visit func(Position, Vote) error) (ScanResult, error) {
	var result ScanResult
	if err := c.validate(); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	f, err := os.Open(filepath.Join(c.Directory, walName))
	if err != nil {
		return result, err
	}
	defer f.Close()
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
		return result, fmt.Errorf("journal is owned by a live writer: %w", err)
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return scanFile(ctx, c, f, visit)
}

// scanFile shares the exact parser between read-only audit and recovery. Its
// caller owns the appropriate flock for the entire scan and any later repair.
func scanFile(ctx context.Context, c Config, f *os.File, visit func(Position, Vote) error) (ScanResult, error) {
	var result ScanResult
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return result, err
	}
	poll, err := os.Open(filepath.Join(c.Directory, pollName))
	if err != nil {
		return result, err
	}
	metadata, err := io.ReadAll(io.LimitReader(poll, 4097))
	closeErr := poll.Close()
	if err != nil {
		return result, err
	}
	if closeErr != nil {
		return result, closeErr
	}
	if !bytes.Equal(metadata, pollBytes(c)) {
		return result, fmt.Errorf("immutable poll metadata mismatch")
	}
	header := make([]byte, fileHeaderBytes)
	if _, err := io.ReadFull(f, header); err != nil {
		return result, fmt.Errorf("incomplete WAL header: %w", err)
	}
	if !bytes.Equal(header, fileHeader(c)) {
		return result, fmt.Errorf("immutable WAL header mismatch")
	}
	result.ValidBytes = fileHeaderBytes
	header = make([]byte, frameHeaderBytes)
	payload := make([]byte, maxFrameVotes*entryBytes)
	for sequence := int64(0); ; sequence++ {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		n, err := io.ReadFull(f, header)
		if err == io.EOF {
			return result, nil
		}
		if result.Closed {
			return result, fmt.Errorf("bytes after CLOSED")
		}
		if err == io.ErrUnexpectedEOF && n > 0 {
			result.IncompleteTail = true
			return result, nil
		}
		if err != nil {
			return result, err
		}
		kind, count, err := checkFrameHeader(header, sequence)
		if err != nil {
			return result, err
		}
		body := payload[:int(count)*entryBytes]
		if _, err = io.ReadFull(f, body); errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			result.IncompleteTail = true
			return result, nil
		} else if err != nil {
			return result, err
		}
		if err = validatePayload(c, header, body); err != nil {
			return result, fmt.Errorf("frame %d: %w", sequence, err)
		}
		if kind == kindClosed {
			result.Closed = true
			result.Manifest = Manifest{Partitions: []PartitionEnd{{Partition: 0, Offset: sequence}}}
		} else {
			// Validate the entire frame before exposing any of its entries.
			for index := uint32(0); index < count; index++ {
				if visit != nil {
					if err := visit(Position{Partition: 0, Offset: sequence, Index: index}, decodeEntry(body[int(index)*entryBytes:])); err != nil {
						return result, err
					}
				}
			}
			result.Votes += uint64(count)
		}
		result.Frames++
		result.ValidBytes += int64(frameHeaderBytes + len(body))
		if sequence == math.MaxInt64 {
			return result, fmt.Errorf("journal sequence exhausted")
		}
	}
}

// Replay requires a complete, durable-format CLOSED barrier and no trailing
// bytes. Callers must discard visitor-derived results when Replay returns an
// error; it is streaming and may have visited a valid prefix first.
func Replay(ctx context.Context, c Config, visit func(Position, Vote) error) (Manifest, error) {
	r, err := Scan(ctx, c, visit)
	if err != nil {
		return Manifest{}, err
	}
	if !r.Closed || r.IncompleteTail {
		return Manifest{}, fmt.Errorf("journal has no complete CLOSED barrier")
	}
	return r.Manifest, nil
}

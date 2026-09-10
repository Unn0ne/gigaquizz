package filelog

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func closedFixture(t *testing.T) (Config, []byte) {
	t.Helper()
	c := testConfig(t)
	s, clock := testStore(t, c)
	for _, id := range []byte{1, 2} {
		if _, err := s.SubmitFrame(context.Background(), []Input{testInput(id, 1)}); err != nil {
			t.Fatal(err)
		}
	}
	sealTest(t, s, clock)
	b, err := os.ReadFile(filepath.Join(c.Directory, walName))
	if err != nil {
		t.Fatal(err)
	}
	return c, b
}

func TestScanDistinguishesPartialTailFromCorruption(t *testing.T) {
	for _, test := range []struct {
		name       string
		mutate     func([]byte) []byte
		incomplete bool
		votes      uint64
	}{
		{"partial_header", func(b []byte) []byte { return b[:fileHeaderBytes+68+13] }, true, 1},
		{"partial_payload", func(b []byte) []byte { return b[:fileHeaderBytes+68+frameHeaderBytes+13] }, true, 1},
		{"header_crc_at_tail", func(b []byte) []byte {
			b = b[:fileHeaderBytes+68+frameHeaderBytes]
			b[fileHeaderBytes+68+4] ^= 1
			return b
		}, false, 0},
		{"payload_crc_at_tail", func(b []byte) []byte {
			b = b[:fileHeaderBytes+68]
			b[fileHeaderBytes+frameHeaderBytes+3] ^= 1
			return b
		}, false, 0},
		{"payload_crc_in_middle", func(b []byte) []byte { b[fileHeaderBytes+frameHeaderBytes+3] ^= 1; return b }, false, 0},
		{"byte_after_closed", func(b []byte) []byte { return append(b, 1) }, false, 0},
		{"file_header_corruption", func(b []byte) []byte { b[31] ^= 1; return b }, false, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, b := closedFixture(t)
			if err := os.WriteFile(filepath.Join(c.Directory, walName), test.mutate(b), 0600); err != nil {
				t.Fatal(err)
			}
			r, err := Scan(context.Background(), c, nil)
			if test.incomplete {
				if err != nil || !r.IncompleteTail || r.Closed || r.Votes != test.votes {
					t.Fatalf("partial tail: %+v %v", r, err)
				}
			} else if err == nil {
				t.Fatalf("corruption returned successful prefix: %+v", r)
			}
			if _, err := Replay(context.Background(), c, nil); err == nil {
				t.Fatal("damaged or incomplete journal returned final result")
			}
		})
	}
}

func TestReplayRejectsValidFrameAfterClosed(t *testing.T) {
	c, b := closedFixture(t)
	frame := encodeInputs([]Input{testInput(9, 1)}, c.StartsAt.Add(time.Second))
	finishFrame(frame, 3, kindVotes, 1)
	if err := os.WriteFile(filepath.Join(c.Directory, walName), append(b, frame...), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Replay(context.Background(), c, nil); err == nil {
		t.Fatal("validly encoded frame after CLOSED accepted")
	}
}

func TestCodecRejectsMalformedCompleteFramesBeforeVisit(t *testing.T) {
	for _, kind := range []string{"zero_token", "choice", "timestamp", "count", "sequence", "kind", "reserved", "length"} {
		t.Run(kind, func(t *testing.T) {
			c := testConfig(t)
			s, _ := testStore(t, c)
			s.Close()
			frame := encodeInputs([]Input{testInput(1, 1), testInput(2, 2)}, c.StartsAt.Add(time.Second))
			finishFrame(frame, 0, kindVotes, 2)
			switch kind {
			case "zero_token":
				clear(frame[frameHeaderBytes+entryBytes : frameHeaderBytes+entryBytes+16])
			case "choice":
				binary.BigEndian.PutUint32(frame[frameHeaderBytes+entryBytes+16:], 3)
			case "timestamp":
				binary.BigEndian.PutUint64(frame[frameHeaderBytes+entryBytes+20:], uint64(c.EndsAt.UnixNano()))
			case "count":
				binary.BigEndian.PutUint32(frame[20:24], maxFrameVotes+1)
			case "sequence":
				binary.BigEndian.PutUint64(frame[8:16], 9)
			case "kind":
				binary.BigEndian.PutUint32(frame[16:20], 99)
			case "reserved":
				frame[27] = 1
			case "length":
				binary.BigEndian.PutUint32(frame[4:8], 1)
			}
			binary.BigEndian.PutUint32(frame[32:36], crc32.Checksum(frame[frameHeaderBytes:], crcTable))
			binary.BigEndian.PutUint32(frame[36:40], crc32.Checksum(frame[:36], crcTable))
			if err := os.WriteFile(filepath.Join(c.Directory, walName), append(fileHeader(c), frame...), 0600); err != nil {
				t.Fatal(err)
			}
			visits := 0
			if _, err := Scan(context.Background(), c, func(Position, Vote) error { visits++; return nil }); err == nil {
				t.Fatal("malformed frame accepted")
			}
			if visits != 0 {
				t.Fatalf("partially exposed malformed frame: %d", visits)
			}
		})
	}
}

func TestSeveralFramesShareOneSyncGroup(t *testing.T) {
	c := testConfig(t)
	c.BatchSize = 3
	c.Linger = time.Second
	s, clock := testStore(t, c)
	first, second := make(chan answer, 1), make(chan answer, 1)
	go func() {
		r, err := s.SubmitFrame(context.Background(), []Input{testInput(1, 1)})
		first <- answer{r, err}
	}()
	deadline := time.Now().Add(3 * time.Second)
	for s.Metrics()["active_votes"] != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.Metrics()["active_votes"] != 1 {
		t.Fatal("first frame did not enter batch")
	}
	go func() {
		r, err := s.SubmitFrame(context.Background(), []Input{testInput(2, 1), testInput(3, 1)})
		second <- answer{r, err}
	}()
	a, b := awaitValue(t, first), awaitValue(t, second)
	if a.err != nil || b.err != nil || a.receipt.Offset != 0 || b.receipt.Offset != 1 || s.Metrics()["group_syncs"] != 1 {
		t.Fatalf("frames did not share a sync: %+v %+v %v", a, b, s.Metrics())
	}
	sealTest(t, s, clock)
}

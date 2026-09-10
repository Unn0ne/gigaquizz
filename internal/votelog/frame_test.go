package votelog

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"testing"
	"time"
)

func frameFixture() (Config, []Vote) {
	start := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	cfg := Config{Brokers: []string{"127.0.0.1:19092"}, Topic: "gqlog_frame_0123456789abcdef",
		PollID: [16]byte{1, 2}, Partitions: 2, StartsAt: start, EndsAt: start.Add(time.Minute),
		AllowedMask: 7, Multiple: true, BatchSize: 64, Linger: time.Millisecond,
		QueuePerPartition: 128, TransactionTimeout: time.Second}
	votes := []Vote{
		{Token: [16]byte{0: 3, 15: 1}, Choice: 5, AdmittedAt: cfg.StartsAt},
		{Token: [16]byte{0: 3, 15: 2}, Choice: 2, AdmittedAt: cfg.EndsAt.Add(-time.Nanosecond)},
	}
	return cfg, votes
}

func TestFramePreservesFullTokenChoiceTimeAndOrder(t *testing.T) {
	cfg, votes := frameFixture()
	encoded, err := encodeFrame(cfg, votes)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != 80+28*len(votes) {
		t.Fatalf("unexpected packed frame length: %d", len(encoded))
	}
	original := bytes.Clone(encoded)
	seen := 0
	if err := decodeFrame(cfg, encoded, func(index uint32, got Vote) error {
		if index != uint32(seen) || got.Token != votes[seen].Token || got.Choice != votes[seen].Choice || !got.AdmittedAt.Equal(votes[seen].AdmittedAt) {
			t.Fatalf("packed vote changed at index %d", index)
		}
		seen++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if seen != len(votes) || !bytes.Equal(original, encoded) {
		t.Fatal("decoder skipped entries or mutated immutable input")
	}
}

func TestFrameRejectsMalformedWholeFrameBeforeVisiting(t *testing.T) {
	cfg, votes := frameFixture()
	encoded, err := encodeFrame(cfg, votes)
	if err != nil {
		t.Fatal(err)
	}
	mutations := []struct {
		name string
		edit func([]byte) []byte
	}{
		{"short header", func(b []byte) []byte { return b[:79] }},
		{"truncated entry", func(b []byte) []byte { return b[:len(b)-1] }},
		{"trailing byte", func(b []byte) []byte { return append(b, 0) }},
		{"future version", func(b []byte) []byte { b[0] = 3; return b }},
		{"packed BOOT", func(b []byte) []byte { b[1] = recordBoot; return b }},
		{"packed CLOSED", func(b []byte) []byte { b[1] = recordClosed; return b }},
		{"header choice", func(b []byte) []byte { b[7] = 1; return b }},
		{"header voter token", func(b []byte) []byte { b[24] = 1; return b }},
		{"header admission", func(b []byte) []byte { b[47] = 1; return b }},
		{"reserved header byte", func(b []byte) []byte { b[3] = 1; return b }},
		{"unknown multiple flag", func(b []byte) []byte { b[2] = 2; return b }},
		{"zero count", func(b []byte) []byte { clear(b[72:76]); return b }},
		{"overflow count", func(b []byte) []byte { binary.BigEndian.PutUint32(b[72:76], math.MaxUint32); return b }},
		{"count above limit", func(b []byte) []byte { binary.BigEndian.PutUint32(b[72:76], maxFrameVotes+1); return b }},
		{"count does not match length", func(b []byte) []byte { binary.BigEndian.PutUint32(b[72:76], 1); return b }},
		{"unknown entry width", func(b []byte) []byte { binary.BigEndian.PutUint32(b[76:80], 32); return b }},
		{"last token zero", func(b []byte) []byte { clear(b[108:124]); return b }},
		{"last choice zero", func(b []byte) []byte { clear(b[124:128]); return b }},
		{"last choice unknown", func(b []byte) []byte { binary.BigEndian.PutUint32(b[124:128], 8); return b }},
		{"last admission before start", func(b []byte) []byte {
			binary.BigEndian.PutUint64(b[128:136], uint64(cfg.StartsAt.Add(-time.Nanosecond).UnixNano()))
			return b
		}},
		{"last admission exactly closed", func(b []byte) []byte { binary.BigEndian.PutUint64(b[128:136], uint64(cfg.EndsAt.UnixNano())); return b }},
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			visited := 0
			err := decodeFrame(cfg, tc.edit(bytes.Clone(encoded)), func(uint32, Vote) error { visited++; return nil })
			if err == nil || visited != 0 {
				t.Fatalf("malformed frame was partly applied: err=%v visited=%d", err, visited)
			}
		})
	}
	for _, tc := range []struct {
		name string
		edit func(*Config)
	}{
		{"poll", func(c *Config) { c.PollID[0]++ }},
		{"route", func(c *Config) { c.Partitions++ }},
		{"start", func(c *Config) { c.StartsAt = c.StartsAt.Add(time.Nanosecond) }},
		{"end", func(c *Config) { c.EndsAt = c.EndsAt.Add(time.Nanosecond) }},
		{"choices", func(c *Config) { c.AllowedMask = 15 }},
		{"choice mode", func(c *Config) { c.Multiple = false }},
	} {
		t.Run("changed "+tc.name, func(t *testing.T) {
			changed := cfg
			tc.edit(&changed)
			visited := 0
			if err := decodeFrame(changed, encoded, func(uint32, Vote) error { visited++; return nil }); err == nil || visited != 0 {
				t.Fatal("frame accepted under a changed immutable poll configuration")
			}
		})
	}
}

func TestFrameEncoderBoundsAndVisitorError(t *testing.T) {
	cfg, votes := frameFixture()
	for _, count := range []int{0, maxFrameVotes + 1} {
		if _, err := encodeFrame(cfg, make([]Vote, count)); err == nil {
			t.Fatalf("encoder accepted %d votes", count)
		}
	}
	large := make([]Vote, maxFrameVotes)
	for i := range large {
		large[i] = votes[i%len(votes)]
	}
	encoded, err := encodeFrame(cfg, large)
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	if err := decodeFrame(cfg, encoded, func(uint32, Vote) error { seen++; return nil }); err != nil || seen != maxFrameVotes {
		t.Fatalf("maximum frame failed: err=%v visited=%d", err, seen)
	}
	expected := errors.New("visitor stopped")
	seen = 0
	if err := decodeFrame(cfg, encoded, func(uint32, Vote) error { seen++; return expected }); !errors.Is(err, expected) || seen != 1 {
		t.Fatal("visitor error was swallowed or later votes were visited")
	}
	invalid := votes[0]
	invalid.AdmittedAt = cfg.EndsAt
	if _, err := encodeFrame(cfg, []Vote{invalid}); err == nil {
		t.Fatal("encoder admitted a vote at CLOSED")
	}
	invalid = votes[0]
	invalid.Token = [16]byte{}
	if _, err := encodeFrame(cfg, []Vote{invalid}); err == nil {
		t.Fatal("encoder accepted a zero token")
	}
}

func TestFrameKeepsLegacyCodecSeparate(t *testing.T) {
	cfg, votes := frameFixture()
	legacy := encodeRecord(cfg, recordVote, votes[0])
	kind, got, err := decodeRecord(cfg, legacy)
	if err != nil || kind != recordVote || got.Token != votes[0].Token || got.Choice != votes[0].Choice {
		t.Fatal("v1 vote codec changed")
	}
	if err := decodeFrame(cfg, legacy, nil); err == nil {
		t.Fatal("v1 record was interpreted as a v2 frame")
	}
	packed, err := encodeFrame(cfg, votes)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := decodeRecord(cfg, packed); err == nil {
		t.Fatal("v2 frame was interpreted as a v1 record")
	}
	if (Position{Partition: 1, Offset: 9}) != (Position{Partition: 1, Offset: 9, Index: 0}) {
		t.Fatal("legacy position lost ordinal-zero compatibility")
	}
}

func TestWaitUntilRechecksWallClockAfterEveryTimerWake(t *testing.T) {
	base := time.Unix(1700000000, 0)
	calls := 0
	now := func() time.Time {
		current := base.Add(time.Duration(calls) * time.Millisecond)
		calls++
		return current
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := waitUntil(ctx, base.Add(2*time.Millisecond), now); err != nil || calls != 3 {
		t.Fatalf("timer wake sealed without confirming wall deadline: err=%v clock_reads=%d", err, calls)
	}
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	if err := waitUntil(ctx, base.Add(time.Hour), func() time.Time { return base }); !errors.Is(err, context.Canceled) {
		t.Fatal("deadline wait ignored cancellation")
	}
}

package votelog

import (
	"bytes"
	"testing"
	"time"
)

func codecConfig() Config {
	start := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	return Config{
		Brokers: []string{"127.0.0.1:19092"}, Topic: "gqlog_codec_0123456789abcdef",
		PollID: [16]byte{1, 2, 3}, Partitions: 16,
		StartsAt: start, EndsAt: start.Add(time.Minute), AllowedMask: 7,
		BatchSize: 64, Linger: 20 * time.Millisecond, QueuePerPartition: 128,
		TransactionTimeout: 10 * time.Second,
	}
}

func TestRecordRoundTripAndAdmissionBoundary(t *testing.T) {
	cfg := codecConfig()
	for _, tc := range []struct {
		name     string
		admitted time.Time
		valid    bool
	}{
		{"before start", cfg.StartsAt.Add(-time.Nanosecond), false},
		{"at start", cfg.StartsAt, true},
		{"last nanosecond", cfg.EndsAt.Add(-time.Nanosecond), true},
		{"exactly closed", cfg.EndsAt, false},
		{"after close", cfg.EndsAt.Add(time.Nanosecond), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vote := Vote{Token: [16]byte{4, 5, 6}, Choice: 2, AdmittedAt: tc.admitted}
			encoded := encodeRecord(cfg, recordVote, vote)
			if len(encoded) != 80 {
				t.Fatalf("record size changed from measured 80-byte value: %d", len(encoded))
			}
			kind, got, err := decodeRecord(cfg, encoded)
			if !tc.valid {
				if err == nil {
					t.Fatal("out-of-window admitted vote was accepted during replay")
				}
				return
			}
			if err != nil || kind != recordVote || got.Token != vote.Token || got.Choice != vote.Choice || !got.AdmittedAt.Equal(vote.AdmittedAt) {
				t.Fatalf("vote round trip failed: kind=%d vote=%+v err=%v", kind, got, err)
			}
		})
	}
	for _, kind := range []byte{recordBoot, recordClosed} {
		encoded := encodeRecord(cfg, kind, Vote{})
		gotKind, vote, err := decodeRecord(cfg, encoded)
		if err != nil || gotKind != kind || vote != (Vote{}) {
			t.Fatalf("control record round trip failed: kind=%d vote=%+v err=%v", gotKind, vote, err)
		}
	}
}

func TestRecordRejectsMalformedDataAndPollRedefinition(t *testing.T) {
	cfg := codecConfig()
	vote := Vote{Token: [16]byte{9}, Choice: 1, AdmittedAt: cfg.StartsAt.Add(time.Second)}
	encoded := encodeRecord(cfg, recordVote, vote)
	mutations := []struct {
		name string
		edit func([]byte) []byte
	}{
		{"truncated", func(b []byte) []byte { return b[:len(b)-1] }},
		{"trailing data", func(b []byte) []byte { return append(b, 0) }},
		{"future format", func(b []byte) []byte { b[0] = 2; return b }},
		{"unknown kind", func(b []byte) []byte { b[1] = 255; return b }},
		{"unknown boolean", func(b []byte) []byte { b[2] = 2; return b }},
		{"reserved header", func(b []byte) []byte { b[3] = 1; return b }},
		{"reserved tail", func(b []byte) []byte { b[79] = 1; return b }},
		{"zero token", func(b []byte) []byte { clear(b[24:40]); return b }},
		{"zero choice", func(b []byte) []byte { clear(b[4:8]); return b }},
		{"multi choice in single poll", func(b []byte) []byte { b[7] = 3; return b }},
		{"unknown choice", func(b []byte) []byte { b[7] = 8; return b }},
		{"control with voter state", func(b []byte) []byte { b[1] = recordClosed; return b }},
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := decodeRecord(cfg, tc.edit(bytes.Clone(encoded))); err == nil {
				t.Fatal("malformed journal record was accepted")
			}
		})
	}
	for _, tc := range []struct {
		name string
		edit func(*Config)
	}{
		{"poll id", func(c *Config) { c.PollID[0]++ }},
		{"partition map", func(c *Config) { c.Partitions++ }},
		{"allowed choices", func(c *Config) { c.AllowedMask = 15 }},
		{"choice mode", func(c *Config) { c.Multiple = true }},
		{"start", func(c *Config) { c.StartsAt = c.StartsAt.Add(time.Nanosecond) }},
		{"end", func(c *Config) { c.EndsAt = c.EndsAt.Add(time.Nanosecond) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := cfg
			tc.edit(&changed)
			if _, _, err := decodeRecord(changed, encoded); err == nil {
				t.Fatal("record was accepted under changed immutable poll definition")
			}
		})
	}
	// Throughput settings are not part of the immutable vote interpretation.
	changed := cfg
	changed.BatchSize *= 2
	changed.Linger *= 2
	if _, _, err := decodeRecord(changed, encoded); err != nil {
		t.Fatalf("operational tuning invalidated existing records: %v", err)
	}
}

func TestMultipleChoiceKeepsExactMask(t *testing.T) {
	cfg := codecConfig()
	cfg.Multiple = true
	vote := Vote{Token: [16]byte{7}, Choice: 5, AdmittedAt: cfg.StartsAt}
	_, got, err := decodeRecord(cfg, encodeRecord(cfg, recordVote, vote))
	if err != nil || got.Choice != 5 {
		t.Fatalf("multi-choice mask changed: choice=%d err=%v", got.Choice, err)
	}
}

func TestPrototypeRejectsExternalTargetsAndUnboundedConfiguration(t *testing.T) {
	base := codecConfig()
	if err := base.validate(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		edit func(*Config)
	}{
		{"external broker", func(c *Config) { c.Brokers = []string{"192.0.2.1:9092"} }},
		{"unowned topic", func(c *Config) { c.Topic = "production_votes" }},
		{"shortened vote window", func(c *Config) { c.EndsAt = c.StartsAt.Add(55 * time.Second) }},
		{"unbounded queue", func(c *Config) { c.QueuePerPartition = 1 << 30 }},
		{"unbounded transaction", func(c *Config) { c.TransactionTimeout = time.Hour }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := base
			tc.edit(&changed)
			if err := changed.validate(); err == nil {
				t.Fatal("unsafe local benchmark configuration was accepted")
			}
		})
	}
}

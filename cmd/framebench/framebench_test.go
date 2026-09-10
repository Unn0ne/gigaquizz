package main

import (
	"context"
	"crypto/aes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gigaquizz/internal/votelog"
)

func TestBoundsIncludeRepeatsAndFrameMetadata(t *testing.T) {
	c := defaults()
	c.allowLarge = true
	c.rate = 1800000
	if err := c.validate(); err != nil || c.attempts() != 108000000 {
		t.Fatalf("maximum unique rate invalid:%v", err)
	}
	c.repeatEvery = 5
	if err := c.validate(); err == nil {
		t.Fatal("20percent repeats escaped120M bound")
	}
	c.repeatEvery = 0
	c.frameSize = 1
	if err := c.validate(); err == nil {
		t.Fatal("unbounded frame metadata accepted")
	}
	c = defaults()
	c.rate = 1
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"localhost:19092", "192.0.2.1:19092", "127.0.0.1:0"} {
		if _, err := localBrokers(raw); err == nil {
			t.Fatal("nonlocal/invalid broker accepted")
		}
	}
}

func TestCancelledWorkloadPreservesPlannedDenominator(t *testing.T) {
	c := defaults()
	c.rate = 1
	c.repeatEvery = 5
	c.workers = 2
	c.partitions = 1
	c.frameSize = 4
	start := time.Now()
	cfg := votelog.Config{Partitions: 1, StartsAt: start, EndsAt: start.Add(time.Minute)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w, acks := runWorkload(ctx, c, cfg, [16]byte{}, states{make([]byte, 60), make([]byte, 60)}, func(context.Context, []votelog.Input) (votelog.FrameReceipt, error) {
		t.Error("cancelled frame dispatched")
		return votelog.FrameReceipt{}, nil
	})
	if w.Counts.Planned != 72 || w.Counts.Skipped != 72 || w.Counts.Dispatched != 0 || len(acks) != 0 {
		t.Fatalf("cancelled plan lost:%+v", w.Counts)
	}
	var buckets uint64
	for _, b := range w.Buckets {
		buckets += b.Skipped
	}
	if buckets != 72 {
		t.Fatal("per-second planned skips differ")
	}
}

func TestMalformedSuccessfulReceiptIsExplicitContractFailure(t *testing.T) {
	c := defaults()
	c.rate = 1
	c.workers = 1
	c.partitions = 1
	c.frameSize = 4
	start := time.Now().Add(-59 * time.Second)
	cfg := votelog.Config{Partitions: 1, StartsAt: start, EndsAt: start.Add(time.Minute)}
	w, _ := runWorkload(context.Background(), c, cfg, [16]byte{}, states{Original: make([]byte, 60)}, func(context.Context, []votelog.Input) (votelog.FrameReceipt, error) {
		return votelog.FrameReceipt{Partition: 0, Count: 99, Offset: 10, AdmittedAt: time.Now()}, nil
	})
	if w.Counts.InvalidReceipts != 1 || w.Counts.Unknown != 1 || w.Counts.Recorded != 0 || w.Counts.Skipped != 59 {
		t.Fatalf("malformed receipt hidden:%+v", w.Counts)
	}
}

func auditFixture() (manifest, states, []ackFrame, []votelog.Vote) {
	start := time.Unix(1700000000, 0).UTC()
	m := manifest{Version: 1, Rate: 1, Keys: 60, RepeatEvery: 1, FrameSize: 4096, FrameAge: 20 * time.Millisecond, Config: votelog.Config{Topic: "gqlog_framebench_0123456789abcdef", Partitions: 1, StartsAt: start, EndsAt: start.Add(time.Minute)}}
	m.Seed[0] = 7
	block, _ := aes.NewCipher(m.Seed[:])
	var counter, token [16]byte
	binary.BigEndian.PutUint64(counter[8:], 1)
	block.Encrypt(token[:], counter[:])
	inputs := []votelog.Input{{Token: token, Choice: 1}, {Token: token, Choice: 2}}
	s := states{make([]byte, 60), make([]byte, 60)}
	s.Original[0] = stateACK
	s.Repeat[0] = stateACK
	admit := start.Add(time.Second)
	acks := []ackFrame{{Partition: 0, Offset: 10, Count: 2, AdmittedNS: admit.UnixNano(), Digest: digest(inputs)}}
	votes := []votelog.Vote{{Token: token, Choice: 1, AdmittedAt: admit}, {Token: token, Choice: 2, AdmittedAt: admit}}
	return m, s, acks, votes
}

func fakeReplay(votes []votelog.Vote, indices []uint32) replayer {
	return func(ctx context.Context, c votelog.Config, visit func(votelog.Position, votelog.Vote) error) (votelog.Manifest, error) {
		for i, v := range votes {
			index := uint32(i)
			if indices != nil {
				index = indices[i]
			}
			if err := visit(votelog.Position{Partition: 0, Offset: 10, Index: index}, v); err != nil {
				return votelog.Manifest{}, err
			}
		}
		return votelog.Manifest{Partitions: []votelog.PartitionEnd{{Partition: 0, Offset: 11}}}, nil
	}
}

func TestStreamingAuditFullKeysExactCanonicalAndReceipt(t *testing.T) {
	m, s, acks, votes := auditFixture()
	r, err := reconcile(context.Background(), m, s, acks, fakeReplay(votes, nil))
	if err != nil || !r.Correct || r.RecordedAttempts != 2 || r.CanonicalKeys != 1 || r.Duplicates != 1 || r.Choices[0] != 1 || r.MatchedACKFrames != 1 {
		t.Fatalf("valid exact audit failed:%+v err%v", r, err)
	}
	// Concurrently delivered conflicting attempts can make choice2 canonical;
	// receipt order protects the actual committed order rather than choice1 bias.
	votes[0], votes[1] = votes[1], votes[0]
	acks[0].Digest = digest([]votelog.Input{{Token: votes[0].Token, Choice: 2}, {Token: votes[1].Token, Choice: 1}})
	r, err = reconcile(context.Background(), m, s, acks, fakeReplay(votes, nil))
	if err != nil || !r.Correct || r.Choices[1] != 1 {
		t.Fatal("canonical first committed choice was replaced")
	}
}

func TestAuditMissingWrongIndexWrongDigestAndUnexpectedPhysicalCopy(t *testing.T) {
	for _, kind := range []string{"missing", "index", "digest", "choice", "physical"} {
		t.Run(kind, func(t *testing.T) {
			m, s, acks, votes := auditFixture()
			var indices []uint32
			switch kind {
			case "missing":
				votes = nil
			case "index":
				indices = []uint32{0, 2}
			case "digest":
				acks[0].Digest[0] ^= 1
			case "choice":
				votes[0].Choice = 4
			case "physical":
				votes[1].Choice = 1
			}
			r, err := reconcile(context.Background(), m, s, acks, fakeReplay(votes, indices))
			if err == nil || r.Correct {
				t.Fatalf("%s corruption escaped audit", kind)
			}
		})
	}
}

func TestUnknownCommittedAttemptsAreResolvedWithoutInventingACKs(t *testing.T) {
	m, s, _, votes := auditFixture()
	s.Original[0], s.Repeat[0] = stateUnknown, stateUnknown
	r, err := reconcile(context.Background(), m, s, nil, fakeReplay(votes, nil))
	if err != nil || !r.Correct || r.UnknownResolved != 2 || r.ConfirmedAttempts != 0 || r.CanonicalKeys != 1 {
		t.Fatalf("unknown recovery misclassified:%+v err%v", r, err)
	}
	s.Original[0] = stateRejected
	if r, err := reconcile(context.Background(), m, s, nil, fakeReplay(votes, nil)); err == nil || r.Correct {
		t.Fatal("definitively rejected attempt appeared without detection")
	}
}

func TestAESInverseRejectsCounterOutsideOwnedRange(t *testing.T) {
	var seed, source, token, plain [16]byte
	block, _ := aes.NewCipher(seed[:])
	binary.BigEndian.PutUint64(source[8:], 60)
	block.Encrypt(token[:], source[:])
	if key, err := decodeKey(block, token[:], 60, plain[:]); err != nil || key != 59 {
		t.Fatal("AES counter round trip failed")
	}
	if _, err := decodeKey(block, token[:], 59, plain[:]); err == nil {
		t.Fatal("out-of-range counter accepted")
	}
	source[0] = 1
	block.Encrypt(token[:], source[:])
	if _, err := decodeKey(block, token[:], 60, plain[:]); err == nil {
		t.Fatal("nonzero upper counter accepted")
	}
}

func TestPrivateLedgerChecksumsAndStateBounds(t *testing.T) {
	m, s, acks, _ := auditFixture()
	dir := t.TempDir()
	m.ACKCount = uint64(len(acks))
	m.Files = map[string]string{}
	m.Hashes = map[string]string{}
	ab := make([]byte, 64)
	encodeACK(acks[0], ab)
	for name, b := range map[string][]byte{"original": s.Original, "repeat": s.Repeat, "acks": ab} {
		file := m.Config.Topic + "." + name
		m.Files[name] = file
		sum := sha256.Sum256(b)
		m.Hashes[name] = hex.EncodeToString(sum[:])
		if err := os.WriteFile(filepath.Join(dir, file), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(dir, m.Config.Topic+".json")
	b, _ := json.Marshal(m)
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	_, restored, ra, err := readLedger(path)
	if err != nil || restored.Original[0] != stateACK || len(ra) != 1 || ra[0] != acks[0] {
		t.Fatalf("private ledger roundtrip failed:%v", err)
	}
	s.Original[0] = stateUnknown
	if err := os.WriteFile(filepath.Join(dir, m.Files["original"]), s.Original, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := readLedger(path); err == nil {
		t.Fatal("modified private state escaped checksum")
	}
}

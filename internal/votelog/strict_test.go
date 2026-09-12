package votelog

import (
	"context"
	"errors"
	"hash/crc32"
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Real Kafka wire batches exercise the SDK's CRC/transaction/cursor parser;
// a fabricated visible Record slice would not test hidden control offsets.
func strictWireBatch(offset, producer int64, control bool, key, value []byte) []byte {
	record := kmsg.Record{Key: key, Value: value}
	record.Length = int32(len(record.AppendTo(nil)) - 1)
	batch := kmsg.RecordBatch{FirstOffset: offset, Magic: 2, Attributes: 0x10,
		PartitionLeaderEpoch: 1, ProducerID: producer, NumRecords: 1,
		Records: record.AppendTo(nil)}
	if control {
		batch.Attributes |= 0x20
	}
	encoded := batch.AppendTo(nil)
	batch.Length = int32(len(encoded) - 12)
	batch.CRC = int32(crc32.Checksum(encoded[21:], crc32.MakeTable(crc32.Castagnoli)))
	return batch.AppendTo(nil)
}

func strictWireMarker(offset, producer int64, abort bool) []byte {
	key := []byte{0, 0, 0, 1}
	if abort {
		key[3] = 0
	}
	return strictWireBatch(offset, producer, true, key, make([]byte, 6))
}

func strictRawPage(end int64, batches ...[]byte) *kmsg.FetchResponseTopicPartition {
	page := kmsg.NewFetchResponseTopicPartition()
	page.Partition, page.LogStartOffset, page.HighWatermark, page.LastStableOffset = 0, 0, end, end
	for _, batch := range batches {
		page.RecordBatches = append(page.RecordBatches, batch...)
	}
	return &page
}

type strictFixtureSource struct {
	snapshots []strictSnapshot
	pages     map[int64]*kmsg.FetchResponseTopicPartition
	calls     int
	fetched   []int64
}

func (s *strictFixtureSource) snapshot(context.Context, Config) (strictSnapshot, error) {
	i := min(s.calls, len(s.snapshots)-1)
	s.calls++
	return s.snapshots[i], nil
}

func (s *strictFixtureSource) fetch(_ context.Context, _ Config, _ strictSnapshot, _ int32, offset int64) (*kmsg.FetchResponseTopicPartition, error) {
	s.fetched = append(s.fetched, offset)
	page := s.pages[offset]
	if page == nil {
		return nil, errors.New("unexpected fixture fetch offset")
	}
	return page, nil
}

func strictFixture(t *testing.T) (Config, []Vote, *strictFixtureSource) {
	t.Helper()
	cfg, votes := frameFixture()
	cfg.Partitions = 1
	frame, err := encodeFrame(cfg, votes)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := strictSnapshot{topicID: [16]byte{1}, partitions: []strictPartition{{leader: 1, epoch: 1, end: 10}}}
	source := &strictFixtureSource{snapshots: []strictSnapshot{snapshot}, pages: map[int64]*kmsg.FetchResponseTopicPartition{
		0: strictRawPage(10,
			strictWireBatch(0, 1, false, nil, encodeRecord(cfg, recordBoot, Vote{})), strictWireMarker(1, 1, false),
			strictWireBatch(2, 1, false, nil, frame), strictWireMarker(3, 1, false)),
		4: strictRawPage(10,
			strictWireBatch(4, 1, false, nil, encodeRecord(cfg, recordClosed, Vote{})), strictWireMarker(5, 1, false),
			strictWireBatch(6, 2, false, nil, encodeRecord(cfg, recordBoot, Vote{})), strictWireMarker(7, 2, false)),
		8: strictRawPage(10,
			strictWireBatch(8, 3, false, nil, frame), strictWireMarker(9, 3, true)),
	}}
	source.pages[8].AbortedTransactions = []kmsg.FetchResponseTopicPartitionAbortedTransaction{{ProducerID: 3, FirstOffset: 8}}
	return cfg, votes, source
}

func TestStrictReplayConsumesHiddenTailAndValidatesRecoveryBoot(t *testing.T) {
	cfg, votes, source := strictFixture(t)
	var got []Vote
	var positions []Position
	result, err := replayStrict(context.Background(), cfg, func(position Position, vote Vote) error {
		positions = append(positions, position)
		got = append(got, vote)
		return nil
	}, source)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, votes) || !reflect.DeepEqual(positions, []Position{{Offset: 2}, {Offset: 2, Index: 1}}) {
		t.Fatal("strict replay changed full votes, admission times or exact frame indices")
	}
	if !reflect.DeepEqual(source.fetched, []int64{0, 4, 8}) || source.calls != 2 {
		t.Fatal("strict replay did not read the raw aborted tail and recheck the snapshot")
	}
	if result.RecoveryBootRecords != 1 || result.Manifest.Partitions[0].Offset != 4 || result.SnapshotEndOffsets[0].Offset != 10 {
		t.Fatalf("strict boundaries or recovery count changed: %+v", result)
	}
}

func TestStrictReplayRejectsCommittedPostClosedDataAndInvalidControls(t *testing.T) {
	for _, kind := range []string{"vote", "frame", "duplicate_closed", "changed_boot", "keyed_boot"} {
		t.Run(kind, func(t *testing.T) {
			cfg, votes, source := strictFixture(t)
			var key, value []byte
			switch kind {
			case "vote":
				key = votes[0].Token[:]
				value = encodeRecord(cfg, recordVote, votes[0])
			case "frame":
				value, _ = encodeFrame(cfg, votes)
			case "duplicate_closed":
				value = encodeRecord(cfg, recordClosed, Vote{})
			case "changed_boot":
				changed := cfg
				changed.AllowedMask = 15
				value = encodeRecord(changed, recordBoot, Vote{})
			case "keyed_boot":
				key, value = votes[0].Token[:], encodeRecord(cfg, recordBoot, Vote{})
			}
			source.pages[4] = strictRawPage(10,
				strictWireBatch(4, 1, false, nil, encodeRecord(cfg, recordClosed, Vote{})), strictWireMarker(5, 1, false),
				strictWireBatch(6, 2, false, key, value), strictWireMarker(7, 2, false))
			result, err := replayStrict(context.Background(), cfg, nil, source)
			if err == nil || len(result.Manifest.Partitions) != 0 || len(result.SnapshotEndOffsets) != 0 {
				t.Fatal("committed tail produced a successful or partial strict result")
			}
		})
	}
}

func TestStrictReplayNeverInfersRawCompletionFromWatermarksOrEmptyFetch(t *testing.T) {
	for _, kind := range []string{"empty", "truncated", "open_transaction", "expired", "corrupt_crc"} {
		t.Run(kind, func(t *testing.T) {
			cfg, _, source := strictFixture(t)
			tail := source.pages[8]
			switch kind {
			case "empty":
				tail.RecordBatches = nil
			case "truncated":
				tail.RecordBatches = tail.RecordBatches[:20]
			case "open_transaction":
				tail.LastStableOffset = 8
			case "expired":
				tail.LogStartOffset = 1
			case "corrupt_crc":
				tail.RecordBatches[len(tail.RecordBatches)-1] ^= 1
			}
			result, err := replayStrict(context.Background(), cfg, nil, source)
			if err == nil || len(result.Manifest.Partitions) != 0 {
				t.Fatal("incomplete raw tail was mistaken for a complete CLOSED snapshot")
			}
		})
	}
}

func TestStrictReplayRequiresUnchangedFinalSnapshotAndCompleteInventory(t *testing.T) {
	for _, kind := range []string{"new_end", "new_topic_id", "new_leader", "missing_partition"} {
		t.Run(kind, func(t *testing.T) {
			cfg, _, source := strictFixture(t)
			changed := strictSnapshot{topicID: source.snapshots[0].topicID,
				partitions: append([]strictPartition(nil), source.snapshots[0].partitions...)}
			switch kind {
			case "new_end":
				changed.partitions[0].end++
			case "new_topic_id":
				changed.topicID[1]++
			case "new_leader":
				changed.partitions[0].epoch++
			case "missing_partition":
				changed.partitions = nil
			}
			source.snapshots = append(source.snapshots, changed)
			result, err := replayStrict(context.Background(), cfg, nil, source)
			if err == nil || len(result.Manifest.Partitions) != 0 {
				t.Fatal("a changing snapshot returned a successful or partial result")
			}
		})
	}
}

func TestStrictSnapshotRejectsOpenTransactionsAndMovingOrExpiredEnd(t *testing.T) {
	partition := strictPartition{leader: 1, epoch: 1}
	start := kadm.ListedOffset{Offset: 0}
	end := kadm.ListedOffset{Offset: 10, LeaderEpoch: 1}
	if got, err := strictStablePartition(partition, start, end, end, end); err != nil || got.end != 10 {
		t.Fatal("stable complete end rejected")
	}
	for _, which := range []string{"open", "moving", "expired", "epoch", "broker_error"} {
		t.Run(which, func(t *testing.T) {
			a, b, c, d := start, end, end, end
			switch which {
			case "open":
				c.Offset = 8
			case "moving":
				d.Offset = 11
			case "expired":
				a.Offset = 1
			case "epoch":
				c.LeaderEpoch++
			case "broker_error":
				c.Err = errors.New("injected ListOffsets failure")
			}
			if _, err := strictStablePartition(partition, a, b, c, d); err == nil {
				t.Fatal("incomplete or unstable ListOffsets snapshot accepted")
			}
		})
	}
}

func TestStrictReplayPropagatesVisitorErrorAndRequiresClosed(t *testing.T) {
	cfg, _, source := strictFixture(t)
	want := errors.New("independent ledger mismatch")
	if _, err := replayStrict(context.Background(), cfg, func(Position, Vote) error { return want }, source); !errors.Is(err, want) {
		t.Fatalf("ledger mismatch hidden: %v", err)
	}
	cfg, _, source = strictFixture(t)
	source.pages[4] = strictRawPage(10,
		strictWireBatch(4, 1, false, nil, encodeRecord(cfg, recordBoot, Vote{})), strictWireMarker(5, 1, false),
		strictWireBatch(6, 2, false, nil, encodeRecord(cfg, recordBoot, Vote{})), strictWireMarker(7, 2, false))
	if _, err := replayStrict(context.Background(), cfg, nil, source); err == nil {
		t.Fatal("stable end without CLOSED accepted")
	}
}

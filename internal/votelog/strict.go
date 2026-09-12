package votelog

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// StrictReplayResult describes a successfully scanned, stable journal snapshot.
// End offsets are exclusive high watermarks, including Kafka transaction
// markers and aborted batches. They are not counts of committed votes.
type StrictReplayResult struct {
	Manifest            Manifest       `json:"manifest"`
	SnapshotEndOffsets  []PartitionEnd `json:"snapshot_end_offsets"`
	RecoveryBootRecords uint64         `json:"recovery_boot_records"`
}

type strictPartition struct {
	leader, epoch int32
	end           int64
}

type strictSnapshot struct {
	topicID    [16]byte
	partitions []strictPartition
}

type strictSource interface {
	snapshot(context.Context, Config) (strictSnapshot, error)
	fetch(context.Context, Config, strictSnapshot, int32, int64) (*kmsg.FetchResponseTopicPartition, error)
}

// ReplayStrict is the independent audit reader. Production finalization and
// recovery continue to use Replay's CLOSED-prefix contract.
//
// This reader requires a quiescent, non-compacted journal. It captures each
// partition's HW/LSO, requires equality (an open transaction is incomplete),
// reads every raw Kafka offset through that end, and rechecks the snapshot.
// Votes/frames and duplicate CLOSED after CLOSED fail. Valid recovery BOOTs
// are fully decoded against the immutable config and counted separately.
//
// The consumer's decoded raw next offset advances over control and aborted
// batches too: an empty visible fetch or LSO alone never proves completion.
// Callers publish only after success. A timeout, moving end, expired prefix,
// open transaction or later validation error invalidates all partial visits.
// It proves this bounded snapshot, not absence of future writes.
func ReplayStrict(parent context.Context, c Config, visit func(Position, Vote) error) (StrictReplayResult, error) {
	if err := c.validate(); err != nil {
		return StrictReplayResult{}, err
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Minute)
	defer cancel()
	client, err := kgo.NewClient(kgo.SeedBrokers(c.Brokers...),
		kgo.DialTimeout(3*time.Second), kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.FetchMaxBytes(4<<20), kgo.FetchMaxPartitionBytes(4<<20),
		kgo.BrokerMaxReadBytes(8<<20))
	if err != nil {
		return StrictReplayResult{}, err
	}
	defer client.Close()
	source := &strictKafkaSource{client: client, admin: kadm.NewClient(client)}
	return replayStrict(ctx, c, visit, source)
}

func replayStrict(ctx context.Context, c Config, visit func(Position, Vote) error, source strictSource) (StrictReplayResult, error) {
	initial, err := source.snapshot(ctx, c)
	if err != nil {
		return StrictReplayResult{}, err
	}
	if initial.topicID == [16]byte{} || len(initial.partitions) != c.Partitions {
		return StrictReplayResult{}, errors.New("strict journal snapshot lacks complete topic identity")
	}
	result := StrictReplayResult{Manifest: Manifest{Topic: c.Topic, StartsAt: c.StartsAt, EndsAt: c.EndsAt}}
	var attempts, records uint64
	for partition, snapshot := range initial.partitions {
		if snapshot.end <= 0 || snapshot.leader < 0 || snapshot.epoch < 0 {
			return StrictReplayResult{}, errors.New("strict journal snapshot has invalid partition bounds")
		}
		state := strictPartitionState{cfg: c, partition: int32(partition), closed: -1, lastRecord: -1, visit: visit, attempts: &attempts}
		for offset := int64(0); offset < snapshot.end; {
			if err := ctx.Err(); err != nil {
				return StrictReplayResult{}, err
			}
			raw, err := source.fetch(ctx, c, initial, int32(partition), offset)
			if err != nil {
				return StrictReplayResult{}, err
			}
			if raw == nil || raw.Partition != int32(partition) || raw.LogStartOffset != 0 || raw.HighWatermark != snapshot.end || raw.LastStableOffset != snapshot.end {
				return StrictReplayResult{}, errors.New("strict journal fetch differs from stable unexpired snapshot")
			}
			if err := kerr.ErrorForCode(raw.ErrorCode); err != nil {
				return StrictReplayResult{}, err
			}
			if err := strictBatchBounds(raw.RecordBatches); err != nil {
				return StrictReplayResult{}, err
			}
			fetched, next := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{
				Offset: offset, IsolationLevel: kgo.ReadCommitted(), Topic: c.Topic, Partition: int32(partition),
			}, raw, strictDecompressor{}, nil)
			if fetched.Err != nil {
				return StrictReplayResult{}, fetched.Err
			}
			if next <= offset || next > snapshot.end {
				return StrictReplayResult{}, errors.New("strict journal raw cursor did not advance within captured end")
			}
			for _, record := range fetched.Records {
				if record.Offset < offset || record.Offset >= next || records >= 240_000_000 {
					return StrictReplayResult{}, errors.New("strict journal record offset or count exceeds bound")
				}
				records++
				if err := state.accept(record); err != nil {
					return StrictReplayResult{}, err
				}
			}
			offset = next
		}
		if state.closed < 0 {
			return StrictReplayResult{}, errors.New("strict journal snapshot lacks CLOSED")
		}
		result.Manifest.Partitions = append(result.Manifest.Partitions, PartitionEnd{Partition: int32(partition), Offset: state.closed})
		result.SnapshotEndOffsets = append(result.SnapshotEndOffsets, PartitionEnd{Partition: int32(partition), Offset: snapshot.end})
		result.RecoveryBootRecords += state.recoveryBoots
	}
	final, err := source.snapshot(ctx, c)
	if err != nil {
		return StrictReplayResult{}, err
	}
	if final.topicID != initial.topicID || len(final.partitions) != len(initial.partitions) {
		return StrictReplayResult{}, errors.New("strict journal topic identity or inventory changed during audit")
	}
	for p := range initial.partitions {
		if final.partitions[p] != initial.partitions[p] {
			return StrictReplayResult{}, errors.New("strict journal end or leader changed during audit")
		}
	}
	if err := ctx.Err(); err != nil {
		return StrictReplayResult{}, err
	}
	return result, nil
}

type strictPartitionState struct {
	cfg           Config
	partition     int32
	seenBoot      bool
	closed        int64
	lastRecord    int64
	recoveryBoots uint64
	attempts      *uint64
	visit         func(Position, Vote) error
}

func (s *strictPartitionState) accept(record *kgo.Record) error {
	if record.Topic != s.cfg.Topic || record.Partition != s.partition || !record.Attrs.IsTransactional() || record.Attrs.IsControl() || record.Offset <= s.lastRecord {
		return errors.New("strict journal record routing, transaction or ordering mismatch")
	}
	s.lastRecord = record.Offset
	visit := func(index uint32, vote Vote) error {
		if s.cfg.Partition(vote.Token) != s.partition || *s.attempts >= 240_000_000 {
			return errors.New("strict journal vote routing or attempt count exceeds bound")
		}
		(*s.attempts)++
		if s.visit != nil {
			return s.visit(Position{Partition: s.partition, Offset: record.Offset, Index: index}, vote)
		}
		return nil
	}
	if len(record.Value) > 0 && record.Value[0] == frameVersion {
		if !s.seenBoot || s.closed >= 0 || len(record.Key) != 0 {
			return errors.New("strict journal frame outside open partition")
		}
		return decodeFrame(s.cfg, record.Value, visit)
	}
	kind, vote, err := decodeRecord(s.cfg, record.Value)
	if err != nil {
		return err
	}
	if !s.seenBoot && kind != recordBoot {
		return errors.New("strict journal lacks initial BOOT")
	}
	switch kind {
	case recordBoot:
		if len(record.Key) != 0 {
			return errors.New("strict journal BOOT carries a key")
		}
		s.seenBoot = true
		if s.closed >= 0 {
			s.recoveryBoots++
		}
	case recordVote:
		if s.closed >= 0 || string(record.Key) != string(vote.Token[:]) {
			return errors.New("strict journal vote after CLOSED or key mismatch")
		}
		return visit(0, vote)
	case recordClosed:
		if s.closed >= 0 || len(record.Key) != 0 {
			return errors.New("strict journal duplicate or keyed CLOSED")
		}
		s.closed = record.Offset
	}
	return nil
}

// Writers have always selected NoCompression and a 1MiB Kafka batch limit.
// Bound parsing allocations before asking the SDK to allocate record structs.
func strictBatchBounds(raw []byte) error {
	if len(raw) > 8<<20 {
		return errors.New("strict journal fetch exceeds byte bound")
	}
	for len(raw) >= 12 {
		length := int64(int32(binary.BigEndian.Uint32(raw[8:12]))) + 12
		if length < 61 {
			return errors.New("strict journal requires modern complete Kafka batch headers")
		}
		if length > int64(len(raw)) {
			break // SDK leaves a truncated final batch unconsumed.
		}
		batch := raw[:int(length)]
		count := int32(binary.BigEndian.Uint32(batch[57:61]))
		if batch[16] != 2 || binary.BigEndian.Uint16(batch[21:23])&7 != 0 || count < 0 || count > 16384 || int(count) > len(batch)-61 {
			return errors.New("strict journal batch format, compression or record count exceeds bound")
		}
		raw = raw[int(length):]
	}
	return nil
}

type strictDecompressor struct{}

func (strictDecompressor) Decompress(source []byte, codec kgo.CompressionCodecType) ([]byte, error) {
	if codec == kgo.CodecNone {
		return source, nil
	}
	return nil, errors.New("strict journal audit only supports the writer's uncompressed batches")
}

type strictKafkaSource struct {
	client *kgo.Client
	admin  *kadm.Client
}

func (s *strictKafkaSource) snapshot(ctx context.Context, c Config) (strictSnapshot, error) {
	// Issue real metadata requests: kadm's ordinary metadata helper may reuse
	// a five-second cache, which cannot prove a topic was not recreated.
	req := kmsg.NewPtrMetadataRequest()
	req.AllowAutoTopicCreation = false
	req.Topics = []kmsg.MetadataRequestTopic{{Topic: kmsg.StringPtr(c.Topic)}}
	metadata, err := req.RequestWith(ctx, s.client)
	if err != nil {
		return strictSnapshot{}, err
	}
	if len(metadata.Topics) != 1 {
		return strictSnapshot{}, errors.New("strict journal topic inventory is incomplete")
	}
	topic := metadata.Topics[0]
	if topic.Topic == nil || *topic.Topic != c.Topic || topic.ErrorCode != 0 || topic.TopicID == [16]byte{} || len(topic.Partitions) != c.Partitions {
		return strictSnapshot{}, errors.New("strict journal topic metadata is incomplete")
	}
	policies, err := s.admin.DescribeTopicConfigs(ctx, c.Topic)
	if err != nil {
		return strictSnapshot{}, err
	}
	deleteOnly := false
	for _, resource := range policies {
		if resource.Err != nil {
			return strictSnapshot{}, resource.Err
		}
		for _, setting := range resource.Configs {
			if setting.Key == "cleanup.policy" && setting.Value != nil && *setting.Value == "delete" {
				deleteOnly = true
			}
		}
	}
	if !deleteOnly || len(policies) != 1 {
		return strictSnapshot{}, errors.New("strict journal requires delete-only, non-compacted topic retention")
	}
	starts, err := s.admin.ListStartOffsets(ctx, c.Topic)
	if err != nil {
		return strictSnapshot{}, err
	}
	before, err := s.admin.ListEndOffsets(ctx, c.Topic)
	if err != nil {
		return strictSnapshot{}, err
	}
	stable, err := s.admin.ListCommittedOffsets(ctx, c.Topic)
	if err != nil {
		return strictSnapshot{}, err
	}
	after, err := s.admin.ListEndOffsets(ctx, c.Topic)
	if err != nil {
		return strictSnapshot{}, err
	}
	result := strictSnapshot{topicID: topic.TopicID, partitions: make([]strictPartition, c.Partitions)}
	seen := make([]bool, c.Partitions)
	for _, partition := range topic.Partitions {
		p := partition.Partition
		if p < 0 || int(p) >= c.Partitions || seen[p] || partition.ErrorCode != 0 || partition.Leader < 0 || partition.LeaderEpoch < 0 {
			return strictSnapshot{}, errors.New("strict journal partition metadata is invalid")
		}
		seen[p] = true
		result.partitions[p] = strictPartition{leader: partition.Leader, epoch: partition.LeaderEpoch}
	}
	for _, offsets := range []kadm.ListedOffsets{starts, before, stable, after} {
		if len(offsets) != 1 || len(offsets[c.Topic]) != c.Partitions || offsets.Error() != nil {
			return strictSnapshot{}, errors.New("strict journal offset inventory is incomplete")
		}
	}
	for i := 0; i < c.Partitions; i++ {
		p := int32(i)
		a, aok := before[c.Topic][p]
		lso, lok := stable[c.Topic][p]
		b, bok := after[c.Topic][p]
		start, sok := starts[c.Topic][p]
		if !aok || !lok || !bok || !sok {
			return strictSnapshot{}, errors.New("strict journal snapshot partition is missing")
		}
		result.partitions[i], err = strictStablePartition(result.partitions[i], start, a, lso, b)
		if err != nil {
			return strictSnapshot{}, err
		}
	}
	return result, nil
}

func strictStablePartition(partition strictPartition, start, before, stable, after kadm.ListedOffset) (strictPartition, error) {
	if start.Err != nil || before.Err != nil || stable.Err != nil || after.Err != nil || start.Offset != 0 ||
		before.Offset <= 0 || before.Offset != stable.Offset || before.Offset != after.Offset ||
		before.LeaderEpoch != partition.epoch || stable.LeaderEpoch != partition.epoch || after.LeaderEpoch != partition.epoch {
		return strictPartition{}, errors.New("strict journal snapshot is open, changing or expired")
	}
	partition.end = before.Offset
	return partition, nil
}

func (s *strictKafkaSource) fetch(ctx context.Context, c Config, snapshot strictSnapshot, partition int32, offset int64) (*kmsg.FetchResponseTopicPartition, error) {
	req := kmsg.NewPtrFetchRequest()
	req.MaxWaitMillis, req.MinBytes, req.MaxBytes = 100, 1, 4<<20
	req.IsolationLevel = 1
	p := kmsg.NewFetchRequestTopicPartition()
	p.Partition, p.FetchOffset, p.PartitionMaxBytes = partition, offset, 4<<20
	p.CurrentLeaderEpoch = snapshot.partitions[partition].epoch
	req.Topics = []kmsg.FetchRequestTopic{{Topic: c.Topic, TopicID: snapshot.topicID, Partitions: []kmsg.FetchRequestTopicPartition{p}}}
	response, err := req.RequestWith(ctx, s.client.Broker(int(snapshot.partitions[partition].leader)))
	if err != nil {
		return nil, err
	}
	if err := kerr.ErrorForCode(response.ErrorCode); err != nil {
		return nil, err
	}
	if len(response.Topics) != 1 || len(response.Topics[0].Partitions) != 1 {
		return nil, errors.New("strict journal fetch returned unexpected topic/partition inventory")
	}
	topic := response.Topics[0]
	if (response.Version >= 13 && topic.TopicID != snapshot.topicID) || (response.Version < 13 && topic.Topic != c.Topic) {
		return nil, errors.New("strict journal fetch topic identity changed")
	}
	return &topic.Partitions[0], nil
}

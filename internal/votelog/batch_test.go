package votelog

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// This fake controls the real writer.commit path, including its final
// EndTransaction barrier. It never opens a Kafka connection.
type batchProducer struct {
	records       []*kgo.Record
	nextOffset    int64
	beginErr      error
	produceErr    error
	commitErr     error
	commitStarted chan struct{}
	allowCommit   chan struct{}
	startOnce     sync.Once
	closed        atomic.Bool
}

func (*batchProducer) ProducerID(context.Context) (int64, int16, error) { return 1, 0, nil }
func (p *batchProducer) BeginTransaction() error                        { return p.beginErr }
func (p *batchProducer) Close()                                         { p.closed.Store(true) }
func (p *batchProducer) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	results := make(kgo.ProduceResults, len(records))
	for i, record := range records {
		record.Offset = p.nextOffset
		// Offset gaps are normal in a transactional journal. Receipts must
		// use assigned offsets, not infer them from the pending call index.
		p.nextOffset += 7
		results[i] = kgo.ProduceResult{Record: record, Err: p.produceErr}
	}
	p.records = append(p.records, records...)
	return results
}
func (p *batchProducer) EndTransaction(ctx context.Context, commit kgo.TransactionEndTry) error {
	if commit != kgo.TryCommit {
		return errors.New("writer unexpectedly aborted")
	}
	if p.commitStarted != nil {
		p.startOnce.Do(func() { close(p.commitStarted) })
	}
	if p.allowCommit != nil {
		select {
		case <-p.allowCommit:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return p.commitErr
}

func batchTestWriter() (*Store, *writer, *batchProducer) {
	cfg, _ := frameFixture()
	cfg.Partitions = 1
	s := &Store{cfg: cfg, ctx: context.Background()}
	s.now = func() time.Time { return cfg.StartsAt }
	producer := &batchProducer{nextOffset: 100}
	w := &writer{s: s, client: producer, jobs: make(chan *pending, cfg.QueuePerPartition)}
	s.writers = []*writer{w}
	return s, w, producer
}

func batchPending(votes ...Vote) *pending {
	p := &pending{result: make(chan outcome, 1)}
	if len(votes) == 1 {
		p.vote = votes[0]
	} else {
		p.frame = votes
	}
	return p
}

func batchDecodedVotes(t *testing.T, cfg Config, records []*kgo.Record) (map[Position]Vote, []Vote) {
	t.Helper()
	byPosition := make(map[Position]Vote)
	var ordered []Vote
	for _, record := range records {
		visit := func(index uint32, vote Vote) error {
			byPosition[Position{Partition: record.Partition, Offset: record.Offset, Index: index}] = vote
			ordered = append(ordered, vote)
			return nil
		}
		if record.Value[0] == frameVersion {
			if len(record.Key) != 0 {
				t.Fatal("compact frames must not carry a per-voter Kafka key")
			}
			if err := decodeFrame(cfg, record.Value, visit); err != nil {
				t.Fatal(err)
			}
		} else {
			kind, vote, err := decodeRecord(cfg, record.Value)
			if err != nil || kind != recordVote {
				t.Fatalf("legacy record: kind=%d err=%v", kind, err)
			}
			_ = visit(0, vote)
		}
	}
	return byPosition, ordered
}

func TestWriterPacksSinglesPreservesExplicitFramesAndExactReceipts(t *testing.T) {
	s, w, producer := batchTestWriter()
	votes := make([]Vote, 8)
	for i := range votes {
		votes[i] = Vote{Token: [16]byte{byte(i + 1)}, Choice: uint32(1 << (i % 3)), AdmittedAt: s.cfg.StartsAt.Add(time.Duration(i) * time.Nanosecond)}
	}
	// A changed repeat in an explicit frame must not move ahead of its
	// original single call when those preceding calls are packed together.
	votes[3].Token = votes[0].Token
	votes[3].Choice = 2
	votes[7].AdmittedAt = s.cfg.EndsAt.Add(-time.Nanosecond)
	explicitSingleton := &pending{frame: []Vote{votes[5]}, result: make(chan outcome, 1)}
	batch := []*pending{batchPending(votes[0]), batchPending(votes[1]), batchPending(votes[2], votes[3]), batchPending(votes[4]), explicitSingleton, batchPending(votes[6]), batchPending(votes[7])}
	if err := w.flush(batch); err != nil {
		t.Fatal(err)
	}
	if len(producer.records) != 5 {
		t.Fatalf("expected five records with preserved explicit-frame boundaries, got %d", len(producer.records))
	}
	positions, ordered := batchDecodedVotes(t, s.cfg, producer.records)
	if !reflect.DeepEqual(ordered, votes) {
		t.Fatal("packing changed the ordered full token, choice or individual admission time")
	}
	wantPositions := []Position{{Offset: 100}, {Offset: 100, Index: 1}, {Offset: 107}, {Offset: 114}, {Offset: 121}, {Offset: 128}, {Offset: 128, Index: 1}}
	for i, p := range batch {
		got := <-p.result
		pos := Position{Partition: got.receipt.Partition, Offset: got.receipt.Offset, Index: got.receipt.Index}
		if got.err != nil || pos != wantPositions[i] {
			t.Fatalf("pending %d: position=%+v want=%+v err=%v", i, pos, wantPositions[i], got.err)
		}
		want := p.vote
		if len(p.frame) != 0 {
			want = p.frame[0]
			for index, vote := range p.frame {
				if positions[Position{Partition: pos.Partition, Offset: pos.Offset, Index: uint32(index)}] != vote {
					t.Fatal("explicit FrameReceipt Count no longer addresses exactly [0, Count)")
				}
			}
		}
		if positions[pos] != want || !got.receipt.AdmittedAt.Equal(want.AdmittedAt) {
			t.Fatal("receipt does not locate the originally admitted vote")
		}
	}
	// Four frames hold seven votes; the intervening singleton remains v1.
	metrics := s.Metrics()
	for key, want := range map[string]uint64{"committed_attempts": 8, "transactions": 1, "transaction_records": 5, "max_batch_records": 5, "committed_frames": 4, "committed_key_bytes": 16, "committed_value_bytes": 4*80 + 7*28 + 80} {
		if got := metrics[key]; got != want {
			t.Fatalf("metric %s=%d want=%d", key, got, want)
		}
	}
}

func TestPackedSubmitKeepsAdmissionDeadlineAndWaitsForCommit(t *testing.T) {
	s, w, producer := batchTestWriter()
	clock := new(atomic.Int64)
	s.now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	producer.commitStarted = make(chan struct{})
	producer.allowCommit = make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	results := make(chan outcome, 2)
	var batch []*pending
	for i, beforeEnd := range []time.Duration{100 * time.Millisecond, time.Nanosecond} {
		clock.Store(s.cfg.EndsAt.Add(-beforeEnd).UnixNano())
		go func(token byte) {
			r, err := s.Submit(ctx, [16]byte{token}, 1)
			results <- outcome{r, err}
		}(byte(i + 1))
		select {
		case p := <-w.jobs:
			batch = append(batch, p)
		case <-ctx.Done():
			t.Fatal("admission did not reach the bounded writer queue")
		}
	}
	clock.Store(s.cfg.EndsAt.UnixNano())
	for _, token := range []byte{1, 3} {
		if _, err := s.Submit(ctx, [16]byte{token}, 1); !errors.Is(err, ErrClosed) {
			t.Fatalf("known or new token admitted at exact deadline: %v", err)
		}
	}
	flushed := make(chan error, 1)
	go func() { flushed <- w.flush(batch) }()
	select {
	case <-producer.commitStarted:
	case <-ctx.Done():
		t.Fatal("transaction did not reach commit")
	}
	select {
	case r := <-results:
		t.Fatalf("caller received a result before EndTransaction: %+v", r)
	default:
	}
	if s.confirmed.Load() != 0 {
		t.Fatal("confirmed counter advanced before commit")
	}
	close(producer.allowCommit)
	if err := <-flushed; err != nil {
		t.Fatal(err)
	}
	positions, _ := batchDecodedVotes(t, s.cfg, producer.records)
	if len(producer.records) != 1 || len(positions) != 2 {
		t.Fatal("single calls were not packed into one two-entry frame")
	}
	for range 2 {
		got := <-results
		vote, ok := positions[Position{Partition: got.receipt.Partition, Offset: got.receipt.Offset, Index: got.receipt.Index}]
		if got.err != nil || !ok || !vote.AdmittedAt.Equal(got.receipt.AdmittedAt) || !vote.AdmittedAt.Before(s.cfg.EndsAt) {
			t.Fatalf("original pre-deadline admission lost after late commit: %+v", got)
		}
	}
}

func TestPackedCommitFailureNeverReturnsKnownReceipts(t *testing.T) {
	for _, stage := range []string{"begin", "produce", "commit", "after_commit"} {
		t.Run(stage, func(t *testing.T) {
			s, w, producer := batchTestWriter()
			failure := errors.New("injected " + stage)
			switch stage {
			case "begin":
				producer.beginErr = failure
			case "produce":
				producer.produceErr = failure
			case "commit":
				producer.commitErr = failure
			case "after_commit":
				s.afterCommit = func(int32) error { return failure }
			}
			vote := Vote{Token: [16]byte{1}, Choice: 1, AdmittedAt: s.cfg.StartsAt}
			batch := []*pending{batchPending(vote), batchPending(vote), batchPending(vote, vote)}
			if err := w.flush(batch); !errors.Is(err, failure) {
				t.Fatalf("injected failure lost: %v", err)
			}
			for _, p := range batch {
				got := <-p.result
				if !errors.Is(got.err, ErrUnknown) || got.receipt != (Receipt{}) {
					t.Fatalf("false known result after %s failure: %+v", stage, got)
				}
			}
			if s.confirmed.Load() != 0 || s.committedFrames.Load() != 0 || s.committedValueBytes.Load() != 0 || s.committedKeyBytes.Load() != 0 {
				t.Fatal("unacknowledged batch advanced confirmed metrics")
			}
		})
	}
}

func TestPackedWriterFailureBoundsAndDrainsQueuedCalls(t *testing.T) {
	s, w, producer := batchTestWriter()
	s.cfg.BatchSize, s.cfg.QueuePerPartition = 2, 2
	w.jobs = make(chan *pending, 2)
	w.seal, w.done = make(chan struct{}, 1), make(chan struct{})
	s.ctx, s.cancel = context.WithCancel(context.Background())
	t.Cleanup(s.Close)
	producer.commitStarted = make(chan struct{})
	producer.allowCommit = make(chan struct{})
	producer.commitErr = errors.New("injected commit failure")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	results := make(chan error, 4)
	for i := range 4 {
		go func(token byte) { _, err := s.Submit(ctx, [16]byte{token}, 1); results <- err }(byte(i + 1))
		// Wait only for queue admission, not for transaction completion.
		for s.admitted.Load() < uint64(i+1) && ctx.Err() == nil {
			time.Sleep(time.Millisecond)
		}
		if ctx.Err() != nil {
			t.Fatal("bounded queue did not admit expected call")
		}
		if i == 1 {
			// Preload the first complete batch before starting the worker, so
			// this test never depends on scheduling ahead of its linger timer.
			s.wg.Add(1)
			go w.run()
			select {
			case <-producer.commitStarted:
			case <-ctx.Done():
				t.Fatal("active two-vote transaction did not start")
			}
		}
	}
	if _, err := s.Submit(ctx, [16]byte{5}, 1); !errors.Is(err, ErrBusy) {
		t.Fatalf("active transaction plus queue bound bypassed: %v", err)
	}
	close(producer.allowCommit)
	for range 4 {
		if err := <-results; !errors.Is(err, ErrUnknown) {
			t.Fatalf("failed writer left a known/undrained call: %v", err)
		}
	}
	<-w.done
	if !producer.closed.Load() || s.failures.Load() != 1 || len(producer.records) != 1 {
		t.Fatal("failed epoch remained open or wrote its queued calls")
	}
	if _, err := s.Submit(ctx, [16]byte{6}, 1); !errors.Is(err, ErrUnknown) {
		t.Fatalf("failed writer resumed admission: %v", err)
	}
}

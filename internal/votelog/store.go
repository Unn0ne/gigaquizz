package votelog

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Store struct {
	cfg       Config
	ctx       context.Context
	cancel    context.CancelFunc
	writers   []*writer
	wg        sync.WaitGroup
	closeOnce sync.Once
	now       func() time.Time
	// Deterministic integration-test hooks; configured before concurrent use.
	beforeCommit, afterCommit                                                                func(partition int32) error
	admitted, confirmed, transactions, transactionRecords, transactionNS, maxBatch, failures atomic.Uint64
	committedFrames, committedValueBytes, committedKeyBytes                                  atomic.Uint64
}
type writer struct {
	s                       *Store
	partition               int32
	client                  *kgo.Client
	mu                      sync.Mutex
	closing, sealed, failed bool
	failure                 error
	end                     int64
	jobs                    chan *pending
	queuedVotes             int // protected by mu; queued logical votes, not frames
	seal                    chan struct{}
	done                    chan struct{}
}
type pending struct {
	vote   Vote
	frame  []Vote
	result chan outcome
}

func (p *pending) size() int {
	if len(p.frame) != 0 {
		return len(p.frame)
	}
	return 1
}

type outcome struct {
	receipt Receipt
	err     error
}

type transactionFailure struct {
	stage string
	err   error
}

func (e *transactionFailure) Error() string { return e.stage + ": " + e.err.Error() }
func (e *transactionFailure) Unwrap() error { return e.err }

func clientOptions(c Config) []kgo.Opt {
	return []kgo.Opt{kgo.SeedBrokers(c.Brokers...),
		kgo.RequestTimeoutOverhead(2 * time.Second), kgo.DialTimeout(3 * time.Second),
		kgo.ProducerBatchCompression(kgo.NoCompression()), kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(0), kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.MaxBufferedRecords(max(c.BatchSize*2, 1024)), kgo.ProducerBatchMaxBytes(1 << 20),
	}
}

func CreateTopic(ctx context.Context, c Config) error {
	if err := c.validate(); err != nil {
		return err
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.Brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()
	configs := map[string]*string{}
	for k, v := range map[string]string{"min.insync.replicas": "2", "unclean.leader.election.enable": "false", "cleanup.policy": "delete", "retention.ms": "86400000", "segment.bytes": "67108864", "compression.type": "producer"} {
		value := v
		configs[k] = &value
	}
	responses, err := kadm.NewClient(cl).CreateTopics(ctx, int32(c.Partitions), 3, configs, c.Topic)
	if err != nil {
		return err
	}
	if err := responses[c.Topic].Err; err != nil {
		return err
	}
	_, err = readyTopic(ctx, kadm.NewClient(cl), c, true)
	return err
}

// CreateTopics success can precede metadata propagation to other brokers.
// Readiness is explicit and bounded, rather than a fixed setup sleep.
func readyTopic(parent context.Context, admin *kadm.Client, c Config, allISR bool) (kadm.TopicDetail, error) {
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	for {
		details, err := admin.ListTopics(ctx, c.Topic)
		t, ok := details[c.Topic]
		ready := err == nil && ok && t.Err == nil && len(t.Partitions) == c.Partitions
		for _, p := range t.Partitions {
			ready = ready && p.Err == nil && p.Leader >= 0 && len(p.Replicas) == 3 && (!allISR || len(p.ISR) == 3)
		}
		if ready {
			return t, nil
		}
		select {
		case <-ctx.Done():
			return kadm.TopicDetail{}, errors.New("owned topic did not become ready with required partitions/replicas")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// New is an explicit ownership transfer. The caller must first stop/fence the
// previous application owner; this lab does not implement distributed leases.
// Kafka fencing protects against an old in-flight session, not against an old
// application deliberately reinitializing a new producer.
func New(ctx context.Context, c Config) (*Store, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	initCtx, initCancel := context.WithTimeout(ctx, time.Minute)
	defer initCancel()
	ctx = initCtx
	c.Brokers = append([]string(nil), c.Brokers...)
	base, cancel := context.WithCancel(context.Background())
	s := &Store{cfg: c, ctx: base, cancel: cancel, now: time.Now}
	fail := func(err error) (*Store, error) { s.Close(); return nil, err }
	admin, err := kgo.NewClient(kgo.SeedBrokers(c.Brokers...))
	if err != nil {
		return fail(err)
	}
	defer admin.Close()
	a := kadm.NewClient(admin)
	topic, err := readyTopic(ctx, a, c, false)
	if err != nil {
		return fail(err)
	}
	for _, p := range topic.Partitions {
		if len(p.Replicas) != 3 {
			return fail(errors.New("prototype requires three replicas per partition"))
		}
	}
	configs, err := a.DescribeTopicConfigs(ctx, c.Topic)
	if err != nil {
		return fail(err)
	}
	matched := map[string]bool{}
	for _, resource := range configs {
		if resource.Err != nil {
			return fail(resource.Err)
		}
		for _, item := range resource.Configs {
			if item.Value == nil {
				continue
			}
			for k, v := range map[string]string{"min.insync.replicas": "2", "unclean.leader.election.enable": "false", "cleanup.policy": "delete"} {
				if item.Key == k && *item.Value == v {
					matched[k] = true
				}
			}
		}
	}
	if len(matched) != 3 {
		return fail(errors.New("topic durability/retention configuration mismatch"))
	}
	// A new topic's leader metadata and ListOffsets can converge separately.
	// No requests are admitted until a complete readable prefix is established.
	var ends kadm.ListedOffsets
	offsetCtx, offsetCancel := context.WithTimeout(ctx, 20*time.Second)
	defer offsetCancel()
	for {
		ends, err = a.ListEndOffsets(offsetCtx, c.Topic)
		ready := err == nil && len(ends[c.Topic]) == c.Partitions
		for _, p := range ends[c.Topic] {
			ready = ready && p.Err == nil && p.Offset >= 0
		}
		if ready {
			break
		}
		select {
		case <-offsetCtx.Done():
			return fail(errors.New("initial journal offsets did not become readable"))
		case <-time.After(50 * time.Millisecond):
		}
	}
	fresh := true
	for _, p := range ends[c.Topic] {
		if p.Err != nil {
			return fail(p.Err)
		}
		fresh = fresh && p.Offset == 0
	}
	// Validate the original committed configuration before appending recovery
	// barriers. Recovery never binds an existing topic to a new poll definition.
	if !fresh {
		verifyCtx, verifyCancel := context.WithTimeout(ctx, 20*time.Second)
		verifyErr := verifyFirstRecords(verifyCtx, c)
		verifyCancel()
		if errors.Is(verifyErr, context.DeadlineExceeded) {
			return fail(errors.New("initial committed configuration incomplete; prepare a fresh topic before the event"))
		}
		if verifyErr != nil {
			return fail(verifyErr)
		}
	}
	for i := 0; i < c.Partitions; i++ {
		w := &writer{s: s, partition: int32(i), jobs: make(chan *pending, c.QueuePerPartition), seal: make(chan struct{}, 1), done: make(chan struct{}), end: -1}
		opts := append(clientOptions(c), kgo.TransactionalID(fmt.Sprintf("%s-p%d", c.Topic, i)), kgo.TransactionTimeout(c.TransactionTimeout))
		w.client, err = kgo.NewClient(opts...)
		if err != nil {
			return fail(err)
		}
		s.writers = append(s.writers, w)
	}
	barriers := make([]int64, c.Partitions)
	err = parallel(c.Partitions, func(i int) error {
		w := s.writers[i]
		// Only initial ownership acquisition may retry a transient coordinator
		// response. Runtime writers never reinitialize after failure/fencing.
		for {
			_, _, err := w.client.ProducerID(ctx)
			if err == nil {
				break
			}
			if !errors.Is(err, kerr.ConcurrentTransactions) && !kerr.IsRetriable(err) {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(50 * time.Millisecond):
			}
		}
		r := &kgo.Record{Topic: c.Topic, Partition: w.partition, Value: encodeRecord(c, recordBoot, Vote{})}
		if err := w.commit(ctx, []*kgo.Record{r}, false); err != nil {
			return err
		}
		barriers[i] = r.Offset
		return nil
	})
	if err != nil {
		return fail(unknown(err))
	}
	closed, err := replay(ctx, c, barriers, nil)
	if err != nil {
		return fail(err)
	}
	for i, w := range s.writers {
		if closed[i] >= 0 {
			w.closing, w.sealed, w.end = true, true, closed[i]
		}
		s.wg.Add(1)
		go w.run()
	}
	return s, nil
}

func parallel(n int, fn func(int) error) error {
	var wg sync.WaitGroup
	errs := make(chan error, n)
	limit := make(chan struct{}, 16)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			limit <- struct{}{}
			defer func() { <-limit }()
			if err := fn(i); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	return nil
}

func (s *Store) Config() Config {
	c := s.cfg
	c.Brokers = append([]string(nil), c.Brokers...)
	return c
}

func (s *Store) Submit(ctx context.Context, token [16]byte, choice uint32) (Receipt, error) {
	if token == [16]byte{} || !s.cfg.validChoice(choice) {
		return Receipt{}, ErrInvalid
	}
	if ctx.Err() != nil {
		return Receipt{}, ErrUnknown
	}
	w := s.writers[s.cfg.Partition(token)]
	w.mu.Lock()
	if w.sealed || w.closing {
		w.mu.Unlock()
		return Receipt{}, ErrClosed
	}
	if w.failed || s.ctx.Err() != nil {
		w.mu.Unlock()
		return Receipt{}, ErrUnknown
	}
	if w.queuedVotes >= s.cfg.QueuePerPartition || len(w.jobs) == cap(w.jobs) {
		w.mu.Unlock()
		return Receipt{}, ErrBusy
	}
	now := s.now()
	if now.Before(s.cfg.StartsAt) {
		w.mu.Unlock()
		return Receipt{}, ErrNotOpen
	}
	if !now.Before(s.cfg.EndsAt) {
		w.mu.Unlock()
		return Receipt{}, ErrClosed
	}
	p := &pending{vote: Vote{Token: token, Choice: choice, AdmittedAt: now.UTC()}, result: make(chan outcome, 1)}
	w.queuedVotes++
	w.jobs <- p
	s.admitted.Add(1)
	w.mu.Unlock()
	select {
	case r := <-p.result:
		return r.receipt, r.err
	case <-ctx.Done():
		return Receipt{}, ErrUnknown
	case <-s.ctx.Done():
		return Receipt{}, ErrUnknown
	}
}

func (w *writer) commit(parent context.Context, records []*kgo.Record, runtime bool) error {
	ctx, cancel := context.WithTimeout(parent, w.s.cfg.TransactionTimeout)
	defer cancel()
	// In-flight idempotent produces can outlive their request context. Closing
	// the client bounds the worker and permanently prevents this epoch's reuse.
	stop := context.AfterFunc(ctx, w.client.Close)
	defer stop()
	started := time.Now()
	if err := w.client.BeginTransaction(); err != nil {
		return &transactionFailure{"begin", err}
	}
	if err := w.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return &transactionFailure{"produce", err}
	}
	if runtime && w.s.beforeCommit != nil {
		if err := w.s.beforeCommit(w.partition); err != nil {
			return &transactionFailure{"before_commit_hook", err}
		}
	}
	if err := w.client.EndTransaction(ctx, kgo.TryCommit); err != nil {
		return &transactionFailure{"commit", err}
	}
	if runtime {
		w.s.transactions.Add(1)
		w.s.transactionRecords.Add(uint64(len(records)))
		w.s.transactionNS.Add(uint64(time.Since(started)))
		for old := w.s.maxBatch.Load(); uint64(len(records)) > old; old = w.s.maxBatch.Load() {
			if w.s.maxBatch.CompareAndSwap(old, uint64(len(records))) {
				break
			}
		}
		if w.s.afterCommit != nil {
			if err := w.s.afterCommit(w.partition); err != nil {
				return &transactionFailure{"after_commit_hook", err}
			}
		}
	}
	return nil
}

func (w *writer) flush(batch []*pending) error {
	if len(batch) == 0 {
		return nil
	}
	records := make([]*kgo.Record, len(batch))
	for i, p := range batch {
		if len(p.frame) > 0 {
			value, err := encodeFrame(w.s.cfg, p.frame)
			if err != nil {
				for _, pending := range batch {
					pending.result <- outcome{err: ErrUnknown}
				}
				return err
			}
			records[i] = &kgo.Record{Topic: w.s.cfg.Topic, Partition: w.partition, Value: value}
		} else {
			records[i] = &kgo.Record{Topic: w.s.cfg.Topic, Partition: w.partition, Key: p.vote.Token[:], Value: encodeRecord(w.s.cfg, recordVote, p.vote)}
		}
	}
	err := w.commit(w.s.ctx, records, true)
	for i, p := range batch {
		o := outcome{err: unknown(err)}
		if err == nil {
			admittedAt := p.vote.AdmittedAt
			if len(p.frame) > 0 {
				admittedAt = p.frame[0].AdmittedAt
				w.s.committedFrames.Add(1)
			}
			o.receipt = Receipt{Partition: w.partition, Offset: records[i].Offset, AdmittedAt: admittedAt}
			w.s.confirmed.Add(uint64(p.size()))
			w.s.committedValueBytes.Add(uint64(len(records[i].Value)))
			w.s.committedKeyBytes.Add(uint64(len(records[i].Key)))
		}
		p.result <- o
	}
	return err
}

func (w *writer) run() {
	defer w.s.wg.Done()
	defer close(w.done)
	ticker := time.NewTicker(w.s.cfg.Linger)
	defer ticker.Stop()
	batch := make([]*pending, 0, w.s.cfg.BatchSize)
	batchVotes := 0
	defer func() {
		for _, p := range batch {
			p.result <- outcome{err: ErrUnknown}
		}
		for {
			select {
			case p := <-w.jobs:
				p.result <- outcome{err: ErrUnknown}
			default:
				return
			}
		}
	}()
	fail := func(err error) {
		w.mu.Lock()
		w.failed = true
		w.failure = err
		w.mu.Unlock()
		w.s.failures.Add(1)
		w.client.Close()
	}
	flush := func() bool {
		b := batch
		batch = batch[:0]
		batchVotes = 0
		err := w.flush(b)
		clear(b)
		if err != nil {
			fail(err)
			return false
		}
		return true
	}
	appendJob := func(p *pending) bool {
		if batchVotes+p.size() > w.s.cfg.BatchSize && !flush() {
			p.result <- outcome{err: ErrUnknown}
			return false
		}
		// Keep credits while waiting for a previous active batch to commit.
		// The bound is queued logical votes plus one active transaction batch.
		w.mu.Lock()
		w.queuedVotes -= p.size()
		w.mu.Unlock()
		batch = append(batch, p)
		batchVotes += p.size()
		return batchVotes < w.s.cfg.BatchSize || flush()
	}
	for {
		select {
		case <-w.s.ctx.Done():
			return
		case p := <-w.jobs:
			if !appendJob(p) {
				return
			}
		case <-ticker.C:
			if !flush() {
				return
			}
		case <-w.seal:
			// Submit closes its gate under the same mutex before requesting seal.
			for len(w.jobs) > 0 {
				if !appendJob(<-w.jobs) {
					return
				}
			}
			if !flush() {
				return
			}
			r := &kgo.Record{Topic: w.s.cfg.Topic, Partition: w.partition, Value: encodeRecord(w.s.cfg, recordClosed, Vote{})}
			if err := w.commit(w.s.ctx, []*kgo.Record{r}, false); err != nil {
				fail(err)
				return
			}
			w.mu.Lock()
			w.sealed = true
			w.end = r.Offset
			w.mu.Unlock()
			return
		}
	}
}

func (s *Store) Seal(ctx context.Context) (Manifest, error) {
	if err := waitUntil(ctx, s.cfg.EndsAt, s.now); err != nil {
		return Manifest{}, err
	}
	err := parallel(len(s.writers), func(i int) error {
		w := s.writers[i]
		w.mu.Lock()
		if w.sealed {
			w.mu.Unlock()
			return nil
		}
		if w.failed {
			w.mu.Unlock()
			return ErrUnknown
		}
		if !w.closing {
			w.closing = true
			w.seal <- struct{}{}
		}
		w.mu.Unlock()
		select {
		case <-w.done:
		case <-ctx.Done():
			return ctx.Err()
		}
		w.mu.Lock()
		defer w.mu.Unlock()
		if !w.sealed {
			return ErrUnknown
		}
		return nil
	})
	if err != nil {
		return Manifest{}, err
	}
	m := Manifest{Topic: s.cfg.Topic, StartsAt: s.cfg.StartsAt, EndsAt: s.cfg.EndsAt}
	for _, w := range s.writers {
		w.mu.Lock()
		m.Partitions = append(m.Partitions, PartitionEnd{w.partition, w.end})
		w.mu.Unlock()
	}
	return m, nil
}

func (s *Store) Close() {
	s.closeOnce.Do(func() {
		s.cancel()
		_ = parallel(len(s.writers), func(i int) error { s.writers[i].client.Close(); return nil })
		s.wg.Wait()
	})
}

func (s *Store) Metrics() map[string]uint64 {
	m := map[string]uint64{"admitted": s.admitted.Load(), "committed_attempts": s.confirmed.Load(), "transactions": s.transactions.Load(), "transaction_records": s.transactionRecords.Load(), "transaction_ns": s.transactionNS.Load(), "max_batch_records": s.maxBatch.Load(), "failed_writers": s.failures.Load(), "committed_frames": s.committedFrames.Load(), "committed_value_bytes": s.committedValueBytes.Load(), "committed_key_bytes": s.committedKeyBytes.Load()}
	for _, w := range s.writers {
		w.mu.Lock()
		err := w.failure
		w.mu.Unlock()
		if err == nil {
			continue
		}
		var failure *transactionFailure
		if errors.As(err, &failure) {
			m["failed_at_"+failure.stage]++
		}
		var kafkaError *kerr.Error
		if errors.As(err, &kafkaError) {
			m["failure_kafka_code_"+strconv.Itoa(int(kafkaError.Code))]++
		}
		if errors.Is(err, context.DeadlineExceeded) {
			m["failure_deadline"]++
		}
		if errors.Is(err, kgo.ErrClientClosed) {
			m["failure_client_closed"]++
		}
	}
	return m
}

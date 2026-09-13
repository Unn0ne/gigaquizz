package votelog

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Store struct {
	cfg             Config
	ctx             context.Context
	cancel          context.CancelFunc
	writers         []*writer
	byPartition     []*writer
	wg              sync.WaitGroup
	closeOnce       sync.Once
	admissionClosed atomic.Bool
	now             func() time.Time
	// Deterministic integration-test hooks; configured before concurrent use.
	beforeCommit, afterCommit                                                                func(partition int32) error
	admitted, confirmed, transactions, transactionRecords, transactionNS, maxBatch, failures atomic.Uint64
	committedFrames, committedValueBytes, committedKeyBytes                                  atomic.Uint64
	busyVotes                                                                                atomic.Uint64
}
type writer struct {
	s                       *Store
	partition               int32
	client                  transactionalClient
	mu                      sync.Mutex
	closing, sealed, failed bool
	failure                 error
	end                     int64
	jobs                    chan *pending
	queuedVotes             int // protected by mu; queued logical votes, not frames
	activeVotes             int // collected/in-flight logical votes; protected by mu
	seal                    chan struct{}
	done                    chan struct{}
}

// The writer uses one transactional producer for its entire ownership epoch.
// Keeping this boundary narrow also permits deterministic commit-failure tests.
type transactionalClient interface {
	ProducerID(context.Context) (int64, int16, error)
	BeginTransaction() error
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
	EndTransaction(context.Context, kgo.TransactionEndTry) error
	Close()
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
	cl, err := NewClient(c)
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
	if c.OwnedPartitions != nil {
		c.OwnedPartitions = append([]int32{}, c.OwnedPartitions...)
	}
	base, cancel := context.WithCancel(context.Background())
	s := &Store{cfg: c, ctx: base, cancel: cancel, now: time.Now}
	fail := func(err error) (*Store, error) { s.Close(); return nil, err }
	admin, err := NewClient(c)
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
	initialized := make([]int32, 0, c.Partitions)
	for partition, p := range ends[c.Topic] {
		if p.Err != nil {
			return fail(p.Err)
		}
		if p.Offset > 0 {
			initialized = append(initialized, partition)
		}
	}
	// Validate the original committed configuration before appending recovery
	// barriers. Recovery never binds an existing topic to a new poll definition.
	if len(initialized) > 0 {
		verifyCfg := c
		verifyCfg.OwnedPartitions = initialized
		verifyCtx, verifyCancel := context.WithTimeout(ctx, 20*time.Second)
		verifyErr := verifyFirstRecords(verifyCtx, verifyCfg)
		verifyCancel()
		if errors.Is(verifyErr, context.DeadlineExceeded) {
			return fail(errors.New("initial committed configuration incomplete; prepare a fresh topic before the event"))
		}
		if verifyErr != nil {
			return fail(verifyErr)
		}
	}
	s.byPartition = make([]*writer, c.Partitions)
	owned, _ := c.ownedPartitions()
	for _, partition := range owned {
		i := int(partition)
		w := &writer{s: s, partition: int32(i), jobs: make(chan *pending, c.QueuePerPartition), seal: make(chan struct{}, 1), done: make(chan struct{}), end: -1}
		opts := append(clientOptions(c), kgo.TransactionalID(fmt.Sprintf("%s-p%d", c.Topic, i)), kgo.TransactionTimeout(c.TransactionTimeout))
		w.client, err = NewClient(c, opts...)
		if err != nil {
			return fail(err)
		}
		s.writers = append(s.writers, w)
		s.byPartition[w.partition] = w
	}
	barriers := make([]int64, c.Partitions)
	err = parallel(len(s.writers), func(i int) error {
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
		barriers[w.partition] = r.Offset
		return nil
	})
	if err != nil {
		return fail(unknown(err))
	}
	closed, err := replay(ctx, c, barriers, nil)
	if err != nil {
		return fail(err)
	}
	for _, w := range s.writers {
		if closed[w.partition] >= 0 {
			w.closing, w.sealed, w.end = true, true, closed[w.partition]
			s.admissionClosed.Store(true)
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

func (s *Store) Submit(ctx context.Context, token [16]byte, choice uint32) (Receipt, error) {
	if token == [16]byte{} || !s.cfg.validChoice(choice) {
		return Receipt{}, ErrInvalid
	}
	if ctx.Err() != nil {
		return Receipt{}, ErrUnknown
	}
	w := s.writerFor(s.cfg.Partition(token))
	if w == nil {
		return Receipt{}, ErrNotOwned
	}
	w.mu.Lock()
	if s.admissionClosed.Load() || w.sealed || w.closing {
		s.admissionClosed.Store(true)
		w.mu.Unlock()
		return Receipt{}, ErrClosed
	}
	if w.failed || s.ctx.Err() != nil {
		w.mu.Unlock()
		return Receipt{}, ErrUnknown
	}
	if w.queuedVotes >= s.cfg.QueuePerPartition || len(w.jobs) == cap(w.jobs) {
		w.mu.Unlock()
		s.busyVotes.Add(1)
		return Receipt{}, ErrBusy
	}
	now := s.now()
	if now.Before(s.cfg.StartsAt) {
		w.mu.Unlock()
		return Receipt{}, ErrNotOpen
	}
	if !now.Before(s.cfg.EndsAt) {
		s.admissionClosed.Store(true)
		w.mu.Unlock()
		return Receipt{}, ErrClosed
	}
	if s.admissionClosed.Load() {
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
	records := make([]*kgo.Record, 0, len(batch))
	positions := make([]struct {
		record int
		index  uint32
	}, len(batch))
	var frames uint64
	for i := 0; i < len(batch); {
		p := batch[i]
		end := i + 1
		votes := p.frame
		if len(votes) == 0 {
			// Consecutive single calls share an existing v2 frame. Preserve both
			// queue order and each call's original admission time; flushing after
			// the deadline never re-admits or timestamps these votes again.
			for end < len(batch) && len(batch[end].frame) == 0 {
				end++
			}
			if end-i > 1 {
				votes = make([]Vote, end-i)
				for j := i; j < end; j++ {
					votes[j-i] = batch[j].vote
				}
			}
		}
		for j := i; j < end; j++ {
			positions[j].record = len(records)
			positions[j].index = uint32(j - i)
		}
		if len(votes) > 0 {
			// An explicit SubmitFrame stays a standalone record. Its receipt's
			// Count continues to address exactly indices [0, Count).
			value, err := encodeFrame(w.s.cfg, votes)
			if err != nil {
				for _, pending := range batch {
					pending.result <- outcome{err: ErrUnknown}
				}
				return err
			}
			records = append(records, &kgo.Record{Topic: w.s.cfg.Topic, Partition: w.partition, Value: value})
			frames++
		} else {
			// Keep a singleton's v1 encoding and its zero receipt index.
			records = append(records, &kgo.Record{Topic: w.s.cfg.Topic, Partition: w.partition, Key: p.vote.Token[:], Value: encodeRecord(w.s.cfg, recordVote, p.vote)})
		}
		i = end
	}
	err := w.commit(w.s.ctx, records, true)
	if err == nil {
		w.s.committedFrames.Add(frames)
		for _, record := range records {
			w.s.committedValueBytes.Add(uint64(len(record.Value)))
			w.s.committedKeyBytes.Add(uint64(len(record.Key)))
		}
	}
	for i, p := range batch {
		o := outcome{err: unknown(err)}
		if err == nil {
			admittedAt := p.vote.AdmittedAt
			if len(p.frame) > 0 {
				admittedAt = p.frame[0].AdmittedAt
			}
			pos := positions[i]
			o.receipt = Receipt{Partition: w.partition, Offset: records[pos.record].Offset, Index: pos.index, AdmittedAt: admittedAt}
			w.s.confirmed.Add(uint64(p.size()))
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
		w.mu.Lock()
		w.activeVotes = 0
		w.mu.Unlock()
		for _, p := range batch {
			p.result <- outcome{err: ErrUnknown}
		}
		for {
			select {
			case p := <-w.jobs:
				w.mu.Lock()
				w.queuedVotes -= p.size()
				w.mu.Unlock()
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
		w.mu.Lock()
		w.activeVotes = 0
		w.mu.Unlock()
		clear(b)
		if err != nil {
			fail(err)
			return false
		}
		return true
	}
	appendJob := func(p *pending) bool {
		if batchVotes+p.size() > w.s.cfg.BatchSize && !flush() {
			w.mu.Lock()
			w.queuedVotes -= p.size()
			w.mu.Unlock()
			p.result <- outcome{err: ErrUnknown}
			return false
		}
		// Keep credits while waiting for a previous active batch to commit.
		// The bound is queued logical votes plus one active transaction batch.
		w.mu.Lock()
		w.queuedVotes -= p.size()
		w.activeVotes += p.size()
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
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	if !s.admissionClosed.Load() {
		if err := waitUntil(ctx, s.cfg.EndsAt, s.now); err != nil {
			return Manifest{}, err
		}
	}
	s.admissionClosed.Store(true)
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
	m := s.cfg.manifest()
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
	m["busy_votes"] = s.busyVotes.Load()
	for _, w := range s.writers {
		w.mu.Lock()
		m["queued_votes"] += uint64(w.queuedVotes)
		m["active_votes"] += uint64(w.activeVotes)
		err := w.failure
		w.mu.Unlock()
		if err == nil {
			continue
		}
		m[writerFailureCategory(err)]++
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

// Only fixed categories may reach application telemetry. Error strings and
// broker addresses remain private, and a category describes the observed error
// rather than claiming that the underlying transaction was committed/aborted.
func writerFailureCategory(err error) string {
	var network net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, kerr.RequestTimedOut):
		return "writer_failures_timeout"
	case errors.As(err, &network) && network.Timeout():
		return "writer_failures_timeout"
	case errors.Is(err, kerr.ProducerFenced), errors.Is(err, kerr.InvalidProducerEpoch):
		return "writer_failures_fenced"
	case errors.Is(err, kerr.NetworkException), errors.As(err, &network):
		return "writer_failures_network"
	case errors.Is(err, kgo.ErrClientClosed):
		return "writer_failures_client_closed"
	default:
		return "writer_failures_other"
	}
}

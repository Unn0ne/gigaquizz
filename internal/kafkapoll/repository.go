package kafkapoll

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"gigaquizz/internal/poll"
	"gigaquizz/internal/votelog"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func (s *Store) operation(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(s.ctx, cancel)
	return ctx, func() { stop(); cancel() }
}

func (s *Store) load(ctx context.Context) error {
	rows, err := s.pool.Query(ctx, "SELECT id::text, description, journal, finalized_at, prepared FROM "+s.table()+" ORDER BY starts_at LIMIT $1", s.opts.MaxPolls+1)
	if err != nil {
		return err
	}
	defer rows.Close()
	pendingCount := 0
	for rows.Next() {
		if len(s.polls) >= s.opts.MaxPolls {
			return errors.New("metadata poll inventory bound exceeded")
		}
		var id string
		var desc, config []byte
		var final *time.Time
		var prepared bool
		if err := rows.Scan(&id, &desc, &config, &final, &prepared); err != nil {
			return err
		}
		e := new(entry)
		if json.Unmarshal(desc, &e.poll) != nil || json.Unmarshal(config, &e.config) != nil {
			return errors.New("invalid persisted poll configuration")
		}
		idBytes, err := parseID(id)
		if err != nil || e.poll.ID != id || e.config.PollID != idBytes || !e.poll.StartsAt.Equal(e.config.StartsAt) || !e.poll.EndsAt.Equal(e.config.EndsAt) || !validTopic(e.config.Topic, idBytes) || e.config.AllowedMask != (uint32(1)<<len(e.poll.Options))-1 || e.config.Multiple != (e.poll.Type == "multiple") {
			return errors.New("persisted poll and journal definitions disagree")
		}
		e.poll.FinalizedAt = final
		if final == nil {
			pendingCount++
			if pendingCount > maxPendingPolls {
				return errors.New("unfinished poll inventory exceeds 32-controller bound")
			}
		}
		e.prepared = prepared
		e.config.Brokers = append([]string(nil), s.opts.Brokers...)
		e.config.AllowRemoteBrokers = s.opts.AllowRemoteBrokers
		s.polls[id] = e
	}
	return rows.Err()
}

// prepare is called only by the single administrative controller. Stable
// transactional IDs transfer writer epochs; a failed epoch is never resumed.
func (s *Store) prepare(parent context.Context, e *entry) error {
	ctx, cancel := s.operation(parent)
	defer cancel()
	if err := s.checkOwner(ctx); err != nil {
		return err
	}
	// Resolve a possibly cancelled metadata commit before deciding whether a
	// topic can be rotated. A prepared topic is NEVER recreated or replaced.
	var encoded []byte
	if err := s.pool.QueryRow(ctx, "SELECT journal, prepared FROM "+s.table()+" WHERE id=$1", e.poll.ID).Scan(&encoded, &e.prepared); err != nil {
		return err
	}
	var persisted votelog.Config
	if json.Unmarshal(encoded, &persisted) != nil || persisted.PollID != e.config.PollID || !validTopic(persisted.Topic, e.config.PollID) {
		return errors.New("invalid persisted journal ownership")
	}
	e.config = persisted
	e.config.Brokers = append([]string(nil), s.opts.Brokers...)
	e.config.AllowRemoteBrokers = s.opts.AllowRemoteBrokers
	if !e.prepared {
		if err := votelog.CreateTopic(ctx, e.config); errors.Is(err, kerr.TopicAlreadyExists) {
			// No writer is exposed until prepared is durable. The old topic can
			// contain a partial BOOT but no application-acknowledged attempts.
			_, generation, err := newID()
			if err != nil {
				return err
			}
			next := e.config
			next.Topic = "gqlog_app_" + hex.EncodeToString(next.PollID[:]) + "_" + hex.EncodeToString(generation[:])
			data, _ := json.Marshal(next)
			command, err := s.pool.Exec(ctx, "UPDATE "+s.table()+" SET journal=$2 WHERE id=$1 AND NOT prepared", e.poll.ID, data)
			if err != nil {
				return err
			}
			if command.RowsAffected() != 1 {
				return errors.New("poll preparation ownership changed")
			}
			if err = s.guard.WaitDurable(ctx); err != nil {
				return err
			}
			e.config = next
			if err = votelog.CreateTopic(ctx, e.config); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(e.config.Brokers...))
	if err != nil {
		return err
	}
	// Polls may be scheduled far ahead, and recovery must not lose BOOT or
	// acknowledged votes after the lab's default 24h retention. Explicit manual
	// archival/removal is an operator task; this application deletes no topics.
	retention := "-1"
	responses, err := kadm.NewClient(cl).AlterTopicConfigs(ctx, []kadm.AlterConfig{
		{Name: "retention.ms", Value: &retention, Op: kadm.SetConfig},
		{Name: "retention.bytes", Value: &retention, Op: kadm.SetConfig},
	}, e.config.Topic)
	cl.Close()
	if err != nil {
		return err
	}
	for _, response := range responses {
		if response.Err != nil {
			return response.Err
		}
	}
	w, err := votelog.New(ctx, e.config)
	if err != nil {
		return err
	}
	if err = s.checkOwner(ctx); err != nil {
		w.Close()
		return err
	}
	if !e.prepared {
		if _, err = s.pool.Exec(ctx, "UPDATE "+s.table()+" SET prepared=true WHERE id=$1", e.poll.ID); err != nil {
			w.Close()
			return err
		}
		if err = s.guard.WaitDurable(ctx); err != nil {
			w.Close()
			return err
		}
		e.prepared = true
	}
	if s.ctx.Err() != nil {
		w.Close()
		return ErrOwnership
	}
	s.mu.Lock()
	e.writer = w
	s.mu.Unlock()
	return nil
}

func validTopic(topic string, id [16]byte) bool {
	base := "gqlog_app_" + hex.EncodeToString(id[:])
	if topic == base {
		return true
	}
	if len(topic) != len(base)+33 || topic[:len(base)+1] != base+"_" {
		return false
	}
	_, err := hex.DecodeString(topic[len(base)+1:])
	return err == nil
}

func (s *Store) Create(parent context.Context, input poll.CreateInput) (poll.Poll, error) {
	if err := input.Validate(); err != nil {
		return poll.Poll{}, err
	}
	ctx, cancel := s.operation(parent)
	defer cancel()
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if err := s.checkOwner(ctx); err != nil {
		return poll.Poll{}, err
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	starts := now.Add(s.opts.PreparationLead)
	if input.StartsAt != nil {
		starts = input.StartsAt.UTC().Truncate(time.Microsecond)
		if starts.Before(now.Add(s.opts.PreparationLead)) {
			return poll.Poll{}, fmt.Errorf("schedule the poll at least %s ahead for Kafka preparation", s.opts.PreparationLead)
		}
	}
	id, token, err := newID()
	if err != nil {
		return poll.Poll{}, errors.New("cannot generate poll identifier")
	}
	p := poll.Poll{ID: id, Question: input.Question, Type: input.Type, StartsAt: starts, EndsAt: starts.Add(time.Minute), CreatedAt: now, Options: make([]poll.Option, len(input.Options))}
	for i, label := range input.Options {
		p.Options[i] = poll.Option{ID: i + 1, Label: label}
	}
	s.mu.RLock()
	full := len(s.polls) >= s.opts.MaxPolls
	pendingCount := 0
	for _, existing := range s.polls {
		if existing.poll.FinalizedAt == nil {
			pendingCount++
		}
	}
	s.mu.RUnlock()
	if full || pendingCount >= maxPendingPolls {
		return poll.Poll{}, errors.New("metadata poll inventory bound reached")
	}
	e := &entry{poll: p, config: votelog.Config{Brokers: s.opts.Brokers, Topic: "gqlog_app_" + hex.EncodeToString(token[:]), PollID: token, Partitions: s.opts.Partitions, StartsAt: p.StartsAt, EndsAt: p.EndsAt, AllowedMask: (uint32(1) << len(p.Options)) - 1, Multiple: p.Type == "multiple", BatchSize: s.opts.BatchSize, Linger: s.opts.Linger, QueuePerPartition: s.opts.QueuePerPartition, TransactionTimeout: 10 * time.Second}}
	e.config.AllowRemoteBrokers = s.opts.AllowRemoteBrokers
	description, _ := json.Marshal(p)
	journal, _ := json.Marshal(e.config)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return poll.Poll{}, err
	}
	defer rollback(tx)
	var overlap bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM "+s.table()+" WHERE starts_at < $2 AND ends_at > $1)", p.StartsAt, p.EndsAt).Scan(&overlap); err != nil {
		return poll.Poll{}, err
	}
	if overlap {
		return poll.Poll{}, poll.ErrOverlap
	}
	if _, err = tx.Exec(ctx, "INSERT INTO "+s.table()+" (id,description,journal,starts_at,ends_at) VALUES ($1,$2,$3,$4,$5)", id, description, journal, p.StartsAt, p.EndsAt); err != nil {
		return poll.Poll{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return poll.Poll{}, err
	}
	// Cache the durable definition even if subsequent preparation is cancelled.
	// A failed HTTP creation can be found in List and resumed by maintenance.
	s.mu.Lock()
	s.polls[id] = e
	s.mu.Unlock()
	if err = s.guard.WaitDurable(ctx); err != nil {
		return poll.Poll{}, err
	}
	if err = s.prepare(ctx, e); err != nil {
		return poll.Poll{}, fmt.Errorf("poll persisted; journal preparation incomplete (inspect the poll list): %w", err)
	}
	if !time.Now().Before(p.StartsAt) {
		return poll.Poll{}, errors.New("poll persisted but preparation missed the scheduled start; inspect the poll list")
	}
	return clonePoll(p), nil
}

func (s *Store) Get(ctx context.Context, id string) (poll.Poll, error) {
	if err := ctx.Err(); err != nil {
		return poll.Poll{}, err
	}
	if _, err := parseID(id); err != nil {
		return poll.Poll{}, poll.ErrNotFound
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	e := s.polls[id]
	if e == nil {
		return poll.Poll{}, poll.ErrNotFound
	}
	return clonePoll(e.poll), nil
}

func (s *Store) List(ctx context.Context) ([]poll.Poll, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	result := make([]poll.Poll, 0, len(s.polls))
	for _, e := range s.polls {
		result = append(result, clonePoll(e.poll))
	}
	s.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool { return result[i].StartsAt.After(result[j].StartsAt) })
	if len(result) > 100 {
		result = result[:100]
	}
	return result, nil
}

func (s *Store) Vote(ctx context.Context, id, token string, choices []int) (poll.Receipt, error) {
	if s.closed.Load() || s.ctx.Err() != nil {
		return poll.Receipt{}, ErrOwnership
	}
	if _, err := parseID(id); err != nil {
		return poll.Receipt{Status: "not_found"}, nil
	}
	tokenBytes, err := parseToken(token)
	if err != nil {
		return poll.Receipt{Status: "invalid"}, nil
	}
	normalized, err := poll.NormalizeChoices(choices)
	if err != nil {
		return poll.Receipt{Status: "invalid"}, nil
	}
	s.mu.RLock()
	e := s.polls[id]
	if e == nil {
		s.mu.RUnlock()
		return poll.Receipt{Status: "not_found"}, nil
	}
	w := e.writer
	final := e.poll.FinalizedAt != nil
	starts, ends := e.poll.StartsAt, e.poll.EndsAt
	multiple, optionCount := e.poll.Type == "multiple", len(e.poll.Options)
	s.mu.RUnlock()
	if (!multiple && len(normalized) != 1) || normalized[len(normalized)-1] > optionCount {
		return poll.Receipt{Status: "invalid"}, nil
	}
	now := time.Now()
	if final || !now.Before(ends) {
		return poll.Receipt{Status: "closed"}, nil
	}
	if now.Before(starts) {
		return poll.Receipt{Status: "not_open"}, nil
	}
	if w == nil {
		return poll.Receipt{Status: "busy"}, nil
	}
	var mask uint32
	for _, choice := range normalized {
		mask |= 1 << uint(choice-1)
	}
	receipt, err := w.Submit(ctx, tokenBytes, mask)
	switch {
	case err == nil:
		return poll.Receipt{Status: "recorded", Choices: normalized, AcceptedAt: &receipt.AdmittedAt}, nil
	case errors.Is(err, votelog.ErrClosed):
		return poll.Receipt{Status: "closed"}, nil
	case errors.Is(err, votelog.ErrNotOpen):
		return poll.Receipt{Status: "not_open"}, nil
	case errors.Is(err, votelog.ErrBusy):
		return poll.Receipt{Status: "busy"}, nil
	case errors.Is(err, votelog.ErrInvalid):
		return poll.Receipt{Status: "invalid"}, nil
	default:
		return poll.Receipt{}, errors.New("Kafka attempt outcome unknown")
	}
}

func (s *Store) Results(ctx context.Context, id string) (poll.Results, error) {
	p, err := s.Get(ctx, id)
	if err != nil {
		return poll.Results{}, err
	}
	r := poll.Results{PollID: id, State: p.State(time.Now()), Pending: true, Options: make([]poll.OptionCount, len(p.Options))}
	for i, o := range p.Options {
		r.Options[i] = poll.OptionCount{ID: o.ID, Label: o.Label}
	}
	if p.FinalizedAt == nil {
		return r, nil
	}
	var data []byte
	if err = s.pool.QueryRow(ctx, "SELECT result FROM "+s.table()+" WHERE id=$1 AND finalized_at IS NOT NULL", id).Scan(&data); err != nil {
		return poll.Results{}, err
	}
	r = poll.Results{} // omitted pending:false must not retain the pending default
	if err = json.Unmarshal(data, &r); err != nil {
		return poll.Results{}, err
	}
	if r.Pending || r.State != "final" || r.PollID != id {
		return poll.Results{}, errors.New("invalid persisted final result")
	}
	return r, nil
}

// FinalizeDue publishes only a fully validated CLOSED replay. The exact map
// grows with actual unique full 128-bit IDs, bounded by MaxUnique. No hash-only
// membership test or approximate unique count is used.
func (s *Store) FinalizeDue(parent context.Context) (int, error) {
	ctx, cancel := s.operation(parent)
	defer cancel()
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if err := s.checkOwner(ctx); err != nil {
		return 0, err
	}
	s.mu.RLock()
	pending := make([]*entry, 0)
	for _, e := range s.polls {
		if e.poll.FinalizedAt == nil {
			pending = append(pending, e)
		}
	}
	s.mu.RUnlock()
	count := 0
	for _, e := range pending {
		if e.writer == nil {
			if err := s.prepare(ctx, e); err != nil {
				return count, err
			}
		}
		if time.Now().Before(e.poll.EndsAt) {
			continue
		}
		if _, err := e.writer.Seal(ctx); err != nil {
			return count, err
		}
		r, err := aggregate(ctx, e, s.opts.MaxUnique)
		if err != nil {
			return count, err
		}
		if err = s.checkOwner(ctx); err != nil {
			return count, err
		}
		encoded, _ := json.Marshal(r)
		// Result and finalized_at change in one PostgreSQL transaction. A
		// cancelled commit is retried idempotently by reading the stored result.
		_, err = s.pool.Exec(ctx, "UPDATE "+s.table()+" SET result=$2, finalized_at=$3 WHERE id=$1 AND finalized_at IS NULL", e.poll.ID, encoded, r.CalculatedAt)
		if err != nil {
			return count, err
		}
		if err = s.guard.WaitDurable(ctx); err != nil {
			return count, err
		}
		var finalized time.Time
		if err = s.pool.QueryRow(ctx, "SELECT finalized_at FROM "+s.table()+" WHERE id=$1", e.poll.ID).Scan(&finalized); err != nil {
			return count, err
		}
		s.mu.Lock()
		e.poll.FinalizedAt = &finalized
		s.mu.Unlock()
		e.writer.Close()
		s.mu.Lock()
		e.writer = nil // final metadata must not retain producer buffers/queues
		s.mu.Unlock()
		count++
	}
	return count, nil
}

func aggregate(ctx context.Context, e *entry, maxUnique int) (poll.Results, error) {
	r := poll.Results{PollID: e.poll.ID, State: "final", Options: make([]poll.OptionCount, len(e.poll.Options))}
	for i, o := range e.poll.Options {
		r.Options[i] = poll.OptionCount{ID: o.ID, Label: o.Label}
	}
	seen := make(map[[16]byte]struct{})
	_, err := votelog.Replay(ctx, e.config, func(_ votelog.Position, v votelog.Vote) error {
		if _, ok := seen[v.Token]; ok {
			return nil
		}
		if len(seen) >= maxUnique {
			return errors.New("exact aggregation unique-ID memory bound reached; result remains pending")
		}
		seen[v.Token] = struct{}{}
		r.TotalVotes++
		for i := range r.Options {
			if v.Choice&(1<<uint(i)) != 0 {
				r.Options[i].Votes++
			}
		}
		return nil
	})
	if err != nil {
		return poll.Results{}, err
	}
	r.CalculatedAt = time.Now().UTC()
	return r, nil
}

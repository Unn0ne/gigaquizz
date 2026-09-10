package kafkapoll

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"gigaquizz/internal/httpapi"
	"gigaquizz/internal/poll"
	"gigaquizz/internal/votelog"
	"gigaquizz/internal/web"

	"github.com/twmb/franz-go/pkg/kgo"
)

// This opt-in test uses its own schema/topics and httptest listener. It never
// stops PostgreSQL, Kafka, or another application, and preserves test journals.
func TestHTTPKafkaDurableAttemptsRestartAndExactFinal(t *testing.T) {
	if os.Getenv("GIGAQUIZZ_APP_KAFKA_TEST") != "1" {
		t.Skip("set GIGAQUIZZ_APP_KAFKA_TEST=1, TEST_DATABASE_URL and KAFKA_BROKERS for isolated integration")
	}
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("TEST_DATABASE_URL is required (connection details suppressed)")
	}
	id, _, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	schema := "gq_app_test_" + strings.ReplaceAll(id, "-", "")
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094"
	}
	opts := Options{DatabaseURL: dsn, Schema: schema, Brokers: strings.Split(brokers, ","), Partitions: 2, PreparationLead: 5 * time.Second, MaxUnique: 1000, MaxPolls: 10}
	ctx, cancel := context.WithTimeout(context.Background(), 115*time.Second)
	defer cancel()
	s, err := New(ctx, opts)
	if err != nil {
		t.Fatalf("open isolated repository: %v", err)
	}
	defer func() { s.Close() }()
	if other, err := New(ctx, opts); err == nil {
		other.Close()
		t.Fatal("a second controller acquired the same schema")
	}
	const password = "isolated-kafka-test-password"
	var server *httptest.Server
	var client *http.Client
	startHTTP := func() {
		api, err := httpapi.New(s, s, web.Files, httpapi.Config{AdminPassword: password, PublicURL: "http://127.0.0.1", MaxInflight: 64, OperationTimeout: 10 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		api.SetReady(true)
		server = httptest.NewServer(api.Handler())
		jar, _ := cookiejar.New(nil)
		client = &http.Client{Jar: jar, Timeout: 12 * time.Second}
	}
	startHTTP()
	defer func() { server.Close() }()
	request := func(method, path string, body any) (int, []byte) {
		t.Helper()
		var data []byte
		if body != nil {
			data, _ = json.Marshal(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, server.URL+path, bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		b, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		if err != nil {
			t.Fatal(err)
		}
		return res.StatusCode, b
	}
	login := func() {
		t.Helper()
		if code, _ := request("POST", "/api/admin/login", map[string]string{"password": password}); code != 200 {
			t.Fatal("admin login failed")
		}
	}
	if code, _ := request("GET", "/admin", nil); code != 200 {
		t.Fatal("admin page missing")
	}
	if code, _ := request("GET", "/api/admin/polls", nil); code != 401 {
		t.Fatal("admin data was public")
	}
	login()
	code, data := request("POST", "/api/admin/polls", poll.CreateInput{Question: "Kafka integration", Type: "multiple", Options: []string{"A", "B", "C"}})
	if code != 201 {
		t.Fatalf("create status %d: %s", code, data)
	}
	var p poll.Poll
	if json.Unmarshal(data, &p) != nil || p.ID == "" || p.EndsAt.Sub(p.StartsAt) != time.Minute {
		t.Fatal("invalid poll definition")
	}
	t.Logf("preserved isolated schema %s; poll %s", schema, p.ID)
	path := "/api/polls/" + p.ID + "/votes"
	keyA, _, _ := newID()
	keyB, _, _ := newID()
	keyC, _, _ := newID()
	keyA, keyB, keyC = strings.ReplaceAll(keyA, "-", ""), strings.ReplaceAll(keyB, "-", ""), strings.ReplaceAll(keyC, "-", "")
	body := func(key string, choices ...int) any { return map[string]any{"token": key, "choices": choices} }
	if code, _ := request("POST", path, body(keyA, 1)); code != 425 {
		t.Fatalf("before-start status %d", code)
	}
	if code, _ := request("GET", "/p/"+p.ID, nil); code != 200 {
		t.Fatal("public poll page missing")
	}
	code, data = request("GET", "/api/admin/polls/"+p.ID+"/results", nil)
	var pending poll.Results
	if code != 200 || json.Unmarshal(data, &pending) != nil || !pending.Pending || len(pending.Options) != 3 {
		t.Fatal("results did not expose pending state")
	}
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(time.Until(p.StartsAt) + 10*time.Millisecond):
	}
	if code, _ := request("POST", path, body(keyA, 1, 3)); code != 202 {
		t.Fatalf("durable attempt status %d", code)
	}
	// Concurrent changed repeats are all durable attempts, with the first full
	// token's committed choice staying canonical across ownership transfer.
	var wg sync.WaitGroup
	errs := make(chan error, 12)
	for range 12 {
		wg.Go(func() {
			data, _ := json.Marshal(body(keyA, 2))
			req, _ := http.NewRequestWithContext(ctx, "POST", server.URL+path, bytes.NewReader(data))
			req.Header.Set("Content-Type", "application/json")
			res, err := client.Do(req)
			if err != nil {
				errs <- err
				return
			}
			_, _ = io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != 202 {
				errs <- fmt.Errorf("repeat status %d", res.StatusCode)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	if code, _ := request("POST", path, body(keyB, 2)); code != 202 {
		t.Fatalf("second token status %d", code)
	}
	if code, _ := request("POST", path, body(keyB, 4)); code != 422 {
		t.Fatalf("invalid choice status %d", code)
	}
	// Terminate only this test's dedicated ownership connection, proving that
	// loss of its PG session causes irreversible admission shutdown.
	var terminated bool
	if err = s.pool.QueryRow(ctx, "SELECT pg_terminate_backend($1)", s.owner.PgConn().PID()).Scan(&terminated); err != nil || !terminated {
		t.Fatal("cannot interrupt the test-owned lease session")
	}
	select {
	case <-s.heartbeatDone:
	case <-time.After(5 * time.Second):
		t.Fatal("ownership loss did not close writers")
	}
	if _, err = s.Vote(ctx, p.ID, keyC, []int{2}); !errors.Is(err, ErrOwnership) {
		t.Fatal("lost owner admitted an attempt")
	}
	server.Close()
	s.Close()
	s, err = New(ctx, opts)
	if err != nil {
		t.Fatalf("recover during open poll: %v", err)
	}
	startHTTP()
	login()
	if code, _ := request("POST", path, body(keyA, 2)); code != 202 {
		t.Fatalf("repeat after restart status %d", code)
	}
	if code, _ := request("POST", path, body(keyC, 2, 3)); code != 202 {
		t.Fatalf("new token after restart status %d", code)
	}
	// Close without Seal, then reopen after the immutable deadline. Recovery
	// must preserve ACKs and append CLOSED rather than reopen admission.
	server.Close()
	s.Close()
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(time.Until(p.EndsAt) + 10*time.Millisecond):
	}
	s, err = New(ctx, opts)
	if err != nil {
		t.Fatalf("recover expired unfinished poll: %v", err)
	}
	startHTTP()
	login()
	for _, key := range []string{keyA, keyC} {
		if code, _ := request("POST", path, body(key, 1)); code != 410 {
			t.Fatalf("late vote status %d", code)
		}
	}
	if n, err := s.FinalizeDue(ctx); err != nil || n != 1 {
		t.Fatalf("finalize: %d %v", n, err)
	}
	if n, err := s.FinalizeDue(ctx); err != nil || n != 0 {
		t.Fatalf("idempotent finalize: %d %v", n, err)
	}
	assertFinal := func() {
		t.Helper()
		code, data := request("GET", "/api/admin/polls/"+p.ID+"/results", nil)
		var result poll.Results
		if code != 200 || json.Unmarshal(data, &result) != nil || result.Pending || result.State != "final" || result.TotalVotes != 3 || len(result.Options) != 3 || result.Options[0].Votes != 1 || result.Options[1].Votes != 2 || result.Options[2].Votes != 2 {
			t.Fatalf("incorrect exact final result: code %d, %s", code, data)
		}
		got, err := s.Get(ctx, p.ID)
		if err != nil || got.FinalizedAt == nil {
			t.Fatal("finalized timestamp was not persisted")
		}
	}
	assertFinal()
	server.Close()
	s.Close()
	s, err = New(ctx, opts)
	if err != nil {
		t.Fatalf("reopen final metadata: %v", err)
	}
	startHTTP()
	login()
	assertFinal()
	var rows int
	if err = s.pool.QueryRow(ctx, "SELECT count(*) FROM "+s.table()).Scan(&rows); err != nil || rows != 1 {
		t.Fatal("unexpected metadata inventory")
	}
}

func TestInterruptedInitialPreparationRetainsOldTopic(t *testing.T) {
	if os.Getenv("GIGAQUIZZ_APP_KAFKA_TEST") != "1" {
		t.Skip("requires isolated PostgreSQL/Kafka integration")
	}
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("TEST_DATABASE_URL required")
	}
	id, token, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	opts := Options{DatabaseURL: dsn, Schema: "gq_app_test_" + strings.ReplaceAll(id, "-", ""), Brokers: []string{"127.0.0.1:19092", "127.0.0.1:19093", "127.0.0.1:19094"}, Partitions: 2, MaxPolls: 10, MaxUnique: 1000}
	if raw := os.Getenv("KAFKA_BROKERS"); raw != "" {
		opts.Brokers = strings.Split(raw, ",")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	s, err := New(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	now := time.Now().UTC().Truncate(time.Microsecond)
	p := poll.Poll{ID: id, Question: "Interrupted preparation", Type: "single", Options: []poll.Option{{ID: 1, Label: "A"}, {ID: 2, Label: "B"}}, StartsAt: now.Add(time.Minute), EndsAt: now.Add(2 * time.Minute), CreatedAt: now}
	cfg := votelog.Config{Brokers: opts.Brokers, Topic: "gqlog_app_" + strings.ReplaceAll(id, "-", ""), PollID: token, Partitions: 2, StartsAt: p.StartsAt, EndsAt: p.EndsAt, AllowedMask: 3, BatchSize: 16, Linger: time.Millisecond, QueuePerPartition: 128, TransactionTimeout: 10 * time.Second}
	description, _ := json.Marshal(p)
	journal, _ := json.Marshal(cfg)
	if _, err = s.pool.Exec(ctx, "INSERT INTO "+s.table()+" (id,description,journal,starts_at,ends_at) VALUES ($1,$2,$3,$4,$5)", id, description, journal, p.StartsAt, p.EndsAt); err != nil {
		t.Fatal(err)
	}
	if err = votelog.CreateTopic(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	// Initial setup can leave only one partition with data before a crash.
	// The unprepared journal is deliberately unreadable as a complete poll.
	cl, err := kgo.NewClient(kgo.SeedBrokers(opts.Brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.TransactionalID(cfg.Topic+"-interrupted-fixture"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err = cl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err = cl.ProduceSync(ctx, &kgo.Record{Topic: cfg.Topic, Partition: 0, Value: []byte("interrupted initial control data; no application receipt")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err = cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = New(ctx, opts)
	if err != nil {
		t.Fatalf("incomplete initial preparation was not recoverable: %v", err)
	}
	e := s.polls[id]
	if e == nil || !e.prepared || e.writer == nil || e.config.Topic == cfg.Topic {
		t.Fatal("unprepared topic was reused or published before completed preparation")
	}
	reader, err := kgo.NewClient(kgo.SeedBrokers(opts.Brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{cfg.Topic: {0: kgo.NewOffset().AtStart()}}), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
	fetch := reader.PollRecords(readCtx, 1)
	readCancel()
	if fetch.Err() != nil || len(fetch.Records()) != 1 || string(fetch.Records()[0].Value) != "interrupted initial control data; no application receipt" {
		t.Fatal("old initial journal evidence was not retained")
	}
	preparedTopic := e.config.Topic
	s.Close()
	s, err = New(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if s.polls[id].config.Topic != preparedTopic {
		t.Fatal("prepared topic rotated on restart")
	}
	t.Logf("preserved isolated schema %s, old and recovered topics", opts.Schema)
}

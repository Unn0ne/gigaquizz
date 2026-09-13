package httpapi

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"gigaquizz/internal/poll"
)

func definitionFixture() poll.Poll {
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	return poll.Poll{ID: testPollID, Question: "A public question", Type: "single", Options: []poll.Option{{ID: 1, Label: "One"}, {ID: 2, Label: "Two"}}, StartsAt: now, EndsAt: now.Add(time.Minute), CreatedAt: now.Add(-time.Hour), FinalizedAt: &now}
}

func TestDefinitionIsImmutableCachedAndDoesNotUseRepositoryOnHit(t *testing.T) {
	p := definitionFixture()
	var calls atomic.Int32
	f := &fakeRepository{get: func(context.Context, string) (poll.Poll, error) { calls.Add(1); return p, nil }}
	s := testServer(t, f)
	h := s.Handler()
	url := "/api/polls/" + testPollID + "/definition"
	first := request(h, "GET", url, "", nil)
	if first.Code != 200 || first.Header().Get("Cache-Control") != definitionPolicy || first.Header().Get("ETag") == "" || first.Header().Get("Date") == "" {
		t.Fatalf("definition headers %+v code%d", first.Header(), first.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(first.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"created_at", "finalized_at", "state", "server_time", "total_votes"} {
		if _, ok := body[forbidden]; ok {
			t.Fatalf("mutable/private field %q escaped", forbidden)
		}
	}
	if len(body) != 6 {
		t.Fatalf("unexpected public definition fields: %d", len(body))
	}
	for range cap(s.readInflight) {
		s.readInflight <- struct{}{}
	}
	t.Cleanup(func() {
		for len(s.readInflight) > 0 {
			<-s.readInflight
		}
	})
	second := request(h, "GET", url, "", nil)
	if second.Code != 200 || !bytes.Equal(first.Body.Bytes(), second.Body.Bytes()) || calls.Load() != 1 {
		t.Fatal("hot definition used gated storage or changed")
	}
	conditional := httptest.NewRequest("GET", url, nil)
	conditional.Header.Set("If-None-Match", "W/"+first.Header().Get("ETag"))
	out := httptest.NewRecorder()
	h.ServeHTTP(out, conditional)
	if out.Code != 304 || out.Body.Len() != 0 || out.Header().Get("Date") == "" || calls.Load() != 1 {
		t.Fatal("conditional request lost metadata or used storage")
	}
}

func TestDefinitionErrorsAreNotCachedAndCacheIsBounded(t *testing.T) {
	var available atomic.Bool
	var calls atomic.Int32
	f := &fakeRepository{get: func(_ context.Context, id string) (poll.Poll, error) {
		calls.Add(1)
		if !available.Load() {
			return poll.Poll{}, poll.ErrNotFound
		}
		p := definitionFixture()
		p.ID = id
		return p, nil
	}}
	s, err := New(f, f, fstest.MapFS{"static/index.html": {Data: []byte("fixture")}}, Config{AdminPassword: testPassword, PublicURL: "http://127.0.0.1", MaxInflight: 1, OperationTimeout: time.Second, DefinitionCacheEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	h := s.Handler()
	url := "/api/polls/" + testPollID + "/definition"
	for range 2 {
		w := request(h, "GET", url, "", nil)
		if w.Code != 404 || w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("ETag") != "" {
			t.Fatal("negative response cacheable")
		}
	}
	if calls.Load() != 2 || s.Metrics()["definition_cache_entries"] != 0 {
		t.Fatal("negative lookup cached")
	}
	available.Store(true)
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
		if request(h, "GET", "/api/polls/"+id+"/definition", "", nil).Code != 200 {
			t.Fatal("definition failed")
		}
	}
	if s.Metrics()["definition_cache_entries"] != 2 {
		t.Fatal("definition inventory bound failed")
	}
}

func TestAssetGzipETagNegotiationAndUncachedFailures(t *testing.T) {
	body := bytes.Repeat([]byte("body { color: black; }\n"), 100)
	s, err := New(&fakeRepository{}, &fakeRepository{}, fstest.MapFS{"static/app.css": {Data: body}}, Config{AdminPassword: testPassword, PublicURL: "http://example.com", MaxInflight: 1, OperationTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	h := s.Handler()
	get := func(accept, etag string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/static/app.css", nil)
		r.Header.Set("Accept-Encoding", accept)
		r.Header.Set("If-None-Match", etag)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	raw := get("gzip;q=0, *;q=1", "")
	if raw.Code != 200 || raw.Header().Get("Content-Encoding") != "" || !bytes.Equal(raw.Body.Bytes(), body) {
		t.Fatal("gzip q=0 ignored")
	}
	compressed := get("gzip", "")
	z, err := gzip.NewReader(bytes.NewReader(compressed.Body.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := io.ReadAll(z)
	z.Close()
	if err != nil || !bytes.Equal(decoded, body) || compressed.Header().Get("Content-Encoding") != "gzip" || compressed.Header().Get("Vary") != "Accept-Encoding" || compressed.Header().Get("ETag") == raw.Header().Get("ETag") {
		t.Fatal("gzip representation or ETag mismatch")
	}
	if got := get("gzip", compressed.Header().Get("ETag")); got.Code != 304 || got.Body.Len() != 0 {
		t.Fatal("gzip304 incorrect")
	}
	if got := get("gzip", raw.Header().Get("ETag")); got.Code != 200 {
		t.Fatal("raw ETag incorrectly identifies gzip representation")
	}
	if got := get("gzip;q=0, identity;q=0", ""); got.Code != 406 || got.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("unacceptable coding cached")
	}
	missing := request(h, "GET", "/static/private.env", "", nil)
	if missing.Code != 404 || missing.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("missing asset cacheable")
	}
	m := s.Metrics()
	if m["public_raw_body_bytes"] != uint64(len(body)) || m["public_gzip_body_bytes"] != uint64(compressed.Body.Len()*2) || m["public_response_body_bytes"] != m["public_raw_body_bytes"]+m["public_gzip_body_bytes"] || m["public_not_modified"] != 1 {
		t.Fatalf("body byte accounting mismatch %+v", m)
	}
}

func TestNormalizedOriginsAndUncachedTime(t *testing.T) {
	for _, pair := range [][2]string{{"https://EXAMPLE.COM:443", "https://example.com"}, {"http://example.com:080", "http://EXAMPLE.COM"}, {"http://[0:0:0:0:0:0:0:1]:80", "http://[::1]"}} {
		s, err := New(&fakeRepository{}, &fakeRepository{}, fstest.MapFS{"static/index.html": {Data: []byte("fixture")}}, Config{AdminPassword: testPassword, PublicURL: pair[0], MaxInflight: 1, OperationTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest("POST", "/api/admin/login", strings.NewReader(`{"password":"`+testPassword+`"}`))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Origin", pair[1])
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatalf("equivalent origin rejected: %d", w.Code)
		}
		r.Header.Set("Origin", pair[1]+".evil")
		w = httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != 403 {
			t.Fatal("different origin accepted")
		}
	}
	s := testServer(t, &fakeRepository{})
	w := request(s.Handler(), "GET", "/api/time", "", nil)
	var clock struct {
		ServerTime time.Time `json:"server_time"`
	}
	if json.Unmarshal(w.Body.Bytes(), &clock) != nil || clock.ServerTime.IsZero() || w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Date") == "" {
		t.Fatal("fallback time contract failed")
	}
}

type narrowPublic struct{}

func (narrowPublic) Get(context.Context, string) (poll.Poll, error) {
	return poll.Poll{}, poll.ErrNotFound
}
func (narrowPublic) Vote(context.Context, string, string, []int) (poll.Receipt, error) {
	return poll.Receipt{Status: "busy"}, nil
}
func (narrowPublic) Diagnostics() map[string]uint64 {
	return map[string]uint64{"storage_queued_votes": 12, "secret_poll_id": 99}
}

type narrowAdmin struct{}

func (narrowAdmin) Create(context.Context, poll.CreateInput) (poll.Poll, error) {
	return poll.Poll{}, errors.New("synthetic preparation failure")
}
func (narrowAdmin) List(context.Context) ([]poll.Poll, error) { return nil, nil }
func (narrowAdmin) Results(context.Context, string) (poll.Results, error) {
	return poll.Results{}, poll.ErrNotFound
}

func TestNarrowInterfacesAndMetricsSeparateGateFromQueue(t *testing.T) {
	s, err := New(narrowPublic{}, narrowAdmin{}, fstest.MapFS{"static/index.html": {Data: []byte("fixture")}}, Config{AdminPassword: testPassword, PublicURL: "http://127.0.0.1", MaxInflight: 1, OperationTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	h := s.Handler()
	s.inflight <- struct{}{}
	request(h, "POST", "/api/polls/"+testPollID+"/votes", voteBody(), nil)
	<-s.inflight
	request(h, "POST", "/api/polls/"+testPollID+"/votes", voteBody(), nil)
	login := request(h, "POST", "/api/admin/login", `{"password":"`+testPassword+`"}`, nil)
	request(h, "POST", "/api/admin/polls", `{"question":"Q","type":"single","options":["A","B"]}`, login.Result().Cookies()[0])
	m := s.Metrics()
	if m["http_vote_gate_rejections"] != 1 || m["storage_queue_rejections"] != 1 || m["poll_preparation_failures"] != 1 || m["storage_queued_votes"] != 12 {
		t.Fatalf("wrong separate metrics %+v", m)
	}
	if _, exists := m["secret_poll_id"]; exists {
		t.Fatal("diagnostics allowlist leaked arbitrary field")
	}
}

func TestDefinitionConcurrentPublicationKeepsCompleteBoundedSnapshots(t *testing.T) {
	s := testServer(t, &fakeRepository{})
	s.definitions = newDefinitionCache(4)
	var workers sync.WaitGroup
	for i := range 24 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			p := definitionFixture()
			p.ID = fmt.Sprintf("00000000-0000-4000-8000-%012d", 1+i%8)
			if rep := s.cacheDefinition(p); rep == nil {
				t.Error("valid concurrent publication failed")
			}
			snapshot := s.definitions.current.Load()
			if len(snapshot.entries) > 4 || len(snapshot.entries) != len(snapshot.order) {
				t.Error("incomplete or unbounded snapshot")
			}
			for key, rep := range snapshot.entries {
				var decoded poll.Definition
				if err := json.Unmarshal(rep.raw, &decoded); err != nil || decoded.ID != key {
					t.Error("partially published definition")
				}
			}
		}()
	}
	workers.Wait()
}

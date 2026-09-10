package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"gigaquizz/internal/poll"
)

const testPollID = "00112233-4455-6677-8899-aabbccddeeff"
const testToken = "ffeeddccbbaa99887766554433221100"
const testPassword = "a-unique-long-test-password"

type fakeRepository struct {
	vote  func(context.Context, string, string, []int) (poll.Receipt, error)
	get   func(context.Context, string) (poll.Poll, error)
	calls atomic.Int32
}

func (f *fakeRepository) Create(context.Context, poll.CreateInput) (poll.Poll, error) {
	return poll.Poll{}, nil
}
func (f *fakeRepository) Get(ctx context.Context, id string) (poll.Poll, error) {
	if f.get != nil {
		return f.get(ctx, id)
	}
	return poll.Poll{}, poll.ErrNotFound
}
func (f *fakeRepository) List(context.Context) ([]poll.Poll, error) { return []poll.Poll{}, nil }
func (f *fakeRepository) Results(context.Context, string) (poll.Results, error) {
	return poll.Results{}, nil
}
func (f *fakeRepository) FinalizeDue(context.Context) (int, error) { return 0, nil }
func (f *fakeRepository) Ping(context.Context) error               { return nil }
func (f *fakeRepository) Vote(ctx context.Context, id, token string, choices []int) (poll.Receipt, error) {
	f.calls.Add(1)
	return f.vote(ctx, id, token, choices)
}

func testServer(t *testing.T, f *fakeRepository) *Server {
	t.Helper()
	s, err := New(f, f, fstest.MapFS{"static/index.html": {Data: []byte("hello")}}, Config{AdminPassword: testPassword, PublicURL: "http://127.0.0.1:8080", MaxInflight: 1, OperationTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func request(h http.Handler, method, path, body string, cookie *http.Cookie) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	if cookie != nil {
		r.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func voteBody() string { return `{"token":"` + testToken + `","choices":[1]}` }

func TestAdminAuthenticationAndRevocation(t *testing.T) {
	s := testServer(t, &fakeRepository{})
	h := s.Handler()
	if got := request(h, "GET", "/api/admin/polls", "", nil).Code; got != 401 {
		t.Fatalf("unauthenticated: %d", got)
	}
	w := request(h, "POST", "/api/admin/login", `{"password":"`+testPassword+`"}`, nil)
	if w.Code != 200 {
		t.Fatalf("login: %d %s", w.Code, w.Body)
	}
	cookies := w.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatal("missing session cookie")
	}
	cookie := cookies[0]
	if !cookie.HttpOnly || cookie.SameSite != http.SameSiteStrictMode || cookie.Path != "/api/admin" {
		t.Fatal("unsafe cookie")
	}
	if got := request(h, "GET", "/api/admin/polls", "", cookie).Code; got != 200 {
		t.Fatalf("authenticated: %d", got)
	}
	if got := request(h, "POST", "/api/admin/logout", "{}", cookie).Code; got != 204 {
		t.Fatalf("logout: %d", got)
	}
	if got := request(h, "GET", "/api/admin/polls", "", cookie).Code; got != 401 {
		t.Fatalf("revoked session accepted: %d", got)
	}
}

func TestCrossOriginWritesRejected(t *testing.T) {
	h := testServer(t, &fakeRepository{}).Handler()
	r := httptest.NewRequest("POST", "/api/admin/login", strings.NewReader(`{"password":"`+testPassword+`"}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Origin", "https://untrusted.example")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 403 || len(w.Result().Cookies()) != 0 {
		t.Fatalf("cross-origin login allowed: %d", w.Code)
	}
}

func TestBadPasswordsCannotLockOutCorrectAdministrator(t *testing.T) {
	h := testServer(t, &fakeRepository{}).Handler()
	for range 20 {
		request(h, "POST", "/api/admin/login", `{"password":"wrong"}`, nil)
	}
	w := request(h, "POST", "/api/admin/login", `{"password":"`+testPassword+`"}`, nil)
	if w.Code != 200 {
		t.Fatalf("bad attempts locked out valid administrator: %d", w.Code)
	}
}

func TestPublicReadsCannotQueuePastTheirLimit(t *testing.T) {
	entered := make(chan struct{}, 16)
	release := make(chan struct{})
	completed := make(chan struct{}, 16)
	f := &fakeRepository{get: func(ctx context.Context, id string) (poll.Poll, error) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return poll.Poll{}, poll.ErrNotFound
	}, vote: func(context.Context, string, string, []int) (poll.Receipt, error) {
		return poll.Receipt{Status: "accepted"}, nil
	}}
	s := testServer(t, f)
	h := s.Handler()
	t.Cleanup(func() {
		close(release)
		for range 16 {
			<-completed
		}
	})
	for range 16 {
		go func() {
			request(h, "GET", "/api/polls/"+testPollID, "", nil)
			completed <- struct{}{}
		}()
	}
	for range 16 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("read did not reach storage")
		}
	}
	w := request(h, "GET", "/api/polls/"+testPollID, "", nil)
	if w.Code != 503 {
		t.Fatalf("saturated read path accepted more work: %d", w.Code)
	}
	if got := request(h, "POST", "/api/polls/"+testPollID+"/votes", voteBody(), nil).Code; got != 201 {
		t.Fatalf("read overload blocked the separate vote gate: %d", got)
	}
}

func TestVoteOutcomesAndNoSensitiveErrors(t *testing.T) {
	for _, tc := range []struct {
		status string
		want   int
	}{{"accepted", 201}, {"duplicate", 200}, {"conflict", 409}, {"closed", 410}, {"not_open", 425}, {"invalid", 422}, {"not_found", 404}, {"unknown", 503}} {
		t.Run(tc.status, func(t *testing.T) {
			f := &fakeRepository{vote: func(context.Context, string, string, []int) (poll.Receipt, error) {
				return poll.Receipt{Status: tc.status, Choices: []int{1}}, nil
			}}
			w := request(testServer(t, f).Handler(), "POST", "/api/polls/"+testPollID+"/votes", voteBody(), nil)
			if w.Code != tc.want {
				t.Fatalf("status=%d body=%s", w.Code, w.Body)
			}
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("personal result cacheable")
			}
			if tc.status == "unknown" && !strings.Contains(w.Body.String(), `"outcome":"unknown"`) {
				t.Fatal("uncertain result presented as rejection")
			}
		})
	}
	f := &fakeRepository{vote: func(context.Context, string, string, []int) (poll.Receipt, error) {
		return poll.Receipt{}, errors.New("secret-token SQL user password")
	}}
	w := request(testServer(t, f).Handler(), "POST", "/api/polls/"+testPollID+"/votes", voteBody(), nil)
	if w.Code != 503 || strings.Contains(w.Body.String(), "secret-token") || !strings.Contains(w.Body.String(), `"outcome":"unknown"`) {
		t.Fatalf("unsafe DB error: %s", w.Body)
	}
}

func TestInvalidVoteNeverReachesStorage(t *testing.T) {
	f := &fakeRepository{}
	h := testServer(t, f).Handler()
	for _, body := range []string{`{"token":"invalid","choices":[1]}`, `{"token":"` + testToken + `","choices":[1,1]}`, `{"token":"` + testToken + `","choices":[]}`, `{"token":"` + testToken + `","choices":[1],"ip":"127.0.0.1"}`, voteBody() + ` {}`} {
		if got := request(h, "POST", "/api/polls/"+testPollID+"/votes", body, nil).Code; got < 400 {
			t.Errorf("invalid body accepted: %d", got)
		}
	}
	if f.calls.Load() != 0 {
		t.Fatal("invalid payload reached storage")
	}
}

func TestAdmittedWorkSurvivesClientCancellationAndIsBounded(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	completed := make(chan *httptest.ResponseRecorder, 1)
	f := &fakeRepository{vote: func(ctx context.Context, _ string, _ string, _ []int) (poll.Receipt, error) {
		close(entered)
		<-release
		if ctx.Err() != nil {
			return poll.Receipt{}, ctx.Err()
		}
		return poll.Receipt{Status: "accepted"}, nil
	}}
	s := testServer(t, f)
	h := s.Handler()
	client, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := httptest.NewRequest("POST", "/api/polls/"+testPollID+"/votes", strings.NewReader(voteBody())).WithContext(client)
	r.Header.Set("Content-Type", "application/json")
	go func() { w := httptest.NewRecorder(); h.ServeHTTP(w, r); completed <- w }()
	<-entered
	cancel()
	w := request(h, "POST", "/api/polls/"+testPollID+"/votes", voteBody(), nil)
	if w.Code != 503 || !strings.Contains(w.Body.String(), `"outcome":"not_admitted"`) {
		t.Errorf("unbounded work: %d %s", w.Code, w.Body)
	}
	close(release)
	if w := <-completed; w.Code != 201 {
		t.Fatalf("client cancellation killed work: %d %s", w.Code, w.Body)
	}
	if f.calls.Load() != 1 {
		t.Fatalf("storage calls: %d", f.calls.Load())
	}
}

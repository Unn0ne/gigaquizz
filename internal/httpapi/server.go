package httpapi

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"gigaquizz/internal/poll"
)

type Config struct {
	AdminPassword    string
	PublicURL        string
	MaxInflight      int
	OperationTimeout time.Duration
}

type Server struct {
	votes         poll.Repository
	admin         poll.Repository
	auth          *auth
	origin        string
	files         fs.FS
	inflight      chan struct{}
	readInflight  chan struct{}
	adminInflight chan struct{}
	timeout       time.Duration
	ready         atomic.Bool
	attempts      atomic.Uint64
	accepted      atomic.Uint64
	recorded      atomic.Uint64
	duplicates    atomic.Uint64
	conflicts     atomic.Uint64
	rejected      atomic.Uint64
	unknown       atomic.Uint64
	storageErrors atomic.Uint64
}

func New(votes, admin poll.Repository, files fs.FS, cfg Config) (*Server, error) {
	u, err := url.Parse(cfg.PublicURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("PUBLIC_URL must be an http(s) origin")
	}
	if len(cfg.AdminPassword) < 16 {
		return nil, errors.New("ADMIN_PASSWORD must contain at least 16 bytes")
	}
	if cfg.MaxInflight < 1 || cfg.OperationTimeout <= 0 {
		return nil, errors.New("invalid concurrency or timeout configuration")
	}
	return &Server{votes: votes, admin: admin, files: files, origin: u.Scheme + "://" + u.Host, auth: newAuth(cfg.AdminPassword, u.Scheme == "https"), inflight: make(chan struct{}, cfg.MaxInflight), readInflight: make(chan struct{}, 16), adminInflight: make(chan struct{}, 8), timeout: cfg.OperationTimeout}, nil
}

func (s *Server) SetReady(ready bool) { s.ready.Store(ready) }

func (s *Server) Metrics() map[string]uint64 {
	metrics := map[string]uint64{"vote_http_attempts": s.attempts.Load(), "accepted_responses": s.accepted.Load(), "recorded_responses": s.recorded.Load(), "duplicate_responses": s.duplicates.Load(), "conflict_responses": s.conflicts.Load(), "rejected_attempts": s.rejected.Load(), "unknown_outcomes": s.unknown.Load(), "storage_errors": s.storageErrors.Load(), "inflight": uint64(len(s.inflight))}
	if repository, ok := s.votes.(interface{ Diagnostics() map[string]uint64 }); ok {
		diagnostics := repository.Diagnostics()
		for _, key := range [...]string{
			"db_vote_sql_calls", "db_vote_sql_ns",
			"db_proof_queries", "db_proof_query_ns", "db_proof_retries", "db_proof_wait_ns",
			"db_pool_acquire_count", "db_pool_acquire_ns", "db_pool_empty_acquire_count",
			"db_pool_canceled_acquire_count", "db_pool_new_conns_count",
			"db_pool_acquired_conns", "db_pool_total_conns",
		} {
			if value, exists := diagnostics[key]; exists {
				metrics[key] = value
			}
		}
	}
	return metrics
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if !s.ready.Load() {
			writeError(w, 503, "Хранилище недоступно")
			return
		}
		writeJSON(w, 200, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("POST /api/admin/login", s.login)
	mux.Handle("POST /api/admin/logout", s.protected(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s.auth.logout(w, r); w.WriteHeader(204) })))
	mux.Handle("GET /api/admin/session", s.protected(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]bool{"authenticated": true})
	})))
	mux.Handle("GET /api/admin/metrics", s.protected(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, s.Metrics()) })))
	mux.Handle("POST /api/admin/polls", s.protected(http.HandlerFunc(s.createPoll)))
	mux.Handle("GET /api/admin/polls", s.protected(http.HandlerFunc(s.listPolls)))
	mux.Handle("GET /api/admin/polls/{id}/results", s.protected(http.HandlerFunc(s.results)))
	mux.HandleFunc("GET /api/polls/{id}", s.getPoll)
	mux.HandleFunc("POST /api/polls/{id}/votes", s.vote)
	mux.HandleFunc("GET /{$}", s.page("index.html"))
	mux.HandleFunc("GET /admin", s.page("admin.html"))
	mux.HandleFunc("GET /p/{id}", s.page("poll.html"))
	mux.Handle("GET /static/", http.FileServerFS(s.files))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
		w.Header().Set("Cache-Control", "no-store")
		if strings.HasPrefix(r.URL.Path, "/static/") {
			w.Header().Set("Cache-Control", "public, max-age=300")
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead && !s.sameOrigin(r) {
			writeError(w, 403, "Запрос с другого сайта запрещён")
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *Server) sameOrigin(r *http.Request) bool {
	if r.Header.Get("Sec-Fetch-Site") == "cross-site" {
		return false
	}
	if origin := r.Header.Get("Origin"); origin != "" {
		return origin == s.origin
	}
	if ref := r.Referer(); ref != "" {
		u, err := url.Parse(ref)
		return err == nil && u.Scheme+"://"+u.Host == s.origin
	}
	return true // Non-browser JSON clients, e.g. the load generator.
}

func (s *Server) protected(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.auth.valid(r) {
			writeError(w, 401, "Требуется вход администратора")
			return
		}
		select {
		case s.adminInflight <- struct{}{}:
			defer func() { <-s.adminInflight }()
		default:
			w.Header().Set("Retry-After", "1")
			writeError(w, 503, "Повторите запрос позже")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) page(name string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if name == "poll.html" && !validUUID(r.PathValue("id")) {
			http.NotFound(w, r)
			return
		}
		b, err := fs.ReadFile(s.files, "static/"+name)
		if err != nil {
			writeError(w, 500, "Страница недоступна")
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(b)
	}
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Password string `json:"password"`
	}
	if !decode(w, r, &in, 4096) {
		return
	}
	status := s.auth.login(w, in.Password)
	if status != 200 {
		if status == 429 {
			w.Header().Set("Retry-After", "1")
		}
		writeError(w, status, "Вход не выполнен. Проверьте пароль или повторите позже")
		return
	}
	writeJSON(w, 200, map[string]bool{"authenticated": true})
}

type publicPoll struct {
	poll.Poll
	State      string    `json:"state"`
	ServerTime time.Time `json:"server_time"`
}

func public(p poll.Poll) publicPoll {
	now := time.Now().UTC()
	return publicPoll{Poll: p, State: p.State(now), ServerTime: now}
}

func (s *Server) createPoll(w http.ResponseWriter, r *http.Request) {
	var in poll.CreateInput
	if !decode(w, r, &in, 16384) {
		return
	}
	if err := in.Validate(); err != nil {
		writeError(w, 422, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()
	p, err := s.admin.Create(ctx, in)
	if errors.Is(err, poll.ErrOverlap) {
		writeError(w, 409, "В это время уже запланирован другой опрос")
		return
	}
	if err != nil {
		s.storageErrors.Add(1)
		writeError(w, 503, "Не удалось подтвердить создание опроса. Проверьте список перед повтором")
		return
	}
	writeJSON(w, 201, public(p))
}

func (s *Server) listPolls(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()
	ps, err := s.admin.List(ctx)
	if err != nil {
		s.storageErrors.Add(1)
		writeError(w, 503, "Список временно недоступен")
		return
	}
	items := make([]publicPoll, 0, len(ps))
	for _, p := range ps {
		items = append(items, public(p))
	}
	writeJSON(w, 200, map[string]any{"polls": items, "server_time": time.Now().UTC()})
}

func (s *Server) getPoll(w http.ResponseWriter, r *http.Request) {
	select {
	case s.readInflight <- struct{}{}:
		defer func() { <-s.readInflight }()
	default:
		w.Header().Set("Retry-After", "1")
		writeError(w, 503, "Опрос временно недоступен. Повторите запрос позже")
		return
	}
	id := r.PathValue("id")
	if !validUUID(id) {
		writeError(w, 404, "Опрос не найден")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()
	p, err := s.votes.Get(ctx, id)
	if errors.Is(err, poll.ErrNotFound) {
		writeError(w, 404, "Опрос не найден")
		return
	}
	if err != nil {
		s.storageErrors.Add(1)
		writeError(w, 503, "Опрос временно недоступен")
		return
	}
	writeJSON(w, 200, public(p))
}

func (s *Server) results(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validUUID(id) {
		writeError(w, 404, "Опрос не найден")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()
	res, err := s.admin.Results(ctx, id)
	if errors.Is(err, poll.ErrNotFound) {
		writeError(w, 404, "Опрос не найден")
		return
	}
	if err != nil {
		s.storageErrors.Add(1)
		writeError(w, 503, "Результаты временно недоступны")
		return
	}
	writeJSON(w, 200, res)
}

func (s *Server) vote(w http.ResponseWriter, r *http.Request) {
	s.attempts.Add(1)
	select {
	case s.inflight <- struct{}{}:
		defer func() { <-s.inflight }()
	default:
		s.rejected.Add(1)
		w.Header().Set("Retry-After", "1")
		writeJSON(w, 503, map[string]string{"error": "Сервис занят. Повторите с тем же идентификатором", "outcome": "not_admitted"})
		return
	}
	id := r.PathValue("id")
	if !validUUID(id) {
		s.rejected.Add(1)
		writeError(w, 404, "Опрос не найден")
		return
	}
	var in struct {
		Token   string `json:"token"`
		Choices []int  `json:"choices"`
	}
	if !decode(w, r, &in, 4096) {
		s.rejected.Add(1)
		return
	}
	if !validToken(in.Token) {
		s.rejected.Add(1)
		writeError(w, 422, "Некорректный идентификатор голосования")
		return
	}
	choices, err := poll.NormalizeChoices(in.Choices)
	if err != nil {
		s.rejected.Add(1)
		writeError(w, 422, "Некорректный набор вариантов")
		return
	}
	if r.Context().Err() != nil {
		return
	}
	// The bounded operation is owned by this handler. A disconnected client must
	// not cancel an admitted storage operation; Shutdown still waits for it.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), s.timeout)
	defer cancel()
	receipt, err := s.votes.Vote(ctx, id, in.Token, choices)
	if err != nil {
		s.storageErrors.Add(1)
		s.unknown.Add(1)
		w.Header().Set("Retry-After", "1")
		writeJSON(w, 503, map[string]string{"error": "Исход пока неизвестен. Повторите тот же запрос", "outcome": "unknown"})
		return
	}
	status := 200
	switch receipt.Status {
	case "recorded":
		status = http.StatusAccepted
		s.recorded.Add(1)
	case "accepted":
		status = 201
		s.accepted.Add(1)
	case "duplicate":
		s.duplicates.Add(1)
	case "conflict":
		status = 409
		s.conflicts.Add(1)
	case "closed":
		status = 410
		s.rejected.Add(1)
	case "not_open":
		status = 425
		s.rejected.Add(1)
	case "invalid":
		status = 422
		s.rejected.Add(1)
	case "not_found":
		status = 404
		s.rejected.Add(1)
	case "busy":
		s.rejected.Add(1)
		w.Header().Set("Retry-After", "1")
		writeJSON(w, 503, map[string]string{"error": "Сервис занят. Повторите с тем же идентификатором", "outcome": "not_admitted"})
		return
	default:
		s.unknown.Add(1)
		w.Header().Set("Retry-After", "1")
		writeJSON(w, 503, map[string]string{"error": "Исход пока неизвестен. Повторите тот же запрос", "outcome": "unknown"})
		return
	}
	writeJSON(w, status, receipt)
}

func validToken(s string) bool {
	if len(s) != 32 || s != strings.ToLower(s) || s == "00000000000000000000000000000000" {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
func validUUID(s string) bool {
	return len(s) == 36 && s[8] == '-' && s[13] == '-' && s[18] == '-' && s[23] == '-' && validToken(strings.ReplaceAll(s, "-", ""))
}

func decode(w http.ResponseWriter, r *http.Request, dst any, limit int64) bool {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		writeError(w, 415, "Ожидается application/json")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		writeError(w, 400, "Некорректный JSON или слишком большой запрос")
		return false
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		writeError(w, 400, "Ожидается один JSON-объект")
		return false
	}
	return true
}
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

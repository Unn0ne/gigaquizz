package httpapi

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"math"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gigaquizz/internal/poll"
)

const definitionPolicy = "public, max-age=0, s-maxage=86400, must-revalidate"
const maxDefinitionBytes = 16 * 1024

type representation struct {
	raw, gzip                      []byte
	rawETag, gzipETag, contentType string
}

func prepareRepresentation(raw []byte, contentType string) (*representation, error) {
	var compressed bytes.Buffer
	z, err := gzip.NewWriterLevel(&compressed, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err = z.Write(raw); err != nil {
		return nil, err
	}
	if err = z.Close(); err != nil {
		return nil, err
	}
	etag := func(b []byte) string { sum := sha256.Sum256(b); return `"` + hex.EncodeToString(sum[:]) + `"` }
	return &representation{raw: raw, gzip: compressed.Bytes(), rawETag: etag(raw), gzipETag: etag(compressed.Bytes()), contentType: contentType}, nil
}

func prepareAssets(files fs.FS) (map[string]*representation, error) {
	assets := make(map[string]*representation)
	var total int64
	err := fs.WalkDir(files, "static", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() > 2<<20 || len(assets) >= 128 || total+info.Size() > 8<<20 {
			return errors.New("embedded asset inventory exceeds bound")
		}
		body, err := fs.ReadFile(files, name)
		if err != nil {
			return err
		}
		total += int64(len(body))
		contentType := mime.TypeByExtension(path.Ext(name))
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		rep, err := prepareRepresentation(body, contentType)
		if err != nil {
			return err
		}
		assets["/"+name] = rep
		return nil
	})
	return assets, err
}

type definitionSnapshot struct {
	entries map[string]*representation
	order   []string
}
type definitionCache struct {
	limit                  int
	mu                     sync.Mutex
	current                atomic.Pointer[definitionSnapshot]
	hits, misses, failures atomic.Uint64
}

func newDefinitionCache(limit int) *definitionCache {
	c := &definitionCache{limit: limit}
	c.current.Store(&definitionSnapshot{entries: make(map[string]*representation)})
	return c
}
func (c *definitionCache) get(id string) *representation { return c.current.Load().entries[id] }
func (c *definitionCache) put(id string, rep *representation) *representation {
	c.mu.Lock()
	defer c.mu.Unlock()
	old := c.current.Load()
	if exists := old.entries[id]; exists != nil {
		return exists
	}
	next := &definitionSnapshot{entries: make(map[string]*representation, min(c.limit, len(old.entries)+1))}
	start := 0
	if len(old.order) == c.limit {
		start = 1
	}
	next.order = append(next.order, old.order[start:]...)
	for _, key := range next.order {
		next.entries[key] = old.entries[key]
	}
	next.entries[id] = rep
	next.order = append(next.order, id)
	c.current.Store(next)
	return rep
}

func (s *Server) cacheDefinition(p poll.Poll) *representation {
	if cached := s.definitions.get(p.ID); cached != nil {
		return cached
	}
	if !validUUID(p.ID) || p.StartsAt.IsZero() || p.EndsAt.Sub(p.StartsAt) != time.Minute || len(p.Options) < 2 || len(p.Options) > 20 {
		return nil
	}
	in := poll.CreateInput{Question: p.Question, Type: p.Type}
	for i, o := range p.Options {
		if o.ID != i+1 {
			return nil
		}
		in.Options = append(in.Options, o.Label)
	}
	if in.Validate() != nil {
		return nil
	}
	body, err := json.Marshal(p.Definition())
	if err != nil || len(body) > maxDefinitionBytes {
		return nil
	}
	rep, err := prepareRepresentation(body, "application/json; charset=utf-8")
	if err != nil {
		return nil
	}
	return s.definitions.put(p.ID, rep)
}

func (s *Server) getDefinition(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validUUID(id) {
		writeError(w, 404, "Опрос не найден")
		return
	}
	rep := s.definitions.get(id)
	if rep != nil {
		s.definitions.hits.Add(1)
	} else {
		s.definitions.misses.Add(1)
		select {
		case s.readInflight <- struct{}{}:
		default:
			s.gateReads.Add(1)
			w.Header().Set("Retry-After", "1")
			writeError(w, 503, "Опрос временно недоступен")
			return
		}
		// Keep serialization and response writes outside the scarce storage-read
		// slots. A hot definition bypasses this gate and the repository entirely.
		ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
		p, err := s.votes.Get(ctx, id)
		cancel()
		<-s.readInflight
		if errors.Is(err, poll.ErrNotFound) {
			writeError(w, 404, "Опрос не найден")
			return
		}
		if err != nil || p.ID != id {
			s.definitions.failures.Add(1)
			writeError(w, 503, "Опрос временно недоступен")
			return
		}
		rep = s.cacheDefinition(p)
		if rep == nil {
			s.definitions.failures.Add(1)
			writeError(w, 503, "Опрос временно недоступен")
			return
		}
	}
	s.serveRepresentation(w, r, rep, definitionPolicy)
}

func (s *Server) staticAsset(w http.ResponseWriter, r *http.Request) {
	rep := s.assets[r.URL.Path]
	if rep == nil {
		writeError(w, 404, "Ресурс не найден")
		return
	}
	policy := "public, max-age=300"
	if strings.HasSuffix(r.URL.Path, "/admin.html") {
		policy = "no-store"
	}
	s.serveRepresentation(w, r, rep, policy)
}
func (s *Server) getTime(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Date", time.Now().UTC().Format(http.TimeFormat))
	writeJSON(w, 200, map[string]time.Time{"server_time": time.Now().UTC()})
}

// Accept-Encoding has separate identity/gzip qualities; q=0 is a prohibition,
// including when a wildcard would otherwise allow gzip. No per-request map.
func encodingQualities(raw string) (float64, float64) {
	gz, id, wild := -1.0, -1.0, -1.0
	for raw != "" {
		var part string
		part, raw, _ = strings.Cut(raw, ",")
		coding, params, _ := strings.Cut(strings.TrimSpace(part), ";")
		coding = strings.TrimSpace(coding)
		q := 1.0
		for params != "" {
			var param string
			param, params, _ = strings.Cut(params, ";")
			key, value, ok := strings.Cut(strings.TrimSpace(param), "=")
			if strings.EqualFold(key, "q") {
				n, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
				if !ok || err != nil || math.IsNaN(n) || n < 0 || n > 1 {
					q = 0
				} else {
					q = n
				}
			}
		}
		var dst *float64
		switch {
		case strings.EqualFold(coding, "gzip"):
			dst = &gz
		case strings.EqualFold(coding, "identity"):
			dst = &id
		case coding == "*":
			dst = &wild
		}
		if dst != nil {
			if *dst < 0 {
				*dst = q
			} else {
				*dst = min(*dst, q)
			}
		}
	}
	if gz < 0 {
		gz = max(0, wild)
	}
	if id < 0 {
		id = 1
		if wild == 0 {
			id = 0
		}
	}
	return gz, id
}
func matchesETag(header, etag string) bool {
	for header != "" {
		var part string
		part, header, _ = strings.Cut(header, ",")
		part = strings.TrimSpace(part)
		if part == "*" || strings.TrimPrefix(part, "W/") == etag {
			return true
		}
	}
	return false
}
func (s *Server) serveRepresentation(w http.ResponseWriter, r *http.Request, rep *representation, policy string) {
	gz, identity := encodingQualities(r.Header.Get("Accept-Encoding"))
	if gz <= 0 && identity <= 0 {
		writeError(w, 406, "Поддерживаемая кодировка не выбрана")
		return
	}
	body, etag := rep.raw, rep.rawETag
	compressed := gz > 0 && gz >= identity
	if compressed {
		body, etag = rep.gzip, rep.gzipETag
		w.Header().Set("Content-Encoding", "gzip")
	}
	w.Header().Set("Content-Type", rep.contentType)
	w.Header().Set("Cache-Control", policy)
	w.Header().Set("Vary", "Accept-Encoding")
	w.Header().Set("ETag", etag)
	w.Header().Set("Date", time.Now().UTC().Format(http.TimeFormat))
	if policy != "no-store" && matchesETag(r.Header.Get("If-None-Match"), etag) {
		s.public304.Add(1)
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	if r.Method == http.MethodHead {
		w.WriteHeader(200)
		return
	}
	n, _ := w.Write(body)
	s.publicBytes.Add(uint64(n))
	if compressed {
		s.publicGzipBytes.Add(uint64(n))
	} else {
		s.publicRawBytes.Add(uint64(n))
	}
}

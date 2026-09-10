package votelog

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// Handler is a bounded loopback benchmark API, not the application's public
// voting contract. It deliberately returns 202 for a committed attempt.
func (s *Store) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if s.ctx.Err() != nil || s.failures.Load() > 0 {
			http.Error(w, "unavailable", 503)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /vote", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		var input struct {
			Token  string `json:"token"`
			Choice uint32 `json:"choice"`
		}
		d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256))
		d.DisallowUnknownFields()
		if err := d.Decode(&input); err != nil {
			http.Error(w, `{"error":"invalid"}`, 400)
			return
		}
		var extra any
		if d.Decode(&extra) != io.EOF {
			http.Error(w, `{"error":"invalid"}`, 400)
			return
		}
		token, err := hex.DecodeString(input.Token)
		if err != nil || len(token) != 16 {
			http.Error(w, `{"error":"invalid"}`, 400)
			return
		}
		var key [16]byte
		copy(key[:], token)
		receipt, err := s.Submit(r.Context(), key, input.Choice)
		if err != nil {
			code, label := 503, "unknown"
			switch {
			case errors.Is(err, ErrInvalid):
				code, label = 400, "invalid"
			case errors.Is(err, ErrClosed):
				code, label = 410, "closed"
			case errors.Is(err, ErrNotOpen):
				code, label = 425, "not_open"
			case errors.Is(err, ErrBusy):
				code, label = 429, "busy"
				w.Header().Set("Retry-After", "1")
			}
			w.WriteHeader(code)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": label})
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(receipt)
	})
	return mux
}

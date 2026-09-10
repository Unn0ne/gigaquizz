package httpapi

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"sync"
	"time"
)

const sessionCookie = "gq_admin"

type auth struct {
	mu         sync.Mutex
	password   [32]byte
	sessions   map[[32]byte]time.Time
	tokens     float64
	lastRefill time.Time
	secure     bool
}

func newAuth(password string, secure bool) *auth {
	return &auth{password: sha256.Sum256([]byte(password)), sessions: make(map[[32]byte]time.Time), tokens: 10, lastRefill: time.Now(), secure: secure}
}

func (a *auth) login(w http.ResponseWriter, password string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	a.tokens = min(10, a.tokens+now.Sub(a.lastRefill).Seconds())
	a.lastRefill = now
	got := sha256.Sum256([]byte(password))
	if subtle.ConstantTimeCompare(got[:], a.password[:]) != 1 {
		if a.tokens < 1 {
			return http.StatusTooManyRequests
		}
		a.tokens--
		return http.StatusUnauthorized
	}
	for key, expiry := range a.sessions {
		if !now.Before(expiry) {
			delete(a.sessions, key)
		}
	}
	if len(a.sessions) >= 256 {
		return http.StatusTooManyRequests
	}
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return http.StatusServiceUnavailable
	}
	token := hex.EncodeToString(b[:])
	a.sessions[sha256.Sum256([]byte(token))] = now.Add(8 * time.Hour)
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: token, Path: "/api/admin", HttpOnly: true, Secure: a.secure, SameSite: http.SameSiteStrictMode, MaxAge: 8 * 3600})
	return http.StatusOK
}

func (a *auth) valid(r *http.Request) bool {
	cookie, err := r.Cookie(sessionCookie)
	if err != nil || len(cookie.Value) != 64 {
		return false
	}
	key := sha256.Sum256([]byte(cookie.Value))
	a.mu.Lock()
	defer a.mu.Unlock()
	expiry, ok := a.sessions[key]
	if ok && time.Now().Before(expiry) {
		return true
	}
	delete(a.sessions, key)
	return false
}

func (a *auth) logout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie(sessionCookie); err == nil {
		a.mu.Lock()
		delete(a.sessions, sha256.Sum256([]byte(cookie.Value)))
		a.mu.Unlock()
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/api/admin", HttpOnly: true, Secure: a.secure, SameSite: http.SameSiteStrictMode, MaxAge: -1})
}

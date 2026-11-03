// Package api serves the ssefy HTTP surface.
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/alash3al/ssefy/internal/store"
)

type API struct {
	store   store.Store
	secret  string
	origins []string
	log     *slog.Logger

	done chan struct{}
	once sync.Once
}

func New(st store.Store, secret string, origins []string, log *slog.Logger) *API {
	return &API{
		store:   st,
		secret:  secret,
		origins: origins,
		log:     log,
		done:    make(chan struct{}),
	}
}

// Close ends every open stream so http.Server.Shutdown can return.
func (a *API) Close() {
	a.once.Do(func() { close(a.done) })
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/streams", a.auth(a.create))
	mux.HandleFunc("POST /v1/streams/{id}", a.auth(a.append))
	mux.HandleFunc("DELETE /v1/streams/{id}", a.auth(a.remove))
	mux.HandleFunc("GET /v1/streams/{id}", a.stream)
	mux.HandleFunc("GET /healthz", a.health)
	return a.cors(mux)
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := a.store.Ping(ctx); err != nil {
		a.fail(w, http.StatusServiceUnavailable, "store unavailable")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// auth protects writes. Reads stay open; the stream id is the capability.
func (a *API) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(token), []byte(a.secret)) != 1 {
			a.fail(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next(w, r)
	}
}

func (a *API) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == "" || !a.originAllowed(origin) {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Add("Vary", "Origin")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Idempotency-Key, Last-Event-ID")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (a *API) originAllowed(origin string) bool {
	for _, o := range a.origins {
		if o == "*" || o == origin {
			return true
		}
	}
	return false
}

func (a *API) fail(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

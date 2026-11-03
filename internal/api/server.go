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

type Server struct {
	store   store.Store
	secret  string
	origins []string
	log     *slog.Logger

	done chan struct{}
	once sync.Once
}

func New(st store.Store, secret string, origins []string, log *slog.Logger) *Server {
	return &Server{
		store:   st,
		secret:  secret,
		origins: origins,
		log:     log,
		done:    make(chan struct{}),
	}
}

// Close ends every open stream so http.Server.Shutdown can return.
func (s *Server) Close() {
	s.once.Do(func() { close(s.done) })
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/streams", s.auth(s.create))
	mux.HandleFunc("POST /v1/streams/{id}", s.auth(s.append))
	mux.HandleFunc("DELETE /v1/streams/{id}", s.auth(s.remove))
	mux.HandleFunc("GET /v1/streams/{id}", s.stream)
	mux.HandleFunc("GET /healthz", s.health)
	return s.cors(mux)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		s.fail(w, http.StatusServiceUnavailable, "store unavailable")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// auth protects writes. Reads stay open; the stream id is the capability.
func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(token), []byte(s.secret)) != 1 {
			s.fail(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next(w, r)
	}
}

func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Vary", "Origin")
		origin := r.Header.Get("Origin")
		if origin == "" || !s.originAllowed(origin) {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Idempotency-Key, Last-Event-ID")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) originAllowed(origin string) bool {
	for _, o := range s.origins {
		if o == "*" || o == origin {
			return true
		}
	}
	return false
}

func (s *Server) fail(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

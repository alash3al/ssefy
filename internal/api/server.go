// Package api serves the ssefy HTTP surface.
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alash3al/ssefy/internal/metrics"
	"github.com/alash3al/ssefy/internal/store"
)

// Server serves the ssefy HTTP API.
type Server struct {
	store   store.Store
	secret  string
	origins []string
	log     *slog.Logger

	done chan struct{}
	once sync.Once
}

// New wires a Server to a store. secret guards writes; origins limits CORS.
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

// Handler returns the routes wrapped in CORS and request metrics.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/streams", s.auth(s.create))
	mux.HandleFunc("POST /v1/streams/{id}", s.auth(s.append))
	mux.HandleFunc("POST /v1/push", s.auth(s.push))
	mux.HandleFunc("DELETE /v1/streams/{id}", s.auth(s.remove))
	mux.HandleFunc("GET /v1/streams/{id}", s.stream)
	mux.HandleFunc("GET /v1/streams/{id}/stat", s.auth(s.stat))
	mux.HandleFunc("POST /v1/streams/{id}/rotate", s.auth(s.rotate))
	mux.HandleFunc("POST /v1/streams/{id}/revoke", s.auth(s.revoke))
	mux.HandleFunc("GET /healthz", s.health)
	mux.Handle("GET /metrics", metrics.Handler())
	return instrument(s.cors(mux))
}

// failStream answers the lookup failure every stream endpoint shares.
func (s *Server) failStream(w http.ResponseWriter, err error) bool {
	if errors.Is(err, store.ErrNotFound) {
		s.fail(w, http.StatusNotFound, "stream not found")
		return true
	}
	return false
}

// logStreamErr logs a store failure inside a live stream at a level that
// matches its importance: a deleted stream or a leaving reader is normal.
func (s *Server) logStreamErr(err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, store.ErrNotFound) {
		return
	}
	s.log.Warn("stream store error", "err", err)
}

// instrument records every request, including CORS preflights. For a stream
// the duration is the life of the connection.
func instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &recorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)

		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		status := rec.status
		if status == 0 {
			status = http.StatusOK
		}
		metrics.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(status)).Inc()
		metrics.HTTPDuration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
	})
}

// recorder captures the status but leaves Flush and deadlines to the real
// writer through Unwrap, which http.ResponseController follows.
type recorder struct {
	http.ResponseWriter
	status int
}

func (r *recorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.Ready(ctx); err != nil {
		s.fail(w, http.StatusServiceUnavailable, "not ready")
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

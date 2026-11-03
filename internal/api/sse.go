package api

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/alash3al/ssefy/internal/metrics"
	"github.com/alash3al/ssefy/internal/store"
)

const (
	pageSize    = 200
	pingEvery   = 15 * time.Second
	writeWindow = 30 * time.Second
)

// stream replays stored events after the client's cursor, then follows live
// ones until the client leaves, the stream is deleted, or the server stops.
func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodHead {
		// GET patterns also match HEAD; an endless body for HEAD would only
		// pin a goroutine until the client gives up.
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	id := r.PathValue("id")
	cursor := r.Header.Get("Last-Event-ID")
	if cursor == "" {
		cursor = r.URL.Query().Get("after")
	}

	// Subscribe before reading the high-water mark, or an event committed in
	// between is delivered by nobody.
	live, unsubscribe := s.store.Live(id)
	defer unsubscribe()

	last, err := s.store.LastSeq(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		s.fail(w, http.StatusNotFound, "stream not found")
		return
	}
	if err != nil {
		s.log.Error("last seq", "err", err)
		s.fail(w, http.StatusInternalServerError, "store error")
		return
	}

	var after int64
	switch cursor {
	case "", "now":
		after = last
	case "0":
		after = 0
	default:
		n, err := strconv.ParseInt(cursor, 10, 64)
		if err != nil || n < 0 {
			s.fail(w, http.StatusBadRequest, "invalid cursor")
			return
		}
		after = n
	}
	if after > last {
		after = 0 // defensive: never wait forever behind a bad cursor
	}

	rc := http.NewResponseController(w)
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if err := rc.Flush(); err != nil {
		return
	}
	metrics.Readers.Inc()
	defer metrics.Readers.Dec()

	send := func(seq int64, event, data string) error {
		if err := rc.SetWriteDeadline(time.Now().Add(writeWindow)); err != nil {
			return err
		}
		if _, err := io.WriteString(w, formatEvent(seq, event, data)); err != nil {
			return err
		}
		kind := "stored"
		if seq == 0 {
			kind = "transient"
		}
		metrics.Delivered.WithLabelValues(kind).Inc()
		return rc.Flush()
	}

	sent := after
	following := false
	drain := func() error {
		for {
			rows, err := s.store.Read(r.Context(), id, sent, pageSize)
			if err != nil {
				return err
			}
			for _, row := range rows {
				if err := send(row.Seq, row.Event, row.Data); err != nil {
					return err
				}
				if following {
					metrics.DeliveryLag.Observe(time.Since(row.CreatedAt).Seconds())
				}
				sent = row.Seq
			}
			if len(rows) < pageSize {
				return nil
			}
		}
	}

	// Replay, then go live.
	if err := drain(); err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			s.log.Debug("replay", "err", err)
		}
		return
	}
	following = true

	// No polling: the store wakes us on every commit, and broadcasts to all
	// local readers after a notification outage.
	ping := time.NewTicker(pingEvery)
	defer ping.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-s.done:
			return
		case n, ok := <-live:
			if !ok {
				return
			}
			if n.Transient {
				if err := send(0, n.Event, n.Data); err != nil {
					return
				}
				continue
			}
			if err := drain(); err != nil {
				if !errors.Is(err, store.ErrNotFound) {
					s.log.Debug("live", "err", err)
				}
				return
			}
		case <-ping.C:
			if err := rc.SetWriteDeadline(time.Now().Add(writeWindow)); err != nil {
				return
			}
			if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
				return
			}
			if err := rc.Flush(); err != nil {
				return
			}
		}
	}
}

// formatEvent encodes one SSE frame. Data is line-based, so every line of the
// value needs its own data field; CR and LF both end a line in SSE.
func formatEvent(seq int64, event, data string) string {
	var b strings.Builder
	b.Grow(len(data) + len(event) + 32)
	if seq > 0 {
		b.WriteString("id: ")
		b.WriteString(strconv.FormatInt(seq, 10))
		b.WriteByte('\n')
	}
	b.WriteString("event: ")
	b.WriteString(event)
	b.WriteByte('\n')
	data = strings.ReplaceAll(data, "\r\n", "\n")
	data = strings.ReplaceAll(data, "\r", "\n")
	for _, line := range strings.Split(data, "\n") {
		b.WriteString("data: ")
		b.WriteString(line)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	return b.String()
}

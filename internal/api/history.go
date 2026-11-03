package api

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/alash3al/ssefy/internal/store"
)

// defaultPage is the listing size when the client asks for none.
const defaultPage = 50

type historyEvent struct {
	Seq       int64     `json:"seq"`
	Event     string    `json:"event"`
	Data      string    `json:"data"`
	CreatedAt time.Time `json:"created_at"`
}

type historyResponse struct {
	Events  []historyEvent `json:"events"`
	HasMore bool           `json:"has_more"`
}

// history pages through stored events with the reader credential — the
// stream token or the server secret — one bounded page per call, always
// oldest to newest. It shares only that credential with the live path: a
// stream can be listed with nobody listening.
func (s *Server) history(w http.ResponseWriter, r *http.Request) {
	token := readToken(r)
	if token == "" {
		s.fail(w, http.StatusUnauthorized, "token required")
		return
	}
	dir, cursor, limit, err := parseHistory(r.URL.Query())
	if err != nil {
		s.fail(w, http.StatusBadRequest, err.Error())
		return
	}
	if _, ok := s.authorize(w, r, r.PathValue("id"), token); !ok {
		return
	}

	rows, more, err := s.store.List(r.Context(), r.PathValue("id"), dir, cursor, limit)
	if err != nil {
		s.log.Error("list events", "err", err)
		s.fail(w, http.StatusInternalServerError, "store error")
		return
	}
	// Never nil: an empty page must render as [], not null.
	events := make([]historyEvent, 0, len(rows))
	for _, row := range rows {
		events = append(events, historyEvent{
			Seq:       row.Seq,
			Event:     row.Event,
			Data:      row.Data,
			CreatedAt: row.CreatedAt,
		})
	}
	writeJSON(w, http.StatusOK, historyResponse{Events: events, HasMore: more})
}

// parseHistory validates the listing query: a direction, an exclusive cursor
// and a bounded limit. The page bound matches the SSE replay page.
func parseHistory(q url.Values) (store.Direction, int64, int, error) {
	dir := store.Backward
	switch q.Get("direction") {
	case "", "backward":
	case "forward":
		dir = store.Forward
	default:
		return 0, 0, 0, errors.New("invalid direction")
	}

	var cursor int64
	if raw := q.Get("cursor"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			return 0, 0, 0, errors.New("invalid cursor")
		}
		cursor = n
	}

	limit := defaultPage
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > pageSize {
			return 0, 0, 0, fmt.Errorf("limit must be 1 to %d", pageSize)
		}
		limit = n
	}
	return dir, cursor, limit, nil
}

// trim removes stored events on one side of an exclusive bound — before drops
// the oldest, after the newest — and answers how many went. last_seq is
// preserved and the holes are harmless: every cursor in ssefy is exclusive.
// Live readers are untouched; trim is a history concern, not a delivery one.
func (s *Server) trim(w http.ResponseWriter, r *http.Request) {
	dir, cursor, err := parseTrim(r.URL.Query())
	if err != nil {
		s.fail(w, http.StatusBadRequest, err.Error())
		return
	}
	removed, err := s.store.Trim(r.Context(), r.PathValue("id"), dir, cursor)
	if s.failStream(w, err) {
		return
	}
	if err != nil {
		s.log.Error("trim events", "err", err)
		s.fail(w, http.StatusInternalServerError, "could not trim events")
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"events": removed})
}

// parseTrim takes exactly one exclusive bound: before drops older events,
// after drops newer ones.
func parseTrim(q url.Values) (store.Direction, int64, error) {
	before, after := q.Get("before"), q.Get("after")
	switch {
	case before != "" && after != "":
		return 0, 0, errors.New("before and after are mutually exclusive")
	case before == "" && after == "":
		return 0, 0, errors.New("before or after is required")
	}
	raw, dir := before, store.Backward
	if after != "" {
		raw, dir = after, store.Forward
	}
	cursor, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || cursor < 0 {
		return 0, 0, errors.New("invalid bound")
	}
	return dir, cursor, nil
}

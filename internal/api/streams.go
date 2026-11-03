package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/alash3al/ssefy/internal/metrics"
	"github.com/alash3al/ssefy/internal/store"
)

const (
	maxBody   = 1 << 20
	maxEvents = 500
	maxData   = 64 << 10 // keeps one replay page small in memory
	maxName   = 64
	maxKey    = 255
)

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	id, err := s.store.Create(r.Context())
	if err != nil {
		s.log.Error("create stream", "err", err)
		s.fail(w, http.StatusInternalServerError, "could not create stream")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"id": id})
}

func (s *Server) append(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			s.fail(w, http.StatusRequestEntityTooLarge, "body too large")
			return
		}
		s.fail(w, http.StatusBadRequest, "could not read body")
		return
	}

	events, err := decodeEvents(body)
	if err != nil {
		s.fail(w, http.StatusBadRequest, err.Error())
		return
	}
	idemKey := r.Header.Get("Idempotency-Key")
	if err := validate(events, idemKey); err != nil {
		s.fail(w, http.StatusBadRequest, err.Error())
		return
	}

	seqs, err := s.store.Append(r.Context(), r.PathValue("id"), events, idemKey)
	switch {
	case errors.Is(err, store.ErrNotFound):
		s.fail(w, http.StatusNotFound, "stream not found")
		return
	case errors.Is(err, store.ErrGone):
		s.fail(w, http.StatusGone, "stream deleted")
		return
	case errors.Is(err, store.ErrKeyMismatch):
		s.fail(w, http.StatusUnprocessableEntity, err.Error())
		return
	case errors.Is(err, store.ErrTooLarge):
		s.fail(w, http.StatusRequestEntityTooLarge, err.Error())
		return
	case err != nil:
		s.log.Error("append", "err", err)
		s.fail(w, http.StatusInternalServerError, "could not append")
		return
	}
	if seqs == nil {
		seqs = []int64{}
	}
	kind := "stored"
	if events[0].Transient {
		kind = "transient"
	}
	metrics.Appended.WithLabelValues(kind).Add(float64(len(events)))
	writeJSON(w, http.StatusOK, map[string][]int64{"seqs": seqs})
}

func (s *Server) remove(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Delete(r.Context(), r.PathValue("id")); err != nil {
		s.log.Error("delete stream", "err", err)
		s.fail(w, http.StatusInternalServerError, "could not delete stream")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// decodeEvents accepts one event object or an array of them.
func decodeEvents(body []byte) ([]store.Event, error) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 || bytes.Equal(body, []byte("null")) {
		return nil, errors.New("empty body")
	}
	if body[0] == '[' {
		var events []store.Event
		if err := json.Unmarshal(body, &events); err != nil {
			return nil, errors.New("invalid event array")
		}
		return events, nil
	}
	var event store.Event
	if err := json.Unmarshal(body, &event); err != nil {
		return nil, errors.New("invalid event")
	}
	return []store.Event{event}, nil
}

// validate normalises defaults and rejects anything the store cannot represent.
func validate(events []store.Event, idemKey string) error {
	if len(events) == 0 {
		return errors.New("no events")
	}
	if len(events) > maxEvents {
		return fmt.Errorf("too many events (max %d)", maxEvents)
	}
	if len(idemKey) > maxKey {
		return fmt.Errorf("Idempotency-Key longer than %d bytes", maxKey)
	}

	transient := 0
	collapse := make(map[string]struct{}, len(events))
	for i := range events {
		ev := &events[i]
		if ev.Event == "" {
			ev.Event = "message"
		}
		if len(ev.Event) > maxName {
			return fmt.Errorf("event name longer than %d bytes", maxName)
		}
		for _, r := range ev.Event {
			if r < 0x20 || r == 0x7f {
				return errors.New("event name contains control characters")
			}
		}
		if !utf8.ValidString(ev.Data) || strings.IndexByte(ev.Data, 0) >= 0 {
			return errors.New("data is not valid text")
		}
		if len(ev.Data) > maxData {
			return fmt.Errorf("data longer than %d bytes", maxData)
		}
		if len(ev.CollapseKey) > maxKey {
			return fmt.Errorf("collapse_key longer than %d bytes", maxKey)
		}
		if ev.CollapseKey != "" {
			if _, ok := collapse[ev.CollapseKey]; ok {
				return errors.New("duplicate collapse_key in one push")
			}
			collapse[ev.CollapseKey] = struct{}{}
		}
		if ev.Transient {
			transient++
		}
	}

	if transient == 0 {
		return nil
	}
	if transient != len(events) {
		return errors.New("a push is either all stored or all transient")
	}
	if idemKey != "" {
		return errors.New("transient events cannot use Idempotency-Key")
	}
	for _, ev := range events {
		if ev.CollapseKey != "" {
			return errors.New("transient events cannot use collapse_key")
		}
	}
	return nil
}

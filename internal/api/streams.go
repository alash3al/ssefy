package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/alash3al/ssefy/internal/store"
)

const (
	maxBody      = 1 << 20
	maxEvents    = 500
	maxData      = 256 << 10
	maxName      = 64
	maxKey       = 255
	maxTransient = 7000 // NOTIFY carries transient events; stay under 8000 bytes
	pageSize     = 500
	pingEvery    = 15 * time.Second
	catchupEvery = 5 * time.Second
	writeWindow  = 30 * time.Second
)

func (a *API) create(w http.ResponseWriter, r *http.Request) {
	id, err := a.store.Create(r.Context())
	if err != nil {
		a.log.Error("create stream", "err", err)
		a.fail(w, http.StatusInternalServerError, "could not create stream")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"id": id})
}

func (a *API) append(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			a.fail(w, http.StatusRequestEntityTooLarge, "body too large")
			return
		}
		a.fail(w, http.StatusBadRequest, "could not read body")
		return
	}

	events, err := decodeEvents(body)
	if err != nil {
		a.fail(w, http.StatusBadRequest, err.Error())
		return
	}
	idemKey := r.Header.Get("Idempotency-Key")
	if err := validate(events, idemKey); err != nil {
		a.fail(w, http.StatusBadRequest, err.Error())
		return
	}

	seqs, err := a.store.Append(r.Context(), r.PathValue("id"), events, idemKey)
	switch {
	case errors.Is(err, store.ErrNotFound):
		a.fail(w, http.StatusNotFound, "stream not found")
		return
	case errors.Is(err, store.ErrKeyMismatch):
		a.fail(w, http.StatusUnprocessableEntity, err.Error())
		return
	case errors.Is(err, store.ErrTooLarge):
		a.fail(w, http.StatusRequestEntityTooLarge, err.Error())
		return
	case err != nil:
		a.log.Error("append", "err", err)
		a.fail(w, http.StatusInternalServerError, "could not append")
		return
	}
	if seqs == nil {
		seqs = []int64{}
	}
	writeJSON(w, http.StatusOK, map[string][]int64{"seqs": seqs})
}

func (a *API) remove(w http.ResponseWriter, r *http.Request) {
	if err := a.store.Delete(r.Context(), r.PathValue("id")); err != nil {
		a.log.Error("delete stream", "err", err)
		a.fail(w, http.StatusInternalServerError, "could not delete stream")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) stream(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	cursor := r.Header.Get("Last-Event-ID")
	if cursor == "" {
		cursor = r.URL.Query().Get("after")
	}

	// Subscribe before reading the high-water mark, or an event committed in
	// between is delivered by nobody.
	live, unsubscribe := a.store.Live(id)
	defer unsubscribe()

	last, err := a.store.LastSeq(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		a.fail(w, http.StatusNotFound, "stream not found")
		return
	}
	if err != nil {
		a.log.Error("last seq", "err", err)
		a.fail(w, http.StatusInternalServerError, "store error")
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
			a.fail(w, http.StatusBadRequest, "invalid cursor")
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

	send := func(seq int64, event, data string) error {
		if err := rc.SetWriteDeadline(time.Now().Add(writeWindow)); err != nil {
			return err
		}
		if _, err := io.WriteString(w, formatEvent(seq, event, data)); err != nil {
			return err
		}
		return rc.Flush()
	}

	sent := after
	drain := func() error {
		for {
			rows, err := a.store.Read(r.Context(), id, sent, pageSize)
			if err != nil {
				return err
			}
			for _, row := range rows {
				if err := send(row.Seq, row.Event, row.Data); err != nil {
					return err
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
			a.log.Debug("replay", "err", err)
		}
		return
	}

	ping := time.NewTicker(pingEvery)
	defer ping.Stop()
	catchup := time.NewTicker(catchupEvery)
	defer catchup.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-a.done:
			return
		case n, ok := <-live:
			if !ok || n.Closed {
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
					a.log.Debug("live", "err", err)
				}
				return
			}
		case <-catchup.C:
			// Covers a missed NOTIFY and notices a deleted stream.
			if err := drain(); err != nil {
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

func decodeEvents(body []byte) ([]store.Event, error) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
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
		if !utf8.ValidString(ev.Data) {
			return errors.New("data is not valid UTF-8")
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
			if len(ev.Event)+len(ev.Data) > maxTransient {
				return fmt.Errorf("transient event larger than %d bytes", maxTransient)
			}
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

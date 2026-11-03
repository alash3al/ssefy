package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/alash3al/ssefy/internal/metrics"
)

// maxTargets bounds one batch push: one transaction per target.
const maxTargets = 100

type pushRequest struct {
	Streams []string        `json:"streams"`
	Events  json.RawMessage `json:"events"`
}

type pushResult struct {
	Seqs  []int64 `json:"seqs,omitempty"`
	Error string  `json:"error,omitempty"`
}

// push appends the same events to several streams in one call. Targets are
// independent: one failure does not undo the others, and the response says
// which stream got what. A retry with the same Idempotency-Key returns the
// original seqs for the streams that already succeeded.
func (s *Server) push(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	var req pushRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.fail(w, http.StatusBadRequest, "could not read body")
		return
	}
	if len(req.Streams) == 0 {
		s.fail(w, http.StatusBadRequest, "no streams")
		return
	}
	if len(req.Streams) > maxTargets {
		s.fail(w, http.StatusBadRequest, fmt.Sprintf("more than %d streams", maxTargets))
		return
	}
	seen := make(map[string]struct{}, len(req.Streams))
	for _, id := range req.Streams {
		if _, dup := seen[id]; dup {
			s.fail(w, http.StatusBadRequest, "duplicate stream")
			return
		}
		seen[id] = struct{}{}
	}

	events, err := decodeEvents(req.Events)
	if err != nil {
		s.fail(w, http.StatusBadRequest, err.Error())
		return
	}
	idemKey := r.Header.Get("Idempotency-Key")
	if err := validate(events, idemKey); err != nil {
		s.fail(w, http.StatusBadRequest, err.Error())
		return
	}

	kind := "stored"
	if events[0].Transient {
		kind = "transient"
	}
	results := make(map[string]pushResult, len(req.Streams))
	for _, id := range req.Streams {
		seqs, err := s.store.Append(r.Context(), id, events, idemKey)
		if err != nil {
			results[id] = pushResult{Error: err.Error()}
			continue
		}
		metrics.Appended.WithLabelValues(kind).Add(float64(len(events)))
		if seqs == nil {
			seqs = []int64{}
		}
		results[id] = pushResult{Seqs: seqs}
	}
	writeJSON(w, http.StatusOK, map[string]map[string]pushResult{"results": results})
}

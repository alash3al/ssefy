package api

import (
	"net/http"
	"time"
)

type statResponse struct {
	LastSeq   int64      `json:"last_seq"`
	CreatedAt time.Time  `json:"created_at"`
	DeletedAt *time.Time `json:"deleted_at"`
	Readable  bool       `json:"readable"`
}

func (s *Server) stat(w http.ResponseWriter, r *http.Request) {
	st, err := s.store.Stat(r.Context(), r.PathValue("id"))
	if s.failStream(w, err) {
		return
	}
	if err != nil {
		s.log.Error("stat", "err", err)
		s.fail(w, http.StatusInternalServerError, "store error")
		return
	}
	writeJSON(w, http.StatusOK, statResponse{
		LastSeq:   st.LastSeq,
		CreatedAt: st.CreatedAt,
		DeletedAt: st.DeletedAt,
		Readable:  st.Readable,
	})
}

func (s *Server) rotate(w http.ResponseWriter, r *http.Request) {
	token, err := s.store.Rotate(r.Context(), r.PathValue("id"))
	if s.failStream(w, err) {
		return
	}
	if err != nil {
		s.log.Error("rotate", "err", err)
		s.fail(w, http.StatusInternalServerError, "could not rotate token")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"token": token})
}

func (s *Server) revoke(w http.ResponseWriter, r *http.Request) {
	err := s.store.Revoke(r.Context(), r.PathValue("id"))
	if s.failStream(w, err) {
		return
	}
	if err != nil {
		s.log.Error("revoke", "err", err)
		s.fail(w, http.StatusInternalServerError, "could not revoke token")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

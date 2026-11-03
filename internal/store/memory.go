package store

import (
	"context"
	"sort"
	"sync"
	"time"
)

// memory is the development store: no PostgreSQL, one process.
type memory struct {
	mu      sync.Mutex
	hub     *hub
	streams map[string]*memStream
}

type memStream struct {
	token     string
	lastSeq   int64
	createdAt time.Time
	deletedAt *time.Time
	events    []memEvent
	idem      map[string][]int64
}

type memEvent struct {
	Row         Row
	CollapseKey string
}

// NewMemory returns the development store: one process, no PostgreSQL.
func NewMemory() Store {
	return &memory{hub: newHub(), streams: make(map[string]*memStream)}
}

func (m *memory) Create(context.Context) (string, string, error) {
	id, err := newStreamID()
	if err != nil {
		return "", "", err
	}
	token, err := newToken()
	if err != nil {
		return "", "", err
	}
	m.mu.Lock()
	m.streams[id] = &memStream{
		token:     token,
		createdAt: time.Now(),
		idem:      make(map[string][]int64),
	}
	m.mu.Unlock()
	return id, token, nil
}

func (m *memory) Append(_ context.Context, id string, events []Event, idemKey string) ([]int64, error) {
	m.mu.Lock()
	s, ok := m.streams[id]
	if !ok {
		m.mu.Unlock()
		return nil, ErrNotFound
	}
	if s.deletedAt != nil {
		m.mu.Unlock()
		return nil, ErrGone
	}
	if events[0].Transient {
		m.mu.Unlock()
		if _, err := encodeTransients(id, events); err != nil {
			return nil, err
		}
		for _, ev := range events {
			m.hub.notify(id, Notice{Transient: true, Event: ev.Event, Data: ev.Data})
		}
		return nil, nil
	}
	if idemKey != "" {
		if prev, ok := s.idem[idemKey]; ok {
			m.mu.Unlock()
			return prev, nil
		}
	}
	for _, ev := range events {
		if ev.CollapseKey == "" {
			continue
		}
		kept := s.events[:0]
		for _, e := range s.events {
			if e.CollapseKey != ev.CollapseKey {
				kept = append(kept, e)
			}
		}
		s.events = kept
	}
	seqs := make([]int64, len(events))
	for i, ev := range events {
		s.lastSeq++
		seqs[i] = s.lastSeq
		s.events = append(s.events, memEvent{
			Row:         Row{Seq: s.lastSeq, Event: ev.Event, Data: ev.Data, CreatedAt: time.Now()},
			CollapseKey: ev.CollapseKey,
		})
	}
	if idemKey != "" {
		s.idem[idemKey] = seqs
	}
	m.mu.Unlock()

	m.hub.notify(id, Notice{})
	return seqs, nil
}

func (m *memory) Delete(_ context.Context, id string) error {
	m.mu.Lock()
	s, ok := m.streams[id]
	if !ok || s.deletedAt != nil {
		m.mu.Unlock()
		return nil
	}
	now := time.Now()
	s.deletedAt = &now
	s.events = nil
	s.idem = map[string][]int64{}
	m.mu.Unlock()
	m.hub.close(id)
	return nil
}

func (m *memory) Read(_ context.Context, id string, after int64, limit int) ([]Row, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.streams[id]
	if !ok || s.deletedAt != nil {
		return nil, ErrNotFound
	}
	first := sort.Search(len(s.events), func(i int) bool { return s.events[i].Row.Seq > after })
	var out []Row
	for _, e := range s.events[first:] {
		if len(out) == limit {
			break
		}
		out = append(out, e.Row)
	}
	return out, nil
}

func (m *memory) OpenReader(_ context.Context, id, token string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.streams[id]
	if !ok || token == "" || s.token != token {
		return 0, ErrNotFound
	}
	if s.deletedAt != nil {
		return 0, ErrGone
	}
	return s.lastSeq, nil
}

func (m *memory) Rotate(_ context.Context, id string) (string, error) {
	token, err := newToken()
	if err != nil {
		return "", err
	}
	m.mu.Lock()
	s, ok := m.streams[id]
	if !ok {
		m.mu.Unlock()
		return "", ErrNotFound
	}
	if s.deletedAt != nil {
		m.mu.Unlock()
		return "", ErrGone
	}
	s.token = token
	m.mu.Unlock()
	m.hub.close(id)
	return token, nil
}

func (m *memory) Revoke(_ context.Context, id string) error {
	m.mu.Lock()
	s, ok := m.streams[id]
	if !ok {
		m.mu.Unlock()
		return ErrNotFound
	}
	if s.deletedAt != nil {
		m.mu.Unlock()
		return ErrGone
	}
	s.token = ""
	m.mu.Unlock()
	m.hub.close(id)
	return nil
}

func (m *memory) Stat(_ context.Context, id string) (Stat, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.streams[id]
	if !ok {
		return Stat{}, ErrNotFound
	}
	return Stat{
		LastSeq:   s.lastSeq,
		CreatedAt: s.createdAt,
		DeletedAt: s.deletedAt,
		Readable:  s.token != "" && s.deletedAt == nil,
	}, nil
}

func (m *memory) Live(stream string) (<-chan Notice, func()) { return m.hub.subscribe(stream) }
func (m *memory) Ready(context.Context) error                { return nil }
func (m *memory) Close() error                               { return nil }

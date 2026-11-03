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
	tokenHash string
	lastSeq   int64
	createdAt time.Time
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
		tokenHash: hashToken(token),
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
	_, ok := m.streams[id]
	delete(m.streams, id)
	m.mu.Unlock()
	if ok {
		m.hub.close(id)
	}
	return nil
}

func (m *memory) Read(_ context.Context, id string, after int64, limit int) ([]Row, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.streams[id]
	if !ok {
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

func (m *memory) List(_ context.Context, id string, dir Direction, cursor int64, limit int) ([]Row, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.streams[id]
	if !ok {
		return nil, false, nil // the handler authorizes before listing
	}

	var page []memEvent
	more := false
	if dir == Forward {
		first := sort.Search(len(s.events), func(i int) bool { return s.events[i].Row.Seq > cursor })
		last := first + limit
		if last > len(s.events) {
			last = len(s.events)
		}
		more = first+limit < len(s.events)
		page = s.events[first:last]
	} else {
		end := len(s.events)
		if cursor > 0 {
			end = sort.Search(len(s.events), func(i int) bool { return s.events[i].Row.Seq >= cursor })
		}
		start := end - limit
		if start < 0 {
			start = 0
		}
		more = end-limit > 0
		page = s.events[start:end] // already oldest to newest
	}
	var out []Row
	for _, e := range page {
		out = append(out, e.Row)
	}
	return out, more, nil
}

// Trim removes events on one side of an exclusive bound, like postgres.
func (m *memory) Trim(_ context.Context, id string, dir Direction, cursor int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.streams[id]
	if !ok {
		return 0, ErrNotFound
	}
	kept := s.events[:0]
	var removed int64
	for _, e := range s.events {
		if (dir == Backward && e.Row.Seq < cursor) || (dir == Forward && e.Row.Seq > cursor) {
			removed++
			continue
		}
		kept = append(kept, e)
	}
	s.events = kept
	return removed, nil
}

func (m *memory) OpenReader(_ context.Context, id, token string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.streams[id]
	if !ok {
		return 0, ErrNotFound
	}
	if token == "" || !sameToken(s.tokenHash, hashToken(token)) {
		return 0, ErrUnauthorized
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
	s.tokenHash = hashToken(token)
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
	s.tokenHash = ""
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
		Readable:  s.tokenHash != "",
	}, nil
}

func (m *memory) Live(stream string) (<-chan Notice, func()) { return m.hub.subscribe(stream) }
func (m *memory) Ready(context.Context) error                { return nil }
func (m *memory) Close() error                               { return nil }

package store

import (
	"context"
	"sync"
)

// memory is the development store: no PostgreSQL, one process.
type memory struct {
	mu      sync.Mutex
	hub     *hub
	streams map[string]*memStream
}

type memStream struct {
	lastSeq int64
	events  []memEvent
	idem    map[string]memIdem
}

type memEvent struct {
	Row         Row
	CollapseKey string
}

type memIdem struct {
	hash string
	seqs []int64
}

func NewMemory() Store {
	return &memory{hub: newHub(), streams: make(map[string]*memStream)}
}

func (m *memory) Create(context.Context) (string, error) {
	id, err := newID()
	if err != nil {
		return "", err
	}
	m.mu.Lock()
	m.streams[id] = &memStream{idem: make(map[string]memIdem)}
	m.mu.Unlock()
	return id, nil
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
		for _, ev := range events {
			m.hub.notify(id, Notice{Transient: true, Event: ev.Event, Data: ev.Data})
		}
		return nil, nil
	}
	hash := hashEvents(events)
	if idemKey != "" {
		if prev, ok := s.idem[idemKey]; ok {
			m.mu.Unlock()
			if prev.hash != hash {
				return nil, ErrKeyMismatch
			}
			return prev.seqs, nil
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
			Row:         Row{Seq: s.lastSeq, Event: ev.Event, Data: ev.Data},
			CollapseKey: ev.CollapseKey,
		})
	}
	if idemKey != "" {
		if s.idem == nil {
			s.idem = make(map[string]memIdem)
		}
		s.idem[idemKey] = memIdem{hash: hash, seqs: seqs}
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
	var out []Row
	for _, e := range s.events {
		if e.Row.Seq <= after {
			continue
		}
		out = append(out, e.Row)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (m *memory) LastSeq(_ context.Context, id string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.streams[id]
	if !ok {
		return 0, ErrNotFound
	}
	return s.lastSeq, nil
}

func (m *memory) Live(stream string) (<-chan Notice, func()) {
	return m.hub.subscribe(stream)
}

func (m *memory) Ping(context.Context) error { return nil }
func (m *memory) Close() error               { return nil }

package store

import "sync"

// hub fans notices out to the readers attached to this process.
// Sends never block: a slow reader catches up on its next poll.
type hub struct {
	mu sync.Mutex
	m  map[string]map[chan Notice]struct{}
}

func newHub() *hub {
	return &hub{m: make(map[string]map[chan Notice]struct{})}
}

func (h *hub) subscribe(stream string) (<-chan Notice, func()) {
	ch := make(chan Notice, 8)
	h.mu.Lock()
	if h.m[stream] == nil {
		h.m[stream] = make(map[chan Notice]struct{})
	}
	h.m[stream][ch] = struct{}{}
	h.mu.Unlock()

	return ch, func() {
		h.mu.Lock()
		if _, ok := h.m[stream][ch]; ok {
			delete(h.m[stream], ch)
			close(ch)
			if len(h.m[stream]) == 0 {
				delete(h.m, stream)
			}
		}
		h.mu.Unlock()
	}
}

func (h *hub) notify(stream string, n Notice) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.m[stream] {
		if !push(ch, n) {
			// A stored wake-up must not be lost. End the slow reader so it
			// reconnects with Last-Event-ID and catches up from the store.
			delete(h.m[stream], ch)
			close(ch)
		}
	}
	if len(h.m[stream]) == 0 {
		delete(h.m, stream)
	}
}

// push reports whether the subscriber should stay. Only a transient notice may
// be dropped: it has no stored copy to fall back to.
func push(ch chan Notice, n Notice) bool {
	select {
	case ch <- n:
		return true
	default:
		return n.Transient
	}
}

// close ends every reader of a deleted stream immediately.
func (h *hub) close(stream string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.m[stream] {
		close(ch)
	}
	delete(h.m, stream)
}

// broadcast wakes every local reader. Used after the notification listener
// reconnects, when commits made during the outage have no notice.
func (h *hub) broadcast() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for stream, chans := range h.m {
		for ch := range chans {
			if !push(ch, Notice{}) {
				delete(chans, ch)
				close(ch)
			}
		}
		if len(chans) == 0 {
			delete(h.m, stream)
		}
	}
}

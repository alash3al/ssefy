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
		select {
		case ch <- n:
		default: // full: the reader re-reads from the store anyway
		}
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
	for _, chans := range h.m {
		for ch := range chans {
			select {
			case ch <- Notice{}:
			default:
			}
		}
	}
}

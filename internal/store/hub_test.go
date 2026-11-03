package store

import "testing"

func TestHubDropPolicy(t *testing.T) {
	h := newHub()
	live, stop := h.subscribe("s")
	defer stop()

	// Transient notices are best-effort: overflow drops them but keeps the reader.
	for range 8 {
		h.notify("s", Notice{Transient: true})
	}
	h.notify("s", Notice{Transient: true})
	for range 8 {
		<-live
	}
	select {
	case _, ok := <-live:
		if !ok {
			t.Fatal("transient overflow closed the reader")
		}
	default:
	}

	// A stored wake-up must never be dropped: a full reader is disconnected so
	// it reconnects with Last-Event-ID and catches up from the store.
	for range 8 {
		h.notify("s", Notice{Transient: true})
	}
	h.notify("s", Notice{})
	for range 8 {
		<-live
	}
	if _, ok := <-live; ok {
		t.Fatal("stored wake-up was dropped instead of closing the reader")
	}
}

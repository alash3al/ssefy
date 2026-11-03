package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// testLifecycle runs the same contract against every Store implementation.
func testLifecycle(t *testing.T, s Store) {
	t.Helper()
	ctx := context.Background()

	if err := s.Ready(ctx); err != nil {
		t.Fatalf("ready = %v", err)
	}

	id, err := s.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(id) != 32 {
		t.Fatalf("id %q is not 32 hex chars", id)
	}

	events := []Event{{Event: "message", Data: "one"}, {Event: "tick", Data: "two"}}
	seqs, err := s.Append(ctx, id, events, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(seqs) != 2 || seqs[0] != 1 || seqs[1] != 2 {
		t.Fatalf("seqs = %v, want [1 2]", seqs)
	}

	rows, err := s.Read(ctx, id, 0, 10)
	if err != nil || len(rows) != 2 || rows[0].Data != "one" || rows[1].Event != "tick" {
		t.Fatalf("read = %v, %v", rows, err)
	}
	if rows, _ := s.Read(ctx, id, 1, 10); len(rows) != 1 || rows[0].Seq != 2 {
		t.Fatalf("read after 1 = %v", rows)
	}
	if last, err := s.LastSeq(ctx, id); err != nil || last != 2 {
		t.Fatalf("last = %d, %v", last, err)
	}

	// Idempotency: same key and body returns the original seqs, a different body fails.
	if _, err := s.Append(ctx, id, []Event{{Event: "message", Data: "three"}}, "k"); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Append(ctx, id, []Event{{Event: "message", Data: "three"}}, "k"); err != nil || got[0] != 3 {
		t.Fatalf("replay = %v, %v", got, err)
	}

	// An array push with one key works and replays the same seqs.
	batch := []Event{{Event: "message", Data: "m1"}, {Event: "message", Data: "m2"}}
	firstSeqs, err := s.Append(ctx, id, batch, "multi")
	if err != nil {
		t.Fatal(err)
	}
	if len(firstSeqs) != 2 || firstSeqs[1] != firstSeqs[0]+1 {
		t.Fatalf("batch seqs = %v", firstSeqs)
	}
	againSeqs, err := s.Append(ctx, id, batch, "multi")
	if err != nil || len(againSeqs) != 2 || againSeqs[0] != firstSeqs[0] {
		t.Fatalf("batch retry = %v, %v", againSeqs, err)
	}

	// Collapsing an event away must not erase its idempotency record.
	lastBefore, _ := s.LastSeq(ctx, id)
	keyed := []Event{{Event: "presence", Data: "first", CollapseKey: "cz"}}
	keyedSeqs, err := s.Append(ctx, id, keyed, "collapse-keyed")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(ctx, id, []Event{{Event: "presence", Data: "second", CollapseKey: "cz"}}, ""); err != nil {
		t.Fatal(err)
	}
	retrySeqs, err := s.Append(ctx, id, keyed, "collapse-keyed")
	if err != nil || len(retrySeqs) != 1 || retrySeqs[0] != keyedSeqs[0] {
		t.Fatalf("collapse retry = %v, %v", retrySeqs, err)
	}
	if last, _ := s.LastSeq(ctx, id); last != lastBefore+2 {
		t.Fatalf("collapse retry inserted: last = %d, want %d", last, lastBefore+2)
	}

	// Collapse keeps the newest event with a key.
	if _, err := s.Append(ctx, id, []Event{{Event: "state", Data: "a", CollapseKey: "c"}}, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(ctx, id, []Event{{Event: "state", Data: "b", CollapseKey: "c"}}, ""); err != nil {
		t.Fatal(err)
	}
	rows, _ = s.Read(ctx, id, 0, 100)
	collapsed := 0
	for _, r := range rows {
		if r.Event == "state" {
			collapsed++
			if r.Data != "b" {
				t.Fatalf("collapsed event = %q, want b", r.Data)
			}
		}
	}
	if collapsed != 1 {
		t.Fatalf("collapsed events = %d, want 1", collapsed)
	}

	// A transient event has to fit one wake-up payload.
	oversized := []Event{{Event: "typing", Data: strings.Repeat("x", 8000), Transient: true}}
	if _, err := s.Append(ctx, id, oversized, ""); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized transient = %v", err)
	}

	// Transient events are delivered live and never stored.
	live, stop := s.Live(id)
	defer stop()
	if _, err := s.Append(ctx, id, []Event{{Event: "typing", Data: "x", Transient: true}}, ""); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-live:
		if !n.Transient || n.Data != "x" {
			t.Fatalf("notice = %+v", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no transient notice")
	}

	before, _ := s.Read(ctx, id, 0, 100)
	if _, err := s.Append(ctx, id, []Event{{Event: "typing", Data: "y", Transient: true}}, ""); err != nil {
		t.Fatal(err)
	}
	after, _ := s.Read(ctx, id, 0, 100)
	if len(before) != len(after) {
		t.Fatalf("transient event was stored: %d -> %d", len(before), len(after))
	}

	if err := s.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Read(ctx, id, 0, 10); !errors.Is(err, ErrNotFound) {
		t.Fatalf("read after delete = %v", err)
	}
	if _, err := s.LastSeq(ctx, id); !errors.Is(err, ErrGone) {
		t.Fatalf("last after delete = %v", err)
	}
	if _, err := s.Append(ctx, id, events, ""); !errors.Is(err, ErrGone) {
		t.Fatalf("append after delete = %v", err)
	}
	if _, err := s.Append(ctx, id, []Event{{Event: "typing", Data: "x", Transient: true}}, ""); !errors.Is(err, ErrGone) {
		t.Fatalf("transient after delete = %v", err)
	}
}

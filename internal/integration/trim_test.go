package integration

import (
	"slices"
	"testing"
	"time"
)

// TestTrimBoundariesAndRetries pins the exclusive bounds, the preserved
// last_seq, and the idempotent retry of a batch whose events were trimmed
// away: the record answers with the original seqs, the events are not
// recreated.
func TestTrimBoundariesAndRetries(t *testing.T) {
	requireDB(t)
	a, b := newPair(t)
	id, token := a.create(t)
	original := b.appendEvents(t, id, `[{"data":"1"},{"data":"2"},{"data":"3"},{"data":"4"},{"data":"5"}]`, "origin")

	if n := a.trim(t, id, "before=3"); n != 2 {
		t.Fatalf("before=3 removed %d", n)
	}
	if n := b.trim(t, id, "after=4"); n != 1 {
		t.Fatalf("after=4 removed %d", n)
	}

	// The boundary events survive: both bounds are exclusive.
	if seqs := seqsOf(a.allEvents(t, id, token)); !slices.Equal(seqs, []int64{3, 4}) {
		t.Fatalf("history between the bounds = %v", seqs)
	}

	// Sequence numbers are never reset by a trim.
	if seqs := b.appendEvents(t, id, `{"data":"6"}`, ""); !slices.Equal(seqs, []int64{6}) {
		t.Fatalf("append after trim = %v", seqs)
	}

	// Retrying the original key returns its recorded seqs and does not
	// recreate the trimmed events.
	retry := a.appendEvents(t, id, `[{"data":"1"},{"data":"2"},{"data":"3"},{"data":"4"},{"data":"5"}]`, "origin")
	if !slices.Equal(retry, original) {
		t.Fatalf("retry after trim = %v, original = %v", retry, original)
	}
	if seqs := seqsOf(b.allEvents(t, id, token)); !slices.Equal(seqs, []int64{3, 4, 6}) {
		t.Fatalf("history after retry = %v", seqs)
	}
}

// TestTrimKeepsLiveReaders trims while a reader is connected: no close
// notice, no disconnect, and delivery of the next event continues.
func TestTrimKeepsLiveReaders(t *testing.T) {
	requireDB(t)
	a, b := newPair(t)
	id, token := a.create(t)
	b.appendEvents(t, id, `[{"data":"1"},{"data":"2"}]`, "")

	reader := openSSE(t, a, "/v1/streams/"+id+"?token="+token+"&after=0", nil)
	if f := reader.next(t, 5*time.Second); f.id != 1 {
		t.Fatalf("frame 1 = %+v", f)
	}
	if f := reader.next(t, 5*time.Second); f.id != 2 {
		t.Fatalf("frame 2 = %+v", f)
	}

	before := a.metric(t, "ssefy_readers")
	a.trim(t, id, "before=2") // remove seq 1 under the reader's feet
	if got := a.metric(t, "ssefy_readers"); got != before {
		t.Fatalf("readers %v then %v: trim disconnected a live reader", before, got)
	}
	if reader.dead() {
		t.Fatal("reader ended")
	}

	b.appendEvents(t, id, `{"data":"3"}`, "")
	if f := reader.next(t, 5*time.Second); f.id != 3 || f.data != "3" {
		t.Fatalf("delivery after trim = %+v", f)
	}
	if reader.dead() {
		t.Fatal("reader ended after delivery")
	}
}

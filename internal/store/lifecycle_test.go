package store

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"uuid"
)

// testLifecycle runs the same contract against every Store implementation.
func testLifecycle(t *testing.T, s Store) {
	t.Helper()
	ctx := context.Background()

	if err := s.Ready(ctx); err != nil {
		t.Fatalf("ready = %v", err)
	}

	id, token, err := s.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if parsed, err := uuid.Parse(id); err != nil || parsed[6]>>4 != 7 {
		t.Fatalf("id %q is not a UUIDv7", id)
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
	if last, err := s.OpenReader(ctx, id, token); err != nil || last != 2 {
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
	lastBefore, _ := s.OpenReader(ctx, id, token)
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
	if last, _ := s.OpenReader(ctx, id, token); last != lastBefore+2 {
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

	// rotate must also end the local reader, not just invalidate the token.
	waitClosed := func() {
		for {
			select {
			case _, ok := <-live:
				if !ok {
					return
				}
			case <-time.After(time.Second):
				t.Fatal("rotate did not close the local reader")
			}
		}
	}

	// Token lifecycle: a wrong token is invisible, rotate invalidates, revoke
	// stops reads until the next rotate.
	if _, err := s.OpenReader(ctx, id, "wrong"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("wrong token = %v", err)
	}
	rotated, err := s.Rotate(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	waitClosed()
	if _, err := s.OpenReader(ctx, id, token); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("old token after rotate = %v", err)
	}
	if _, err := s.OpenReader(ctx, id, rotated); err != nil {
		t.Fatalf("rotated token = %v", err)
	}
	if err := s.Revoke(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.OpenReader(ctx, id, rotated); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("token after revoke = %v", err)
	}
	if st, err := s.Stat(ctx, id); err != nil || st.Readable {
		t.Fatalf("stat after revoke = %+v, %v", st, err)
	}
	token, err = s.Rotate(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if st, err := s.Stat(ctx, id); err != nil || !st.Readable {
		t.Fatalf("stat after rotate = %+v, %v", st, err)
	}

	if err := s.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Read(ctx, id, 0, 10); !errors.Is(err, ErrNotFound) {
		t.Fatalf("read after delete = %v", err)
	}
	if _, err := s.OpenReader(ctx, id, token); !errors.Is(err, ErrNotFound) {
		t.Fatalf("open after delete = %v", err)
	}
	if _, err := s.Stat(ctx, id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stat after delete = %v", err)
	}
	if _, err := s.Append(ctx, id, events, ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("append after delete = %v", err)
	}
	if _, err := s.Append(ctx, id, []Event{{Event: "typing", Data: "x", Transient: true}}, ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("transient after delete = %v", err)
	}
}

// testList pins the history contract against every store: pages are oldest to
// newest in both directions, cursors are exclusive, and the limit+1 probe gets
// has_more right on the exact-boundary case.
func testList(t *testing.T, s Store) {
	t.Helper()
	ctx := context.Background()
	id, _, err := s.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var events []Event
	for i := 1; i <= 5; i++ {
		events = append(events, Event{Event: "message", Data: "m" + strconv.Itoa(i)})
	}
	if _, err := s.Append(ctx, id, events, ""); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name   string
		dir    Direction
		cursor int64
		limit  int
		want   []int64
		more   bool
	}{
		{"newest page", Backward, 0, 3, []int64{3, 4, 5}, true},
		{"walk older", Backward, 3, 3, []int64{1, 2}, false},
		{"exactly limit backward", Backward, 4, 3, []int64{1, 2, 3}, false},
		{"from first", Forward, 0, 3, []int64{1, 2, 3}, true},
		{"walk newer", Forward, 3, 2, []int64{4, 5}, false},
		{"exactly limit forward", Forward, 2, 3, []int64{3, 4, 5}, false},
		{"empty forward", Forward, 5, 3, []int64{}, false},
	}
	for _, c := range cases {
		rows, more, err := s.List(ctx, id, c.dir, c.cursor, c.limit)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		got := make([]int64, len(rows))
		for i, row := range rows {
			got[i] = row.Seq
		}
		if !slices.Equal(got, c.want) || more != c.more {
			t.Errorf("%s = %v, more=%v; want %v, %v", c.name, got, more, c.want, c.more)
		}
	}

	// Listing never probes existence: the handler authorizes first.
	rows, more, err := s.List(ctx, "01900000-0000-7000-8000-000000000000", Backward, 0, 10)
	if err != nil || len(rows) != 0 || more {
		t.Fatalf("unknown stream = %v, %v, %v", rows, more, err)
	}
}

// testTrim pins the trim contract on every store: exclusive bounds on either
// side, last_seq preserved, ghost idempotency seqs, and the range extremes.
func testTrim(t *testing.T, s Store) {
	t.Helper()
	ctx := context.Background()
	id, token, err := s.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var events []Event
	for i := 1; i <= 5; i++ {
		events = append(events, Event{Event: "message", Data: "m" + strconv.Itoa(i)})
	}
	if _, err := s.Append(ctx, id, events, "first"); err != nil {
		t.Fatal(err)
	}

	// Drop the oldest two: seq < 3.
	removed, err := s.Trim(ctx, id, Backward, 3)
	if err != nil || removed != 2 {
		t.Fatalf("trim backward = %d, %v", removed, err)
	}
	rows, _, err := s.List(ctx, id, Forward, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || rows[0].Seq != 3 || rows[2].Seq != 5 {
		t.Fatalf("after backward trim = %d rows, first %d last %d", len(rows), rows[0].Seq, rows[len(rows)-1].Seq)
	}

	// last_seq survives a trim, so the next append continues the sequence.
	last, err := s.OpenReader(ctx, id, token)
	if err != nil || last != 5 {
		t.Fatalf("last after trim = %d, %v", last, err)
	}
	seqs, err := s.Append(ctx, id, []Event{{Event: "message", Data: "m6"}}, "")
	if err != nil || len(seqs) != 1 || seqs[0] != 6 {
		t.Fatalf("append after trim = %v, %v", seqs, err)
	}

	// Drop the newest two: seq > 4 removes 5 and 6.
	removed, err = s.Trim(ctx, id, Forward, 4)
	if err != nil || removed != 2 {
		t.Fatalf("trim forward = %d, %v", removed, err)
	}
	rows, _, err = s.List(ctx, id, Forward, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Seq != 3 || rows[1].Seq != 4 {
		t.Fatalf("after forward trim = %v", rows)
	}

	// An idempotent retry reports the seqs it originally returned even though
	// trim removed those rows: the record is the answer, and it must not
	// re-insert anything.
	retry, err := s.Append(ctx, id, events, "first")
	if err != nil || len(retry) != 5 || retry[0] != 1 || retry[4] != 5 {
		t.Fatalf("idempotent retry after trim = %v, %v", retry, err)
	}
	rows, _, err = s.List(ctx, id, Forward, 0, 10)
	if err != nil || len(rows) != 2 {
		t.Fatalf("retry re-inserted after trim: %v", rows)
	}

	// Range extremes: nothing is older than seq 0, everything is newer.
	if removed, err := s.Trim(ctx, id, Backward, 0); err != nil || removed != 0 {
		t.Fatalf("backward from 0 = %d, %v", removed, err)
	}
	if removed, err := s.Trim(ctx, id, Forward, 0); err != nil || removed != 2 {
		t.Fatalf("forward from 0 = %d, %v", removed, err)
	}
	if _, err := s.Trim(ctx, "01900000-0000-7000-8000-000000000000", Backward, 5); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown stream = %v", err)
	}
}

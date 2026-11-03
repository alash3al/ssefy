package integration

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestPaginationUnderConcurrentWrites pages backward and forward while both
// nodes keep appending: pages stay ascending, cursors stay exclusive, and
// nothing is duplicated or skipped.
func TestPaginationUnderConcurrentWrites(t *testing.T) {
	requireDB(t)
	a, b := newPair(t)
	id, token := a.create(t)

	const total = 40
	errc := make(chan error, 1)
	go func() {
		defer close(errc)
		for i := 1; i <= total; i++ {
			n := a
			if i%2 == 0 {
				n = b
			}
			if _, err := n.appendQuiet(id, fmt.Sprintf(`{"data":"event-%d"}`, i), ""); err != nil {
				errc <- err
				return
			}
		}
	}()

	// Walk backward while the writer runs. New events land above the walk,
	// so the walked range must come out complete and contiguous. The first
	// page may race the writer's first commit, so wait for one event.
	waitFor(t, 10*time.Second, func() bool {
		return len(a.history(t, id, token, "limit=1").Events) > 0
	})
	var walk []int64
	var cursor int64
	for {
		var p historyPage
		if cursor == 0 {
			p = a.history(t, id, token, "limit=10")
		} else {
			p = a.history(t, id, token, fmt.Sprintf("direction=backward&cursor=%d&limit=10", cursor))
		}
		seqs := pageSeqs(p)
		if len(seqs) == 0 {
			t.Fatal("empty continuation page: has_more promised a row below the cursor")
		}
		if !slices.IsSorted(seqs) {
			t.Fatalf("backward page not ascending: %v", seqs)
		}
		if cursor != 0 && seqs[len(seqs)-1] >= cursor {
			t.Fatalf("cursor %d repeated: %v", cursor, seqs)
		}
		walk = append(walk, seqs...)
		if !p.HasMore {
			break
		}
		cursor = seqs[0]
	}
	if err := <-errc; err != nil {
		t.Fatalf("writer: %v", err)
	}

	sorted := slices.Clone(walk)
	slices.Sort(sorted)
	if !slices.Equal(sorted, seqRange(1, len(sorted))) {
		t.Fatalf("backward walk incomplete or duplicated: %v", sorted)
	}

	// Forward from the beginning once the writer is done: exact pages.
	var fwd []int64
	cursor = 0
	for {
		p := b.history(t, id, token, fmt.Sprintf("direction=forward&cursor=%d&limit=7", cursor))
		fwd = append(fwd, pageSeqs(p)...)
		if !p.HasMore {
			break
		}
		cursor = p.Events[len(p.Events)-1].Seq
	}
	if !slices.Equal(fwd, seqRange(1, total)) {
		t.Fatalf("forward walk = %v", fwd)
	}

	// Forward has_more:false means caught up *as of now*: a late append
	// through the other node re-opens the same cursor.
	if p := a.history(t, id, token, "direction=forward&cursor=0&limit=200"); p.HasMore {
		t.Fatal("expected to be caught up")
	}
	b.appendEvents(t, id, `{"data":"late"}`, "")
	p := a.history(t, id, token, "direction=forward&cursor="+strconv.Itoa(total)+"&limit=200")
	if len(p.Events) != 1 || p.Events[0].Seq != total+1 {
		t.Fatalf("page after a late append = %+v", p.Events)
	}
}

func pageSeqs(p historyPage) []int64 {
	return seqsOf(p.Events)
}

// TestPaginationEdges covers the empty stream, the exact page boundary (the
// limit+1 probe), and sequence holes left by collapse and trim.
func TestPaginationEdges(t *testing.T) {
	requireDB(t)
	a, _ := newPair(t)

	// Empty stream, both directions.
	empty, etok := a.create(t)
	for _, q := range []string{"", "direction=forward"} {
		if p := a.history(t, empty, etok, q); len(p.Events) != 0 || p.HasMore {
			t.Fatalf("empty history %q = %+v", q, p)
		}
	}

	// Exact boundary: ten events, so limit=10 answers has_more:false and
	// limit=9 answers has_more:true, in both directions.
	bounded, btok := a.create(t)
	var batch []string
	for i := 1; i <= 10; i++ {
		batch = append(batch, fmt.Sprintf(`{"data":"b%d"}`, i))
	}
	a.appendEvents(t, bounded, "["+strings.Join(batch, ",")+"]", "")
	if p := a.history(t, bounded, btok, "limit=10"); len(p.Events) != 10 || p.HasMore {
		t.Fatalf("limit=10 = %d events, more=%v", len(p.Events), p.HasMore)
	}
	if p := a.history(t, bounded, btok, "limit=9"); len(p.Events) != 9 || !p.HasMore {
		t.Fatalf("limit=9 = %d events, more=%v", len(p.Events), p.HasMore)
	}
	if p := a.history(t, bounded, btok, "limit=9&direction=backward"); len(p.Events) != 9 || !p.HasMore {
		t.Fatalf("backward limit=9 = %d events, more=%v", len(p.Events), p.HasMore)
	}

	// Sequence holes: a collapse removes seq 1, a trim removes seq 2, and
	// pages skip what is gone while cursors stay exclusive across the gap.
	gappy, gtok := a.create(t)
	a.appendEvents(t, gappy, `[{"data":"old","collapse_key":"k"},{"data":"keep"}]`, "")
	a.appendEvents(t, gappy, `{"data":"new","collapse_key":"k"}`, "") // seq 3, removes seq 1
	if n := a.trim(t, gappy, "before=3"); n != 1 {                    // removes seq 2
		t.Fatalf("trim removed %d", n)
	}
	if seqs := pageSeqs(a.history(t, gappy, gtok, "direction=forward&cursor=0&limit=10")); !slices.Equal(seqs, []int64{3}) {
		t.Fatalf("gappy forward = %v", seqs)
	}
	if seqs := pageSeqs(a.history(t, gappy, gtok, "direction=forward&cursor=1&limit=10")); !slices.Equal(seqs, []int64{3}) {
		t.Fatalf("across the hole = %v", seqs)
	}
	if seqs := pageSeqs(a.history(t, gappy, gtok, "direction=backward&limit=10")); !slices.Equal(seqs, []int64{3}) {
		t.Fatalf("gappy backward = %v", seqs)
	}
	// Sequence numbers are never reset by the holes above them.
	if seqs := a.appendEvents(t, gappy, `{"data":"after"}`, ""); !slices.Equal(seqs, []int64{4}) {
		t.Fatalf("append after holes = %v", seqs)
	}
}

package integration

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestAmbiguousAppendRetry simulates a publisher that never learns whether
// its append committed: the request is abandoned mid-flight, then retried
// with the same key through the other node. Whichever way the race ended,
// exactly one batch may exist and the retry must answer with its seqs.
func TestAmbiguousAppendRetry(t *testing.T) {
	requireDB(t)
	a, b := newPair(t)
	id, token := a.create(t)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		req, err := http.NewRequestWithContext(ctx, "POST", a.base+"/v1/streams/"+id,
			strings.NewReader(`[{"data":"p1"},{"data":"p2"},{"data":"p3"}]`))
		if err != nil {
			return
		}
		req.Header.Set("Authorization", "Bearer "+secret)
		req.Header.Set("Idempotency-Key", "ambiguous")
		resp, err := sseClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
	time.Sleep(50 * time.Millisecond) // let the request race the commit
	cancel()

	seqs := b.appendEvents(t, id, `[{"data":"p1"},{"data":"p2"},{"data":"p3"}]`, "ambiguous")
	if len(seqs) != 3 {
		t.Fatalf("retry seqs = %v", seqs)
	}
	events := b.allEvents(t, id, token)
	if len(events) != 3 {
		t.Fatalf("history = %d events, want exactly one batch: %+v", len(events), events)
	}
	for _, p := range []string{"p1", "p2", "p3"} {
		if n := countData(events, p); n != 1 {
			t.Fatalf("payload %q stored %d times", p, n)
		}
	}
}

// TestSimultaneousRetries fires the same idempotent append at both nodes at
// once: every caller must get the same sequences, and only one batch may be
// stored.
func TestSimultaneousRetries(t *testing.T) {
	requireDB(t)
	a, b := newPair(t)
	id, token := a.create(t)
	body := `[{"data":"a"},{"data":"b"}]`

	var wg sync.WaitGroup
	var mu sync.Mutex
	var got [][]int64
	for i := range 4 {
		wg.Go(func() {
			n := a
			if i%2 == 1 {
				n = b
			}
			seqs, err := n.appendQuiet(id, body, "same-key")
			if err != nil {
				t.Errorf("concurrent retry: %v", err)
				return
			}
			mu.Lock()
			got = append(got, seqs)
			mu.Unlock()
		})
	}
	wg.Wait()

	for i, seqs := range got {
		if len(seqs) != 2 || !slices.Equal(seqs, got[0]) {
			t.Fatalf("retry %d = %v, first = %v", i, seqs, got[0])
		}
	}
	events := a.allEvents(t, id, token)
	if len(events) != 2 || countData(events, "a") != 1 || countData(events, "b") != 1 {
		t.Fatalf("history after simultaneous retries = %+v", events)
	}
}

// TestBatchPushPartialFailureAndRecovery pushes to a valid stream, a stream
// that does not exist yet, and a deleted one. ssefy has no half-broken
// target: the only transient failure is a stream that is not there yet, so
// recovery means creating one (ids are server-minted, so the replacement has
// a new id). Every target is reported independently, and a retry with the
// same key must not duplicate the stream that already got the events.
func TestBatchPushPartialFailureAndRecovery(t *testing.T) {
	requireDB(t)
	a, b := newPair(t)
	good, token := a.create(t)
	deleted, _ := a.create(t)
	a.remove(t, deleted)
	missing := "01999999-9999-7999-8999-999999999999"

	key := "batch-retry"
	events := `{"events":[{"data":"x"},{"data":"y"}],"streams":["` + good + `","` + missing + `","` + deleted + `"]}`
	first := a.push(t, events, key)
	if r := first[good]; len(r.Seqs) != 2 || r.Error != "" {
		t.Fatalf("good target = %+v", r)
	}
	if r := first[missing]; r.Error != "stream not found" {
		t.Fatalf("missing target = %+v", r)
	}
	if r := first[deleted]; r.Error != "stream not found" {
		t.Fatalf("deleted target = %+v", r)
	}

	// Recovery: the stream that was missing is created, and the same batch
	// is retried with it in place of the missing id.
	recovered, rtoken := b.create(t)
	events = `{"events":[{"data":"x"},{"data":"y"}],"streams":["` + good + `","` + recovered + `","` + deleted + `"]}`
	second := b.push(t, events, key)
	if r := second[good]; !slices.Equal(r.Seqs, first[good].Seqs) {
		t.Fatalf("good target duplicated: %+v then %+v", first[good], r)
	}
	if r := second[recovered]; len(r.Seqs) != 2 || r.Error != "" {
		t.Fatalf("recovered target = %+v", r)
	}
	if r := second[deleted]; r.Error != "stream not found" {
		t.Fatalf("deleted target after retry = %+v", r)
	}

	goodEvents := a.allEvents(t, good, token)
	if len(goodEvents) != 2 || countData(goodEvents, "x") != 1 || countData(goodEvents, "y") != 1 {
		t.Fatalf("good stream history = %+v", goodEvents)
	}
	recoveredEvents := b.allEvents(t, recovered, rtoken)
	if len(recoveredEvents) != 2 {
		t.Fatalf("recovered stream history = %+v", recoveredEvents)
	}
}

// TestCrashAtomicityAndRestart kills a node with SIGKILL — once after a
// commit, once while an append is in flight — and retries through the other
// node. A batch is either entirely committed or entirely absent, and
// committed history must survive the restart.
func TestCrashAtomicityAndRestart(t *testing.T) {
	requireDB(t)
	a, b := newPair(t)
	id, token := a.create(t)

	// After a commit: kill, restart, retry through the other node.
	original := a.appendEvents(t, id, `[{"data":"c1"},{"data":"c2"},{"data":"c3"}]`, "committed")
	a.kill()
	restarted := startNode(t, "restarted")
	if seqs := b.appendEvents(t, id, `[{"data":"c1"},{"data":"c2"},{"data":"c3"}]`, "committed"); !slices.Equal(seqs, original) {
		t.Fatalf("retry after crash = %v, original = %v", seqs, original)
	}
	events := restarted.allEvents(t, id, token)
	if len(events) != 3 {
		t.Fatalf("history after restart = %+v", events)
	}
	for _, p := range []string{"c1", "c2", "c3"} {
		if n := countData(events, p); n != 1 {
			t.Fatalf("payload %q stored %d times after restart", p, n)
		}
	}

	// During a commit: kill mid-flight, retry, and expect exactly one batch
	// whichever way the race ended.
	doomed := startNode(t, "doomed")
	go func() {
		// The kill often wins the race before the response arrives; that is
		// exactly the ambiguity being retried.
		_, _ = doomed.appendQuiet(id, `[{"data":"k1"},{"data":"k2"}]`, "midflight")
	}()
	time.Sleep(20 * time.Millisecond)
	doomed.kill()

	seqs := b.appendEvents(t, id, `[{"data":"k1"},{"data":"k2"}]`, "midflight")
	if len(seqs) != 2 {
		t.Fatalf("retry seqs = %v", seqs)
	}
	events = b.allEvents(t, id, token)
	if len(events) != 5 {
		t.Fatalf("history = %d events, want exactly the two batches: %+v", len(events), events)
	}
	if countData(events, "k1") != 1 || countData(events, "k2") != 1 {
		t.Fatalf("mid-flight batch partially stored: %+v", events)
	}
}

// TestBatchPushAcrossNodes is the two-node flavor of batch idempotency: the
// first push lands on one node, the retry on the other, and the seqs must
// match. (Covered by the recovery test above; this keeps a minimal cross-node
// case that does not depend on target failures.)
func TestBatchPushAcrossNodes(t *testing.T) {
	requireDB(t)
	a, b := newPair(t)
	s1, t1 := a.create(t)
	s2, t2 := a.create(t)
	body := fmt.Sprintf(`{"events":[{"data":"z"}],"streams":["%s","%s"]}`, s1, s2)

	first := a.push(t, body, "cross")
	second := b.push(t, body, "cross")
	for _, results := range []map[string]pushResult{first, second} {
		if len(results) != 2 {
			t.Fatalf("results = %+v", results)
		}
	}
	for id, token := range map[string]string{s1: t1, s2: t2} {
		if !slices.Equal(first[id].Seqs, second[id].Seqs) {
			t.Fatalf("stream %s: %v then %v", id, first[id].Seqs, second[id].Seqs)
		}
		events := b.allEvents(t, id, token)
		if len(events) != 1 || countData(events, "z") != 1 {
			t.Fatalf("stream %s history = %+v", id, events)
		}
	}
}

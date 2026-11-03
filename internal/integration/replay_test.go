package integration

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

// TestReconnectReplayEitherNode disconnects a reader at sequence N, appends
// more through the other node, and reconnects to each node with
// Last-Event-ID: everything retained after N must arrive, in order, with N
// itself never repeated.
func TestReconnectReplayEitherNode(t *testing.T) {
	requireDB(t)
	a, b := newPair(t)
	id, token := a.create(t)
	b.appendEvents(t, id, `[{"data":"1"},{"data":"2"},{"data":"3"}]`, "")

	first := openSSE(t, a, "/v1/streams/"+id+"?token="+token+"&after=0", nil)
	for i := int64(1); i <= 3; i++ {
		if f := first.next(t, 5*time.Second); f.id != i {
			t.Fatalf("frame %d = %+v", i, f)
		}
	}
	first.close()

	b.appendEvents(t, id, `[{"data":"4"},{"data":"5"},{"data":"6"}]`, "")

	// The other node, resuming from the header a browser sends on reconnect.
	second := openSSE(t, b, "/v1/streams/"+id+"?token="+token, map[string]string{"Last-Event-ID": "3"})
	for i := int64(4); i <= 6; i++ {
		if f := second.next(t, 5*time.Second); f.id != i {
			t.Fatalf("node b frame %d = %+v", i, f)
		}
	}
	if !second.quiet(500 * time.Millisecond) {
		t.Fatal("unexpected extra frame after replay")
	}
	second.close()

	// The original node, with the query-string cursor.
	third := openSSE(t, a, "/v1/streams/"+id+"?token="+token+"&after=3", nil)
	for i := int64(4); i <= 6; i++ {
		if f := third.next(t, 5*time.Second); f.id != i {
			t.Fatalf("node a frame %d = %+v", i, f)
		}
	}
	if !third.quiet(500 * time.Millisecond) {
		t.Fatal("unexpected extra frame after replay")
	}
}

// TestReplayToLiveNoGaps connects readers while events are still being
// appended across both nodes: subscribing before reading the high-water mark
// must make the replay→live transition seamless for every reader.
func TestReplayToLiveNoGaps(t *testing.T) {
	requireDB(t)
	a, b := newPair(t)
	id, token := a.create(t)

	const total = 20
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 1; i <= total; i++ {
			n := a
			if i%2 == 0 {
				n = b
			}
			if _, err := n.appendQuiet(id, fmt.Sprintf(`{"data":"event-%d"}`, i), ""); err != nil {
				t.Errorf("writer: %v", err)
				return
			}
		}
	}()

	readers := []*sse{
		openSSE(t, a, "/v1/streams/"+id+"?token="+token+"&after=0", nil),
		openSSE(t, a, "/v1/streams/"+id+"?token="+token+"&after=0", nil),
		openSSE(t, b, "/v1/streams/"+id+"?token="+token+"&after=0", nil),
		openSSE(t, b, "/v1/streams/"+id+"?token="+token+"&after=0", nil),
	}
	<-done

	for ri, r := range readers {
		for i := int64(1); i <= total; i++ {
			if f := r.next(t, 10*time.Second); f.id != i {
				t.Fatalf("reader %d frame %d = %+v", ri, i, f)
			}
		}
		if !r.quiet(500 * time.Millisecond) {
			t.Fatalf("reader %d saw extra frames", ri)
		}
	}
	if code := a.status(t, "GET", "/healthz", "", nil); code != http.StatusOK {
		t.Fatalf("health after the storm = %d", code)
	}
}

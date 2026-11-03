package integration

import (
	"net/http"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

// TestListenerOutage holds one node's notification listener down — by
// terminating its PostgreSQL LISTEN backend in a loop — while the other node
// keeps publishing. Readers on the deaf node must catch up on everything
// stored once the listener returns, never see the transient published while
// they were deaf, and lose their connection when their token is rotated even
// though the close notice cannot reach them: the 15s re-authorization ends it.
//
// The loop kills roughly once a second and the node reconnects between
// kills, so the outage is a hard flap rather than a silent partition; that is
// also why healthz stays 200 throughout — each reconnect resets the 30s
// grace, which is the grace's whole job. The past-grace 503 decision is
// pinned deterministically in internal/store.
func TestListenerOutage(t *testing.T) {
	requireDB(t)
	a, b := newPair(t)

	// s1 is the catch-up stream; s2 is the rotate-during-outage stream.
	s1, t1 := a.create(t)
	s2, t2 := a.create(t)
	b.appendEvents(t, s1, `[{"data":"one"},{"data":"two"},{"data":"three"}]`, "")
	b.appendEvents(t, s2, `{"data":"solo"}`, "")

	catchUp := openSSE(t, a, "/v1/streams/"+s1+"?token="+t1+"&after=0", nil)
	for i, data := range []string{"one", "two", "three"} {
		if f := catchUp.next(t, 5*time.Second); f.id != int64(i+1) || f.data != data {
			t.Fatalf("replay frame %d = %+v", i+1, f)
		}
	}
	healthy := openSSE(t, b, "/v1/streams/"+s1+"?token="+t1+"&after=0", nil)
	for range 3 {
		healthy.next(t, 5*time.Second)
	}
	rotated := openSSE(t, a, "/v1/streams/"+s2+"?token="+t2+"&after=0", nil)
	if f := rotated.next(t, 5*time.Second); f.id != 1 {
		t.Fatalf("s2 replay = %+v", f)
	}

	// Hold node a's listener down.
	conn := pg(t)
	if killed := killListeners(conn, a); killed == 0 {
		t.Fatal("no listener found to terminate")
	}
	var hold atomic.Bool
	hold.Store(true)
	go func() {
		for hold.Load() {
			killListeners(conn, a)
			time.Sleep(250 * time.Millisecond)
		}
	}()
	defer hold.Store(false)

	// A flapping listener must not trip health; that is what the grace is.
	if code := a.health(); code != http.StatusOK {
		t.Fatalf("health during the flap = %d", code)
	}

	// The transient is published right after a termination, while node a is
	// certainly deaf: PostgreSQL discards notifications nobody listens for,
	// and reconnecting later does not replay them.
	const transientData = "lost-while-deaf"
	b.appendEvents(t, s1, `{"event":"typing","data":"`+transientData+`","transient":true}`, "")
	b.appendEvents(t, s1, `{"data":"four"}`, "")
	b.appendEvents(t, s1, `{"data":"five"}`, "")

	// The healthy node's reader sees all three, in publish order: the
	// transient first, then the stored events.
	if f := healthy.next(t, 5*time.Second); f.id != 0 || f.data != transientData {
		t.Fatalf("healthy reader transient = %+v", f)
	}
	if f := healthy.next(t, 5*time.Second); f.id != 4 {
		t.Fatalf("healthy reader = %+v", f)
	}
	if f := healthy.next(t, 5*time.Second); f.id != 5 {
		t.Fatalf("healthy reader = %+v", f)
	}

	// Rotate while node a is deaf: the close notice cannot arrive, so the
	// reader must be ended by the periodic re-authorization instead.
	fresh := b.rotate(t, s2)
	if !rotated.endedWithin(20 * time.Second) {
		t.Fatal("deaf reader survived a rotate")
	}
	if code := a.status(t, "GET", "/v1/streams/"+s2+"?token="+t2, "", nil); code != http.StatusUnauthorized {
		t.Fatalf("old token after rotate = %d", code)
	}
	if code := a.status(t, "GET", "/v1/streams/"+s2, "", map[string]string{"Authorization": "Bearer " + fresh}); code != http.StatusOK {
		t.Fatalf("fresh token on the deaf node = %d", code)
	}

	// Health stayed 200 through the flap, and the outage was real.
	if code := a.health(); code != http.StatusOK {
		t.Fatalf("health during the flap = %d", code)
	}
	if a.metric(t, "ssefy_listener_reconnects_total") < 1 {
		t.Fatal("listener never dropped; the outage did not happen")
	}

	// Restore: stop terminating, let the listener reconnect — it broadcasts
	// a catch-up wake to every local reader — and collect what was missed.
	hold.Store(false)
	waitFor(t, 10*time.Second, func() bool { return a.metric(t, "ssefy_listener_up") == 1 })
	time.Sleep(2 * time.Second) // let the catch-up drain land

	got := catchUp.drain()
	var seqs []int64
	for _, f := range got {
		if f.id == 0 || f.data == transientData {
			t.Fatalf("transient reached the deaf reader: %+v", f)
		}
		seqs = append(seqs, f.id)
	}
	if !slices.Equal(seqs, []int64{4, 5}) {
		t.Fatalf("catch-up frames = %v", seqs)
	}
	if catchUp.dead() {
		t.Fatal("catch-up reader ended")
	}
	if !catchUp.quiet(time.Second) {
		t.Fatal("unexpected extra frame after catch-up")
	}
	if code := a.health(); code != http.StatusOK {
		t.Fatalf("health after recovery = %d", code)
	}
}

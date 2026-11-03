package integration

import (
	"net/http"
	"testing"
	"time"
)

// TestCredentialIsolation proves a stream token opens exactly one stream's
// reads and nothing administrative.
func TestCredentialIsolation(t *testing.T) {
	requireDB(t)
	a, b := newPair(t)
	s1, t1 := a.create(t)
	s2, _ := b.create(t)

	// The token of s1 does not open s2, on either surface.
	for _, path := range []string{
		"/v1/streams/" + s2 + "?token=" + t1,
		"/v1/streams/" + s2 + "/events?token=" + t1,
	} {
		for _, n := range []*node{a, b} {
			if code := n.status(t, "GET", path, "", nil); code != http.StatusUnauthorized {
				t.Fatalf("token of s1 on s2 via %s = %d", n.name, code)
			}
		}
	}

	// The token performs no administrative operation.
	bearer := map[string]string{"Authorization": "Bearer " + t1}
	for _, call := range []struct{ method, path, body string }{
		{"GET", "/v1/streams/" + s1 + "/stat", ""},
		{"POST", "/v1/streams/" + s1, `{"data":"x"}`},
		{"POST", "/v1/streams", ""},
		{"POST", "/v1/push", `{"streams":["` + s1 + `"],"events":[{"data":"x"}]}`},
		{"DELETE", "/v1/streams/" + s1 + "/events?before=2", ""},
		{"POST", "/v1/streams/" + s1 + "/rotate", ""},
		{"POST", "/v1/streams/" + s1 + "/revoke", ""},
		{"DELETE", "/v1/streams/" + s1, ""},
	} {
		if code := a.status(t, call.method, call.path, call.body, bearer); code != http.StatusUnauthorized {
			t.Fatalf("stream token on %s %s = %d", call.method, call.path, code)
		}
	}

	// None of that ran: s1 is untouched and still readable.
	if code := b.status(t, "GET", "/v1/streams/"+s1+"?token="+t1, "", nil); code != http.StatusOK {
		t.Fatalf("s1 still readable = %d", code)
	}
}

// TestRotateRevokeAcrossNodes rotates and revokes while readers are
// connected to both nodes: the close notice must reach every node, old
// credentials must stop working everywhere, and only the server secret keeps
// reading afterwards.
func TestRotateRevokeAcrossNodes(t *testing.T) {
	requireDB(t)
	a, b := newPair(t)
	id, token := a.create(t)
	b.appendEvents(t, id, `{"data":"1"}`, "")

	ra := openSSE(t, a, "/v1/streams/"+id+"?token="+token+"&after=0", nil)
	rb := openSSE(t, b, "/v1/streams/"+id+"?token="+token+"&after=0", nil)
	if f := ra.next(t, 5*time.Second); f.id != 1 {
		t.Fatalf("reader on a = %+v", f)
	}
	if f := rb.next(t, 5*time.Second); f.id != 1 {
		t.Fatalf("reader on b = %+v", f)
	}

	fresh := b.rotate(t, id)
	if !ra.endedWithin(5*time.Second) || !rb.endedWithin(5*time.Second) {
		t.Fatal("rotate did not close readers on both nodes")
	}
	for _, n := range []*node{a, b} {
		if code := n.status(t, "GET", "/v1/streams/"+id+"?token="+token, "", nil); code != http.StatusUnauthorized {
			t.Fatalf("old token via %s = %d", n.name, code)
		}
		if code := n.status(t, "GET", "/v1/streams/"+id, "", map[string]string{"Authorization": "Bearer " + fresh}); code != http.StatusOK {
			t.Fatalf("fresh token via %s = %d", n.name, code)
		}
	}

	// Revoke: same fan-out, and afterwards only the server secret reads.
	ra2 := openSSE(t, a, "/v1/streams/"+id+"?token="+fresh+"&after=1", nil)
	a.revoke(t, id)
	if !ra2.endedWithin(5 * time.Second) {
		t.Fatal("revoke did not close the reader")
	}
	if code := b.status(t, "GET", "/v1/streams/"+id+"?token="+fresh, "", nil); code != http.StatusUnauthorized {
		t.Fatalf("token after revoke = %d", code)
	}
	if code := a.status(t, "GET", "/v1/streams/"+id+"/events", "", map[string]string{"Authorization": "Bearer " + secret}); code != http.StatusOK {
		t.Fatalf("secret history after revoke = %d", code)
	}

	// The secret never travels in the query string.
	for _, n := range []*node{a, b} {
		for _, path := range []string{
			"/v1/streams/" + id + "?token=" + secret,
			"/v1/streams/" + id + "/events?token=" + secret,
		} {
			if code := n.status(t, "GET", path, "", nil); code != http.StatusUnauthorized {
				t.Fatalf("secret in query via %s = %d", n.name, code)
			}
		}
	}
}

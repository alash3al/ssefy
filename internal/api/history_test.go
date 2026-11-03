package api

import (
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
)

func trimEvents(t *testing.T, r *rig, id, query string) int64 {
	t.Helper()
	resp := r.write("DELETE", "/v1/streams/"+id+"/events?"+query, "")
	defer closeBody(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("trim %s = %d", query, resp.StatusCode)
	}
	var out struct {
		Events int64 `json:"events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out.Events
}

func history(t *testing.T, r *rig, id, token, query string) (historyResponse, string) {
	t.Helper()
	path := "/v1/streams/" + id + "/events?token=" + token
	if query != "" {
		path += "&" + query
	}
	resp := r.do("GET", path, "", nil)
	defer closeBody(resp)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("history %q = %d %s", query, resp.StatusCode, body)
	}
	var out historyResponse
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	return out, string(body)
}

func historySeqs(h historyResponse) []int64 {
	out := make([]int64, len(h.Events))
	for i, e := range h.Events {
		out[i] = e.Seq
	}
	return out
}

func TestHistory(t *testing.T) {
	r := newRig(t)
	id, token := r.create()
	r.append(id, `[{"data":"1"},{"data":"2"},{"data":"3"},{"data":"4"},{"data":"5"}]`)

	// No params: the newest page, oldest to newest.
	h, _ := history(t, r, id, token, "")
	if got := historySeqs(h); !slices.Equal(got, []int64{1, 2, 3, 4, 5}) || h.HasMore {
		t.Fatalf("default = %v, more=%v", got, h.HasMore)
	}

	// Scroll-top: newest page, then walk older with the edge seq.
	h, _ = history(t, r, id, token, "limit=2")
	if got := historySeqs(h); !slices.Equal(got, []int64{4, 5}) || !h.HasMore {
		t.Fatalf("newest page = %v, more=%v", got, h.HasMore)
	}
	h, _ = history(t, r, id, token, "cursor=4&limit=2")
	if got := historySeqs(h); !slices.Equal(got, []int64{2, 3}) || !h.HasMore {
		t.Fatalf("older page = %v, more=%v", got, h.HasMore)
	}
	h, _ = history(t, r, id, token, "cursor=2&limit=2")
	if got := historySeqs(h); !slices.Equal(got, []int64{1}) || h.HasMore {
		t.Fatalf("oldest page = %v, more=%v", got, h.HasMore)
	}

	// Forward: catch-up from a cursor.
	h, _ = history(t, r, id, token, "direction=forward&cursor=3&limit=2")
	if got := historySeqs(h); !slices.Equal(got, []int64{4, 5}) || h.HasMore {
		t.Fatalf("forward = %v, more=%v", got, h.HasMore)
	}

	// An empty page is an array, never null and never omitted.
	h, raw := history(t, r, id, token, "direction=forward&cursor=5")
	if len(h.Events) != 0 || h.HasMore || !strings.Contains(raw, `"events":[]`) {
		t.Fatalf("empty page = %q", raw)
	}

	// The token rides the Authorization header too, like SSE.
	head := r.do("GET", "/v1/streams/"+id+"/events?limit=2", "", map[string]string{"Authorization": "Bearer " + token})
	defer closeBody(head)
	if head.StatusCode != http.StatusOK {
		t.Fatalf("bearer = %d", head.StatusCode)
	}

	// Auth and validation.
	for _, tc := range []struct {
		name, path string
		want       int
	}{
		{"no token", "/v1/streams/" + id + "/events", http.StatusUnauthorized},
		{"wrong token", "/v1/streams/" + id + "/events?token=wrong", http.StatusUnauthorized},
		{"unknown stream", "/v1/streams/01900000-0000-7000-8000-000000000000/events?token=x", http.StatusNotFound},
		{"bad direction", "/v1/streams/" + id + "/events?token=" + token + "&direction=sideways", http.StatusBadRequest},
		{"bad cursor", "/v1/streams/" + id + "/events?token=" + token + "&cursor=abc", http.StatusBadRequest},
		{"negative cursor", "/v1/streams/" + id + "/events?token=" + token + "&cursor=-1", http.StatusBadRequest},
		{"zero limit", "/v1/streams/" + id + "/events?token=" + token + "&limit=0", http.StatusBadRequest},
		{"over limit", "/v1/streams/" + id + "/events?token=" + token + "&limit=201", http.StatusBadRequest},
	} {
		resp := r.do("GET", tc.path, "", nil)
		_ = resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Errorf("%s = %d, want %d", tc.name, resp.StatusCode, tc.want)
		}
	}
}

func TestTrim(t *testing.T) {
	r := newRig(t)
	id, token := r.create()
	r.append(id, `[{"data":"1"},{"data":"2"},{"data":"3"},{"data":"4"},{"data":"5"}]`)

	// A write: the server secret, not the stream token.
	if resp := r.do("DELETE", "/v1/streams/"+id+"/events?before=3", "", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no secret = %d", resp.StatusCode)
	}

	// Oldest side, then the sequence continues from last_seq.
	if n := trimEvents(t, r, id, "before=3"); n != 2 {
		t.Fatalf("before=3 removed %d", n)
	}
	h, _ := history(t, r, id, token, "")
	if got := historySeqs(h); !slices.Equal(got, []int64{3, 4, 5}) {
		t.Fatalf("after before=3 = %v", got)
	}
	r.append(id, `{"data":"6"}`)
	h, _ = history(t, r, id, token, "")
	if got := historySeqs(h); !slices.Equal(got, []int64{3, 4, 5, 6}) {
		t.Fatalf("append after trim = %v", got)
	}

	// Newest side.
	if n := trimEvents(t, r, id, "after=4"); n != 2 {
		t.Fatalf("after=4 removed %d", n)
	}
	h, _ = history(t, r, id, token, "")
	if got := historySeqs(h); !slices.Equal(got, []int64{3, 4}) {
		t.Fatalf("after after=4 = %v", got)
	}

	// The full range clears the history and keeps the stream.
	if n := trimEvents(t, r, id, "after=0"); n != 2 {
		t.Fatalf("after=0 removed %d", n)
	}
	h, _ = history(t, r, id, token, "")
	if len(h.Events) != 0 || h.HasMore {
		t.Fatalf("cleared = %+v", h)
	}

	// Validation and unknown streams.
	for _, q := range []string{"", "before=3&after=3", "before=abc", "before=-1"} {
		resp := r.write("DELETE", "/v1/streams/"+id+"/events?"+q, "")
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("trim %q = %d", q, resp.StatusCode)
		}
	}
	resp := r.write("DELETE", "/v1/streams/01900000-0000-7000-8000-000000000000/events?before=3", "")
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown stream = %d", resp.StatusCode)
	}
}

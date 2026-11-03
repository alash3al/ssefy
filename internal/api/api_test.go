package api

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alash3al/ssefy/internal/store"
)

type rig struct {
	t   *testing.T
	srv *httptest.Server
}

func newRig(t *testing.T) *rig {
	t.Helper()
	st := store.NewMemory()
	a := New(st, "secret", []string{"*"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(a.Handler())
	t.Cleanup(func() {
		a.Close()
		srv.Close()
		_ = st.Close()
	})
	return &rig{t: t, srv: srv}
}

func closeBody(resp *http.Response) { _ = resp.Body.Close() }

func (r *rig) do(method, path string, body string, headers map[string]string) *http.Response {
	r.t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, r.srv.URL+path, rd)
	if err != nil {
		r.t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		r.t.Fatal(err)
	}
	return resp
}

func (r *rig) write(method, path, body string) *http.Response {
	r.t.Helper()
	return r.do(method, path, body, map[string]string{"Authorization": "Bearer secret"})
}

func (r *rig) create() string {
	r.t.Helper()
	resp := r.write("POST", "/v1/streams", "")
	defer closeBody(resp)
	if resp.StatusCode != http.StatusCreated {
		r.t.Fatalf("create = %d", resp.StatusCode)
	}
	var out struct{ ID string }
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || out.ID == "" {
		r.t.Fatalf("create body: %v %q", err, out.ID)
	}
	return out.ID
}

func (r *rig) append(id, body string) []int64 {
	r.t.Helper()
	resp := r.write("POST", "/v1/streams/"+id, body)
	defer closeBody(resp)
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(resp.Body)
		r.t.Fatalf("append = %d %s", resp.StatusCode, msg)
	}
	var out struct{ Seqs []int64 }
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		r.t.Fatal(err)
	}
	return out.Seqs
}

type frame struct {
	id    string
	event string
	data  string
}

func readFrame(t *testing.T, br *bufio.Reader) frame {
	t.Helper()
	var f frame
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("read frame: %v", err)
		}
		line = strings.TrimSuffix(line, "\n")
		if line == "" {
			return f
		}
		switch {
		case strings.HasPrefix(line, "id: "):
			f.id = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "event: "):
			f.event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			f.data = strings.TrimPrefix(line, "data: ")
		}
	}
}

func openStream(t *testing.T, url string, header map[string]string) (*http.Response, *bufio.Reader, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		cancel()
		t.Fatalf("open stream = %d", resp.StatusCode)
	}
	return resp, bufio.NewReader(resp.Body), cancel
}

func TestAuth(t *testing.T) {
	r := newRig(t)
	if resp := r.do("POST", "/v1/streams", "", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no token = %d", resp.StatusCode)
	}
	if resp := r.do("POST", "/v1/streams", "", map[string]string{"Authorization": "Bearer nope"}); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad token = %d", resp.StatusCode)
	}
}

func TestReplayLiveAndDelete(t *testing.T) {
	r := newRig(t)
	id := r.create()
	if seqs := r.append(id, `{"data":"one"}`); len(seqs) != 1 || seqs[0] != 1 {
		t.Fatalf("seqs = %v", seqs)
	}
	r.append(id, `{"event":"tick","data":"two"}`)

	resp, br, cancel := openStream(t, r.srv.URL+"/v1/streams/"+id+"?after=0", nil)
	defer cancel()
	defer closeBody(resp)

	if f := readFrame(t, br); f.id != "1" || f.event != "message" || f.data != "one" {
		t.Fatalf("frame 1 = %+v", f)
	}
	if f := readFrame(t, br); f.id != "2" || f.event != "tick" || f.data != "two" {
		t.Fatalf("frame 2 = %+v", f)
	}

	// Live delivery through the store's notice.
	r.append(id, `{"data":"three"}`)
	if f := readFrame(t, br); f.id != "3" || f.data != "three" {
		t.Fatalf("live frame = %+v", f)
	}

	// A deleted stream wakes readers and then 404s on reconnect.
	if resp := r.write("DELETE", "/v1/streams/"+id, ""); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete = %d", resp.StatusCode)
	}
	if _, err := br.ReadString('\n'); err == nil {
		t.Fatal("stream stayed open after delete")
	}
	if resp := r.do("GET", "/v1/streams/"+id, "", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("get after delete = %d", resp.StatusCode)
	}
}

func TestResumeWithLastEventID(t *testing.T) {
	r := newRig(t)
	id := r.create()
	r.append(id, `{"data":"one"}`)
	r.append(id, `{"data":"two"}`)

	resp, br, cancel := openStream(t, r.srv.URL+"/v1/streams/"+id, map[string]string{"Last-Event-ID": "1"})
	defer cancel()
	defer closeBody(resp)
	if f := readFrame(t, br); f.id != "2" || f.data != "two" {
		t.Fatalf("resume frame = %+v", f)
	}
}

func TestTransientHasNoID(t *testing.T) {
	r := newRig(t)
	id := r.create()
	resp, br, cancel := openStream(t, r.srv.URL+"/v1/streams/"+id+"?after=0", nil)
	defer cancel()
	defer closeBody(resp)

	if seqs := r.append(id, `{"event":"typing","data":"...","transient":true}`); len(seqs) != 0 {
		t.Fatalf("seqs = %v", seqs)
	}
	f := readFrame(t, br)
	if f.id != "" || f.event != "typing" || f.data != "..." {
		t.Fatalf("transient frame = %+v", f)
	}
}

func TestValidation(t *testing.T) {
	r := newRig(t)
	id := r.create()

	cases := []struct {
		body string
		want int
	}{
		{`{"data":"x","transient":true,"collapse_key":"c"}`, http.StatusBadRequest},
		{`[{"data":"x"},{"data":"y","transient":true}]`, http.StatusBadRequest},
		{`[]`, http.StatusBadRequest},
	}
	for _, c := range cases {
		resp := r.write("POST", "/v1/streams/"+id, c.body)
		_ = resp.Body.Close()
		if resp.StatusCode != c.want {
			t.Errorf("%s = %d, want %d", c.body, resp.StatusCode, c.want)
		}
	}

	if resp := r.write("POST", "/v1/streams/"+id, `{"data":"ok"}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("valid = %d", resp.StatusCode)
	}
}

func TestIdempotencyHTTP(t *testing.T) {
	r := newRig(t)
	id := r.create()
	hdr := map[string]string{"Authorization": "Bearer secret", "Idempotency-Key": "k"}

	first := r.do("POST", "/v1/streams/"+id, `{"data":"x"}`, hdr)
	var out struct{ Seqs []int64 }
	_ = json.NewDecoder(first.Body).Decode(&out)
	_ = first.Body.Close()

	second := r.do("POST", "/v1/streams/"+id, `{"data":"x"}`, hdr)
	defer closeBody(second)
	if second.StatusCode != http.StatusOK {
		t.Fatalf("retry = %d", second.StatusCode)
	}
	var again struct{ Seqs []int64 }
	_ = json.NewDecoder(second.Body).Decode(&again)
	if len(out.Seqs) != 1 || len(again.Seqs) != 1 || again.Seqs[0] != out.Seqs[0] {
		t.Fatalf("retry seqs = %v, want %v", again.Seqs, out.Seqs)
	}

	third := r.do("POST", "/v1/streams/"+id, `{"data":"different"}`, hdr)
	defer closeBody(third)
	if third.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("mismatch = %d", third.StatusCode)
	}
}

func TestNotFoundAndNow(t *testing.T) {
	r := newRig(t)
	if resp := r.write("POST", "/v1/streams/missing", `{"data":"x"}`); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("append to missing = %d", resp.StatusCode)
	}

	id := r.create()
	r.append(id, `{"data":"old"}`)

	// ?after=now skips what is already stored, then follows live events.
	resp, br, cancel := openStream(t, r.srv.URL+"/v1/streams/"+id+"?after=now", nil)
	defer cancel()
	defer closeBody(resp)

	r.append(id, `{"data":"new"}`)
	if f := readFrame(t, br); f.id != "2" || f.data != "new" {
		t.Fatalf("now frame = %+v", f)
	}
}

func TestFormatEventSplitsLines(t *testing.T) {
	got := formatEvent(7, "message", "a\r\nb\rc")
	want := "id: 7\nevent: message\ndata: a\ndata: b\ndata: c\n\n"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if got := formatEvent(0, "typing", ""); strings.Contains(got, "id:") {
		t.Fatalf("transient frame has an id: %q", got)
	}
}

func BenchmarkFormatEvent(b *testing.B) {
	data := strings.Repeat("some realtime payload line\n", 64)
	b.ReportAllocs()
	for b.Loop() {
		_ = formatEvent(42, "message", data)
	}
}

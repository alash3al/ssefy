package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
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
	return newRigWith(t, store.NewMemory())
}

func newRigWith(t *testing.T, st store.Store) *rig {
	t.Helper()
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
	if resp := r.do("GET", "/v1/streams/"+id, "", nil); resp.StatusCode != http.StatusGone {
		t.Fatalf("get after delete = %d", resp.StatusCode)
	}
	if resp := r.write("POST", "/v1/streams/"+id, `{"data":"late"}`); resp.StatusCode != http.StatusGone {
		t.Fatalf("append after delete = %d", resp.StatusCode)
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

	oversized := `{"data":"` + strings.Repeat("x", 8000) + `","transient":true}`
	if resp := r.write("POST", "/v1/streams/"+id, oversized); resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized transient = %d", resp.StatusCode)
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

func TestMissingStreamAheadCursorAndHead(t *testing.T) {
	r := newRig(t)
	missing := "00000000000000000000000000000000"
	if resp := r.do("GET", "/v1/streams/"+missing, "", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("missing stream = %d", resp.StatusCode)
	}

	id := r.create()
	r.append(id, `{"data":"one"}`)

	// A cursor past the last seq can only be bogus, so the stream starts at now
	// instead of replaying everything.
	resp, br, cancel := openStream(t, r.srv.URL+"/v1/streams/"+id+"?after=99", nil)
	defer cancel()
	defer closeBody(resp)
	r.append(id, `{"data":"two"}`)
	if f := readFrame(t, br); f.id != "2" || f.data != "two" {
		t.Fatalf("ahead frame = %+v", f)
	}

	if resp := r.do("HEAD", "/v1/streams/"+id, "", nil); resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("HEAD = %d", resp.StatusCode)
	}
}

func TestReplayPaging(t *testing.T) {
	r := newRig(t)
	id := r.create()

	var body strings.Builder
	body.WriteByte('[')
	for i := range 450 {
		if i > 0 {
			body.WriteByte(',')
		}
		body.WriteString(`{"data":"x"}`)
	}
	body.WriteByte(']')
	if seqs := r.append(id, body.String()); len(seqs) != 450 || seqs[449] != 450 {
		t.Fatalf("seqs = %d entries", len(seqs))
	}

	resp, br, cancel := openStream(t, r.srv.URL+"/v1/streams/"+id+"?after=0", nil)
	defer cancel()
	defer closeBody(resp)
	for i := 1; i <= 450; i++ {
		if f := readFrame(t, br); f.id != strconv.Itoa(i) {
			t.Fatalf("frame %d has id %q", i, f.id)
		}
	}
}

// The only test that runs the HTTP layer against PostgreSQL, so the
// pg_notify -> listener -> hub -> SSE chain is covered end to end.
func TestPostgresEndToEnd(t *testing.T) {
	dsn := os.Getenv("SSEFY_TEST_DB")
	if dsn == "" {
		t.Skip("set SSEFY_TEST_DB to run")
	}
	st, err := store.OpenPostgres(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	r := newRigWith(t, st)

	id := r.create()
	r.append(id, `{"data":"one"}`)

	resp, br, cancel := openStream(t, r.srv.URL+"/v1/streams/"+id+"?after=0", nil)
	defer cancel()
	defer closeBody(resp)
	if f := readFrame(t, br); f.id != "1" || f.data != "one" {
		t.Fatalf("replay frame = %+v", f)
	}

	r.append(id, `{"data":"two"}`)
	if f := readFrame(t, br); f.id != "2" || f.data != "two" {
		t.Fatalf("live frame = %+v", f)
	}

	del := r.write("DELETE", "/v1/streams/"+id, "")
	_ = del.Body.Close()
	if resp := r.do("GET", "/v1/streams/"+id, "", nil); resp.StatusCode != http.StatusGone {
		t.Fatalf("get after delete = %d", resp.StatusCode)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	r := newRig(t)
	id := r.create()
	r.append(id, `{"data":"x"}`)

	resp, err := http.Get(r.srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer closeBody(resp)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics = %d", resp.StatusCode)
	}
	for _, want := range []string{
		`ssefy_readers`,
		`ssefy_events_appended_total{kind="stored"}`,
		`ssefy_http_requests_total`,
		`go_goroutines`,
	} {
		if !bytes.Contains(body, []byte(want)) {
			t.Errorf("metrics output misses %q", want)
		}
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

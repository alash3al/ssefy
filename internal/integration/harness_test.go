// Package integration breaks a real ssefy deployment on purpose: it builds
// the binary, runs two nodes over one shared PostgreSQL database, and checks
// what survives. The suite is skipped unless SSEFY_TEST_DB is set.
package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// secret is the server secret every node is started with.
const secret = "integration-secret"

var testDB = os.Getenv("SSEFY_TEST_DB")

func requireDB(t *testing.T) {
	t.Helper()
	if testDB == "" {
		t.Skip("set SSEFY_TEST_DB to run the integration suite")
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	if binPath != "" {
		_ = os.RemoveAll(filepath.Dir(binPath))
	}
	if pgConn != nil {
		_ = pgConn.Close(context.Background())
	}
	os.Exit(code)
}

// node is one ssefy process. name becomes its PostgreSQL application_name,
// which is how the suite finds — and terminates — its LISTEN connection.
type node struct {
	name string
	base string
	cmd  *exec.Cmd
	logs bytes.Buffer
}

var (
	buildOnce sync.Once
	binPath   string
	buildErr  error
)

func build(t *testing.T) string {
	t.Helper()
	requireDB(t)
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "ssefy-integration-")
		if err != nil {
			buildErr = err
			return
		}
		binPath = filepath.Join(dir, "ssefy")
		out, err := exec.Command("go", "build", "-o", binPath, "github.com/alash3al/ssefy").CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("go build: %v: %s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return binPath
}

// startNode runs one node on a free port and waits until it is healthy and
// its notification listener is up. The node is killed when the test ends.
func startNode(t *testing.T, name string) *node {
	t.Helper()
	requireDB(t)
	port := freePort(t)

	u, err := url.Parse(testDB)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("application_name", "ssefy-"+name)
	u.RawQuery = q.Encode()

	cmd := exec.Command(build(t))
	cmd.Env = append(os.Environ(),
		"SSEFY_LISTEN=127.0.0.1:"+strconv.Itoa(port),
		"SSEFY_SERVER_SECRET="+secret,
		"SSEFY_STORE="+u.String(),
	)
	n := &node{name: name, base: "http://127.0.0.1:" + strconv.Itoa(port), cmd: cmd}
	cmd.Stdout, cmd.Stderr = &n.logs, &n.logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		n.kill()
		if t.Failed() {
			t.Logf("node %s logs:\n%s", name, n.logs.String())
		}
	})
	n.waitReady(t)
	return n
}

// newPair is the standard setup: two nodes sharing one database.
func newPair(t *testing.T) (*node, *node) {
	t.Helper()
	return startNode(t, "a"), startNode(t, "b")
}

func (n *node) kill() {
	if n.cmd.Process != nil {
		_ = n.cmd.Process.Kill()
	}
	_ = n.cmd.Wait()
}

func (n *node) waitReady(t *testing.T) {
	t.Helper()
	waitFor(t, 15*time.Second, func() bool {
		return n.health() == http.StatusOK && n.metric(t, "ssefy_listener_up") == 1
	})
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

// waitFor polls a condition until it holds or the deadline fails the test.
func waitFor(t *testing.T, within time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", within)
}

// probeClient is for short liveness calls; apiClient for regular ones.
// Streams get their own client without a timeout.
var (
	probeClient = &http.Client{Timeout: 2 * time.Second}
	apiClient   = &http.Client{Timeout: 10 * time.Second}
	sseClient   = &http.Client{}
)

func closeBody(resp *http.Response) { _ = resp.Body.Close() }

func (n *node) do(t *testing.T, method, path, body string, headers map[string]string) *http.Response {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, n.base+path, rd)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := apiClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// status answers just the response code, for calls whose body nobody reads.
func (n *node) status(t *testing.T, method, path, body string, headers map[string]string) int {
	t.Helper()
	resp := n.do(t, method, path, body, headers)
	code := resp.StatusCode
	closeBody(resp)
	return code
}

func (n *node) write(t *testing.T, method, path string) *http.Response {
	t.Helper()
	return n.do(t, method, path, "", map[string]string{"Authorization": "Bearer " + secret})
}

// decode reads one JSON body and closes the response.
func decode(t *testing.T, resp *http.Response, into any) {
	t.Helper()
	defer closeBody(resp)
	if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
		t.Fatal(err)
	}
}

func (n *node) create(t *testing.T) (string, string) {
	t.Helper()
	resp := n.write(t, "POST", "/v1/streams")
	if resp.StatusCode != http.StatusCreated {
		closeBody(resp)
		t.Fatalf("create via %s = %d", n.name, resp.StatusCode)
	}
	var out struct{ ID, Token string }
	decode(t, resp, &out)
	if out.ID == "" || out.Token == "" {
		t.Fatal("create returned no id or token")
	}
	return out.ID, out.Token
}

// appendQuiet pushes events and reports errors instead of failing the test,
// so goroutines may call it; appendEvents is the failing wrapper.
func (n *node) appendQuiet(id, body, idemKey string) ([]int64, error) {
	h := map[string]string{"Authorization": "Bearer " + secret}
	if idemKey != "" {
		h["Idempotency-Key"] = idemKey
	}
	req, err := http.NewRequest("POST", n.base+"/v1/streams/"+id, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	for k, v := range h {
		req.Header.Set(k, v)
	}
	resp, err := apiClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer closeBody(resp)
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("append via %s = %d %s", n.name, resp.StatusCode, msg)
	}
	var out struct{ Seqs []int64 }
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.Seqs, nil
}

func (n *node) appendEvents(t *testing.T, id, body, idemKey string) []int64 {
	t.Helper()
	seqs, err := n.appendQuiet(id, body, idemKey)
	if err != nil {
		t.Fatal(err)
	}
	return seqs
}

func (n *node) trim(t *testing.T, id, query string) int64 {
	t.Helper()
	resp := n.write(t, "DELETE", "/v1/streams/"+id+"/events?"+query)
	if resp.StatusCode != http.StatusOK {
		closeBody(resp)
		t.Fatalf("trim = %d", resp.StatusCode)
	}
	var out struct{ Events int64 }
	decode(t, resp, &out)
	return out.Events
}

func (n *node) rotate(t *testing.T, id string) string {
	t.Helper()
	resp := n.write(t, "POST", "/v1/streams/"+id+"/rotate")
	if resp.StatusCode != http.StatusOK {
		closeBody(resp)
		t.Fatalf("rotate = %d", resp.StatusCode)
	}
	var out struct{ Token string }
	decode(t, resp, &out)
	if out.Token == "" {
		t.Fatal("rotate returned no token")
	}
	return out.Token
}

func (n *node) revoke(t *testing.T, id string) {
	t.Helper()
	resp := n.write(t, "POST", "/v1/streams/"+id+"/revoke")
	closeBody(resp)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("revoke = %d", resp.StatusCode)
	}
}

func (n *node) remove(t *testing.T, id string) {
	t.Helper()
	resp := n.write(t, "DELETE", "/v1/streams/"+id)
	closeBody(resp)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete = %d", resp.StatusCode)
	}
}

func (n *node) health() int {
	resp, err := probeClient.Get("http://" + n.base[len("http://"):] + "/healthz")
	if err != nil {
		return 0
	}
	defer closeBody(resp)
	return resp.StatusCode
}

// metric reads one unlabeled Prometheus value; -1 means the node did not
// answer, which callers treat as "not yet".
func (n *node) metric(t *testing.T, name string) float64 {
	t.Helper()
	resp, err := probeClient.Get("http://" + n.base[len("http://"):] + "/metrics")
	if err != nil {
		return -1
	}
	defer closeBody(resp)
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) == 2 && f[0] == name {
			v, err := strconv.ParseFloat(f[1], 64)
			if err != nil {
				t.Fatalf("metric %s: %v", name, err)
			}
			return v
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("read metrics: %v", err)
	}
	t.Fatalf("metric %s not exported", name)
	return 0
}

type histEvent struct {
	Seq  int64  `json:"seq"`
	Data string `json:"data"`
}

type historyPage struct {
	Events  []histEvent `json:"events"`
	HasMore bool        `json:"has_more"`
}

func (n *node) history(t *testing.T, id, token, query string) historyPage {
	t.Helper()
	path := "/v1/streams/" + id + "/events?token=" + token
	if query != "" {
		path += "&" + query
	}
	resp := n.do(t, "GET", path, "", nil)
	if resp.StatusCode != http.StatusOK {
		closeBody(resp)
		t.Fatalf("history via %s = %d", n.name, resp.StatusCode)
	}
	var p historyPage
	decode(t, resp, &p)
	return p
}

// allEvents returns the whole stored history, oldest to newest.
func (n *node) allEvents(t *testing.T, id, token string) []histEvent {
	t.Helper()
	var out []histEvent
	var cursor int64
	for {
		p := n.history(t, id, token, fmt.Sprintf("direction=forward&cursor=%d&limit=200", cursor))
		out = append(out, p.Events...)
		if !p.HasMore {
			return out
		}
		cursor = p.Events[len(p.Events)-1].Seq
	}
}

func seqsOf(events []histEvent) []int64 {
	out := make([]int64, len(events))
	for i, e := range events {
		out[i] = e.Seq
	}
	return out
}

func countData(events []histEvent, data string) int {
	n := 0
	for _, e := range events {
		if e.Data == data {
			n++
		}
	}
	return n
}

// seqRange is the inclusive range from..to, for exact page comparisons.
func seqRange(from, to int) []int64 {
	out := make([]int64, 0, to-from+1)
	for i := from; i <= to; i++ {
		out = append(out, int64(i))
	}
	return out
}

type pushResult struct {
	Seqs  []int64 `json:"seqs"`
	Error string  `json:"error"`
}

func (n *node) push(t *testing.T, body, idemKey string) map[string]pushResult {
	t.Helper()
	h := map[string]string{"Authorization": "Bearer " + secret}
	if idemKey != "" {
		h["Idempotency-Key"] = idemKey
	}
	resp := n.do(t, "POST", "/v1/push", body, h)
	if resp.StatusCode != http.StatusOK {
		closeBody(resp)
		t.Fatalf("push = %d", resp.StatusCode)
	}
	var out struct {
		Results map[string]pushResult `json:"results"`
	}
	decode(t, resp, &out)
	return out.Results
}

// frame is one parsed SSE frame; id 0 means the frame carried no id line,
// which is how transient events and heartbeats arrive.
type frame struct {
	id    int64
	event string
	data  string
}

// sse is one open reader. A goroutine pumps parsed frames into a channel so
// tests can wait on them with deadlines; ended closes when the body does.
type sse struct {
	resp   *http.Response
	cancel context.CancelFunc
	frames chan frame
	ended  chan struct{}
	once   sync.Once
}

func openSSE(t *testing.T, n *node, path string, headers map[string]string) *sse {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, "GET", n.base+path, nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := sseClient.Do(req)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		code := resp.StatusCode
		closeBody(resp)
		cancel()
		t.Fatalf("open stream via %s = %d", n.name, code)
	}
	s := &sse{
		resp:   resp,
		cancel: cancel,
		frames: make(chan frame, 1024),
		ended:  make(chan struct{}),
	}
	t.Cleanup(s.close)
	go func() {
		defer close(s.ended)
		br := bufio.NewReader(resp.Body)
		for {
			f, err := readFrame(br)
			if err != nil {
				return
			}
			if f.id == 0 && f.event == "" && f.data == "" {
				continue // heartbeat comment
			}
			s.frames <- f
		}
	}()
	return s
}

func (s *sse) close() {
	s.once.Do(func() {
		s.cancel()
		_ = s.resp.Body.Close()
	})
}

// readFrame parses one SSE frame; comment lines fall through, and a frame
// carrying nothing at all is a heartbeat.
func readFrame(br *bufio.Reader) (frame, error) {
	var f frame
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return f, err
		}
		line = strings.TrimSuffix(line, "\n")
		if line == "" {
			return f, nil
		}
		switch {
		case strings.HasPrefix(line, "id: "):
			f.id, _ = strconv.ParseInt(strings.TrimPrefix(line, "id: "), 10, 64)
		case strings.HasPrefix(line, "event: "):
			f.event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			if f.data != "" {
				f.data += "\n"
			}
			f.data += strings.TrimPrefix(line, "data: ")
		}
	}
}

// next returns one frame, failing the test on timeout or stream end.
func (s *sse) next(t *testing.T, within time.Duration) frame {
	t.Helper()
	select {
	case f := <-s.frames:
		return f
	case <-s.ended:
		t.Fatalf("stream ended before the expected frame (%s)", within)
	case <-time.After(within):
		t.Fatalf("no frame within %s", within)
	}
	return frame{}
}

// quiet reports whether nothing arrived within the window.
func (s *sse) quiet(within time.Duration) bool {
	select {
	case <-s.frames:
		return false
	case <-s.ended:
		return false
	case <-time.After(within):
		return true
	}
}

// endedWithin reports whether the stream ended within the window.
func (s *sse) endedWithin(within time.Duration) bool {
	select {
	case <-s.ended:
		return true
	case <-time.After(within):
		return false
	}
}

// dead reports whether the stream has already ended.
func (s *sse) dead() bool {
	select {
	case <-s.ended:
		return true
	default:
		return false
	}
}

// drain empties every buffered frame without waiting.
func (s *sse) drain() []frame {
	var out []frame
	for {
		select {
		case f := <-s.frames:
			out = append(out, f)
		default:
			return out
		}
	}
}

var (
	pgOnce sync.Once
	pgConn *pgx.Conn
	pgErr  error
)

// pg is the suite's own database connection, for the few things HTTP cannot
// reach — so far only terminating a node's LISTEN backend.
func pg(t *testing.T) *pgx.Conn {
	t.Helper()
	requireDB(t)
	pgOnce.Do(func() {
		pgConn, pgErr = pgx.Connect(context.Background(), testDB)
	})
	if pgErr != nil {
		t.Fatal(pgErr)
	}
	return pgConn
}

// killListeners terminates one node's LISTEN backend. The node reconnects
// within a second, so a sustained outage means calling this in a loop.
func killListeners(conn *pgx.Conn, n *node) int {
	tag, err := conn.Exec(context.Background(), `SELECT pg_terminate_backend(pid)
		FROM pg_stat_activity
		WHERE application_name = $1 AND query LIKE '%LISTEN%'`, "ssefy-"+n.name)
	if err != nil {
		return 0
	}
	return int(tag.RowsAffected())
}

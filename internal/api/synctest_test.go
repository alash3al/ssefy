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
	"testing/synctest"
	"time"

	"github.com/alash3al/ssefy/internal/store"
)

// TestHeartbeatKeepsReadersAlive drives the 15-second heartbeat that no other
// unit test can reach in real time: every tick re-authorizes its reader — a
// token reader through OpenReader, a secret reader through Stat — and the
// wrong check there silently drops the reader within one tick. The in-memory
// test server plus synctest's fake clock make three ticks cost nothing.
func TestHeartbeatKeepsReadersAlive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		st := store.NewMemory()
		a := New(st, "secret", nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
		srv := httptest.NewTestServer(t, a.Handler())
		client := srv.Client()

		// Teardown must run inside the bubble, in this order: the reader
		// bodies close their connections, the done channel ends the SSE
		// handlers, and the transport loops get scheduled to notice their
		// closed connections before the bubble checks for leaked goroutines.
		defer func() { _ = st.Close() }()
		defer srv.Close()
		defer func() {
			client.CloseIdleConnections()
			synctest.Wait()
		}()
		defer a.Close()

		do := func(method, path, body string, headers map[string]string) *http.Response {
			t.Helper()
			var rd io.Reader
			if body != "" {
				rd = strings.NewReader(body)
			}
			req, err := http.NewRequestWithContext(context.Background(), method, "http://example.com"+path, rd)
			if err != nil {
				t.Fatal(err)
			}
			for k, v := range headers {
				req.Header.Set(k, v)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			return resp
		}
		secretHead := map[string]string{"Authorization": "Bearer secret"}

		resp := do("POST", "/v1/streams", "", secretHead)
		var created struct{ ID, Token string }
		ok := resp.StatusCode == http.StatusCreated && json.NewDecoder(resp.Body).Decode(&created) == nil
		closeBody(resp)
		if !ok {
			t.Fatalf("create = %d", resp.StatusCode)
		}
		resp = do("POST", "/v1/streams/"+created.ID, `{"data":"one"}`, secretHead)
		code := resp.StatusCode
		closeBody(resp)
		if code != http.StatusOK {
			t.Fatalf("append = %d", code)
		}

		// One reader with the stream token, one with the server secret.
		open := func(headers map[string]string) (io.ReadCloser, *bufio.Reader) {
			t.Helper()
			resp := do("GET", "/v1/streams/"+created.ID+"?after=0", "", headers)
			if resp.StatusCode != http.StatusOK {
				closeBody(resp)
				t.Fatalf("open stream = %d", resp.StatusCode)
			}
			return resp.Body, bufio.NewReader(resp.Body)
		}
		tokenBody, tokenR := open(map[string]string{"Authorization": "Bearer " + created.Token})
		secretBody, secretR := open(secretHead)
		defer func() { _ = tokenBody.Close(); _ = secretBody.Close() }()
		// Both replay the stored event, heartbeat comments and all.
		nextEvent := func(br *bufio.Reader) frame {
			t.Helper()
			for {
				f := readFrame(t, br)
				if f.id != "" {
					return f
				}
			}
		}
		if f := nextEvent(tokenR); f.id != "1" {
			t.Fatalf("token reader = %+v", f)
		}
		if f := nextEvent(secretR); f.id != "1" {
			t.Fatalf("secret reader = %+v", f)
		}

		// Three heartbeats pass: each tick re-authorizes both readers.
		start := time.Now()
		time.Sleep(3 * pingEvery)
		if d := time.Since(start); d < 3*pingEvery {
			t.Fatalf("the clock did not advance past the ticks: %s", d)
		}

		// Nobody was dropped: the next event still arrives on both.
		resp = do("POST", "/v1/streams/"+created.ID, `{"data":"two"}`, secretHead)
		code = resp.StatusCode
		closeBody(resp)
		if code != http.StatusOK {
			t.Fatalf("append = %d", code)
		}
		if f := nextEvent(tokenR); f.id != "2" {
			t.Fatalf("token reader after ticks = %+v", f)
		}
		if f := nextEvent(secretR); f.id != "2" {
			t.Fatalf("secret reader after ticks = %+v", f)
		}

		// Wind down inside the bubble: close the reader bodies, then let
		// the clock tick once so the handlers and transport loops get
		// scheduled to notice their closed connections and exit.
		_ = tokenBody.Close()
		_ = secretBody.Close()
		time.Sleep(time.Second)
	})
}

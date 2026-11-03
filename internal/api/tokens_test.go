package api

import (
	"encoding/json"
	"net/http"
	"slices"
	"testing"
)

// TestSecretReads pins the server secret as a reader: header only, valid
// across revoke and rotate, and stat stays a host tool the token cannot open.
func TestSecretReads(t *testing.T) {
	r := newRig(t)
	id, token := r.create()
	r.append(id, `{"data":"one"}`)
	head := map[string]string{"Authorization": "Bearer secret"}

	// History reads with the secret from the header.
	resp := r.do("GET", "/v1/streams/"+id+"/events", "", head)
	defer closeBody(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("history with secret = %d", resp.StatusCode)
	}
	var h historyResponse
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		t.Fatal(err)
	}
	if got := historySeqs(h); !slices.Equal(got, []int64{1}) {
		t.Fatalf("history with secret = %v", got)
	}

	// The secret never rides in the query string: ?token= stays a token.
	if resp := r.do("GET", "/v1/streams/"+id+"?token=secret", "", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("secret in query = %d", resp.StatusCode)
	}
	if resp := r.do("GET", "/v1/streams/"+id+"/stat", "", map[string]string{"Authorization": "Bearer " + token}); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("stat with stream token = %d", resp.StatusCode)
	}

	// SSE with the secret replays, then follows live.
	sresp, br, cancel := openStream(t, r.srv.URL+"/v1/streams/"+id+"?after=0", head)
	defer cancel()
	defer closeBody(sresp)
	if f := readFrame(t, br); f.id != "1" {
		t.Fatalf("replay with secret = %+v", f)
	}
	r.append(id, `{"data":"two"}`)
	if f := readFrame(t, br); f.id != "2" {
		t.Fatalf("live with secret = %+v", f)
	}

	// Revoke kills the token, not the secret; rotate never touches it.
	r.revoke(id)
	resp = r.do("GET", "/v1/streams/"+id+"/events", "", head)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("history with secret after revoke = %d", resp.StatusCode)
	}
	r.rotate(id)
	probe := r.do("GET", "/v1/streams/"+id, "", head)
	_ = probe.Body.Close()
	if probe.StatusCode != http.StatusOK {
		t.Fatalf("stream with secret after rotate = %d", probe.StatusCode)
	}
}

func TestReaderTokens(t *testing.T) {
	r := newRig(t)
	id, token := r.create()
	r.append(id, `{"data":"one"}`)

	if resp := r.do("GET", "/v1/streams/"+id, "", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no token = %d", resp.StatusCode)
	}
	if resp := r.do("GET", "/v1/streams/"+id+"?token=wrong", "", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong token = %d", resp.StatusCode)
	}

	// Non-browser clients may send the token as a bearer credential instead.
	head := r.do("GET", "/v1/streams/"+id, "", map[string]string{"Authorization": "Bearer " + token})
	if head.StatusCode != http.StatusOK {
		t.Fatalf("token header = %d", head.StatusCode)
	}
	_ = head.Body.Close()
	if resp := r.do("GET", "/v1/streams/"+id, "", map[string]string{"Authorization": "Bearer wrong"}); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong token header = %d", resp.StatusCode)
	}

	resp, br, cancel := openStream(t, r.streamURL(id, token, "0"), nil)
	defer cancel()
	defer closeBody(resp)
	if f := readFrame(t, br); f.id != "1" {
		t.Fatalf("frame = %+v", f)
	}

	if st := r.stat(id); st.LastSeq != 1 || !st.Readable {
		t.Fatalf("stat = %+v", st)
	}

	// Rotate: the old token dies and the open reader is disconnected.
	rotated := r.rotate(id)
	if _, err := br.ReadString('\n'); err == nil {
		t.Fatal("reader stayed open after rotate")
	}
	if resp := r.do("GET", "/v1/streams/"+id+"?token="+token, "", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("old token after rotate = %d", resp.StatusCode)
	}
	probe := r.do("GET", "/v1/streams/"+id+"?token="+rotated, "", nil)
	if probe.StatusCode != http.StatusOK {
		t.Fatalf("rotated token = %d", probe.StatusCode)
	}
	_ = probe.Body.Close()

	// Revoke: no token opens the stream until the next rotate.
	if code := r.revoke(id); code != http.StatusNoContent {
		t.Fatalf("revoke = %d", code)
	}
	if resp := r.do("GET", "/v1/streams/"+id+"?token="+rotated, "", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("token after revoke = %d", resp.StatusCode)
	}
	if st := r.stat(id); st.Readable {
		t.Fatalf("readable after revoke = %+v", st)
	}
	rotated = r.rotate(id)
	if st := r.stat(id); !st.Readable {
		t.Fatalf("readable after rotate = %+v", st)
	}

	// Delete removes the stream: 404, even with the token that used to work.
	del := r.write("DELETE", "/v1/streams/"+id, "")
	_ = del.Body.Close()
	if resp := r.do("GET", "/v1/streams/"+id+"?token="+rotated, "", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("get after delete = %d", resp.StatusCode)
	}
	if resp := r.do("GET", "/v1/streams/"+id+"/stat", "", map[string]string{"Authorization": "Bearer secret"}); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("stat after delete = %d", resp.StatusCode)
	}
}

func (r *rig) stat(id string) statResponse {
	r.t.Helper()
	resp := r.write("GET", "/v1/streams/"+id+"/stat", "")
	defer closeBody(resp)
	if resp.StatusCode != http.StatusOK {
		r.t.Fatalf("stat = %d", resp.StatusCode)
	}
	var out statResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		r.t.Fatal(err)
	}
	return out
}

func (r *rig) rotate(id string) string {
	r.t.Helper()
	resp := r.write("POST", "/v1/streams/"+id+"/rotate", "")
	defer closeBody(resp)
	if resp.StatusCode != http.StatusOK {
		r.t.Fatalf("rotate = %d", resp.StatusCode)
	}
	var out struct{ Token string }
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || out.Token == "" {
		r.t.Fatalf("rotate body: %v %+v", err, out)
	}
	return out.Token
}

func (r *rig) revoke(id string) int {
	r.t.Helper()
	resp := r.write("POST", "/v1/streams/"+id+"/revoke", "")
	_ = resp.Body.Close()
	return resp.StatusCode
}

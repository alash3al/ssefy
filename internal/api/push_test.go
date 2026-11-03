package api

import (
	"encoding/json"
	"net/http"
	"testing"
)

func push(t *testing.T, r *rig, body, idemKey string) map[string]pushResult {
	t.Helper()
	hdr := map[string]string{"Authorization": "Bearer secret"}
	if idemKey != "" {
		hdr["Idempotency-Key"] = idemKey
	}
	resp := r.do("POST", "/v1/push", body, hdr)
	defer closeBody(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("push = %d", resp.StatusCode)
	}
	var out struct {
		Results map[string]pushResult `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out.Results
}

func TestBatchPush(t *testing.T) {
	r := newRig(t)
	a, _ := r.create()
	b, _ := r.create()

	results := push(t, r, `{"streams":["`+a+`","`+b+`","missing"],"events":[{"data":"one"}]}`, "")
	if got := results[a]; got.Error != "" || len(got.Seqs) != 1 || got.Seqs[0] != 1 {
		t.Fatalf("a = %+v", got)
	}
	if got := results[b]; got.Error != "" || got.Seqs[0] != 1 {
		t.Fatalf("b = %+v", got)
	}
	if got := results["missing"]; got.Error != "stream not found" || got.Seqs != nil {
		t.Fatalf("missing = %+v", got)
	}

	// Each stream keeps its own seq and cursor space.
	r.append(a, `{"data":"two"}`)
	if got := push(t, r, `{"streams":["`+b+`"],"events":[{"data":"x"}]}`, "")[b]; got.Seqs[0] != 2 {
		t.Fatalf("b seqs = %+v", got)
	}
}

func TestBatchPushIdempotency(t *testing.T) {
	r := newRig(t)
	a, _ := r.create()
	b, _ := r.create()

	body := `{"streams":["` + a + `","` + b + `"],"events":[{"data":"one"},{"data":"two"}]}`
	first := push(t, r, body, "batch-1")
	again := push(t, r, body, "batch-1")

	for _, id := range []string{a, b} {
		if first[id].Error != "" || len(first[id].Seqs) != 2 {
			t.Fatalf("first[%s] = %+v", id, first[id])
		}
		if again[id].Error != "" || again[id].Seqs[0] != first[id].Seqs[0] {
			t.Fatalf("retry[%s] = %+v, want %+v", id, again[id], first[id])
		}
	}
}

func TestBatchPushValidation(t *testing.T) {
	r := newRig(t)
	id, _ := r.create()

	cases := []struct{ body string }{
		{`{"streams":[],"events":[{"data":"x"}]}`},
		{`{"streams":["` + id + `","` + id + `"],"events":[{"data":"x"}]}`},
		{`{"streams":["` + id + `"]}`},
		{`{"streams":["` + id + `"],"events":[]}`},
	}
	for _, c := range cases {
		resp := r.do("POST", "/v1/push", c.body, map[string]string{"Authorization": "Bearer secret"})
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s = %d, want 400", c.body, resp.StatusCode)
		}
	}
}

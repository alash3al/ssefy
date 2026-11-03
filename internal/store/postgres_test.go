package store

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func openTestPostgres(t *testing.T) Store {
	t.Helper()
	dsn := os.Getenv("SSEFY_TEST_DB")
	if dsn == "" {
		t.Skip("set SSEFY_TEST_DB to run PostgreSQL tests")
	}
	s, err := OpenPostgres(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPostgresLifecycle(t *testing.T) {
	s := openTestPostgres(t)
	defer func() { _ = s.Close() }()
	testLifecycle(t, s)
	testList(t, s)
	testTrim(t, s)
}

// The whole design rests on seqs committing in order, so readers never skip one.
func TestPostgresConcurrentSeqs(t *testing.T) {
	s := openTestPostgres(t)
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	id, token, err := s.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}

	const writers, each = 8, 5
	var wg sync.WaitGroup
	for range writers {
		wg.Go(func() {
			for range each {
				if _, err := s.Append(ctx, id, []Event{{Event: "message", Data: "x"}}, ""); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()

	rows, err := s.Read(ctx, id, 0, writers*each+1)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != writers*each {
		t.Fatalf("rows = %d, want %d", len(rows), writers*each)
	}
	for i, row := range rows {
		if row.Seq != int64(i+1) {
			t.Fatalf("row %d has seq %d, want %d", i, row.Seq, i+1)
		}
	}
	if last, err := s.OpenReader(ctx, id, token); err != nil || last != writers*each {
		t.Fatalf("last = %d, %v", last, err)
	}
}

// Ready keeps answering healthy for listenerGrace after the listener drops,
// then stops: a balancer only sees the failure once it is real. The grace
// clock resets on every reconnect, so the integration suite's kill loop (a
// hard flap, by construction) can never observe the 503 — the decision
// itself is pinned here instead, deterministically.
func TestReadyGrace(t *testing.T) {
	dsn := os.Getenv("SSEFY_TEST_DB")
	if dsn == "" {
		t.Skip("set SSEFY_TEST_DB to run PostgreSQL tests")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	p := &postgres{pool: pool}

	p.markListenerDown() // downSince starts now: inside the grace
	if err := p.Ready(context.Background()); err != nil {
		t.Fatalf("within grace = %v", err)
	}
	p.downSince.Store(time.Now().Add(-listenerGrace - time.Second).UnixNano())
	if err := p.Ready(context.Background()); err == nil {
		t.Fatal("past grace = healthy")
	}
	p.markListenerUp()
	if err := p.Ready(context.Background()); err != nil {
		t.Fatalf("listener up = %v", err)
	}
}

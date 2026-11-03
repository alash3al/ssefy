//go:build unix

package integration

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestSlowReaderCannotBlock stalls one reader — a raw socket with a 4KB
// receive buffer that connects, reads its headers, and never reads again —
// while publishers and healthy readers on both nodes carry on. The kernel's
// send-buffer autotune absorbs several megabytes before the node's write to
// such a socket actually blocks, so the test pushes past that ceiling (read
// from the running kernel) and then proves the hub's own bound: a transient
// for a full wake-up channel may be dropped, a stored wake-up never is, so
// the stalled reader is disconnected instead. Publishers never block, and
// once the stalled client is gone its resources are released. It can then
// reconnect and re-read everything retained.
func TestSlowReaderCannotBlock(t *testing.T) {
	requireDB(t)
	a, b := newPair(t)
	id, token := a.create(t)

	// The stalled reader: raw socket, tiny receive buffer, headers only.
	conn := dialStalled(t, a, "/v1/streams/"+id+"?token="+token+"&after=0")
	defer func() { _ = conn.Close() }()

	hA := openSSE(t, a, "/v1/streams/"+id+"?token="+token+"&after=0", nil)
	hB := openSSE(t, b, "/v1/streams/"+id+"?token="+token+"&after=0", nil)
	waitFor(t, 5*time.Second, func() bool { return a.metric(t, "ssefy_readers") == 2 })

	// Push past the send-buffer ceiling so the node's write to the stalled
	// reader genuinely blocks mid-drain; the publisher stays fast throughout.
	const eventsPerMB = 16
	big := strings.Repeat("x", 60<<10)
	oneMB := make([]string, eventsPerMB)
	for i := range oneMB {
		oneMB[i] = `{"data":"` + big + `"}`
	}
	batches := sendBufferCap(t)*3/2/(1<<20) + 1
	for i := 1; i <= batches; i++ {
		start := time.Now()
		b.appendEvents(t, id, "["+strings.Join(oneMB, ",")+"]", "")
		if d := time.Since(start); d > 5*time.Second {
			t.Fatalf("publisher blocked %s behind a slow reader", d)
		}
	}
	bigTotal := int64(eventsPerMB * batches)

	// The handler's write is now blocked, so its wake-up channel only fills.
	// A burst of transients queues what fits and drops the rest — a
	// transient may be lost for a reader that is behind.
	const bursts = 12
	for range bursts {
		b.appendEvents(t, id, `{"event":"typing","data":"tick","transient":true}`, "")
	}
	waitFor(t, 10*time.Second, func() bool { return a.metric(t, "ssefy_transient_dropped_total") >= 1 })

	// The channel is full now, so the next stored wake-up cannot be queued:
	// the stalled reader is disconnected instead — the hub never loses a
	// stored wake, and it never blocks on anyone.
	b.appendEvents(t, id, `{"data":"last"}`, "")
	waitFor(t, 10*time.Second, func() bool { return a.metric(t, "ssefy_slow_readers_total") >= 1 })

	// The healthy readers saw everything in order: the big events, the
	// transients, then the last stored event.
	for i := int64(1); i <= bigTotal; i++ {
		if f := hA.next(t, 10*time.Second); f.id != i {
			t.Fatalf("healthy reader on a, frame %d = %+v", i, f)
		}
		if f := hB.next(t, 10*time.Second); f.id != i {
			t.Fatalf("healthy reader on b, frame %d = %+v", i, f)
		}
	}
	for range bursts {
		for _, r := range []*sse{hA, hB} {
			if f := r.next(t, 5*time.Second); f.id != 0 || f.data != "tick" {
				t.Fatalf("healthy transient = %+v", f)
			}
		}
	}
	for _, r := range []*sse{hA, hB} {
		if f := r.next(t, 5*time.Second); f.id != bigTotal+1 {
			t.Fatalf("healthy last frame = %+v", f)
		}
	}

	// Resources are released once the stalled client is gone.
	_ = conn.Close()
	waitFor(t, 10*time.Second, func() bool { return a.metric(t, "ssefy_readers") == 1 })

	// And it can come back and re-read everything retained.
	recovered := openSSE(t, a, "/v1/streams/"+id+"?token="+token+"&after=0", nil)
	for i := int64(1); i <= bigTotal+1; i++ {
		if f := recovered.next(t, 10*time.Second); f.id != i {
			t.Fatalf("recovered reader, frame %d = %+v", i, f)
		}
	}
	if !recovered.quiet(500 * time.Millisecond) {
		t.Fatal("unexpected frame after recovery")
	}
}

// dialStalled opens one raw HTTP connection with a tiny receive buffer and
// reads only the response headers: the node streams to it forever, but the
// socket never drains.
func dialStalled(t *testing.T, n *node, path string) net.Conn {
	t.Helper()
	d := net.Dialer{Timeout: 5 * time.Second, Control: func(_, _ string, c syscall.RawConn) error {
		return c.Control(func(fd uintptr) {
			_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 4096)
		})
	}}
	conn, err := d.Dial("tcp", strings.TrimPrefix(n.base, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: ssefy\r\n\r\n", path); err != nil {
		t.Fatalf("stalled reader request: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	br := bufio.NewReader(conn)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("stalled reader headers: %v", err)
		}
		if strings.HasPrefix(line, "HTTP/") && !strings.Contains(line, " 200 ") {
			t.Fatalf("stalled reader status: %s", strings.TrimSpace(line))
		}
		if line == "\r\n" {
			break
		}
	}
	_ = conn.SetReadDeadline(time.Time{})
	return conn
}

// sendBufferCap is the kernel's send-buffer autotune ceiling: a write to a
// never-reading socket blocks only after that many bytes are queued.
func sendBufferCap(t *testing.T) int {
	t.Helper()
	if b, err := os.ReadFile("/proc/sys/net/ipv4/tcp_wmem"); err == nil {
		f := strings.Fields(string(b))
		if len(f) == 3 {
			if n, err := strconv.Atoi(f[2]); err == nil {
				return n
			}
		}
	}
	return 4 << 20 // the common Linux default
}

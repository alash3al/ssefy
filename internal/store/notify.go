package store

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"time"
)

const (
	notifyChannel = "ssefy"
	maxNotify     = 7800 // NOTIFY payloads must stay under 8000 bytes
)

// listen holds one dedicated connection so every node sees every commit.
// Notices are hints, never the data: a reader always reads the database.
func (p *postgres) listen() {
	for p.ctx.Err() == nil {
		if err := p.listenOnce(); err != nil && p.ctx.Err() == nil {
			slog.Warn("notification listener reconnecting", "err", err)
			sleepCtx(p.ctx, time.Second)
		}
	}
}

func (p *postgres) listenOnce() error {
	pc, err := p.pool.Acquire(p.ctx)
	if err != nil {
		return err
	}
	conn := pc.Hijack()
	defer func() { _ = conn.Close(context.Background()) }()

	if _, err := conn.Exec(p.ctx, "LISTEN "+notifyChannel); err != nil {
		return err
	}
	// Anything committed while this node was not listening has no notice;
	// wake every local reader to re-read its stream.
	p.hub.broadcast()

	for {
		n, err := conn.WaitForNotification(p.ctx)
		if err != nil {
			return err
		}
		p.dispatch(n.Payload)
	}
}

func (p *postgres) dispatch(payload string) {
	var msg notice
	if err := json.Unmarshal([]byte(payload), &msg); err != nil || msg.ID == "" {
		return
	}
	switch {
	case msg.Deleted:
		p.hub.close(msg.ID)
	case msg.Transient:
		p.hub.notify(msg.ID, Notice{Transient: true, Event: msg.Event, Data: msg.Data})
	default:
		p.hub.notify(msg.ID, Notice{})
	}
}

type notice struct {
	ID        string `json:"i"`
	Deleted   bool   `json:"d,omitempty"`
	Transient bool   `json:"t,omitempty"`
	Event     string `json:"e,omitempty"`
	Data      string `json:"x,omitempty"`
}

func encodeNotice(n notice) (string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false) // keep payloads small; this is not HTML
	if err := enc.Encode(n); err != nil {
		return "", err
	}
	return string(bytes.TrimRight(buf.Bytes(), "\n")), nil
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

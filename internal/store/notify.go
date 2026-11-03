package store

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/alash3al/ssefy/internal/metrics"
)

const (
	notifyChannel = "ssefy"
	// PostgreSQL rejects NOTIFY payloads of 8000 bytes or more.
	maxTransientPayload = 7800
)

// listen holds one dedicated connection so every node sees every commit.
// Notices are hints, never the data: a reader always reads the database.
func (p *postgres) listen() {
	defer p.markListenerDown()
	for p.ctx.Err() == nil {
		if err := p.listenOnce(); err != nil && p.ctx.Err() == nil {
			p.markListenerDown()
			metrics.ListenerReconnects.Inc()
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
	p.markListenerUp()
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
	case msg.Close:
		metrics.Notifications.WithLabelValues("close").Inc()
		p.hub.close(msg.ID)
	case msg.Transient:
		metrics.Notifications.WithLabelValues("transient").Inc()
		p.hub.notify(msg.ID, Notice{Transient: true, Event: msg.Event, Data: msg.Data})
	default:
		metrics.Notifications.WithLabelValues("append").Inc()
		p.hub.notify(msg.ID, Notice{})
	}
}

type notice struct {
	ID        string `json:"i"`
	Close     bool   `json:"c,omitempty"`
	Transient bool   `json:"t,omitempty"`
	Event     string `json:"e,omitempty"`
	Data      string `json:"x,omitempty"`
}

// encodeTransients encodes one wake-up payload per transient event and rejects
// any that would not fit. Both stores use it, so memory:// and postgres://
// accept exactly the same transient pushes.
func encodeTransients(id string, events []Event) ([]string, error) {
	payloads := make([]string, len(events))
	for i, ev := range events {
		payload, err := encodeNotice(notice{ID: id, Transient: true, Event: ev.Event, Data: ev.Data})
		if err != nil {
			return nil, err
		}
		if len(payload) > maxTransientPayload {
			return nil, ErrTooLarge
		}
		payloads[i] = payload
	}
	return payloads, nil
}

// notifyClose tells every node to end its readers of this stream. The reason
// is not in the payload: the reconnect gets 404 after rotate/revoke and 410
// after delete.
func notifyClose(ctx context.Context, tx pgx.Tx, id string) error {
	payload, err := encodeNotice(notice{ID: id, Close: true})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChannel, payload)
	return err
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

// Package store keeps streams of events and wakes readers when one changes.
package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNotFound    = errors.New("stream not found")
	ErrKeyMismatch = errors.New("idempotency key reused with a different body")
	ErrTooLarge    = errors.New("transient event too large")
	ErrGone        = errors.New("stream deleted")
)

// Event is one pushed event. Event defaults to "message" before it reaches a Store.
type Event struct {
	Event       string `json:"event"`
	Data        string `json:"data"`
	Transient   bool   `json:"transient"`
	CollapseKey string `json:"collapse_key"`
}

// Row is a stored event.
type Row struct {
	Seq       int64
	Event     string
	Data      string
	CreatedAt time.Time
}

// Notice wakes readers of one stream. A zero Notice means "read what is
// stored"; a transient one also carries an event that was never stored.
// Deleting a stream closes its subscribers' channels instead.
type Notice struct {
	Transient bool
	Event     string
	Data      string
}

// Store persists streams and wakes local readers. Append requires validated,
// non-empty events; seqs are per stream, assigned in commit order, never reused.
type Store interface {
	// Create mints a stream. A retried create makes a new one: the caller
	// owns the id, so an unknown id is simply lost.
	Create(ctx context.Context) (string, error)
	Append(ctx context.Context, stream string, events []Event, idemKey string) ([]int64, error)
	Delete(ctx context.Context, stream string) error
	// Read returns stored events after a seq. limit must be positive.
	Read(ctx context.Context, stream string, after int64, limit int) ([]Row, error)
	LastSeq(ctx context.Context, stream string) (int64, error)
	Live(stream string) (<-chan Notice, func())
	// Ready reports whether this node can serve: the database answers and,
	// in postgres mode, notifications are flowing.
	Ready(ctx context.Context) error
	Close() error
}

// newID returns 128 random bits, the whole identity of a stream.
func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// hashEvents is the idempotency fingerprint. Events have a fixed field order,
// so encoding/json output is stable.
func hashEvents(events []Event) string {
	b, _ := json.Marshal(events)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Package store keeps streams of events and wakes readers when one changes.
package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
)

var (
	ErrNotFound    = errors.New("stream not found")
	ErrKeyMismatch = errors.New("idempotency key reused with a different body")
	ErrTooLarge    = errors.New("transient event too large")
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
	Seq   int64
	Event string
	Data  string
}

// Notice wakes readers of one stream. A plain Notice means "read what is
// stored"; the others carry state that is not in the database.
type Notice struct {
	Closed    bool
	Transient bool
	Event     string
	Data      string
}

type Store interface {
	Create(ctx context.Context) (string, error)
	Append(ctx context.Context, stream string, events []Event, idemKey string) ([]int64, error)
	Delete(ctx context.Context, stream string) error
	Read(ctx context.Context, stream string, after int64, limit int) ([]Row, error)
	LastSeq(ctx context.Context, stream string) (int64, error)
	Live(stream string) (<-chan Notice, func())
	Ping(ctx context.Context) error
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

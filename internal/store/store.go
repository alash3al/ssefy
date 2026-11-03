// Package store keeps streams of events and wakes readers when one changes.
package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrNotFound means the id never existed or the token does not open it.
	ErrNotFound = errors.New("stream not found")
	// ErrTooLarge means a transient event does not fit one wake-up payload.
	ErrTooLarge = errors.New("transient event too large")
	// ErrGone means the stream was deleted; its id is a tombstone.
	ErrGone = errors.New("stream deleted")
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

// Stat is the operational view of one stream.
type Stat struct {
	LastSeq   int64
	CreatedAt time.Time
	DeletedAt *time.Time
	Readable  bool
}

// Notice wakes readers of one stream. A zero Notice means "read what is
// stored"; a transient one also carries an event that was never stored.
type Notice struct {
	Transient bool
	Event     string
	Data      string
}

// Store persists streams and wakes local readers. Append requires validated,
// non-empty events; seqs are per stream, assigned in commit order, never reused.
type Store interface {
	// Create mints a stream and the token that opens its reads.
	Create(ctx context.Context) (id, token string, err error)
	// Append stores events and returns their seqs.
	Append(ctx context.Context, stream string, events []Event, idemKey string) ([]int64, error)
	// Delete tombstones a stream and removes its events.
	Delete(ctx context.Context, stream string) error
	// Read returns stored events after a seq. limit must be positive.
	Read(ctx context.Context, stream string, after int64, limit int) ([]Row, error)
	// OpenReader authorizes a token and returns the high-water seq.
	OpenReader(ctx context.Context, stream, token string) (int64, error)
	// Rotate mints a new token, invalidating the old one.
	Rotate(ctx context.Context, stream string) (string, error)
	// Revoke clears the token until the next Rotate.
	Revoke(ctx context.Context, stream string) error
	// Stat returns the operational view of one stream, tombstones included.
	Stat(ctx context.Context, stream string) (Stat, error)
	// Live subscribes to local wake-ups for a stream. The returned func unsubscribes.
	Live(stream string) (<-chan Notice, func())
	// Ready reports whether this node can serve: the database answers and,
	// in postgres mode, notifications are flowing.
	Ready(ctx context.Context) error
	// Close releases the store's resources.
	Close() error
}

// newStreamID returns a time-sortable UUIDv7, the public stream id.
func newStreamID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

// newToken returns 128 random bits, the secret that opens one stream.
func newToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

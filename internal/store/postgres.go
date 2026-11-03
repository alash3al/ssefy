package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"io/fs"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	// Registers "pgx" with database/sql for goose.
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"github.com/alash3al/ssefy/internal/metrics"
)

//go:embed migrations/*.sql
var migrations embed.FS

// postgres is the real store. Writes bump one stream row under FOR UPDATE, so
// seqs commit in order and no reader can ever skip one.
type postgres struct {
	pool   *pgxpool.Pool
	hub    *hub
	ctx    context.Context
	cancel context.CancelFunc

	listenerUp atomic.Bool
	downSince  atomic.Int64 // unix nanos, 0 while the listener is up
}

// listenerGrace is how long a node keeps answering healthy after losing its
// LISTEN connection, so a one-second blip does not pull it from the balancer.
const listenerGrace = 30 * time.Second

// OpenPostgres migrates the schema, opens the pool and starts the listener.
func OpenPostgres(ctx context.Context, dsn string) (Store, error) {
	if err := migrate(ctx, dsn); err != nil {
		return nil, err
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	p := &postgres{pool: pool, hub: newHub()}
	p.downSince.Store(time.Now().UnixNano())
	p.ctx, p.cancel = context.WithCancel(context.Background())
	go p.listen()
	return p, nil
}

// migrate runs goose under a PostgreSQL advisory lock, so two nodes starting
// at once cannot apply the same migration twice.
func migrate(ctx context.Context, dsn string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	fsys, err := fs.Sub(migrations, "migrations")
	if err != nil {
		return err
	}
	sessionLocker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return err
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, fsys,
		goose.WithSessionLocker(sessionLocker))
	if err != nil {
		return err
	}
	_, err = provider.Up(ctx)
	return err
}

func (p *postgres) Create(ctx context.Context) (string, string, error) {
	for range 3 {
		id, err := newStreamID()
		if err != nil {
			return "", "", err
		}
		token, err := newToken()
		if err != nil {
			return "", "", err
		}
		_, err = p.pool.Exec(ctx, "INSERT INTO ssefy_streams (id, token) VALUES ($1, $2)", id, token)
		if err == nil {
			return id, token, nil
		}
		if !isUnique(err) {
			return "", "", err
		}
	}
	return "", "", errors.New("could not allocate a stream id")
}

func (p *postgres) Append(ctx context.Context, id string, events []Event, idemKey string) ([]int64, error) {
	if events[0].Transient {
		err := p.appendTransient(ctx, id, events)
		return nil, err
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var last int64
	err = tx.QueryRow(ctx, "SELECT last_seq FROM ssefy_streams WHERE id = $1 FOR UPDATE", id).Scan(&last)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	if idemKey != "" {
		var storedSeqs []int64
		err := tx.QueryRow(ctx,
			"SELECT seqs FROM ssefy_appends WHERE stream_id = $1 AND key = $2",
			id, idemKey).Scan(&storedSeqs)
		switch {
		case err == nil:
			return storedSeqs, nil
		case errors.Is(err, pgx.ErrNoRows):
			// first push
		default:
			return nil, err
		}
	}

	for _, key := range collapseKeys(events) {
		if _, err := tx.Exec(ctx, "DELETE FROM ssefy_events WHERE stream_id = $1 AND collapse_key = $2", id, key); err != nil {
			return nil, err
		}
	}

	seqs := make([]int64, len(events))
	batch := &pgx.Batch{}
	for i, ev := range events {
		seqs[i] = last + int64(i) + 1
		batch.Queue(`INSERT INTO ssefy_events
			(stream_id, seq, event, data, collapse_key)
			VALUES ($1, $2, $3, $4, $5)`,
			id, seqs[i], ev.Event, ev.Data, nullString(ev.CollapseKey))
	}
	results := tx.SendBatch(ctx, batch)
	for range events {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return nil, err
		}
	}
	if err := results.Close(); err != nil {
		return nil, err
	}

	if idemKey != "" {
		if _, err := tx.Exec(ctx,
			"INSERT INTO ssefy_appends (stream_id, key, seqs) VALUES ($1, $2, $3)",
			id, idemKey, seqs); err != nil {
			return nil, err
		}
	}

	newLast := last + int64(len(events))
	if _, err := tx.Exec(ctx, "UPDATE ssefy_streams SET last_seq = $2 WHERE id = $1", id, newLast); err != nil {
		return nil, err
	}
	payload, err := encodeNotice(notice{ID: id})
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChannel, payload); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return seqs, nil
}

func (p *postgres) appendTransient(ctx context.Context, id string, events []Event) error {
	payloads, err := encodeTransients(id, events)
	if err != nil {
		return err
	}

	var one int
	err = p.pool.QueryRow(ctx, "SELECT 1 FROM ssefy_streams WHERE id = $1", id).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}

	for _, payload := range payloads {
		if _, err := p.pool.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChannel, payload); err != nil {
			return err
		}
	}
	return nil
}

// Delete removes the stream; its events and append records cascade.
func (p *postgres) Delete(ctx context.Context, id string) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, "DELETE FROM ssefy_streams WHERE id = $1", id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx) // unknown id: no notice
	}
	if err := notifyClose(ctx, tx, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (p *postgres) Read(ctx context.Context, id string, after int64, limit int) ([]Row, error) {
	rows, err := p.pool.Query(ctx,
		"SELECT seq, event, data, created_at FROM ssefy_events WHERE stream_id = $1 AND seq > $2 ORDER BY seq LIMIT $3",
		id, after, limit)
	if err != nil {
		return nil, err
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByName[Row])
	if err != nil {
		return nil, err
	}
	if len(out) > 0 {
		return out, nil
	}

	var one int
	err = p.pool.QueryRow(ctx, "SELECT 1 FROM ssefy_streams WHERE id = $1", id).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return nil, err
}

func (p *postgres) OpenReader(ctx context.Context, id, token string) (int64, error) {
	var n int64
	var stored *string
	err := p.pool.QueryRow(ctx,
		"SELECT last_seq, token FROM ssefy_streams WHERE id = $1",
		id).Scan(&n, &stored)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	if token == "" || stored == nil || !sameToken(*stored, token) {
		return 0, ErrUnauthorized
	}
	return n, nil
}

func (p *postgres) Rotate(ctx context.Context, id string) (string, error) {
	token, err := newToken()
	if err != nil {
		return "", err
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, "UPDATE ssefy_streams SET token = $2 WHERE id = $1", id, token)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() == 0 {
		return "", ErrNotFound
	}
	if err := notifyClose(ctx, tx, id); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return token, nil
}

func (p *postgres) Revoke(ctx context.Context, id string) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, "UPDATE ssefy_streams SET token = NULL WHERE id = $1", id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	if err := notifyClose(ctx, tx, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (p *postgres) Stat(ctx context.Context, id string) (Stat, error) {
	var st Stat
	var hasToken bool
	err := p.pool.QueryRow(ctx,
		"SELECT last_seq, created_at, token IS NOT NULL FROM ssefy_streams WHERE id = $1",
		id).Scan(&st.LastSeq, &st.CreatedAt, &hasToken)
	if errors.Is(err, pgx.ErrNoRows) {
		return Stat{}, ErrNotFound
	}
	if err != nil {
		return Stat{}, err
	}
	st.Readable = hasToken
	return st, nil
}

func (p *postgres) Live(stream string) (<-chan Notice, func()) {
	return p.hub.subscribe(stream)
}

func (p *postgres) Ready(ctx context.Context) error {
	if err := p.pool.Ping(ctx); err != nil {
		return err
	}
	if p.listenerUp.Load() {
		return nil
	}
	if time.Since(time.Unix(0, p.downSince.Load())) < listenerGrace {
		return nil
	}
	return errors.New("notifications are not flowing")
}

func (p *postgres) markListenerUp() {
	p.downSince.Store(0)
	p.listenerUp.Store(true)
	metrics.ListenerUp.Set(1)
}

func (p *postgres) markListenerDown() {
	p.downSince.CompareAndSwap(0, time.Now().UnixNano())
	p.listenerUp.Store(false)
	metrics.ListenerUp.Set(0)
}

func (p *postgres) Close() error {
	p.cancel()
	p.pool.Close()
	return nil
}

func collapseKeys(events []Event) []string {
	var keys []string
	for _, ev := range events {
		if ev.CollapseKey != "" {
			keys = append(keys, ev.CollapseKey)
		}
	}
	return keys
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func isUnique(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

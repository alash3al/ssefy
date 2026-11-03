package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
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
}

func OpenPostgres(ctx context.Context, dsn string) (Store, error) {
	if err := migrate(dsn); err != nil {
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
	p.ctx, p.cancel = context.WithCancel(context.Background())
	go p.listen()
	return p, nil
}

func migrate(dsn string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	goose.SetBaseFS(migrations)
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	return goose.Up(db, "migrations")
}

func (p *postgres) Create(ctx context.Context, key string) (string, error) {
	if key != "" {
		return p.createWithKey(ctx, key)
	}
	for range 3 {
		id, err := newID()
		if err != nil {
			return "", err
		}
		_, err = p.pool.Exec(ctx, "INSERT INTO ssefy_streams (id) VALUES ($1)", id)
		if err == nil {
			return id, nil
		}
		if !isUnique(err) {
			return "", err
		}
	}
	return "", errors.New("could not allocate a stream id")
}

// createWithKey inserts the stream and its key atomically. When the key is
// already taken the transaction rolls back, so no orphan stream is left.
func (p *postgres) createWithKey(ctx context.Context, key string) (string, error) {
	for range 3 {
		id, err := newID()
		if err != nil {
			return "", err
		}
		tx, err := p.pool.Begin(ctx)
		if err != nil {
			return "", err
		}
		if _, err := tx.Exec(ctx, "INSERT INTO ssefy_streams (id) VALUES ($1)", id); err != nil {
			_ = tx.Rollback(ctx)
			if isUnique(err) {
				continue
			}
			return "", err
		}
		tag, err := tx.Exec(ctx,
			"INSERT INTO ssefy_create_keys (key, stream_id) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING",
			key, id)
		if err != nil {
			_ = tx.Rollback(ctx)
			return "", err
		}
		if tag.RowsAffected() == 1 {
			if err := tx.Commit(ctx); err != nil {
				return "", err
			}
			return id, nil
		}
		_ = tx.Rollback(ctx)

		var existing string
		err = p.pool.QueryRow(ctx, "SELECT stream_id FROM ssefy_create_keys WHERE key = $1", key).Scan(&existing)
		if errors.Is(err, pgx.ErrNoRows) {
			continue // the other create was deleted in between
		}
		if err != nil {
			return "", err
		}
		return existing, nil
	}
	return "", errors.New("could not allocate a stream id")
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
	var deletedAt *time.Time
	err = tx.QueryRow(ctx, "SELECT last_seq, deleted_at FROM ssefy_streams WHERE id = $1 FOR UPDATE", id).Scan(&last, &deletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if deletedAt != nil {
		return nil, ErrGone
	}

	idemHash := ""
	if idemKey != "" {
		idemHash = hashEvents(events)
		var storedHash string
		var storedSeqs []int64
		err := tx.QueryRow(ctx,
			"SELECT hash, seqs FROM ssefy_appends WHERE stream_id = $1 AND key = $2",
			id, idemKey).Scan(&storedHash, &storedSeqs)
		switch {
		case err == nil:
			if storedHash != idemHash {
				return nil, ErrKeyMismatch
			}
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
			"INSERT INTO ssefy_appends (stream_id, key, hash, seqs) VALUES ($1, $2, $3, $4)",
			id, idemKey, idemHash, seqs); err != nil {
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
	var one int
	err := p.pool.QueryRow(ctx, "SELECT 1 FROM ssefy_streams WHERE id = $1", id).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	for _, ev := range events {
		payload, err := encodeNotice(notice{ID: id, Transient: true, Event: ev.Event, Data: ev.Data})
		if err != nil {
			return err
		}
		if len(payload) > maxNotify {
			return ErrTooLarge
		}
		if _, err := p.pool.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChannel, payload); err != nil {
			return err
		}
	}
	return nil
}

func (p *postgres) Delete(ctx context.Context, id string) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, "UPDATE ssefy_streams SET deleted_at = now() WHERE id = $1 AND deleted_at IS NULL", id)
	if err != nil {
		return err
	}
	for _, table := range []string{"ssefy_events", "ssefy_appends", "ssefy_create_keys"} {
		if _, err := tx.Exec(ctx, "DELETE FROM "+table+" WHERE stream_id = $1", id); err != nil {
			return err
		}
	}
	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx) // already gone or never existed: no notice
	}
	payload, err := encodeNotice(notice{ID: id, Deleted: true})
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChannel, payload); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (p *postgres) Read(ctx context.Context, id string, after int64, limit int) ([]Row, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := p.pool.Query(ctx,
		"SELECT seq, event, data, created_at FROM ssefy_events WHERE stream_id = $1 AND seq > $2 ORDER BY seq LIMIT $3",
		id, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Row
	for rows.Next() {
		var r Row
		if err := rows.Scan(&r.Seq, &r.Event, &r.Data, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) > 0 {
		return out, nil
	}

	var one int
	err = p.pool.QueryRow(ctx, "SELECT 1 FROM ssefy_streams WHERE id = $1 AND deleted_at IS NULL", id).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return nil, err
}

func (p *postgres) LastSeq(ctx context.Context, id string) (int64, error) {
	var n int64
	var deletedAt *time.Time
	err := p.pool.QueryRow(ctx, "SELECT last_seq, deleted_at FROM ssefy_streams WHERE id = $1", id).Scan(&n, &deletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err == nil && deletedAt != nil {
		return 0, ErrGone
	}
	return n, err
}

func (p *postgres) Live(stream string) (<-chan Notice, func()) {
	return p.hub.subscribe(stream)
}

func (p *postgres) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }

func (p *postgres) Close() error {
	p.cancel()
	p.pool.Close()
	return nil
}

func collapseKeys(events []Event) []string {
	seen := make(map[string]struct{}, len(events))
	var keys []string
	for _, ev := range events {
		if ev.CollapseKey == "" {
			continue
		}
		if _, ok := seen[ev.CollapseKey]; ok {
			continue
		}
		seen[ev.CollapseKey] = struct{}{}
		keys = append(keys, ev.CollapseKey)
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

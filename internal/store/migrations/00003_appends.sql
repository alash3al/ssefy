-- +goose Up
CREATE TABLE ssefy_appends (
    stream_id  text NOT NULL REFERENCES ssefy_streams(id) ON DELETE CASCADE,
    key        text NOT NULL,
    hash       text NOT NULL,
    seqs       bigint[] NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (stream_id, key)
);

DROP INDEX ssefy_events_idempotency_key;
ALTER TABLE ssefy_events DROP COLUMN idempotency_key;
ALTER TABLE ssefy_events DROP COLUMN idempotency_hash;

-- +goose Down
ALTER TABLE ssefy_events ADD COLUMN idempotency_key text;
ALTER TABLE ssefy_events ADD COLUMN idempotency_hash text;
CREATE UNIQUE INDEX ssefy_events_idempotency_key
    ON ssefy_events (stream_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
DROP TABLE ssefy_appends;

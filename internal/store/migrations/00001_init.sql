-- +goose Up
CREATE TABLE ssefy_streams (
    id         text PRIMARY KEY,
    last_seq   bigint NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ssefy_events (
    stream_id        text NOT NULL REFERENCES ssefy_streams(id) ON DELETE CASCADE,
    seq              bigint NOT NULL,
    event            text NOT NULL DEFAULT 'message',
    data             text NOT NULL,
    collapse_key     text,
    idempotency_key  text,
    idempotency_hash text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (stream_id, seq)
);

CREATE UNIQUE INDEX ssefy_events_collapse_key
    ON ssefy_events (stream_id, collapse_key)
    WHERE collapse_key IS NOT NULL;

CREATE UNIQUE INDEX ssefy_events_idempotency_key
    ON ssefy_events (stream_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- +goose Down
DROP TABLE ssefy_events;
DROP TABLE ssefy_streams;

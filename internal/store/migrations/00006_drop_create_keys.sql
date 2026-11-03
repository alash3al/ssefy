-- +goose Up
DROP TABLE ssefy_create_keys;

-- +goose Down
CREATE TABLE ssefy_create_keys (
    key        text PRIMARY KEY,
    stream_id  text NOT NULL REFERENCES ssefy_streams(id) ON DELETE CASCADE,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ssefy_create_keys_stream_id ON ssefy_create_keys (stream_id);

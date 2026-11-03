-- +goose Up
DROP INDEX ssefy_streams_deleted_at;

-- +goose Down
CREATE INDEX ssefy_streams_deleted_at ON ssefy_streams (deleted_at) WHERE deleted_at IS NOT NULL;

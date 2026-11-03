-- +goose Up
ALTER TABLE ssefy_streams ADD COLUMN deleted_at timestamptz;
CREATE INDEX ssefy_streams_deleted_at ON ssefy_streams (deleted_at) WHERE deleted_at IS NOT NULL;

-- +goose Down
DROP INDEX ssefy_streams_deleted_at;
ALTER TABLE ssefy_streams DROP COLUMN deleted_at;

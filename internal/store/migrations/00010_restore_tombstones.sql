-- +goose Up
ALTER TABLE ssefy_streams ADD COLUMN deleted_at timestamptz;

-- +goose Down
ALTER TABLE ssefy_streams DROP COLUMN deleted_at;

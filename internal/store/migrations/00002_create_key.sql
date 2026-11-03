-- +goose Up
ALTER TABLE ssefy_streams ADD COLUMN create_key text;
CREATE UNIQUE INDEX ssefy_streams_create_key ON ssefy_streams (create_key);

-- +goose Down
DROP INDEX ssefy_streams_create_key;
ALTER TABLE ssefy_streams DROP COLUMN create_key;

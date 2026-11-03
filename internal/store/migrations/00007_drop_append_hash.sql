-- +goose Up
ALTER TABLE ssefy_appends DROP COLUMN hash;

-- +goose Down
ALTER TABLE ssefy_appends ADD COLUMN hash text NOT NULL DEFAULT '';

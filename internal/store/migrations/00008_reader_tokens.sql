-- +goose Up
ALTER TABLE ssefy_streams ADD COLUMN token text;
UPDATE ssefy_streams SET token = replace(gen_random_uuid()::text, '-', '');

-- +goose Down
ALTER TABLE ssefy_streams DROP COLUMN token;

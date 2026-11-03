-- +goose Up
ALTER TABLE ssefy_streams RENAME COLUMN token TO token_hash;
UPDATE ssefy_streams SET token_hash = encode(sha256(convert_to(token_hash, 'utf8')), 'hex');

-- +goose Down
UPDATE ssefy_streams SET token_hash = 'irrecoverable';
ALTER TABLE ssefy_streams RENAME COLUMN token_hash TO token;

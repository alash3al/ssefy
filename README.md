# ssefy

One Go binary plus PostgreSQL. Backends append events to streams by ID; browsers read them over plain SSE and resume after any disconnect with `Last-Event-ID`.

## Run

```sh
SSEFY_SERVER_SECRET=change-me \
SSEFY_STORE='postgres://user:pass@host/db' \
./ssefy
```

`SSEFY_STORE=memory://` (the default) runs without PostgreSQL, for development only. Migrations run at startup.

## API

| Call | Auth | Does |
| --- | --- | --- |
| `POST /v1/streams` | Server secret | Creates a stream. `201 {"id":"..."}` |
| `POST /v1/streams/{id}` | Server secret | Appends one event or an array. `{"seqs":[...]}`, `404` if the stream is gone |
| `GET /v1/streams/{id}` | Open | SSE: replays, then streams live |
| `DELETE /v1/streams/{id}` | Server secret | Deletes the stream and its events. `204` |

Stream IDs are 128 random bits. They are the only thing protecting reads, so treat an ID as a bearer token and keep it out of logs.

## Events

```json
{"event": "message", "data": "hello", "transient": false, "collapse_key": null}
```

- An array is stored in order, all or nothing.
- `event` defaults to `message`.
- `data` is a string. Encode JSON inside it when you need structure.
- `transient` events are delivered live only, never stored, and carry no `id:`. A push is either all stored or all transient.
- `collapse_key` replaces the stored event with the same key. Live readers still see every push.
- `Idempotency-Key` makes a retried push return the original seqs and deliver once. The same key with a different body is `422`.

## Resuming

- No cursor: start at now.
- `?after=0`: replay everything.
- `?after=now`: skip history.
- `Last-Event-ID` wins over `?after`.
- Deleted stream: open connections close, the next connect gets `404` (EventSource stops reconnecting on a non-200).

## Configuration

| Setting | Default |
| --- | --- |
| `SSEFY_LISTEN` | `:8080` |
| `SSEFY_STORE` | `memory://` |
| `SSEFY_SERVER_SECRET` | required unless `--dev` |
| `SSEFY_CORS_ORIGINS` | `*` with `--dev` |
| `SSEFY_TLS_CERT` / `SSEFY_TLS_KEY` | off |

Each flag has the same name in lowercase (`--listen`, `--store`, `--secret`, `--cors`, `--dev`, `--tls-cert`, `--tls-key`).

Serve over HTTP/2 (TLS, or h2c on a plain listener) so browsers do not hit their six-connections-per-origin limit on HTTP/1.1.

## Tests

```sh
go test ./...
SSEFY_TEST_DB='postgres://postgres:pass@127.0.0.1:5432/ssefy?sslmode=disable' go test ./internal/store
```

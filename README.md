# ssefy

One Go binary plus PostgreSQL. Backends append events to streams by ID; browsers read them over plain SSE and resume after any disconnect with `Last-Event-ID`.

## Run

```sh
docker build -t ssefy .
docker run --rm -p 8080:8080 \
  -e SSEFY_SERVER_SECRET=change-me \
  -e SSEFY_STORE='postgres://user:pass@host/db' \
  ssefy
```

Or from source:

```sh
cp .env.example .env   # edit it
./ssefy
```

`SSEFY_STORE=memory://` (the default) runs without PostgreSQL, for development only. Migrations run at startup.

TLS is not handled here: terminate it at the proxy and point it at this port.

## API

| Call | Auth | Does |
| --- | --- | --- |
| `POST /v1/streams` | Server secret | Creates a stream. `201 {"id":"..."}`. `Idempotency-Key` makes a retry return the same id |
| `POST /v1/streams/{id}` | Server secret | Appends one event or an array. `{"seqs":[...]}`, `404` unknown, `410` deleted |
| `GET /v1/streams/{id}` | Open | SSE: replays, then streams live. `404` unknown, `410` deleted |
| `DELETE /v1/streams/{id}` | Server secret | Deletes the stream and its events. `204` |
| `GET /healthz` | Open | `200` when the store answers and notifications flow; `503` otherwise |
| `GET /metrics` | Open | Prometheus text for this node |

Stream IDs are 128 random bits. They are the only thing protecting reads, so treat an ID as a bearer token and keep it out of logs.

Deletion removes the events, append records and create keys; the id stays as a tombstone so later reads answer `410 Gone`. An id that never existed is `404`.

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

Limits: 1 MiB request body, 500 events per push, 64 KiB of `data` per event, 255-byte keys. A transient event must also fit one NOTIFY payload: about 7.8 KiB after JSON encoding. Push idempotency keys are per stream; create keys are global, so producers sharing one server secret must namespace them (for example `billing:invoice-42`).

## Resuming

- No cursor: start at now.
- `?after=0`: replay everything.
- `?after=now`: skip history.
- `Last-Event-ID` wins over `?after`.
- Deleted stream: open connections close, the next connect gets `410`; an unknown id gets `404`. EventSource stops reconnecting on either.

## Configuration

Settings come from the environment. `./.env` is loaded first when it exists, then real environment variables win. Use `--env-file path` to load a different file; a missing explicit file is an error.

| Setting | Default |
| --- | --- |
| `SSEFY_LISTEN` | `:8080` |
| `SSEFY_STORE` | `memory://` |
| `SSEFY_SERVER_SECRET` | required unless `SSEFY_DEV=true` |
| `SSEFY_CORS_ORIGINS` | `*` with `SSEFY_DEV=true`, comma separated |
| `SSEFY_DEV` | `false` |

## Metrics

A node keeps answering `200` for 30 seconds after losing its LISTEN connection, so a reconnect blip does not pull it from the load balancer. After that it answers `503`: reads would still replay, but live events would never arrive.

`GET /metrics` exposes the default Prometheus registry. It is open like reads; gate it at the proxy if the counters are sensitive.

- `ssefy_readers` — open SSE connections on this node.
- `ssefy_events_appended_total{kind}` and `ssefy_events_delivered_total{kind}` — `stored` or `transient`.
- `ssefy_event_delivery_lag_seconds` — commit to live delivery, after the initial replay.
- `ssefy_http_requests_total{method,route,status}` and `ssefy_http_duration_seconds` — an SSE request's duration is the life of the connection.
- `ssefy_notifications_total{kind}`, `ssefy_listener_up`, `ssefy_listener_reconnects_total`.
- `ssefy_slow_readers_total`, `ssefy_transient_dropped_total`.

## Behind a proxy

Terminate TLS at the proxy. ssefy speaks HTTP/1.1, which every proxy handles; browsers still get HTTP/2 from the proxy, and that is what lifts their six-connections-per-origin limit on SSE.

SSE needs buffering off and a read timeout longer than the 15s heartbeat:

```nginx
location /v1/ {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_buffering off;
    proxy_read_timeout 1h;
}
```

ssefy sends `X-Accel-Buffering: no` and a `: ping` comment every 15s, so any proxy idle timeout above that works.

## Tests

```sh
go test ./...
SSEFY_TEST_DB='postgres://postgres:pass@127.0.0.1:5432/ssefy?sslmode=disable' go test ./internal/store
```

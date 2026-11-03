# ssefy

Push events from your backend, stream them to browsers over plain SSE, resume exactly where each client left off. One Go binary, PostgreSQL, nothing else to run.

## The model

- A **stream** is an ordered, per-stream event log. Writes append; reads are SSE.
- The **id** is a public, time-sortable UUIDv7 — only an address.
- The **token** is a 128-bit secret that opens reads for one stream. Browsers pass it as `?token=` (EventSource cannot set headers); any other client may send `Authorization: Bearer <token>` instead.
- The **server secret** guards writes and admin calls, and also opens reads — SSE and history. It travels only in the `Authorization` header, never `?token=`, where it would land in proxy logs; a host that reads server-side never needs to keep the stream token. Your backend holds it; browsers never see it.
- Every stored event gets a per-stream **seq**, which is the SSE `id:`. A reconnect with `Last-Event-ID` gets exactly what it missed, nothing twice.
- Nodes hold only open connections: run any number of them behind a plain load balancer.

## Try it

```sh
docker run --rm -p 8080:8080 \
  -e SSEFY_SERVER_SECRET=dev-secret \
  -e SSEFY_STORE=memory:// \
  ghcr.io/alash3al/ssefy:latest
```

The image is multi-arch (amd64, arm64) and every published release attaches
binaries for linux and darwin with their `SHA256SUMS`; building from source
is just `go build` or `docker build .`.

`memory://` keeps everything in the process — fine for trying it, not for real use. Point `SSEFY_STORE` at PostgreSQL for that; the schema migrates itself at startup and ships inside the binary.

```sh
# create a stream
curl -s -X POST -H 'Authorization: Bearer dev-secret' localhost:8080/v1/streams
# {"id":"0190a2b4-…","token":"f0af9d2c…"}

# append
curl -s -X POST -H 'Authorization: Bearer dev-secret' \
  -d '{"data":"hello"}' localhost:8080/v1/streams/$ID
# {"seqs":[1]}

# read it back
curl -N 'localhost:8080/v1/streams/$ID?token=$TOKEN&after=0'
# id: 1
# event: message
# data: hello
```

In a browser: `new EventSource("/v1/streams/" + id + "?token=" + token)`.

## API

| Call | Auth | Does |
| --- | --- | --- |
| `POST /v1/streams` | Server secret | Creates a stream. `201 {"id","token"}` |
| `POST /v1/streams/{id}` | Server secret | Appends one event or an array. `{"seqs":[…]}`, `404` unknown or deleted |
| `POST /v1/push` | Server secret | Appends the same events to up to 100 streams in one call |
| `GET /v1/streams/{id}` | Stream token or server secret | SSE: replays, then streams live. `401` no or bad token, `404` unknown or deleted |
| `GET /v1/streams/{id}/events` | Stream token or server secret | History: one page of stored events, always oldest→newest |
| `DELETE /v1/streams/{id}/events` | Server secret | Trim: removes events on one side of an exclusive bound. `200 {"events":n}` |
| `DELETE /v1/streams/{id}` | Server secret | Removes the stream and its events. `204`, also for an unknown id |
| `GET /v1/streams/{id}/stat` | Server secret | `{"last_seq","created_at","readable"}` — `readable` is `false` while revoked |
| `POST /v1/streams/{id}/rotate` | Server secret | Mints a new token, closes readers. `200 {"token"}` |
| `POST /v1/streams/{id}/revoke` | Server secret | Clears the token, closes readers. `204` |
| `GET /healthz` | Open | `200` when the store answers and notifications flow |
| `GET /metrics` | Open | Prometheus text for this node |

**Batch push** takes `{"streams":[…],"events":…}` and answers per stream:

```json
{"results": {"<id>": {"seqs": [1,2]}, "<id>": {"error": "stream not found"}}}
```

Targets are independent — one failure undoes nothing — so read the results. A retry with the same `Idempotency-Key` returns the original seqs for streams that already succeeded and appends to the rest.

## Events

```json
{"event": "message", "data": "hello", "transient": false, "collapse_key": null}
```

- `event` defaults to `message`; `data` is a string — encode JSON inside it when you need structure.
- An array is stored in order, all or nothing.
- `transient` events are delivered live only, never stored, and carry no `id:`. A push is either all stored or all transient.
- `collapse_key` replaces the stored event with the same key. Live readers still see every push.
- `Idempotency-Key` (per stream) makes a retried push return the original seqs and deliver once; a retry's body is ignored.

Limits: 1 MiB request body · 500 events per push · 64 KiB of `data` per event · 255-byte keys · a transient event must fit one NOTIFY payload, about 7.8 KiB after JSON encoding.

## Resuming

- No cursor: start at now.
- `?after=0`: replay everything.
- `?after=now`: skip history.
- `Last-Event-ID` wins over `?after`.
- A missing or wrong token is `401`; an unknown or deleted stream is `404`. EventSource stops reconnecting on any non-200.

## History

`GET /v1/streams/{id}/events` pages through stored events with the same credentials as SSE reads: the stream token or the server secret. A stream can be listed with nobody listening — SSE and history are two readers over one log.

- `direction` — `forward` or `backward`, default `backward`: the latest page first, walking toward older events.
- `cursor` — a seq, exclusive. Absent starts at the beginning of the walk: the first event forward, the newest backward. Same number space as the SSE `id:`, so `Last-Event-ID` seeds it.
- `limit` — default 50, max 200.
- Every page is oldest→newest, in both directions. `has_more` says whether the walk continues; the next cursor is the edge seq — `last.seq` forward, `first.seq` backward.

**Trim** removes stored events on one side of an exclusive bound: `?before=N` drops `seq < N` (the oldest side) and `?after=N` drops `seq > N` (the newest; `after=0` clears the whole history but keeps the stream and its token). Exactly one of the two is required. `last_seq` is preserved — seqs are never reused — and the holes a trim leaves are harmless because every cursor in ssefy is an exclusive bound. Trim sends no close notice: live readers are unaffected, and history simply stops listing what was removed. An idempotent retry of an earlier push still answers with the seqs it originally returned, even for events a trim has since removed.

A chat opens with `?limit=50`, then pages `?cursor=<oldest seq>` as the user scrolls up. Forward `has_more:false` means caught up **as of now** — newer events may still arrive, which is the SSE connection's job (or re-poll the same cursor). Backward `has_more:false` is final. `collapse_key` can drop an event from a later re-fetch, and transient events never appear: they were never stored.

## Tokens

- Stored only as a SHA-256 digest. Create and rotate return the plaintext exactly once — keep it.
- In the URL it lands in proxy access logs. Strip query strings at the proxy, route browsers through your own gateway, or accept it.
- Rotate, revoke and delete close every reader on every node. EventSource cannot change its URL, so after a rotate the host must reopen the connection with the new token; the old token reconnects to `401` — the stream exists, the token no longer opens it — and a deleted stream answers `404`. EventSource stops reconnecting on either.
- No expiry by design: revoke is the off switch, rotate is the re-issue.
- The server secret reads any stream, including a revoked one — revoke kills the token, not the host — and keeps working across a rotate. Stat, rotate and revoke stay secret-only: they are host tools, not reader endpoints.

## Configuration

Settings come from the environment. `./.env` is read first when it exists; real environment variables win. `--env-file path` loads a different file, and a missing explicit file is an error.

| Setting | Default |
| --- | --- |
| `SSEFY_LISTEN` | `:8080` |
| `SSEFY_STORE` | `memory://` |
| `SSEFY_SERVER_SECRET` | required unless `SSEFY_DEV=true` |
| `SSEFY_CORS_ORIGINS` | `*` with `SSEFY_DEV=true`, comma separated |
| `SSEFY_DEV` | `false` |

## Operating

- `/healthz` answers `503` once a node has been without its LISTEN connection for 30 seconds: reads would still replay, but live events would never arrive. The 30s grace keeps a one-second blip from pulling the node off the balancer.
- `/metrics` is open like `/healthz` — reads themselves need a credential — so gate both at the proxy if that matters. Exported: `ssefy_readers`, `ssefy_events_appended_total{kind}`, `ssefy_events_delivered_total{kind}`, `ssefy_event_delivery_lag_seconds`, `ssefy_http_requests_total{method,route,status}`, `ssefy_http_duration_seconds`, `ssefy_notifications_total{kind}`, `ssefy_listener_up`, `ssefy_listener_reconnects_total`, `ssefy_slow_readers_total`, `ssefy_transient_dropped_total`, plus the Go and process collectors.
- ssefy speaks HTTP/1.1. Terminate TLS at the proxy; browsers get HTTP/2 there, which is what lifts their six-connections-per-origin limit on SSE.
- SSE needs buffering off and a read timeout above the 15s heartbeat:

```nginx
location /v1/ {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_buffering off;
    proxy_read_timeout 1h;
}
```

ssefy already sends `X-Accel-Buffering: no` and a `: ping` comment every 15 seconds.

## Tests

```sh
go test ./...
SSEFY_TEST_DB='postgres://postgres:pass@127.0.0.1:5432/ssefy?sslmode=disable' \
  go test ./internal/store ./internal/api
SSEFY_TEST_DB='postgres://postgres:pass@127.0.0.1:5432/ssefy?sslmode=disable' \
  go test ./internal/integration -timeout 5m
```

The integration suite builds the binary and runs two nodes over one shared
PostgreSQL, then breaks things on purpose: reconnects and replay-to-live
transitions, ambiguous and simultaneous idempotent retries, partial batch-push
failures, SIGKILLed processes, a terminated LISTEN connection (readers must
catch up on everything stored, and a transient published while the node is
deaf is lost), credential isolation across nodes, pagination under concurrent
writes, trim under live readers, and a stalled reader that must be dropped
rather than allowed to block publishers. It takes about a minute.

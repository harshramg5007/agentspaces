# Configuration

The supported launch path uses the Docker Compose stack in the repo root and defaults to the Postgres backend.

## Local Defaults

- HTTP API: `http://localhost:8080/api/v1`
- Store backend: `postgres`
- Local startup: `make up`

## Common Runtime Controls

- `STORE_TYPE`: `postgres`, `sqlite-file`, `sqlite-memory`, or `valkey`
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DATABASE`
- `POSTGRES_QUEUE_CLAIM_MODE`: `strict` or `parallel`
- `SQLITE_PATH` for local-only SQLite runs
- `VALKEY_ADDR` for single-node Valkey
- `VALKEY_SHARDS` for comma-separated Valkey shard addresses
- `VALKEY_PUBLISH_PUBSUB`, `VALKEY_LEASE_REAP_INTERVAL`, `VALKEY_LEASE_REAP_BATCH`
- `HTTP_ADDR`
- `LOG_LEVEL`

## Backend Guidance

- `postgres`: supported production and pilot backend
- `sqlite-file` / `sqlite-memory`: local-only modes, not production supported
- `valkey`: experimental runtime backend with single-node and sharded modes

## Optional Local Valkey Path

The default `make up` workflow stays Postgres-first. If you want a local Valkey node for backend evaluation, start the optional profile and run the server with `STORE_TYPE=valkey`:

```bash
docker compose --profile valkey up -d valkey
go run ./cmd/server --store-type valkey --valkey-addr 127.0.0.1:6379
```

For multi-shard evaluation, use `--valkey-shards host1:6379,host2:6379,...` instead of `--valkey-addr`.

## Production-Facing Server Flags

- `--runtime-profile production`
- `--auth-mode required`
- `--rate-limit-requests`
- `--rate-limit-window`
- `--telemetry-mode disabled|optional|required`
- `--cors-allow-origins`

See [`cmd/server`](../cmd/server) for the runtime entrypoint and [operations.md](operations.md) for deployment guidance.

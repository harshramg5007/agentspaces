# Configuration

The supported launch path uses the Docker Compose stack in the repo root and defaults to the Postgres backend.

## Local Defaults

- HTTP API: `http://localhost:8080/api/v1`
- Store backend: `postgres`
- Local startup: `make up`

## Common Runtime Controls

- `STORE_TYPE`: `postgres`, `sqlite-file`, or `sqlite-memory`
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DATABASE`
- `POSTGRES_QUEUE_CLAIM_MODE`: `strict` or `parallel`
- `SQLITE_PATH` for local-only SQLite runs
- `HTTP_ADDR`
- `LOG_LEVEL`

## Backend Guidance

- `postgres`: supported production and pilot backend
- `sqlite-file` / `sqlite-memory`: local-only modes, not production supported

## Production-Facing Server Flags

- `--runtime-profile production`
- `--auth-mode required`
- `--rate-limit-requests`
- `--rate-limit-window`
- `--telemetry-mode disabled|optional|required`
- `--cors-allow-origins`

See [`cmd/server`](../cmd/server) for the runtime entrypoint and [operations.md](operations.md) for deployment guidance.

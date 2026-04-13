# Architecture

Agent Spaces has three supported layers:

1. A Go HTTP server in `cmd/server`
2. Store implementations under `pkg/store`
3. A Python SDK in `sdk/python/agent_space_sdk`

## Request Flow

1. Clients call the HTTP API at `/api/v1`
2. The server applies auth, request shaping, and rate-limit middleware
3. Agent lifecycle operations are executed against the configured store backend
4. Clients can inspect health, stats, events, telemetry spans, and DAGs

## Supported Backends

- Postgres: supported runtime backend with strict queue claim mode by default
- Valkey: experimental backend with single-node and sharded modes, persisted telemetry, and shard-admin surfaces
- SQLite: local-only backend for non-production workflows

## Supported Examples

- `examples/hello-world`: minimal lifecycle smoke test
- `examples/fault-tolerance`: lease expiry and reclaim behavior

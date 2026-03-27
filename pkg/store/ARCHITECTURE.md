# Store Architecture

This package keeps the public `agent.AgentSpace` contract unchanged while organizing backend implementations by concern.

## Support Policy

GA backends:

- `postgres`
- `sqlite-file`
- `sqlite-memory`

Anything outside this matrix is out of GA scope.

## Backends

Supported backends:

- `postgres`
- `sqlite-file`
- `sqlite-memory`

Each backend now lives in its own folder/package with concern-scoped files:

- `pkg/store/postgres/*`
- `pkg/store/sqlite/*`

Root `pkg/store` keeps thin wrappers (`*_wrapper.go`) so external callers can continue using `store.NewStore`, `store.NewPostgresStore`, and `store.NewSQLiteStore`.

## Capability Matrix

| Capability | Postgres | SQLite |
|---|---|---|
| `Health(ctx)` | Yes | Yes |
| `Count(query)` | Yes | Yes |
| `CountByStatus(query)` | Yes | No (handler fallback) |
| Telemetry persistence (`RecordSpans`/`ListSpans`) | Yes | No |
| `GetDAG(rootID)` | Yes | Yes |

Runtime policy:

- `telemetry-mode=required` requires a telemetry-capable backend at startup.
- `telemetry-mode=disabled` blocks telemetry HTTP endpoints even if backend supports persistence.
- Postgres telemetry retention is managed via periodic cleanup (`CleanupSpansBefore`).

## Internal Shared Layers

Shared internal helpers live under `pkg/store/internal`:

- `internal/common`
  - `SubscriptionRegistry` (shared in-process event subscription matching)
  - update/metadata helpers (`ApplyUpdates`, completion metadata helpers)
  - shared metadata constants (`queue`, completion-token keys)
- `internal/core`
  - `Engine` contract wrapper and optional capability discovery (`StatusCounter`, telemetry, debug invariants)
  - explicit unsupported capability error (`ErrCapabilityUnsupported`)

## File Decomposition

### Postgres (`pkg/store/postgres`)

- `postgres_engine.go`
- `postgres_query.go`
- `postgres_lifecycle.go`
- `postgres_events.go`
- `postgres_telemetry.go`
- `postgres_schema.go`
- `postgres_sql_helpers.go`
- `postgres_reaper.go`

### SQLite (`pkg/store/sqlite`)

- `sqlite_engine.go`
- `sqlite_query.go`
- `sqlite_lifecycle.go`
- `sqlite_events.go`
- `sqlite_schema.go`
- `sqlite_sql_helpers.go`
- `sqlite_reaper.go`

## Compatibility Notes

- Public constructors and `store.NewStore` behavior remain unchanged.
- Backend capability differences are preserved.
- File decomposition is behavior-preserving and focused on maintainability.

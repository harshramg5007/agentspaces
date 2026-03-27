# Agent Spaces

Queue-partitioned Postgres coordination runtime for multi-agent workflows.

Agent Spaces lets producers post work, workers claim it with leases, and clients inspect agent state and traces through an HTTP API and Python SDK. The supported production path is Postgres-first; SQLite remains local-only and non-production.

Agent Spaces is source-available under the Business Source License 1.1 (`BUSL-1.1`). Evaluation, CI, and other non-production use are allowed under BUSL. Production use requires a commercial license. See [LICENSE](LICENSE) and [COMMERCIAL-LICENSING.md](COMMERCIAL-LICENSING.md).

[Quickstart](docs/getting-started.md) | [Python SDK](docs/python-sdk.md) | [Examples](examples/README.md) | [Architecture](docs/architecture.md) | [Operations](docs/operations.md)

## Run It Locally In 3 Commands

```bash
make doctor
make up
make smoke
```

## What This Proves

- The local server starts and answers health checks.
- The Python SDK connects to the HTTP API.
- Agent create, claim, and complete works end to end.

Run `make down` when you are finished, or continue with `make example-fault-tolerance`.

## Supported Surface

| Area | Status | Notes |
| --- | --- | --- |
| Go server (`cmd/server`) | supported | Core runtime and lifecycle API |
| Lifecycle HTTP API | supported | `create`, `query`, `read`, `take`, `in`, `complete`, `release`, `renew`, `complete-and-out` |
| Python SDK (`sdk/python/agent_space_sdk`) | supported | Manual SDK, sync + async clients |
| Postgres runtime | supported | Queue-partitioned, strict claim mode by default |
| Local Docker path | supported | `make up`, `make smoke`, `make down` on Postgres |
| SQLite runtime | experimental | Local-only; not recommended for pilot or production deployments |
| Hello World example | supported | [`examples/hello-world`](examples/hello-world/README.md) |
| Fault tolerance example | supported | [`examples/fault-tolerance`](examples/fault-tolerance/README.md) |

## Next Example

- [`examples/hello-world`](examples/hello-world/README.md): the canonical smoke test and first-run path.
- [`examples/fault-tolerance`](examples/fault-tolerance/README.md): lease expiry, reclaim, and worker recovery.

## Docs

- [Getting Started](docs/getting-started.md)
- [Python SDK](docs/python-sdk.md)
- [Configuration](docs/configuration.md)
- [Release and Verification](docs/release-and-verification.md)

## Architecture

The runtime is a small HTTP server over a Postgres-backed queue service, with a Python SDK and examples built on top. See [docs/architecture.md](docs/architecture.md) for the supported component model and deployment shape.

## Operations

Operational guidance for local Docker usage, auth, rate limiting, and deployment boundaries lives in [docs/operations.md](docs/operations.md).

## License

Agent Spaces is source-available under `BUSL-1.1`, with `Additional Use Grant: None` and `Change License: MPL-2.0`. Commercial licensing: `jase@urobora.ai`.

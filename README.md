# Agent Spaces

[![CI](https://github.com/urobora-ai/agentspaces/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/urobora-ai/agentspaces/actions/workflows/ci.yml)

Agent Spaces gives agent systems a shared coordination runtime for posting work, claiming it with leases, routing by capability, and recovering cleanly from worker crashes.
Use it when queues are too narrow and workflow engines are too rigid: you need shared queryable state, atomic claims, and traceable multi-agent handoffs in one system.

Agent Spaces is source-available under the Business Source License 1.1 (`BUSL-1.1`). Evaluation, CI, and other non-production use are allowed under BUSL. Production use requires a commercial license. See [LICENSE](LICENSE) and [COMMERCIAL-LICENSING.md](COMMERCIAL-LICENSING.md).

[Quickstart](docs/getting-started.md) | [Concepts](docs/concepts.md) | [Python SDK](docs/python-sdk.md) | [TypeScript SDK](sdk/typescript/README.md) | [Examples](examples/README.md) | [Architecture](docs/architecture.md) | [Observability](docs/observability.md) | [Discussions](https://github.com/urobora-ai/agentspaces/discussions)

## How It Works

```text
┌──────────────┐    out() / create     ┌──────────────────────┐    take() / in()     ┌──────────────┐
│ Producer or  │ --------------------> │ Agent Space          │ --------------------> │ Worker or    │
│ Router       │                       │ Postgres runtime     │                       │ Tool Server  │
└──────┬───────┘ <-------------------- │ query/read/events    │ <-------------------- └──────┬───────┘
       │         results / traces      └──────────┬───────────┘       complete()/out()       │
       └──────────────────────────────────────────>│                                         │
                                                   ▼                                         │
                                            ┌──────────────┐ <────────────────────────────────┘
                                            │ Result or    │
                                            │ next work    │
                                            └──────────────┘
```

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

## Why Agent Spaces?

- `Celery`: great for async tasks and retries, but task state is split across broker/result backend semantics. Agent Spaces adds shared queryable coordination state plus explicit lease ownership and reclaim.
- `Temporal`: great for durable workflow execution, but it assumes you want workflow code and history as the primary control plane. Agent Spaces gives you a shared coordination space for ad hoc routing, claiming, and handoff across many agents.
- `Bare Redis / Redis Streams`: great for fast transport and consumer groups, but they do not give you one lifecycle surface for `read`, `take`, `in`, `complete`, traces, and queryable metadata. Agent Spaces packages those coordination primitives behind one API.
- `Kafka consumer groups`: great for partitioned log processing, but they are built around partition ownership rather than stateful request routing and blocking match queries. Agent Spaces is designed for capability-driven claims and multi-agent handoffs.

## Use Cases

- Background agent task processing
- Parallel inference pipelines
- Multi-agent tool coordination
- Fault-tolerant job distribution
- Stateful request routing

## Quick Comparison

| Capability | Agent Spaces | Celery | Temporal | Redis Streams |
| --- | --- | --- | --- | --- |
| Atomic claim | Yes | Limited | Yes | Yes |
| Crash recovery | Yes | Yes | Yes | Yes |
| Execution traces | Yes | Limited | Yes | No |
| No broker dependency | Yes | No | No | No |
| Shared queryable coordination state | Yes | Limited | Limited | No |


## Supported Surface

| Area | Status | Notes |
| --- | --- | --- |
| Go server (`cmd/server`) | supported | Core runtime and lifecycle API |
| Lifecycle HTTP API | supported | `create`, `query`, `read`, `take`, `in`, `complete`, `release`, `renew`, `complete-and-out` |
| Python SDK (`sdk/python/agent_space_sdk`) | supported | `pip install agent-space-sdk`, sync + async clients |
| TypeScript SDK (`sdk/typescript`) | preview | Generated from OpenAPI with a thin fetch wrapper |
| Postgres runtime | supported | Queue-partitioned, strict claim mode by default |
| Local Docker path | supported | `make up`, `make smoke`, `make down` on Postgres |
| SQLite runtime | experimental | Local-only; not recommended for pilot or production deployments |
| Hello World example | supported | [`examples/hello-world`](examples/hello-world/README.md) |
| Fault tolerance example | supported | [`examples/fault-tolerance`](examples/fault-tolerance/README.md) |
| Queue fanout example | supported | [`examples/queue-fanout`](examples/queue-fanout/README.md) |
| MCP mesh example | supported | [`examples/mcp-mesh`](examples/mcp-mesh/README.md) |
| Inference routing example | supported | [`examples/inference-routing`](examples/inference-routing/README.md) |

## Next Example

- [`examples/hello-world`](examples/hello-world/README.md): the canonical smoke test and first-run path.
- [`examples/fault-tolerance`](examples/fault-tolerance/README.md): lease expiry, reclaim, and worker recovery.
- [`examples/queue-fanout`](examples/queue-fanout/README.md): 10 workers on one queue, one crash, no double processing.
- [`examples/mcp-mesh`](examples/mcp-mesh/README.md): capability heartbeats for MCP worker discovery and routing.
- [`examples/inference-routing`](examples/inference-routing/README.md): model capability heartbeats and capacity-aware request routing.

## Docs

- [Getting Started](docs/getting-started.md)
- [Concepts](docs/concepts.md)
- [Python SDK](docs/python-sdk.md)
- [Configuration](docs/configuration.md)
- [Benchmarks](docs/benchmarks.md)
- [Observability](docs/observability.md)
- [Agent Spaces vs etcd](docs/agentspaces-vs-etcd.md)
- [Launch Blog Post](docs/launch-blog-post.md)
- [Formal Results Blog Series](docs/proofs-blog-series.md)
- [Release and Verification](docs/release-and-verification.md)

## Architecture

The runtime is a small HTTP server over a Postgres-backed coordination service, with Python and TypeScript SDKs and runnable examples built on top. See [docs/architecture.md](docs/architecture.md) for the supported component model and deployment shape.

## Operations

Operational guidance for local Docker usage, auth, rate limiting, and deployment boundaries lives in [docs/operations.md](docs/operations.md).

## Community

Use [GitHub Discussions](https://github.com/urobora-ai/agentspaces/discussions) for evaluation questions, launch feedback, and integration notes.

## License

Agent Spaces is source-available under `BUSL-1.1`, with `Additional Use Grant: None` and `Change License: MPL-2.0` on `2030-03-27`. Commercial licensing: `jase@urobora.ai`.

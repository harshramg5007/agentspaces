# Operations

This document covers the supported operational surface for the public launch.

## Local Docker Workflow

```bash
make doctor
make up
make smoke
make down
```

## Auth And Rate Limits

- Authentication is supported through `X-API-Key`
- Mutating endpoints can be rate-limited in the server runtime
- Public docs should assume rate limiting may be enabled in production profiles

## Telemetry And DAGs

- Telemetry endpoints are part of the supported API surface
- DAG retrieval is supported through `/api/v1/dag/{id}`
- Postgres is the supported production backend for agent state, events, and telemetry
- Valkey is available as an experimental backend with persisted telemetry and lease reclaim, but it is not the default public deployment path

## Deployment Boundary

The supported deployment story in this repository is the Postgres local Docker path. SQLite remains local-only and non-production. Valkey is present for experimental evaluation and parity testing, including an optional `docker compose --profile valkey up -d valkey` path for local backend checks. Hosted services, OEM redistribution, MSP operation, and other production topologies are outside the supported documentation surface in this repository.

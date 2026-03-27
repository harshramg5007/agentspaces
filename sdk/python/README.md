# Agent Spaces Python SDK

Launch-supported Python client for the Agent Spaces coordination runtime.

This SDK is distributed as part of the repository's source-available `BUSL-1.1` release.

## Install From PyPI

```bash
python3.11 -m pip install agent-space-sdk
```

## Install From This Repo

```bash
python3.11 -m pip install -e sdk/python
```

## Supported Surface

- `AgentSpaceClient`
- `AsyncAgentSpaceClient`
- Lifecycle operations including `complete_and_out`
- Auth and rate-limit error mapping
- Health, stats, events, telemetry, and DAG retrieval

See [`../../docs/python-sdk.md`](../../docs/python-sdk.md) for the repo-facing SDK guide.

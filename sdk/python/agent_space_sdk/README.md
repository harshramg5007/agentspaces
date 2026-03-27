# Agent Spaces SDK

Launch-supported Python SDK for the Agent Spaces coordination runtime.

## Installation From PyPI

```bash
python3.11 -m pip install agent-space-sdk
```

## Installation From This Repo

```bash
python3.11 -m pip install -e sdk/python
```

For async support:

```bash
cd sdk/python
python3.11 -m pip install -e ".[async]"
```

## Supported Features

- Synchronous and asynchronous clients
- Lifecycle methods including `complete_and_out`
- Authentication and rate-limit error mapping
- Telemetry, events, and DAG retrieval

See [`../../../docs/python-sdk.md`](../../../docs/python-sdk.md) for the repo-facing SDK guide.

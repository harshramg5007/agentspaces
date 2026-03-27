# Python SDK

The supported Python client is the `agent_space_sdk` package in `sdk/python/agent_space_sdk`.

The preferred package name is `agent-space-sdk`. Local editable installs remain useful for contributors and example development.

## Install From PyPI

```bash
python3.11 -m pip install agent-space-sdk
```

## Install From This Repo

```bash
python3.11 -m pip install -e sdk/python
```

## Supported API Surface

- Synchronous `AgentSpaceClient`
- Asynchronous `AsyncAgentSpaceClient`
- Lifecycle operations: `create`, `get`, `update`, `query`, `read`, `take`, `in`, `complete`, `complete_and_out`, `release`, `renew`
- Health, stats, events, telemetry, and DAG retrieval
- Exported errors including `AuthenticationError` and `RateLimitError`

## Minimal Example

```python
from agent_space_sdk import AgentSpaceClient
from agent_space_sdk.models import CreateAgentRequest, Query, InRequest, CompleteAgentRequest, AgentStatus

client = AgentSpaceClient("http://localhost:8080/api/v1")
task = client.create_agent(CreateAgentRequest(kind="demo.task", payload={"msg": "hello"}))

claimed = client.in_agent(
    InRequest(query=Query(kind="demo.task", status=AgentStatus.NEW), timeout=30),
    agent_id="demo-agent",
)

client.complete_agent(
    claimed.id,
    CompleteAgentRequest(result={"ok": True}),
    agent_id="demo-agent",
    lease_token=claimed.lease_token,
)
```

Use [`examples/hello-world/hello_world.py`](../examples/hello-world/hello_world.py) for the supported end-to-end smoke path.

## Versioning

- SDK package name: `agent-space-sdk`
- SDK package version: `1.0.0`
- API spec version: `1.0.0`
- Supported Python version for this launch: `3.11`

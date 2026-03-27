"""Supported hello-world smoke example for Agent Spaces."""

from __future__ import annotations

import os
from uuid import uuid4

from agent_space_sdk import AgentSpaceClient
from agent_space_sdk.models import CompleteAgentRequest, CreateAgentRequest, InRequest, Query, AgentStatus


def main() -> None:
    base_url = os.environ.get("AGENT_SPACE_URL", "http://localhost:8080/api/v1")
    client = AgentSpaceClient(base_url)
    try:
        tag = f"hello-{uuid4().hex[:8]}"
        agent_id = f"hello-worker-{tag}"

        health = client.health_check()
        if health.get("status") not in {"ok", "healthy"}:
            raise RuntimeError(f"unexpected health response: {health}")
        print("PASS: local server started")

        created = client.create_agent(
            CreateAgentRequest(
                kind="example.hello",
                payload={"msg": "hello"},
                tags=[tag],
            )
        )
        print("PASS: SDK connected")

        claimed = client.in_agent(
            InRequest(query=Query(kind="example.hello", status=AgentStatus.NEW, tags=[tag]), timeout=30),
            agent_id=agent_id,
        )
        if claimed is None:
            raise RuntimeError("hello-world claim returned no agent")

        client.complete_agent(
            claimed.id,
            CompleteAgentRequest(result={"ok": True}),
            agent_id=agent_id,
            lease_token=claimed.lease_token,
        )
        completed = client.get_agent(created.id)
        if completed.status != AgentStatus.COMPLETED:
            raise RuntimeError(f"expected COMPLETED status, got {completed.status}")

        print("PASS: agent lifecycle verified")
        print("Next: make example-fault-tolerance")
    finally:
        client.close()


if __name__ == "__main__":
    main()

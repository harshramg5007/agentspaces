#!/usr/bin/env python3
"""Bridge Agent Spaces needs to MCP servers with capability heartbeats."""

import argparse
import asyncio
import json
import sys
from datetime import datetime
from uuid import uuid4

sys.path.insert(0, '../../sdk/python')

from fastmcp import Client as MCPClient
from agent_space_sdk import AgentSpaceClient
from agent_space_sdk.models import CreateAgentRequest, TakeRequest, Query, AgentStatus, CompleteAgentRequest


async def heartbeat_loop(client, tool_name, mcp_url, agent_id):
    while True:
        try:
            client.create_agent(CreateAgentRequest(
                kind="capability",
                tags=[f"cap:{tool_name}", "demo:mcp"],
                payload={
                    "mcp_url": mcp_url,
                    "tools": [tool_name],
                },
                ttl=10,
                metadata={
                    "agent_id": agent_id,
                    "updated_at": datetime.utcnow().isoformat(),
                },
            ))
            print(f"[{agent_id}] published capability heartbeat")
        except Exception as exc:
            print(f"[{agent_id}] heartbeat error: {exc}")
        await asyncio.sleep(5)


async def worker_loop(client, tool_name, mcp_url, agent_id):
    async with MCPClient(mcp_url) as mcp:
        tools = await mcp.list_tools()
        print(f"[{agent_id}] MCP tools: {[tool.name for tool in tools]}")

        while True:
            try:
                need = client.take_agent(
                    TakeRequest(
                        query=Query(
                            kind="need",
                            tags=[f"need:{tool_name}", "demo:mcp"],
                            status=AgentStatus.NEW,
                        )
                    ),
                    agent_id=agent_id,
                )
            except Exception as exc:
                print(f"[{agent_id}] take error: {exc}")
                await asyncio.sleep(1)
                continue

            if need is None:
                await asyncio.sleep(0.5)
                continue

            request_id = need.metadata.get("request_id", str(need.id))
            try:
                args = need.payload.get("args", {})
                result = await mcp.call_tool(tool_name, args)
                output = result.content[0].text if result.content else str(result)
                try:
                    output = json.loads(output)
                except (json.JSONDecodeError, TypeError):
                    pass

                client.create_agent(CreateAgentRequest(
                    kind="result",
                    tags=[f"result:{tool_name}", "demo:mcp"],
                    payload={
                        "request_id": request_id,
                        "tool": tool_name,
                        "output": output,
                        "provider": agent_id,
                    },
                    parent_id=need.id,
                ))
                client.complete_agent(
                    need.id,
                    CompleteAgentRequest(result={"status": "done", "provider": agent_id}),
                    agent_id=agent_id,
                    lease_token=need.lease_token,
                )
                print(f"[{agent_id}] completed request {request_id}")
            except Exception as exc:
                print(f"[{agent_id}] execution error: {exc}")
                try:
                    client.release_agent(need.id, agent_id, need.lease_token, reason="worker_error")
                except Exception as release_exc:
                    print(f"[{agent_id}] release error: {release_exc}")


async def main():
    parser = argparse.ArgumentParser(description='Run an MCP mesh worker')
    parser.add_argument('--tool', required=True, help='Tool name')
    parser.add_argument('--mcp-url', required=True, help='MCP server URL')
    parser.add_argument('--ts-url', default='http://localhost:8080/api/v1', help='Agent Spaces URL')
    parser.add_argument('--agent-id', default=None, help='Optional worker id')
    args = parser.parse_args()

    agent_id = args.agent_id or f"worker-{args.tool}-{uuid4().hex[:6]}"
    client = AgentSpaceClient(args.ts_url)

    await asyncio.gather(
        heartbeat_loop(client, args.tool, args.mcp_url, agent_id),
        worker_loop(client, args.tool, args.mcp_url, agent_id),
    )


if __name__ == "__main__":
    asyncio.run(main())

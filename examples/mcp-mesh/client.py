#!/usr/bin/env python3
"""Client for the supported MCP mesh example."""

import argparse
import json
import sys
import time
from datetime import datetime
from uuid import uuid4

sys.path.insert(0, '../../sdk/python')

from agent_space_sdk import AgentSpaceClient
from agent_space_sdk.models import CreateAgentRequest, Query


def discover_capabilities(client, wait_seconds=20, interval=1.0):
    print("=" * 50)
    print("  Discovering Available Capabilities")
    print("=" * 50)

    deadline = time.time() + wait_seconds
    while True:
        caps = client.query_agents(Query(kind="capability", tags=["demo:mcp"]))
        if caps or time.time() >= deadline:
            break
        print("  Waiting for workers...", end="\r")
        time.sleep(interval)

    latest_by_agent = {}
    for cap in caps:
        agent_id = cap.metadata.get("agent_id") or str(cap.id)
        updated_at = cap.metadata.get("updated_at")
        previous = latest_by_agent.get(agent_id)
        if previous is None or (updated_at and updated_at > previous.metadata.get("updated_at", "")):
            latest_by_agent[agent_id] = cap

    if not latest_by_agent:
        print("  No capabilities found. Start the example stack first.")
        return []

    for agent_id, cap in latest_by_agent.items():
        print(f"  {agent_id}:")
        print(f"    Tools: {cap.payload.get('tools', [])}")
        print(f"    MCP URL: {cap.payload.get('mcp_url', 'unknown')}")

    return list(latest_by_agent.values())


def post_need(client, tool_name, args):
    print("\n" + "=" * 50)
    print(f"  Posting Need: {tool_name}")
    print("=" * 50)

    request_id = str(uuid4())
    need = client.create_agent(CreateAgentRequest(
        kind="need",
        tags=[f"need:{tool_name}", "demo:mcp"],
        payload={"tool": tool_name, "args": args},
        metadata={"request_id": request_id},
    ))

    print(f"  Request ID: {request_id}")
    print(f"  Agent ID: {need.id}")
    print(f"  Args: {json.dumps(args)}")
    return request_id


def wait_for_result(client, tool_name, request_id, timeout=30):
    print("\n" + "=" * 50)
    print("  Waiting for Result")
    print("=" * 50)

    deadline = time.time() + timeout
    while time.time() < deadline:
        results = client.query_agents(Query(kind="result", tags=[f"result:{tool_name}", "demo:mcp"]))
        for result in results:
            if result.payload.get("request_id") != request_id:
                continue
            print(f"  Provider: {result.payload.get('provider', 'unknown')}")
            print("  Output:")
            print(json.dumps(result.payload.get("output"), indent=4))
            return True
        time.sleep(0.5)

    print(f"  Timeout after {timeout}s waiting for result")
    return False


def main():
    parser = argparse.ArgumentParser(description='Run the MCP mesh client')
    parser.add_argument('--server', default='http://localhost:8080/api/v1', help='Agent Spaces URL')
    parser.add_argument('--tool', default='web_search', help='Tool to invoke')
    parser.add_argument('--args', default=None, help='Tool arguments as JSON')
    parser.add_argument('--discover-only', action='store_true', help='Only list capabilities')
    parser.add_argument('--cap-timeout', type=int, default=20, help='Wait time for capabilities')
    args = parser.parse_args()

    client = AgentSpaceClient(args.server)
    caps = discover_capabilities(client, wait_seconds=args.cap_timeout)
    if args.discover_only or not caps:
        client.close()
        return

    if args.args:
        tool_args = json.loads(args.args)
    elif args.tool == "code_review":
        tool_args = {"code": "def hello():\n    print('hello')", "language": "python"}
    else:
        tool_args = {"query": "CRISPR gene editing", "top_k": 3}

    request_id = post_need(client, args.tool, tool_args)
    success = wait_for_result(client, args.tool, request_id)
    client.close()

    print("\n" + "=" * 50)
    print("  Demo completed successfully!" if success else "  Demo did not complete")
    print("=" * 50)
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()

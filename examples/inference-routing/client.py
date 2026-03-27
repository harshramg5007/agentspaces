#!/usr/bin/env python3
"""Client for the supported inference-routing example."""

import argparse
import json
import sys
import time
from uuid import uuid4

sys.path.insert(0, '../../sdk/python')

from agent_space_sdk import AgentSpaceClient
from agent_space_sdk.models import CreateAgentRequest, Query


def latest_capabilities(client):
    caps = client.query_agents(Query(kind="model.capability", tags=["demo:inference"]))
    latest = {}
    for cap in caps:
        agent_id = cap.payload.get("agent_id") or cap.metadata.get("agent_id") or str(cap.id)
        updated_at = cap.metadata.get("updated_at", "")
        current = latest.get(agent_id)
        if current is None or updated_at > current.metadata.get("updated_at", ""):
            latest[agent_id] = cap
    return list(latest.values())


def choose_provider(capabilities, model_type):
    matches = [cap for cap in capabilities if cap.payload.get("model_type") == model_type]
    if not matches:
        return None
    matches.sort(key=lambda cap: int(cap.payload.get("capacity", 0)), reverse=True)
    return matches[0]


def main():
    parser = argparse.ArgumentParser(description='Run the inference-routing client')
    parser.add_argument('--server', default='http://localhost:8080/api/v1', help='Agent Spaces URL')
    parser.add_argument('--model-type', default='chat', help='Desired model type')
    parser.add_argument('--prompt', default='Summarize the request', help='Prompt to route')
    parser.add_argument('--discover-only', action='store_true', help='Only list capabilities')
    parser.add_argument('--timeout', type=int, default=30, help='Seconds to wait for result')
    args = parser.parse_args()

    client = AgentSpaceClient(args.server)
    capabilities = latest_capabilities(client)

    print("=" * 50)
    print("  Available Model Capabilities")
    print("=" * 50)
    for cap in capabilities:
        print(f"  {cap.payload.get('agent_id')}:")
        print(f"    model_type={cap.payload.get('model_type')}")
        print(f"    capacity={cap.payload.get('capacity')}")

    if args.discover_only:
        client.close()
        return

    chosen = choose_provider(capabilities, args.model_type)
    if chosen is None:
        print(f"No provider found for model_type={args.model_type!r}")
        client.close()
        sys.exit(1)

    request_id = str(uuid4())
    provider = chosen.payload["agent_id"]
    capacity = chosen.payload["capacity"]
    print("\n" + "=" * 50)
    print("  Posting Routed Request")
    print("=" * 50)
    print(f"  provider={provider}")
    print(f"  model_type={args.model_type}")
    print(f"  capacity={capacity}")

    client.create_agent(CreateAgentRequest(
        kind="inference.request",
        tags=["demo:inference", f"provider:{provider}", f"model:{args.model_type}"],
        payload={
            "prompt": args.prompt,
            "model_type": args.model_type,
            "min_capacity": capacity,
        },
        metadata={"request_id": request_id},
    ))

    deadline = time.time() + args.timeout
    while time.time() < deadline:
        results = client.query_agents(Query(kind="inference.result", tags=["demo:inference", f"request:{request_id}"]))
        if results:
            result = results[0]
            print("\n" + "=" * 50)
            print("  Result")
            print("=" * 50)
            print(json.dumps(result.payload, indent=4))
            client.close()
            return
        time.sleep(0.5)

    client.close()
    print(f"Timed out waiting for result after {args.timeout}s")
    sys.exit(1)


if __name__ == "__main__":
    main()

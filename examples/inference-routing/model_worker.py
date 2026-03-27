#!/usr/bin/env python3
"""Mock model worker that advertises capacity and processes routed requests."""

import argparse
import sys
import time
from datetime import datetime

sys.path.insert(0, '../../sdk/python')

from agent_space_sdk import AgentSpaceClient
from agent_space_sdk.models import CreateAgentRequest, TakeRequest, Query, AgentStatus, CompleteAgentRequest


def publish_heartbeat(client, agent_id, model_type, capacity):
    client.create_agent(CreateAgentRequest(
        kind="model.capability",
        tags=["demo:inference", f"model:{model_type}", f"provider:{agent_id}"],
        payload={
            "agent_id": agent_id,
            "model_type": model_type,
            "capacity": capacity,
        },
        ttl=10,
        metadata={
            "agent_id": agent_id,
            "updated_at": datetime.utcnow().isoformat(),
        },
    ))


def process_requests(client, agent_id, model_type, capacity):
    request = client.take_agent(
        TakeRequest(
            query=Query(
                kind="inference.request",
                tags=["demo:inference", f"provider:{agent_id}"],
                status=AgentStatus.NEW,
            )
        ),
        agent_id=agent_id,
    )
    if request is None:
        return False

    request_id = request.metadata.get("request_id", str(request.id))
    prompt = request.payload.get("prompt", "")
    output = {
        "provider": agent_id,
        "model_type": model_type,
        "capacity": capacity,
        "response": f"[{model_type}] handled prompt: {prompt}",
    }

    client.create_agent(CreateAgentRequest(
        kind="inference.result",
        tags=["demo:inference", f"request:{request_id}"],
        payload={
            "request_id": request_id,
            "provider": agent_id,
            "model_type": model_type,
            "capacity": capacity,
            "output": output,
        },
        parent_id=request.id,
    ))
    client.complete_agent(
        request.id,
        CompleteAgentRequest(result={"status": "done", "provider": agent_id}),
        agent_id=agent_id,
        lease_token=request.lease_token,
    )
    print(f"[{agent_id}] completed request {request_id}")
    return True


def main():
    parser = argparse.ArgumentParser(description='Run a mock inference worker')
    parser.add_argument('--server', default='http://localhost:8080/api/v1', help='Agent Spaces URL')
    parser.add_argument('--agent-id', required=True, help='Worker id')
    parser.add_argument('--model-type', required=True, help='Model type this worker serves')
    parser.add_argument('--capacity', type=int, required=True, help='Advertised capacity score')
    args = parser.parse_args()

    client = AgentSpaceClient(args.server)
    next_heartbeat = 0.0

    try:
        while True:
            now = time.time()
            if now >= next_heartbeat:
                publish_heartbeat(client, args.agent_id, args.model_type, args.capacity)
                print(f"[{args.agent_id}] heartbeat model={args.model_type} capacity={args.capacity}")
                next_heartbeat = now + 5.0

            if not process_requests(client, args.agent_id, args.model_type, args.capacity):
                time.sleep(0.5)
    finally:
        client.close()


if __name__ == "__main__":
    main()

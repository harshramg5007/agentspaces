#!/usr/bin/env python3
"""
Producer for the fault tolerance demo.

Creates fault_task agents that workers will process.
Each task has a configurable work time and lease duration.
"""

import argparse
import sys
import time
import uuid

# Add parent directory to path to import the SDK
sys.path.insert(0, '../../sdk/python')

from agent_space_sdk import AgentSpaceClient
from agent_space_sdk.models import CreateAgentRequest


def main():
    parser = argparse.ArgumentParser(description='Create fault tolerance test tasks')
    parser.add_argument('--server', default='http://localhost:8080/api/v1',
                        help='Agent space server URL')
    parser.add_argument('--tasks', type=int, default=10,
                        help='Number of tasks to create')
    parser.add_argument('--work-ms', type=int, default=8000,
                        help='Work time in milliseconds per task')
    parser.add_argument('--lease-sec', type=int, default=15,
                        help='Lease duration in seconds')
    parser.add_argument('--ttl-sec', type=int, default=None,
                        help='Agent TTL in seconds (default: derived from work/lease; 0 disables)')
    args = parser.parse_args()

    work_sec = (args.work_ms + 999) // 1000
    default_ttl_sec = max(args.lease_sec * 4, work_sec + args.lease_sec * 2)
    ttl_sec = default_ttl_sec if args.ttl_sec is None else args.ttl_sec
    ttl = ttl_sec if ttl_sec > 0 else None

    print(f"Connecting to {args.server}")
    client = AgentSpaceClient(args.server)

    ttl_label = str(ttl_sec) if ttl is not None else "disabled"
    print(
        f"Creating {args.tasks} tasks (work_ms={args.work_ms}, "
        f"lease_sec={args.lease_sec}, ttl_sec={ttl_label})"
    )

    for i in range(args.tasks):
        task_id = str(uuid.uuid4())
        metadata = {
            "lease_sec": str(args.lease_sec),
        }
        if ttl is not None:
            metadata["ttl_sec"] = str(ttl_sec)
        request = CreateAgentRequest(
            kind="fault_task",
            payload={
                "task_id": task_id,
                "task_num": i + 1,
                "work_ms": args.work_ms,
            },
            tags=["demo:fault"],
            ttl=ttl,
            metadata=metadata,
        )

        try:
            created = client.create_agent(request)
            print(f"  Created task {i + 1}/{args.tasks}: {created.id}")
        except Exception as e:
            print(f"  Failed to create task {i + 1}: {e}")
            continue

    print(f"\nAll {args.tasks} tasks created. Workers can now process them.")
    print("Producer staying alive (waiting for demo to complete)...")

    # Stay alive so --abort-on-container-exit doesn't trigger on producer exit
    # The validator will exit when done, triggering the proper shutdown
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        pass
    finally:
        client.close()


if __name__ == '__main__':
    main()

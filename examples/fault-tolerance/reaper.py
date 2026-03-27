#!/usr/bin/env python3
"""
Reaper for the fault tolerance demo.

Monitors IN_PROGRESS tasks and releases those with expired leases.
This allows tasks abandoned by crashed workers to be picked up by other workers.
"""

import argparse
import sys
import time
from datetime import datetime, timezone, timedelta

# Add parent directory to path to import the SDK
sys.path.insert(0, '../../sdk/python')

from agent_space_sdk import AgentSpaceClient
from agent_space_sdk.models import Query, AgentStatus, UpdateAgentRequest


def parse_iso_datetime(dt_str):
    """Parse ISO format datetime string."""
    if dt_str is None:
        return None
    # Handle various ISO formats
    dt_str = dt_str.replace('Z', '+00:00')
    try:
        return datetime.fromisoformat(dt_str)
    except ValueError:
        return None


def main():
    parser = argparse.ArgumentParser(description='Reap expired fault_task leases')
    parser.add_argument('--server', default='http://localhost:8080/api/v1',
                        help='Agent space server URL')
    parser.add_argument('--interval', type=float, default=1.0,
                        help='Check interval in seconds')
    parser.add_argument('--reaper-id', default='reaper',
                        help='Reaper agent ID')
    args = parser.parse_args()

    print(f"Reaper starting (id={args.reaper_id})")
    print(f"Connecting to {args.server}")
    print(f"Check interval: {args.interval}s")

    client = AgentSpaceClient(args.server)

    try:
        while True:
            # Query all IN_PROGRESS fault_tasks
            query = Query(kind="fault_task", status=AgentStatus.IN_PROGRESS)

            try:
                tasks = client.query_agents(query)
            except Exception as e:
                print(f"[reaper] Error querying agents: {e}")
                time.sleep(args.interval)
                continue

            now = datetime.now(timezone.utc)
            reaped = 0

            for task in tasks:
                lease_sec_str = task.metadata.get("lease_sec", "15")
                try:
                    lease_sec = int(lease_sec_str)
                except ValueError:
                    lease_sec = 15

                lease_until = task.lease_until
                if lease_until is None:
                    claimed_at_str = task.metadata.get("claimed_at")
                    claimed_at = parse_iso_datetime(claimed_at_str)
                    if claimed_at is None:
                        if task.owner_time:
                            claimed_at = task.owner_time
                        else:
                            continue
                    if claimed_at.tzinfo is None:
                        claimed_at = claimed_at.replace(tzinfo=timezone.utc)
                    lease_until = claimed_at + timedelta(seconds=lease_sec)

                if lease_until.tzinfo is None:
                    lease_until = lease_until.replace(tzinfo=timezone.utc)

                # Check if lease expired
                elapsed = (now - lease_until).total_seconds()
                if elapsed > 0:
                    task_num = task.payload.get("task_num", "?")
                    agent_id = task.metadata.get("agent_id", task.owner or "unknown")
                    print(f"[reaper] Task {task_num} expired (elapsed={elapsed:.1f}s > lease={lease_sec}s, owner={agent_id})")

                    # Stamp reap metadata then release the agent
                    try:
                        # Use task.owner for release - it's set atomically by server during Take
                        # (metadata.agent_id is set by worker AFTER take, may be empty if crash)
                        owner_id = task.owner
                        if owner_id:
                            # First, stamp reap metadata for audit trail
                            reap_metadata = dict(task.metadata) if task.metadata else {}
                            reap_metadata["reaped_by"] = args.reaper_id
                            reap_metadata["reaped_at"] = now.isoformat()
                            reap_metadata["reap_reason"] = "lease_expired"
                            reap_metadata["reap_elapsed_sec"] = str(int(elapsed))

                            try:
                                update_req = UpdateAgentRequest(metadata=reap_metadata)
                                client.update_agent(task.id, update_req)
                            except Exception as e:
                                print(f"[reaper] Warning: could not stamp reap metadata: {e}")

                            # Now release the agent with reason
                            client.release_agent(task.id, owner_id, task.lease_token, reason="lease_expired")
                            print(f"[reaper] Released task {task_num}")
                            reaped += 1
                        else:
                            print(f"[reaper] Cannot release task {task_num}: no owner info")
                    except Exception as e:
                        print(f"[reaper] Failed to release task {task_num}: {e}")

            if reaped > 0:
                print(f"[reaper] Reaped {reaped} expired tasks")

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n[reaper] Shutting down")
    finally:
        client.close()


if __name__ == '__main__':
    main()

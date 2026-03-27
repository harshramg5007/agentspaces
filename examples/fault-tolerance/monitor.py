#!/usr/bin/env python3
"""
Monitor for the fault tolerance demo.

Displays real-time stats and events from the agent space.
"""

import argparse
import sys
import time
from datetime import datetime

# Add parent directory to path to import the SDK
sys.path.insert(0, '../../sdk/python')

from agent_space_sdk import AgentSpaceClient
from agent_space_sdk.models import Query, AgentStatus


def main():
    parser = argparse.ArgumentParser(description='Monitor fault_task queue status')
    parser.add_argument('--server', default='http://localhost:8080/api/v1',
                        help='Agent space server URL')
    parser.add_argument('--interval', type=float, default=2.0,
                        help='Refresh interval in seconds')
    parser.add_argument('--events', type=int, default=10,
                        help='Number of recent events to show')
    args = parser.parse_args()

    print(f"Monitor connecting to {args.server}")
    client = AgentSpaceClient(args.server)

    is_tty = sys.stdout.isatty()

    try:
        while True:
            timestamp = datetime.now().strftime('%H:%M:%S')

            if is_tty:
                # Clear screen for interactive terminal
                print("\033[2J\033[H", end="")
                print("=" * 60)
                print(f"  Fault Tolerance Demo Monitor - {timestamp}")
                print("=" * 60)
            else:
                # Compact output for docker logs
                print(f"\n--- Monitor Update [{timestamp}] ---")

            # Query counts by status
            counts = {}
            for status in [AgentStatus.NEW, AgentStatus.IN_PROGRESS, AgentStatus.COMPLETED, AgentStatus.FAILED]:
                try:
                    query = Query(kind="fault_task", status=status, limit=1000)
                    agents = client.query_agents(query)
                    counts[status.value] = len(agents)
                except Exception as e:
                    counts[status.value] = f"err: {e}"

            print("\n  Task Status Counts:")
            print(f"    NEW:          {counts.get('NEW', 0)}")
            print(f"    IN_PROGRESS:  {counts.get('IN_PROGRESS', 0)}")
            print(f"    COMPLETED:    {counts.get('COMPLETED', 0)}")
            print(f"    FAILED:       {counts.get('FAILED', 0)}")

            # Get recent events
            try:
                events = client.get_events(limit=args.events)
                print(f"\n  Recent Events (last {len(events)}):")
                for event in events:
                    event_time = event.timestamp.strftime('%H:%M:%S') if event.timestamp else '?'
                    agent = event.agent_id or '-'
                    print(f"    [{event_time}] {event.type.value:10} by {agent:15} agent={str(event.tuple_id)[:8]}...")
            except Exception as e:
                print(f"\n  Events: error - {e}")

            # Show in-progress tasks
            try:
                query = Query(kind="fault_task", status=AgentStatus.IN_PROGRESS)
                in_progress = client.query_agents(query)
                if in_progress:
                    print(f"\n  In Progress Tasks ({len(in_progress)}):")
                    for task in in_progress[:5]:
                        task_num = task.payload.get("task_num", "?")
                        agent = task.metadata.get("agent_id") or task.owner or "?"
                        claimed = task.metadata.get("claimed_at", "?")
                        if claimed != "?":
                            claimed = claimed.split("T")[1].split(".")[0] if "T" in claimed else claimed
                        print(f"    Task {task_num}: owner={agent}, claimed={claimed}")
                    if len(in_progress) > 5:
                        print(f"    ... and {len(in_progress) - 5} more")
            except Exception as e:
                print(f"\n  In Progress: error - {e}")

            if is_tty:
                print("\n" + "-" * 60)
                print("  Press Ctrl+C to exit")
                print("-" * 60)

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nMonitor stopped")
    finally:
        client.close()


if __name__ == '__main__':
    main()

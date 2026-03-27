#!/usr/bin/env python3
"""
Validator for the fault tolerance demo.

Validates that:
1. All tasks complete successfully
2. At least one task was reaped (demonstrating fault tolerance)
3. No tasks are stuck in progress

Exits with 0 on PASS, 1 on FAIL.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

# Add parent directory to path to import the SDK
sys.path.insert(0, '../../sdk/python')

# Debug mode: set DEBUG=1 or DEBUG=true to enable verbose logging
DEBUG = os.environ.get('DEBUG', '').lower() in ('1', 'true', 'yes')

from agent_space_sdk import AgentSpaceClient
from agent_space_sdk.models import Query, AgentStatus


def _query(kind, status=None, queue=None, limit=1000):
    query = Query(kind=kind, status=status, limit=limit)
    if queue:
        query.metadata = {"queue": str(queue)}
    return query


def get_counts(client, kind, queue=None):
    """Get task counts by status."""
    counts = {}
    for status in [AgentStatus.NEW, AgentStatus.IN_PROGRESS, AgentStatus.COMPLETED, AgentStatus.FAILED]:
        try:
            agents = client.query_agents(_query(kind, status=status, queue=queue))
            counts[status.value] = len(agents)
        except Exception:
            counts[status.value] = 0
    return counts


def _as_utc(ts):
    if ts is None:
        return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def find_stuck_tasks(client, grace_sec: int, kind: str, queue=None):
    """Find IN_PROGRESS tasks whose leases expired beyond grace."""
    stuck = []
    now = datetime.now(timezone.utc)
    try:
        tasks = client.query_agents(_query(kind, status=AgentStatus.IN_PROGRESS, queue=queue))
    except Exception:
        return stuck

    for task in tasks:
        lease_until = _as_utc(task.lease_until)
        if lease_until is None and task.owner_time:
            lease_sec = 0
            try:
                lease_sec = int((task.metadata or {}).get("lease_sec", "0"))
            except (TypeError, ValueError):
                lease_sec = 0
            owner_time = _as_utc(task.owner_time)
            if owner_time and lease_sec > 0:
                lease_until = owner_time + timedelta(seconds=lease_sec)

        if lease_until and now > (lease_until + timedelta(seconds=grace_sec)):
            stuck.append(task)

    return stuck


def write_results(path, results):
    try:
        with open(path, "w") as handle:
            json.dump(results, handle, indent=2)
    except Exception as e:
        if DEBUG:
            print(f"[DEBUG] Failed to write results to {path}: {e}")


def _event_has_reap_reason(event):
    """Check if an event indicates a lease was reaped."""
    data = event.data or {}

    if isinstance(data, str):
        try:
            data = json.loads(data)
        except (json.JSONDecodeError, TypeError):
            if DEBUG:
                print(f"  [DEBUG] Event {event.id}: data is unparseable string: {data[:100]}")
            return False

    if isinstance(data, dict):
        if data.get("reap_reason") == "lease_expired":
            if DEBUG:
                print(f"  [DEBUG] Event {event.id}: found reap_reason=lease_expired in data")
            return True
        metadata = data.get("metadata")
        if isinstance(metadata, dict) and metadata.get("reap_reason") == "lease_expired":
            if DEBUG:
                print(f"  [DEBUG] Event {event.id}: found reap_reason=lease_expired in metadata")
            return True

    if DEBUG:
        print(f"  [DEBUG] Event {event.id}: no reap_reason found, data={data}")
    return False


def scoped_tuple_ids(client, kind, queue=None):
    ids = set()
    for status in [AgentStatus.NEW, AgentStatus.IN_PROGRESS, AgentStatus.COMPLETED, AgentStatus.FAILED]:
        try:
            for task in client.query_agents(_query(kind, status=status, queue=queue)):
                ids.add(str(task.id))
        except Exception:
            continue
    return ids


def count_reaped(client, allowed_tuple_ids=None):
    """Count tasks that were reaped using event data."""
    reaped = set()
    try:
        for event_type in ["RELEASED", "UPDATED"]:
            events = client.get_events(event_type=event_type, limit=1000)
            if DEBUG:
                print(f"[DEBUG] Checking {len(events)} {event_type} events for reap_reason")
            for event in events:
                if allowed_tuple_ids is not None and str(event.tuple_id) not in allowed_tuple_ids:
                    continue
                if _event_has_reap_reason(event):
                    reaped.add(str(event.tuple_id))
                    if DEBUG:
                        print(f"  [DEBUG] Found reaped agent: {event.tuple_id}")
    except Exception as e:
        if DEBUG:
            print(f"[DEBUG] Error counting reaped events: {e}")

    if DEBUG:
        print(f"[DEBUG] Total unique reaped agents: {len(reaped)}")
    return len(reaped)


def completed_task_id_duplicates(client, kind, queue=None):
    duplicates = {}
    seen = {}

    try:
        tasks = client.query_agents(_query(kind, status=AgentStatus.COMPLETED, queue=queue))
    except Exception:
        return duplicates

    for task in tasks:
        task_id = str((task.payload or {}).get("task_id") or task.id)
        seen.setdefault(task_id, []).append(str(task.id))

    for task_id, tuple_ids in seen.items():
        if len(tuple_ids) > 1:
            duplicates[task_id] = tuple_ids
    return duplicates


def main():
    parser = argparse.ArgumentParser(description='Validate fault tolerance demo')
    parser.add_argument('--server', default='http://localhost:8080/api/v1',
                        help='Agent space server URL')
    parser.add_argument('--expected', type=int, default=10,
                        help='Expected number of completed tasks')
    parser.add_argument('--timeout', type=int, default=120,
                        help='Timeout in seconds')
    parser.add_argument('--interval', type=float, default=2.0,
                        help='Poll interval in seconds')
    parser.add_argument('--require-reap', action='store_true', default=True,
                        help='Require at least one reaped task')
    parser.add_argument('--kind', default='fault_task',
                        help='Agent kind to validate')
    parser.add_argument('--queue', default='',
                        help='Optional metadata.queue filter')
    parser.add_argument('--require-unique-task-ids', action='store_true',
                        help='Fail if duplicate payload.task_id values complete')
    parser.add_argument('--grace-sec', type=int, default=2,
                        help='Grace period past lease expiry before failing')
    parser.add_argument('--results', default='results.json',
                        help='Path to write results JSON')
    args = parser.parse_args()

    queue = args.queue or None

    print(f"Validator connecting to {args.server}")
    print(f"Expecting {args.expected} tasks to complete within {args.timeout}s")
    print(f"Grace period: {args.grace_sec}s")
    print(f"Results file: {args.results}")
    print(f"Kind filter: {args.kind}")
    if queue:
        print(f"Queue filter: {queue}")
    if args.require_reap:
        print("Requiring at least one reaped task to demonstrate fault tolerance")
        print("  (Reaping via the server lease reaper)")
    if DEBUG:
        print("DEBUG mode enabled - verbose event logging active")
    print("-" * 60)

    client = AgentSpaceClient(args.server)
    start_time = time.time()

    try:
        while True:
            elapsed = time.time() - start_time
            counts = get_counts(client, args.kind, queue)
            ids = scoped_tuple_ids(client, args.kind, queue)
            reaped = count_reaped(client, ids)
            stuck = find_stuck_tasks(client, args.grace_sec, args.kind, queue)

            if elapsed > args.timeout:
                print(f"\nFAIL: Timeout after {args.timeout}s")
                print(f"  NEW: {counts.get('NEW', 0)}")
                print(f"  IN_PROGRESS: {counts.get('IN_PROGRESS', 0)}")
                print(f"  COMPLETED: {counts.get('COMPLETED', 0)}")
                print(f"  FAILED: {counts.get('FAILED', 0)}")
                print(f"  Reaped: {reaped}")
                results = {
                    "status": "FAIL",
                    "reason": "timeout",
                    "counts": counts,
                    "reaped": reaped,
                    "stuck_ids": [str(t.id) for t in stuck],
                    "elapsed_sec": int(elapsed),
                }
                write_results(args.results, results)
                sys.exit(1)

            timestamp = datetime.now().strftime('%H:%M:%S')
            print(f"[{timestamp}] NEW={counts.get('NEW', 0)} IN_PROGRESS={counts.get('IN_PROGRESS', 0)} "
                  f"COMPLETED={counts.get('COMPLETED', 0)} FAILED={counts.get('FAILED', 0)} "
                  f"Reaped={reaped} ({int(elapsed)}s)")

            if stuck:
                stuck_ids = [str(t.id) for t in stuck]
                print("\nFAIL: Tasks stuck IN_PROGRESS past lease+grace")
                print(f"  Stuck count: {len(stuck_ids)}")
                print(f"  Stuck IDs: {', '.join(stuck_ids[:10])}")
                results = {
                    "status": "FAIL",
                    "reason": "stuck_in_progress",
                    "counts": counts,
                    "reaped": reaped,
                    "stuck_ids": stuck_ids,
                    "elapsed_sec": int(elapsed),
                }
                write_results(args.results, results)
                sys.exit(1)

            completed = counts.get('COMPLETED', 0)
            in_progress = counts.get('IN_PROGRESS', 0)
            new_count = counts.get('NEW', 0)
            failed = counts.get('FAILED', 0)

            if completed >= args.expected and in_progress == 0 and new_count == 0:
                print("\n" + "=" * 60)

                if args.require_reap and reaped == 0:
                    print("FAIL: All tasks completed but no tasks were reaped")
                    print("  This means fault tolerance was not demonstrated")
                    results = {
                        "status": "FAIL",
                        "reason": "no_reaped_tasks",
                        "counts": counts,
                        "reaped": reaped,
                        "elapsed_sec": int(elapsed),
                    }
                    write_results(args.results, results)
                    sys.exit(1)

                duplicates = completed_task_id_duplicates(client, args.kind, queue)
                if args.require_unique_task_ids and duplicates:
                    print("FAIL: duplicate completed payload.task_id values found")
                    for task_id, tuple_ids in sorted(duplicates.items()):
                        print(f"  task_id={task_id} tuple_ids={', '.join(tuple_ids)}")
                    results = {
                        "status": "FAIL",
                        "reason": "duplicate_completed_task_ids",
                        "counts": counts,
                        "reaped": reaped,
                        "duplicates": duplicates,
                        "elapsed_sec": int(elapsed),
                    }
                    write_results(args.results, results)
                    sys.exit(1)

                print(f"PASS: completed={completed} failed={failed} reaped={reaped}")
                print(f"  Duration: {int(elapsed)}s")
                results = {
                    "status": "PASS",
                    "counts": counts,
                    "reaped": reaped,
                    "duplicates": duplicates if args.require_unique_task_ids else {},
                    "elapsed_sec": int(elapsed),
                }
                write_results(args.results, results)
                print("=" * 60)
                sys.exit(0)

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nValidator interrupted")
        sys.exit(1)
    finally:
        client.close()


if __name__ == '__main__':
    main()

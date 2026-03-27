#!/usr/bin/env python3
"""
Worker for the fault tolerance demo.

Claims tasks via Take (no central assignment), processes them, and marks complete.
Supports crash injection to simulate worker failures mid-task.

Enhancements (backwards compatible):
- --trace-id: restricts work to a single run (useful for multi-run sweeps)
- --queue: sets Query.metadata["queue"], enabling Valkey/Redis queue fast-path
- --ledger-dsn/--run-id: optional completion ledger writes for reliability runs
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import os
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from uuid import UUID

# Add parent directory to path to import the SDK
sys.path.insert(0, '../../sdk/python')

from agent_space_sdk import AgentSpaceClient
from agent_space_sdk.models import (
    Query,
    TakeRequest,
    CompleteAgentRequest,
    RenewLeaseRequest,
    AgentStatus,
)


def _parse_uuid(v: str | None) -> UUID | None:
    if not v:
        return None
    try:
        return UUID(str(v))
    except (TypeError, ValueError):
        return None


def _as_bool(value: str | bool | int | None, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in {"1", "true", "yes", "on"}:
        return True
    if s in {"0", "false", "no", "off"}:
        return False
    return default


def _load_ledger_client_class():
    """Load LedgerClient from either local copy or repo path."""
    try:
        from ledger import LedgerClient

        return LedgerClient
    except Exception:
        pass

    candidates = [
        Path(__file__).resolve().with_name("ledger.py"),
        Path(__file__).resolve().parents[1] / "azure_aks_fault_tolerance" / "ledger.py",
    ]

    for candidate in candidates:
        if not candidate.exists():
            continue
        spec = importlib.util.spec_from_file_location("ledger", str(candidate))
        if spec is None or spec.loader is None:
            continue
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        if hasattr(module, "LedgerClient"):
            return module.LedgerClient

    return None


def _parse_lease_sec(metadata: dict | None, default: int = 15) -> int:
    if not metadata:
        return default
    try:
        lease_sec = int(metadata.get("lease_sec", "0"))
    except (TypeError, ValueError):
        lease_sec = 0
    if lease_sec <= 0:
        return default
    return lease_sec


def _task_id_for_agent(task) -> str:
    payload = task.payload or {}
    task_id = payload.get("task_id")
    if task_id:
        return str(task_id)
    return str(task.id)


def _assigned_queue(worker_id: str, queue_count: int, queue_prefix: str) -> str | None:
    if queue_count <= 0:
        return None
    digest = hashlib.sha1(worker_id.encode("utf-8")).hexdigest()
    idx = int(digest, 16) % queue_count
    return f"{queue_prefix}-{idx}"


def _is_retryable_completion_error(err_msg: str) -> bool:
    lowered = (err_msg or "").lower()
    retry_markers = [
        "service unavailable",
        "gateway timeout",
        "timeout",
        "timed out",
        "connection refused",
        "connection reset",
        "connection aborted",
        "temporarily unavailable",
        "agent not found",
        "write ack",
        "insufficient replicas",
    ]
    return any(marker in lowered for marker in retry_markers)


def _is_agent_not_found_error(err_msg: str) -> bool:
    lowered = (err_msg or "").lower()
    return "agent not found" in lowered


def _retry_backoff_sec(base_delay_sec: float, attempt: int) -> float:
    base = max(0.0, float(base_delay_sec))
    if base == 0:
        return 0.0
    exp = max(0, int(attempt) - 1)
    return min(5.0, base * (2.0 ** exp))


def main():
    parser = argparse.ArgumentParser(description='Process fault tolerance test tasks')
    parser.add_argument('--server', default='http://localhost:8080/api/v1',
                        help='Agent space server URL')
    parser.add_argument('--worker-id', default=None,
                        help='Worker ID (auto-generated if not provided)')
    parser.add_argument('--crash-after', type=int, default=0,
                        help='Crash after claiming N agents (0 = never crash)')
    parser.add_argument('--poll-interval', type=float, default=1.0,
                        help='Seconds to wait between polling for tasks')
    parser.add_argument('--trace-id', default=None,
                        help='Trace ID (UUID) to restrict work to a single run')
    parser.add_argument('--queue', default=None,
                        help='Queue name (metadata.queue). Strongly recommended for Valkey/Redis scaling')
    parser.add_argument('--queue-count', type=int, default=int(os.environ.get('QUEUE_COUNT', '0')),
                        help='If >0 and --queue is unset, auto-assign queue as <queue-prefix>-N using worker hash')
    parser.add_argument('--queue-prefix', default=os.environ.get('QUEUE_PREFIX', 'ftq'),
                        help='Queue prefix used with --queue-count auto-assignment')
    parser.add_argument('--ledger-dsn', default=os.environ.get('LEDGER_DSN', ''),
                        help='Optional Postgres DSN for completion ledger writes')
    parser.add_argument('--run-id', default=os.environ.get('RUN_ID', None),
                        help='Reliability run ID used by ledger rows')
    parser.add_argument('--ledger-connect-timeout-sec', type=int,
                        default=int(os.environ.get('LEDGER_CONNECT_TIMEOUT_SEC', '5')),
                        help='Ledger Postgres connect timeout in seconds')
    parser.add_argument('--enforce-run-id-match', default=os.environ.get('ENFORCE_RUN_ID_MATCH', ''),
                        help='Require payload.run_id to match --run-id/ RUN_ID (default: enabled when run-id is set)')
    parser.add_argument('--allow-missing-run-id', default=os.environ.get('ALLOW_MISSING_RUN_ID', '0'),
                        help='Allow agents with missing payload.run_id when run-id matching is enforced (0/1)')
    parser.add_argument('--complete-retry-max-attempts', type=int,
                        default=int(os.environ.get('COMPLETE_RETRY_MAX_ATTEMPTS', '8')),
                        help='Max attempts for completion API retries (>=1)')
    parser.add_argument('--complete-retry-base-delay-sec', type=float,
                        default=float(os.environ.get('COMPLETE_RETRY_BASE_DELAY_SEC', '0.5')),
                        help='Base delay in seconds for completion/release retry backoff')
    parser.add_argument('--complete-not-found-recheck-sec', type=int,
                        default=int(os.environ.get('COMPLETE_NOT_FOUND_RECHECK_SEC', '30')),
                        help='Seconds to recheck ledger completion after agent-not-found completion failures')
    args = parser.parse_args()

    trace_id = _parse_uuid(args.trace_id)

    worker_id = args.worker_id or f"worker-{uuid.uuid4().hex[:8]}"
    print(f"Worker {worker_id} starting")
    print(f"Connecting to {args.server}")
    if trace_id:
        print(f"trace_id={trace_id}")
    queue = args.queue or _assigned_queue(worker_id, int(args.queue_count), str(args.queue_prefix))
    if queue:
        print(f"queue={queue}")
    else:
        print("queue=unset (only valid when server does not require metadata.queue)")
    if args.complete_retry_max_attempts < 1:
        raise SystemExit("--complete-retry-max-attempts must be >= 1")
    if args.complete_retry_base_delay_sec < 0:
        raise SystemExit("--complete-retry-base-delay-sec must be >= 0")
    if args.complete_not_found_recheck_sec < 0:
        raise SystemExit("--complete-not-found-recheck-sec must be >= 0")

    expected_run_id = str(args.run_id).strip() if args.run_id else ""
    if str(args.enforce_run_id_match).strip() == "":
        enforce_run_id_match = bool(expected_run_id)
    else:
        enforce_run_id_match = _as_bool(args.enforce_run_id_match, default=bool(expected_run_id))
    allow_missing_run_id = _as_bool(args.allow_missing_run_id, default=False)
    print(
        f"run_id_guard enforce={int(enforce_run_id_match)} "
        f"allow_missing={int(allow_missing_run_id)} expected_run_id={expected_run_id or '<unset>'}"
    )

    ledger = None
    ledger_run_id = args.run_id or (str(trace_id) if trace_id else "default")
    if args.ledger_dsn:
        LedgerClient = _load_ledger_client_class()
        if LedgerClient is None:
            print("[worker] ERROR: --ledger-dsn set but LedgerClient could not be loaded")
            raise SystemExit(2)

        try:
            ledger = LedgerClient(
                args.ledger_dsn,
                ledger_run_id,
                connect_timeout_sec=max(1, int(args.ledger_connect_timeout_sec)),
                auto_init=True,
            )
            ledger.ensure_run_state(status="RUNNING", notes={"worker_id": worker_id})
            print(f"ledger run_id={ledger_run_id}")
        except Exception as exc:
            print(f"[worker] ERROR: failed to initialize ledger: {exc}")
            raise SystemExit(2)

    client = AgentSpaceClient(args.server)
    tasks_claimed = 0
    run_id_mismatch_released = 0
    completion_retry_count = 0
    completion_not_found_after_retries = 0
    release_retry_count = 0
    last_counter_emit = time.time()

    try:
        while True:
            if (time.time() - last_counter_emit) >= 30.0:
                print(
                    f"[{worker_id}] counters completion_retry_count={completion_retry_count} "
                    f"completion_not_found_after_retries={completion_not_found_after_retries} "
                    f"release_retry_count={release_retry_count} "
                    f"run_id_mismatch_released={run_id_mismatch_released}"
                )
                last_counter_emit = time.time()
            # Try to take a NEW fault_task (pass agent_id via header for ownership)
            query = Query(kind="fault_task", status=AgentStatus.NEW)
            if trace_id:
                query.trace_id = trace_id
            if queue:
                query.metadata = {"queue": str(queue)}

            request = TakeRequest(query=query)

            try:
                task = client.take_agent(request, agent_id=worker_id)
            except Exception as e:
                print(f"[{worker_id}] Error taking agent: {e}")
                time.sleep(args.poll_interval)
                continue

            if task is None:
                # No tasks available
                time.sleep(args.poll_interval)
                continue

            payload = task.payload or {}
            task_num = payload.get("task_num", "?")
            work_ms = payload.get("work_ms", 5000)
            lease_sec = _parse_lease_sec(task.metadata)
            task_id = _task_id_for_agent(task)

            if enforce_run_id_match and expected_run_id:
                payload_run_id_raw = payload.get("run_id")
                payload_run_id = str(payload_run_id_raw).strip() if payload_run_id_raw is not None else ""
                mismatch_reason = None
                if not payload_run_id and not allow_missing_run_id:
                    mismatch_reason = "missing_run_id"
                elif payload_run_id and payload_run_id != expected_run_id:
                    mismatch_reason = "run_id_mismatch"

                if mismatch_reason is not None:
                    run_id_mismatch_released += 1
                    print(
                        f"[{worker_id}] run_id_mismatch_released={run_id_mismatch_released} "
                        f"task_id={task_id} tuple_id={task.id} reason={mismatch_reason} "
                        f"expected_run_id={expected_run_id} payload_run_id={payload_run_id or '<missing>'}"
                    )
                    if ledger is not None:
                        try:
                            ledger.record_completion_conflict(
                                task_id=task_id,
                                tuple_id=str(task.id),
                                worker_id=worker_id,
                                lease_token=str(task.lease_token),
                                conflict_reason="run_id_mismatch_claim",
                                details={
                                    "expected_run_id": expected_run_id,
                                    "payload_run_id": payload_run_id or "",
                                    "reason": mismatch_reason,
                                },
                            )
                        except Exception as exc:
                            print(f"[{worker_id}] Warning: failed to record run-id mismatch in ledger: {exc}")
                    try:
                        client.release_agent(task.id, worker_id, task.lease_token)
                        print(f"[{worker_id}] Released mismatched task {task_num}")
                    except Exception as e:
                        print(f"[{worker_id}] Failed to release mismatched task: {e}")
                    continue

            # Claimed a valid task
            tasks_claimed += 1
            print(f"[{worker_id}] Claimed task {task_num} (id={task.id}, task_id={task_id})")

            if ledger is not None:
                try:
                    is_reclaim = ledger.has_prior_claim(task_id)
                    ledger.record_claim(
                        task_id=task_id,
                        tuple_id=str(task.id),
                        agent_id=worker_id,
                        lease_token=str(task.lease_token),
                        lease_sec=lease_sec,
                        is_reclaim=is_reclaim,
                    )
                except Exception as exc:
                    print(f"[{worker_id}] Warning: failed to record claim in ledger: {exc}")

            # Check if we should crash (simulate failure)
            if args.crash_after > 0 and tasks_claimed >= args.crash_after:
                print(f"[{worker_id}] CRASH! Simulating failure after claiming {tasks_claimed} tasks")
                print(f"[{worker_id}] Task {task_num} will be stuck until reaper releases it")
                # Sleep forever to simulate a hung/crashed worker that holds the lease.
                try:
                    while True:
                        time.sleep(60)
                except KeyboardInterrupt:
                    pass
                return

            # Simulate work
            print(f"[{worker_id}] Processing task {task_num} for {work_ms}ms...")
            deadline = time.time() + (work_ms / 1000.0)
            renew_interval = max(1.0, lease_sec / 2.0)
            next_renew = time.time() + renew_interval

            while True:
                now = time.time()
                if now >= deadline:
                    break
                if lease_sec > 0 and now >= next_renew:
                    try:
                        client.renew_lease(
                            task.id,
                            agent_id=worker_id,
                            lease_token=task.lease_token,
                            request=RenewLeaseRequest(),
                        )
                        print(f"[{worker_id}] Renewed lease for task {task_num}")
                    except Exception as e:
                        print(f"[{worker_id}] Failed to renew lease: {e}")
                    next_renew = now + renew_interval
                sleep_for = min(0.2, deadline - now, max(0.0, next_renew - now))
                if sleep_for > 0:
                    time.sleep(sleep_for)

            # Complete the task
            completion_ts = datetime.utcnow().isoformat()
            completion_succeeded = False
            complete_error_msg = ""
            complete_request = CompleteAgentRequest(
                result={
                    "status": "done",
                    "worker": worker_id,
                    "completed_at": completion_ts,
                }
            )
            for attempt in range(1, max(1, int(args.complete_retry_max_attempts)) + 1):
                try:
                    client.complete_agent(
                        task.id,
                        complete_request,
                        agent_id=worker_id,
                        lease_token=task.lease_token,
                    )
                    completion_succeeded = True
                    break
                except Exception as e:
                    complete_error_msg = str(e)
                    retryable = _is_retryable_completion_error(complete_error_msg)
                    if retryable and attempt < int(args.complete_retry_max_attempts):
                        completion_retry_count += 1
                        delay = _retry_backoff_sec(args.complete_retry_base_delay_sec, attempt)
                        print(
                            f"[{worker_id}] complete retry attempt={attempt} "
                            f"task={task_num} delay={delay:.2f}s err={complete_error_msg}"
                        )
                        if delay > 0:
                            time.sleep(delay)
                        continue
                    break

            if completion_succeeded:
                print(f"[{worker_id}] Completed task {task_num}")

                if ledger is not None:
                    inserted = ledger.record_completion(
                        task_id=task_id,
                        tuple_id=str(task.id),
                        worker_id=worker_id,
                        lease_token=str(task.lease_token),
                    )
                    if inserted:
                        ledger.increment_completed(1)
                    else:
                        ledger.record_completion_conflict(
                            task_id=task_id,
                            tuple_id=str(task.id),
                            worker_id=worker_id,
                            lease_token=str(task.lease_token),
                            conflict_reason="duplicate_completion_insert_conflict",
                        )
                continue

            err_msg = complete_error_msg or "unknown completion error"
            print(f"[{worker_id}] Failed to complete task after retries: {err_msg}")
            lowered = err_msg.lower()
            reason = "completion_api_error"
            if "token_mismatch_stale" in lowered or "stale" in lowered:
                reason = "stale_completion_rejected"
            elif "token_mismatch" in lowered or "lease token mismatch" in lowered:
                reason = "lease_token_mismatch"

            if _is_agent_not_found_error(err_msg):
                completion_not_found_after_retries += 1
                reason = "completion_not_found_after_retries"
                recovered = False
                if ledger is not None:
                    try:
                        if ledger.has_completion(task_id):
                            recovered = True
                    except Exception as exc:
                        print(f"[{worker_id}] Warning: has_completion check failed: {exc}")
                    if not recovered and int(args.complete_not_found_recheck_sec) > 0:
                        recheck_deadline = time.time() + float(args.complete_not_found_recheck_sec)
                        while time.time() < recheck_deadline:
                            try:
                                if ledger.has_completion(task_id):
                                    recovered = True
                                    break
                            except Exception:
                                pass
                            time.sleep(1.0)
                if recovered:
                    print(
                        f"[{worker_id}] completion agent-not-found recovered via ledger check "
                        f"task={task_num} task_id={task_id}"
                    )
                    continue

            if ledger is not None:
                ledger.record_completion_conflict(
                    task_id=task_id,
                    tuple_id=str(task.id),
                    worker_id=worker_id,
                    lease_token=str(task.lease_token),
                    conflict_reason=reason,
                    details={"error": err_msg},
                )

            # Try to release if we can't complete.
            release_ok = False
            last_release_error = ""
            for attempt in range(1, max(1, int(args.complete_retry_max_attempts)) + 1):
                try:
                    client.release_agent(task.id, worker_id, task.lease_token)
                    print(f"[{worker_id}] Released task {task_num}")
                    release_ok = True
                    break
                except Exception as e2:
                    last_release_error = str(e2)
                    retryable_release = _is_retryable_completion_error(last_release_error)
                    if retryable_release and attempt < int(args.complete_retry_max_attempts):
                        release_retry_count += 1
                        delay = _retry_backoff_sec(args.complete_retry_base_delay_sec, attempt)
                        print(
                            f"[{worker_id}] release retry attempt={attempt} "
                            f"task={task_num} delay={delay:.2f}s err={last_release_error}"
                        )
                        if delay > 0:
                            time.sleep(delay)
                        continue
                    break
            if not release_ok:
                print(f"[{worker_id}] Failed to release task after retries: {last_release_error}")
                if ledger is not None:
                    try:
                        ledger.record_completion_conflict(
                            task_id=task_id,
                            tuple_id=str(task.id),
                            worker_id=worker_id,
                            lease_token=str(task.lease_token),
                            conflict_reason="release_failed_after_retries",
                            details={"error": last_release_error},
                        )
                    except Exception as exc:
                        print(f"[{worker_id}] Warning: failed to record release failure in ledger: {exc}")

    except KeyboardInterrupt:
        print(f"\n[{worker_id}] Shutting down gracefully")
    finally:
        print(
            f"[{worker_id}] final_counters completion_retry_count={completion_retry_count} "
            f"completion_not_found_after_retries={completion_not_found_after_retries} "
            f"release_retry_count={release_retry_count} "
            f"run_id_mismatch_released={run_id_mismatch_released}"
        )
        client.close()
        if ledger is not None:
            try:
                ledger.close()
            except Exception:
                pass


if __name__ == '__main__':
    main()

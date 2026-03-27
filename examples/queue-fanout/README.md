# Queue Fanout

Status: supported

## What This Example Shows

- 10 workers claim from one shared queue
- one worker crashes after its first claim
- the runtime reclaims abandoned work
- all 100 task IDs finish exactly once
- no task remains stuck in `IN_PROGRESS`

## Prerequisites

- Docker with a running daemon
- `make doctor`

## Run

```bash
make example-queue-fanout
```

## Expected Output

- The validator exits successfully
- `completed=100`
- `reaped` is at least `1`
- duplicate `task_id` validation passes

## Notes

This example reuses the supported fault-tolerance worker and producer flow, but scales the queue out to 10 competing workers to show atomic claims and recovery under contention.

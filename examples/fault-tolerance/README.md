# Fault Tolerance

Status: supported

## What This Example Shows

- Workers claim tasks through leases
- A worker crashes mid-task
- The lease expires and work is reclaimed
- Another worker completes the abandoned task

## Prerequisites

- Docker with a running daemon
- `make doctor`

## Run

```bash
make example-fault-tolerance
```

## Expected Output

- The validator exits successfully
- The crashed worker's task is reclaimed and completed
- No task remains stuck in `IN_PROGRESS`

## Cleanup

`make example-fault-tolerance` tears its own compose stack down after the validator exits.

The example remains intentionally narrow and self-verifying so it can stay in the supported launch surface.

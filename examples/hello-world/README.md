# Hello World

Status: supported

## What This Example Shows

- The local server is reachable
- The Python SDK connects
- A agent can be created, claimed, and completed

## Prerequisites

- `make doctor`
- `make up`

## Run

```bash
make example-hello
```

## Expected Output

```text
PASS: local server started
PASS: SDK connected
PASS: agent lifecycle verified
Next: make example-fault-tolerance
```

## Cleanup

```bash
make down
```

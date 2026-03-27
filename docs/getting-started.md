# Getting Started

Use this path if you want the supported local experience.

## Prerequisites

- Docker with a running daemon
- Docker Compose or `docker compose`
- Python 3.11

## Quickstart

```bash
make doctor
make up
make smoke
```

Expected output:

```text
PASS: local server started
PASS: SDK connected
PASS: agent lifecycle verified
Next: make example-fault-tolerance
```

The supported smoke example lives in [`examples/hello-world`](../examples/hello-world/README.md).

## Cleanup

```bash
make down
```

## Next Step

Run the supported recovery example:

```bash
make example-fault-tolerance
```

Then fan that same queue out across 10 workers:

```bash
make example-queue-fanout
```

For the broader repo layout, see [../examples/README.md](../examples/README.md), [concepts.md](concepts.md), and [operations.md](operations.md).

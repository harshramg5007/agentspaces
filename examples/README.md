# Examples

These are the launch-supported examples.

| Example | Shows | Time | Status | Command |
| --- | --- | ---: | --- | --- |
| Hello World | create / claim / complete | 2 min | supported | `make example-hello` |
| Fault Tolerance | lease expiry / reclaim | 5 min | supported | `make example-fault-tolerance` |
| Queue Fanout | 10 workers on one queue with one crash | 6 min | supported | `make example-queue-fanout` |
| MCP Mesh | tool capability heartbeats and routing | 8 min | supported | `cd examples/mcp-mesh && docker compose up --build` |
| Inference Routing | model capability heartbeats and capacity-aware routing | 6 min | supported | `cd examples/inference-routing && docker compose up --build` |

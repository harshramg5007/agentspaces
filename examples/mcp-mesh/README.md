# MCP Mesh

Status: supported

## What This Example Shows

- workers publish MCP tool capabilities with TTL heartbeats
- clients discover available tools from Agent Spaces
- needs are claimed by matching workers instead of a central registry
- results are written back into the same coordination runtime

## Architecture

```text
┌─────────────┐     ┌─────────────┐
│ MCP Server  │     │ MCP Server  │
│ web_search  │     │ code_review │
└──────┬──────┘     └──────┬──────┘
       │                   │
┌──────┴──────┐     ┌──────┴──────┐
│ Tool Worker │     │ Tool Worker │
│ web_search  │     │ code_review │
└──────┬──────┘     └──────┴──────┘
       └────────┬──────────┘
                │
         ┌──────┴──────┐
         │ Agent Space │
         │ capability  │
         │ + need flow │
         └──────┬──────┘
                │
         ┌──────┴──────┐
         │   Client    │
         └─────────────┘
```

## Run

```bash
cd examples/mcp-mesh
docker compose up --build
```

In another terminal:

```bash
python client.py --discover-only
python client.py --tool web_search
python client.py --tool code_review --args '{"code":"def foo(): pass","language":"python"}'
```

## Expected Output

- at least two capability heartbeats are discoverable
- a `need` is claimed by the matching worker
- a `result` agent is written back with provider metadata

## Why It Matters

MCP standardizes tool invocation after a client is connected.
This example shows how Agent Spaces handles the coordination problem around that call path: discovery, routing, presence, and recovery.

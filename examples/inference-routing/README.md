# Inference Routing

Status: supported

## What This Example Shows

- model workers publish capability heartbeats with TTL
- the client filters capabilities by model type
- the client picks the highest-capacity matching worker
- requests are routed through Agent Spaces instead of a side registry

## Architecture

```text
┌─────────────┐      heartbeat      ┌──────────────────────┐
│ Model       │ ------------------> │ Agent Space          │
│ Worker A    │                     │ capability + request │
└─────────────┘                     └──────────┬───────────┘
                                              │
┌─────────────┐      heartbeat                │
│ Model       │ -----------------------------┘
│ Worker B    │
└─────────────┘
       ▲
       │ result
       │
┌──────┴──────┐      request       ┌─────────────┐
│ Routed      │ <----------------> │   Client    │
│ execution   │                    └─────────────┘
└─────────────┘
```

## Run

```bash
cd examples/inference-routing
docker compose up --build
```

In another terminal:

```bash
python client.py --discover-only
python client.py --model-type chat --prompt "Summarize the request"
python client.py --model-type reasoning --prompt "Plan a rollout"
```

## Expected Output

- at least one capability heartbeat is available per model type
- the client shows which provider was selected
- the result includes the selected provider and its advertised capacity

## Why It Matters

This is the simplest version of a real inference routing problem:
capability heartbeats, request selection, and worker claims all live in one coordination system.

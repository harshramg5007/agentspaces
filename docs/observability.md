# Observability

Agent Spaces already exposes the pieces platform teams usually need to connect coordination state to an observability stack: lifecycle events, telemetry spans, and DAG views.

## What The Runtime Emits

- `/events`: lifecycle history for created, claimed, completed, failed, updated, and released agents
- `/telemetry/spans`: trace-shaped records for request and worker activity
- `/dag/{id}`: parent-child execution graph for multi-step flows
- `/stats` and `/agents/stats`: system and filtered workload counts

## How To Use It With Existing Tooling

- Use `/events` for audit logs, failure timelines, reclaim analysis, and queue health checks.
- Use `/telemetry/spans` as the handoff point into trace pipelines, or to mirror trace-shaped data into a central store.
- Use DAG output to reconstruct fanout, retries, and downstream result creation for post-incident review.
- Use stats endpoints to back Grafana panels for queue depth, in-progress work, and completion rates.

## OpenTelemetry Positioning

Agent Spaces does not claim native OTLP export in the supported launch surface.
The current supported model is to read spans and events from the runtime and forward or transform them into your existing OpenTelemetry pipeline.

That keeps the launch surface narrow while still mapping cleanly onto common platform workflows:

- Tempo or Jaeger for trace views
- Grafana for queue and lease dashboards
- Loki or another log store for event archives
- Prometheus for service and capacity metrics

## Grafana Dashboard Template

The repo already includes a Grafana dashboard template at [`monitoring/grafana/dashboards/agent-space-overview.json`](../monitoring/grafana/dashboards/agent-space-overview.json).

Use it as the starting point for panels such as:

- `NEW` vs `IN_PROGRESS` vs `COMPLETED`
- claim throughput over time
- release and reclaim counts
- worker ownership spread
- long-running in-progress tasks

## Suggested Rollup Views

- Queue health: depth, claim rate, completion rate, reclaim count
- Reliability: stuck-in-progress count, expired lease count, retry rate
- Topology: DAG width, DAG depth, downstream output count
- Routing: claims by tag, claims by worker, claims by capability

## Minimal Integration Loop

1. Poll `/events` for recent lifecycle changes.
2. Poll `/telemetry/spans` by `trace_id` for trace-shaped data.
3. Pull `/dag/{id}` for interesting or failed flows.
4. Forward the data into your existing metrics, logs, and traces backends.

That is enough to give platform engineers a unified view of queue health, crash recovery, and multi-agent execution paths without changing the runtime surface.

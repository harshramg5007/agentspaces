# Benchmarks

This page is the public benchmark landing page for Agent Spaces until a dedicated website is live.

## What It Covers

- throughput under shared-queue load
- lease expiry and crash recovery behavior
- multi-node and scaling scenarios
- report generation from benchmark output

## Included Benchmark Surfaces

- load-test coverage for queue throughput and contention
- web-facing load generation for HTTP entrypoints
- meta-benchmark sweeps across scenarios and backends
- report generation for turning raw runs into comparison charts

## What To Publish From A Run

- throughput by worker count
- latency percentiles for claim and completion paths
- reclaim count and recovery time after worker failure
- duplicate-completion count, which should stay at zero

## Recommended Public Charts

- completed tasks per second
- `NEW` vs `IN_PROGRESS` vs `COMPLETED`
- lease expiry to successful reclaim time
- worker spread across one shared queue

## Current Public Positioning

Use benchmark results to show three concrete claims:

- Agent Spaces claims work atomically under contention.
- Agent Spaces recovers cleanly after worker crashes.
- Agent Spaces stays inspectable while work is waiting, claimed, and completed.

## Next Packaging Step

The dedicated website page should mirror this document and embed exported charts from the benchmark report generator.

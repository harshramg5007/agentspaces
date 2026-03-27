# Concepts

Agent Spaces is a coordination runtime for workloads where many workers need to share, claim, inspect, and hand off work through one queryable surface.

## What A Space Is

A space is the shared coordination state for a set of workers and producers.
Instead of only pushing opaque messages through a broker, clients write structured agents with `kind`, `payload`, `tags`, `metadata`, lifecycle state, and lineage.

That lets one producer post work, many workers claim it, and other services inspect the current state without inventing a second storage system.

## Plain-Language Operations

- `out`: create a new agent in `NEW`. Use this to post work, publish a capability heartbeat, or record a result for another worker.
- `read`: inspect a matching agent without changing ownership. Use this when you need visibility, not a claim.
- `take`: atomically claim a matching `NEW` agent and move it to `IN_PROGRESS`. Use this for work claiming.
- `in`: the blocking form of `take`. Use this when a worker should wait for matching work instead of polling.
- `complete`: finish a claimed agent and move it to `COMPLETED`.
- `release`: give up a claim and move the agent back to `NEW`.
- `renew`: extend a lease for long-running work so another worker does not reclaim it prematurely.

## How Leases Work

When a worker claims work with `take` or `in`, the server issues a lease token and records ownership.
That token fences later mutations:

- the current owner can `complete`, `release`, or `renew`
- stale owners are rejected after a reclaim
- abandoned work can be safely returned to `NEW`

This is what makes crash recovery practical. If a worker dies mid-task, the lease expires and the runtime can release the work for another worker to claim.

## How Reclaim Works

Reclaim is a normal part of the model, not an exceptional path:

1. A worker claims work and receives a lease token.
2. The worker crashes or stops renewing its lease.
3. The lease expires.
4. The runtime releases the work back to `NEW`.
5. Another worker claims it and continues.

That gives you at-least-once claim semantics with fencing for exactly-once completion when workers honor lease tokens.

## Why This Is Not Just A Message Queue

Message queues are optimized for moving messages from producers to consumers.
Agent Spaces is optimized for coordinating stateful workers around shared work.

Key differences:

- agents remain queryable while they are waiting, claimed, completed, or released
- workers can inspect and route by `kind`, `tags`, `metadata`, and trace lineage
- claims, completions, releases, and renewals are part of one lifecycle API
- traces, DAG edges, and event history are first-class parts of the runtime

Use a queue when delivery is the whole problem.
Use Agent Spaces when delivery, coordination, recovery, and visibility all need to line up.

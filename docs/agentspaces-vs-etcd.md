# Agent Spaces vs etcd For Agent Coordination

etcd and Agent Spaces solve different core problems.
etcd is a strongly consistent key-value store.
Agent Spaces is a coordination runtime with first-class claim, lease, release, completion, event, and DAG semantics.

## Where etcd Is Strong

- leader election
- configuration distribution
- service discovery metadata
- low-level compare-and-swap workflows

If your problem is "store a small piece of strongly consistent cluster state," etcd is usually the right primitive.

## Where Agent Spaces Is Strong

- posting structured work for many workers
- atomically claiming work with leases
- reclaiming abandoned work after crashes
- routing work by tags, metadata, and capability
- inspecting lifecycle events and execution lineage

If your problem is "coordinate many workers around shared work and recover safely when they crash," Agent Spaces is the higher-level primitive.

## Practical Differences

| Question | etcd | Agent Spaces |
| --- | --- | --- |
| Built-in work claim lifecycle | No | Yes |
| Lease token fencing for completion | No | Yes |
| Query work by tags and metadata | Limited | Yes |
| Event history for claims/releases/completions | No | Yes |
| DAG and trace retrieval | No | Yes |

## The Tradeoff

With etcd, you assemble coordination semantics yourself out of keys, leases, watches, and transaction rules.
With Agent Spaces, those semantics are already the runtime surface.

That means:

- fewer custom state machines to maintain
- fewer hidden race conditions around ownership
- a cleaner path from "work exists" to "work was claimed, retried, and completed"

## Good Boundary

Use etcd for cluster control-plane state.
Use Agent Spaces for workload coordination state.

They can coexist cleanly in the same system.

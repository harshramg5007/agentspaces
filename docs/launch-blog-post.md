# Launch Blog Post

## Agent Spaces: Coordination Runtime For Leased Work, Crash Recovery, And Multi-Agent Orchestration

Most agent infrastructure does not fail because models are weak.
It fails because the coordination layer around those models is too thin.

Queues move messages.
Workflow engines preserve long-running execution histories.
But agent systems need something slightly different:

- many workers claiming from shared state
- capability-driven routing
- lease-based crash recovery
- queryable state during and after execution
- traces and lineage that survive retries and handoffs

That is the gap Agent Spaces is designed to fill.

## The Problem We Kept Hitting

As soon as a system has more than one worker, the hard questions are no longer just about inference quality:

- who owns this task right now
- how do we safely recover after a crash
- how does another worker discover capabilities
- how do we route statefully instead of round-robin
- how do we inspect what happened without stitching logs and side tables together

Agent Spaces answers those questions through one runtime surface: `out`, `read`, `take`, `in`, `complete`, `release`, and `renew`, all backed by queryable state, events, and DAGs.

## Why This Gets More Valuable As Models Improve

Better models do not remove coordination pressure.
They increase it.

Stronger models make it practical to split work across more specialized agents, run more parallel branches, use more tools, and retry more aggressively.
That means the coordination layer sees more claims, more handoffs, more routing decisions, and more failure recovery paths.

The better the models get, the more valuable a clean coordination primitive becomes.

## The Ecosystem Is Converging On The Same Need

The surrounding ecosystem is moving toward the same coordination problems from several directions:

- [`llm-d`](https://github.com/llm-d/llm-d/releases/tag/v0.5.1) published `v0.5.1` on March 5, 2026, highlighting production serving building blocks for LLM infrastructure.
- The [Gateway API Inference Extension](https://github.com/kubernetes-sigs/gateway-api-inference-extension/releases/tag/v1.4.0) published `v1.4.0` on March 20, 2026, pushing standardized inference routing deeper into cloud-native infrastructure.
- [`kubernetes-sigs/agent-sandbox`](https://github.com/kubernetes-sigs/agent-sandbox/releases/tag/v0.2.1) published `v0.2.1` on March 14, 2026, reflecting growing demand for dedicated agent execution environments.
- [IBM Research published "Agentic Networking: Securing AI Agents on Kubernetes"](https://research.ibm.com/publications/agentic-networking-securing-ai-agents-on-kubernetes) on March 23, 2026, showing how agent coordination is becoming a first-class platform topic.

Those projects attack different slices of the stack.
Agent Spaces focuses on the shared coordination primitive underneath them: leased claims, crash recovery, capability presence, routing, and traceable handoffs.

## What You Can Run Today

- `make smoke`: create, claim, and complete one unit of work
- `make example-fault-tolerance`: watch lease expiry and reclaim
- `make example-queue-fanout`: run 10 workers on one queue with one crash and no double-processing
- `examples/mcp-mesh`: publish tool capabilities with TTL and route MCP requests without a central registry
- `examples/inference-routing`: route requests to the highest-capacity matching model worker

## What Comes Next

The next layer is about making the runtime easier to adopt:

- PyPI publishing for `agent-space-sdk`
- a minimal TypeScript SDK
- more direct observability guidance
- Helm packaging for Kubernetes-first installs

The goal stays the same: make agent coordination simpler, safer, and easier to inspect.

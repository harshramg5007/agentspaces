# Formal Results Blog Series

This page is the public landing page for the formal-results writing track.

The goal is to turn the theory behind Agent Spaces into short, accessible engineering posts rather than one dense paper drop.

## Planned Posts

1. **What Can We Prove About Agent Coordination?**
   Translate the core guarantees into plain language for platform engineers.
2. **Leases, Claims, And Crash Recovery**
   Explain why fenced claims matter and what can be proved after a worker dies.
3. **Why Coordination Gets Harder As Agent Count Grows**
   Connect parallel workers, retries, and routing fanout to the coordination problem.
4. **Causal Identification For Coordination Runtime Behavior**
   Show how the runtime makes coordination traces analyzable instead of opaque.

## Editorial Rules

- lead with operational problems, not formalism
- use one real runtime example per post
- keep proofs as supporting material, not the opening frame
- connect each post back to visible product behavior

## Intended Outcome

Readers should come away with two things:

- a concrete understanding of what the runtime guarantees
- a practical reason to care about those guarantees in production systems

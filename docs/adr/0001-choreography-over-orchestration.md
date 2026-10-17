# 0001. Choreography with an observing state machine
Status: accepted
Date: 2026-10-17

## Context
An order spans inventory reservation, payment and notification, owned by different services. Someone has to know where each order is and trigger compensation when payment fails.

## Options
1. Central orchestrator service (or Temporal). The workflow lives in one place and is easy to read. Adds a component every flow depends on, and services become RPC targets of the orchestrator.
2. Pure choreography. Services react to each other's events. Fully decoupled, but nobody can answer "where is order 42?".
3. Choreography plus an observing state machine in orders. Services react to facts; orders only tracks the saga and records its history.

## Decision
Option 3. It keeps services independently deployable while still giving one place to answer status questions.

## Consequences
- The workflow is spread across handlers. The sequence diagram, the saga history per order and distributed tracing compensate.
- The state machine is monotonic: late, duplicated or reordered events never move an order backwards (property-tested over 10k random sequences).
- Revisit when flows start branching heavily or need timers; that is where an orchestrator earns its cost.

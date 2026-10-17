# 0003. SQLite per service
Status: accepted
Date: 2026-10-17

## Context
Each service needs its own database (no shared tables) with real ACID transactions, because the outbox and inbox patterns depend on them. The project's focus is messaging semantics, and tests must be reproducible on any machine.

## Options
1. Postgres per service. Production-grade and supports multiple replicas. Four extra containers, slower tests, more setup for anyone reading the code.
2. SQLite per service, single writer connection. Zero operations, real transactions, in-process tests.

## Decision
Option 2. It gives the transactional guarantees the patterns need with nothing to run.

## Consequences
- One writer per database matches SQLite's model and serialises writes. That caps write throughput and means one replica per service.
- Schema invariants (`CHECK (available >= 0)`, `CHECK (balance_cents >= 0)`) still hold even with a logic bug.
- Moving to Postgres touches `internal/platform/db` and the SQL dialect; the relay then needs `FOR UPDATE SKIP LOCKED` to run several replicas.

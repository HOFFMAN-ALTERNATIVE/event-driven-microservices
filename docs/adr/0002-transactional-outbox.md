# 0002. Transactional outbox for publishing events
Status: accepted
Date: 2026-10-17

## Context
A service changes its database and must publish an event about it. If it commits and then crashes before publishing, the event is lost and the saga stalls. If it publishes and then the commit fails, other services act on something that never happened.

## Options
1. Publish after commit. Simple. Loses events on crash.
2. Two-phase commit across database and broker. Correct in theory. JetStream does not take part in XA, and 2PC couples availability of both systems.
3. Outbox: write the event as a row in the same transaction; a relay publishes rows in commit order and marks them sent after the JetStream `PubAck`.
4. Change data capture (log tailing). No polling. Needs Debezium-style infrastructure.

## Decision
Option 3. It needs only a local transaction and gives at-least-once publishing; JetStream de-duplication (`Nats-Msg-Id`) and consumer inbox tables turn that into effectively-once effects.

## Consequences
- Worst-case publish latency is the relay interval; `Kick()` after commit removes it for the common path.
- The relay stops at the first failure so events are never reordered, and backs off exponentially.
- The JetStream de-dup window is 2 minutes, so consumers keep their own inbox table as well; each covers a different failure.
- Revisit with CDC if polling load or latency becomes a problem.

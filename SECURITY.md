# Security

## Reporting

Report vulnerabilities privately through GitHub's "Report a vulnerability" button on this repository (Security tab). I aim to reply within 72 hours. Please do not open public issues for exploitable bugs.

## Threat model

- **Assets:** order, stock and balance data; the integrity of the saga (no oversell, no double charge).
- **Actors:** API clients; anything that can publish to NATS; a webhook receiver.
- **Entry points:** service HTTP APIs, NATS subjects, the notifier's outgoing webhook, environment configuration.
- **Top risks and mitigations:**
  - Duplicate or replayed events causing double effects: inbox table per consumer, JetStream de-dup, business guards (one reservation and one payment per order).
  - Malformed or hostile messages: envelopes are validated; malformed ones go to the DLQ instead of crashing a consumer; handler panics are recovered.
  - Oversized or malformed HTTP input: strict JSON decoding with unknown fields rejected and a 1 MiB cap; HTTP server timeouts.
  - Data corruption by logic bugs: `CHECK` constraints on stock and balances.
  - Container surface: distroless, non-root, read-only root filesystem and `no-new-privileges` in compose.

## Not provided

- The service APIs have no authentication. Put them behind a gateway.
- NATS runs without auth or TLS in the demo compose file. In production, enable NATS accounts with per-service publish/subscribe permissions and TLS, so a compromised service cannot publish another service's events.
- `WEBHOOK_URL` is operator-supplied; the notifier will call whatever it points to.

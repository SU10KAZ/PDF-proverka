# Draft rollback runbook — zero-active policy

For the first canary, automatic rollback is forbidden and manual rollback is
allowed only after zero active attempts.

1. Stop new offers and record the incident evidence.
2. Inspect active attempts, EventOutbox, result and cancel state.
3. If any gRPC attempt is active, do **not** start polling. Escalate the
   attempt-specific recovery instead.
4. With zero active attempts, stop the gRPC Agent and verify gRPC ownership is
   released.
5. Restore the known polling config and ownership.
6. Start the polling Agent, verify heartbeat and prove a new polling lease is
   possible only after ownership changes.
7. Record no duplicate attempt before considering the incident closed.

This is future 12F operational guidance; no production process is stopped by
12E.

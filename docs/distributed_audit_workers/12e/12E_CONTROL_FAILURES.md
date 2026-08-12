# Control and Center failure evidence

- C01: `test_12e_gateway_graceful_stop_recovery_is_persistence_only` passed
  against a clean, loopback-only Gateway process. It waits for a running
  Executor, sends process-scoped SIGTERM, observes a higher durable epoch on a
  replacement process, then verifies completed result, retention and unique
  event sequences.
- C02: the sibling SIGKILL test passed with the same persistence assertions.
  It previously exposed the stale request-iterator race; commit `043a28f4`
  fences an obsolete gRPC iterator by connection epoch.
- C03/C04: 12B scheduler/offered-attempt persistence and expiry recovery
  regressions passed in the completed 12B suite. Exact process crash
  instrumentation is still required before a final PASS.
- C05: duplicate/gap event tests pass locally, but the exact persisted-event /
  lost-ack process window remains pending.
- C06: the 12B result validation/lost-ack reconnect regression passed locally.

No item here used production `:8081`, the production DB, a tunnel or a public
test listener.

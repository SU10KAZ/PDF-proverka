# Immutable adversarial review

The review is performed against the commit candidate and repeated after any fix. Required lenses and acceptance criteria:

1. HTTP/domain semantic reuse — no second scheduler/lifecycle/event/result implementation.
2. Claim/offer crash windows — lease is durable before send and expires safely.
3. EventOutbox/exactly-once — persistence/cursor remain authoritative and replay safe.
4. Result ACK/retention — ACK follows persisted validation and is recoverable.
5. Connection epoch/reconnect — strict greater-only durable fence, stale cleanup cannot win.
6. Security/public-bind/no-RCE — loopback guard, no 8443, bounded semantic validation, no executable payload.
7. Concurrency/backpressure — bounded queue/transport/batches, 20 streams and burst events pass.

Review evidence is the committed diff, generated descriptor reproducibility, the A–BO integration matrix, polling regressions, stress/backpressure tests, `git diff --check`, listener inspection, and secret-pattern inspection. Final disposition is recorded after the candidate review; no push or merge is part of this task.

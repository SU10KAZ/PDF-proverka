# Immutable adversarial review

The review is performed against the commit candidate and repeated after any fix. Required lenses and acceptance criteria:

1. HTTP/domain semantic reuse — no second scheduler/lifecycle/event/result implementation.
2. Claim/offer crash windows — lease is durable before send and expires safely.
3. EventOutbox/exactly-once — persistence/cursor remain authoritative and replay safe.
4. Result ACK/retention — ACK follows persisted validation and is recoverable.
5. Connection epoch/reconnect — strict greater-only durable fence, stale cleanup cannot win.
6. Security/public-bind/no-RCE — loopback guard, no 8443, bounded semantic validation, no executable payload.
7. Concurrency/backpressure — bounded queue/transport/batches, 20 streams and burst events pass.

Candidate reviewed: `9ad71d41d5f438ca9b76dd225497d8868ec159b7`.

Findings from the first immutable pass were fixed before re-review:

- config parsing now rejects malformed booleans/versions with `GatewayConfigError`;
- environment names are allowlisted, so a production-name typo cannot bypass the guard;
- health and metrics cannot be disabled and reflection cannot claim an unimplemented mode;
- log level is typed and actually applied at the single startup boundary;
- IPv6 loopback bind formatting is correct;
- a replacement epoch clears stale durable heartbeat time;
- `ResultReady` now validates transfer id, HTTPS protocol, direction, package type, size, and hash before domain use.

Re-review disposition: all seven lenses PASS. Evidence is the committed candidate diff plus the explicit fix diff, unchanged generated descriptor SHA-256, 92 passing 12A/12B tests, polling regressions, 20-stream stress, burst backpressure, `git diff --check`, listener inspection with no `:8443`, and secret-pattern inspection. No push or merge was performed.

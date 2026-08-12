# 12E test report — interim

Executed in the isolated worktree/test environment (no production backend,
production DB, `.31` production process, tunnel or provider inference):

| Suite | Result |
| --- | --- |
| 12A protocol descriptors | 45 passed (after adding test-only `grpcio-tools` to `/tmp/12e-grpc-venv`) |
| 12B Gateway | 47 passed |
| 12C real Agent gRPC | 33 passed |
| 12D mTLS/security | 27 passed |
| 12E unit reliability | 6 passed |
| 12E process Gateway C01/C02 | 2 passed |
| Worker hardening | 70 passed |
| Center/polling E2E | 34 passed |
| Worker agent | 38 passed |
| 11K bootstrap | 31 passed |
| 11L bootstrap | 3 passed |

Two stale fixed-number schema assertions were discovered in legacy tests during
this work and corrected to assert the authoritative `SCHEMA_VERSION`; both
preserve the substantive old-DB migration test. The first real C02 failure was
a stale gRPC request iterator consuming post-reconnect work; it has a direct
regression and was fixed in commit `043a28f4`.

A subsequent planned combined regression launch was rejected by the execution
environment before it ran; no partial output from that run is counted. The
remaining suites and physical phase are pending.

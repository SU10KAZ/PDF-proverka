# 12F.1 rollback plan

No rollback was executed because Phase A made no production mutation.

The intended Phase B rollback boundary is Center-only:

1. fail health/core routes/distributed routes/scheduler/polling acceptance;
2. stop only the newly started exact Center process;
3. restore the verified previous configuration backup;
4. start the immutable previous Center release on `127.0.0.1:8081`;
5. verify health and core APIs;
6. retain the separate `workers.db` without deleting or downgrading it;
7. verify that no distributed job was created or leased.

This is not yet executable. The current backend has no dedicated service and
the exact in-memory code is not reproducible from commit `9168c393...` or the
current disk. A known-good immutable rollback release plus supervised launcher
must be prepared before restart authorization.

The current polling Agent is already unreachable because its dispatcher DNS
is stale. That pre-existing condition cannot be used as evidence that a future
candidate caused or fixed polling connectivity.

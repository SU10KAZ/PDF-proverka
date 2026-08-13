# 12F.1E immutable adversarial review

Review target: exact commit
`775f66b78eb5674ab1251c76f00d84bbddb9fb8b`, tree
`9f01d55607d98f4630f6554bd9d1db5f2898585b`, parent actual production commit
`f6ca2ca932db52ba0d5166ae3816fc1dc1d9b682`. The mutable canary worktree was
not the review target.

1. **Arbitrary Worker-ID takeover — PASS.** Generic request has no ID field;
   separate completion takes authority from the stored ADMIN pair. Empty DB has
   no bypass.
2. **Worker↔instance binding — PASS.** Stored pair and request pair must match;
   both conflict directions reject without reassignment.
3. **Enrollment token lifecycle — PASS.** 256-bit random source, 300-second
   default, bounded 30–3600, hash-only, constant-time, typed state, single-use,
   revocable, rate-limited and non-oracular.
4. **Runtime credential lifecycle — PASS.** New token only after validation,
   hash-only Center storage, first response only; old token not read/imported.
5. **Transaction/crash safety — PASS.** Worker, polling ownership, token hash,
   authorization consumption and event are one write transaction; three fault
   seams fully rolled back.
6. **Idempotency/lost response — PASS.** Exact retry returns safe completed
   state and no second credential. Existing ADMIN rotation is explicit recovery.
7. **RBAC/audit — PASS.** Creation/revocation ADMIN-only; viewer, operator,
   machine and anonymous denied. Typed secret-free events are persisted.
8. **Secret leakage/backward compatibility — PASS.** No real credentials in
   DB/log/events/docs/argv. Generic registration and existing token/heartbeat/
   bootstrap tests pass. Migration 13 is additive.

Main questions:

- Can an unauthorized Worker claim `wrk_19c87718`? **NO.**
- Can a correctly authorized installation substitute another Worker ID? **NO.**
- Can replay mint an uncontrolled second credential? **NO.**

Security verdict: **PASS**. Deployment verdict is separate and remains
**BLOCKED** because production/main baselines changed externally during the
review window and require an operator re-baseline decision.

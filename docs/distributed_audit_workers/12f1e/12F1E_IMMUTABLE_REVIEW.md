# 12F.1E immutable adversarial review

Review target: exact commit
`e6015d33bf4fa6b8986a21fa4b9e33c10ec3139f`, tree
`00e75c545eb314ff0bac82990b9ad5f11050e476`, parent actual production commit
`e2b98c3b5b290f97257ddcb914e71a65289c26bb`. The mutable canary worktree was
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
**BLOCKED** because production/main/final-candidate baselines changed externally during the
review window and require an operator re-baseline decision.

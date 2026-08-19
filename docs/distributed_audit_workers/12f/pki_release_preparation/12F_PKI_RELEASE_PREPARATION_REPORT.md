# 12F PKI shared-state fix production release preparation

## Verdict

**PKI FIX RELEASE PREPARATION: PASS**

**READY_FOR_PKI_FIX_DEPLOY: YES**

This is technical readiness only. `operator_deploy_authorization=false`, so
production deployment/restart and the sudo preparation retry remain forbidden
until new explicit authorization.

## Final requested facts

1. Verdict: **PASS / READY_FOR_PKI_FIX_DEPLOY**.
2. Current production: commit `e6015d33bf4fa6b8986a21fa4b9e33c10ec3139f`,
   PID `2379476`, active, NRestarts `0`, `/api/info` 200.
3. New PKI fix commit: `5e3750a26f65a884c1c644c6bc25c490f4b665ef`.
4. Parent: exact production commit `e6015d33bf4fa6b8986a21fa4b9e33c10ec3139f`.
5. Changed runtime/build files: **7**; one additional regression test.
6. Unexpected runtime files: **0**.
7. Release: `pki-shared-5e3750a2` at
   `/home/coder/auditmanager/releases/pki-shared-5e3750a2`.
8. Tree: `d9a9c42732337b567d4fe7efe6c339e0612dcc3c`.
9. Bundle SHA-256:
   `2ecf5842b4b052bb7134fa3c429775143c51ba32fa627e8837b74ff7d9e7046b`.
10. Shared-state fix included: **YES**.
11. Identity-preserving re-enrollment preserved: **YES**.
12. DB schema impact: **none**, schema 13 -> 13; no migration.
13. PKI persistent state: `/etc/auditmanager/pki`; Worker registry state:
    `/var/lib/auditmanager/distributed_workers`; both release-independent.
14. Existing partial CA reusable: **YES**; production fingerprints remain the
    previously validated root/issuer/server fingerprints.
15. Duplicate CA risk: **fail-closed / no duplicate observed**. The new script
    is preservation-only and rejects incomplete groups.
16. Future issuer ExecStart:
    `/opt/auditmanager/releases/pki-shared-5e3750a2/venv/bin/python -m backend.app.security.issuer_service`.
17. Mutable source-root dependency: **NO**.
18. Isolated backend boot: **PASS**, API/OpenAPI 200, schema 13, shared modes
    `2770/0660`, scheduler disabled.
19. Isolated issuer start: **PASS**, two exact-release starts, exit `0/0`.
20. Isolated signing E2E: **PASS**, ACTIVE Worker certificate.
21. Partial retry idempotency: **PASS**, eight PKI files preserved and one DB
    certificate row for repeated identical request.
22. Rollback dry run: **PASS**, `e6015d33 -> 5e3750a2 -> e6015d33`, logical DB
    and registry preserved.
23. Relevant test count: **385 PASS**, 0 FAIL on final content; 100 were
    repeated directly from the read-only durable artifact.
24. Secret leaks: **0**.
25. Immutable review: **PASS**; detached archive byte-match, verified bundle,
    canonical read-only modes.
26. Production backend restarted: **NO**.
27. Production DB mutated: **NO**.
28. Production issuer started: **NO**.
29. Production Gateway started: **NO**.
30. Worker `.31` changed: **NO**; Agent PID `2212836`, Executor PID `1384880`.
31. Provider inference Claude/Codex/OpenRouter: **0/0/0**.
32. New preparation script SHA-256:
    `89129b4445e120a636fd644ea89dce800ae69d1cf26d85f8940cf2b039bd360d`.
33. Script expected-release guard: **YES**; it requires release/commit,
    running backend cwd and explicit backend shared-state activation before the
    first mutation. Current production intentionally fails the guard.
34. `READY_FOR_PKI_FIX_DEPLOY = YES`.
35. Technical blockers: **none**. Authority blocker: explicit production
    deploy/restart authorization is absent by design.
36. Future deploy needs explicit operator approval: **YES**.
37. Evidence commit: the local commit containing this report, reported at
    handoff.
38. Push/Merge: **NO/NO**.

No production deploy, sudo preparation, Worker certificate issuance, Gateway
start, transport switch or canary was performed. Stop at this gate.

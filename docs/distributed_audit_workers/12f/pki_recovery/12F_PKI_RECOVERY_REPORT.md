# 12F production PKI recovery report

## Verdict

**RECOVERY VERDICT: BLOCKED**
**READY_FOR_PKI_OPERATOR_RETRY: NO**

The root cause is exact, all existing production PKI material is valid and
preserved, and the code-level launcher/shared-state defect passes isolated
regression and signing E2E. Production retry is nevertheless unsafe because
the active immutable backend `e6015d33` does not contain the shared-state fix.
Deploying that fix requires a new immutable release and a separately authorized
controlled production backend restart, which this recovery pass explicitly did
not authorize.

## Requested facts

1. Exact root cause: issuer UID could not traverse root-owned `0700`
   `/etc/auditmanager[/pki]`; after that fix the active backend would still
   collapse shared DB ACLs through repeated `0700/0600` chmod.
2. PYTHONPATH/WorkingDirectory root cause: **NO**. Journal proves the module was
   imported from the exact immutable release before the permission exception.
3. Existing root CA: **YES**, valid, key pair matches.
4. Existing issuing CA: **YES**, valid, key pair and root signature match.
5. New CA generated during recovery: **NO**.
6. Root fingerprint:
   `52EBD11F74F5FA862349A01E309CDBA6C94C766E4E4B57F6CCF0C3BF4FC734C6`.
7. Issuer fingerprint:
   `E6784E3AC885E2986021F25E7298F9E56EEA4C6F007831001A22E1BAD637B6B5`.
8. Private key contents exposed: **NO**.
9. Gateway access to CA signing key: **NO**; issuer key remains issuer-owned
   `0600`.
10. Unit defect: caller-independent module root was not canonical, `/etc`
    traversal was wrong, and the DB was modeled as single-owner state despite
    three cooperating service identities.
11. Exact code fix: explicit immutable `WorkingDirectory`/`PYTHONPATH`, strict
    opt-in shared state (`2770/0660`, ACL-mask preserving, optional exact GID),
    fail-closed path checks, and read-only complete-PKI validation.
    Local recovery fix commit:
    `f0a293f3d6c49efeea700b4acdd0f443e56a1b66` (not pushed or merged).
12. Immutable isolated runtime: read-only temporary release copy; arbitrary
    caller cwd; mutable checkout absent from child `PYTHONPATH`.
13. Launcher regression: **8/8 PASS**.
14. Isolated issuer start: **PASS**.
15. Isolated signing E2E: **PASS**.
16. Partial-state retry idempotency: **PASS**; identical CA files and one
    registry row after repeat.
17. Related regression: **100 PASS**, one unrelated deprecation warning.
18. Broad suite: 1292 PASS, 1 SKIP, 3 pre-existing current-branch failures
    outside the changed files (one stale import expectation; two polling
    multi-process timeouts reproduced separately).
19. Production issuer: loaded/enabled, **inactive/dead**, PID 0; not started by
    recovery.
20. Gateway unit: installed and disabled; Gateway inactive; `:8443` absent.
21. Backend: PID `2379476`, active, NRestarts 0, HTTP `127.0.0.1:8081` = 200.
22. Polling Agent `.31`: PID `2212836`, active, NRestarts 0.
23. Executor `.31`: PID `1384880`, active, NRestarts 0.
24. Jobs/attempts/offers: `0/0/0`; active Worker attempts/processes/outbox/
    commands: `0/0/0/0`.
25. Provider inference Claude/Codex/OpenRouter: **0/0/0**.
26. Script changed: **YES**. The previous mutating script was replaced with a
    fail-closed guard, SHA-256
    `0cde6ea58f82ac4a15b71207dcc2f593578cc8158e779c9209a8785d1ba95550`.
27. Operator command: **NONE while blocked**. The guard exits 78 and makes no
    changes. A later approved successful action must end in
    `12F_PKI_PREP_PASS` and write `/tmp/12f_pki_prepare_receipt.json`.
28. Production changed by this recovery pass: **NO**.
29. Production certificate issued: **NO**. Gateway/canary/Worker cutover: **NO**.
30. Push/Merge: **NO/NO**.

## Exact blockers

- Rebase/apply the reviewed recovery fix onto exact active commit
  `e6015d33bf4fa6b8986a21fa4b9e33c10ec3139f` rather than deploying the much
  newer feature-worktree tree wholesale.
- Build and review a new immutable release.
- Obtain explicit authorization for its controlled production backend restart
  and typed shared-state activation.
- Only then generate a hash-bound idempotent root action that preserves the
  recorded CA fingerprints and starts only the protected issuer.

12F canary remains stopped.

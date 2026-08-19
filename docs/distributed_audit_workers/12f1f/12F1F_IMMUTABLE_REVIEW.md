# 12F.1F immutable security and release review

Exact target: baseline `e2b98c3b5b290f97257ddcb914e71a65289c26bb`
(tree `e2d67c5c4b5b62b3921bff6443cbbe7d52f9a72a`) plus its direct child
`e6015d33bf4fa6b8986a21fa4b9e33c10ec3139f`
(tree `00e75c545eb314ff0bac82990b9ad5f11050e476`). The mutable canary branch was
not the code-review target.

1. **Patch ancestry — PASS.** `e6015d33^` is exactly the accepted production
   baseline. No rebase or SHA churn is justified.
2. **Patch scope — PASS.** Eleven files are limited to typed re-enrollment
   runtime/schema/RBAC/tests. Unexpected files, UI changes, block-vector code,
   dependency manifests, DBs, logs, and production values: zero.
3. **Arbitrary Worker-ID takeover — PASS.** Generic registration has no
   Worker-ID field and continues to use a random Center-generated ID. The
   separate machine command has no `--worker-id`; it repeats the IDs stored in
   local historical state and must match the ADMIN-owned authorization.
4. **Exact Worker↔instance binding — PASS.** Both stored values must match.
   Existing Worker/other-instance, instance/other-Worker, and inconsistent
   registry states fail closed without reassignment.
5. **Token lifecycle — PASS.** The authorization token has 256-bit random
   source, 300-second default, 30–3600 bounds, single use, hash-only storage,
   constant-time comparison, durable rate limiting, revocation, and no
   state/token oracle in the machine response.
6. **Runtime credential — PASS.** A new credential is generated inside the
   completion transaction, only its hash is stored, and the old Worker token is
   never read or imported. An exact lost-response retry cannot mint or replay a
   credential and instead requires explicit ADMIN rotation.
7. **Transaction/idempotency — PASS.** Worker identity, conservative polling
   ownership, runtime-token hash, authorization consumption, and audit event
   commit in one SQLite write transaction. Three fault seams roll back.
8. **RBAC/audit — PASS.** Authorization creation and revocation require ADMIN
   plus intent. Viewer/operator/machine/anonymous callers are denied. Events
   are typed and have no credential/hash columns.
9. **Schema/rollback — PASS.** Migration 13 only adds two tables. Fresh,
   populated-v12, and production-snapshot copies migrate successfully. The
   e2b98c3b schema-12 application opens and reads the migrated v13 copy.
10. **Release immutability — PASS.** The durable app is byte-identical to the
    exact archive; dependency freeze and bundle hashes match; app/venv/files
    are read-only; current production still points to `ui-e2b98c3b`.

No runtime/security defect was found. The existing patch is
**REUSABLE_AS_IS**. Security verdict: **PASS**.

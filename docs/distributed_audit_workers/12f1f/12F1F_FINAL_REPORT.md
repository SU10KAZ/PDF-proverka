# 12F.1F final production baseline freeze and patch deploy gate

## Verdicts

- **12F.1F BASELINE FREEZE: PASS.** A new 20-minute window produced 21
  one-minute samples with zero source changes, release changes, or restarts.
- **AUTHORITATIVE PRODUCTION BASELINE:**
  `e2b98c3b5b290f97257ddcb914e71a65289c26bb`, tree
  `e2d67c5c4b5b62b3921bff6443cbbe7d52f9a72a`, durable release
  `/home/coder/auditmanager/releases/ui-e2b98c3b`.
- **REENROLLMENT PATCH: REUSABLE_AS_IS.** Patch
  `e6015d33bf4fa6b8986a21fa4b9e33c10ec3139f` has the authoritative baseline
  as its direct parent and tree
  `00e75c545eb314ff0bac82990b9ad5f11050e476`. No new patch/rebase was made.
- **PATCH SECURITY: PASS.** Generic registration remains Center-random;
  arbitrary Worker-ID claim is impossible; exact pair, ADMIN-only intent,
  hash-only single-use TTL token, atomicity, idempotency, and new credential
  lifecycle are preserved.
- **PATCH DEPLOY: READY.** `READY_FOR_REENROLLMENT_PATCH_DEPLOY = true`.
  This is technical readiness only; `operator_patch_deploy_authorization =
  false`, so production deployment/restart remains forbidden.
- **PRODUCTION: UNCHANGED.** PID `2281536`, `NRestarts=0`, release
  `e2b98c3b`, health 200, DB unchanged, Worker `.31` untouched, Gateway absent,
  provider runtime inference `0/0/0`.

## Freeze and exact-patch proof

External source writers/deployers were `0/0` before the window and at the
final freeze guard. From `13:49:42` through `14:09:42 MSK`, final-candidate
HEAD/tree/status, production PID/restart count, current symlink, release
manifest, and HTTP health remained identical. The dirty main development root
was read-only and does not define production identity.

`e6015d33^` equals `e2b98c3b`; its bundle SHA-256 remains
`1a94337d444ea977d73e1be164f6e2d77e10beeff472905e242e9b706726e7f6`.
The patch changes 11 expected files and zero unexpected files. Dependency
manifests are unchanged. The exact app, pinned venv, manifest, and verified
bundle are materialized as the inactive durable release
`/home/coder/auditmanager/releases/reenrollment-e6015d33`; production `current`
still points to `ui-e2b98c3b`.

## Validation and rollback

12F.1F completed **272 exact-patch tests PASS, 0 FAIL**, kept separate from the
historical **443 PASS** in 12F.1E. Real isolated uvicorn boots, route mounting,
schema 13 initialization, exact identity re-enrollment, authenticated polling
heartbeat, conflict rejection, feature-off behavior, RBAC, and provider
isolation all passed. Secret leaks: zero.

Live production DB remained schema 12 and read-only. Its isolated snapshot and
a separate populated v12 fixture migrated additively to 13; the e2b98c3b
schema-12 application opened and read the migrated copy. Immediate rollback is
the immutable `e2b98c3b` release. Deep fallback
`46bcd527065aff074ca5fe730e3594e982d080d2` remains available.

Push/Merge: **NO/NO**. Evidence commit is the commit containing this report and
is reported at handoff. Stop here; no production deploy was performed.

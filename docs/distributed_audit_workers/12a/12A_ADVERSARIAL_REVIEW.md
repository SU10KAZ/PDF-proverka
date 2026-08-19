# 12A — immutable adversarial review

## Candidate 1

- Commit: `4b641b303ef1dd5fa7008fda464dc2907aa1c481`
- Detached worktree: `/tmp/12a-review-4b641b30`
- Candidate was not modified during review.

Read-only results:

1. Existing HTTP semantic parity — PASS: all 19 router actions represented; DTO/state/capability/job parity tests pass.
2. Exactly-once/EventOutbox — PASS: disk JSONL + SQLite sequence allocator retained; EventBatch sequence and highest-contiguous ACK remain separate from stream sequence.
3. Reconnect/resume — PASS: active attempts, written/acked cursors, center resume cursors and greater epoch are represented; disconnect does not fail job.
4. Evolution/versioning — PASS: v1 package, explicit negotiation, enum zero, reserved ranges, critical descriptor snapshot and reproducible descriptor.
5. Security/no-secret/no-RCE — **FAIL**: descriptor contained no RCE/secret message, but generic CanonicalJson adapter accepted an explicit `command` key. Downstream current Pydantic DTO would reject it, but defense-in-depth at the adapter boundary was incomplete.
6. Control/data/admin boundary — PASS: no runtime source changed; no package bytes/URL, runtime grpc dependency, stub/listener or admin message.

## Fix

Commit `095353c9dd6a9422987a81e8428ab05bb36c36e8` adds exact executable/admin-key rejection to bounded CanonicalJson, keeps typed `command_id` valid, documents mandatory downstream domain validation, and adds a regression test.

## Candidate 2

- Commit: `095353c9dd6a9422987a81e8428ab05bb36c36e8`
- Detached worktree: `/tmp/12a-review-095353c9`
- Candidate remained clean/detached throughout review.

All six lenses repeated from zero: **PASS / PASS / PASS / PASS / PASS / PASS**. Full required protocol suite in detached worktree: **45 passed**. Security probes independently rejected `api_key`, `command`, `argv`, and `install_package`, accepted typed `command_id`, and descriptor traversal found no credential fields. Plane review confirmed no change below `backend/` or `audit_worker/`, no runtime grpc dependency/stub, and no bytes/URL/endpoint/credentials field in `PackageTransferDescriptor`.

Review used no socket, production access, provider inference, push or merge.

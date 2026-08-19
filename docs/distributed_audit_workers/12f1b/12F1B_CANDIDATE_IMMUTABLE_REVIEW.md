# 12F.1B captured candidate immutable review

- Commit: `965337f11382b8bf0bac0ed81c560eff24a83cdb`
- Tree: `378fe8702261a4a67c2f8699269010db264a2484`
- Release: `/tmp/12f1b-releases/candidate-965337f1`
- Manifest SHA-256: `e66fd983656feb0081bf86946a997c2e271ac6183daf7bbd299bc3ee6b468df2`
- Worktree clean; `git show --check`/`git diff --check` passed.
- Release files are read-only; no writable regular files were found.
- No live `.env`, DB, log or real private key is tracked. Private-key-like
  scan hits were dummy negative-test/redaction markers only.
- Release-specific dependencies include grpcio/protobuf and contain 132
  frozen entries.
- Distributed execution, real inference and new worker intake are disabled by
  default.
- Worker state is typed and external at
  `/var/lib/auditmanager/distributed_workers`; isolated proof used `/tmp`.
- Reusable bootstrap-secret fallback was removed. Registration uses a
  one-time, TTL, instance/session-scoped, hashed-at-rest token.
- Human drain/resume is role-gated, durable and audit-logged atomically with
  the intake state transition.

Documented integration resolutions retained the production removal of the old
model-control page, retained the newer production Codex transient retry, and
did not resurrect the deleted model-control service.

Review verdict: `PASS_AS_IMMUTABLE_CAPTURED_CANDIDATE`, but
`NOT_DEPLOYABLE_UNTIL_REBASED_AND_RETESTED` because the production baseline
continued changing afterward.

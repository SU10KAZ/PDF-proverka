# 12F.1A actual production dirty inventory

Captured read-only at `2026-08-12T18:04:34+03:00` from
`/home/coder/projects/PDF-proverka`, HEAD `9168c393...`.

The `23 tracked + 20 untracked` task context is preserved exactly as the
`context_core_43` set in the JSON inventory. The complete current worktree is
larger:

- 59 tracked modified/deleted files;
- 41 untracked files;
- 100 paths total;
- 75 `SOURCE_CODE`, 2 `CONFIG_TEMPLATE`, 2 `OPERATOR_SCRIPT`, 20
  `GENERATED_RUNTIME`, 1 `PRODUCTION_LOCAL_CONFIG`;
- zero path-category `UNKNOWN` entries.

Two implementation paths appeared after the original 43-file snapshot:
`backend/app/services/stage_comparison/semantic_diff.py` and
`backend/tests/test_semantic_diff.py`. They make the current core set
`23 tracked + 22 untracked` and are not silently folded into the historical
count.

For every tracked entry the inventory records the HEAD blob, current SHA-256,
hash of its patch, size and metadata. For every untracked file it records type,
mode, size and SHA-256. No live `.env` content, credential, token, private key,
certificate key or provider-auth value was emitted.

## Runtime/disk split

Production PID `1968811` started at `17:31:06+03:00`. Afterwards:

- `stage_comparison.py` and `store.py` changed at `17:56:23`;
- `semantic_diff.py` was added/edited through `18:02:50`;
- `test_semantic_diff.py` was added at `17:57:02`.

The running process cannot have loaded the current `stage_comparison.py` and
`store.py` bytes at startup. The prior `store.py` source was overwritten, so
the in-memory application cannot be reproduced from the current disk snapshot.
This is the baseline-equivalence hard blocker, not a file-category ambiguity.

The restricted forensic material is outside Git under
`/tmp/12f1a-forensic-20260812T180400/`; docs record hashes only. It is evidence
of the stable disk point, not an immutable production rollback release.

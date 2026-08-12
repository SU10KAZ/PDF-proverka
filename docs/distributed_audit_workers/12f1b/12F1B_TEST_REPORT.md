# 12F.1B test report

The immutable captured baseline and candidate were tested in isolated worktrees
and release directories. No provider inference, real project, production DB or
production process was used.

| Scope | Result |
| --- | --- |
| Four post-start semantic review | 14 passed |
| Captured baseline regression with production-compatible synonym configuration | 264 passed, 2 stale assertion failures, 1 skipped |
| Candidate broad AuditManager sweep, including targeted correction of the one stale model-control expectation | 1043 passed, 1 skipped |
| Polling/routes/persistence/bootstrap/role/E2E group | 199 passed |
| 12A–12E gRPC/mTLS/chaos regressions | 169 passed |
| Changed AuditManager contract group | 57 passed |
| 12F.1B candidate acceptance | 11 passed |
| JavaScript syntax | passed |
| Python compile check | passed |
| `git diff --check` for immutable commits | passed |

The selected groups overlap, so their numbers are deliberately not summed.
The two baseline failures are old expectation assertions retained to preserve
the exact captured disk tree; the corresponding candidate behavior was
adapted and passed. This does not qualify the current production disk because
seven later release-scope deltas were not part of these immutable artifacts.

Provider calls: `Claude/Codex/OpenRouter = 0/0/0`.

Final test gate for the current disk: **BLOCKED**, not failed product behavior.
The moving source must first be frozen, reviewed, integrated and retested.

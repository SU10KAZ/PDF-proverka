# 12F.1C final test report

All final acceptance suites were repeated from the exact sealed release trees.
No real audit/project or provider inference was run.

| Scope | Result |
| --- | --- |
| Final baseline isolated sealed boot | PASS, HTTP 200 |
| Final candidate isolated sealed boot | PASS, HTTP 200; unauthenticated admin 401 |
| Production-core broad regression | 1687 passed, 14 skipped, 11 deselected |
| Stage-comparison/block-vector/semantic diff | 1527 passed, 57 skipped |
| `semantic_diff_v6a2` direct unit group | 8 passed |
| Development worktree guard | 5 passed; production-root mutation intent exits 2 |
| Polling/routes/persistence/role/bootstrap/E2E | 181 passed |
| 12B/12C/12D/12E process/12F acceptance critical | 122 passed |
| 11K/11L/12A/12E reliability | 90 passed |
| JavaScript syntax and `git diff --check` | PASS |
| Candidate→baseline rollback | PASS; DB hash preserved |

The suite counts overlap and are not summed. The broad core selection excludes
two modules that require non-Git real-project datasets (running them would
violate this task's real-project prohibition) and 11 unrelated stale Critic-v2
assertions identified by a full diagnostic. The unfiltered diagnostic was
`1767 passed, 14 skipped, 32 failed`: 22 failures were those missing external
datasets, and 10 were date/UI/mock/safety-contract assertions outside the
12F.1C changed scope. Mandatory production-core, stage-comparison and
distributed-worker acceptance groups are clean.

Provider calls: `Claude/Codex/OpenRouter = 0/0/0`.

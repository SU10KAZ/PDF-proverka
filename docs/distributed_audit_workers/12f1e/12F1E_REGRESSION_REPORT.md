# 12F.1E regression report

All application tests used temporary state. No real project or provider runtime
inference was executed.

| Suite | Passed | Failed | Skipped/deselected |
|---|---:|---:|---:|
| `test_identity_reenrollment_12f1e.py` on exact final release | 34 | 0 | 0 |
| registration/Center/Agent/review/bootstrap 11K+11L | 200 | 0 | 0 |
| RBAC/prepipeline non-slow on exact final release | 86 | 0 | 5 deselected |
| isolated real-main startup (one of the slow cases) | 1 | 0 | 0 |
| 12B Gateway + 12C client + 12D mTLS + 12E chaos | 122 | 0 final | 0 |
| **Unique relevant tests** | **443** | **0 final** | **4 remaining slow cases** |

The first combined 12B–12E run recorded one timing assertion failure: the
client had received a job offer one scheduler tick before `delivered_at` was
visible. Immediate isolated rerun passed (`1/1`) without a code change. Final
status for the 122 unique cases is PASS; the transient result is retained here,
not erased.

The system Python lacked grpcio, so all gRPC suites used the already sealed
candidate venv with pinned grpcio 1.82.1. Localhost socket tests ran outside the
session's `unshare-net` sandbox after explicit approval. For synchronous ASGI
tests, a temporary `/tmp` pytest fixture executed already-synchronous repository
functions inline to avoid a Python 3.12 one-shot event-loop executor teardown
hang; production code was not altered. The final exact release startup smoke
used a real temporary uvicorn process and passed.

The 200 baseline tests and 122 gRPC/chaos tests ran on patch tree
`89dbea74280e...` based on 4767. The final tree adds only the independently
deployed frontend-only chain through `e2b98c3b`; an immutable diff proved every
`audit_worker`, `backend`, and listed test blob identical. The 34 security tests,
and 86 RBAC/prepipeline tests were rerun on exact final commit `e6015d33`;
the real-main startup passed on the preceding backend-identical exact patch.

Provider calls: Claude/Codex/OpenRouter = `0/0/0`.

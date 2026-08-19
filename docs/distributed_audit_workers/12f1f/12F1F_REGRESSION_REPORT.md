# 12F.1F exact baseline/patch revalidation

Review target: immutable patch `e6015d33bf4fa6b8986a21fa4b9e33c10ec3139f`
(tree `00e75c545eb314ff0bac82990b9ad5f11050e476`) directly on the accepted
production baseline `e2b98c3b5b290f97257ddcb914e71a65289c26bb`.
Tests ran from an exact `git archive` in `/tmp`, with temporary state and the
pinned baseline virtual environment. No production DB, Worker, real project,
or provider runtime was used.

## 12F.1F completed suites

| Suite | Passed | Failed |
|---|---:|---:|
| Identity re-enrollment security/E2E | 34 | 0 |
| Distributed-worker hardening | 70 | 0 |
| Worker bootstrap 11K | 31 | 0 |
| Worker bootstrap 11L | 3 | 0 |
| RBAC/audit/intent subset | 17 | 0 |
| Feature-off/startup contour | 16 | 0 |
| Center registry/API | 31 | 0 |
| Agent/local security | 38 | 0 |
| Prior immutable review fixes | 27 | 0 |
| Execution flag/provider isolation hard gates | 5 | 0 |
| **12F.1F completed total** | **272** | **0** |

The count includes only runs that produced a final pytest summary. It does not
include two aggregate attempts that completed visible cases but then waited in
the known Python 3.12 synchronous-ASGI/default-executor teardown condition.
Those isolated trees were stopped and replaced with the completed per-file
runs above. The temporary `inline_db_plugin` used in those completed runs calls
already-synchronous SQLite repository functions inline; it changes no
application or release source. The exact re-enrollment suite itself passed
without this plugin.

In addition, two real isolated uvicorn boots passed. The first proved the
operator contour fails closed without portal auth. The second used the
documented localhost-only development opt-in, mounted both Worker and ADMIN
routes, returned core/status/list HTTP 200, initialized schema 13 in `/tmp`,
kept distributed audit execution disabled, and shut down cleanly.

## Historical evidence kept separate

12F.1E previously recorded **443 unique relevant tests PASS**, zero final
failures, and four remaining slow cases. That is historical evidence and is
not added to the 272 tests above.

Provider runtime inference caused by 12F.1F: Claude/Codex/OpenRouter =
`0/0/0`. Verdict: **PASS**.

# Staging Soak — Start Report

**Date:** 2026-05-20
**Status:** Staging backend **UP** on port 8083 with `STAGE01_DEDUP_ENABLED=true`. Production untouched on port 8082.

---

## 1. Staging instance

| Item | Value |
|---|---|
| Hostname | `andrewuzun3.hlab.kz` |
| Staging PID | **3120881** |
| Staging port | **8083** (HTTP 200 on `/docs`, 182 API routes loaded) |
| Staging stdout/stderr log | `/tmp/staging_backend.log` |
| Code revision | `main` @ `f50c3ae` (merge commit) → Phase 0 commit `ade03f7` parent |
| Working directory | `/home/coder/projects/PDF-proverka/` |
| Python interpreter | `/usr/bin/python` (same as production) |
| Uvicorn entry | `backend.app.main:app` |
| Detach mode | `nohup ... &` (PPID=1; survives shell exit) |

### Env vars set on staging process (verified via `/proc/3120881/environ`)

```
STAGE01_DEDUP_ENABLED=true
STAGE01_DEDUP_FUZZY_THRESHOLD=0.7
```

All other env vars come from the shared `.env` file (read-only inheritance:
`PAID_API_ENABLED`, `OPENROUTER_API_KEY`, etc. — these load the same way for
both instances; staging does NOT have its own `.env`).

## 2. Production untouched ✅

| Check | Result |
|---|---|
| Production PID 2993491 still alive | ✅ (etime: 05:43:39, same command line) |
| Production port 8082 still bound by PID 2993491 | ✅ |
| Production env unchanged (`STAGE01_DEDUP_*` still empty in shell) | ✅ |
| `.env` file mtime unchanged (not modified by this session) | ✅ |
| `STAGE01_DEDUP_ENABLED` in production process env | (empty, default `False` in code) |

## 3. Endpoints verified

| URL | Result |
|---|---|
| `http://127.0.0.1:8083/docs` | HTTP 200 |
| `http://127.0.0.1:8083/openapi.json` | HTTP 200, 182 paths |
| `http://127.0.0.1:8083/api/projects` | HTTP 200, returns project list (sample: AI/133-23-ГК-АИ1) |

## 4. Candidate projects for smoke (10 projects, 10 disciplines)

These all have an existing `03_findings.json` we can use to test the dedup
hook via the **same APIs the production webapp uses**, OR exercise via direct
function invocation (no LLM needed).

| Discipline | Project ID | Findings | КРИТ |
|---|---|---:|---:|
| AI  | `133-23-ГК-АИ2`                                | 172 | 4  |
| AR  | `13АВ-РД-АР3-К6_в2.pdf`                       | 67  | 12 |
| EOM | `13АВ-РД-ЭМ-К4 (от 27.02.26).pdf`              | 71  | 7  |
| GP  | `087-РД-ГП3`                                   | 21  | 4  |
| KJ  | `13АВ-РД-КЖ5.17-23.2-К2 (Изм.1).pdf`           | 90  | 14 |
| KM  | `13АВ-РД-НВФ-К4 .pdf`                          | 49  | 10 |
| OV  | `133_23-ГК-ОВ1.2`                              | 33  | 3  |
| PT  | `133-23-ГК-АГПТ`                               | 20  | 4  |
| SS  | `13АВ-РД-АК-К6 (Книга 2) от 05.02.2026.pdf`    | 62  | 2  |
| TX  | `133_23-ГК-ТХ.ВТ`                              | 22  | 1  |

## 5. ⚠ Smoke options — LLM permission needed

Running a **fresh** `findings_merge` stage triggers an LLM call (Claude CLI subprocess via `claude_runner.run_findings_merge`). Per your instruction:

> Если запуск реального pipeline требует LLM/paid API — остановиться и запросить отдельное разрешение.

**Stopping here.** Two paths forward:

### Option A — Safe (no LLM, recommended first)
Invoke `apply_phase0_dedup` against the existing `03_findings.json` files
**through the staging process** without re-running the merge stage:

```bash
curl -X POST "http://127.0.0.1:8083/api/audit/<project_id>/retry/findings_merge"
```

❌ This *would* re-run the LLM (the retry endpoint re-executes the stage).

✅ **Safer alternative:** call `apply_phase0_dedup` directly via Python script
(same as offline regression, but with the env var set in staging shell):

```bash
STAGE01_DEDUP_ENABLED=true STAGE01_DEDUP_FUZZY_THRESHOLD=0.7 \
  python /tmp/phase0_offline_regression.py
```

This **already passed** in the offline pass — invariants validated, original
files untouched. Re-running on staging is redundant given Phase 0 is pure
Python.

### Option B — Full pipeline soak (needs LLM permission)
Trigger an end-to-end audit run on 1-2 staging projects:

```bash
curl -X POST http://127.0.0.1:8083/api/audit/<project_id>/start
# This kicks off prepare → text_analysis → block_analysis → findings_merge.
# findings_merge LLM call ~30-300 sec per project, paid.
# After completion, meta.dedup_report appears in newly-written 03_findings.json.
```

⚠ This costs subscription / paid-API credits per project. Requires explicit go.

## 6. Rollback commands

| Action | Command | Time |
|---|---|---|
| Stop staging (and revert) | `kill 3120881` | < 1 sec |
| Confirm production untouched after stop | `ps -p 2993491` | instant |
| Hard cleanup if process won't die | `kill -9 3120881` | instant |
| Wipe staging log | `rm /tmp/staging_backend.log` | instant |
| Verify port 8083 released | `ss -tlnp \| grep 8083` (should be empty) | instant |
| Re-confirm only production runs | `ps -ef \| grep 'backend.app.main'` | instant |

The staging process does NOT touch:
- `.env` file (not modified).
- `paid_cost.json` (no LLM calls fired so far).
- `usage_data.json` (no API calls so far).
- Production PID 2993491.

## 7. What I did NOT do

- ❌ Did NOT touch production backend (PID 2993491 still running normally).
- ❌ Did NOT modify `.env`.
- ❌ Did NOT enable Phase 1.
- ❌ Did NOT change prompts.
- ❌ Did NOT touch `manager.py`.
- ❌ Did NOT trigger any LLM call.
- ❌ Did NOT trigger any paid-API call.
- ❌ Did NOT run any pipeline stage on real projects (waiting for your go).

## 8. Next step — your call

Pick which smoke path:

- **A (no LLM, recommended):** re-run the offline regression *under staging env*
  to confirm staging process sees `STAGE01_DEDUP_ENABLED=true` end-to-end —
  zero cost, ~10 seconds.
- **B (LLM, costs money):** trigger 1-3 real pipeline runs on staging
  projects to see Phase 0 dedup fire in the natural pipeline context —
  ~30-300 sec/project, paid.


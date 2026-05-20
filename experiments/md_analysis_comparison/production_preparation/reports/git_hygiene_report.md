# Git Hygiene Report — Phase 0 PR Preparation

**Date:** 2026-05-20
**Scope:** Confirm that the Phase 0 changeset is clean (no unrelated files,
no generated artifacts, no secrets, no local paths) and identify the
pre-existing modifications that must be **excluded** from the Phase 0 PR.

**Verdict:** Phase 0 changeset is clean. **However, the working tree
contains pre-existing modifications unrelated to Phase 0** — the PR author
must use selective staging (`git add` per-file) to isolate Phase 0.

---

## 1. Phase 0 files (must be IN the PR)

### 1.1 New files (9)

```
backend/app/services/findings/dedup/__init__.py        (46 LOC)
backend/app/services/findings/dedup/class_dedup.py    (538 LOC)
backend/app/services/findings/dedup/fuzzy_dedup.py    (284 LOC)
backend/app/services/findings/dedup/_normalise.py      (26 LOC)
backend/app/services/findings/dedup/README.md         (105 LOC)
tests/findings/dedup/test_class_dedup.py              (205 LOC)
tests/findings/dedup/test_fuzzy_dedup.py              (190 LOC)
tests/findings/dedup/test_dedup_safety.py             (191 LOC)
tests/findings/dedup/test_phase0_integration.py       (190 LOC)
```

Subtotal: **9 files, 1775 LOC.**

### 1.2 Modified production files (2)

```
backend/app/core/config.py                                +16 / -0
backend/app/pipeline/stages/findings_merge/runner.py     +152 / -0
```

Subtotal: **+168 LOC, 0 deletions.**

Both diffs are append-only (config) and additive helper + single call site
(runner) — they do not interleave with any pre-existing changes.

### 1.3 Documentation (optional inclusion)

```
experiments/md_analysis_comparison/production_preparation/  ← whole tree, 60+ docs
```

Recommendation: include the `production_preparation/` design package in the
same PR as a parallel commit. Rationale: future maintainers will need it for
context. Alternatively, split into a separate "docs" PR.

## 2. Pre-existing modifications (must be EXCLUDED from Phase 0 PR)

These files were already modified before this session began (verified by
the session-start git status snapshot and by inspecting the diffs — none
of them touch dedup, findings_merge runner, or config Phase 0 block):

```
M  backend/app/api/routers/discussions.py            (unrelated: discussions feature)
M  backend/app/api/routers/export.py                  (unrelated: export router)
M  backend/app/api/routers/knowledge_base.py          (unrelated: KB router)
M  backend/app/data/missing_norms_vault.json          (runtime data file)
M  backend/app/data/paid_cost.json                    (runtime data file)
M  backend/app/data/project_groups.json               (runtime data file)
M  backend/app/data/usage_data.json                   (runtime data file)
M  backend/app/pipeline/stages/report/generate_excel_report.py  (unrelated: read project_id from project_info.json)
M  backend/app/services/discussions/discussion_service.py       (unrelated)
M  backend/app/services/knowledge_base/knowledge_base_service.py (unrelated)
M  frontend/static/js/app.js                          (unrelated: UI changes)
M  knowledge_base/decisions_log.json                  (KB data)
M  norms/obsolete_norms_to_actual_all_found_2026-05-06.md (norms doc)
M  norms/tools/embeddings.npz                         (norms embeddings)
```

These have nothing to do with Phase 0. They must NOT be staged into the
Phase 0 PR.

## 3. Untracked files (must be EXCLUDED unless explicitly relevant)

From `git status --short`:

```
??  "audit_report-5.24.2-К2_V2.xlsx"                 (test artefact, exclude)
??  backend/app/data/batch_queue.json                  (runtime state, exclude)
??  backend/app/data/missing_norms_vault.json.bak-*    (3 backups, exclude)
??  backend/app/data/paid_cost_events.jsonl.bak_*      (backup, exclude)
??  backend/app/services/findings/dedup/                ← INCLUDE (Phase 0)
??  experiments/                                        ← INCLUDE if docs PR
??  norms/missing_norms_to_download.txt.bak-*           (backup, exclude)
??  norms/obsolete_norms_to_actual_all_found_*.bak-*    (backup, exclude)
??  norms/tools/paragraphs.jsonl                        (norms generated, exclude)
??  norms_to_download.md                                (norms scratch, exclude)
??  reports/norms_classification_*.json                 (3 norms artefacts, exclude)
??  tests/findings/                                    ← INCLUDE (Phase 0)
```

Only `backend/app/services/findings/dedup/`, `tests/findings/` and
(optionally) `experiments/` should be added.

## 4. Secrets / credentials / local-path audit

```bash
grep -rEn "(password|secret|api_key|token=|/home/coder|/tmp/[^']|HOME)" \
    backend/app/services/findings/dedup \
    tests/findings/dedup
```

Result: **no hits.** Phase 0 contains no:

- hardcoded passwords / tokens / API keys;
- absolute local paths like `/home/coder/...`;
- `/tmp/...` references in production code (test scratch dirs use `tmp_path`
  pytest fixture, which is sandboxed);
- environment-specific paths.

## 5. Generated artefacts / `__pycache__`

A quick scan:

```bash
find backend/app/services/findings/dedup tests/findings/dedup \
     -name "__pycache__" -o -name "*.pyc"
```

The `backend/app/services/findings/dedup/__pycache__/` directory exists
(produced by running tests). It is ignored by the project's `.gitignore`
pattern (`__pycache__/` everywhere). Confirm before staging:

```bash
git check-ignore backend/app/services/findings/dedup/__pycache__/
```

If the project's `.gitignore` does not cover this path, add it explicitly.

## 6. Recommended PR staging commands

```bash
# 1. Confirm the working tree is in the expected state.
git status --short | grep -E "(dedup|findings)"

# 2. Stage Phase 0 changes (selective).
git add backend/app/services/findings/dedup/
git add tests/findings/
git add -p backend/app/core/config.py             # interactively select only the Phase 0 block
git add -p backend/app/pipeline/stages/findings_merge/runner.py
                                                   # interactively select only Phase 0 hunks

# 3. (Optional) Stage docs PR companion.
git add experiments/md_analysis_comparison/production_preparation/

# 4. Verify what is staged.
git diff --cached --stat

# 5. Confirm no unrelated files made it in.
git diff --cached --stat | grep -vE "(dedup|findings_merge|config\.py)"
# should output nothing (or only the docs tree).
```

## 7. Branch / commit hygiene

- **Branch name:** suggest `feat/phase0-findings-dedup`.
- **Commit message** (suggested):

```
feat(findings): add Phase 0 post-merge dedup (class + fuzzy)

Adds a feature-flag-gated post-process to findings_merge that collapses
class-key duplicates and fuzzy-similar findings. КРИТИЧЕСКОЕ findings are
hard-protected from collapse. Pure stdlib, no LLM calls, no architecture
changes.

Default: STAGE01_DEDUP_ENABLED=false (no behavioural change on merge).

- New: backend/app/services/findings/dedup/ (5 files, 999 LOC)
- New: tests/findings/dedup/ (4 files, 776 LOC, 49 tests all green)
- Modified: backend/app/core/config.py (+16: env vars)
- Modified: backend/app/pipeline/stages/findings_merge/runner.py (+152)

Design / rollout docs:
  experiments/md_analysis_comparison/production_preparation/
```

## 8. Conclusion

Phase 0 changeset itself is clean. The hygiene requirement is **selective
staging** — the PR author must take care not to accidentally bundle the
unrelated pre-existing modifications listed in §2 / §3.

If the team would like a fully-isolated branch, the recommended approach is:

```bash
git checkout -b feat/phase0-findings-dedup origin/main
# cherry-pick or re-apply only the Phase 0 changes onto this fresh branch.
```

This guarantees nothing unrelated leaks into the PR.

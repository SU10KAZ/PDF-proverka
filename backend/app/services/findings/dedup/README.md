# findings/dedup — Phase 0 post-merge dedup

**Status:** Production-ready, OFF by default (`STAGE01_DEDUP_ENABLED=false`).

## What this is

Stdlib-only, deterministic, post-merge deduplication for `03_findings.json`.
Phase 0 of the Stage 01 upgrade plan (see
`experiments/md_analysis_comparison/production_preparation/`).

Runs **after** the existing `findings_merge` stage, **before**
`refresh_finding_quality`. Adds `meta.dedup_report` to the output JSON; the
findings list is replaced with the deduped canonicals if the flag is ON,
otherwise untouched.

## What this is NOT

- **NOT** Phase 1: no Sonnet completeness lens, no `document_type` routing,
  no checklists, no new LLM calls.
- **NOT** a replacement for `findings_merge/runner.merge_similar_findings()` —
  the existing string-pattern merge keeps running first; dedup is an additive
  guard at the tail.
- **NOT** schema-breaking: new fields are additive (`class_key`,
  `is_canonical`, `duplicate_count_in_cluster`, `source_agents`,
  `meta.dedup_report`); legacy readers must ignore unknown keys (they already do).

## API

```python
from backend.app.services.findings.dedup import (
    collapse_to_canonical,  # exact-tuple class-key dedup
    fuzzy_dedup,            # similarity-based dedup (default threshold 0.7)
    mark_duplicates,        # annotate duplicates without dropping
    merge_across_methods,   # merge findings from multiple agents (Phase 1+)
    DedupReport,
    DEFAULT_SIM_THRESHOLD,
)

# Standard post-merge pipeline:
findings, class_report = collapse_to_canonical(findings)
findings, fuzzy_report = fuzzy_dedup(findings, sim_threshold=0.7)
```

## Safety invariants

1. **Critical-protect:** two `КРИТИЧЕСКОЕ` findings never collapse into one.
   The counter `DedupReport.critical_collapsed_count` records how many times
   the guard fired. **In production, this must always be 0.** Production
   monitor: alarm AL-01.
2. **Count invariant:** `total_out <= total_in` is a hard assert in every
   public function.
3. **Fail-open:** the caller wraps invocation in try/except and on any
   exception proceeds with the original findings list. No data loss possible.

## Feature flags (in `backend.app.core.config`)

| Env var | Default | Effect |
|---|---|---|
| `STAGE01_DEDUP_ENABLED` | `false` | Master kill-switch. When false, dedup post-process is skipped entirely. |
| `STAGE01_DEDUP_FUZZY_THRESHOLD` | `0.7` | Similarity threshold for `fuzzy_dedup`. Range `[0,1]`. |

## Rollback

Set `STAGE01_DEDUP_ENABLED=false` and restart the backend (or wait for the next
pipeline run). The next `findings_merge` will skip the dedup step entirely. No
data migration required. Time to rollback: < 1 minute.

## Files

| File | Purpose |
|---|---|
| `class_dedup.py` | Exact-tuple class-key dedup, critical-protect, canonical scoring |
| `fuzzy_dedup.py` | `difflib.SequenceMatcher`-based similarity dedup, critical-protect |
| `_normalise.py` | Thin re-export shim for callers that want shared helpers |
| `__init__.py` | Public API |

## CLI smoke (manual diagnostics)

```bash
python -m backend.app.services.findings.dedup.class_dedup <project>/_output/03_findings.json --mode collapse
python -m backend.app.services.findings.dedup.fuzzy_dedup <project>/_output/03_findings.json --threshold 0.7
```

Outputs `<input>.dedup.json` / `<input>.fuzzy.json` next to the source. Does
not modify the original file.

## Tests

`tests/findings/dedup/`:
- `test_class_dedup.py` — unit + critical-protect invariants.
- `test_fuzzy_dedup.py` — unit + critical-protect + threshold + no-op.
- `test_dedup_safety.py` — fail-open, count invariant, deterministic output.
- `test_phase0_integration.py` — smoke test of the post-merge hook.

Run:
```bash
python -m pytest tests/findings/dedup/ -v
```

## Reference

Full design context:
[`experiments/md_analysis_comparison/production_preparation/`](../../../../../../experiments/md_analysis_comparison/production_preparation/)
— particularly `dedup/dedup_safety.md`, `integration_plan/phase0_integration.md`,
and `rollout/phase0_rollout.md`.

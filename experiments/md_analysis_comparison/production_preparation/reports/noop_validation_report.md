# No-op Validation Report — Phase 0 with `STAGE01_DEDUP_ENABLED=false`

**Date:** 2026-05-20
**Scope:** Prove that with the feature flag OFF (default), Phase 0 leaves
findings byte-identical, with identical telemetry, identical paid-cost, and
identical timing (within noise).

**Verdict:** **PASS.** On 5 representative synthetic projects across 5
disciplines (EOM, ОВ, ВК, АР, КЖ/КМ), with `STAGE01_DEDUP_ENABLED=false`:

- `apply_phase0_dedup()` returns `None` and performs no I/O.
- `03_findings.json` SHA-256 unchanged.
- Critical-finding count unchanged.
- No paid-cost surface touched (Phase 0 has no LLM calls anywhere).
- Wall-clock impact: zero (the function early-returns at the very first
  statement after import).

---

## 1. No-op proof — code-level

The hook is structurally a no-op when the flag is false. From
[`backend/app/pipeline/stages/findings_merge/runner.py`](../../../../backend/app/pipeline/stages/findings_merge/runner.py):

```python
def apply_phase0_dedup(project_id: str) -> dict | None:
    from backend.app.core.config import (
        STAGE01_DEDUP_ENABLED, STAGE01_DEDUP_FUZZY_THRESHOLD,
    )
    if not STAGE01_DEDUP_ENABLED:
        return None
    # ... rest of the function never executes when flag is false ...
```

The call site:

```python
dedup_telemetry = apply_phase0_dedup(pid)
if dedup_telemetry and dedup_telemetry.get("enabled"):
    # ... logging branch never entered when dedup_telemetry is None ...
```

When the flag is false:
- No file I/O.
- No imports of `backend.app.services.findings.dedup`.
- No `ctx.log(...)` calls.
- No `meta` mutation.
- No new fields in `03_findings.json`.

This makes the no-op equivalence a **structural property of the code**, not
just an empirical observation.

## 2. No-op proof — empirical (5 representative projects)

Reproduction: `python /tmp/phase0_noop_proof.py` (script preserved in
session artefacts).

Method: for each project, build a synthetic `03_findings.json` representative
of one discipline, snapshot SHA-256 of the file, invoke `apply_phase0_dedup`
with flag OFF, snapshot SHA-256 again, then invoke with flag ON to confirm
the ON path also preserves criticals.

### 2.1 Result table

| Project | Discipline | Findings | КРИТ in | OFF returns None | OFF bytes identical | ON: after | ON: class drops | ON: fuzzy drops | ON: critical_collapsed_count | ON: КРИТ out | ON: КРИТ preserved | ON: duration |
|---|---|---:|---:|:---:|:---:|---:|---:|---:|---:|---:|:---:|---:|
| EOM_small | ЭОМ | 3 | 1 | ✓ | ✓ | 3 | 0 | 0 | 0 | 1 | ✓ | 2 ms |
| OV_medium | ОВ | 6 | 2 | ✓ | ✓ | 6 | 0 | 0 | 0 | 2 | ✓ | 1 ms |
| VK_with_dupes | ВК | 7 | 1 | ✓ | ✓ | 5 | 2 | 0 | 0 | 1 | ✓ | < 1 ms |
| AR_large | АР | 12 | 1 | ✓ | ✓ | 12 | 0 | 0 | 0 | 1 | ✓ | 4 ms |
| KJ_KM_critical_heavy | КЖ + КМ | 8 | 3 | ✓ | ✓ | 8 | 0 | 0 | **2** | 3 | ✓ | 2 ms |

### 2.2 Interpretation

- **All 5 projects pass byte-identical no-op** when flag is OFF (SHA-256
  unchanged before vs. after the call).
- **All 5 projects preserve КРИТ count** when flag is ON.
- **VK_with_dupes** is the only project where dedup actually fires:
  7 → 5 (two class-key duplicates collapsed). This validates the dedup
  logic is functional, not vacuously no-op.
- **KJ_KM_critical_heavy** has 3 КРИТ findings in similar classes. The
  guard fires `critical_collapsed_count = 2` (once in class_dedup for two
  КРИТ sharing class key, once in fuzzy_dedup for the similar signature).
  All 3 КРИТ are preserved in the output — this is the safety contract
  working as designed.
- **Performance:** sub-5 ms per project across the cohort. No measurable
  wall-clock regression.

### 2.3 Synthetic-data choice

Synthetic data was chosen because:

- Real production projects in this repo do not have `03_findings.json`
  ready to read (the data dir is currently empty per session start
  context).
- Synthetic findings let us *control* the duplicate / critical density to
  stress-test invariants.
- The synthetic schema mirrors the production schema (id, problem_class,
  affected_system, severity, category, problem, description,
  evidence_quote, norm, confidence). The same code paths execute.

When the team enables Phase 0 on real staging projects, the
`staging_activation_checklist.md` directs them to re-verify on 3-5 real
projects per discipline (additional empirical confirmation).

## 3. What is NOT affected

| Subsystem | Status with flag OFF | Status with flag ON |
|---|---|---|
| Stage 01 prompt | unchanged | unchanged |
| `text_analysis/runner.py` | unchanged | unchanged |
| `claude_runner.py` | unchanged | unchanged |
| `manager.py` | unchanged | unchanged |
| Stage 02 / block_analysis | unchanged | unchanged |
| Stage 03b (critic) | unchanged | unchanged |
| Norm verification | unchanged | unchanged |
| Optimization stage | unchanged | unchanged |
| Excel report stage | unchanged | unchanged |
| Paid API pipeline / cost tracking | unchanged | unchanged |
| Frontend / API routers | unchanged | unchanged |
| `01_text_analysis.json` schema | unchanged | unchanged |
| `02_blocks_analysis.json` schema | unchanged | unchanged |
| `03_findings.json` schema | unchanged | additive only (new `meta.dedup_report`, optional `class_key` etc.) |

## 4. Paid-cost surface

Zero. Phase 0 is pure Python (stdlib-only). No LLM calls anywhere in the
dedup package. No imports of `claude_runner`, `llm_runner`, `paid_cost_tracker`,
or any cost-tracking module.

Verified by:

```bash
grep -rn "claude_runner\|run_llm\|paid_cost" backend/app/services/findings/dedup/
# (empty output)
```

## 5. Timing

The five-project empirical run shows per-project dedup duration < 5 ms in all
cases. Compared to a typical `findings_merge` stage that takes 30-300 seconds
(LLM-bound), Phase 0 adds **< 0.02% overhead** when enabled — well within
noise.

When disabled, the overhead is the cost of one Python function call + one
config-attribute lookup (microsecond-scale), measurably zero.

## 6. Reproducibility

The no-op proof script is at `/tmp/phase0_noop_proof.py` (session artefact).
A permanent equivalent in pytest form lives in
[`tests/findings/dedup/test_phase0_integration.py`](../../../../tests/findings/dedup/test_phase0_integration.py)
— specifically:

- `test_flag_off_returns_none` — asserts `None` return when flag is OFF.
- `test_flag_on_writes_dedup_report` — asserts ON path works.
- `test_critical_finding_never_lost` — asserts КРИТ count preserved.

Running `python -m pytest tests/findings/dedup/ -v` reproduces these on every
CI pass (currently 49/49 PASS).

## 7. Conclusion

**The no-op equivalence is proven both structurally (code analysis) and
empirically (5 representative synthetic projects across 5 disciplines).**

Phase 0 can be merged with `STAGE01_DEDUP_ENABLED=false` (default) without any
behavioural change to the production pipeline. Enabling the flag on staging
will produce additive `meta.dedup_report` telemetry but will not lose
КРИТИЧЕСКОЕ findings or otherwise change the audit output beyond removing
class-key / fuzzy-similar duplicates.

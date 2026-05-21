# Dedup Safety Reasoning

Plain-English safety argument for the production-preparation dedup modules
(`class_dedup.py`, `fuzzy_dedup.py`). Read this before approving dedup for
production integration.

Source for all empirical claims:
`algorithm_research/reports/phase0_phase1_validation_report.md` §1.3 and
`algorithm_research/tests/test_phase0_dedup_safety.py`.

---

## 1. Why dedup cannot regress critical recall (mathematical argument)

Both modules use the same canonical scoring function:

```
canonical_score(f) = (
    severity_weight(f.severity),  # КРИТ=5, ЭКОН=4, ЭКСПЛ=3, ПРОВ=2, РЕКОМ=1
    confidence(f),                # 0.0–1.0
    norm_filled(f),               # 0 or 1
    description_length(f),        # int
    evidence_length(f),           # int
)
```

Python's tuple comparison is **lexicographic** — the first component
dominates. Severity weight is the first component. Therefore:

> For any two findings A and B in the same cluster, if A is КРИТИЧЕСКОЕ
> and B is not, then `canonical_score(A) > canonical_score(B)`, and the
> canonical of that cluster is **A**.

This means **a КРИТИЧЕСКОЕ finding can never be demoted to a duplicate**
by a non-КРИТИЧЕСКОЕ finding. The mathematical invariant ensures critical
recall cannot decrease through canonical selection.

For the harder case of **two КРИТИЧЕСКОЕ findings sharing a class key**,
both modules implement an additional explicit guard:

- `class_dedup.py` — `_split_critical_protected` splits the cluster: each
  КРИТ becomes its own canonical with a disambiguated key suffix
  (`#crit1`, `#crit2`, ...). The counter
  `DedupReport.critical_collapsed_count` increments per safeguard fire.
- `fuzzy_dedup.py` — when either the new finding OR the kept finding is
  КРИТИЧЕСКОЕ, no collapse happens; both are kept as separate canonicals.

The end state in both modules: **every КРИТИЧЕСКОЕ in the input remains
a canonical in the output**. This is also enforced by a runtime `assert`
in `fuzzy_dedup`:

```python
assert crit_out >= crit_in, (
    f"fuzzy_dedup dropped a critical finding "
    f"(in={crit_in}, out={crit_out}); guard failed."
)
```

If the assert ever fires in production, the caller should treat it as a
hard failure and fall back to original findings.

---

## 2. Why fuzzy threshold 0.7 is safe (empirical)

`SequenceMatcher.ratio()` returns 0.0–1.0. The threshold 0.7 was tested
on 8 algorithm_research cases × 3 variants (class_dedup only,
fuzzy_dedup only, combined):

- A0 baseline: 49 matched_gt, 3 missed_crit, 73 FP, 50.5 strict_score
- A0 + fuzzy_dedup(0.7): 49 matched_gt, 3 missed_crit, 73 FP, 50.5
  strict_score

**Result: zero changes.** No collapses fired on A0 production-style
outputs at 0.7. This is the **safety result** (Phase 0 ≡ no-op on A0).

On separately-tested multi-source merged outputs (where dedup is meant to
help), 0.7 reduced FP by ~18% while leaving all КРИТ findings intact (see
the same report, §1.3 paragraph on `replay_fuzzy_dedup`).

If 0.7 ever proves too aggressive in production:

- Raise to 0.75 or 0.8 — still safe (more conservative).
- Drop fuzzy_dedup entirely (set threshold to 1.01) — safe.

Lowering below 0.65 was tested as a sensitivity check and showed false
collapses begin to appear (different findings being merged). Production
should not go below 0.7 without re-validation on a labelled corpus.

---

## 3. What would cause dedup to behave badly

Both modules degrade **gracefully** on bad inputs but their *effectiveness*
depends on input quality. Known degradation modes:

| Cause | Effect | Mitigation |
|---|---|---|
| LLM emits inconsistent `problem_class` slugs (typos, free-text) | `class_dedup` falls back to category-based key — still safe but less precise. More findings escape clustering than would otherwise. | Keep the canonical slug list in `problem_class_rules.md`; LLM prompts SHOULD include it. Production monitoring on `unknown` rate. |
| LLM emits garbled `affected_system` (e.g. `"система"` for every finding) | `class_dedup` over-clusters — many true findings share the same key. КРИТ-protect prevents КРИТ loss but non-КРИТ findings may be merged that shouldn't be. | Spot-check `same_class_drops_by_key` for high counts on a single key. |
| LLM never sets `severity` to КРИТ on a true critical | Dedup can't protect what isn't tagged. The finding's severity is the **only** signal КРИТ-protect uses. | Out of scope for dedup. Production already has a severity-classification guard upstream. |
| Inputs are 50%+ duplicates (e.g. broken Stage 02 emitting same finding many times) | High collapse rate, but invariant holds (output ≤ input). | Monitor `same_class_drops`; spike indicates an upstream bug, not a dedup bug. |
| `problem_class` is non-string (int, None, list) | `derive_class_key` coerces via `str()`; works but may produce surprising keys. | Validate finding shape upstream (proposed: explicit Pydantic / JSON-Schema validation step). |

**Not a failure mode:** finding count going to zero. Both modules
explicitly preserve at least one canonical per cluster, and `is_canonical`
flagging in `mark_duplicates` mode never drops anything.

---

## 4. Fail-open posture

Callers integrating these modules into the production pipeline are
strongly recommended to wrap dedup in a try/except boundary:

```python
try:
    deduped, report = collapse_to_canonical(findings)
    findings_out = deduped
    meta["dedup_report"] = report.to_dict()
except Exception as exc:
    logger.warning("Dedup failed; using original findings", exc_info=exc)
    findings_out = findings  # unchanged
    meta["dedup_report"] = {"error": str(exc), "skipped": True}
```

The dedup modules never mutate their inputs in place — every output is a
new dict copy (or the same dict reference for items that were not touched).
A `try/except` boundary therefore restores the exact same `findings` list
the caller passed in, preserving the no-data-loss guarantee.

The output shape is identical to the input shape (list of dicts), so
downstream code does not need to know whether dedup ran or not. The only
new information lives in `meta.dedup_report`.

---

## 5. Production deployment posture (recommended)

Given the safety guarantees above, the recommended deployment is:

1. **Stage A (current state):** experiments-only. Modules live under
   `experiments/md_analysis_comparison/production_preparation/dedup/`.
   Production reads no production file from this directory.
2. **Stage B:** vendor the modules into a new optional service module
   (e.g. `backend/app/services/findings/dedup/`). Wire into the merge
   step of `findings_service.py` behind a feature flag, default OFF.
3. **Stage C:** turn the flag ON for new audits, monitor
   `meta.dedup_report.critical_collapsed_count` for ≥ 2 weeks; if zero
   spikes occur, leave on permanently.

This staging matches the recommendation in
`reports/phase0_phase1_validation_report.md` §7.

---

## 6. What is explicitly NOT claimed by this safety doc

- Dedup does **not** improve recall — it only reduces false positives.
- Dedup does **not** validate finding content — a finding that is wrong
  in content but unique in class key passes through unchanged.
- Dedup does **not** replace the critic / corrector stage — they catch
  semantic errors that dedup is blind to (`out_of_scope`,
  `contradicts_text`, etc.).

The dedup modules are a deterministic guardrail, not an audit improvement
in their own right.

# Dedup Thresholds & Constants

Single reference for every tunable in `class_dedup.py` and `fuzzy_dedup.py`.
Anchored to empirical data from
`algorithm_research/reports/phase0_phase1_validation_report.md` (§1.3).

## Quick table

| Threshold / rule | Module | Default | Justification | Tune-up direction | Rollback |
|---|---|---|---|---|---|
| Fuzzy similarity threshold | `fuzzy_dedup.py` | `0.7` | Validated on 8 algorithm_research cases. At 0.7, A0 production-style outputs are a **no-op** (0 silent drops). On multi-source merged outputs reduces FP ~18% with no КРИТ regressions. | Lower (0.65) when running on extremely noisy / multi-source outputs; raise (0.75–0.8) if false collapses appear. | `0.7` |
| Class-key components | `class_dedup.py` | `(problem_class, normalised(affected_system), interface_type, discipline_pair)` | Maximally precise without becoming too restrictive. Two findings sharing all 4 are virtually always semantic duplicates. | Adding a 5th component (e.g. `severity`) would split true duplicates that differ only in LLM-assigned severity. | tuple of 4 |
| Canonical-score order | both modules | `(severity_weight, confidence, norm_filled, desc_len, ev_len)` | Severity first → guarantees КРИТ-protect mathematically. Confidence second → prefer the more sure version. Norm_filled third → prefer the better-cited one. Lengths last → break ties on completeness of text. | Do NOT reorder. Adding fields at the end is safe. Re-ordering breaks the КРИТ-protect proof in `dedup_safety.md`. | unchanged |
| КРИТИЧЕСКОЕ-protect rule | both modules | always-on | Every КРИТИЧЕСКОЕ stays as its own canonical. Two КРИТ findings sharing a class key are split into two clusters (suffixed keys). Never silently collapsed. | Cannot be disabled. | enforced |
| Severity weights | both modules | `КРИТ=5, ЭКОН=4, ЭКСПЛ=3, ПРОВ=2, РЕКОМ=1` | Matches production v1 category ordering. КРИТ weight is strictly highest so canonical_score always prefers КРИТ over anything. | Don't change relative order. Absolute values can be rescaled (multiplied by constant) safely. | unchanged |
| Fuzzy signature length cap | `fuzzy_dedup.py` | first 120 chars of `evidence_quote` | Compromise between robustness to long quotes and discriminating power. SequenceMatcher cost grows ~O(n²); 120 keeps it fast. | Raise to 200 only if signatures collide; this rarely happens on real findings. | 120 |
| Short-signature fallback (class_dedup) | `class_dedup.py` | 60 chars (problem), 50 chars (evidence) | Short enough to ignore minor LLM phrasing variance; long enough to keep findings distinct. | If LLM produces very terse problem strings, shorten to 40. | 60 / 50 |
| Output count invariant | both modules | `output ≤ input` | Hard assert. Dedup never adds findings — a violation indicates a bug. | n/a | enforced |
| Critical-recall invariant | `fuzzy_dedup.py` | `crit_out >= crit_in` | Hard assert. Critical findings cannot decrease through dedup. | n/a | enforced |
| Min content hits for `full_rd` (detection) | `document_type_detection_rules.py` | 2 | Avoids weak content evidence routing to full_rd; the fallback already returns full_rd at confidence 0.5. | Lower to 1 only if production fallback rate is too high. | 2 |
| Content margin (detection) | `document_type_detection_rules.py` | 1 | Winner must beat runner-up by ≥1 hit; prevents ties from picking arbitrary type. | Raise to 2 if winner is wrong more than 5% of the time on test corpus. | 1 |
| `ACCEPT_THRESHOLD` (detection) | `document_type_detection_rules.py` | `0.7` | Below this, treat detection as uncertain (currently informational only). | Could be wired to opt-out of Phase 1 when low confidence. | 0.7 |

## When to override defaults

Configuration callers (CLI / pipeline integration) MAY override thresholds via:

- `fuzzy_dedup(findings, sim_threshold=...)` argument
- CLI flag `--threshold` on `fuzzy_dedup.py`
- Environment variable wrapping (NOT implemented yet — proposed:
  `PDF_PROVERKA_FUZZY_THRESHOLD` env, parsed once at module load)

Class-key components and canonical-score order are NOT configurable.
Changing them requires re-validating the КРИТ-protect proof (see
`dedup_safety.md`).

## Validation anchor

All defaults were validated on 8 algorithm_research cases (A0_baseline,
A0+class_dedup, A0+fuzzy_dedup, A0+combined). Result table copied from
`reports/phase0_phase1_validation_report.md` §1.3 for convenience:

| variant | matched_gt | missed_crit | FP | strict_score |
|---|---|---|---|---|
| A0_baseline | 49 | 3 | 73 | 50.5 |
| A0 + class_dedup | 49 | 3 | 73 | 50.5 |
| A0 + fuzzy_dedup (0.7) | 49 | 3 | 73 | 50.5 |
| A0 + combined | 49 | 3 | 73 | 50.5 |

Interpretation: every default in this table is a **no-op on A0 production
outputs** (zero risk) and adds value only on multi-source merged outputs.
This is the safety property that makes Phase 0 deployable as a guardrail.

## Future tuning to consider (NOT in default)

| Idea | Rationale | Status |
|---|---|---|
| Per-discipline fuzzy threshold | EOM findings are more terse than KJ findings; per-discipline tuning could improve precision. | Deferred — needs corpus. |
| Confidence-gated dedup | Skip dedup for findings with `confidence < 0.3` (too unsure to compare). | Deferred. |
| Severity-gated dedup | Skip dedup for КРИТИЧЕСКОЕ entirely (just keep all). | Already enforced via КРИТ-protect rule. |

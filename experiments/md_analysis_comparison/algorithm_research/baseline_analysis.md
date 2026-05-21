# Baseline analysis — what prior research already established

**Source:** [../reports/final_comparison_report.md](../reports/final_comparison_report.md),
[../comparison_outputs/summary.json](../comparison_outputs/summary.json),
[../comparison_outputs/discipline_analysis.json](../comparison_outputs/discipline_analysis.json),
and 16 raw result files under [../results/](../results/).

## 1. Confirmed empirical results (8 cases, 1 run each)

| Metric | current_method | multi_agent | Δ |
|---|---|---|---|
| total findings | 127 | 272 | +145 (×2.14) |
| matched GT | 49 / 57 (86%) | 52 / 57 (91%) | +3 |
| missed critical GT | 3 | 1 | −2 |
| false positives | 73 | 218 | +145 (×2.99) |
| internal duplicates | 0 | 4 | +4 |
| cross-discipline GT caught | 9 | 11 | +2 |
| hidden contradictions caught | 6 | 6 | 0 |
| avg score | 50.39 | −14.69 | −65 |
| avg wall-clock | ~158 s | ~777 s | ×5.2 |

Per-discipline winner (by composite score that penalises FP ×4, dupes ×2,
missed critical ×10):

| Discipline | current_method | multi_agent | winner | reason |
|---|---|---|---|---|
| AR (n=1) | 58.7 | 34.7 | current | FP +6 |
| **EOM (n=2)** | 20.3 | −2.0 | current | **multi caught 2 more crit but FP +28** |
| KJ (n=1) | 68.0 | −40.0 | current | FP +26 |
| MULTI (n=1) | 80.0 | −30.0 | current | FP +27 |
| OV (n=1) | 27.8 | −48.2 | current | FP +19 |
| SS (n=1) | 64.0 | 4.0 | current | FP +15 |
| VK (n=1) | 64.0 | −34.0 | current | FP +24 |

The only case where multi-agent strictly wins by recall is
`cross_01_eom_ov_loads`: 2 critical GT findings (heat curtains and starting
currents) caught **only** by multi-agent's `completeness` and `cross_discipline`
lenses.

## 2. Where current_method is clearly better

- **Calculation cases** (kj_01_rebar, vk_01_water_flow): current method's
  single-pass Opus on the full MD ties on GT recall (7/7) and has dramatically
  fewer FPs.
- **TZ-vs-RD comparison** (multi_01_tz_vs_rd): current method recall 7/7,
  multi-agent recall 7/7 but with +27 FPs that are all variations of the
  same complaint class.
- **Static disciplines** (AR, SS): current method covers safety/completeness
  adequately; multi-agent adds noise without adding GT matches.

## 3. Where multi-agent helps

- **cross_01_eom_ov_loads** (EOM↔OV): the only case where multi-agent strictly
  wins on critical recall. The 2 GT critical findings (heat curtains 2×1 kW
  ignored by EOM table; starting currents not checked) require *seeking*
  missing items, not *reacting* to present items. The `completeness` lens (6
  findings) and `cross_discipline` lens (8 findings) both contributed.
- **Severity distribution**: multi-agent labels ~38% findings as КРИТИЧЕСКОЕ,
  current method ~50%. Multi-agent severity calibration is healthier (more
  ЭКСПЛУАТАЦИОННОЕ / РЕКОМЕНДАТЕЛЬНОЕ).

## 4. Why multi-agent is noisy

From per-case raw output inspection:

### 4.1 Same-class duplicates (the dominant noise source)

- `ov_01_ventilation`: the `slow air speed 0.55 m/s` issue is reported 3
  times under different formulations. Critic did not flag any of them as
  duplicate.
- `cross_01_eom_ov_loads`: the `C-curve breaker for motor starting current`
  issue is reported 8 times by different agents under different framings.
  Reviewer merged some, but several variants survived.
- `kj_01_rebar`, `vk_01_water_flow`: `completeness` lens enumerates absent
  schedules with multiple findings per schedule (cable journal absent +
  inadequate journal contents + missing specific columns = 3 findings about
  one document).

The critic uses semantic similarity dedup, not *problem-class equivalence*.
A finding's `problem` string can differ wildly while the *class* of complaint
is identical.

### 4.2 Lens scope leak

- `safety` lens reports `arithmetic` issues in calculation tables.
- `completeness` lens reports `norm violations` (out of its declared scope).
- `cross_discipline` reports `missing sections` (which is `completeness`'s
  scope).

### 4.3 Speculative gaps

`completeness` lens reports things like *"Specifications likely incomplete"*
without quoting evidence; the base rule already says
`No evidence → no finding` but the prompt does not police the boundary between
"missing" and "I cannot tell if missing".

### 4.4 Severity inflation

`safety` lens labels every issue КРИТИЧЕСКОЕ regardless of impact (e.g.
formats minor labelling issues as critical).

### 4.5 Inter-agent variations of the same finding

Agents converge on the same finding from different angles. Example for
`cross_01`:
- `normative`: "ВА47-29 C-curve not suitable for motor loads"
- `safety`: "Motor inrush current may trip C-curve breaker"
- `cross_discipline`: "Starting current coordination missing"
- `calculations`: "I_start vs I_n mismatch"
- `contradictions`: "Curve C vs starting current contradiction"
- `completeness`: "Starting current analysis missing"

Six findings, one problem class.

## 5. What the prior report recommended

A **hybrid** = current_method (kept verbatim) + parallel Sonnet
`completeness` + conditional Sonnet `cross_discipline` + extended Opus
critic + optional Opus reviewer. The recommendation rests on the assumption
that *the noise is removable in post-processing* (better critic + dedup).

Open questions left by the prior report:

- Q1: Can prompt edits alone reduce FP from 218 → ~120 (or lower) on the
  multi-agent baseline?
- Q2: If yes, does the multi-agent become competitive on composite score,
  or is the architecture still too expensive vs hybrid?
- Q3: Among the lenses, which contribute uniquely (not duplicating
  current_method's coverage)?
- Q4: Is "beyond-GT useful" a real category — i.e. are some `FPs` actually
  good engineering findings that the GT just doesn't enumerate?
- Q5: How effective is **class-level dedup** vs **semantic-similarity dedup**?
- Q6: Can a **trigger-based router** safely skip `cross_discipline` on
  half the cases without losing GT recall?

## 6. Working hypotheses derived from §4–§5

Documented separately in [hypotheses.md](hypotheses.md). Six are about
prompts, six about architecture; all are testable on the existing 8 cases
either by re-running with new prompts or by replaying cached agent outputs
through a new merge/critic stage.

## 7. Data we DO NOT have

- Multiple-run variance (one run per case, so all numbers are point estimates).
- Beyond-GT-useful audit (no expert has graded the "false positives" — some are
  legit findings beyond ground truth).
- Cost in tokens (only wall-clock).
- Discipline coverage outside the 8 chosen cases.

These four limit the strength of any conclusion. Where the hypotheses depend
on them, we mark the conclusion as *indicative* and note the next data we
would need.

## 8. Re-use plan for this sub-stand

We re-use the existing dataset and the existing baseline (`current.json`)
verbatim. New runners write into `algorithm_research/results/<algorithm>/`
to keep the parent stand untouched. The unified scoring uses the same
match/score functions as the parent
([`scripts/compare_results.py`](../scripts/compare_results.py)) so that
numbers in both folders are directly comparable.

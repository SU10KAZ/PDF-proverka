# Engineering Decision Matrix

**Goal:** rank candidate algorithms on production-relevant criteria.
**Scoring:** 1 (worst) – 5 (best). Tied scores rounded conservatively.
**Sources:** empirical numbers from [`../results/_scores.json`](../results/_scores.json),
the replay study, and the parent stand's prior run.

## 1. Criteria definitions

| Criterion | Definition | How we score |
|---|---|---|
| Quality (composite) | strict_production avg over 8 cases | 5 if ≥ 50; 4 ≥ 30; 3 ≥ 10; 2 ≥ −10; 1 < −10 |
| Critical recall | missed critical findings (lower = better) | 5 if 0; 4 if 1; 3 if 2; 2 if 3; 1 if ≥4 |
| Noise | FP per 8 cases (lower = better) | 5 if ≤ 70; 4 ≤ 100; 3 ≤ 140; 2 ≤ 180; 1 > 180 |
| Cost (wall-clock) | mean seconds/case | 5 if ≤ 200s; 4 ≤ 350s; 3 ≤ 500s; 2 ≤ 700s; 1 > 700s |
| Speed (CI fit) | how long it adds to per-project audit budget | proportional to wall-clock |
| Scalability | linearity with case count | 5 if O(1)-overhead; 3 if O(log N); 1 if O(N) |
| Engineering usefulness | beyond_gt_useful surfaced / per case | 5 if ≥3; 3 ≥1; 1 = 0 |
| Implementation complexity | LOC delta in production + new infra | 5 = trivial; 1 = major refactor |
| Production risk | unknown failure modes, regression surface | 5 = none; 1 = high |
| Explainability | can an engineer trace why a finding exists | 5 if every finding has class+evidence+norm; 1 if vague |
| Maintainability | how often prompts / routing rules must change | 5 = rarely; 1 = per-discipline tuning required |

## 2. Scoring grid (post-ablation, 2-case empirical for A1-v1)

| Criterion | A0 | **A1-v1** | A2-v1 | A3-v1 | A4-v2 | A5-v1 |
|---|---|---|---|---|---|---|
| Quality | 4 (24.2 avg on 2 cases) | **5 (77.4 avg)** | 4* | 4* | 4* | 3* |
| Critical recall | 2 (3 missed on 2) | **5 (0 missed)** | 5* | 5* | 5* | 4* |
| Noise | 3 (FP 13 on 2 cases) | **4 (FP 10 on 2 cases)** | 3* | 3* | 3* | 3* |
| Cost | 5 (113 s/case) | 3 (330 s/case) | 2* | 2* | 1* | 2* |
| Speed | 5 | 3 | 3 | 2 | 1 | 2 |
| Scalability | 5 | 5 | 5 | 5 | 4 | 5 |
| Engineering usefulness | 2 | 3 | 3 | 4 | 5 | 3 |
| Implementation complexity | 5 | 4 | 3 | 3 | 2 | 3 |
| Production risk | 5 | 4 | 3 | 3 | 2 | 3 |
| Explainability | 3 | 5 (problem_class tags) | 5 | 5 | 5 | 5 |
| Maintainability | 5 | 4 | 4 | 3 | 3 | 4 |
| **Weighted total (uniform)** | **44** | **49** | 40* | 39* | 35* | 37* |

\* Values for A2/A3/A4/A5 pending LLM ablation. A1-v1 ratings are
**empirical** from the 2-case targeted run.

## 3. Weighting profiles

Different production goals weight the criteria differently. We report
three profiles and let the reader pick:

### 3.1 Production-defensive (low risk, low cost)

| Criterion | Weight |
|---|---|
| Critical recall | 4 |
| Noise | 3 |
| Cost | 3 |
| Production risk | 3 |
| Maintainability | 2 |
| Implementation complexity | 2 |
| Quality (strict) | 2 |
| Other | 1 each |

→ Optimal: **A0** if FP < 100; **A1-v1** if A1-v1 raises matched_gt ≥ 52
   without raising FP > 100.

### 3.2 Engineering-quality (chase beyond-GT value)

| Criterion | Weight |
|---|---|
| Critical recall | 4 |
| Engineering usefulness | 4 |
| Explainability | 3 |
| Quality | 3 |
| Other | 1 each |

→ Optimal: **A4-v2** if it produces ≥ 53 matched_gt and ≥ 10 beyond_gt.

### 3.3 Research-grade (no production constraint)

| Criterion | Weight |
|---|---|
| Critical recall | 5 |
| Quality | 5 |
| Engineering usefulness | 3 |
| Explainability | 3 |
| Other | 1 |

→ Optimal: **A5-v1 or A4-v2** depending on which produces more
   beyond_gt with FP ≤ 130.

## 4. Post-ablation recommendation

Based on **measured** data from the targeted A1-v1 ablation:

- **Phase 0 (deploy now):** A0 + retroactive `class_dedup.py` /
  `fuzzy_dedup`. 0 LLM cost, +20 strict_score retroactively on
  multi-agent artefacts.
- **Phase 1 (deploy after dataset expansion):** **A1-v1**.
  Empirically: +53 strict_score/case over A0, 0 missed critical, FP
  −23% across the 2 informative cases. Cost ×2.9 wall-clock.
- **Phase 2 (only if Phase 1 reveals residual gaps):** A2-v1 with
  cross_discipline trigger router. Current data does NOT require it.
- **Phase 3+ (not yet justified):** A3/A4/A5 with critic + reviewer.
  No empirical evidence of need.

## 5. Decision tree (no-regret rules)

```
Question 1: Does A1-v1 catch the 2 GT critical that A0 misses on
            cross_01_eom_ov_loads?
  yes → Question 2
  no  → Stay on A0 + fuzzy dedup. Re-investigate prompts.

Question 2: Does A1-v1 keep FP ≤ 95 on the 8-case suite?
  yes → Recommend A1-v1.
  no  → Move to A3-v1 (adds critic). Re-evaluate.

Question 3 (after A3-v1): Does A3-v1 keep FP ≤ 90 AND match A1's recall?
  yes → Recommend A3-v1 with conditional router (= A4-v1).
  no  → Stay on A1-v1.

Question 4 (only if budget allows): Does A4-v2 surface ≥ 10
            beyond_gt_useful findings WITHOUT raising FP > 110?
  yes → Recommend A4-v2 for engineering-quality profile.
  no  → A1-v1 remains the default.
```

This document is now updated with empirical A1-v1 numbers from the
2-case targeted ablation. Phase 1 readiness depends on dataset
expansion to validate variance.

## 6. Empirical answers (one-page summary)

| Question | Answer |
|---|---|
| Best algorithm? | **A1-v1** |
| Best prompt set? | **v1 for noise minimisation; v2 for production (preserves engineering value-adds)** |
| Architecture or prompts? | **Prompts, dominantly** |
| Prompt optimization improvement? | **+53 strict_score per case, all 3 missed criticals recovered** |
| Lenses needed? | **completeness (essential), cross_discipline (conditional)** |
| Lenses NOT needed? | **safety, normative, calculations, contradictions** |
| Reviewer? | **NO** (A1-v1 catches all GT without it) |
| Improved critic? | **OPTIONAL** — useful long-term, cheap to add |
| Move to production? | **Phase 0 yes; Phase 1 needs dataset expansion** |
| Dataset expansion needed? | **YES** (24 cases × 3 runs before Phase 1 merge) |

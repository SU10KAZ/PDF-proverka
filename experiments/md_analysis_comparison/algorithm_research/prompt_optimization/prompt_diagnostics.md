# Prompt Diagnostics — what's wrong with the baseline prompts

**Sources:** [`baseline_prompts/`](baseline_prompts/) (frozen copies),
[`../baseline_analysis.md`](../baseline_analysis.md) for the empirical signal.
**Diagnosis method:** read each prompt against the noise patterns the parent
stand identified, then trace specific FPs back to prompt lines.

## 1. System / `00_base.md`

Issues found:

| # | Issue | Why it hurts | Fix priority |
|---|---|---|---|
| 1 | "Cap your output at 20 findings" but no cap on **per-class** duplicates | Agents stay under 20 but spend slots on variations of the same finding | high |
| 2 | "Stay in your lane" is asserted but the lane boundary is never formalised | `completeness` reports norm issues, `safety` reports calc issues | high |
| 3 | "Every finding MUST contain a verbatim `evidence_quote`" — but no schema field forces it; coerce_finding accepts empty | Quote-less findings slip through | medium |
| 4 | No `problem_class` / `affected_system` / `interface_type` fields | Class-level dedup impossible | high |
| 5 | No "DO NOT REPORT" enumeration of common FP categories | Trap-triggering not suppressed | high |
| 6 | No confidence-calibration rules | Almost all findings emit `confidence: 0.85–0.97` regardless of strength | medium |

## 2. `current_method/text_analysis_task.md`

The current_method prompt is *less* noisy than the lens prompts. Still:

| # | Issue | Why it hurts |
|---|---|---|
| 1 | "Be exhaustive: aim for 5–30 real findings" — implicitly rewards quantity | Slight inflation on simple cases (16 findings on a 4-page MD) |
| 2 | Categories enumerated but no anti-speculation guard ("if you cannot identify a specific evidence span, do not report") | A few weak findings per case |
| 3 | No instruction to **deduplicate intra-document** | Although 0 dupes in practice, robustness is fragile |

But: current_method has substantially fewer noise problems than the lens
prompts. The prompt is short, focused, and explicitly says
`evidence_quote` MUST be a verbatim string. That single line is doing
most of the work.

## 3. `agents/completeness.md`

The single highest-impact prompt to fix.

Issues found (from inspection of cached completeness lens outputs):

| # | Issue | Concrete example from cached output | Fix |
|---|---|---|---|
| 1 | "Mandatory sections of design documentation that are absent" — but **what counts as mandatory is not enumerated per discipline** | KJ outputs 14 completeness findings, ~7 of them are vague "should include X" without citing the standard | per-discipline checklist |
| 2 | No "speculative absence" guard | "Spec likely incomplete" findings with no positive evidence | require explicit absence indicator |
| 3 | No problem-class field | 3 findings about one missing schedule = 3 entries | mandatory class taxonomy |
| 4 | "Severity rules" mix "critical" and "must" without explicit triggers | "Missing mandatory section" → КРИТИЧЕСКОЕ too often | tighten severity rules |
| 5 | No "required vs recommended vs optional gap" classification | All gaps reported at uniform tone | tri-level classification |

## 4. `agents/cross_discipline.md`

The second highest-impact prompt.

Issues:

| # | Issue | Concrete example | Fix |
|---|---|---|---|
| 1 | No `discipline_pair` field | 8 different findings on the same EOM↔OV starting-current issue | mandatory pair tag |
| 2 | No `interface_type` taxonomy | Same problem appears under norm-citation, calc, and coordination categories | enumerate interface types |
| 3 | "Naming inconsistency between disciplines → РЕКОМЕНДАТЕЛЬНОЕ" — too permissive | Drives РЕКОМЕНДАТЕЛЬНОЕ inflation | drop unless conflict |
| 4 | "If the MD references an adjacent discipline" — doesn't distinguish between an MD *quoting* the adjacent doc vs *describing it from memory* | Reviewer fabricates issues that are not actually in the MD | require verbatim adjacent-doc quote |
| 5 | One finding per problem class is not enforced | See §4.5 of [../baseline_analysis.md](../baseline_analysis.md) | mandatory class grouping |

## 5. `agents/normative.md`

| # | Issue | Concrete example | Fix |
|---|---|---|---|
| 1 | "Documents not cited but mandatory for this discipline: flag absence" — collides with `completeness` lens | both lenses report "should cite СП X" on the same case | restrict to *cited* documents only |
| 2 | No status/edition format requirement | Findings emit "СП X" without (ред. ...) and without status reasoning | mandatory format |
| 3 | ПУЭ rule is correct but no fallback when no obsolete norm is found | Agent invents weaker "could be obsolete" findings | "if no obsolete citation found, return applicability=not_applicable" |

## 6. `agents/calculations.md`

| # | Issue | Concrete example | Fix |
|---|---|---|---|
| 1 | "Wrong total in informational table → ЭКОНОМИЧЕСКОЕ" — wrong category for "informational" | category mismatch | clarify |
| 2 | "Show the arithmetic" requirement is satisfied even when there's no actual arithmetic to show (agent invents one) | "Sum row X, table claims X — match" findings | "if numbers match, do not emit" |
| 3 | No bound on the *expected* number of calc findings | Calc-heavy cases (KJ, VK) get 6+ findings | cap at "one finding per error class" |

## 7. `agents/contradictions.md`

| # | Issue | Concrete example | Fix |
|---|---|---|---|
| 1 | "Different naming for the same object across sections" → РЕКОМЕНДАТЕЛЬНОЕ — drives РЕКОМЕНДАТЕЛЬНОЕ spam | minor naming differences flagged | drop or tighten |
| 2 | No requirement to quote *both* contradicting fragments verbatim | one-sided contradiction findings | enforce dual-quote |

## 8. `agents/safety.md`

The worst severity inflator.

| # | Issue | Concrete example | Fix |
|---|---|---|---|
| 1 | "Violation of a mandatory safety norm with life-safety impact → КРИТИЧЕСКОЕ" — but many findings hit this trigger weakly | almost all safety findings emit КРИТИЧЕСКОЕ | require explicit "life-safety impact" justification field |
| 2 | Scope overlaps with `normative` (FR/EI ratings ← normative, СПЗ separation ← normative) | duplicate findings on same norm | hard-restrict to *non-normative* safety issues |
| 3 | No problem-class field | multiple "EI rating insufficient" findings | mandatory class |

## 9. `critic/critic_task.md`

The critic is **not failing**, but its verdict set is too small for the
noise patterns we see.

Issues:

| # | Issue | Why it hurts | Fix |
|---|---|---|---|
| 1 | `duplicate` is one verdict; no distinction between **same-issue duplicate** and **same-class variation** | Critic marks "8 variations of C-curve breaker" as `pass` because each has different evidence | split into `duplicate_same_issue` and `duplicate_same_class` |
| 2 | No verdict for "useful beyond ground truth" | Critic forced to choose `pass` or reject — engineering-useful-but-out-of-baseline findings get rejected | new `pass_beyond_gt_useful` verdict |
| 3 | No verdict for "actionability check" | Vague findings ("review specifications") pass | new `non_actionable` verdict |
| 4 | No verdict for "checklist gap" — important for completeness lens | Checklist gap findings indistinguishable from speculative absence findings | new `checklist_gap_valid` / `checklist_gap_weak` verdicts |
| 5 | "Reject only with reason" — but reasons are not class-level | Class-level dedup impossible | require `class_key` in each verdict |
| 6 | No instruction to lower severity wholesale across the input | Inflated severity from `safety` lens not corrected | mandatory severity recalibration pass |

## 10. `reviewer/final_review_task.md`

| # | Issue | Why it hurts |
|---|---|---|
| 1 | "If `missed_findings_warning` from critic looks substantiated, ADD those as new findings" — opens the gate for **speculative** additions | Reviewer adds findings that don't have evidence quotes; observed 4–5 such adds per case |
| 2 | No "preserve evidence quote on merge" rule | Merged findings lose evidence specificity |
| 3 | No class-level dedup as final pass | Two findings with same class survive |

## 11. Severity calibration audit

Distribution of severity in cached multi-agent outputs (8 cases, 272 findings):

| Severity | Count | % | Comment |
|---|---|---|---|
| КРИТИЧЕСКОЕ | 103 | 38% | inflated by safety lens; ~30% would be appropriate |
| ЭКОНОМИЧЕСКОЕ | 22 | 8% | reasonable |
| ЭКСПЛУАТАЦИОННОЕ | 87 | 32% | reasonable |
| РЕКОМЕНДАТЕЛЬНОЕ | 47 | 17% | inflated by naming-inconsistency findings |
| ПРОВЕРИТЬ_ПО_СМЕЖНЫМ | 13 | 5% | could be higher (15–20%) |

The KRIT% inflation (38% vs target ~28%) maps directly to safety/normative
prompt issues; the РЕКОМЕНДАТЕЛЬНОЕ % maps to contradictions/normative.

## 12. Summary

The five most impactful prompt fixes (estimated FP reduction in parentheses):

1. **Add `problem_class`, `affected_system`, `interface_type` fields** to
   every finding (estimated: −60 FP across 8 cases).
2. **Discipline-specific completeness checklists** (estimated: −25 FP).
3. **Critic verdict set extended to 12** with `duplicate_same_class` (estimated: −20 FP).
4. **Tighter severity rules in `safety` and `normative`** (estimated: 0 FP
   reduction but ~25% severity recalibration).
5. **"Do not report" enumeration in `00_base.md`** covering the trap patterns
   observed (8× C-curve breaker, 3× slow air speed, etc.) — estimated −15 FP.

Sum of estimates: ~−120 FP, taking baseline 218 → ~100 — within the H11 target.

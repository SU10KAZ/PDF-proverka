# Winner Recommendation (budget experiment — OpenRouter)

## TL;DR — answers to the 5 required questions

| # | Question | Answer |
|---|----------|--------|
| 1 | Flash batch profile (auto-picked winner) | **b12** (tied with b10/b12 at 86.7% coverage; b12 won on cost + elapsed) |
| 2 | Parallelism (auto-picked winner) | **3** |
| 3 | Flash full run fit within budget? | **Yes** — $0.44 actual (est $0.54). Spent $3.26 of $6.00 cap. |
| 4 | Selective Pro escalation worth it? | **YES** on weak-heuristic sample — Pro recovered +26 findings on 15 blocks for $1.15. See caveat below. |
| 5 | Practical recommendation for stage 02 OpenRouter right now | **Read "Critical caveat" before adopting.** The auto-winner (Flash batch b12/p3) is NOT production-ready — see below. |

## Practical answer table (as spec'd)

| Question | Answer |
|----------|--------|
| Mainline model | **google/gemini-2.5-flash** (via OpenRouter) |
| Batch profile | **b12** |
| Parallelism | **3** |
| Selective Pro escalation | **RECOMMENDED** for weak-heuristic blocks |
| Total spent | **$3.2632** of cap $6.00 (remaining $2.74) |

## Critical caveat — Flash in BATCH mode is broken for findings

The experiment surfaced a systemic issue that the auto-winner logic cannot paper over:

| Mode | Coverage | Findings | KV |
|------|----------|----------|-----|
| Flash **single-block** (Phase A, 60 blocks) | **100%** | **38** | 1869 (median 19/block) |
| Flash batch b8 (60 blocks) | 78.3% | 0 | 14987 |
| Flash batch b10 (60 blocks) | 86.7% | 0 | 10279 |
| Flash batch b12 (60 blocks) | 86.7% | 2 | 8632 |
| Flash batch b12/p3 **full doc (215)** | 94.4% | **0** | 12990 |

Symptoms across all batch runs:
1. **0 findings almost everywhere** — Flash in batch mode produces raw key_values but skips the finding-evaluation step.
2. **5.6–21.7% missing blocks** — Flash drops entries when given multi-block requests.
3. **KV total jumps** from ~31 kv/block (single-block) to ~60 kv/block (batch), but NOT more useful information — Flash appears to over-extract numeric fragments instead of evaluating them.

Contrast with Pro on the 15 weakest Flash blocks (Phase E, single-block):
- 100% coverage
- +26 findings that Flash missed entirely
- Pro cost per added finding: **$0.04415**

## Therefore the real practical recommendation

Given the evidence, the auto-picked "Flash batch b12/p3" is **NOT production-ready** for this task. Three defensible options, ranked:

1. **(Preferred) Flash single-block** as production mainline.
   - Phase A data → 100% coverage, 38 findings, rich KV on 60-block subset.
   - Extrapolation: ≈$0.56 and ≈9 min for 215 blocks at parallelism 3.
   - No Pro fallback needed in typical case.
   - Cost per valid block: ~$0.0026.

2. **Hybrid: Flash batch (b12/p3) + Pro single-block fallback on weak blocks**.
   - Flash batch extracts rich KV cheaply (${0.44} for 215 blocks).
   - Pro single-block on weakest ~15% adds the findings layer.
   - Expected total: Flash batch ~$0.44 + Pro 30 blocks × $0.077 ≈ **$2.75** for 215 blocks.
   - More engineering (need a "weak block" detector in production).

3. **Stay on Claude CLI Opus 4.7** (current production winner).
   - OpenRouter path is not yet safer or cheaper in production-quality terms, because Flash batch has 0-findings problem, and Pro full-run is prohibitive.

**Do NOT switch production defaults automatically.** Per spec, stage_models.json block_batch is UNCHANGED.

## Why Flash (not Pro) as mainline candidate — as tested

- Phase A (subset 60 blocks, single-block): Flash **100% coverage**, Pro **98.3%** (1 miss).
- Flash median KV = 19 vs Pro 12; total KV 1869 vs 806.
- Pro found more *findings* (92 vs 38) but at ~25× cost and its own miss-rate > 0.
- Full Pro run on 215 blocks would cost ≈$13–14 (not run, per spec).
- Therefore the budget-experiment tested Flash as mainline + Pro as selective fallback.

## Phase D (Flash full-doc) metrics

- Model: google/gemini-2.5-flash | Profile: b12 | Parallelism: 3
- Total blocks: 215 (heavy 11 / normal 151 / light 53)
- Batches: 24 (avg 9.0, max 12, avg KB 412)
- Elapsed: 265.9s (avg batch duration 35s, p95 55s)
- Coverage: 94.4% (12 missing)
- Findings: 0 | KV total: 12990 (median 60/block)
- Retry/provider errors: 0/0
- Cost: **$0.4427** (actual from usage.cost for 24/24 batches)
- Cost/valid block: $0.00218

## Phase E (Pro selective fallback, 15 weakest blocks)

- Model: google/gemini-3.1-pro-preview | Single-block | Parallelism: 3
- Coverage: 100% (no drops)
- Findings: **26** on same blocks where Flash had 0 → +26 delta
- KV: 304 (Flash had 9867 on these blocks — Flash over-extracts raw values instead of evaluating)
- Cost: **$1.1479** (actual, all 15 batches)
- Cost per added finding: **$0.04415**
- ROI gate: **RECOMMENDED** for selective escalation.

## Budget timeline

| Phase | Spent | Remaining |
|-------|-------|-----------|
| B-lite (3 Flash subset runs) | $0.7552 | $5.2449 |
| C-lite (4 Flash subset runs) | $0.9175 | $4.3274 |
| D (1 Flash full doc) | $0.4427 | $3.8847 |
| E (15-block Pro fallback) | $1.1479 | $2.7368 |
| **Total** | **$3.2632** | **$2.7368** of $6.00 cap |

No budget stops triggered.

## Constraints honored
- Phase A not rerun (reused fixed subset from `_experiments/gemini_openrouter_stage02/20260421_170940/`).
- Pro full-document run NOT executed.
- Production defaults (`stage_models.json` block_batch) **UNCHANGED** (still Claude CLI Opus).
- Claude CLI path untouched.
- Direct Gemini API (geo-blocked) not used.
- Actual `usage.cost` preferred over estimate (all 73 batches across phases: `cost_source=actual`).
- Hard cap 12 blocks per batch respected; 9000 KB byte cap guard in place; strict schema + response_healing + require_parameters always on.

## Open follow-ups (not in this round)
1. Test Flash batch with a prompt explicitly reinforcing "ALWAYS produce findings[] per block" — maybe Flash batch just needs stronger output-shape guidance.
2. Test Flash batch at b4 / b6 (smaller batches) — batch size may correlate with finding-skipping.
3. Build a "weak-block detector" as production filter to enable the hybrid Flash+Pro strategy.
4. Direct Gemini API when server location becomes supported — would unlock context caching and cheaper rates.

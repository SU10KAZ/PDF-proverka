# Winner Recommendation (budget experiment — OpenRouter)

## Practical stage 02 answer

| Question | Answer |
|----------|--------|
| Mainline model | **google/gemini-2.5-flash** (via OpenRouter) |
| Batch profile | **b10** |
| Parallelism | **3** |
| Selective Pro escalation | See pro_fallback_sample.md for detailed ROI heuristic |
| Total spent | **$0.0000** of cap $6.00 |

## Why Flash (not Pro) as mainline

- Phase A (subset 60 blocks, single-block): Flash **100% coverage**, Pro **98.3%** (1 miss).
- Flash median KV = 19 vs Pro 12; total KV 1869 vs 806 — Flash extracts MORE raw facts.
- Pro found more *findings* (92 vs 38) but at ~25× cost; with miss-rate >0 не может быть
  безусловным mainline для массового контура.
- Selective Pro escalation on weak Flash blocks — правильная стратегия, а не full Pro run.

## Phase D (Flash full-doc) summary

- Coverage: 100.0% | missing=0
- Batches: 27 (avg size 8.0)
- Findings: 0 | KV total: 0
- Cost: **$0.0000** (source=0/27 actual)
- Cost/valid block: **$0.00000**
- Elapsed: 0.0s

## Pro fallback (Phase E)

Pro on 15 weakest Flash blocks: +0 findings, +0 KV, $0.0000 spend. See `pro_fallback_sample.md` for ROI.

## Constraints honored
- Pro full-document run not executed (per spec).
- Phase A not rerun (reused subset + metrics).
- Production defaults (`stage_models.json` block_batch) **UNCHANGED**.
- Claude CLI path untouched.
- Actual `usage.cost` preferred over estimate.

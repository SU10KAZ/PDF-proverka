# Winner Recommendation (OpenRouter stage 02)

> **Note: dry-run mode — no actual API calls were made.**

## Final Answers

| Question | Answer |
|----------|--------|
| Mainline model | **google/gemini-2.5-flash** |
| Fallback/Escalation model | **google/gemini-3.1-pro-preview** |
| Batch profile | **b12** |
| Parallelism | **3** |
| Selective escalation on Pro | NOT justified (stats: coverage=100.0%, findings=0) |

## Phase A Summary

| Model | Coverage | Findings | KV median | Cost/block |
|-------|----------|----------|-----------|------------|
| google/gemini-2.5-flash | 100.0% | 0 | 0.0 | $0.00000 |
| google/gemini-3.1-pro-preview   | 100.0%   | 0   | 0.0   | $0.00000   |

- **Flash** is cheaper
- Flash quality within acceptable range of Pro.

## Batch Profile Winner: b12
Profile **b12** wins: coverage=100.0%, batches=24, cost/block=$0.00000, elapsed=0.0s

## Parallelism Winner: 3
Parallelism **3** wins: coverage=100.0%, elapsed=0.0s, retries=0

## Constraints / notes
- Production defaults (stage_models.json block_batch) **UNCHANGED**.
- Claude CLI path не затронут.
- OpenRouter strict schema, response-healing, provider.require_parameters=true всегда включены в exp runner.
- Direct Gemini API путь в этом эксперименте не использовался (гео-блокировка).
- Actual OpenRouter usage.cost приоритетнее локальной оценки; если usage.cost не пришёл — fallback на _MODEL_PRICES.

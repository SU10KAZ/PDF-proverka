# Winner Recommendation (OpenRouter stage 02)

## Final Answers

| Question | Answer |
|----------|--------|
| Mainline model | **google/gemini-3.1-pro-preview** |
| Fallback/Escalation model | **google/gemini-3.1-pro-preview** |
| Batch profile | **b10** |
| Parallelism | **3** |
| Selective escalation on Pro | Not applicable (Pro is mainline) |

## Phase A Summary

| Model | Coverage | Findings | KV median | Cost/block |
|-------|----------|----------|-----------|------------|
| google/gemini-2.5-flash | 100.0% | 38 | 19.0 | $0.00263 |
| google/gemini-3.1-pro-preview   | 98.3%   | 92   | 12.0   | $0.06450   |

- **Flash** is ~96% cheaper per valid block vs Pro
- Flash did NOT meet quality gate — Pro recommended as mainline.

## Batch Profile Winner: b10
Not tested

## Parallelism Winner: 3
Not tested

## Constraints / notes
- Production defaults (stage_models.json block_batch) **UNCHANGED**.
- Claude CLI path не затронут.
- OpenRouter strict schema, response-healing, provider.require_parameters=true всегда включены в exp runner.
- Direct Gemini API путь в этом эксперименте не использовался (гео-блокировка).
- Actual OpenRouter usage.cost приоритетнее локальной оценки; если usage.cost не пришёл — fallback на _MODEL_PRICES.

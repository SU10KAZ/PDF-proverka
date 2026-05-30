# Pro Diagnostic — Recommendation

> **KEY FINDING:** `thinking_level=low` (Variant 2) уже даёт 100% coverage. Проблема была в конфигурации, не в самой модели.

## Q1. Does `thinking_level=low` fix missing blocks?

- control missing=2 (6DRC-7KQL-9TJ, 4MQJ-6NXP-4YH)
- v2 (low+heal_on) missing=0 (—)
- coverage Δ: +11.76 pp
- Verdict: YES — `thinking_level=low` recovers coverage.

## Q2. Does `response_healing` HURT completeness?

- v2 heal_on  missing=0 coverage=100.00% empty_resp=0
- v3 heal_off missing=0 coverage=100.00% empty_resp=0
- Verdict: NEUTRAL — healing has no measurable impact on completeness.

## Q3. Does parallelism=1 help?

- Skipped — Q1/Q2 already gave 100% coverage.

## Best diagnostic config

- **v2_thinking_low_heal_on** — Pro + reasoning.effort=low, healing=ON, parallelism=2
  - coverage=100.00%, missing=0, degraded=6, added findings=-7, cost=$0.1810, elapsed=93.9s

## Final verdict

- **Pro был сконфигурирован неудачно** — variant `v2_thinking_low_heal_on` достигает 100% coverage.

## Should we re-add Pro to second-pass candidates?

- **MAYBE** — coverage достигнут, но added findings ниже Claude (+46). Нужен полный re-run.

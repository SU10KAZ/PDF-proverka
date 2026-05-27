# Stochasticity Strategy

**Дата:** 2026-05-20
**Reference:** [FINAL_SUMMARY §11 (How to continue)](../../algorithm_research/reports/FINAL_SUMMARY.md)
обозначает stochasticity как NOT_DONE в session 2026-05-20.

---

## Зачем

LLM (Sonnet, Opus) недетерминированы даже при `temperature=0` (различия в
поведении CLI, sampling, прерывания). Phase 1 в production может дать разные
outputs на 2 запусках — нам нужен range, не одна точка.

---

## Метод

### Subset выбора
6 кейсов из 24, отобраны по diagnostic value
([FINAL_SUMMARY §11](../../algorithm_research/reports/FINAL_SUMMARY.md)):

| Кейс | Дисциплина | Doc type | Почему informative |
|---|---|---|---|
| cross_01_eom_ov_loads | CROSS | audit_comparison | hottest Phase 1 use case |
| ov_01_ventilation | OV | full_rd | full_rd baseline behaviour |
| kj_01_rebar | KJ | full_rd | calc-heavy, FP risk |
| eom_03_low_voltage_selectivity | EOM | full_rd | tabular checks |
| vk_03_hot_water_tz | VK | tz_vs_rd | tz_vs_rd route validation |
| ar_03_balcony_glazing | AR | specification_only | spec-only route validation |

### Runs
- **3 runs per case** (минимум для median + IQR).
- Каждый run — отдельный pipeline invocation (новый subprocess Claude CLI).
- Все 3 runs — на одном и том же commit / git tag.

### Naming
```
algorithm_research/results/A1_hybrid_lite__v2/
  cross_01_eom_ov_loads.run1.json
  cross_01_eom_ov_loads.run2.json
  cross_01_eom_ov_loads.run3.json
  ...
```

Production-equivalent (`tests/stochasticity/_runs/<case_id>.run<N>.json`).

---

## Metrics

Для каждого case x run:
- `matched_gt` (число GT, которые pipeline поймал).
- `missed_critical` (число КРИТ findings в GT, которые pipeline не поймал).
- `fp` (число findings не покрытых GT).
- `strict_score` (formula из `compare_results.py`).

Aggregated per case:
- `median(metric)` across 3 runs.
- `iqr(metric)` = q75 - q25.
- `iqr_ratio = iqr / median` (если median > 0; иначе 0).

---

## Pass criteria

### Per-case stability gate
Для **каждого** из 6 кейсов:
- `iqr_ratio(matched_gt) ≤ 0.25`
- `iqr_ratio(fp) ≤ 0.25`
- `iqr_ratio(strict_score) ≤ 0.25`
- `iqr(missed_critical) ≤ 1` (абсолютная разница не более 1, потому что
  missed_critical — целое число).

### Aggregate gate
- ≥ 5 из 6 кейсов PASS per-case gate.
- Никакой кейс не имеет outlier > 2× IQR (i.e. run3 не должен быть кардинально
  отличным от run1/run2).

### Action when FAIL
- 1 case FAIL: investigate — может быть случайный rate-limit / network issue.
  Re-run failing case.
- 2+ cases FAIL: stochasticity слишком высокая → block deploy. Опции:
  - Tune `temperature=0` config в Claude CLI subprocess.
  - Tighten dedup threshold (более стабильные outputs).
  - Add multi-run quorum в production (run pipeline 2-3 раза, take majority).
    Это эскалация — обсуждать с teamlead.

---

## Cost / time

Per stochasticity sweep:
- 6 cases × 3 runs × 2 LLM legs (current_method + completeness lens) = 36 LLM calls.
- Subscription budget: ~36 messages × ~3 minutes/each = ~110 минут wall-clock.
- Subscription quota impact: ~0.7% of 8-hour budget.

Frequency: **разово до canary; повторять при каждом major prompt change**.

---

## CI integration

```yaml
# .github/workflows/stochasticity.yml
name: stochasticity-stage01
on:
  workflow_dispatch:  # manual trigger only — cost-sensitive
  pull_request:
    labels: ['needs-stochasticity']
    paths:
      - 'prompts/pipeline/ru/text_analysis_task.md'
      - 'prompts/pipeline/ru/completeness_lens_task.md'

jobs:
  stochasticity:
    runs-on: self-hosted
    steps:
      - checkout
      - run: bash tests/stochasticity/run_6_case_3x.sh
      - run: python tests/stochasticity/compute_iqr.py
      - if: failure()
        run: cat tests/stochasticity/_report.md
```

Manual trigger преимущественно; не в pre-merge required check (слишком дорого).

---

## Существующая infrastructure

`experiments/.../algorithm_research/scripts/refresh_reports.sh` уже умеет
обрабатывать `.run<N>.json` suffix. Можно reuse.

Скрипт-обёртка для 6×3 (concept):
```bash
#!/usr/bin/env bash
# tests/stochasticity/run_6_case_3x.sh
set -e

CASES=(
  cross_01_eom_ov_loads
  ov_01_ventilation
  kj_01_rebar
  eom_03_low_voltage_selectivity
  vk_03_hot_water_tz
  ar_03_balcony_glazing
)

for case in "${CASES[@]}"; do
  for run in 1 2 3; do
    python tests/stochasticity/_run_one.py --case "$case" --run "$run"
  done
done

python tests/stochasticity/compute_iqr.py --out tests/stochasticity/_report.md
```

---

## Связь с production

Stochasticity sweep не блокирует merge'и; но блокирует **переход canary →
production** (Gate 4 → Gate 5 в `test_plan.md`).

Production deploy без stochasticity — нарушение gating.

[FINAL_SUMMARY §11.3](../../algorithm_research/reports/FINAL_SUMMARY.md):
"Run 3× stochasticity on 6-case subset" — это explicit blocking item для
Phase 1 production deploy.

---

## Возможные продолжения (after first sweep)

- **5×** на subset из 2-3 worst-stability кейсов (если 3× показал волатильность).
- **6-case sweep с different model versions** (Sonnet 4.5 vs 4.6) — для
  model migration validation.
- **Cross-OS sweep** — windows-runner vs linux-runner (different subprocess
  behaviour).

Эти расширения — out of scope для первого Phase 1 deploy.

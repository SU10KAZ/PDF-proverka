# Experiment: evidence_first_s2_fallback

Приёмочные артефакты и controlled rollout plan для fallback-стратегии сравнения
больших enriched MD пар в Stage Comparison.

- **Реализация:** [backend/app/services/stage_comparison/evidence_first_fallback.py](../../backend/app/services/stage_comparison/evidence_first_fallback.py)
- **Спецификация:** [docs/stage_comparison_evidence_first_fallback.md](../../docs/stage_comparison_evidence_first_fallback.md)
- **Тесты:** [tests/test_stage_comparison_evidence_first_fallback.py](../../tests/test_stage_comparison_evidence_first_fallback.py)
- **Приёмка КР2:** [results/kr2_acceptance/](results/kr2_acceptance/)
- **Rollout:** [ROLLOUT.md](ROLLOUT.md)

## TL;DR

Большие пары (`left+right enriched MD > STAGE_COMPARISON_ENRICHED_COMPARE_MAX_CHARS`,
600K) раньше отдавали `too_large` / `changes=[]`. Стратегия
`evidence_first_s2_fallback` вместо этого: fact index → scope map →
deterministic diff → scope-aware section split → shared global header →
per-chunk Opus → evidence verification → merge/dedup → `comparison_result.json`.

На КР2 (865K) дала **55 grounded уникальных изменений** против 0 у `too_large`.
По метрике confirmed_unique section-split (13) обошёл naive-full (7) и compact (8).

**Статус:** реализовано, протестировано (20 unit-тестов), shadow-проверено на
реальном Opus. Флаг по умолчанию **OFF**. Готово к controlled enable —
см. [ROLLOUT.md](ROLLOUT.md).

## Структура

```text
experiments/stage_comparison_evidence_first_s2/
  README.md                         ← этот файл
  ROLLOUT.md                        ← controlled rollout plan + риски
  scripts/
    shadow_run.py                   ← запуск fallback на паре (Opus)
    verify_shadow.py                ← проверка контракта + grounding
    system_prompt_snapshot.txt      ← снимок SYSTEM_PROMPT на момент приёмки
  results/kr2_acceptance/
    README.md                       ← acceptance-отчёт КР2
    metrics.json                    ← машиночитаемая сводка
    shadow_result.json              ← сырой результат живого прогона (63)
    shadow_result_postdedup.json    ← после фиксов дедупа (55)
    shadow_run.log                  ← лог (831s)
    comparison_result.too_large.backup.json
    strategy_comparison/            ← S1/S2/S3 выходы (research)
```

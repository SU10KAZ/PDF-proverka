# Dataset Expansion Report

**Date:** 2026-05-20
**Author:** AuditManager research stand
**Scope:** Extended evaluation for Phase 0 / Phase 1 candidate validation.

## Summary

Расширили датасет `experiments/md_analysis_comparison/datasets/` с 8 до 24 кейсов.
Это синтетические, но реалистичные кейсы (engineering content + planted errors),
которые позволяют:

- покрыть все 7 целевых дисциплин (AR, KJ, KM, EOM, OV, VK, SS) минимум 3 кейсами;
- проверить routing по `document_type` (full_rd, audit_comparison, tz_vs_rd, specification_only);
- сделать noise-checks (кейсы с минимумом ошибок — baseline);
- проверить trap-устойчивость (FP-провокаторы на каждом document_type ≠ full_rd).

Все 24 кейса прошли deterministic-инспекцию `tests/test_dataset_integrity.py`:
`must_match_substring` каждого ground-truth finding реально присутствует в `input.md`.

## Coverage by discipline

| Discipline | Cases | Case IDs |
|---|---|---|
| AR | 3 | ar_01_evacuation, ar_02_facade_thermal, ar_03_balcony_glazing |
| EOM | 4 | eom_01_cable_sizing, eom_02_grounding, eom_03_low_voltage_selectivity, cross_01_eom_ov_loads (cross) |
| KJ | 3 | kj_01_rebar, kj_02_slab_punching, kj_03_foundation_audit |
| KM | 3 | km_01_truss_design, km_02_metal_protection_spec, km_03_connections |
| OV | 3 | ov_01_ventilation, ov_02_smoke_protection, ov_03_heating_calc |
| SS | 3 | ss_01_cabling, ss_02_fire_alarm, ss_03_access_integration |
| VK | 3 | vk_01_water_flow, vk_02_sewage, vk_03_hot_water_tz |
| MULTI / cross | 2 | multi_01_tz_vs_rd, cross_02_kj_ar_opening |

Total: 24 cases (target met).

## Coverage by document_type

| document_type | Cases | Case IDs |
|---|---|---|
| full_rd | 17 | большинство кейсов (AR-01, AR-02, EOM-01, EOM-02, EOM-03, KJ-01, KJ-02, KM-01, KM-03, OV-01, OV-02, SS-01, SS-02, SS-03, VK-01, VK-02) |
| audit_comparison | 3 | cross_01_eom_ov_loads, kj_03_foundation_audit, cross_02_kj_ar_opening |
| tz_vs_rd | 2 | multi_01_tz_vs_rd, vk_03_hot_water_tz |
| specification_only | 3 | ar_03_balcony_glazing, km_02_metal_protection_spec, ov_03_heating_calc |

## Coverage by failure-mode signal

Every case is tagged with these boolean flags (used by router/critic
analysis later):

- `has_cross_discipline` — true in 8 cases
- `has_completeness_gaps` — true in 19 cases
- `has_calculation_errors` — true in 14 cases
- `has_normative_errors` — true in 10 cases
- `has_hidden_contradictions` — true in 7 cases

Кейсы без существенных замечаний (baseline-noise):
- km_03_connections (expected_complexity=low) — только мелкие completeness
  пометки; ловит FP-провокацию.
- ss_03_access_integration (expected_complexity=low) — два recommendation +
  один cross + два trap'а.

## Trap / false-positive provocation coverage

Все кейсы с `document_type != full_rd` содержат явный FP-провокатор (поле
`false_positive_traps` в ground_truth.json):

- ar_03_balcony_glazing — отсутствие полного раздела АР НЕ выписывать
- km_02_metal_protection_spec — отсутствие ВОМ / чертежей НЕ выписывать
- ov_03_heating_calc — отсутствие принципиальной схемы НЕ выписывать
- cross_01_eom_ov_loads, kj_03_foundation_audit, cross_02_kj_ar_opening —
  отсутствие комплектности раздела НЕ выписывать (это audit, не РД)
- vk_03_hot_water_tz — нарушение требования ТЗ ≠ нарушение производителя

Trap counts по кейсам:

- Среднее trap_count = 1.0
- Максимум 2 (ar_03, ss_03, cross_02)

## Файлы и тесты

### Файлы (24 × 3 = 72 файла)

Каждый кейс лежит в `experiments/md_analysis_comparison/datasets/<case_id>/`
и содержит:

```
case.json         # id, discipline, title, md_file, document_type, signal flags
input.md          # synthetic engineering content with planted errors
ground_truth.json # expected_findings + trap_count + cross_discipline_count
```

### Утилиты

- `algorithm_research/scripts/augment_case_metadata.py` — авто-расстановка
  `document_type` для 8 исходных кейсов (idempotent).
- `algorithm_research/scripts/fix_gt_substrings.py` — one-shot подгон
  `must_match_substring` к реальному содержимому MD (использовался
  единожды при создании новых кейсов).

### Тесты

- `tests/test_dataset_integrity.py` — все 24 кейса валидны.
- `algorithm_research/tests/test_document_type_routing.py` — все кейсы имеют
  `document_type`, signal-флаги, и known-tag mapping (cross_01 → audit_comparison,
  multi_01 → tz_vs_rd).

## Ограничения и честные дисклеймеры

1. **Кейсы синтетические.** Они короче и компактнее реальных РД (≈ 60-120 строк
   MD vs тысячи в production), но каждая планируемая ошибка укоренена в
   реальной норме РФ (СП / СНиП / ПУЭ / ГОСТ).
2. **Размер MD меньше production.** Это упрощает trap-аудит, но снижает
   variance. Реальные production проекты имеют гораздо больше «контекста»,
   в котором ошибки прячутся.
3. **Ground truth субъективен.** Severity между ЭКОНОМИЧЕСКОЕ /
   ЭКСПЛУАТАЦИОННОЕ / ПРОВЕРИТЬ_ПО_СМЕЖНЫМ — оценочна; LLM может
   расходиться с GT по severity при правильном по сути замечании.
4. **`is_beyond_gt_useful`** не размечен для синтетических кейсов — это
   значит, что LLM, выдающие полезные за-GT-замечания, не получат бонус
   на `balanced_engineering`-score'е.
5. **Inter-rater agreement** ground truth не валидирован вторым инженером.
   Для production-grade оценки нужен экспертный пересмотр.

## Что осталось (не выполнено)

- Многораундовое (3×) исполнение A1-v2 на 6 информативных кейсах
  (cross_01, ov_01, kj_01, eom_*, tz_vs_rd, specification_only) для
  оценки stochasticity — отложено в Stage 5 (отдельно отмечено в финальном
  отчёте).
- Полное сравнение A2/A3/A4 на расширенном датасете — выходит за рамки
  Phase 1 validation; решение по A2-A5 откладывается до отдельной задачи.
- Manual inter-rater review ground truth по новым 16 кейсам — отложено;
  при подготовке production-grade оценки требуется экспертный пересмотр.

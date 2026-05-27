# Golden Dataset Strategy

**Дата:** 2026-05-20
**Reference:** [FINAL_SUMMARY §1](../../algorithm_research/reports/FINAL_SUMMARY.md),
[dataset_expansion_report.md](../../algorithm_research/reports/dataset_expansion_report.md).

---

## Состав

24 кейса:
- 8 original (pre-2026-05).
- 16 new (созданы в 2026-05-20 session).

Покрытие:
- 7 дисциплин × ≥ 3 кейса = 21.
- 3 multi/cross.
- 4 document_types: `full_rd` (17), `audit_comparison` (3), `tz_vs_rd` (2),
  `specification_only` (3).

Каждый кейс — отдельная директория `datasets/<case_id>/`:
- `case.json` — метаданные (project_id, discipline, document_type, signal flags).
- `md_input.md` — синтетическая Markdown-репрезентация проекта (~50-200 строк).
- `ground_truth.json` — список GT findings с substring markers + FP-traps +
  `is_beyond_gt_useful` markers.

---

## Как был построен GT

### Источник
- Каждый кейс — синтетический, написан вручную в session 2026-05.
- Кейсы отражают **реалистичные ошибки проектирования**, наблюдаемые в реальных
  МКД-проектах (по опыту engineering team).
- Не результат реального аудита настоящих документов (для repeatable
  experiments).

### Sign flags
Каждый кейс имеет:
- `signals` — ярлыки intent'а кейса (e.g. `"prompts_routing_test"`,
  `"cap_stress_test"`).
- `false_positive_traps` — список текстов finding'ов, которые pipeline НЕ должен
  выдать (типичные ловушки: vendor names, устаревшие нормы упомянутые-но-не-применённые).
- `is_beyond_gt_useful_examples` — допустимые beyond-GT findings (если будут
  выданы — это плюс, не минус).

### Substring matching
GT findings содержат `substring` — точная подстрока, которую должен содержать
production finding для match. Это compromise:
- Точнее чем semantic match (less false positives in matching).
- Хуже чем semantic match (false negatives когда LLM rephrase'ит).

[FINAL_SUMMARY §4.4](../../algorithm_research/reports/FINAL_SUMMARY.md)
обозначает improve `compare_results.evaluate_case` как future task.

---

## Ограничения GT

1. **Синтетические данные.** Реальные проекты могут иметь edge cases, не
   покрытые этим набором.
2. **Покрытие узкое.** 3 кейса на дисциплину — лучше чем 1, но статистическая
   значимость per discipline ограничена.
3. **Single-author bias.** Все кейсы от одного автора в одну сессию — есть
   риск что pipeline учится на стиле, а не на сути.
4. **Substring-based matching** — pipeline может пропустить GT если rephrase'ит
   абстрактно (см. FINAL_SUMMARY §5: cross_01 v2 = strict_score 37.4 хотя
   semantically correct).
5. **`is_beyond_gt_useful` annotations** — частично subjective; engineer review
   нужен.

---

## Validation требования (до production использования)

1. **Inter-rater review.** ≥ 2 engineers проходят GT, маркируют:
   - подтверждается / не подтверждается.
   - отсутствующие GT (false negatives в GT).
2. **Discipline review.** Каждая дисциплина — review от proficient engineer
   в этой дисциплине.
3. **FP trap audit.** Убедиться что `false_positive_traps` действительно
   "trapping" (не просто generic statements).

Эти 3 пункта **не сделаны** в session 2026-05-20. Это требует separate task.
Используем golden dataset с этим caveat.

---

## Расширение

Future expansions:
- ≥ 5 кейсов на дисциплину (currently 3) для variance estimate.
- Реальные проекты-anonymized (если получаем sample'ы от production).
- 30-finding sample multi_agent FPs labelled (см. final_verdict.md "Dataset
  expansion need").
- Дополнительные document_type variations:
  - `audit_comparison` × 5 (currently 3) — это hottest path для Phase 1.
  - `specification_only` × 5 (currently 3) — important для cap calibration.

---

## Версионирование

GT кейсы **immutable once tagged**. Любое изменение существующего кейса:
- Создаётся `<case_id>_v2/` рядом.
- Старый `<case_id>/` остаётся для historical regression.
- `tests/regression/_baselines/` хранит baselines для **обеих** версий.

Это нужно потому, что:
- A baseline снимок зависит от точного GT.
- Изменение GT инвалидирует все historical baselines.
- Поэтому GT versioning — explicit, not implicit.

Новые кейсы — просто `<case_id>` (без suffix).

Naming convention:
- `<discipline>_<NN>_<short_topic>` — нет version → v1.
- `<discipline>_<NN>_<short_topic>_v2` — explicit version.

---

## Использование в pipeline

```python
# tests/regression/run_24_case_golden.py
DATASET_ROOT = Path("experiments/md_analysis_comparison/datasets")
ALL_CASES = sorted(p.name for p in DATASET_ROOT.iterdir() if (p / "case.json").exists())

for case_id in ALL_CASES:
    case = load_case(case_id)
    md_content = (DATASET_ROOT / case_id / "md_input.md").read_text()
    gt = load_gt(case_id)

    result = run_pipeline_locally(md_content, project_info=case)
    metrics = evaluate_case(result, gt)
    save_run(case_id, result, metrics)
```

Локальный pipeline run — это:
- Mock'нутый ctx + claude_runner subprocess invocation (либо реальный subscription).
- НЕ требует реальный `projects/...` directory.
- Можно запускать isolated в CI runner.

---

## Связь с experiments-стендом

Production tests **импортируют** dataset из `experiments/.../datasets/`. Это
unusual — обычно tests копируют data в `tests/`. Здесь сознательно:
- Dataset большой (~24 × 3 файла).
- Версионируется в одном месте.
- Изменения dataset нужно делать в одном месте.

Risk: если кто-то рандомно правит `experiments/.../datasets/<case_id>/`, ломаются
production regression tests. **Mitigation:** CI-checksum baseline + warning в
README.md эксперимента.

---

## Сводка

| Признак | Значение |
|---|---|
| Cases | 24 |
| Disciplines covered | 7 + multi/cross |
| Doc_types covered | 4 |
| GT кейсов synthetic | 100% |
| Inter-rater reviewed | NO (TBD) |
| Substring-matched | YES (limitation) |
| Immutable once tagged | YES |
| Used by regression | YES |
| Used by stochasticity (subset of 6) | YES |
| Used by canary | NO (canary uses real projects) |

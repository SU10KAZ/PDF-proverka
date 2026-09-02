# Candidate Generator v4 — benchmark report

## Итог

Вердикт: **A — Candidate Generator готов для повторного AI experiment**.

Это изолированный deterministic research generator. `production-sheet-matcher.v3`, UI,
AI Selector, engineer mappings и production pipeline не изменялись. Model calls не выполнялись;
materialization, deploy и push не выполнялись. Reference hypotheses использованы только при
аудите результата и не подмешивались в retrieval/ranking.

## Архитектура v4

- каждый лист имеет provenance-bearing Sheet Passport, поверх него построены один или несколько
  Function Passports;
- каждый Function Passport независимо оценивается против полного RIGHT corpus; результаты
  объединяются с шестью каналами `FUNCTION`, `ENTITY`, `OBJECT_ZONE`, `TOPOLOGY`,
  `TITLE_STAMP`, `NEIGHBOR_TOC`;
- bounded weak bridge сохраняет полезные deterministic v3 signals, но не ограничивает
  corpus-wide v4 retrieval; page proximity имеет только слабый вес;
- null остаётся нейтральным, а только явные corpus/function contradictions получают отдельный
  штраф; ни один contradiction не удаляет кандидата до ranking;
- группы строятся до любой assignment по lineage, scope, sheet series и complementary role
  coverage. v4 не выполняет 1→1 assignment и сохраняет конфликтующие варианты.

## V3 vs v4

| Проект | v3 R@1 | v3 R@3 | v3 R@5 | v3 R@10 | v4 R@1 | v4 R@3 | v4 R@5 | v4 R@10 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ИОС 1.1 | 12.5% | 65.0% | 72.5% | 87.5% | 67.5% | 85.0% | 97.5% | 100.0% |
| ИОС 3.1 | 0.0% | 75.0% | 75.0% | 100.0% | 37.5% | 100.0% | 100.0% | 100.0% |
| ИОС 2.1 | 4.5% | 22.7% | 22.7% | 40.9% | 59.1% | 77.3% | 90.9% | 95.5% |
| **Итого** | **8.6%** | **52.9%** | **57.1%** | **74.3%** | **61.4%** | **84.3%** | **95.7%** | **98.6%** |

Engineer mapping recall@10: `100.0%`.
Reference hypothesis recall@10: `94.4%`.
Single-page recall@10: `100.0%`.

## IOS 2.1 authority checks

- `17→7`: rank `5`
- `18→8`: rank `2`
- `19→9`: rank `4`

## A sheet 5 distributed candidate

- candidate: `fcand_6294159aac7851a636dd`
- relation: `FUNCTION_DISTRIBUTED`
- RIGHT physical pages: `[26, 28, 29]`
- covered functions: `['DOMESTIC_PRESSURE_BOOST', 'FIRE_PRESSURE_BOOST', 'INCOMING_METERING']`
- grounds: `['bounded set cover of distinct deterministic equipment roles', 'each member is the strongest same-role fragment in the retrieval union']`

## Boundedness and groups

- candidate count per LEFT: median `10.0`, p95 `10.0`;
- returned/cartesian pairs: `1380` / `6910` (`20.0%`);
- group candidates: `{'FUNCTION_DISTRIBUTED': 533, 'SPLIT_1_TO_N': 273, 'MERGED_N_TO_1': 240}`;
- exact group recall: `{'case_count': 7, 'exact_group_hits': 7, 'recall': 1.0}`.

## Failure delta (IOS 2.1 forensic scope)

Before: `{'SEARCH_WINDOW_MISS': 2, 'RANKING_MISS': 5, 'GROUP_CANDIDATE_MISSING': 3, 'GLOBAL_ASSIGNMENT_DISPLACEMENT': 3}`.

After: `{'SEARCH_WINDOW_MISS': 0, 'RANKING_MISS': 2, 'GROUP_CANDIDATE_MISSING': 0, 'GLOBAL_ASSIGNMENT_DISPLACEMENT': 0}`.

## Remaining root causes

- Remaining benchmark misses: `['ИОС 2.1 LEFT 20 → [29, 30] (reference_hypothesis)']`. IOS 2.1 LEFT 20 → `[29,30]` остаётся
  page-level reference miss, но physical 29 сохранена внутри доказательной группы `[26,28,29]`.
- Forensic individual-edge ranks 20→28=`8` and
  20→29=`None` explain the two remaining ranking
  misses; both edges are retained by the group candidate rather than an unbounded page list.
- Function classes are deterministic lexical normalizations of saved OCR/image descriptions,
  not model-generated engineering facts.
- One-to-many composition is bounded to three RIGHT pages and eight groups per LEFT; merge and
  distributed LEFT-pair expansions are capped at two per adjacent pair. Related-document
  expansion remains evidence-only and is not performed when the document is unavailable.

## Verification

- `python -m pytest -q tests/test_candidate_generator_v4.py tests/test_ai_sheet_matcher_experiment.py`
- 26 tests passed: 13 targeted v4 tests and 13 unchanged AI-research safety tests.
- Production source and frozen artifact hashes remain unchanged after generation.

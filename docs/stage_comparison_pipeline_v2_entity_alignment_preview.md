# Pipeline V2 — Mapping-aware Graphic Entity Alignment Preview (mark-only)

**Дата:** 2026-06-12
**Статус:** offline / mark-only слой; в dry-run включён по умолчанию (fail-soft).
**Модуль:** [pipeline_v2_entity_alignment_preview.py](../backend/app/services/stage_comparison/pipeline_v2_entity_alignment_preview.py)

## Зачем

При попытке расширить runtime vision/grounding по ИОС 1.1 выяснилось: «новых
кандидатов» на vision много, но большинство — это **разные сущности**, потому
что между стадиями щиты переименовали/реорганизовали:

```text
ВРУ-3 OLD ↔ ВРУ-2 NEW      # разные щиты (реорганизация состава)
ЯК   OLD ↔ ЩО-3 NEW        # разные family
ВРУ-4 OLD ↔ ВРУ-А NEW      # возможно переименование, нужна проверка
```

Обычный entity-aware отбор кандидатов на vision
([pipeline_v2_graphic_vision_enrichment](../backend/app/services/stage_comparison/pipeline_v2_graphic_vision_enrichment.py))
схлопывает все эти случаи в `mismatch_likely`, не различая переименование,
реорганизацию и настоящий mismatch. Этот слой даёт **более тонкую
классификацию**, чтобы решать, что безопасно слать в нормальный enrichment, что
требует ручного сопоставления, а что вообще не сравнивать блок-в-блок.

Это **mark-only** аналитический слой: НЕ запускает vision, НЕ применяет связи,
НЕ создаёт замечаний. Только читает готовые Pipeline V2 артефакты и пишет
`entity_alignment_preview_report.json`.

## Классы выравнивания

| classification | смысл | recommended_action |
|---|---|---|
| `same_entity_likely` | ВРУ-3 ↔ ВРУ-3, ГРЩ ↔ ГРЩ — одна сущность | `use_for_enrichment` |
| `possible_rename` | та же family, номер/имя другие, НО сильные признаки идентичности (совпадает аппаратный состав) | `manual_mapping` |
| `scope_reorganized` | та же family, номер конфликтует, аппараты/состав разные — реальная переработка | `manual_mapping` |
| `mismatch_likely` | разные family (ЯК↔ЩО), схема↔план, ОЗДС↔квартиры | `exclude_from_enrichment` |
| `link_validation_candidate` | слабая/спорная связь — только для проверки слабой связи | `link_validation_only` |

`needs_manual_mapping` = `possible_rename` + `scope_reorganized`.

### Почему `ВРУ-3 ↔ ВРУ-2` нельзя в обычный enrichment

У них совпадает только **family** (ВРУ), но номер конфликтует (3 vs 2) — это
`numbered_conflict`. Если без проверки прогнать такую пару через vision и
grounding, аппараты ВРУ-3 (OLD) будут «сравниваться» с аппаратами ВРУ-2 (NEW), и
почти каждый номинал/линия даст ложное `changed`. Это и есть ложный grounding +
риск OLD/NEW-путаницы. Поэтому такие пары → `scope_reorganized` (ручное
сопоставление), а НЕ `same_entity`.

`possible_rename` отличается от `scope_reorganized` **только** наличием сильной
корроборации идентичности — совпадения аппаратного состава
(`equipment_overlap` ≥ 0.4 или `grounded_entities_overlap` ≥ 0.4). Сходство
заголовка листа НЕ используется как решающий сигнал: заголовки шаблонные и
отличаются только номером (хранится в `evidence.sheet_title_similarity` для
прозрачности, но в решение не входит).

## Логика классификации (`classify_entity_alignment`)

Переиспользует базовый `score_vision_candidate` + entity-хелперы из
`pipeline_v2_graphic_vision_enrichment` (единая нормализация маркировок:
`extract_entity_ids`, `entity_identity_signal`, `sheet_kind_of`,
`_domain_signature`). Порядок решения:

1. `identity == match` (общая нумерованная маркировка) и нет конфликта вида
   листа → **same_entity_likely**.
2. `family_conflict` ИЛИ `sheet_kind_mismatch` (схема↔план) ИЛИ `domain_mismatch`
   (ОЗДС↔квартиры) → **mismatch_likely**.
3. `identity == numbered_conflict` (та же family, разные номера):
   - сильная корроборация аппаратов (`equipment_overlap`/`grounded_overlap`) +
     совместимые тип/дисциплина → **possible_rename**;
   - иначе → **scope_reorganized**.
4. базовый класс `same_entity_likely` (например ГРЩ↔ГРЩ, family_only_match с
   высоким score) → **same_entity_likely**.
5. базовый класс `mismatch_likely` → **mismatch_likely**.
6. иначе (слабое/неоднозначное) → **link_validation_candidate**.

`primary identity` берётся по `sheet_name` (наименование листа), а не по пулу
упоминаний: упоминание ГРЩ на схеме ВРУ-2 не должно ни подтверждать, ни
маскировать конфликт.

## Entity labels (`extract_entity_labels`)

Извлекает маркировки из `sheet_name` / текста / токенов дескриптора и возвращает
`{labels[], primary, family, number, confidence}`. primary — наиболее
специфичная нумерованная метка известной family (ВРУ-3 предпочтительнее голого
ВРУ). Распознаются ГРЩ, ВРУ-1…ВРУ-А, ЩО-3, ЩАО-1, ЯК/ЯК1, АВР, ШК, ОЗДС, ТП,
ИТП, ЩР, РП, РУ и т.п.

## Связь с block link preview и link_validation

- **block_link_preview** — витрина предложенных связей блоков (что с чем
  сопоставлено). Этот слой объясняет, **корректна ли сущностно** такая связь.
- **link_validation** — режим vision для целенаправленной проверки слабых/
  подозрительных связей. `link_validation_candidate` / `scope_reorganized` —
  естественные кандидаты для него (НЕ для обычного enrichment).

## Входные артефакты

| Артефакт | Обязателен | Назначение |
|---|---|---|
| `visual_equivalence_gate_report.json` | да | список граф. блок-пар |
| `left/right_graphic_descriptor_report.json` | желательно | sheet_name/type/discipline/tokens |
| `block_matching_report.json` | optional | контекст (передаётся как block-match) |
| `block_link_preview_report.json` | optional | контекст |
| `graphic_descriptor_matched_report.json` | optional | token_overlap / match_quality |
| `graphic_vision_grounding_report.json` | optional | grounded-сущности (rename-сигнал) |
| `left/right_normalized_document_model.json` | optional | fallback sheet_name по странице |

Без gate → `status=completed_with_warnings`, пустой report (fail-soft). Нет
дескрипторов → labels деградируют до gate-only, report не падает.

## Контракт отчёта

`entity_alignment_preview_report.json`:

```json
{
  "version": 1,
  "kind": "stage_comparison_pipeline_v2_entity_alignment_preview",
  "status": "ok|completed_with_warnings",
  "summary": {
    "graphic_pairs_total": 0, "same_entity_likely": 0, "possible_rename": 0,
    "scope_reorganized": 0, "mismatch_likely": 0, "link_validation_candidate": 0,
    "needs_manual_mapping": 0, "unpaired_left": 0, "unpaired_right": 0
  },
  "pairs": [
    {"pair_key": "...", "left_block_id": "...", "right_block_id": "...",
     "left_page_number": 28, "right_page_number": 27,
     "left_sheet_name": "...", "right_sheet_name": "...",
     "left_entity_label": "ВРУ-3", "right_entity_label": "ВРУ-2",
     "entity_family": "ВРУ", "classification": "scope_reorganized",
     "confidence": 0.6, "reasons": [...], "risk_flags": [...],
     "recommended_action": "manual_mapping",
     "evidence": {"sheet_title_similarity": 0.0, "entity_id_match": false,
       "entity_family_match": true, "numbered_entity_conflict": true,
       "discipline_match": true, "graphic_type_match": true,
       "visual_status": "changed_visual", "grounded_entities_overlap": null,
       "equipment_overlap_informative": null}}
  ],
  "unpaired_entities": {"left": [...], "right": [...]},
  "warnings": []
}
```

## Интеграция в dry-run

`run_pipeline_v2_dry_run` добавляет этап **[3c2] entity_alignment_preview** после
block_link_preview [3c] и до graphic_vision [3d]: классифицирует пары gate +
descriptors + (опц.) grounding, пишет `entity_alignment_preview_report.json`,
добавляет в манифест и summary-секцию. Default ON, fail-soft (падение не валит
pipeline). `options.entity_alignment_preview.enabled=false` отключает.

## UI payload

`build_pipeline_v2_ui_payload` добавляет (если слой включён):

```json
"entity_alignment_preview": {
  "available": true, "same_entity_likely": 0, "possible_rename": 0,
  "scope_reorganized": 0, "mismatch_likely": 0, "needs_manual_mapping": 0
}
```

Frontend в этой задаче НЕ менялся.

## Будущий wiring в graphic vision selection (НЕ в этой задаче)

`pipeline_v2_graphic_vision_enrichment` может опционально читать
`entity_alignment_preview_report.json` (`options.use_entity_alignment_preview`)
и фильтровать кандидатов:

- `same_entity_likely` → можно в enrichment;
- `possible_rename` → только при `include_possible_rename=true` / high confidence;
- `scope_reorganized` → manual_review / link_validation;
- `mismatch_likely` → исключить из enrichment;
- `link_validation_candidate` → только `selection_mode=link_validation`.

`entity_alignment_by_pair_key(report)` даёт индекс `pair_key → классификация`
для такого wiring. В этой задаче слой **report-only** (selection не меняется).

## Безопасность

- mark-only: vision/grounding/связи не запускаются и не применяются;
- fail-soft на уровне пары и слоя;
- никаких сетевых/LLM-вызовов (тест `test_14_no_llm_or_vision_imports`).

## Smoke (ИОС 1.1, read-only)

На реальной паре `ba413a93c5754f6c/pf06effb7` (54 граф. пары):
same_entity_likely 7 (4 реальные схемы ВРУ-1/ВРУ-3/ВРУ-4/ГРЩ + 3 штампа),
scope_reorganized 5 (ВРУ-3↔ВРУ-2, ВРУ-2↔ВРУ-1, ВРУ-4↔ВРУ-А …), mismatch_likely
17 (ЯК↔ЩО/ЩАО, ЩР↔ЩО, ВРУ-1↔ГРЩ/ТП, 7VMV legend), link_validation_candidate 25,
possible_rename 0 (ни одна numbered_conflict-пара не имеет сильной аппаратной
корроборации — панели реально реорганизованы, а не просто переименованы).

## Тесты

[tests/test_stage_comparison_pipeline_v2_entity_alignment_preview.py](../tests/test_stage_comparison_pipeline_v2_entity_alignment_preview.py)
— 14 spec-кейсов + helpers.

## Связанные файлы

- [pipeline_v2_entity_alignment_preview.py](../backend/app/services/stage_comparison/pipeline_v2_entity_alignment_preview.py)
- [pipeline_v2_graphic_vision_enrichment.py](../backend/app/services/stage_comparison/pipeline_v2_graphic_vision_enrichment.py) — entity-хелперы + базовый scoring
- [pipeline_v2_dry_run.py](../backend/app/services/stage_comparison/pipeline_v2_dry_run.py) — этап [3c2]
- [pipeline_v2_ui_payload.py](../backend/app/services/stage_comparison/pipeline_v2_ui_payload.py) — summary

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

## Read-only endpoint + UI (runtime)

`GET /api/stage-comparison/pipeline-v2/{session_id}/entity-alignment-preview`
(`?pair_id=&classification=&limit=&offset=`) — read-only выдача
([pipeline_v2_payload_service.discover_entity_alignment_preview](../backend/app/services/stage_comparison/pipeline_v2_payload_service.py)
→ [pipeline_v2_entity_alignment_detail.build_entity_alignment_detail](../backend/app/services/stage_comparison/pipeline_v2_entity_alignment_detail.py)):

- отдаёт готовый `entity_alignment_preview_report.json`, либо собирает его
  **on-the-fly** из артефактов dry-run (visual gate + descriptors + models +
  опц. matched/grounding) — ничего не пишет, не запускает, не вызывает модели;
- статусы: `ok` (готовый/собранный), `not_found` (нет ни отчёта, ни gate),
  `error` (битый JSON) — НЕ 404/500;
- фильтр `classification` (`same_entity_likely` … `link_validation_candidate` /
  `all`) и пагинация (`limit` clamp ≤500, `offset`) применяются к `pairs`;
  `summary` и `unpaired_entities` отдаются целиком;
- **no raw leak**: pairs/evidence режутся по whitelist'у (только скаляры/булевы
  признаки + короткий `visual_status`), длинные имена листов усекаются; raw
  Qwen / большие anchor-тексты не отдаются (тест `test_8_no_raw_text_leak`);
- путь НЕ в auth-exempt (PortalAuthMiddleware отдаёт 401 анониму).

**Frontend** (read-only панель в «Сравнение стадий → 2. Связь блоков»):
кнопка **«🧩 Pipeline V2 сущности β»** рядом с «🔗 Pipeline V2 связи β»
открывает панель `scPv2Ea*` ([app.js](../frontend/static/js/app.js)):

- summary-карточки (same/rename/scope/mismatch/link_validation/needs_manual_mapping/unpaired);
- фильтры по классу + «Без пары»; карточки пар OLD↔NEW с цветом класса,
  уверенностью, reasons, risk_flags, рекомендацией;
- список unpaired-сущностей (OLD/NEW) — «требует ручного маппинга»;
- read-only переход **«🔗 Открыть связь блоков»** (переиспользует мост
  `scPv2OpenBlockLinkFromGrounding` → подсветка пары в «Связь блоков», связь НЕ
  применяется);
- **никаких** кнопок «Подтвердить / Перепривязать / Применить» — явная пометка,
  что ручной маппинг это будущий этап (`test`: read-only contract по
  index.html / app.js).

Тесты: [tests/test_stage_comparison_pipeline_v2_entity_alignment_endpoint.py](../tests/test_stage_comparison_pipeline_v2_entity_alignment_endpoint.py)
(10 backend-кейсов), [frontend/tests/pipeline_v2_panel.test.js](../frontend/tests/pipeline_v2_panel.test.js)
(блок «Entity Alignment …»: summary/cards/filters/states + read-only contract).

## Manual entity mapping overrides (write-слой)

Поверх read-only preview инженер сохраняет ручные решения по парам/сущностям —
отдельный **обратимый** artifact, который НЕ меняет ни preview, ни связи блоков,
ни сравнение/findings, и НЕ запускает vision/Qwen/Opus/Claude/jobs.

**Модуль:** [pipeline_v2_entity_mapping_overrides.py](../backend/app/services/stage_comparison/pipeline_v2_entity_mapping_overrides.py).
**Artifact:** `entity_mapping_overrides.json` в `pipeline_v2/` пары:

```json
{
  "version": 1, "kind": "stage_comparison_pipeline_v2_entity_mapping_overrides",
  "status": "ok", "session_id": "...", "pair_id": "...", "updated_at": "...",
  "mappings": [{
    "mapping_id": "m_<sha1>", "left_entity_label": "ВРУ-3",
    "right_entity_label": "ВРУ-2", "left_block_id": "...", "right_block_id": "...",
    "left_page_number": 28, "right_page_number": 26,
    "source_classification": "scope_reorganized",
    "manual_decision": "confirmed_same_entity|confirmed_rename|confirmed_reorganized|rejected_mapping|no_match",
    "confidence": "manual_confirmed", "comment": "...",
    "created_at": "...", "created_by": "...", "updated_at": "..."
  }],
  "rejected": [], "no_match": [], "history": []
}
```

`mapping_id` детерминирован по идентичности пары (приоритет block-ids, иначе
метки+класс) → **upsert идемпотентен** (повторное сохранение обновляет на месте,
не дублирует). `rejected`/`no_match` — derived-вью из canonical `mappings`;
`history` — append-only лог (cap). Запись atomic (tmp+`os.replace`), read
fail-soft (битый файл → пустой ok+warning). Path traversal обрезается (`_safe_id`,
строгая проверка id). confirmed_* требует обе стороны; no_match/rejected — одна.

**Endpoints** (read/write, в threadpool, mark-only):

| Метод | Путь | Действие |
|---|---|---|
| GET | `/pipeline-v2/{sid}/entity-mapping-overrides?pair_id=` | текущие override'ы (пустой ok если нет) |
| PUT | `/pipeline-v2/{sid}/entity-mapping-overrides?pair_id=` | upsert одного решения (`{mapping, created_by}`) → `{ok, override, created, summary}` |
| DELETE | `/pipeline-v2/{sid}/entity-mapping-overrides/{mapping_id}?pair_id=` | удалить override (обратимо) |

Невалидный decision → 422; невалидный id → 400. GET резолвит путь без `mkdir`.

**Интеграция в preview.** `discover_entity_alignment_preview` читает overrides
(fail-soft, без mkdir) и `build_entity_alignment_detail` добавляет к КАЖДОЙ
карточке/сущности `manual_mapping`:

```json
"manual_mapping": {"status": "mapped|rejected|no_match|none",
  "decision": "confirmed_reorganized", "mapping_id": "...",
  "comment": "...", "updated_at": "..."}
```

и в summary — агрегат `manual_mapping: {total, confirmed, rejected, no_match}`.
Нет overrides → `status:"none"` на карточках, агрегат не добавляется (как раньше).

**Frontend.** В панели «🧩 Pipeline V2 сущности β» каждая карточка пары и каждая
unpaired-сущность получают select решения + комментарий + «💾 Сохранить решение»
(unpaired confirmed_* — ещё counterpart-picker с другой стороны). После
сохранения показывается статус и подсказка, что block links/vision НЕ
запускаются. Кнопок «Применить к связям»/«Запустить vision» НЕТ.

Тесты: [tests/test_stage_comparison_pipeline_v2_entity_mapping_overrides.py](../tests/test_stage_comparison_pipeline_v2_entity_mapping_overrides.py)
(10 backend-кейсов), блок «Manual Entity Mapping» в
[frontend/tests/pipeline_v2_panel.test.js](../frontend/tests/pipeline_v2_panel.test.js).

## Wiring overrides → graphic vision candidate selection (2026-06-12)

Реализовано: `select_vision_candidates_v2(..., overrides_report=...)` в
[pipeline_v2_graphic_vision_enrichment.py](../backend/app/services/stage_comparison/pipeline_v2_graphic_vision_enrichment.py)
учитывает ручные решения при отборе кандидатов на vision. Это всё ещё
**mark-only**: НЕ применяет block links, НЕ запускает vision/Qwen/Opus/Claude,
НЕ создаёт замечаний — только определяет enrichment / link_validation /
исключение.

Primary source — `entity_mapping_overrides.json` (этот слой); вторичный —
`manual_mapping` в `entity_alignment_preview_report.json`. Матч кандидата →
override: **block-ids → pair_key → labels → mapping_id**
(`index_overrides_for_lookup` / `find_override_for_pair`).

Правила по `manual_decision` (полная таблица + почему `confirmed_reorganized` по
умолчанию идёт в link_validation, а не enrichment — см.
[stage_comparison_pipeline_v2_graphic_vision_enrichment.md](stage_comparison_pipeline_v2_graphic_vision_enrichment.md#manual-entity-mapping-overrides-in-candidate-selection-2026-06-12)):

- `confirmed_same_entity` / `confirmed_rename` → `same_entity_likely`, в enrichment (score boost);
- `confirmed_reorganized` → `manual_confirmed_reorganized`, по умолчанию ТОЛЬКО link_validation (приоритет); в enrichment лишь при `include_confirmed_reorganized=true` (+ `requires_human_review`);
- `rejected_mapping` → исключён из enrichment (и link_validation, кроме debug);
- `no_match` → исключён из обоих.

Options (default OFF, старое поведение сохранено):
`use_entity_mapping_overrides`, `manual_mapping_mode` (`enrichment|link_validation|both`),
`include_confirmed_reorganized`, `manual_mapping_debug`.

`entity_alignment_by_pair_key(report)` остаётся для preview-индексации.

## Link Validation Report (vision-проверка manual mapping, 2026-06-12)

Следующий слой поверх manual mapping: для пар `confirmed_reorganized` (и
других link-validation-кандидатов) vision проверяет, одна ли это
реорганизованная сущность или разные, и сверяет вердикт с ручным решением
(agreement / **conflict**). Mark-only, НЕ grounded-факт, runner инъектируется
(None → skipped_no_runner). Контракт, agreement-таблица и controlled validation
(ИОС 1.1: manual `confirmed_reorganized` ↔ vision `different_entity` = conflict)
описаны в
[stage_comparison_pipeline_v2_graphic_vision_enrichment.md → Link Validation Report](stage_comparison_pipeline_v2_graphic_vision_enrichment.md#link-validation-report-mark-only-проверка-manual-mapping-2026-06-12).
Модуль: [pipeline_v2_link_validation.py](../backend/app/services/stage_comparison/pipeline_v2_link_validation.py).

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

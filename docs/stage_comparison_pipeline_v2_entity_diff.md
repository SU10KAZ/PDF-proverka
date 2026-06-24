# Stage Comparison Pipeline V2 — Deterministic Entity Diff OLD↔NEW (этап 4)

**Дата:** 2026-06-10
**Статус:** новый изолированный режим, этап 4 — **детерминированный diff
(observe / read-only)**. Старую логику Stage Comparison НЕ заменяет.
**Модуль:** [backend/app/services/stage_comparison/pipeline_v2_entity_diff.py](../backend/app/services/stage_comparison/pipeline_v2_entity_diff.py)
**Базируется на:** [этап 1 — Ingest](stage_comparison_pipeline_v2_prepared_package_ingest.md) + [этап 2 — Block Matching](stage_comparison_pipeline_v2_block_matching.md) + [этап 3 — Entity Extraction](stage_comparison_pipeline_v2_entity_extraction.md)

## Зачем нужен deterministic entity diff

Этапы 1–3 нормализуют комплект, сопоставляют блоки и извлекают сравнимые
сущности. Этап 4 ДЕТЕРМИНИРОВАННО строит список атомарных отличий OLD↔NEW
(deltas): что добавлено, удалено или изменено — без LLM.

```text
entity_extraction_report
  → per matched block pair:
        match_entities (exact_key → identity → fuzzy)
        → compare_matched_entities (field-level, с нормализацией)
        → added / removed / changed / unchanged / uncertain
  → unmatched blocks: one-sided added/removed (low-info → uncertain)
  → entity_diff_report.json
```

## Почему Opus больше не должен искать отличия по всему тому

Раньше Opus получал на вход весь том (или огромные чанки) и сам «искал отличия» —
отсюда нестабильность («34 замечания на том vs 24 на лист»), потеря per-sheet
структуры и невоспроизводимость. Здесь отличия находит **детерминированный код**
по уже заякоренным сущностям. На следующем этапе LLM лишь **объясняет готовые
deltas** (что это значит, влияние), а не ищет их по всему документу. Это делает
diff воспроизводимым, заякоренным на блок/лист/сущность и дешёвым.

## Поддержанные `entity_type`

`stamp_field`, `document_section`, `change_log_item`, `contents_item`,
`requirement`, `norm_reference`, `equipment`, `cable`, `power_supply`,
`scheme_component`, `scheme_connection_hint`, `table_row`, `unknown`.

## Как сопоставляются сущности

Сопоставление идёт **внутри пары сопоставленных блоков** (`matched_block_entities`
из этапа 3), только между сущностями одного `entity_type`, жадно 1:1 по убыванию
score. Два ключа на сущность:

- **`make_entity_match_key`** — точный ключ (одинаковая сущность с одинаковым
  значением); даёт `exact_key`/`subject_type` (score 1.0);
- **`make_entity_identity_key`** — «логический» ключ (та же сущность, значение
  могло измениться); даёт `normalized_key`/`numeric_overlap` (score 0.8).

Проходы: **exact** → **identity** → **fuzzy** (`requirement`/`document_section`
по `SequenceMatcher`). Остаток слева → `removed`, справа → `added`.

| entity_type | match_key (точный) | identity_key (логический) |
|---|---|---|
| `stamp_field` | имя поля (`subject`) → сравнить value | то же (по имени поля) |
| `norm_reference` | полный код `сп 256.1325800.2016` | база без года/редакции `сп 256.1325800` |
| `equipment` | канон + синонимы (`PoE/POE/РоЕ→poe_switch`, `шкаф СВН→shk_svn`) | то же |
| `cable` | марка+категория+**сечение** (`utpcat5e`, `…1x2x0.5`) | семейство **без сечения** |
| `power_supply` | нормализованное значение (`220b`, `0.5a`) | вид (`voltage`/`current`/`ups`/`category`) |
| `contents_item` | `document_code`+`sheet_name` (без страницы) | `sheet_name` |
| `change_log_item` | `change_no`+`sheet` | `change_no` |
| `table_row` | нормализованные cells целиком | первая ячейка (метка строки) |
| `scheme_component` | нормализованное имя | то же |
| `requirement` | полный текст | первые токены / fuzzy |

Это даёт: одинаковые после нормализации → exact → `unchanged`; изменилась
редакция нормы / сечение кабеля / напряжение → identity-матч → `changed`;
пропало/появилось → `removed`/`added`.

## Field-level сравнение и что НЕ считается отличием

`compare_matched_entities` (+ `compare_entity_fields` для словаря `fields`)
сравнивает `value`/`unit`/`fields.<key>`/`fields.cells` с **нормализацией по
типу** (`normalize_entity_value`, `normalize_cable_value`, `normalize_power_value`,
канон нормы/оборудования). НЕ создаёт дельту, если различие только:
регистр, пробелы, ё/е, латиница↔кириллица-гомоглифы (симметричный fold),
`220В`↔`220 В`, `cat.5e`↔`cat. 5Е`, `PoE`↔`POE`↔`РоЕ`.

Создаёт дельту, если изменилось: сечение/категория кабеля, напряжение,
количество, номер листа, стадия, шифр, название листа, оборудование,
наличие/отсутствие элемента, числовой токен в требовании и т.п.
`extract_numeric_tokens` (`1x2x0,5`→`['1','2','0.5']`) помечает `numeric_change`.

## Формат delta

```json
{
  "delta_id": "delta_<left>__<right>__<field>",
  "delta_type": "added|removed|changed|uncertain",
  "entity_type": "...", "semantic_group": "...",
  "left_entity_id": "...", "right_entity_id": "...",
  "left_block_id": "...", "right_block_id": "...", "block_match_id": "bm_...",
  "page_numbers": {"left": 1, "right": 1},
  "subject": "...", "field": "value|name|subject|unit|fields.<key>|presence",
  "old_value": "...", "new_value": "...", "change_summary": "...",
  "confidence": 0.0,
  "evidence": {"left": {"quote","source","block_id","page_number"}, "right": {…}},
  "match": {"method": "exact_key|normalized_key|subject_type|numeric_overlap|fuzzy|fallback",
            "score": 0.0, "reasons": []},
  "quality_flags": []
}
```

`unchanged`-дельты в `deltas` НЕ включаются (считаются в
`summary.matched_unchanged_total`).

## Confidence и quality_flags

**Confidence** (float 0..1): `changed` при exact/subject_type → high (~0.85),
при normalized/numeric_overlap → medium (~0.65), `numeric_change` поднимает;
fuzzy ограничен score'ом; `added`/`removed` → ~0.7 (one-sided c непарного блока
понижается до ≤0.6); `uncertain` → ~0.3; нехватка evidence/низкий score —
штраф. Бакеты `summary.by_confidence`: high≥0.75, medium 0.45–0.75, low<0.45.

**quality_flags:** `left_evidence_missing`, `right_evidence_missing`,
`low_match_score`, `fuzzy_match`, `possible_ocr_noise`, `numeric_change`,
`one_sided_entity`, `entity_type_mismatch`, `normalized_equal_raw_different`,
`table_row_changed`, `stamp_field_changed`, `requirement_text_changed`,
`needs_human_review`.

## Анти-шум

- одинаковое `220В` (или норма с другой OCR-разметкой) → `unchanged`, не дельта;
- одинаковые после нормализации сущности → не дельта;
- low-info `unknown`/`scheme_connection_hint` без evidence в одностороннем виде →
  `uncertain` (+ `needs_human_review`), а не high-confidence `added`/`removed`;
- сущности с непарного целиком блока → одностороннее с пониженным confidence.

## Формат отчёта

`diff_entity_extraction_report(entity_report, options=None)` → отчёт с `summary`
(deltas_total / added / removed / changed / uncertain / matched_entities_total /
matched_unchanged_total / unmatched_*_entities_total / by_entity_type /
by_semantic_group / by_delta_type / by_confidence / warnings_count), полным
`deltas[]`, `matched_entity_pairs[]` (с `block_match_id`, `method`, `score`,
`changed`, `delta_ids`), `unmatched_left/right_entities[]`, `block_summaries[]`
(per block_match: added/removed/changed/uncertain + quality_flags), `warnings[]`.

Чистые функции: `diff_entity_extraction_report`, `diff_matched_block_entities`,
`match_entities`, `build_entity_match_candidates`, `make_entity_match_key`,
`make_entity_identity_key`, `compare_matched_entities`, `compare_entity_fields`,
`normalize_entity_value`, `normalize_entity_subject`, `normalize_cable_value`,
`normalize_power_value`, `extract_numeric_tokens`, `make_delta_id`,
`write_entity_diff_report`.

## Что этот этап НЕ делает

- **НЕ** вызывает Qwen/Opus/LLM, **НЕ** скачивает `crop_url`, **НЕ** ходит в сеть;
- **НЕ** формулирует строительное/нормативное влияние отличия (это следующий
  этап — объяснение/critic);
- **НЕ** создаёт findings, **НЕ** подключён к UI, не запускается автоматически;
- **НЕ** трогает старую логику, runtime comparison data, `.env`, deploy, backend.

Импорты — только stdlib (`json/os/re/tempfile/unicodedata/collections/difflib/
pathlib/typing`).

## Тесты

[tests/test_stage_comparison_pipeline_v2_entity_diff.py](../tests/test_stage_comparison_pipeline_v2_entity_diff.py)
— synthetic entity-отчёты: stamp changed/unchanged, added/removed, норма
unchanged/changed-edition, equipment PoE-синоним/added, кабель
unchanged/section-changed, питание unchanged/changed, contents page changed,
change_log description changed, table_row changed, requirement fuzzy+numeric,
low-info one-sided → uncertain, block_summaries, summary-счётчики, атомарная
запись JSON, отсутствие сети/LLM-импортов и сквозная интеграция `result_json →
normalize → match → extract → diff` (стадия П→Р даёт changed stamp delta).

## Следующий этап — LLM Explanation / Critic по готовым deltas

LLM (точечно, по одной дельте/группе дельт) объясняет смысл и возможное влияние
готовых deltas (что изменилось, зачем, нормативный контекст) и/или critic
проверяет грунтованность дельты против исходного текст-слоя — **без поиска
отличий по всему тому**. Вход — `entity_diff_report.deltas` с `evidence` обеих
сторон и `confidence`; low-confidence/`needs_human_review` дельты приоритетны для
проверки. Это сохраняет воспроизводимость (LLM не генерирует список отличий, а
комментирует детерминированный).

## Связанные файлы

- [pipeline_v2_entity_diff.py](../backend/app/services/stage_comparison/pipeline_v2_entity_diff.py)
- [pipeline_v2_entity_extraction.py](../backend/app/services/stage_comparison/pipeline_v2_entity_extraction.py) — этап 3 (вход)
- [pipeline_v2_block_matching.py](../backend/app/services/stage_comparison/pipeline_v2_block_matching.py) — этап 2
- [pipeline_v2_prepared_ingest.py](../backend/app/services/stage_comparison/pipeline_v2_prepared_ingest.py) — этап 1

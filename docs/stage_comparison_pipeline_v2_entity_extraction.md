# Stage Comparison Pipeline V2 — Entity Extraction по matched blocks (этап 3)

**Дата:** 2026-06-09
**Статус:** новый изолированный режим, этап 3 — **только извлечение сущностей
(observe / read-only)**. Старую логику Stage Comparison НЕ заменяет и не трогает.
**Модуль:** [backend/app/services/stage_comparison/pipeline_v2_entity_extraction.py](../backend/app/services/stage_comparison/pipeline_v2_entity_extraction.py)
**Базируется на:** [этап 1 — Ingest](stage_comparison_pipeline_v2_prepared_package_ingest.md) + [этап 2 — Block Matching](stage_comparison_pipeline_v2_block_matching.md)

## Зачем нужен entity extraction

Этап 1 нормализует комплект в `normalized_document_model`, этап 2 строит
`block_matching_report` (какой блок OLD какому блоку NEW соответствует). Этап 3
извлекает из блоков **нормализованные сравнимые сущности**: поля штампа, текстовые
требования, нормативные ссылки, оборудование, кабели, электропитание, строки
таблиц, строки справки изменений / содержания, компоненты схем и подсказки связей.

```text
left model (OLD) ─┐
right model (NEW)─┤
block_matching   ─┘
        │
        ▼
  extract_entities_for_matched_documents
        │  (per-block extractors → dedup → finalize → group by block_match)
        ▼
  entity_extraction_report.json
```

## Почему это отдельный этап между matching и diff

Diff должен сравнивать **сопоставимые единицы**, а не сырой текст блоков. Если
гнать «голый» блок в LLM, детализация теряется (то же «один большой Opus сжимает
весь том»). Entity extraction выделяет дискретные, заякоренные на блок/лист
сущности с устойчивым `entity_id`, `evidence` и `confidence`. Тогда следующий
этап (deterministic entity diff) сравнивает наборы сущностей OLD↔NEW по парам
сопоставленных блоков — точечно и воспроизводимо, без сжатия детализации.

Этап намеренно **не сравнивает** — он только готовит сущности.

## Поддержанные `entity_type`

`stamp_field`, `document_section`, `change_log_item`, `contents_item`,
`requirement`, `norm_reference`, `equipment`, `cable`, `power_supply`,
`scheme_component`, `scheme_connection_hint`, `table_row`, `unknown`.

`semantic_group`: `stamp | text | table | scheme | equipment | cable | power |
change_log | contents | unknown`.

## Что извлекается

### Из stamp (`extract_stamp_entities`)
Отдельная `stamp_field`-сущность на каждое поле штампа: `document_code`,
`project_name`, `sheet_name`, `stage`, `sheet_number`, `total_sheets`,
`organization`; подписи (`role/surname/date`) и ревизии — отдельными сущностями.
Извлекается только из блоков `semantic_type=stamp` (штамп не дублируется с каждого
блока листа). Source = `stamp_data`.

### Из text (`extract_text_entities`)
- `document_section` — заголовки разделов (`Раздел`/`Общие данные`/`Пояснительная
  записка`/`Текстовая часть`…);
- `requirement` — клаузы со словами `должен/должны/предусматривается/необходимо/
  выполняется/устанавливается/прокладывается`;
- `norm_reference` — `СП`, `ГОСТ`/`ГОСТ Р`, `ФЗ`, `ПУЭ`, `РД`, `СНиП` (+ номер);
- `equipment` — `коммутатор/шкаф/видеорегистратор/ИБП/камера/контроллер/
  считыватель/вызывная панель/АРМ/кросс/патч-панель`;
- `cable` — `UTP/FTP/КПСВВ(нг)/LAN/cat.5e/cat.6/FRLS/LSLTx/ВВГ(нг)/ВОК/нг/LS/HF`;
- `power_supply` — `220В/12В/0.5А/ИБП/I категории`.

### Из table (`extract_table_entities`)
Каждая строка `| … | … |` → `table_row` с массивом ячеек (`fields.cells`) и
распознанными `code`/`sheet`, если есть. Source = `table`.

### Из change_log / contents
Для страниц `page_type=change_log`/`contents` — построчный разбор pipe-таблицы с
маппингом колонок (`Изм./Лист/Содержание изменений/Код/Примечание` →
`change_log_item`; `Обозначение/Наименование/Стр.` → `contents_item`), с
текстовым fallback. Эти экстракторы имеют приоритет над generic-text на своих
страницах.

### Из scheme/large_scheme/plan (`extract_scheme_entities`)
Использует уже имеющиеся поля блока (НЕ скачивая crop):
`ocr_json_summary.key_entities` / `content_summary` / `detailed_description`,
`text_excerpt`, `pdfplumber_text_excerpt`, `stamp_data.sheet_name`. Каждый
`key_entity` классифицируется в `cable`/`power_supply`/`equipment`/
`scheme_component`; по всем текстам схемы также сканируются
`equipment`/`cable`/`power_supply` и `scheme_connection_hint` (`подключается/
питание/Ethernet/…`). Полноценный граф связей здесь НЕ строится — это задача
отдельного этапа deterministic scheme/feeder diff.

## Источник текста и расширение этапа 1

Этап работает с полями нормализованного блока: `text_excerpt`,
`pdfplumber_text_excerpt`, `stamp_data`, `quality_flags`, `semantic_type` и
**`ocr_json_summary`**.

`ocr_json_summary` — минимальное **backward-compatible** расширение этапа 1
([pipeline_v2_prepared_ingest](stage_comparison_pipeline_v2_prepared_package_ingest.md)):
для НЕ-штампного `ocr_json` сохраняется size-bounded сводка
(`content_summary`, `detailed_description`, `key_entities` ≤60). Без неё схемные
блоки в полном конвейере не отдавали бы `key_entities` (этап 1 хранил только
`has_ocr_json`). Старые потребители поле игнорируют; новое поле опционально, и
entity extraction деградирует на excerpt'ы, если его нет.

## Дедупликация

Внутри блока сущности дедуплицируются по `(entity_type,
normalize_entity_text(subject|name|value))` — не создаётся 10 одинаковых `220В`
или `UTP cat.5e` из одного блока. У оставленной сущности при подавлении дублей
ставится `duplicate_entity_suppressed`.

## Quality flags

Сущность: `empty_evidence`, `low_information_entity`, `from_excerpt_only`,
`stamp_field_missing_value`, `possible_ocr_noise`, `duplicate_entity_suppressed`.
Блок (в группировке отчёта): `scheme_without_key_entities`, `scheme_without_crop`.

## Формат сущности

```json
{
  "entity_id": "ent_l_<block>_03",
  "entity_type": "stamp_field|requirement|norm_reference|equipment|cable|power_supply|scheme_component|scheme_connection_hint|table_row|change_log_item|contents_item|document_section|unknown",
  "semantic_group": "stamp|text|table|scheme|equipment|cable|power|change_log|contents|unknown",
  "side": "left|right|unknown",
  "document_code": "...", "page_number": 1,
  "page_type": "text|scheme|contents|change_log|unknown",
  "block_id": "...", "block_semantic_type": "text|scheme|stamp|table|unknown",
  "subject": "...", "name": "...", "value": "...", "unit": "...", "fields": {},
  "confidence": 0.0,
  "evidence": { "quote": "...", "source": "stamp_data|ocr_json|pdfplumber_text|text_excerpt|table|heuristic",
                "block_id": "...", "page_number": 1 },
  "quality_flags": []
}
```

## Формат отчёта

`extract_entities_for_matched_documents(left_model, right_model,
block_matching_report, options=None)` → отчёт с `left/right` сводкой, `summary`
(`by_entity_type`/`by_semantic_group`/`by_source`, `blocks_processed`,
`matched_blocks_processed`, `unmatched_blocks_processed`), полными списками
`left_entities`/`right_entities`, группировкой `matched_block_entities[]`
(привязка к `block_match_id` этапа 2 + `entity_type_counts` + block-level
`quality_flags`), `unmatched_left/right_block_entities[]` и `warnings`.

Чистые функции: `extract_document_entities`, `extract_entities_for_block`,
`extract_entities_for_matched_documents`, `extract_stamp_entities`,
`extract_text_entities`, `extract_table_entities`, `extract_scheme_entities`,
`extract_change_log_entities`, `extract_contents_entities`,
`normalize_entity_text`, `make_entity_id`, `write_entity_extraction_report`.

## Что этот этап НЕ делает

- **НЕ** сравнивает сущности (это следующий этап — diff);
- **НЕ** вызывает Qwen / Opus / OCR / PDF-render и **НЕ** скачивает `crop_url`;
- **НЕ** ходит в сеть; импорты — только stdlib (`json/os/re/tempfile/
  unicodedata/collections/pathlib/typing`);
- **НЕ** создаёт findings;
- **НЕ** подключён к UI и не запускается автоматически;
- **НЕ** трогает старую логику, runtime comparison data, `.env`, deploy, backend.

## Тесты

[tests/test_stage_comparison_pipeline_v2_entity_extraction.py](../tests/test_stage_comparison_pipeline_v2_entity_extraction.py)
— synthetic модели/отчёты: stamp_field, change_log_item, contents_item,
requirement, norm_reference, equipment, cable, power_supply, scheme-компоненты с
`key_entities`, флаг `scheme_without_key_entities`, дедуп, привязка
`matched_block_entities` к `block_match_id`, unmatched, summary-счётчики,
атомарная запись JSON, отсутствие сети/LLM-импортов и сквозная интеграция
`result_json → normalize → match → extract`.

## Следующий этап — Deterministic Entity Diff OLD↔NEW

По парам сопоставленных блоков (`matched_block_entities`) сравнить наборы
сущностей детерминированно: добавленные/удалённые/изменённые `stamp_field`,
`requirement`, `norm_reference`, `equipment`, `cable`, `power_supply`,
`table_row`, `change_log_item`, `scheme_component`. Сопоставление сущностей
внутри пары блоков — по `entity_type` + нормализованному `subject/name/value`
(+ числовой re-cite для номиналов/сечений). На выходе — список дельт с
`evidence` обеих сторон; Opus подключается точечно только для объяснения
неоднозначных дельт, а не для сравнения всего тома.

## Связанные файлы

- [pipeline_v2_entity_extraction.py](../backend/app/services/stage_comparison/pipeline_v2_entity_extraction.py)
- [pipeline_v2_block_matching.py](../backend/app/services/stage_comparison/pipeline_v2_block_matching.py) — этап 2 (вход)
- [pipeline_v2_prepared_ingest.py](../backend/app/services/stage_comparison/pipeline_v2_prepared_ingest.py) — этап 1 (`ocr_json_summary` расширение)
- [graphic_profiles.py](../backend/app/services/stage_comparison/graphic_profiles.py) — родственные доменные профили схем (старый путь)

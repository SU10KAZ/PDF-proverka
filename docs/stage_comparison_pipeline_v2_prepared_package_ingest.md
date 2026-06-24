# Stage Comparison Pipeline V2 — Prepared Package Ingest (этап 1)

**Дата:** 2026-06-09
**Статус:** новый изолированный режим, этап 1 — **только ingest/нормализация**.
Старую логику Stage Comparison НЕ заменяет и не трогает.
**Модуль:** [backend/app/services/stage_comparison/pipeline_v2_prepared_ingest.py](../backend/app/services/stage_comparison/pipeline_v2_prepared_ingest.py)

## Зачем нужен Pipeline V2

Действующий конвейер сравнения стадий вырос вокруг Qwen-обогащения MD и
последующего Opus-сравнения двух больших enriched-MD. У него есть системная
слабость: **гранулярность**. Opus получает на вход «весь том» (или огромный
chunk), теряет per-sheet структуру (lost-in-the-middle), а источник истины по
геометрии/блокам размазан между MD, enriched-MD и result.json. На практике это
даёт нестабильную картину: один прогон находит «34 замечания на весь том»,
другой — «24 на один лист», и сопоставить их между собой тяжело.

Pipeline V2 строит сравнение **снизу вверх, от блока**:

```text
prepared package (pdf + result.json + document.md + ocr.html)
  → [этап 1] normalized_document_model        ← ЭТОТ модуль
  → [этап 2] block matching (OLD ↔ NEW)
  → [этап 3] entity extraction (по типам блоков/дисциплинам)
  → [этап 4] deterministic diff
  → [этап 5] Opus explanation (точечно, по конкретному блоку/сущности)
  → [этап 6] critic / grounding
  → [этап 7] grouping
  → [этап 8] UI
```

Этап 1 — фундамент: превратить «сырой» подготовленный комплект в стабильную,
детерминированную модель документа, на которой можно строить block-level diff.

## Почему `result.json` становится source of truth

Документы приходят **уже подготовленными** внешним OCR/нарезчиком: страницы
обведены, блоки разрезаны по координатам. Вся структурная правда лежит в
`*_result.json`:

- геометрия страниц (`page_number`, `width`, `height`);
- блоки с координатами (`coords_px`, `coords_norm`, `polygon_points`,
  `shape_type`);
- тип блока (`block_type`: `text` / `image`);
- ссылки на PDF-фрагменты блока (`crop_url` → облачный PDF-кроп, `image_file`);
- текст-слой и OCR (`ocr_text`, `ocr_html`, `pdfplumber_text`, `ocr_json`);
- штампы (`stamp_data`: `document_code`, `project_name`, `stage`,
  `organization`, `sheet_number`, `total_sheets`, `sheet_name`, …).

`document.md` и `ocr.html` исторически использовались как основной вход, но это
**производный человекочитаемый слой**, в котором геометрия и привязка к блокам
уже потеряны. Поэтому Pipeline V2 опирается на `result.json`, а MD/HTML держит
как debug/fallback.

## Роль каждого файла комплекта

| Файл | Роль в Pipeline V2 |
|---|---|
| `document.pdf` | source of truth по пикселям (рендер/кроп — на будущих этапах, НЕ на этапе 1) |
| `*_result.json` | **source of truth**: геометрия, блоки, штампы, ссылки на PDF-кропы, OCR |
| `*_document.md` | человекочитаемый Markdown (Chandra OCR); на этапе 1 — опциональный fallback для имени листа + cross-check числа страниц |
| `*_ocr.html` | HTML-слой OCR; debug; на этапе 1 фиксируется только факт наличия/читаемости |

## Что делает `normalized_document_model.json`

`build_normalized_document_model(result_json_path, document_md_path=…,
ocr_html_path=…, pdf_path=…)` возвращает стабильный словарь:

```json
{
  "version": 1,
  "kind": "stage_comparison_pipeline_v2_normalized_document",
  "source": { "pdf_path": "...", "result_json_path": "...",
              "document_md_path": "...", "ocr_html_path": "..." },
  "document": { "document_code": "...", "project_name": "...", "stage": "...",
                "organization": "...", "title": "...", "pages_total": 0 },
  "summary": { "pages_total": 0, "blocks_total": 0,
               "by_block_type": {}, "by_semantic_type": {}, "by_page_type": {},
               "image_blocks_total": 0, "image_blocks_with_crop_url": 0,
               "image_blocks_with_image_file": 0, "text_blocks_total": 0,
               "stamp_blocks_total": 0, "table_blocks_total": 0,
               "scheme_blocks_total": 0, "warnings_count": 0 },
  "pages": [ { "page_number": 1, "page_index": 0, "width": 0, "height": 0,
               "sheet_number": "...", "sheet_name": "...", "document_code": "...",
               "page_type": "title|change_log|contents|text|scheme|table|mixed|unknown",
               "blocks": ["block_id_1"] } ],
  "blocks": { "block_id_1": { "block_id": "...", "page_number": 1,
               "block_type": "text|image|table|unknown",
               "semantic_type": "title|text|table|stamp|scheme|large_scheme|plan|legend|unknown",
               "coords_px": [], "coords_norm": [], "shape_type": "...",
               "crop_url": null, "image_file": null,
               "has_crop_pdf": false, "has_pdfplumber_text": false,
               "has_ocr_json": false, "has_stamp_data": false,
               "text_excerpt": "...", "pdfplumber_text_excerpt": "...",
               "stamp_data": {}, "quality_flags": [] } },
  "warnings": []
}
```

Чистые функции модуля (все детерминированные, без сети):

| Функция | Назначение |
|---|---|
| `normalize_result_json(path)` | читает result.json (форматы A `pages[].blocks[]` и flat B `blocks[]`) → нормализованные страницы + блоки |
| `classify_page_type(page, page_blocks)` | тип страницы: `title/change_log/contents/text/scheme/table/mixed/unknown` |
| `classify_block_semantic_type(block)` | семантика блока: `stamp/text/table/scheme/large_scheme/plan/legend/title/unknown` |
| `extract_document_stamp_summary(blocks)` | документ-уровневая сводка по штампам (`document_code/project_name/stage/organization`) |
| `build_block_registry(blocks)` | реестр `{block_id → per-block model}` с semantic_type + quality_flags (дубликаты id не теряются) |
| `build_normalized_document_model(...)` | оркестрация всего → полная модель |
| `write_normalized_document_model(out, model)` | атомарная запись артефакта на диск (`os.replace`) |

### Классификация

- **Страница** учитывает имя листа из штампа, `document_code`, состав блоков
  (image/table/text), наличие image-блоков с `crop_url` и ключевые слова
  («Содержание тома», «Справка о внесённых изменениях», «Структурная схема»,
  «Графическая часть», «Текстовая часть»). Сопоставление устойчиво к ё/е.
- **Блок** учитывает `block_type`, `category_code`, `ocr_json`
  (`content_summary`/`clean_ocr_text`), `stamp_data.sheet_name`, наличие
  `crop_url` и относительный размер блока (`large_scheme` — крупная схема или
  «структурная схема»). Важный инвариант: `scheme/plan` — **графические** типы,
  поэтому текстовый блок, упоминающий «схему», остаётся `text`, а не становится
  схемой.

### Диагностика качества входа

Документ-уровневые `warnings[]` и per-block `quality_flags[]`:

- `missing_block_id`, `missing_coords`, `unknown_block_type`,
  `duplicate_block_id`;
- `image_block_without_crop_or_image_file` — графический блок без источника
  изображения;
- `empty_stamp_data` / `partial_stamp_data`;
- `strange_page_number` / `strange_page_index`;
- `empty_ocr`;
- `pdfplumber_without_ocr_json`;
- `has_cloud_crop_url` — облачный PDF-кроп есть, но **не скачивается**;
- doc-level: `page_without_blocks`, `no_pages_in_result_json`,
  `document_stamp_summary_empty`, `document_md_path_unreadable`,
  `ocr_html_path_unreadable`, `md_page_count_mismatch`,
  `unknown_block_type_count`.

## Что НЕ делает первый этап

- **НЕ** ходит в сеть (в т.ч. **НЕ** скачивает `crop_url`);
- **НЕ** запускает Qwen / Opus / OCR / PDF-render;
- **НЕ** делает block matching, entity extraction, diff или объяснения;
- **НЕ** подключён к UI и не запускается автоматически (только сервисные
  функции; артефакт можно записать вручную через
  `write_normalized_document_model`);
- **НЕ** трогает старую логику Stage Comparison, runtime comparison data,
  существующие пути/артефакты, `.env`, deploy и backend-процесс.

Импорты модуля — только stdlib (`json/os/re/tempfile/pathlib/typing`). Сетевые/
LLM-клиенты не импортируются.

## Как это уйдёт от «34 замечания на весь том vs 24 на один лист»

Нестабильность возникает потому, что Opus сравнивает огромные enriched-MD
целиком и каждый раз по-разному режет/агрегирует материал. Pipeline V2 делает
единицей сравнения **блок/сущность с устойчивым `block_id` и страницей**:

1. нормализованная модель даёт детерминированный, повторяемый список блоков с
   координатами, типами и привязкой к листу/штампу;
2. block matching сопоставляет блоки OLD↔NEW по геометрии/штампу — diff
   считается **по конкретным блокам**, а не по «всему тому»;
3. объяснение от Opus вызывается точечно на паре сопоставленных блоков, поэтому
   замечание всегда привязано к листу и блоку (стабильно от прогона к прогону);
4. группировка собирает per-block замечания в итоговый отчёт уже после того, как
   каждое заякорено — агрегация перестаёт «плавать».

То есть число и формулировки замечаний перестают зависеть от того, как именно в
конкретном прогоне нарезался гигантский MD.

## Следующие этапы (roadmap)

1. **Block matching** — сопоставление блоков OLD↔NEW по `coords_norm`/IoU +
   page-alignment по штампу (переиспользовать наработки
   [stamp_matching](stage_comparison_stamp_sheet_matching.md) и
   [block_equivalence_precheck](stage_comparison_block_equivalence_precheck.md)).
2. **Entity extraction** — извлечение сущностей по типам блоков/дисциплинам
   (профили из [graphic_profiles](stage_comparison_block_pdf_source.md)).
3. **Deterministic diff** — детерминированная дельта по сущностям/тексту до LLM.
4. **Opus explanation** — точечное объяснение конкретной дельты (а не всего тома).
5. **Critic / grounding** — сверка каждой дельты с исходным текст-слоем/MD
   (переиспользовать `verify_change_evidence` /
   [self-check](stage_comparison_main_path_selfcheck.md)).
6. **Grouping** — сборка per-block замечаний в финальный отчёт.
7. **UI** — отдельный режим Pipeline V2 поверх готовой модели.

## Тесты

[tests/test_stage_comparison_pipeline_v2_prepared_ingest.py](../tests/test_stage_comparison_pipeline_v2_prepared_ingest.py)
— маленький synthetic fixture (без реальных PDF и без сети): многостраничная
нормализация, классификация страниц (`change_log`/`contents`/`scheme`) и блоков
(`stamp`/`text`/`table`/`legend`/`plan`), `has_crop_pdf`, summary-счётчики,
quality_flags для image-блока без crop/image_file, flat-формат B, MD-fallback
имени листа, атомарная запись JSON и проверка отсутствия сетевых вызовов.

## Связанные файлы

- [pipeline_v2_prepared_ingest.py](../backend/app/services/stage_comparison/pipeline_v2_prepared_ingest.py)
- [blocks.py](../backend/app/services/stage_comparison/blocks.py) — родственная `normalize_blocks_from_result_json`
- [block_pdf_source.py](../backend/app/services/stage_comparison/block_pdf_source.py) — block-PDF/текст-слой helper (будущие этапы)
- [evidence_first_fallback.py](../backend/app/services/stage_comparison/evidence_first_fallback.py) — `build_fact_index` (родственный парсинг MD-страниц)

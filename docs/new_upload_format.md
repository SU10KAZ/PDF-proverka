# Новый формат загрузки (портал vibe, с 2026-07-13)

> Актуализировано 2026-07-16 по выгрузке из 5 ZIP (АР×2, ПС, ЭО, АПТ):
> портал вернул геометрию отдельным файлом `_blocks.json`, комплект приходит
> ZIP-архивом, «Конвертер MD» ОТМЕНЁН (решение Андрея Ивановича 14.07 —
> новый формат = стандарт, строим нативное чтение).
> Полный отчёт обследования: `experiments/новая структура./ОБСЛЕДОВАНИЕ_выгрузка_2026-07-16.md`.

## Комплекты

| | Старый метод (до 2026-07) | Новый метод (с 2026-07-13) |
|---|---|---|
| PDF | `<имя>.pdf` | `<имя>.pdf` |
| Текст (MD) | `<имя>_document.md` (Chandra) | `<имя>_results.md` (другой формат!) |
| HTML | `<имя>_ocr.html` | `<имя>_results.html` |
| Геометрия | `<имя>_result.json` (координаты блоков) | `<имя>_blocks.json` (с 2026-07-16, **опционален**) |
| Упаковка | россыпь файлов | **ZIP** `<имя>_V1.zip`, плоско без подпапки (с 2026-07-15) |

Примеры нового формата: `experiments/новая структура./*.zip` (папка с точкой
в конце имени).

## `_blocks.json` (schema_version 1)

Геометрия блоков, вернувшаяся после upstream-запроса порталу:

- `document_id/document_name/document_path` (path содержит дисциплину),
  `generated_at`, `coordinate_space: "normalized_page_top_left"`.
- `pages[]` — ВСЕ страницы PDF: `page_index` (0-based), `width_px/height_px`
  (визуальные, ~300 DPI с потолком ~100 МП — масштаб считать из width_px,
  НЕ хардкодить 300), `rotation` (== `/Rotate` PDF).
- `blocks[]` — включая штампы: `block_id` (`blk_<32hex>`, == заголовкам MD),
  `ordinal` (сквозной, == «BLOCK #N» в MD; у штампов **null**), `page_index`,
  `page_label` (== page_index+1), `block_type` (text|image|stamp),
  `shape_type` (rectangle|polygon), `status/export_status`,
  `coords_norm` [x0,y0,x1,y1] — top-left ВИЗУАЛЬНОЙ (после /Rotate) ориентации,
  `polygon_points` (пары [x,y]; coords_norm = их bbox), `crop_url`
  (у штампов **null**).
- Пересчёт visual→unrotated (обе системы top-left, нормализованные):
  rot=90 → u=y, v=1−x; rot=270 → u=1−y, v=x.
- Кропы `/api/crops/<token>` — одностраничный **PDF** (не PNG), без auth,
  `immutable`; битый токен → HTTP 403. Токены живут per-generation —
  качать при ingest. Текст-слой в кропе не гарантирован (АПТ — вектор,
  АР/ПС/ЭО — растр 300 DPI); проверять pdffonts/pdftotext по факту.

## План деприкации старого метода

- **До ~2026-08-14** принимаются ОБА метода: раздел ВК ещё распознаётся
  по-старому (4 файла), все новые документы готовятся по-новому.
- **После ~2026-08-14** можно удалить только **приём** старого комплекта
  (upload-классификатор, INPUT_SUFFIXES в мигаторе, фронтовые regex).
- **Никогда не удалять** паттерны **чтения** старых суффиксов в резолверах
  (`projects_v2_source_resolver`, `md_resolver`, `_sync_v2_work_copies`):
  341+ загруженная версия хранит старые имена в `01_input` навсегда.
- Грепать точки по строке `2026-08-14`.

## Ключевые факты нового формата

- `results.md`: шапка `# Document:` / `Path:` / `Generated:` / `**Stamp:**`;
  секции `## Page N` (N = физическая страница PDF 1:1); блоки
  `### BLOCK #n [TEXT|IMAGE]: blk_<32hex>` с цитатами `> **Created:**`,
  `> **Crop:** [Crop](https://vibe.cloud-ip.cc/api/crops/<token>)`,
  `> **Stamp:** Code|Stage|Sheet|Object|Name|Organization|Revisions`.
  Штампы (block_type=stamp) в MD/HTML НЕ выгружаются — только в blocks.json.
- Stamp парсить по известным ключам (`(Code|Stage|Sheet|…): `), НЕ по
  двоеточию (Object/Axes содержат `:`); пустые значения — `Sheet:  |`
  (двойной пробел) и хвостовой `Revisions: `.
- `Sheet` из штампа НЕуникален в документе (до 5 страниц на один Sheet;
  спецификации перезапускают нумерацию) — лист показывать как подпись,
  **ключ = страница PDF**.
- Гомоглифы обязательны к нормализации: кириллические «ОО» vs «00» в кодах,
  латиница внутри кириллических марок («КПCнг», «ГOST»), CJK-вкрапления;
  юникод-индексы осей (Д12₂) — семантика, НЕ мусор.
- В выгрузках от 15.07 «страницы без блоков пропускаются» не воспроизводится
  (все страницы в MD), но `pages[]` blocks.json — авторитетный полный список;
  дополнение из PDF оставить как страховку для комплектов без blocks.json.
- IMAGE-блоки уже энричены (`Summary/Description/Entities/Verification`) —
  решение: доверяем как Chandra-описаниям (MD-канал), визуальный анализ свой.
- Для таблиц/text_evidence `results.html` первичен ТОЛЬКО у генераций
  ≤2026-07-14: с генерации 07-15 HTML теряет rowspan/colspan/br/sup —
  выбирать источник по факту наличия этих тегов, не по дате.

## Этапы интеграции

1. **Приём файлов** (СДЕЛАН, 2026-07-14): суффиксы `_results.md`/`_results.html`
   принимаются во всех точках ingest аддитивно к старым. Роль `_results.md` =
   document_md (нормализуется в `02_work/document.md`), `_results.html` =
   ocr_html (`02_work/ocr.html`).
2. **Приём `_blocks.json` + ZIP** (СДЕЛАН, 2026-07-16): роль blocks_json
   (нормализуется в `02_work/blocks.json`), файл опционален; fingerprint
   учитывает blocks.json (комплект с геометрией ≠ дубль комплекта без неё);
   ZIP-архивы распаковываются на бэкенде прозрачно в `_classify_upload_files`
   (`_expand_zip_uploads`: flatten, гарды на зип-бомбы, вложенные ZIP не
   раскрываются), фронт отправляет ZIP как есть и строит кандидатов из архивов.
3. **Нативный парсер `results_md.py`** (вместо ОТМЕНЁННОГО конвертера):
   единый модуль pages/blocks/stamps/crop_url; потребители старого формата
   получают ветку «новый формат → парсер, старый → прежние regex»
   (`block_markdown`, `parse_md_text`, `task_builder`, `md_prescan`,
   `optimization/prescan`, `md_mirror_reconcile`, `evidence_first_fallback`,
   `stamp_matching`, `codex_targeted_findings`).
4. **result.json из blocks.json + кэш кропов**: прямой детерминированный
   маппинг blocks.json → канонический result.json (координаты реальные,
   формулы rotation выше); скачивание всех кропов при загрузке (кэш до
   сотен МБ на документ, лимит размера обязателен — встречаются кропы 15 МБ).
   Если комплект без blocks.json — работаем деградированно (без геометрии).
5. **Парсер results.html** для text_evidence (`_build_ocr_html_index`: ветка
   `data-block-id`, ID-паттерн `blk_<hex>`).

## Точки правок этапов 1–2 (приём)

- `backend/app/services/common/project_service.py` — `_classify_upload_files`
  (+ `_expand_zip_uploads`, роль blocks), `_compute_upload_fingerprint` (роль
  blocks), `_is_new_format_bundle`, `_upload_bundle_warnings(new_format=,
  has_blocks_json=)`, приоритет `md_primary`, `blocks_json_file` в
  project_info, `_save_uploaded_as_new_version` (blocks в extra).
- `scripts/projects_v2/v2lib.py` — `INPUT_QUAD`/`INPUT_SUFFIXES`/
  `WORK_NORMALIZED` + `find_input_quad` (роль blocks_json; runtime-зависимость
  бэкенда через storage_write_facade — нужен рестарт).
- `backend/app/services/storage/projects_v2_source_resolver.py` —
  `_BLOCKS_JSON_SUFFIX`, `_resolve_blocks_json`, поля `blocks_json_path[s]`
  в `V2SourceFiles`/`VersionSourceFiles` (с default'ами), обе ветки layout.
- `backend/app/services/common/md_resolver.py` — `_DOC_SUFFIXES` (этап 1).
- `backend/app/services/common/version_service.py` — `_sync_v2_work_copies`
  (+ `02_work/blocks.json`).
- `frontend/static/js/app.js` — `_uploadBundleFiles` (blocks+zips),
  `_buildUploadCandidate` (слоты blocks/zips, имя из ZIP), flat-sidecar regex
  (+`_blocks.json`), кандидаты из плоских ZIP, `recheckCandidate`/
  `canSubmitUpload`/`runSinglePrecheck` (ZIP без явного PDF допустим).
- `frontend/index.html` — колонка `md/res/ocr/blk`, отображение ZIP-кандидата.
- Тесты: `backend/tests/test_new_results_bundle_ingest.py`.

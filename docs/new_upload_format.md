# Новый 3-файловый формат загрузки (портал vibe, с 2026-07-13)

## Комплекты

| | Старый метод (до 2026-07) | Новый метод (с 2026-07-13) |
|---|---|---|
| PDF | `<имя>.pdf` | `<имя>.pdf` |
| Текст (MD) | `<имя>_document.md` (Chandra) | `<имя>_results.md` (другой формат!) |
| HTML | `<имя>_ocr.html` | `<имя>_results.html` |
| Геометрия | `<имя>_result.json` (координаты блоков) | **нет** |

Примеры нового формата: `experiments/новая структура./<подпапки>` (папка с точкой
в конце имени). Изучены 3 образца (ВК, ЭМ, АР) — формат стабилен между
генерациями, CSS/DOM идентичны.

## План деприкации старого метода

- **До ~2026-08-14** принимаются ОБА метода: раздел ВК ещё распознаётся
  по-старому (4 файла), все новые документы готовятся по-новому (3 файла).
- **После ~2026-08-14** можно удалить только **приём** старого комплекта
  (upload-классификатор, INPUT_SUFFIXES в мигаторе, фронтовые regex).
- **Никогда не удалять** паттерны **чтения** старых суффиксов в резолверах
  (`projects_v2_source_resolver`, `md_resolver`, `_sync_v2_work_copies`):
  341+ загруженная версия хранит старые имена в `01_input` навсегда.
- Грепать точки по строке `2026-08-14`.

## Ключевые факты нового формата (по 3 образцам)

- `results.md`: шапка `# Document:` / `Path:` / `Generated:` / `**Stamp:**`;
  секции `## Page N` (N = физическая страница PDF 1:1); блоки
  `### BLOCK #n [TEXT|IMAGE]: blk_<32hex>` с цитатами `> **Created:**`,
  `> **Crop:** [Crop](https://vibe.cloud-ip.cc/api/crops/<token>)`,
  `> **Stamp:** Code|Stage|Sheet|Object|Name|Organization|Revisions`.
- **Страницы без блоков пропускаются** (и в MD, и в HTML TOC) — полный список
  страниц восстанавливать из PDF; в HTML `data-page-index` = страница PDF − 1.
- **Координат блоков нет нигде**; кропы по ссылкам — растр 300 DPI без
  текст-слоя (планируется переделка на векторные на стороне портала).
- `Sheet` из штампа НЕуникален в документе (прилагаемые спецификации
  перезапускают нумерацию) — лист показывать как подпись, ключ = страница PDF.
- IMAGE-блоки уже энричены (`Summary/Description/Entities/Verification`) —
  решение: доверяем как Chandra-описаниям (MD-канал), визуальный анализ свой.
- Для таблиц/text_evidence первичен `results.html` (в MD теряются
  rowspan/colspan, ложные header'ы, «;»-склейка ячеек).

## Этапы интеграции

1. **Приём файлов** (этот этап, 2026-07-14): суффиксы `_results.md`/`_results.html`
   принимаются во всех точках ingest аддитивно к старым. Роль `_results.md` =
   document_md (нормализуется в `02_work/document.md`), `_results.html` =
   ocr_html (`02_work/ocr.html`). Аудит по новым проектам ещё НЕ работает
   (нет result.json → prepare/crop падают штатно).
2. **Конвертер MD** (следующий): `results.md` → канонический Chandra-вид при
   создании `02_work/document.md` (`## Page N`→`## СТРАНИЦА N`, срез `#n` из
   заголовков блоков, `**Лист:**`/`**Наименование листа:**` из per-block штампов,
   англ. ключи → русские, `Verification` → «Что проверить»).
3. **Псевдо-result.json + кэш кропов**: блоки/страницы/листы/crop_url из
   results.md (+ полный список страниц из PDF); скачивание всех кропов при
   загрузке. Если портал вернёт координаты/result.json — суррогат заменяется
   настоящим файлом без перестройки.
4. **Парсер results.html** для text_evidence (`_build_ocr_html_index`: ветка
   `data-block-id`, ID-паттерн `blk_<hex>`).

## Точки правок этапа 1

- `backend/app/services/common/project_service.py` — `_classify_upload_files`,
  `_is_new_format_bundle`, `_upload_bundle_warnings(new_format=)`, приоритет
  `md_primary`, копирование `*_results.htm*`, `is_source_file`.
- `scripts/projects_v2/v2lib.py` — `INPUT_SUFFIXES` + `find_input_quad`
  (runtime-зависимость бэкенда через storage_write_facade — нужен рестарт).
- `backend/app/services/storage/projects_v2_source_resolver.py` — `_DOC_SUFFIXES`,
  `_OCR_HTML_SUFFIXES`, `_doc_md_files`, обе ветки layout.
- `backend/app/services/common/md_resolver.py` — `_DOC_SUFFIXES`.
- `backend/app/services/common/version_service.py` — `_sync_v2_work_copies`.
- `frontend/static/js/app.js` — `_buildUploadCandidate`, flat-sidecar regex,
  `normalizeProjectName` (одиночный режим загрузки — мёртвый код, не правился).
- Тесты: `backend/tests/test_new_results_bundle_ingest.py`.

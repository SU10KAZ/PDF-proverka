# Stage Comparison Pipeline V2 — Block Matching OLD↔NEW (этап 2)

**Дата:** 2026-06-09
**Статус:** новый изолированный режим, этап 2 — **только сопоставление (observe /
read-only)**. Старую логику Stage Comparison НЕ заменяет и не трогает.
**Модуль:** [backend/app/services/stage_comparison/pipeline_v2_block_matching.py](../backend/app/services/stage_comparison/pipeline_v2_block_matching.py)
**Базируется на:** [этап 1 — Prepared Package Ingest](stage_comparison_pipeline_v2_prepared_package_ingest.md)

## Зачем нужен block matching

Pipeline V2 строит сравнение стадий снизу вверх — от блока. Этап 1 превращает
подготовленный комплект (`pdf + result.json + document.md + ocr.html`) в
детерминированную `normalized_document_model`. Этап 2 берёт ДВЕ такие модели —
**OLD (left, старая стадия)** и **NEW (right, новая стадия)** — и строит
детерминированное сопоставление:

```text
left model (OLD)  ─┐
                   ├─► match_pages  ─► page_matches (1:1)
right model (NEW) ─┘        │
                            ▼
                      match_blocks (внутри пары страниц) ─► block_matches (1:1)
                            │
                            ▼
                      block_matching_report.json
```

Сопоставление — фундамент для всего, что идёт дальше (entity extraction →
deterministic diff → точечное Opus-объяснение → critic → grouping → UI).

## Почему нельзя сравнивать весь том одним Opus

Действующий конвейер отдаёт Opus два больших enriched-MD целиком (или огромными
чанками). Это даёт нестабильную картину: один прогон находит «34 замечания на
весь том», другой — «24 на один лист», и сопоставить их между собой невозможно.
Причины:

- **lost-in-the-middle** на больших входах — модель теряет per-sheet структуру;
- каждый прогон **по-разному режет/агрегирует** материал — число и формулировки
  замечаний «плавают»;
- нет устойчивого якоря: замечание не привязано к конкретному листу/блоку.

Block matching делает единицей сравнения **лист и блок с устойчивым `block_id` и
страницей**. Diff и объяснение считаются по конкретной паре сопоставленных
блоков, а не по «всему тому» — результат становится воспроизводимым и заякоренным.

## Вход — две `normalized_document_model`

Этап 2 работает ТОЛЬКО с готовыми моделями этапа 1 (никаких PDF/cloud/OCR/Qwen/
Opus). Из модели используются:

- `pages[]`: `page_number`, `page_type`, `sheet_name`, `sheet_number`,
  `document_code`, `blocks[]` (id блоков);
- `blocks{}`: `block_id`, `semantic_type`, `block_type`, `coords_norm`,
  `crop_url`/`has_crop_pdf`, `text_excerpt`/`pdfplumber_text_excerpt`,
  `stamp_data`.

## Как матчатся страницы

Нельзя полагаться на физический номер страницы: старый лист **52** может
соответствовать новому листу **21** (после переверстки тома). Поэтому приоритет —
у имени листа/штампа. `_score_page_pair` для каждой пары (L, R) выбирает лучший
метод по убыванию надёжности:

| Приоритет | Метод | Условие | Базовый score |
|---|---|---|---|
| 1 | `exact_sheet` | нормализованный `sheet_name` совпал | 0.95 (+0.03 same type) |
| 2 | `stamp_sheet` | `sheet_number` совпал + подтверждение типом/именем | 0.80–0.97 |
| 3 | `content_fuzzy` | fuzzy-сходство имени листа ≥ порога | 0.50 + 0.40·sim |
| 4 | `document_code` | `document_code` + тип совпали, имён листов нет | 0.55 |
| 5 | `page_number` | совпал только физический номер (слабый fallback) | 0.30–0.40 |

Сопоставление — жадное **1:1** по убыванию score (каждая страница matched не
более одного раза). Имя листа нормализуется (`normalize_match_text`: NFKC, lower,
ё→е, срез «лист N»/«стр N»/«(из N)», пунктуация→пробел), сходство — через
`difflib.SequenceMatcher`. Для схемных листов имя особенно важно (например
`Структурная схема СОВ и СКУД. Корпус 4`).

## Как матчатся блоки

Блоки сопоставляются **только внутри найденной пары страниц** (жадно 1:1):

| Приоритет | Метод | Для каких блоков |
|---|---|---|
| 1 | `same_block_id` | совпал `block_id` → score 1.0 (но на это нельзя рассчитывать между стадиями) |
| 2 | `stamp` | stamp↔stamp (IoU + сходство полей `stamp_data`) |
| 3 | `text_fuzzy` | text-группа (`text/legend/title`) — fuzzy по `text_excerpt` + IoU |
| 4 | `table_fuzzy` | table — fuzzy по табличному excerpt + IoU |
| 5 | `semantic_type_iou` | scheme-группа (`scheme/large_scheme/plan`) — bbox IoU по `coords_norm` |
| 6 | `scheme_crop` | то же, когда обе стороны имеют `has_crop_pdf` (учитываем `crop_url`, но **НЕ скачиваем**) |

**Несовместимые семантические группы не матчатся** (кандидат не создаётся):
`stamp↔scheme`, `text↔stamp`, `table↔scheme` и т.п. Группа `unknown` —
wildcard: матчится с любой, но score капается до «weak» (без явной семантики
сильной пары быть не может). Это и обеспечивает инвариант «несовместимые типы не
strong».

`compute_bbox_iou_norm(left_coords_norm, right_coords_norm)` — IoU двух
нормализованных bbox (0..1), 0.0 при отсутствующих/битых координатах.

## confidence и risk_flags

Confidence из score (детерминированные пороги, тюнятся через `options`):

| | strong | medium | weak |
|---|---|---|---|
| страница | ≥ 0.85 | ≥ 0.60 | < 0.60 |
| блок | ≥ 0.80 | ≥ 0.50 | < 0.50 |

**Page risk_flags:** `page_number_only_match`, `sheet_name_missing`,
`page_type_mismatch`, `low_score`, `duplicate_candidate`, `one_sided_page`
(для непарных страниц без кандидата на другой стороне).

**Block risk_flags:** `low_iou`, `semantic_type_mismatch`, `missing_coords`,
`missing_crop` (scheme-блок без `has_crop_pdf` — но crop не скачивается),
`weak_text_match`, `duplicate_candidate`, `one_sided_block`.

`duplicate_candidate` ставится, когда у выбранной стороны был ещё кандидат с
близким score (в пределах `duplicate_margin`) — сигнал неоднозначности.

## Формат отчёта (`block_matching_report`)

`match_normalized_documents(left_model, right_model, options=None)` возвращает:

```json
{
  "version": 1,
  "kind": "stage_comparison_pipeline_v2_block_matching",
  "left":  { "document_code": "...", "pages_total": 0, "blocks_total": 0 },
  "right": { "document_code": "...", "pages_total": 0, "blocks_total": 0 },
  "summary": { "page_matches_total": 0, "block_matches_total": 0,
               "unmatched_left_pages": 0, "unmatched_right_pages": 0,
               "unmatched_left_blocks": 0, "unmatched_right_blocks": 0,
               "strong_page_matches": 0, "weak_page_matches": 0,
               "strong_block_matches": 0, "weak_block_matches": 0,
               "warnings_count": 0 },
  "page_matches":  [ { "match_id": "pm_52_21", "left_page_number": 52,
                       "right_page_number": 21, "method": "exact_sheet",
                       "score": 0.98, "confidence": "strong",
                       "reasons": [...], "risk_flags": [...], ... } ],
  "block_matches": [ { "match_id": "bm_...", "page_match_id": "pm_52_21",
                       "left_block_id": "...", "right_block_id": "...",
                       "method": "scheme_crop", "score": 0.97, "iou": 0.97,
                       "confidence": "strong", "reasons": [...],
                       "risk_flags": [...], ... } ],
  "unmatched_left_pages": [], "unmatched_right_pages": [],
  "unmatched_left_blocks": [], "unmatched_right_blocks": [],
  "warnings": []
}
```

Чистые функции модуля:

| Функция | Назначение |
|---|---|
| `match_normalized_documents(left, right, options)` | оркестрация: страницы + блоки → полный отчёт |
| `match_pages(left, right, options)` | жадное 1:1 сопоставление страниц |
| `match_blocks(left, right, page_matches, options)` | сопоставление блоков внутри пар страниц |
| `build_page_match_candidates(...)` / `build_block_match_candidates(...)` | генерация всех проходных пар-кандидатов |
| `compute_bbox_iou_norm(a, b)` | IoU нормализованных bbox |
| `normalize_match_text(text)` | канонизация строки для сопоставления |
| `make_page_identity_key(...)` / `make_block_identity_key(...)` | стабильные identity-ключи |
| `write_block_matching_report(out, report)` | атомарная запись отчёта (`os.replace`) |

`options` — необязательный dict с порогами (`page_strong_score`,
`block_iou_low`, `duplicate_margin`, …); при `None` берутся дефолты.

## Что этот этап НЕ делает

- **НЕ** ходит в сеть и **НЕ** скачивает `crop_url`;
- **НЕ** вызывает Qwen / Opus / OCR / PDF-render;
- **НЕ** создаёт findings / замечаний и **НЕ** считает diff содержимого блоков
  (только сопоставляет, какой блок какому соответствует);
- **НЕ** подключён к UI и не запускается автоматически (только сервисные
  функции; отчёт пишется вручную через `write_block_matching_report`);
- **НЕ** трогает старую логику, runtime comparison data, `.env`, deploy,
  backend-процесс.

Импорты модуля — только stdlib (`json/os/re/tempfile/unicodedata/difflib/
pathlib/typing`). Сетевые/LLM/provider-клиенты не импортируются.

## Тесты

[tests/test_stage_comparison_pipeline_v2_block_matching.py](../tests/test_stage_comparison_pipeline_v2_block_matching.py)
— synthetic модели (без реальных файлов и без сети): матч листов с раздвинутыми
номерами (52↔21) по имени, `contents↔contents`, `change_log↔change_log`,
схема по `sheet_name`, IoU-матч image/scheme-блоков, fuzzy text-блоков,
stamp↔stamp, запрет strong для несовместимых типов, односторонние блоки/страницы
в unmatched, warning/risk_flag на отсутствующих координатах, `duplicate_candidate`,
атомарная запись JSON, отсутствие сетевых/LLM-импортов и сквозная совместимость
с этапом 1 (`result_json → normalize_result_json/build_model → match`).

## Следующий этап — Entity extraction по matched blocks

Имея пары сопоставленных блоков, следующий слой извлекает из них **сущности** по
типам блоков/дисциплинам (профили из
[graphic_profiles](stage_comparison_block_pdf_source.md): отходящие линии/
автоматы/кабели для электрики, элементы/класс бетона/армирование для КР, строки
таблиц/ведомостей и т.д.). Сущности извлекаются детерминированно из текст-слоя/
`stamp_data`/excerpt'ов, образуя сравнимые наборы полей OLD↔NEW. На них уже
строится deterministic diff, а Opus подключается точечно — на конкретной паре
блоков/сущностей, а не на всём томе.

## Связанные файлы

- [pipeline_v2_block_matching.py](../backend/app/services/stage_comparison/pipeline_v2_block_matching.py)
- [pipeline_v2_prepared_ingest.py](../backend/app/services/stage_comparison/pipeline_v2_prepared_ingest.py) — этап 1 (вход)
- [stamp_matching.py](../backend/app/services/stage_comparison/stamp_matching.py) — родственный матч листов по штампу (старый путь)
- [block_equivalence_precheck.py](../backend/app/services/stage_comparison/block_equivalence_precheck.py) — родственный IoU-pairing блоков (старый путь)

# Stage Comparison Pipeline V2 — Block Link Preview

**Дата:** 2026-06-11
**Статус:** read-only витрина; ничего не применяет, ручные связи блоков не меняет.
**Модули:**
- builder: [backend/app/services/stage_comparison/pipeline_v2_block_link_preview.py](../backend/app/services/stage_comparison/pipeline_v2_block_link_preview.py)
- discovery/endpoint: [backend/app/services/stage_comparison/pipeline_v2_payload_service.py](../backend/app/services/stage_comparison/pipeline_v2_payload_service.py),
  [backend/app/api/routers/stage_comparison.py](../backend/app/api/routers/stage_comparison.py)
- UI: панель «🔗 Pipeline V2 связи β» в разделе «Сравнение стадий → 2. Связь блоков»

## Зачем

Раньше пользователь сопоставлял блоки OLD↔NEW вручную в «Связи блоков».
Pipeline V2 уже строит `block_matching_report.json` с предложенными связями.
Preview выводит их в UI: старая страница слева, новая справа, соответствующие
блоки подсвечены, видны method/confidence/risk_flags + визуальный статус из
visual equivalence gate. **Автоприменения нет** — это осознанно отдельный
следующий шаг.

## Конвейер

```text
left/right_normalized_document_model.json   (bbox, страницы, типы блоков)
block_matching_report.json                  (предложенные связи + unmatched)
[опц.] left/right_graphic_descriptor_report (readiness risk-флаги)
[опц.] visual_equivalence_gate_report       (identical/changed/uncertain)
  → build_block_link_preview()
  → block_link_preview_report.json
```

В dry-run это этап **[3c]** (после visual gate, до entity extraction),
fail-soft: падение builder'а даёт warning + `block_link_preview.status=failed`
в summary, этапы 4–6 работают как раньше. Отключение:
`options={"block_link_preview": {"enabled": False}}`.

## link_status (детерминированный)

| link_status | Когда | Цвет |
|---|---|---|
| `strong` | block match confidence=strong | green |
| `weak` | confidence medium/weak (исходная градация сохранена в `match_confidence`) | yellow |
| `manual_review` | visual gate decision=manual_review ИЛИ risk-флаг из `MANUAL_REVIEW_RISK_FLAGS` (duplicate_candidate, localized_residual_diff) | orange |
| `unmatched` | односторонний блок без пары | gray |

Синий (`SELECTED_COLOR`) зарезервирован за выбранной в UI связью — отчёт
«выбранность» не хранит.

## Формат отчёта

`kind = stage_comparison_pipeline_v2_block_link_preview`, `version = 1`.

* `summary` — counts: page_links_total, block_links_total,
  strong/weak/manual_review_links, unmatched_left/right_blocks,
  graphic_links_total, visual_identical/minor/changed/uncertain,
  visual_gate_available;
* `page_links[]` — matched пары страниц (`page_link_id` = match_id из
  block matching), номера страниц/имена листов обеих сторон, method/score,
  `block_link_ids[]` (группировка связей по странице),
  `block_links_by_status`;
* `block_links[]` — карточки связей: block ids/страницы/`*_bbox_norm`
  (из `coords_norm` модели; отсутствующий блок → null + risk-флаг
  `{side}_bbox_missing`), `semantic_type`, `is_graphic`, `link_status`,
  `method`, `confidence_score` (score match'а), `risk_flags`
  (match + visual gate + readiness-флаги дескрипторов),
  `visual_status/visual_decision/visual_metrics` (mask_iou, NCC,
  total_diff_ratio, alignment_method; null без visual gate),
  `ui {color,label,default_visible}`;
* `unmatched.left_blocks[] / right_blocks[]` — односторонние блоки
  с bbox/страницей/типом, `link_status=unmatched`;
* `warnings[]`.

Отсутствие visual gate отчёта — норма (визуальные поля null,
`visual_gate_available=false`); отсутствие/битый block_matching → ValueError
у builder'а (endpoint переводит в `not_found`/`error`).

## Read-only endpoint

```text
GET /api/stage-comparison/pipeline-v2/{session_id}/block-link-preview?pair_id=
```

1. готовый `block_link_preview_report.json` → отдаётся как есть
   (`source=ready_report`);
2. иначе, если есть models + block_matching → сборка **on-the-fly**
   (`source=built_from_artifacts`; на диск НЕ пишется);
3. иначе HTTP 200 `{"status": "not_found", "available": false, ...}` +
   `available_pairs` discovery.

Гарантии — те же, что у ui-payload endpoint'а: ничего не запускает
(ни Pipeline V2, ни модели, ни job'ы), ничего не пишет, не меняет статусы;
битые артефакты → fail-soft `status=error`, не 500; NaN/Inf санитизируются;
дисковое I/O в threadpool. Path convention:
`comparison/sessions/<sid>/pipeline_v2/` и
`comparison/sessions/<sid>/pairs/<pid>/pipeline_v2/`.

## UI («Связь блоков»)

Кнопка **«🔗 Pipeline V2 связи β»** в toolbar открывает read-only панель:

* выбор пары (дефолт — активная) + «Загрузить предложенные связи»;
* счётчики strong/weak/manual/unmatched/графика/visual;
* фильтры: все / strong / weak / manual_review / без пары / только графика /
  visual changed / visual identical;
* side-by-side: список пар страниц слева → старая страница | новая страница
  с bbox-overlay блоков (цвет по статусу; выбранная связь — синий контур
  поверх цвета; клик по блоку или строке списка выбирает связь);
* карточка выбранной связи: block ids, страницы, semantic_type, method,
  confidence, risk_flags, visual_status/decision, mask_iou/NCC;
* таблица связей (фильтрованная), включая unmatched.

`not_found` — нормальное состояние с подсказкой (артефакты появятся после
offline dry-run прогона). Кнопок применения связей НЕТ (намеренно — без
отдельного подтверждения пользователя автоприменение не делается). Существующий
ручной режим связывания не затронут: панель — отдельная карточка, ручные
связи/overlay/слоты работают как раньше.

Изображения страниц — существующий endpoint
`/sessions/{sid}/pairs/{pid}/page-image` (pair id берётся из селектора панели,
не из активной пары, чтобы preview другой пары не показывал чужие листы).

## Тесты

* [tests/test_stage_comparison_pipeline_v2_block_link_preview.py](../tests/test_stage_comparison_pipeline_v2_block_link_preview.py)
  — builder (статусы/цвета/bbox/группировка/visual join/без visual), endpoint
  (ready/on-the-fly/not_found/read-only snapshot/no-network/400/битый JSON),
  dry-run интеграция (артефакт+manifest+summary+MD, fail-soft, disable),
  source scan (никаких vision/LLM imports);
* [frontend/tests/pipeline_v2_block_link_preview.test.js](../frontend/tests/pipeline_v2_block_link_preview.test.js)
  — state machine ответа (not_found/error/401), фильтры, color map,
  overlay geometry (bbox→проценты, selected=blue), page overlays.

## Следующий шаг (отдельные задачи)

* graphic vision enrichment только для send_to_vision/manual_review блоков;
* кнопка «Принять strong-связи» (после отдельного подтверждения механики
  применения).

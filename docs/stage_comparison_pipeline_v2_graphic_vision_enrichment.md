# Stage Comparison Pipeline V2 — Graphic Vision Enrichment

**Дата:** 2026-06-11
**Статус:** offline-модуль, runner injectable; реальные Qwen/Gemma вызовы —
отдельной задачей. В dry-run слой по умолчанию **выключен**.
**Модуль:** [backend/app/services/stage_comparison/pipeline_v2_graphic_vision_enrichment.py](../backend/app/services/stage_comparison/pipeline_v2_graphic_vision_enrichment.py)

## Зачем

Visual Equivalence Gate уже разделил matched graphic blocks на три корзины:
`exclude_from_vision` (визуально идентичны), `send_to_vision` (видимое
изменение), `manual_review` (анти-dilution / неуверенность). Этот слой —
следующий шаг: подготовить и (при наличии runner'а) выполнить vision-описание
ТОЛЬКО тех пар, где описание имеет смысл.

```text
visual_equivalence_gate_report.json
  → select_blocks_for_vision        (send_to_vision / manual_review)
  → build_graphic_vision_enrichment_plan  (prompt + crop refs)
  → run_graphic_vision_enrichment(vision_runner=…)
  → graphic_vision_enrichment_report.json
```

**Почему `exclude_from_vision` не идёт в vision:** визуальная идентичность
после выравнивания — самое сильное свидетельство «блок не менялся». Vision на
таком блоке генерирует только description-variance, которая downstream
превращается в ложные дельты. Опция `include_exclude_from_vision=true`
существует исключительно для отладки.

**Почему `send_to_vision`/`manual_review` идут дальше:** там либо
подтверждённое изменение (нужно понять, ЧТО изменилось), либо алгоритм не
уверен (vision как второй взгляд). При cap `max_items` send_to_vision имеют
приоритет над manual_review.

## Runner injectable

Контракт (как `llm_runner` в delta explanation):

```python
def vision_runner(prompt: str, left_image_path: str | None,
                  right_image_path: str | None, options: dict) -> dict: ...
```

Модуль НЕ импортирует vision-модели/провайдеров и не делает сетевых вызовов.

* `vision_runner=None` → `status=skipped_no_runner`: кандидаты выбраны,
  prompt'ы и crop refs записаны в items, реальных вызовов нет;
* исключение/непригодный ответ runner'а → item `failed`, отчёт жив
  (per-item fail-soft); ВСЁ упало (включая render-фейлы до вызова) и ни
  одного успеха → `status=failed`; смешанный исход →
  `completed_with_warnings`; per-item сбои агрегируются в warning отчёта
  («vision items failed: N of M») и видны dry-run'у;
* ответ-строка парсится как JSON fail-soft; невалидный `confidence` → `low`
  с warning'ом.

Рендер кропов (PNG для runner'а) включается `render_crops=true` и происходит
только при наличии runner'а: cv2/fitz импортируются лениво, их отсутствие или
битый PDF — ошибка item'а, не модуля. Без runner'а на диск ничего не
рендерится — items несут только refs.

## Options

```json
{"graphic_vision": {
  "enabled": false,
  "max_items": 5,
  "include_manual_review": true,
  "include_exclude_from_vision": false,
  "write_prompts": true,
  "render_crops": true,
  "render_long_side": 1600,
  "runner_model": "qwen|gemma|fake"
}}
```

Усечение по `max_items` явно warn'ится (no silent caps) и считается в
`summary.dropped_by_cap`; `max_items <= 0` = безлимит. `crops_dir` обязателен,
когда `render_crops=true` и runner передан — иначе item честно `failed`
(«crops_dir not provided»), runner с пустыми изображениями не вызывается;
`render_crops=false` — явная no-image конфигурация (для fake runner'ов).
`write_prompts=false` влияет ТОЛЬКО на персистенцию `item.prompt` — runner
всегда получает полный prompt (со статусом gate и именами листов).

## Prompt contract

`VISION_PROMPT_TEMPLATE` — короткий строгий промпт: описать OLD, описать NEW,
перечислить ВИДИМЫЕ изменения, выписать инженерные сущности буквально
(оборудование/кабели/автоматы/линии/обозначения/помещения-оси-этажи); запреты:
не придумывать невидимое, без юридических выводов, нечитаемое → «[нечитаемо]».
Ответ — строго один JSON-объект (`old_description`, `new_description`,
`observed_changes[]`, `engineering_entities_old/new[]`, `possible_risks[]`,
`confidence high|medium|low`).

## Как читать report

`graphic_vision_enrichment_report.json`, kind
`stage_comparison_pipeline_v2_graphic_vision_enrichment`:

* `status`: `ok` | `completed_with_warnings` | `skipped_no_runner` |
  `skipped_no_visual_gate` | `failed`;
* `summary`: candidates_total (все пары gate), selected_total,
  excluded_by_visual_gate, manual_review_included (по ФАКТИЧЕСКОЙ выборке
  после cap), manual_review_skipped, other_skipped, dropped_by_cap,
  vision_calls_attempted / succeeded / failed, skipped_no_runner,
  runner_model;
* `items[]`: block ids/страницы, visual_status/decision/metrics из gate,
  graphic_type/discipline из дескрипторов, `left/right_crop_source`
  (image_file, pdf_path, page, bbox_norm) + `left/right_crop_ref`
  (rendered PNG → image_file → `pdf#page=N`), prompt (при
  `write_prompts`), `vision_status` (`ok|failed|skipped_no_runner`),
  `result`, `warnings`.

Отсутствие visual gate отчёта → `skipped_no_visual_gate` с пустыми items:
модуль никогда не выбирает кандидатов сам — решения принимает только gate.

## Интеграция в dry-run

Этап **[3d]** (после block link preview, до entity extraction), mark-only:
entity extraction / deterministic diff / delta explanation НЕ читают отчёт и
не меняются. Fail-soft: падение слоя → warning + `graphic_vision.status=
failed` в summary, этапы 4–6 работают. `vision_runner` — отдельный kwarg
`run_pipeline_v2_dry_run(..., vision_runner=...)`. Кропы рендерятся в
`out_dir/vision_crops/`. Артефакт включён в manifest.

## UI payload

`pipeline_v2_ui_payload.py` добавляет top-level секцию `graphic_vision`
ТОЛЬКО если секция есть в summary и слой был включён — старые payload'ы
полностью совместимы (frontend в этой задаче не менялся):

```json
"graphic_vision": {"available": true, "status": "ok", "selected_total": 5,
  "vision_calls_succeeded": 5, "vision_calls_failed": 0,
  "skipped_no_runner": 0}
```

## Что дальше (отдельная задача)

Реальный vision runner (Qwen/Gemma через существующую LM Studio
инфраструктуру) подключается снаружи по контракту `VisionRunner` — controlled
pilot на 3–5 блоках, без изменения этого модуля.

## Тесты

[tests/test_stage_comparison_pipeline_v2_graphic_vision_enrichment.py](../tests/test_stage_comparison_pipeline_v2_graphic_vision_enrichment.py)
— selection (exclude/send/manual/cap/приоритет), skipped_no_visual_gate,
no-runner (prompts/refs сохранены), fake runner (нормализация, JSON-строка,
invalid confidence), bad output / exception / пустые описания (fail-soft),
prompt contract, crop refs, render-failure path, dry-run (артефакт+manifest+MD,
default OFF, fail-soft, инвариантность deterministic deltas), ui_payload
(секция + backward compat), офлайн-гарантии (source scan + socket-patch).

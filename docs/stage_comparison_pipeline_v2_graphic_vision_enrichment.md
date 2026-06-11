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

## Candidate selection v2 (entity-aware) — после пилота

Пилот показал: слепой отбор «всех send_to_vision» тратит vision-бюджет на
пары, где position-based block matching свёл РАЗНЫЕ сущности (схема ВРУ-1 ↔
план ТП, ВРУ-3 ↔ ВРУ-2, ЯК ↔ ЩО-3) — vision честно отвечает «это разные
объекты», но это другой use-case. `candidate_selection: "entity_aware"`
включает scoring v2 (default — `legacy`, прежнее поведение):

* **entity-идентичность**: маркировки (ВРУ-1, ГРЩ, ЩО-3, ЯК5, ЩР-ТХ1…)
  извлекаются из имени листа + equipment/raw_key_entities токенов;
  primary-идентичность (только sheet_name) перебивает mention-pool
  (упоминание ГРЩ на схеме ВРУ-2 не подтверждает идентичность);
  `match` нумерованный (+0.2) / `family_only_match` bare (+0.05) /
  `numbered_conflict` ВРУ-3↔ВРУ-2 (−0.35, generic-токен «вру» обеих сторон
  его НЕ маскирует) / `family_conflict` ЯК↔ЩО (−0.3); анти-false-positive:
  «вручную»/«шум»/«ТПУ» не дают сущностей, «АВР 100А» — рейтинг, не номер,
  «ВРУ2-РП1» не утекает назначением РП-1;
* **вид листа** (схема/план/таблица/узел): совпадение +0.1, конфликт
  scheme↔plan −0.3;
* graphic_type/discipline match (graphic_matched_report или дескрипторы),
  `token_overlap.equipment`, `match_quality`, mask_iou/NCC, duplicate;
* **штамп** депризован (−0.35, `stamp_block_low_vision_value`) — дельты
  штампа ловит текстовый слой, vision-бюджет идёт на инженерную графику.

Каждый кандидат получает `candidate_score` (0..1), `candidate_rank`,
`candidate_reasons`, `candidate_risk_flags`, `candidate_kind`:

| kind | значение | enrichment | link_validation |
|---|---|---|---|
| `same_entity_likely` | одна сущность | основной пул (по score) | в конец |
| `validation_candidate` | риски без прямого конфликта | при недоборе | второй приоритет |
| `mismatch_likely` | entity/вид листа конфликтуют | исключён при `exclude_mismatch_likely=true` (default) | **первый приоритет** |
| `uncertain` | сигналов мало | при недоборе | третий |

`selection_mode`: `enrichment` (default) — vision описывает изменения одной
сущности; `link_validation` — vision целенаправленно проверяет подозрительные
связи («та ли это сущность?»). Smoke ИОС1.1: enrichment top5 = 5 инженерных
схем (ГРЩ/ВРУ-4/…, 0 штампов, 0 false-пар; mismatch_excluded=16 из 54);
link_validation top5 начинается с ВРУ-1↔план ТП.

## Render options (high_res / tiled-контракт)

```json
{"render": {"mode": "normal|high_res|tiled", "long_side": 1600,
            "dense_long_side": 2400, "tile_long_side": 1400, "max_tiles": 6}}
```

`normal` (default) — `long_side` для всех (legacy `render_long_side`
учитывается). `high_res` — для плотных типов (`cabinet_scheme`,
`single_line_scheme`, `dense_scheme`, `table_scheme`) берётся
`dense_long_side` (пилот: ГРЩ при 1600px дал low confidence по номиналам).
`tiled` — зарезервированный контракт (`tile_long_side`/`max_tiles`): до
реализации честно деградирует к high_res с warning'ом уже на merge опций —
независимо от того, дошёл ли прогон до рендера (TODO: пофрагментный рендер +
merge по образцу grsh_feeder_extraction). Невалидный mode → warning + normal.
`summary.render_mode` — ЭФФЕКТИВНЫЙ режим, `summary.render_mode_requested` —
запрошенный; планируемый long_side пишется в `item.render_long_side_used` у
ВСЕХ items (включая plan-only), runner получает per-item
`options.render_long_side`.

Real vision benchmark (сравнение моделей/разрешений) запускается отдельной
controlled-задачей — этот слой только готовит вход.

## Реальный local vision runner (pilot 2026-06-11)

[pipeline_v2_local_vision_runner.py](../backend/app/services/stage_comparison/pipeline_v2_local_vision_runner.py)
— тонкий адаптер контракта поверх существующей локальной инфраструктуры
(`graphic_llm_local.compare_images_local`: OpenAI-compatible LM Studio
endpoint, **two-image input в одном сообщении** — OLD первой картинкой, NEW
второй, basic auth, timeout/image_long_side из env-конфига).

```python
from backend.app.services.stage_comparison.pipeline_v2_local_vision_runner import (
    build_local_vision_runner)
runner = build_local_vision_runner()          # env-конфиг; sync-only
report = run_graphic_vision_enrichment(..., vision_runner=runner, crops_dir=...)
```

Свойства: обе стороны обязательны (односторонний item → ValueError → gv
failed); transport error/timeout → RuntimeError (gv failed, отчёт жив);
непарсабельный ответ → salvage JSON, иначе только `raw_text` (gv пометит
failed); `model_used`/`duration_sec` добавляются в result. Перед прогоном
рекомендуется `ensure_lmstudio_model_loaded(cfg.model)` (fast-profile JIT).

Controlled pilot на 5 инженерных блоках ИОС 1.1 (qwen/qwen3.6-35b-a3b):
5/5 ok, JSON contract соблюдён, OLD/NEW не перепутаны, конкретные номиналы
(T1N1600→T1N2000, 63А→20А, кабели/серии буквально), честные «[нечитаемо]» на
мелком тексте; vision честно вскрывает false block matches (схема↔план,
ВРУ-3↔ВРУ-2). Детали — diagnostics
`smoke_ios11_real_vision_pilot_*/PILOT_SUMMARY.md`.

## Тесты

[tests/test_stage_comparison_pipeline_v2_graphic_vision_enrichment.py](../tests/test_stage_comparison_pipeline_v2_graphic_vision_enrichment.py)
— selection (exclude/send/manual/cap/приоритет), skipped_no_visual_gate,
no-runner (prompts/refs сохранены), fake runner (нормализация, JSON-строка,
invalid confidence), bad output / exception / пустые описания (fail-soft),
prompt contract, crop refs, render-failure path, dry-run (артефакт+manifest+MD,
default OFF, fail-soft, инвариантность deterministic deltas), ui_payload
(секция + backward compat), офлайн-гарантии (source scan + socket-patch).

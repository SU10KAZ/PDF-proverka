# Stage Comparison Pipeline V2 — Portal/UI payload contract

**Дата:** 2026-06-10
**Статус:** offline adapter, **НЕ endpoint и НЕ frontend**; к live backend НЕ
подключён, backend restart НЕ требуется.
**Модуль:** [backend/app/services/stage_comparison/pipeline_v2_ui_payload.py](../backend/app/services/stage_comparison/pipeline_v2_ui_payload.py)
**Вход:** готовые артефакты dry-run — `pipeline_v2_summary.json` (+ опц.
`entity_diff_report.json`, `delta_explanation_report.json`, graphic descriptor
отчёты).

## Зачем нужен UI payload

`pipeline_v2_summary.json` рассчитан на инженера, читающего JSON/MD руками.
Будущему порталу нужен другой срез: компактные секции с карточками дельт,
headline-счётчики «что показать первым», значения для фильтров и понятный
сигнал о готовности графики. Этот модуль фиксирует **контракт данных** для
портала ЗАРАНЕЕ — фронтенд можно проектировать по нему, не дожидаясь endpoint'а.

```text
pipeline_v2_summary.json (+ diff/explanation отчёты)
  → build_pipeline_v2_ui_payload(...)
      ├─ headline (счётчики для шапки)
      ├─ sections[] (5 секций, каждая со своими cards[])
      ├─ filters (значения для фильтров UI)
      ├─ graphic_readiness (светофор по графике)
      └─ warnings / artifact_refs
  → write_pipeline_v2_ui_payload(out_path, payload)   # атомарная запись
```

## Что это НЕ

- **НЕ endpoint** — никаких роутов/FastAPI; модуль не импортируется live
  backend'ом и не регистрируется в API;
- **НЕ frontend** — только данные; вёрстка/Vue не затронуты;
- **НЕ live-вызов** — stdlib-only, без сети, без локальных/облачных LLM;
- **НЕ новая логика сравнения** — секционирование берётся из готового
  `delta_sections` (или пересобирается тем же кодом
  `pipeline_v2_dry_run.build_delta_sections`, если summary старого формата).

## Секции и UX-правила (зашиты в payload)

| key | title | badge | default_visible | display_hint | в диагностике |
|---|---|---|---|---|---|
| `confirmed_changes` | Подтверждённые изменения | `confirmed` | **true** | `normal` | — |
| `needs_review` | На ручную проверку | `review` | **true** | `normal` | — |
| `weak_graphic_review` | Слабая графика / нужна доработка | `weak_graphic` | **true** | `warning` | — |
| `likely_noise_hidden_by_default` | Вероятный шум | `noise` | **false** | `hidden` | — |
| `llm_failed_or_skipped` | Необъяснённые / ошибки LLM | `failed` | **false** | `diagnostics` | **да** (`show_in_diagnostics=true`) |

UI обязан уважать `default_visible`/`display_hint`: шум и LLM-сбои не должны
засорять основной экран, но обязаны быть доступны (фильтр «показать скрытое» и
вкладка диагностики). `weak_graphic_review` показывается по умолчанию, но как
предупреждение: «пустота» на слабой графике — это «нужна дообработка vision»,
а не «изменений нет».

Маппинг дельты в секцию идентичен `delta_sections` summary (приоритет
llm_failed → noise → weak_graphic → needs_review → confirmed, без двоения) —
см. [stage_comparison_pipeline_v2_dry_run.md](stage_comparison_pipeline_v2_dry_run.md).

## Как читать карточку

```json
{
  "delta_id": "delta_…", "section": "confirmed_changes", "badge": "confirmed",
  "title": "Смена проектной организации",
  "subtitle": "stamp_field · changed · high risk",
  "entity_type": "stamp_field", "delta_type": "changed",
  "field": "organization", "subject": "organization",
  "old_value": "A R T E L A R C H I T E C T S", "new_value": "ИНПАД",
  "confidence": 0.85, "risk_level": "high",
  "critic_verdict": "accept", "groundedness": "grounded",
  "should_show_to_engineer": true,
  "summary": "…краткое объяснение…", "contractor_impact": "…",
  "quality_flags": [],
  "page_numbers": {"left": 1, "right": 1},
  "block_ids": {"left": "…", "right": "…"}
}
```

- `title` — человекочитаемый заголовок: summary критика, при его отсутствии
  детерминированный `change_summary` дельты;
- `subtitle` — компактная строка для списка (`entity · type · risk`);
- `old_value`/`new_value`/`summary`/`contractor_impact` **обрезаны** (160
  символов, `truncate_ui_text`) — карточка пригодна для preview без загрузки
  полного diff;
- гигантские evidence-quotes в карточку **не тащатся**: для перехода к
  первоисточнику достаточно `page_numbers` + `block_ids` (+ `delta_id` для
  полного отчёта);
- `critic_verdict`/`groundedness`/`risk_level` = `null`, если у дельты нет
  explanation (например, отчёт частичный) — карточка всё равно строится из
  полей дельты;
- `quality_flags` — объединение флагов дельты и explanation (например,
  `fuzzy_match`, `possible_ocr_noise`, `skipped_no_runner`).

## Headline и фильтры

`headline` — счётчики для шапки экрана: `deltas_total` (всего у diff),
`selected_for_explanation_total` (сколько ушло в LLM), per-section totals
(`confirmed_total`, `needs_review_total`, `weak_graphic_total`,
`hidden_noise_total`, `failed_or_skipped_total`) и `coverage_notes_total`
(сигналы «по графике возможны пропуски»).

`filters` собираются из реально присутствующих карточек:
`entity_types` / `risk_levels` / `critic_verdicts` / `delta_types` — UI строит
фильтр-панель из этих значений, не хардкодя списки.

`graphic_readiness` — компактный светофор: `status`
(`ok` / `needs_vision_enrichment` / `manual_review_required` /
`no_graphic_blocks`) + counters + `by_readiness`; при переданных
graphic-отчётах добавляется `weak_blocks_preview` (≤20 слабых блоков с
block_id/страницей) для прямых ссылок из UI. Weak-набор флагов — канонический
из `pipeline_v2_delta_explanation` (тот же, что строит `coverage_notes`),
поэтому preview и `coverage_notes_total` не расходятся.

Карточки секции ограничены cap'ом (default 100, override через
`options.max_cards_per_section`): `count`/`delta_ids` остаются полными.
Срез ДЕФОЛТНЫМ cap'ом честно репортится warning'ом
`cards truncated to N of M` (silent caps запрещены); явный options-cap —
осознанный выбор вызывающего, warning не пишется.

## Какие поля нужны будущему порталу

Минимум для первого экрана: `status`, `headline`, `sections[]`
(key/title/badge/default_visible/display_hint/count/cards), `filters`,
`graphic_readiness.status`, `warnings`. `artifact_refs` — имена файлов
артефактов dry-run (резолвятся относительно каталога прогона) для кнопки
«открыть полный отчёт». `delta_ids` в секции — для deep-link на дельту, даже
если её карточка не построилась (неполные отчёты).

## Fail-soft гарантии

Граница ответственности: **artifact-данные** (всё, что читается из JSON-файлов
прогона) никогда не роняют builder — junk коэрцируется/отбрасывается с
warning'ом; мусор в **аргументах вызова** (options и т.п.) — ошибка
программиста, может падать fail-fast.

- нет `entity_diff_report` → карточки строятся из `explanation.input_delta`,
  warning, статус деградирует `ok → completed_with_warnings`;
- нет `delta_explanation_report` → карточки из дельт без critic-полей, warning;
- нет обоих → counts/delta_ids из summary сохраняются, cards пустые, warning;
- summary без `delta_sections` (старый формат) → секции пересобираются из
  отчётов тем же кодом, что в dry run (достаточно ОДНОГО из отчётов —
  `build_delta_sections` fail-soft к каждому);
- summary не словарь → `status=failed`, пять пустых секций, payload валиден;
- неизвестная будущая секция в `delta_sections` → не теряется (generic-секция
  + warning `unknown_delta_section:<key>`);
- junk в полях summary → warning вместо краша: `artifacts` не-dict →
  `artifact_refs={}`; `warnings` не-список → игнор; счётчики
  (`selected_total`, `coverage_notes.count`, graphic-counters) null/строка →
  безопасная int-коэрция; `delta_ids` не-список/с мусорными элементами →
  чистка с warning'ом (строка НЕ разбирается посимвольно); смешанные типы в
  `quality_flags` → строковая коэрция;
- статус деградирует `ok → completed_with_warnings` только от
  adapter-warning'ов (неполнота payload); warnings самого summary
  пробрасываются, но статус не меняют — dry run уже учёл их в своём статусе.

## Почему backend restart НЕ нужен на этом этапе

Модуль — чистая offline-библиотека: ни один live-роутер его не импортирует,
поведение работающего uvicorn-процесса не меняется ни на байт. Файл просто
появляется в дереве; он начнёт исполняться только когда его явно вызовут
(тесты, offline-скрипт или будущий endpoint).

## Следующий шаг (отдельная задача, потребует подтверждения)

Read-only API endpoint (например,
`GET /api/stage-comparison/…/pipeline-v2/ui-payload`), который вызывает
`build_pipeline_v2_ui_payload` по сохранённым артефактам, плюс панель в
портале. Это уже изменение live backend: **потребуется backend restart и
отдельное явное подтверждение пользователя** (на сервере могут идти рабочие
процессы) — см. протокол в feedback-памяти и runbook'ах деплоя.

## Тесты

[tests/test_stage_comparison_pipeline_v2_ui_payload.py](../tests/test_stage_comparison_pipeline_v2_ui_payload.py)
— synthetic summary/diff/explanations (без сети/LLM): построение payload,
маршрутизация всех 5 секций, отсутствие дублей, compact-поля и truncation
карточек, фильтры, UX-дефолты (default_visible/display_hint/diagnostics),
graphic_readiness + weak_blocks_preview, деградация на неполных входах
(missing explanation/diff/оба/битый summary/старый summary без delta_sections),
неизвестная будущая секция, атомарная запись JSON, офлайн-гарантии
(socket-monkeypatch + скан источника на provider/network импорты).

## Связанные файлы

- [pipeline_v2_ui_payload.py](../backend/app/services/stage_comparison/pipeline_v2_ui_payload.py) — adapter
- [pipeline_v2_dry_run.py](../backend/app/services/stage_comparison/pipeline_v2_dry_run.py) — `delta_sections` (источник секционирования)
- [stage_comparison_pipeline_v2_dry_run.md](stage_comparison_pipeline_v2_dry_run.md) — приоритет секций
- [stage_comparison_pipeline_v2_delta_explanation.md](stage_comparison_pipeline_v2_delta_explanation.md) — формат explanation/critic

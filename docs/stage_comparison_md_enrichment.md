# Stage Comparison — Qwen MD Image Enrichment

**Дата:** 2026-05-26
**Production cutover:** v4_compact + salvage-first + conditional fallback

## Архитектура

Pipeline для «Сравнение стадий → 1. Загрузка документации» использует
локальный Qwen-vision (`qwen/qwen3.6-35b-a3b` через LM Studio + ngrok)
для извлечения структурированных описаний из image/imagine блоков MD'шек.

```text
MD (Chandra OCR)
  → parse_md_blocks → ordered list of text/image blocks
  → render_block_crop(side_block_id) → PNG
  → describe_image_local(prompt v4_compact) → DescribeResult
      ├─ primary qwen call
      ├─ [conditional] mtp fallback (only if primary unsalvageable)
      ├─ salvage_partial_json on truncated JSON
      └─ continuation loop (forced continues=true if salvaged)
  → enrich_side merges items into enriched MD
  → build_enriched_md → <side>_enriched.md
  → save items + diagnostics → <side>_image_descriptions.json
```

## Production defaults (2026-05-26)

| Параметр | Значение | Где |
|---|---|---|
| `PROMPT_VERSION` | `v4_compact` | `md_image_enrichment.py` |
| `STAGE_COMPARISON_GRAPHIC_LLM_MAX_TOKENS` | `5500` | `.env` |
| `STAGE_COMPARISON_GRAPHIC_LLM_MAX_CONTINUATIONS` | `3` | `.env` |
| `STAGE_COMPARISON_GRAPHIC_LLM_LOAD_CONTEXT_LENGTH` | `16000` | `.env` |
| primary model | `qwen/qwen3.6-35b-a3b` | `.env` |
| fallback model | `qwen3.6-35b-a3b-mtp` (rare-use) | `.env` |

Benchmark 2026-05-26 (4 heavy HVAC blocks): success 100%, avg 27s, max 32s,
avg nodes 25.5, avg conns 3.0.

## Salvage-first architecture

**Ключевая идея:** truncated JSON от primary salvage-friendly.
Не звать второй call впустую — обрабатываем primary content локально.

```text
primary returns invalid_json
  ↓
_classify_parse_error(content)
  ↓
  ├─ truncated_json | malformed_json   → пропускаем fallback, salvage primary
  ├─ markdown_reasoning                → пробуем fallback (primary unsalvageable)
  ├─ no_opening_brace                  → пробуем fallback
  ├─ empty_content                     → пробуем fallback
  └─ http_error                        → пробуем fallback
```

См. `_should_try_fallback_after_invalid_json` в
[backend/app/services/stage_comparison/graphic_llm_local.py](../backend/app/services/stage_comparison/graphic_llm_local.py).

## Conditional fallback policy

Старый bug (до 2026-05-26): fallback `qwen3.6-35b-a3b-mtp` вызывался
ВСЕГДА при invalid_json. На v2_scheme_analysis prompt'е mtp писал
markdown reasoning («1. Analyze the Request:») без единой `{`.

Salvage потом выбирал САМЫЙ ДЛИННЫЙ content_text → markdown reasoning
перебивал короткий truncated JSON primary'я → salvage не находил `{` →
блок уходил в json_parse_failed на пустом месте.

**Fix:** см. `_pick_salvage_candidate` — приоритет content с `{`
максимальной длины. Markdown без `{` отбраковывается даже если он длиннее.

## Compact prompt philosophy

V2 prompt был 230 строк с агрессивными «ВНИМАНИЕ:» / «ПОВТОРЯЮ:» /
«ФИНАЛЬНАЯ ПРОВЕРКА». Эти повторы подталкивали модель в chain-of-thought
(особенно mtp), который потом не лез ни в JSON ни в salvage.

V4 prompt сокращён до ~85 строк:
* короткие строгие правила без повторов;
* **explicit array limits**: `nodes` ≤30, `connections` ≤30,
  `numeric_parameters` ≤40, `visible_text` ≤25,
  `comparison_relevant_facts` ≤8, `comparison_relevant_scheme_facts` ≤8,
  `uncertainties` ≤5 — модель знает верхнюю границу и не пытается
  перечислить «всё», обрезая JSON на max_tokens;
* `continues`/`next_chunk_hint`/`coverage_notes` в КОНЦЕ JSON, не в
  начале (V1 bug: модель «бронировала» continues=false в начале и
  обрывалась посреди тела).

Лимиты также экспонированы в Python: `COMPACT_PROMPT_LIMITS` в
[md_image_enrichment.py](../backend/app/services/stage_comparison/md_image_enrichment.py).

## Continuation strategy

```text
chunk1 (primary)
  ↓ status=partial (salvaged)
  ↓ force continues=true if not set (synthetic hint)
chunk2 (continuation, pinned to same model, allow_fallback=False)
  ↓ merge into base_parsed
  ↓ check stuck-hint / no-progress → break early
  ↓ status=partial if salvaged in any chunk
```

Защиты:
* `cap = STAGE_COMPARISON_GRAPHIC_LLM_MAX_CONTINUATIONS` (default 3) — бесконечного
  цикла быть не может, всегда есть верхняя граница.
* **stuck-hint detection**: если модель в continuation вернула тот же
  `next_chunk_hint`, что и в предыдущем chunk'е — break + warning.
* **no-progress detection**: `_content_signature` сравнивает размеры
  основных списков ДО и ПОСЛЕ merge'а. Если ничего не добавилось — break.
* `allow_fallback=False` на continuation: mtp никогда не лезет в
  середине описания одной картинки.

## CTX preload mandatory

LM Studio JIT поднимает модель с default ctx=4096. Это катастрофично
для compact v4 prompt (~2300 prompt_tokens): остаётся ~1800 токенов
на ответ, JSON хронически обрезается.

`run_md_enrichment_job` зовёт `ensure_lmstudio_model_loaded(primary,
allow_fallback=False)` в самом начале. Если ctx<16000 — primary
unload + reload c desired ctx. protected models (chandra-ocr-2) не
трогаются: если бы они были primary, возвращается
`context_length_mismatch_protected` без reload.

## Diagnostics

### Per-item (в `<side>_image_descriptions.json`, item[])

* `finish_reason` — `stop` | `length` | `error`
* `usage` — `{prompt_tokens, completion_tokens, total_tokens}`
* `response_char_count` — длина content_text от первого chunk'а
* `parse_error_detail` — категория сбоя: `markdown_reasoning`,
  `truncated_json`, `malformed_json`, `no_opening_brace`,
  `empty_content`, `http_error`, `salvaged_from_invalid_json`,
  `salvage_no_safe_boundary`, `salvaged_with_continuation`
* `raw_response_path` — полный raw на диске (не обрезан excerpt'ом)
* `used_prompt_version` — какая prompt-version применена (для cache
  invalidation diagnostics)
* `compact_mode_used` — bool
* `chunks_count`, `continuation_count`, `continued`
* `salvaged`, `fallback_used`, `model_used`
* `final_status_reason` — короткий enum: `primary_done`,
  `salvaged_with_continuation`, `salvaged_partial`, или категория
  ошибки

### Session-level (в `aggregate_job_progress(...)["diagnostics"]`)

* counts: `blocks_done`, `blocks_partial`, `blocks_error`
* durations: `avg_duration_sec`, `p95_duration_sec`, `max_duration_sec`
* rates: `continuation_rate`, `salvage_rate`, `fallback_rate`,
  `compact_mode_rate`
* tokens: `tokens.{prompt, completion, total}`
* distributions: `parse_error_distribution`,
  `final_status_reason_distribution`, `finish_reason_distribution`
* `eta_sec` для running jobs (если есть данные для экстраполяции)

### Per-pair (в `pair_statuses[pid]`)

* `status`: `done` | `partial` | `error` | `running` | `queued` |
  `skipped`
* `ready_for_unified_analysis`: bool — оба side='done'/'skipped',
  без errors
* `block_metrics.{left,right}` — block-level metrics для tooltip'а
* `problem_hint` — человекочитаемое объяснение `partial`/`error`
  состояния (например: «JSON обрезан max_tokens — увеличьте лимит»)

## Failure recovery

Pipeline защищён на нескольких уровнях:

1. **Per-block:** salvage + continuation спасают почти все блоки.
2. **Per-side:** `enrich_side` пишет items даже на ошибках, enriched MD
   создаётся с status=pending/error для проблемных блоков.
3. **Per-pair:** `run_md_enrichment_job` выполняет каждый item в
   try/except — одна пара не валит весь job.
4. **Per-job:** preflight ensure_lmstudio_model_loaded не блокирует
   job на failure (warning в job["warnings"], processing продолжается).
5. **Cancellation:** проверка `latest.status == "cancelled"` перед
   каждым item; cancel чистит queued items в `cancel_job`.
6. **Stale-job detection (uvicorn restart / crash):** при чтении job из
   `<sid>/jobs/<jobid>.json` `_maybe_mark_interrupted` проверяет, есть
   ли живая `asyncio.Task` в `_active_tasks[sid][jobid]`. Если нет
   (uvicorn перезапустился, поток умер) — статус переписывается в
   `failed_interrupted`, незавершённые items тоже. UI показывает
   баннер «⚡ Распознавание было прервано» и кнопку
   «▶ Продолжить (пропустить готовые)» вместо вечного `running`.

   Для `queued` есть 60s grace period (`_STALE_QUEUED_GRACE_SECONDS`),
   чтобы не помечать только что созданный job до того как
   `start_job_in_background` зарегистрирует таску.

## Resume after interrupt

Кнопка «▶ Продолжить (пропустить готовые)» — это обычный POST
`/md-enrichment-jobs` с `{scope:"session", side:"both", force:false,
skip_done:true, confirm:true}`. Готовые стороны определяются через
`_pair_summary_is_done` (есть enriched MD без ошибок) и попадают в
`items` со `status=skipped`. Остальные стороны процессятся как обычно.

## Operator runbook

### Если массово появляются `markdown_reasoning` блоки

```bash
# Проверить, что primary qwen загружен с правильным ctx:
curl -s http://localhost:8081/api/comparison/graphic-llm-config | jq
```

Должно быть `primary_context_ok: true, primary_loaded_ctx: 16000`.
Если нет — перезагрузить primary через `ensure_lmstudio_model_loaded`
или вручную в LM Studio.

### Если массово `truncated_json` без recovery

Бампнуть env:
```
STAGE_COMPARISON_GRAPHIC_LLM_MAX_TOKENS=7500
STAGE_COMPARISON_GRAPHIC_LLM_MAX_CONTINUATIONS=4
```

Удалить failed блоки и rerun:
```bash
python backend/scripts/clear_failed_image_descriptions.py <session_id> --include-salvaged
```

Затем перезапустить job через UI или API.

### Если падает на конкретной паре

Запустить retry только этой пары через UI (force=true). Поскольку
PROMPT_VERSION в cache_key, бамп `PROMPT_VERSION` автоматически
инвалидирует старый кеш.

## Связанные файлы

* [backend/app/services/stage_comparison/md_image_enrichment.py](../backend/app/services/stage_comparison/md_image_enrichment.py)
* [backend/app/services/stage_comparison/graphic_llm_local.py](../backend/app/services/stage_comparison/graphic_llm_local.py)
* [backend/app/services/stage_comparison/md_enrichment_jobs.py](../backend/app/services/stage_comparison/md_enrichment_jobs.py)
* [backend/scripts/clear_failed_image_descriptions.py](../backend/scripts/clear_failed_image_descriptions.py)
* [tests/test_stage_comparison_md_enrichment.py](../tests/test_stage_comparison_md_enrichment.py)
* [tests/test_stage_comparison_graphic_local_llm.py](../tests/test_stage_comparison_graphic_local_llm.py)

# v5: Diff-anchor enrichment для электрических схем (2026-05-27)

Расширение Qwen-pipeline'а под ключевую проблему: на однолинейных схемах
электрики Qwen уходил в общее «описание картинки», обобщал «Щит 1» вместо
буквального «ЩР-1а» и галлюцинировал ряды «ВРП-1 ... ВРП-50». В результате
Opus при сравнении стадий не видел реальной разницы между ЩР-1а и ЩР-2
и ставил большинство changes как `source=text`.

Pipeline дополнен ПЯТЬЮ контурами:

```text
parse_md_blocks
  → classify_image_block (block_type)
  → per-type render_target_long_side + image_input_long_side
  → per-type prompt (v4_compact ↔ v5_scheme_diff_anchors)
  → describe_image_local с per-block cfg override
  → analyze_qwen_description_quality (usable_for_diff + warnings)
  → enrich_side
build_enriched_md
  → IMAGE_DIFF_INDEX (компактный список raw маркировок) в начале
  → QWEN_IMAGE_DESCRIPTION блоки (с DIFF_ANCHORS секцией до summary)
```

## Block types и per-type config

`classify_image_block(mb, side_block, surrounding_context)` смотрит на:
* заголовок MD-блока;
* окружающий MD-текст (агрегирован по странице);
* `area_ratio` / `bbox` из `result.json`, если есть.

Markers (упрощённо):
* strong scheme: `ВРУ`, `ВРП`, `ЩР`, `ЩО`, `ЩАО`, `ГРЩ`, `ЩС-`, `QF`, `QS`,
  `KM`, `АВР`, `РУ-`, `с.ш.`;
* weak scheme: `однолинейн`, `схема`, `кабел`, `автомат`, `линия`;
* table/legend: `таблица`, `спецификация`, `экспликация`, `ведомость`,
  `условные обозначения`, `перечень`;
* stamp: `стадия`, `лист`, `изм.`, `подп.`, `шифр`, `лит.`, `разраб.`, `пров.`;
* plan: `план`, `этаж`, `помещени`, `ось`, `оси`, `трасса`.

`BLOCK_TYPE_CONFIG`:

| block_type | render long_side | qwen input long_side | max_tokens | max_continuations | prompt |
|---|---|---|---|---|---|
| `photo_or_general` | 1200 | 1100 | (cfg default) | (cfg default) | `v4_compact` |
| `scheme` | 2400 | 2200 | 8500 | 4 | `v5_scheme_diff_anchors` |
| `dense_scheme` | 3000 | 2800 | 10000 | 4 | `v5_scheme_diff_anchors` |
| `table_legend` | 2200 | 2000 | 7000 | 3 | `v4_compact` |
| `stamp` | 1800 | 1600 | 3500 | 1 | `v4_compact` |
| `plan` | 2200 | 2000 | 7000 | 3 | `v4_compact` |

Рендер на стороне store: `render_block_crop(target_long_side=...)`. Если
наблюдатель render_crop не принимает kwarg (legacy/тесты), `_make_render_callback`
автоматически фоллбэчится к старому контракту.

Qwen input: для каждого блока строится `dataclasses.replace(cfg, image_long_side=...,
max_tokens=..., max_continuations=...)`. Глобальный cfg не меняется.

## v5_scheme_diff_anchors prompt

Цель — извлечь БУКВАЛЬНЫЕ diff-якоря, а не плавный summary. Структура JSON:

```json
{
  "status": "done",
  "image_kind": "scheme",
  "diff_anchors": {
    "labels": [
      {"raw_text": "ЩР-1а", "normalized_type": "panel", "confidence": 0.86,
       "comment": "видимо рядом с отходящей линией"}
    ],
    "ratings": [
      {"raw_text": "1000А", "value_type": "current_rating",
       "related_to": "ВРУ-2 с.ш.1", "confidence": 0.80}
    ],
    "connections": [
      {"from_raw": "ВРУ-2 с.ш.1", "to_raw": "ЩР-1а",
       "relation": "питает", "confidence": 0.72}
    ],
    "uncertain_text": [
      {"possible_text": "ЩР-1?", "alternatives": ["ЩО-1?", "ЩР-1а?"],
       "confidence": 0.45, "why_uncertain": "мелкий шрифт"}
    ]
  },
  "summary": "…короткий summary без новых фактов…",
  "visible_text": ["…"],
  "numeric_parameters": [{"name":"...","value":"...","unit":"...","context":"..."}],
  "scheme_analysis": { … },
  "confidence": 0.0,
  "coverage_notes": "…",
  "continues": false,
  "next_chunk_hint": ""
}
```

`normalized_type` для labels: `panel | switchgear | breaker | line | cable | room | stamp | other`.
`value_type` для ratings: `current_rating | cable_section | power | voltage | quantity | other`.

Жёсткие правила в prompt'е (нарушение → hallucination_suspected/repeated_pattern_detected):

* `raw_text` — буквальная видимая надпись («ЩР-1а», не «Щит 1»);
* `[маркировка не читается]` — если виден тип объекта, но маркировка нечитаемая;
* `uncertain_text` — для plausible-but-unsure распознаваний;
* НЕ добавлять типовые номиналы/сечения без видимого основания;
* НЕ перечислять искусственные ряды (ВРП-1...50, QF1...50);
* НЕ использовать текст соседних изображений / organisation/address без видимости.

Cache-key включает per-block `prompt_version`, поэтому v5-блоки не пересекаются
со старым v4-кешем (`compute_image_cache_key(img, model, prompt_version)`).

## DIFF_ANCHORS в enriched MD

`_format_qwen_description_md()` рендерит секции в этом порядке:

```text
### Графический блок / схема
Модель: …
Страница: 24
Block ID: …

DIFF_ANCHORS — буквальные маркировки:
- ЩР-1а [panel] (уверенность: 0.86) — видимо рядом с отходящей линией
- ВРУ-2 с.ш.1 [switchgear] (уверенность: 0.85)
- QF3 [breaker] (уверенность: 0.70)

DIFF_ANCHORS — кабели, номиналы, мощности:
- 1000А [current_rating] → ВРУ-2 с.ш.1 (уверенность: 0.80)
- 4х185 [cable_section]

DIFF_ANCHORS — связи:
- ВРУ-2 с.ш.1 → ЩР-1а (питает) [уверенность: 0.72]

Неуверенно прочитанные надписи:
- ЩР-1? (варианты: ЩО-1?, ЩР-1а?) [уверенность: 0.40] — мелкий шрифт

Краткое описание:
…
```

DIFF_ANCHORS-секции идут **до** Краткое описание — Opus читает буквальные
маркировки раньше плавного текста.

## IMAGE_DIFF_INDEX

`build_image_diff_index(descriptions)` строит компактный индекс. Он
вставляется в enriched MD сразу после `<!-- ENRICHED_MD_FORMAT: replace_image_blocks_v1 -->`:

```text
<!-- IMAGE_DIFF_INDEX_START -->
## Page 24 / block 4TMD-ECUV-VHR / scheme / confidence 0.74 / usable_for_diff=true

labels:
- ЩР-1а
- ЩР-2
- ВРУ-2 с.ш.1
- QF3

ratings:
- 1000А
- 4х185

connections:
- ВРУ-2 с.ш.1 -> ЩР-1а
- ВРУ-2 с.ш.1 -> ЩР-2

warnings:
- none

## Page 26 / block 7PP4-KPTP-4PD / dense_scheme / confidence 0.41 / usable_for_diff=false

labels:
- ВРП-1?
- ВРП-2?

warnings:
- hallucination_suspected
- continuation_salvaged
- repeated_pattern_detected
<!-- IMAGE_DIFF_INDEX_END -->
```

Назначение: дать Opus'у быстрый сводный список того, что реально видно на
картинках, до прочтения каждого QWEN_IMAGE_DESCRIPTION тела.

Fallback для v4-блоков (без `diff_anchors`): используем
`visible_text`, `numeric_parameters`, `scheme_analysis.nodes[].visible_mark/label`,
`scheme_analysis.connections`.

`compute_image_diff_index_summary(descriptions)` возвращает session-level
метрики: `total_anchor_labels`, `total_anchor_ratings`, `total_anchor_connections`,
`blocks_with_diff_anchors`, `usable_for_diff_true`, `usable_for_diff_false`.

## usable_for_diff и hallucination warnings

`analyze_qwen_description_quality(desc_payload, item_context)` возвращает
`{usable_for_diff, warnings, adjusted_confidence}`.

Серьёзные warnings (выставляют `usable_for_diff=false`):
* `hallucination_suspected` — комбо-флаг при ≥2 hallucination сигналов
  (см. composite scoring в секции «v5 production tuning»);
* `unexpected_org_or_address_text` — best-effort: anchors/summary содержит
  ОOO/АО/г./ул. строки, отсутствующие в surrounding MD;
* `low_literal_label_recall` — для scheme/dense_scheme labels пусты/только
  generic (`Щит 1`, `[маркировка не читается]`);
* `serial_chain_connection_detected` — последовательная цепочка
  `A.N → A.N+1` ≥5 шагов внутри одной серии (см. anti-chain rule).

Дополнительные info-warnings (не убирают usable_for_diff):
* `repeated_pattern_detected` — обнаружен искусственный ряд ≥6 подряд
  (ВРП-1...50) — info-level; в МКД 6-8 квартирных щитов реально могут
  быть. Эскалирует в hallucination только при суперпозиции сигналов.
* `truncated_output` — JSON оборвался на max_tokens. С prompt cap=25/20/15
  + max_tokens=4000 dense_scheme штатно truncates на cap-fill — это
  ожидаемое поведение, не катастрофа. Эскалирует в hallucination только
  при суперпозиции с repeated/chain/identical_comments/generic_ratings.
* `continuation_salvaged` — был salvage из обрезанного JSON;
* `continuation_repeated` — continuation возвращала тот же hint без прогресса;
* `generic_rating_list_without_labels` — много generic ratings, при этом
  labels пустые/generic.

`usable_for_diff=false` НЕ удаляет блок из enriched MD. Он по-прежнему
рендерится с DIFF_ANCHORS-секцией и всем телом, но Opus получает явный
сигнал, что одного этого блока недостаточно для нового change'а.

## Opus comparison: source attribution + evidence[]

`enriched_comparison.SYSTEM_PROMPT` упомянул IMAGE_DIFF_INDEX и расширил
правила выбора `source`:

* `text` — изменение ИСКЛЮЧИТЕЛЬНО в обычном текстовом слое. Если хоть
  один evidence визуальный → `text` запрещён.
* `table` / `stamp` — обычные таблицы / штамп.
* `image_enrichment` — изменение из visible_text / labels /
  numeric_parameters Qwen-описания image-блока ИЛИ из IMAGE_DIFF_INDEX.
* `scheme_analysis` — изменение из image-derived graph relations.
  Текстовый список листов в пояснительной записке — это `text`/`table`,
  не `scheme_analysis`.
* `mixed` — обязательно, когда текст/таблица/штамп и визуальный источник
  подтверждают одно и то же изменение.

`usable_for_diff=false` блок не может быть единственным основанием для
нового change'а — только weak confirmation при `requires_human_review=true`.

### evidence[] (optional)

В дополнение к `evidence_left` / `evidence_right` (обязательны, backward-compat)
добавлен опциональный массив `evidence[]`:

```json
"evidence": [
  {
    "origin": "text|table|stamp|image_enrichment|scheme_analysis|image_diff_index",
    "side": "left|right",
    "page": 24,
    "block_id": "optional",
    "quote": "Короткая цитата/якорь, до 240 символов"
  }
]
```

`_normalize_change()` принудительно:
* приводит `source` к `mixed`, если evidence содержит и visual, и
  non-visual origin;
* поднимает `source=text` до визуального, если evidence — только visual
  (image_enrichment / scheme_analysis / image_diff_index).

Старые changes без `evidence[]` не получают пустого массива — поле
просто отсутствует.

## Метрики

### Per-side (в `<side>_image_descriptions.json` под `enrichment_metrics`)
* `qwen_blocks_by_type` — count по block_type;
* `usable_for_diff_true` / `usable_for_diff_false`;
* `total_anchor_labels`, `total_anchor_ratings`, `total_anchor_connections`;
* `blocks_with_diff_anchors`;
* `hallucination_warnings_count`, `continuation_warnings_count`;
* `done_with_salvage_count`, `avg_confidence`.

### Session-level (aggregate_job_progress.diagnostics)
Эти поля автоматически суммируются из `_read_side_descriptions_metrics`:
* `qwen_blocks_by_type`, `usable_for_diff_true/false`, `total_anchor_*`,
  `hallucination_warnings_count`, `continuation_warnings_count`.

### Unified findings (unified_findings.json → summary)
* `visual_evidence_changes` — count changes с source in {image_enrichment,
  scheme_analysis, mixed} ИЛИ visual origin в evidence[];
* `mixed_evidence_changes`;
* `image_enrichment_evidence_changes`, `scheme_analysis_evidence_changes`,
  `image_diff_index_evidence_changes` — per-origin счётчики.

Важно: visual_evidence_changes считает И через `source`, И через `evidence[]`,
чтобы не пропустить случаи, когда Opus поставил source=text, а evidence
содержит image_enrichment.

## Замечание для пояснительной записки (ПЗ)

ПЗ обычно содержит мало схем и много обычного текста (списки листов,
наименования организаций, нормы). Это нормально, что для ПЗ изображения
дают меньше diff-якорей, чем для графической части — у листа ГЧ типичный
выход 5-15 anchors, у листа ПЗ может быть 0-2. v5 prompt всё равно
обязывает Qwen сохранять буквальные labels, если они видны на штампе /
стандартизованной графике ПЗ.

`source=scheme_analysis` для текстового списка листов ГЧ в ПЗ —
ошибка attribution. Согласно новому prompt'у такие списки должны идти
с `source=text` или `source=table`, а `scheme_analysis` зарезервирован
для image-derived топологий.

## Operator runbook — v5

### Подозрение на массовые hallucination_suspected

Прочитать summary `enrichment_metrics`:
```bash
jq '.enrichment_metrics' \
   comparison/sessions/<sid>/pairs/<pid>/text_enrichment/<side>_image_descriptions.json
```

Если `hallucination_warnings_count` высок и `usable_for_diff_false` доминирует
для scheme/dense_scheme — задача оператора:

1. Убедиться, что primary qwen загружен с `context_length >= 16000`
   (`GET /api/comparison/graphic-llm-config`).
2. Очистить partials через `clear_failed_image_descriptions.py --include-salvaged`.
3. Запустить retry конкретной пары (force=true). Cache-key включает
   `v5_scheme_diff_anchors`, поэтому пересчёт не подхватит старый v4 кеш.

### Не сработали diff-якоря — Opus всё ещё ставит text

* Проверить, что enriched MD действительно содержит IMAGE_DIFF_INDEX
  (`grep -l IMAGE_DIFF_INDEX_START <side>_enriched.md`).
* Проверить, что block_type для нужных блоков — `scheme`/`dense_scheme`
  (`jq '.items[] | {order, block_type, usable_for_diff}'`).
* Если block_type получился `photo_or_general`, посмотреть `original_md_excerpt`:
  если в окружении нет ни одного strong-scheme-маркера и нет «однолинейн»,
  classifier правильно отказался от scheme. В этом случае стоит добавить
  явный маркер в исходный MD до Chandra OCR (редко доступно).

## Связанные файлы (v5)

* [backend/app/services/stage_comparison/md_image_enrichment.py](../backend/app/services/stage_comparison/md_image_enrichment.py)
* [backend/app/services/stage_comparison/enriched_comparison.py](../backend/app/services/stage_comparison/enriched_comparison.py)
* [backend/app/services/stage_comparison/unified_findings.py](../backend/app/services/stage_comparison/unified_findings.py)

# v5 production tuning after validation report (2026-05-27)

После validation report на проблемной паре `ba413a93c5754f6c / pf06effb7`
(ИОС1.1) исходные значения `BLOCK_TYPE_CONFIG` оказались непригодны для
production:

- `dense_scheme: render=3000, image=2800, max_tokens=10000, cont=4` →
  один блок занимал **245 s** на реальном Qwen 35B deployment;
- worst-case generation 50 000 tokens/блок → 43-блочный force-regen ≈ 3 часа;
- модель доходила до catalog-fill: 40 labels `ЩА-1.1...ЩА-1.40`
  + цепочечные connections `ЩА-1.1 → ЩА-1.2 → ЩА-1.3 → ...`

## Что изменилось

### 1. Production-safe `BLOCK_TYPE_CONFIG` defaults

```text
photo_or_general:  render=1200  image=1100  max_tokens=cfg   cont=cfg
scheme:            render=1800  image=1600  max_tokens=3500  cont=1
dense_scheme:      render=2000  image=1800  max_tokens=4000  cont=1
table_legend:      render=1800  image=1600  max_tokens=3500  cont=1
plan:              render=1800  image=1600  max_tokens=3500  cont=1
stamp:             render=1600  image=1400  max_tokens=2500  cont=0
```

Worst-case generation per block = `max_tokens × (cont+1)`:
- scheme: 7 000 tokens
- dense_scheme: 8 000 tokens
- остальные ≤ 7 000

Тест [test_block_type_config_worst_case_is_bounded](../tests/test_stage_comparison_md_enrichment.py)
гарантирует, что worst-case ≤ 10 000 для обеих схем.

### 2. Env-override для оперативного тюнинга

Каждый параметр читается из env, с fallback на default:

| Env var | Default | Назначение |
|---|---|---|
| `STAGE_COMPARISON_DENSE_SCHEME_RENDER_LONG_SIDE` | 2000 | PNG render |
| `STAGE_COMPARISON_DENSE_SCHEME_IMAGE_LONG_SIDE` | 1800 | Qwen input |
| `STAGE_COMPARISON_DENSE_SCHEME_MAX_TOKENS` | 4000 | per-call max |
| `STAGE_COMPARISON_DENSE_SCHEME_MAX_CONTINUATIONS` | 1 | continuation cap |
| `STAGE_COMPARISON_SCHEME_*` | — | то же для обычных схем |
| `STAGE_COMPARISON_TABLE_*` / `_PLAN_*` / `_STAMP_*` / `_GENERAL_*` | — | per-type tuning |

### 3. v5 prompt теперь bounded + anti-extrapolation + anti-chain

`QWEN_SCHEME_DIFF_ANCHORS_PROMPT` обновлён:

* **Жёсткие лимиты массивов** прямо в prompt'е:
  `labels ≤ 25`, `ratings ≤ 20`, `connections ≤ 15`, `uncertain_text ≤ 10`.
* **Анти-экстраполяция рядов** — модель прямо запрещает достраивать
  «ЩА-1.4, ЩА-1.5, ..., ЩА-1.40», если видны только 1-3 первых номера.
  Вместо этого добавить ОДИН элемент в `uncertain_text` со списком
  читаемых номеров в `alternatives`.
* **Анти-цепочка** — запрещены последовательные `connections` между
  членами одной серии (`ЩА-1.1 → ЩА-1.2 → ЩА-1.3`), если связь не нарисована
  явной линией. По умолчанию квартирные щитки — параллельные потребители
  (звезда от ввода), а не цепочка.
* **Анти-дубликатные comments** — если 10+ labels, comment либо пустой,
  либо индивидуальный. Запрещён повторяющийся «читается в левой части
  схемы» на десятки элементов.

### 4. Subindex hallucination detection

`parse_anchor_series_key()` (новая helper-функция) разбирает буквальный
текст на `(series_key, seq_num)`:

```text
"ЩР-1"     → ("ЩР", 1)
"ЩА-1.5"   → ("ЩА-1", 5)    # subindex
"ЩО-1-12"  → ("ЩО-1", 12)   # dash subindex
"QF-3.7"   → ("QF-3", 7)
"QF12"     → ("QF", 12)
"ВРУ-2"    → ("ВРУ", 2)
```

`_detect_artificial_sequences` теперь учитывает subindex format —
`ЩА-1.1 ... ЩА-1.8` корректно даёт `repeated_pattern_detected` (раньше
все ЩА-1.N парсились как `series=ЩА, num=1` → одна серия, нет ряда).

### 5. Serial chain connection detector

`_detect_serial_chain_connections(connections)` — новый детектор.
Возвращает `series_key` цепочек, где модель проставила
`A.N → A.N+1` ≥5 шагов подряд внутри одной серии.

Не флагает:
* звезду `ВРУ-2 → ЩА-1.1`, `ВРУ-2 → ЩА-1.2` (источник один, потребители разные);
* короткие цепочки (<5);
* связи без `relation` "питает/feeds/connected_to".

### 6. Композитная эскалация до `hallucination_suspected`

Раньше: любой `repeated_pattern_detected` → `hallucination_suspected` →
`usable_for_diff=False`. После v5 tuning: repeated alone — это «подозрительно»,
но **не фатально** (в МКД 6-8 квартирных щитов могут быть реальными).

Эскалация в `hallucination_suspected` требует ≥2 совпадающих сигналов:

| Сигнал | Вес |
|---|---|
| `repeated_pattern_detected` | 1 |
| `serial_chain_connection_detected` | **2** (самый сильный) |
| `identical_comments_detected` | 1 |
| `truncated_output` | 1 |
| `generic_rating_list_without_labels` | 1 |

Сумма ≥ 2 → `hallucination_suspected` + `usable_for_diff=False`.

Это правильно ловит исходный failure case (`ЩА-1.N × 40` + chain + identical
comments → 4 сигнала → hallucination), но не «убивает» легитимные ряды
из 6-8 квартирных щитов.

### Heuristic fix 2026-05-28 (controlled validation report)

После прогона post-tuning smoke benchmark на проблемной паре ИОС1.1
оказалось: 3/3 dense_scheme блока получали `usable_for_diff=False`
из-за двух false-positive путей:

1. `truncated_output` был в `serious` set → любой блок, упёршийся в
   `max_tokens=4000` (что под cap-bound prompt'ом происходит штатно),
   автоматически становился `usable_for_diff=False` независимо от
   качества якорей.
2. Композитный сигнал `len(labels) ≥ 23` срабатывал на legitimate
   cap-fill: prompt ограничивает labels ≤25, поэтому 23-25 — это
   ОЖИДАЕМОЕ поведение, не аномалия. Совместно с `truncated_output`
   эти два сигнала давали 2 балла → `hallucination_suspected` →
   `usable_for_diff=False` даже на полностью валидном выводе.

Fix (минимальный):
* `truncated_output` исключён из `serious` set. Truncated alone с
  валидными anchors остаётся `usable_for_diff=True`. Эскалация в
  hallucination возможна только при суперпозиции с repeated/chain/
  identical_comments/generic_ratings.
* `len(labels) ≥ 23` исключён из composite signals. Под prompt
  cap=25 этот сигнал стал dead-code.

После fix:
* `usable_for_diff` блоки с легитимными mixed-series labels
  (ВРУ2-ПП1-N / ВРУ-1/ВРУ-2/ШС-N), которые штатно truncates на
  cap-fill, теперь сохраняют usable_for_diff=True (smoke 2 right o4
  — 28 различных labels, truncated, usable=True ✓).
* Catalog-fill кейсы (ЩА-1.1...1.40 + identical comments + truncated)
  по-прежнему ловятся через composite scoring (3+ сигналов).

## Benchmark script

```bash
python backend/scripts/benchmark_v5_image_enrichment.py \
    --session <sid> --pair <pid> --side right \
    --blocks <block_id> --force [--allow-partial] [--write-patch PATH]
```

Запускает v5 enrichment на отдельных блоках **без модификации production
state**. Печатает per-block:
- `block_type`, `prompt_version`, render/image sizes;
- `duration_sec`, `finish_reason`, `parse_error_detail`;
- counts labels/ratings/connections + samples;
- `usable_for_diff` + `warnings`.

Exit code 0 — все OK; 1 — strict failure (invalid_json / finish_reason=length),
кроме случая `--allow-partial`; 2 — argument error.

Использование вместо полной regeneration:
- быстро отвалидировать тюнинг конфига на конкретном блоке (~45-60s);
- сверить, что v5 prompt извлекает realistic anchors;
- сверить, что hallucination detector работает на актуальном выходе модели.

## Smoke benchmark — before/after на блоке 47FF-P4TD-MWA (page 25, right)

| Метрика | До tuning | После tuning |
|---|---|---|
| `render_target_long_side` | 3000 (proposed) / 1200 (smoke) | **2000** |
| `image_input_long_side` | 2800 (proposed) / 1100 (smoke) | **1800** |
| `max_tokens` | 10000 (proposed) / 2500 (smoke) | **4000** |
| `max_continuations` | 4 (proposed) / 0 (smoke) | **1** |
| `duration_sec` | **245.2 s** | **43.5 s** (5.6× faster) |
| `status` | `partial` (salvaged) | `partial` (salvaged) |
| `finish_reason` | `length` | `length` |
| labels count | 40 (catalog-fill) | **25** (cap-bound) |
| ratings count | 0 (truncated) | **20** — реальные 1000А/160А/100А/63А/40А |
| connections topology | **chain ЩА-1.1→1.2→1.3→...** ❌ | **star ВРУ-2 с.ш.1 → ЩА-1.N** ✅ |
| `usable_for_diff` | False (one warning) | False (5 warnings) |
| warnings | continuation_salvaged, truncated_output | repeated_pattern_detected, identical_comments_detected, truncated_output×2, continuation_salvaged, **hallucination_suspected** |

Ключевые улучшения:
1. **Star topology** в connections — anti-chain rule в prompt'е сработал.
2. **Ratings извлечены** — 1000А/160А/100А/63А/40А, реальные значения.
3. **5.6× быстрее** — production-safe budget вмещается в 1 минуту на блок.
4. **hallucination_suspected правильно ставится** через композитный счётчик.

## Что НЕ сделано в этом hotfix-PR

* `finish_reason=length` всё ещё происходит — JSON упирается в cap=4000.
  Это не фатально (anchors уже cap-bound на 25/20/15), но если нужен `done`
  без salvage — можно ещё ↑max_tokens до 5500-6000.
* Полный 43-block regen + Opus comparison **не запускался** — это
  следующий шаг после применения этого hotfix-PR. До запуска оператор
  должен сначала прогнать benchmark на 3-5 selected blocks и убедиться,
  что v5 даёт sane выход на разных типах схем.

## Когда запускать full validation

После применения этого PR:

1. Прогнать `benchmark_v5_image_enrichment.py` на 3-5 blocks разных типов
   (по 1 dense_scheme, 1 scheme, 1 table_legend, 1 stamp, 1 general).
2. Убедиться, что для каждого:
   - `duration_sec < 90s`;
   - `finish_reason != length` ИЛИ предсказуемо salvaged + `usable_for_diff=false`;
   - на dense_scheme — star topology, не chain.
3. Только после этого — запустить `POST /md-enrichment-jobs` с
   `force=true skip_done=false` на конкретной паре, дождаться completion,
   затем `POST /unified-analysis force_compare=true` для Opus comparison.
4. Сверить `unified_findings.summary.visual_evidence_changes` —
   должен стать > 0 на этой паре (был 0 в before-baseline).

## Controlled validation 2026-05-28 (ИОС1.1)

Полный end-to-end прогон на проблемной паре подтвердил, что v5 production
tuning + heuristic hotfix дают визуальные изменения в Opus.

**Session/pair:** `ba413a93c5754f6c` / `pf06effb7` (ИОС1.1: «АА_БЭ-03-ДС3-ИОС1.1.pdf»
старая стадия ↔ «АА-БЭ-03-ДС3-ИОС1.1.pdf» новая стадия).

**Модель/endpoint (verified):**
* `qwen/qwen3.6-35b-a3b` на LM Studio через ngrok с basic-auth;
* `loaded_context_length=16000`, в LM Studio ровно один инстанс (`chandra-ocr-2`
  не загружен и не конкурирует за VRAM);
* `response.model` в чат-completion подтверждает = `qwen/qwen3.6-35b-a3b`.

**Before / after (для пары pf06effb7, источник: `unified_findings.json`):**

| Метрика | Before | After |
|---|---|---|
| total_changes | 30 | 36 |
| source=text | 24 | 31 |
| source=stamp | 2 | 1 |
| source=table | 1 | 0 |
| source=scheme_analysis (misattributed textual sheet list) | 3 | **0** ✓ |
| source=image_enrichment | 0 | 0 |
| source=mixed | 0 | **4** ✓ |
| visual_evidence_changes (per /goal definition) | 0 | **4** ✓ |
| mixed_evidence_changes | 0 | **4** ✓ |
| changes c `evidence[]` array | 0 | **36** ✓ |
| evidence origin=`image_diff_index` (count) | 0 | **2** ✓ |
| evidence origin=`image_enrichment` (count) | 0 | **3** ✓ |

**Регенерация (`force=true skip_done=false`):** ~26 минут на оба сайда
(~13 мин/сторона; 17 left + 26 right = 43 блока; p50 duration 30-48s,
max 67s на dense_scheme). Все 39 scheme/dense_scheme блоков использовали
`v5_scheme_diff_anchors` prompt. IMAGE_DIFF_INDEX присутствует на обеих
сторонах с буквальными якорями (`ЩА-1.N`, `ВРУ-2 с.ш.1`, `ШС-N`, `РШ-N`,
номиналы `1000А/4х185/160А/100А/63А/40А`).

**Примеры mixed-changes с визуальным evidence:**

* `chg_vru2_scheme_overhaul` — Однолинейная схема ВРУ-2 полностью
  переработана. Opus подтянул labels ПРЯМО из IMAGE_DIFF_INDEX обеих
  сторон (`origin=image_diff_index side=left page=26` + `side=right page=26`)
  плюс text-evidence для подтверждения. Это golden case использования
  diff-якорей.
* `chg_apartment_cable_replacement` — Пересмотр сечений кабелей вводов
  квартир (1к 3×10 → 5×6/10/16/25/35 мм², ЩК-1...ЩК-Тл 14-40 кВт).
  `origin=text side=left page=12` + `origin=image_enrichment side=right page=30`.
* `chg_dsup_copper_busbar` — Шины ДСУП переведены со стальной полосы на
  медную (30×4). `origin=text side=right page=17` + `origin=image_enrichment
  side=right page=46`.
* `chg_appendix_a_tp_added` — Добавлено Приложение А с планом ТП и схемой
  вентиляции. `origin=text side=right page=4` + `origin=image_enrichment
  side=right page=48`.

**Важная заметка о ПЗ:** `source=image_enrichment` может оставаться 0
для пояснительной записки, и это нормально. ПЗ содержит преимущественно
текст + штампы; графика в ПЗ — это в основном таблицы листов и
организационная информация, а не схемы. Метрики успеха для ПЗ —
`visual_evidence_changes > 0` (visual-evidence через `evidence[]` с
visual origin) И `source=mixed > 0`, а не наличие чистого
`source=image_enrichment`. В этом валидационном прогоне:
- `visual_evidence_changes=4` ✓
- `mixed=4` ✓
- visual evidence origin'ы используются (`image_diff_index=2`,
  `image_enrichment=3`) ✓
- Текстовый список листов больше не идёт через `source=scheme_analysis` ✓

## Backend reload requirement after heuristic changes

`analyze_qwen_description_quality()` живёт в
[backend/app/services/stage_comparison/md_image_enrichment.py](../backend/app/services/stage_comparison/md_image_enrichment.py).
uvicorn без `--reload` загружает модуль в память один раз при старте —
последующие правки в `.py` файлах не подхватываются работающим процессом.
Это значимо для эвристики (`usable_for_diff`/`warnings`/`hallucination_suspected`),
потому что running md-enrichment job будет писать в JSON-файл состояние,
посчитанное СТАРОЙ эвристикой.

**Operator deployment runbook после изменения эвристики:**

1. Применить новые коммиты в working tree.
2. Запустить тесты: `python -m pytest tests/test_stage_comparison_md_enrichment.py -q`
3. Перезапустить backend:
   ```bash
   # Найти PID:
   ps aux | grep "uvicorn backend.app.main" | grep -v grep
   # Аккуратный restart (SIGTERM → новый запуск):
   pkill -f "uvicorn backend.app.main"
   uvicorn backend.app.main:app --host 0.0.0.0 --port 8081 --reload &
   ```
   Использование `--reload` рекомендуется для dev — uvicorn watchdog
   подхватит будущие правки автоматически. Для prod-окружения с
   supervisord/systemd используйте соответствующий restart.
4. Health check: `curl -s http://localhost:8081/api/stage-comparison/graphic-llm-config | jq .primary_context_ok`
5. Smoke check эвристики:
   ```bash
   python backend/scripts/benchmark_v5_image_enrichment.py \
       --session <sid> --pair <pid> --side right \
       --blocks <small_dense_scheme_block> --allow-partial
   ```
   В выходе ищем блок с `warnings: [..., truncated_output, ...]` (один
   сигнал без других composite-сигналов) — он должен иметь
   `usable_for_diff: True`. Если показывает `False`, backend всё ещё
   использует старую эвристику — повторить шаг 3.

**Если backend нельзя перезапустить немедленно** (например, в processing
крупная job), есть пост-процесс-скрипт `/tmp/v5-validation/post_process_heuristic.py`
(не комитится — это однократный recovery tool). Он перечитывает
`<side>_image_descriptions.json`, перезапускает
`analyze_qwen_description_quality()` с актуальной эвристикой из импорта
свежего модуля, и пересобирает enriched MD. Может использоваться как
hot-patch для уже законченных регенов, выполненных под старой эвристикой.

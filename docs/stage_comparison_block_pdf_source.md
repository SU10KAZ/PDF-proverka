# Stage Comparison — block-PDF source + text-layer + GRSH feeder extraction

**Дата:** 2026-06-05
**Статус:** controlled (оба контура по умолчанию **ВЫКЛЮЧЕНЫ**, по одному флагу)
**Модули:**
- [backend/app/services/stage_comparison/block_pdf_source.py](../backend/app/services/stage_comparison/block_pdf_source.py)
- [backend/app/services/stage_comparison/grsh_feeder_extraction.py](../backend/app/services/stage_comparison/grsh_feeder_extraction.py)
- интеграция: [md_image_enrichment.py](../backend/app/services/stage_comparison/md_image_enrichment.py), [blocks.py](../backend/app/services/stage_comparison/blocks.py)

## Зачем

Для image/imagine-блоков основным источником изображения был **page-crop**
(`store.render_block_crop` режет общий лист по bbox). Но у блока в `result.json`
есть **block-PDF фрагмент** (`crop_url` → R2, по факту есть всегда) и его
**векторный текст-слой** (`pdfplumber_text`). Block-PDF даёт чистый рендер без
поля листа, а текст-слой — точные буквальные значения (маркировки, кабели,
сечения, номиналы, токи), которые Qwen на плотных схемах теряет.

Базовый принцип:

```text
text layer / pdfplumber_text = точные буквальные значения (словарь, denominator, анти-галлюцинация)
Qwen                         = визуальная структура, связи, группировка
backend                      = validation / merge / recall / anti-hallucination
```

## Универсальный helper (доступен всем режимам)

`block_pdf_source.py` — без зависимости от Qwen/Opus, сетевой вызов только за
block-PDF (инжектируется для тестов; приватный URL целиком не логируется):

```text
resolve_block_pdf_source(block, *, cache_dir, http_get=None) -> BlockPdfSource
extract_block_text_layer(pdf_path, *, result_json_text=None) -> BlockTextLayer
render_block_pdf(pdf_path, *, long_side, out_path) -> RenderedBlock
build_ocr_literal_anchors(text_layer) -> {tokens, normalized, count}
validate_anchors_against_text_layer(qwen_labels, text_layer, *, expected_anchors=None)
build_block_source_diagnostics(src, text_layer, rendered, validation=None) -> dict
```

### Приоритет источника изображения

1. `crop_url` PDF из result.json — **основной путь** (HTTP 200 + `Content-Type: application/pdf`);
2. `image_file` PDF, если локально существует;
3. иначе `source="none"`, `fallback_used=True` → **page-crop fallback**.

### Текст-слой

1. `result_json_text` (`pdfplumber_text`) — самый дешёвый путь, текст уже извлечён upstream;
2. PyMuPDF `get_text("words")` по block-PDF (даёт words+bbox; pdfplumber в окружении не установлен — он optional fallback);
3. иначе `ok=False` → caller использует Chandra raw OCR.

Битый текст-слой (`garbled_ratio>0.5` или `chars<20`) → `usable=False`: render
block-PDF всё равно используется, но как словарь текст-слой не идёт.

`normalize_blocks_from_result_json` теперь сохраняет в `block["raw"]` поля
`crop_url` / `image_file` / `pdfplumber_text` / `coords_px` (раньше отбрасывались).

## Контур A — block-PDF для ВСЕХ image-блоков (flag-gated)

| Флаг | Default | Назначение |
|---|---|---|
| `STAGE_COMPARISON_BLOCK_PDF_SOURCE_ENABLED` | `false` | block-PDF render + текст-слой-словарь для всех image-блоков |

Когда ВКЛ, в `enrich_side` после резолва блока (`resolve_block_pdf_for_enrichment`):
- block-PDF рендерится и **подменяет** page-crop (`resolution.image_path`);
- текст-слой block-PDF идёт словарём в prompt:
  - GRSH-блок → через существующий `build_grsh_anchor_vocab_block` (но из текст-слоя, а не из `mb.text`);
  - прочие блоки → префикс `OCR_VOCAB: ...` к prompt'у;
- в item пишется диагностика `block_source` / `text_layer_stats` / `qwen_validation`.

Когда ВЫКЛ (default): поведение идентично прежнему (page-crop + `mb.text`).
Любая ошибка контура → fail-soft → page-crop. cache-key завязан на байты
картинки, поэтому смена источника рендера автоматически инвалидирует кеш.

## Универсальный слой — Graphic Structured Extraction (профили)

[graphic_profiles.py](../backend/app/services/stage_comparison/graphic_profiles.py)
превращает обработку image-блоков из «алгоритма под ГРЩ» в **универсальную основу
с pluggable-профилями**. GRSH — первый доказанный профиль
(`electrical_singleline / grsh`), не отдельный костыль.

```text
block crop_url PDF → text layer (OCR vocabulary) → high-res render
  → block type classifier → extraction profile → Qwen structured JSON
  → deterministic validation/merge (field_state, anti-hallucination, recall)
  → enriched MD
```

**Профили** (`PROFILE_REGISTRY`, у каждого свой набор `field_groups`):

| profile_id | дисциплины | production-ready |
|---|---|---|
| `electrical_singleline` | ЭОМ/ИОС1/ЭО (feeders/breakers/cables/loads/metering/compensation/earthing/connections) | ✅ subtype `grsh` |
| `hvac_scheme` | ОВ/ХС (systems/equipment/airflows/ducts/valves/…) | скелет |
| `water_supply_scheme` | ВК/ВПВ/ХПВ (zones/pumps/pipes/diameters/flows/…) | скелет |
| `low_voltage_scheme` | СС/СПС/СОУЭ/АСКУЭ (systems/devices/loops/…) | скелет |
| `structural_scheme` | КР/КЖ (elements/concrete_class/reinforcement/…) | скелет |
| `architectural_plan_or_facade` | АР (zones/materials/facade_systems/…) | скелет |
| `table_or_schedule` | таблицы (rows/columns/units/…) | скелет |
| `title_stamp_notes` | штампы (document_code/sheet_name/…) | скелет |
| `general` | fallback | — |

`classify_graphic_profile(block_type)`: `dense_grsh_singleline → (electrical_singleline, grsh)`;
`table_legend → table_or_schedule`; `stamp → title_stamp_notes`;
`plan → architectural_plan_or_facade`; прочие схемы/фото → `general` (fallback).
`profile_production_ready(profile, subtype)` сейчас `True` только для
`electrical_singleline/grsh` — остальные профили имеют готовую schema, но
**в production не запускаются** до своих extractor'ов и тестов.

**field_state** (универсально для всех профилей, `FieldState`):
`present | not_extracted | not_specified | visual_unverified | ocr_only |
requires_human_review`. `NON_REMOVAL_STATES` гарантирует, что
`not_extracted` / `visual_unverified` / `ocr_only` **не превращаются в «removed»**
на стадии сравнения. `build_electrical_singleline_structured(merged)` собирает
universal structured JSON из feeder-merge с field_state на каждом значимом поле.

**Флаг:** `STAGE_COMPARISON_GRAPHIC_STRUCTURED_EXTRACTION_ENABLED` (default OFF) —
главный включатель слоя. Backward-compat: исторический
`STAGE_COMPARISON_GRSH_FEEDER_EXTRACTION_ENABLED=true` тоже включает слой (GRSH —
профиль внутри него). `enrich_side` классифицирует блок в профиль и для
production-ready профиля запускает extractor; иначе — обычный single-shot
(fail-soft).

## Контур B — GRSH/ВРУ feeder extraction (flag-gated)

`grsh_feeder_extraction.py` — для плотных однолинейных ГРЩ/ВРУ single-shot Qwen
сжимает схему в бедный текст. Режим:

```text
block-PDF (crop_url) → текст-слой → high-res render
  → перекрывающиеся tiles (concurrency=1!)
  → per-tile Qwen feeder-JSON (tile-local OCR vocabulary, anti-extrapolation)
  → детерминированный merge feeders[] + recall vs текст-слой anchors
  → структурированная feeder-таблица (вход Opus)
```

| Флаг | Default | Назначение |
|---|---|---|
| `STAGE_COMPARISON_GRSH_FEEDER_EXTRACTION_ENABLED` | `false` | главный включатель режима |
| `STAGE_COMPARISON_GRSH_FEEDER_USE_BLOCK_PDF` | `true` | block-PDF как источник рендера внутри режима |
| `STAGE_COMPARISON_GRSH_FEEDER_TILE_CONCURRENCY` | `1` | **держать 1**: при параллели LM Studio делит ctx на слоты → `Context size exceeded` |
| `STAGE_COMPARISON_GRSH_FEEDER_RENDER_LONG_SIDE` | `7000` | high-res рендер block-PDF |
| `STAGE_COMPARISON_GRSH_FEEDER_TILE_LONG_SIDE` | `1600` | long_side tile'а в Qwen (live-рекомендация после benchmark 2026-06-05: 1600 = качество как 2000, стабильнее по времени; 2000 — override/debug) |
| `STAGE_COMPARISON_GRSH_FEEDER_MAX_TILES` | `16` | верхняя граница tiles/блок |
| `STAGE_COMPARISON_GRSH_FEEDER_MAX_TOKENS` | `9000` | max_tokens per tile |
| `STAGE_COMPARISON_GRSH_FEEDER_MIN_RECALL` | `0.80` | порог приёмки recall |

Qwen-вызов **инжектируется** (`describe_fn`) — сам модуль сетевых вызовов не
делает, тестируется замоканным. **Concurrency=1 обязательна** (ключевой
рантайм-урок эксперимента).

**Врезка в живой pipeline** (`md_image_enrichment.enrich_side`):
`_run_grsh_feeder_extraction_for_block` срабатывает, когда
`grsh_feeder_extraction_enabled()` И `block_type == dense_grsh_singleline`. Он
resolve'ит block-PDF, извлекает текст-слой с координатами (PyMuPDF words),
рендерит high-res, гоняет `extract_feeders_for_block` с реальным Qwen
(`_describe_image_once`), мёржит и кладёт пофидерную таблицу
(`render_feeder_table_md`) в `item["description"]["grsh_feeder_table"]` —
`_format_qwen_description_md` рендерит её в enriched MD секцией `GRSH_FEEDERS`
(до summary). Отдельная prompt-версия `v9_grsh_feeder_tiled` → свой cache-key.
Любая ошибка контура → `None` → fail-soft на single-shot v7 GRSH. Диагностика
(`grsh_designation_recall`, `grsh_consumer_recall`, `n_tiles`, `block_source`,
`text_layer_source`, `grsh_rejected_artificial_series`) пишется в
`item["grsh_feeder_extraction"]`. Tile-вызов идёт `stream=True` (см. ниже).

### Runtime fast-profile + streaming (benchmark 2026-06-05)

«Медленный» режим оказался не в размере tile, а в runtime-профиле LM Studio:
Qwen генерировал ~24 tok/s, тяжёлый tile шёл ~290s и падал в ngrok `ReadError`.
После clean reload с fast-profile стало ~230 tok/s, тот же tile — 29.3s.

- **Fast-profile загрузки.** `ensure_lmstudio_model_loaded` / `_load_model` на
  СВЕЖЕЙ загрузке поднимают primary с `context_length=16000`,
  `flash_attention=true`, `offload_kv_cache_to_gpu=true`, `parallel=1`.
  `fast_profile_ok` / reload ключуются на throughput-критичных факторах
  (ctx + flash + offload): именно их clean reload поднял throughput с ~24 до
  ~230 tok/s. **`parallel` НЕ форсит reload** — тот же benchmark наблюдал
  быстрый профиль при `parallel=4`, поэтому `parallel=1` — лишь консервативный
  дефолт свежей загрузки + диагностика (`parallel` / `parallel_ok`), а не повод
  перезагружать уже-быстрый инстанс. Если модель загружена с явным
  `flash_attention=false` / `offload_kv_cache_to_gpu=false` / малым ctx —
  `ensure_lmstudio_model_loaded` выгружает её и грузит заново (protected
  `chandra-ocr-2` не трогается → `fast_profile_mismatch_protected`).
  Неизвестные поля (legacy LM Studio не эхоит config) reload НЕ триггерят.
  Env-переопределение: `STAGE_COMPARISON_GRAPHIC_LLM_LOAD_FLASH_ATTENTION` /
  `_LOAD_OFFLOAD_KV_CACHE_TO_GPU` / `_LOAD_PARALLEL`.
- **Streaming.** `STAGE_COMPARISON_GRAPHIC_LLM_STREAM=true` (default) — все
  graphic-LLM вызовы (GRSH tiles, dense image) гонят SSE-дельты сразу, поэтому
  длинный ответ не простаивает до read-timeout. Частичный обрыв стрима →
  partial content → salvage; сервер без SSE → fail-soft на non-streaming.
- **Диагностика** в `GET /api/stage-comparison/graphic-llm-config`:
  `context_length`, `flash_attention`, `offload_kv_cache_to_gpu`, `parallel`,
  `ctx_ok`, `fast_profile_ok` (live snapshot primary) + desired
  `load_*` / `stream_enabled`.
- **Tile-рекомендация:** `STAGE_COMPARISON_GRSH_FEEDER_TILE_LONG_SIDE=1600`
  (качество как 2000, стабильнее по времени), `MAX_TOKENS=9000`,
  `RENDER_LONG_SIDE=7000`, `MIN_RECALL=0.80`, `TILE_CONCURRENCY=1`.

### Анти-галлюцинация / recall

- метка Qwen есть в текст-слое → `verified`;
- нет → `visual_unverified` (НЕ удаляется);
- искусственный ряд (`A-1..A-N`, номеров нет в слое) → `rejected_artificial_series`;
- значение слоя, которого Qwen не извлёк → `missing_text_layer_anchor` (сигнал
  `not_extracted`, НЕ «удалено»);
- `designation_recall` / `consumer_recall` считаются против текст-слоя как denominator.

## Controlled validation (без live Qwen)

Логика merge/recall портирована из проверенного эксперимента
`comparison/qwen_experiments/grsh_pdf_block_feeder_extraction_20260605_142501`.
Прогон production `merge_tile_feeders` на УЖЕ извлечённых tile-JSON эксперимента
воспроизводит метрики (без новых Qwen-вызовов):

| | designation_recall | consumer_recall | connections | artificial series |
|---|---|---|---|---|
| OLD (стр.52) | **0.933** | 1.0 | 25 | 0 |
| NEW (стр.21) | **1.0** | 0.889 | 43 | 0 |

## Деплой

uvicorn без `--reload` держит модуль в памяти — после правок рестарт backend.
Оба контура default OFF → выкатка идентична прежнему поведению, включение
осознанное (контролируемый rollout).

## Тесты (mocked, без live Qwen/Opus)

- [tests/test_stage_comparison_block_pdf_source.py](../tests/test_stage_comparison_block_pdf_source.py) — resolve/extract/render, приоритет crop_url>image_file>fallback, garbled→not usable, anti-hallucination (visual_unverified / missing_anchor), preserve raw-полей, flag OFF;
- [tests/test_stage_comparison_grsh_feeder_extraction.py](../tests/test_stage_comparison_grsh_feeder_extraction.py) — tile-local vocabulary, tiled extraction с замоканным Qwen, merge+recall, rejected_artificial_series, флаги OFF.

## Связанные файлы

- [block_pdf_source.py](../backend/app/services/stage_comparison/block_pdf_source.py)
- [grsh_feeder_extraction.py](../backend/app/services/stage_comparison/grsh_feeder_extraction.py)
- [md_image_enrichment.py](../backend/app/services/stage_comparison/md_image_enrichment.py) — `resolve_block_pdf_for_enrichment`, gated hook в `enrich_side`
- [blocks.py](../backend/app/services/stage_comparison/blocks.py) — `_block_raw_passthrough`
- эксперимент-обоснование: `comparison/qwen_experiments/grsh_pdf_block_feeder_extraction_20260605_142501/final/final_report.md`

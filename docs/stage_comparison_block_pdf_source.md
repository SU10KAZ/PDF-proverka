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
| `STAGE_COMPARISON_GRSH_FEEDER_TILE_LONG_SIDE` | `2000` | long_side tile'а в Qwen |
| `STAGE_COMPARISON_GRSH_FEEDER_MAX_TILES` | `16` | верхняя граница tiles/блок |
| `STAGE_COMPARISON_GRSH_FEEDER_MAX_TOKENS` | `9000` | max_tokens per tile |
| `STAGE_COMPARISON_GRSH_FEEDER_MIN_RECALL` | `0.80` | порог приёмки recall |

Qwen-вызов **инжектируется** (`describe_fn`) — модуль сетевых вызовов не делает,
тестируется замоканным; live Qwen зовётся только при включённом флаге из живого
pipeline. **Concurrency=1 обязательна** (ключевой рантайм-урок эксперимента).

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

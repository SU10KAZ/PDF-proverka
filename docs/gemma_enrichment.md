# Gemma OCR Enrichment

**Дата обновления:** 2026-05-18

Gemma enrichment is a mandatory OCR-audit stage between crop/document graph and
Stage 01 text analysis.

```text
Markdown PDF representation
→ Gemma base OCR enrichment, 100 DPI, GEMMA_BASE_CONTEXT_LENGTH
→ optional Gemma high-detail retry, 300 DPI, GEMMA_HIGH_DETAIL_CONTEXT_LENGTH
→ Stage 01 text analysis
→ Stage 02 findings_only_gemma_pair + GPT-5.4 using 100 DPI Stage 02 crops
→ merge/review/norms/final report
```

Два прохода используют **разный context_length**: base 100 DPI — небольшой и
дешёвый, high-detail 300 DPI — больше, чтобы вместить плотный image-вход.

## Crop Sources Of Truth

- Gemma base pass: `_output/blocks_gemma_100/`
- Gemma high-detail retry: `_output/blocks_gemma_300/`
- Stage 02 image input: `_output/blocks_stage02_100/`

Production must not treat generic `_output/blocks/` as the source of truth for
Gemma or Stage 02.

## Crop Policies

```json
{
  "gemma_base_crop_policy": {
    "profile": "gemma_100_base",
    "dpi": 100,
    "min_long_side": 800,
    "compact": false,
    "skip_small": false
  },
  "gemma_high_detail_crop_policy": {
    "profile": "gemma_300_high_detail",
    "dpi": 300,
    "min_long_side": 800,
    "compact": false,
    "skip_small": false
  },
  "stage02_crop_policy": {
    "profile": "stage02_100",
    "dpi": 100,
    "min_long_side": 800,
    "compact": false
  }
}
```

## High-Detail Retry

Base Gemma runs on all image blocks at 100 DPI. High-detail 300 DPI is used only
for targeted retry candidates:

- base result is empty or too short;
- base result contains low-readability markers such as `unreadable`,
  `нечитаемо`, `неразборчиво`, `blurred`, `cannot read`, `text too small`,
  `partially readable`;
- block metadata suggests a table/spec/schedule/statement or dense OCR-heavy
  small text;
- base returned only a reasoning tail / partial OCR salvage;
- base pass упал с `Context size has been exceeded` на всех scale-tier'ах
  (reason `base_context_overflow`) — high-detail pass под
  `GEMMA_HIGH_DETAIL_CONTEXT_LENGTH` обычно пропускает такие блоки.

Safety limits before sending a 300 DPI crop to Gemma:

- `size_kb_300 <= 300`
- `long_side_300 <= 3500`
- `estimated_image_tokens <= 3500`

If a candidate exceeds safety limits, it is recorded as
`high_detail_status = "skipped_large_block"` and the pipeline falls back to base
100 DPI if that base enrichment exists.

## Reasoning Fallback

Gemma runner reads `content` first. If `content` is empty, it falls back to
`reasoning_content` and extracts the best final chunk from:

- `Final check`
- `final answer`
- `ЧИТАЕМО`
- the last structured bullet/list tail
- otherwise the last ~1000 chars of reasoning

If `finish_reason = length` but the reasoning tail still contains useful OCR, the
result is stored as `partial_ok` instead of a full failure.

Per-block metadata now records:

- `response_source = content | reasoning_content | reasoning_tail`
- `finish_reason = stop | length | error`

## Summary Contract

`_output/gemma_enrichment_summary.json` is the source of truth for skip/resume:

- `schema_version = 2`
- `base_profile`, `base_crop_policy`, `base_blocks_index_hash`
- `high_detail_profile`, `high_detail_crop_policy`, `high_detail_blocks_index_hash`
- `md_hash_before_enrichment`
- `blocks_total`, `base_blocks_ok`
- `high_detail_candidates`, `high_detail_ok`, `high_detail_skipped_large`
- `blocks_failed`, `coverage_ratio`
- `uncovered_block_ids`, `large_block_skipped_ids`
- per-block `base_status`, `high_detail_status`, `final_profile`, `coverage_status`

Adaptive reload diagnostics (новые поля, `schema_version=2` не меняется):

- `adaptive_reload_enabled` — bool, был ли pipeline reload включён в этом запуске
- `base_context_length` — запрошенный ctx для base pass
- `high_detail_context_length` — запрошенный ctx для high-detail pass
- `loaded_base_context_length` — реально загруженный ctx (или null если skipped/preflight)
- `loaded_high_detail_context_length` — то же для high-detail
- `model_reload_events` — список событий reload/preflight: `{phase, requested_context_length, loaded_context_length, identifier, instances_after, ok, error?, skipped?}`
- `context_guard_status` — `ok` / `preflight_ok` / `preflight_warning` / `skipped`

The Markdown marker is human-readable only. Skip/resume must be based on the
summary hashes and final block decisions.

## Gate / Resume Semantics

Gemma stage is considered ready when:

- base 100 DPI pass has been executed;
- summary metadata and hashes are valid;
- every image block has a final decision:
  `ok`, `partial`, `high_detail_skipped_large_block`, or
  `missing_gemma_enrichment`.

The gate does not require 300 DPI for every block.

## Adaptive Reload (Two-Pass Context Switch)

Pipeline сам управляет единственным активным инстансом Gemma в LM Studio, когда
`GEMMA_ADAPTIVE_RELOAD_ENABLED=true`. Между base и high-detail проходами
предыдущий инстанс **обязательно выгружается** и загружается новый с нужным
`context_length`.

Контракт перед каждым проходом:

1. снимок `list_loaded` — узнать, что уже загружено;
2. `unload_all_for(model)` — снять все инстансы Gemma этой модели;
3. `load_model(model, context_length=…)` — загрузить ровно один инстанс;
4. повторный `list_loaded` — verify ровно один инстанс с
   `loaded_context_length >= requested`.

Любое нарушение (после reload остаётся 2+ инстанса, loaded_context_length меньше
запрошенного) поднимает `GemmaAdaptiveReloadError` и завершает stage с понятной
ошибкой. Не «продолжаем как-нибудь».

**Skip/resume.** Если уже загружен ровно один инстанс модели и его
`loaded_context_length >= requested`, reload пропускается — это особенно важно
для retry-flow, где base уже отстрелял и нужно только переключиться на
high-detail.

**Если adaptive reload выключен** (`GEMMA_ADAPTIVE_RELOAD_ENABLED=false`,
default): pipeline не делает load/unload и доверяет оператору. Перед каждым
проходом всё равно выполняется **preflight**: проверяется, что загружен
подходящий инстанс. Если нет — warning в `gemma_enrichment_summary.json`
(`context_guard_status=preflight_warning`), но stage продолжает работу.

**Context overflow на base pass.** Если блок упал на всех scale-tier'ах с
ошибкой `Context size has been exceeded` (даже после уменьшения картинки), он
помечается флагом `context_overflow=True` и добавляется в кандидаты на
high-detail retry с reason `base_context_overflow`. На high-detail pass'е под
`GEMMA_HIGH_DETAIL_CONTEXT_LENGTH` блок повторяется и обычно проходит.

### Env-переменные

| Переменная | Default | Назначение |
|---|---|---|
| `GEMMA_ADAPTIVE_RELOAD_ENABLED` | `false` | Включить runtime управление инстансом |
| `GEMMA_BASE_CONTEXT_LENGTH` | `8192` | context_length для base 100 DPI прохода |
| `GEMMA_HIGH_DETAIL_CONTEXT_LENGTH` | `16000` | context_length для high-detail 300 DPI |

Раньше base default был `4000`. На 100 DPI с page_text это нестабильно: блоки
800×500 px регулярно падают с `Context size has been exceeded`. 8192 — рабочий
минимум.

## Runtime LM Studio Policy (when adaptive reload is OFF)

Если `GEMMA_ADAPTIVE_RELOAD_ENABLED=false`, runtime не меняет конфигурацию
LM Studio во время stage:

- no automatic `load` / `unload` / `reload`
- no runtime `context_length` changes
- no runtime `parallel` changes
- no runtime reasoning toggles

В этом режиме оператор обязан держать **ровно один** инстанс Gemma в LM Studio с
`context_length >= GEMMA_HIGH_DETAIL_CONTEXT_LENGTH`. Два одновременно
загруженных инстанса (например, 4k и 16k) приводят к тому, что часть запросов
роутится в маленький ctx и падает.

## Recommended LM Studio Settings

Do not try to disable reasoning programmatically; treat operator-side LM Studio
configuration as the real control surface.

Recommended operator settings:

- держать **только один** активный инстанс Gemma в LM Studio;
- `context_length: 16000` или больше, если оператор управляет вручную;
- если включён adaptive reload — LM Studio может быть пустой, pipeline сам
  загрузит модель;
- `parallel: 4` for the base 100 DPI pass;
- `parallel: 1` preferred for targeted 300 DPI retry;
- if backend cannot switch parallelism per stage, keep the 300 DPI safety
  thresholds strict.

Do not change `chandra-ocr-2` or unrelated loaded models without an explicit
reason.

## Post-Queue Cleanup

After the whole queue becomes idle, AuditManager may run best-effort cleanup:

- wait a grace period before unloading
- re-check that prepare/audit/retry/resume queues are still idle
- unload only Gemma models from the allowlist
- never unload denylist models such as `chandra-ocr-2`
- unload failures must stay warnings only and must not fail the audit

## Schema v1 Migration

Older projects with missing `schema_version` or `schema_version != 2` are no
longer valid for skip/resume. They must rerun `gemma_enrichment`.

Historical `03_findings.json`, reports, and other completed artifacts may stay
on disk, but they are treated as legacy outputs until Gemma schema v2 is rebuilt.
Resume/status detection should expose this as `migration_required` rather than
showing the project as fully compatible with the new production architecture.

After rerun, the project must have:

- `_output/blocks_gemma_100/index.json`
- `_output/gemma_enrichment_summary.json` with `schema_version = 2`
- `_output/blocks_gemma_300/index.json` only if high-detail candidates exist
- `_output/blocks_stage02_100/index.json` before Stage 02 starts

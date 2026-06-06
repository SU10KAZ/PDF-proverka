# Stage Comparison — Qwen problem-block tiled high-res retry

**Дата:** 2026-05-31
**Статус:** production-возможность, по умолчанию **ВЫКЛЮЧЕНА** (один флаг)
**Модуль:** [backend/app/services/stage_comparison/problem_block_retry.py](../backend/app/services/stage_comparison/problem_block_retry.py)
**Точка интеграции:** `enrich_side` в [md_image_enrichment.py](../backend/app/services/stage_comparison/md_image_enrichment.py)

## Зачем это нужно

На этапе «Сравнение стадий → 1. Загрузка документации» графические блоки
описываются локальным Qwen (`qwen/qwen3.6-35b-a3b` через LM Studio + ngrok).
Часть проблемных блоков (плотные однолинейные схемы, широкие чертежи) проваливалась.

Изолированный эксперимент (`experiments/qwen_problem_block_recognition/`) показал
ключевую причину: **сбой часто не в том, что Qwen не понимает блок**, а в том, что
один большой crop заставляет модель генерировать слишком долго и запрос упирается
в ngrok read-timeout (~300 с) → `ReadError` / `http_error` / обрыв JSON. Лучший
рабочий метод — **tiled high-res retry**: большой блок рендерится в высоком
разрешении, режется на перекрывающиеся tiles, каждый tile обрабатывается отдельным
быстрым запросом, результаты объединяются. Tiled победил в 7/8 проблемных блоков и
восстановил 100% блоков, где baseline падал (timeout / http_error / invalid_json /
нулевой результат).

## Почему PDF URL / PDF Base64 НЕ внедряются

Текущий provider (LM Studio OpenAI-совместимый shim для Qwen-VL) принимает
**только base64-картинки** в `image_url`. Проверено вживую:

| вход | результат |
|---|---|
| `data:image/png;base64` | ✅ работает |
| `data:application/pdf;base64` | ❌ HTTP 400 `'url' field must be a base64 encoded image` |
| PDF по URL | ❌ HTTP 400 |
| любой удалённый URL (даже картинки) | ❌ HTTP 400 (shim не ходит по ссылкам) |

Поэтому «использовать PDF» = рендерить его **самим** в высоком разрешении и слать
как PNG (что и делает tiled retry). PDF в модель не отправляется.

## Когда запускается retry

После основного (baseline) прогона блока, **только для проблемных блоков**.
Хорошие блоки обрабатываются как раньше (fast-path не меняется). Признаки
проблемного блока (`should_retry_problem_block`):

- `timeout` / `ReadError` / `http_error`;
- `invalid_json` / обрыв JSON (`finish_reason=length`);
- `usable_for_diff=false`;
- пустой / почти пустой набор фактов (≤2) при наличии графики;
- output только из общих фраз («чертёж/схема» без фактов);
- confidence ниже `MIN_CONFIDENCE`;
- маркеры в тексте модели: «текст неразборчив», «не удалось распознать», и т.п.

Каждое условие можно выключить отдельным флагом (см. ниже).

## Как работает

```text
baseline crop → Qwen
  → результат хороший?  → да: сохранить как раньше, retry НЕ запускается
                        → нет (problem block):
     render_crop(block_id, target_long_side=RENDER_LONG_SIDE)   # high-res
       → split_image_into_tiles(width,height,overlap,max_tiles) # перекрытие
         → каждый tile → Qwen (строгий tile-prompt, per-tile timeout)
           → merge_tiled_qwen_results (union + dedup + provenance)
             → улучшил baseline? → да: подменить description, method_used=tiled_retry
                                  → нет: оставить baseline + diagnostics
```

Гарантии:

- **baseline никогда не теряется** — сохраняется в `item["baseline_result"]`;
- **retry никогда не валит pipeline** — любая ошибка → baseline + diagnostics;
- **fail-soft по tile** — упавший tile пропускается, остальные мёржатся;
- **лимит времени на блок** (`MAX_TOTAL_SEC_PER_BLOCK`) и **лимит tiles**
  (`MAX_TILES`, при превышении изображение даунскейлится под бюджет);
- **слишком маленькие блоки** (< `MIN_LONG_SIDE`) в tiling не уходят (одиночный
  быстрый вызов и так бы прошёл) — `retry_skipped:too_small_for_tiling`.

Merge: объединяет `visible_text / labels / materials / numeric_parameters /
elevations / dimensions / equipment / connections / tables`, удаляет дубли по
нормализованной сигнатуре, проставляет `_tiles`/`_confirmations` (provenance),
поднимает confidence при подтверждении факта несколькими tiles, не превращает
uncertain в confirmed.

## Env-флаги

Все в `.env`. Default — **OFF** / безопасные значения.

| Переменная | Default | Назначение |
|---|---|---|
| `STAGE_COMPARISON_QWEN_PROBLEM_BLOCK_RETRY_ENABLED` | `false` | **главный включатель** |
| `STAGE_COMPARISON_QWEN_PROBLEM_BLOCK_RETRY_MODE` | `tiled` | режим retry |
| `STAGE_COMPARISON_QWEN_PROBLEM_BLOCK_RETRY_AFTER_MAIN` | `true` | запускать после baseline |
| `STAGE_COMPARISON_QWEN_PROBLEM_BLOCK_MAX_ATTEMPTS` | `1` | попыток retry на блок |
| `STAGE_COMPARISON_QWEN_PROBLEM_BLOCK_MIN_CONFIDENCE` | `0.45` | порог low-confidence |
| `STAGE_COMPARISON_QWEN_PROBLEM_BLOCK_RETRY_ON_TIMEOUT` | `true` | retry на timeout |
| `STAGE_COMPARISON_QWEN_PROBLEM_BLOCK_RETRY_ON_HTTP_ERROR` | `true` | retry на http_error/ReadError |
| `STAGE_COMPARISON_QWEN_PROBLEM_BLOCK_RETRY_ON_INVALID_JSON` | `true` | retry на invalid_json |
| `STAGE_COMPARISON_QWEN_PROBLEM_BLOCK_RETRY_ON_NOT_USABLE` | `true` | retry на usable_for_diff=false |
| `STAGE_COMPARISON_QWEN_TILE_RENDER_DPI` | `600` | информационный DPI (рендер ведётся по long_side) |
| `STAGE_COMPARISON_QWEN_TILE_RENDER_LONG_SIDE` | `4000` | целевая длинная сторона high-res рендера (px) |
| `STAGE_COMPARISON_QWEN_TILE_WIDTH` | `1600` | ширина tile (px) |
| `STAGE_COMPARISON_QWEN_TILE_HEIGHT` | `1600` | высота tile (px) |
| `STAGE_COMPARISON_QWEN_TILE_OVERLAP` | `200` | перекрытие tiles (px) |
| `STAGE_COMPARISON_QWEN_TILE_MAX_TILES` | `24` | макс. число tiles (иначе downscale) |
| `STAGE_COMPARISON_QWEN_TILE_MIN_LONG_SIDE` | `1400` | ниже — tiling пропускается |
| `STAGE_COMPARISON_QWEN_TILE_PROACTIVE_FOR_DENSE` | `false` | проактивный тайлинг `scheme`/`dense_scheme` ДАЖЕ при «ок» baseline (мелочь ТТ/сечения/0,5S); требует `..._RETRY_ENABLED=true` |
| `STAGE_COMPARISON_QWEN_TILE_TIMEOUT_SEC` | `300` | per-tile timeout |
| `STAGE_COMPARISON_QWEN_TILE_MAX_TOTAL_SEC_PER_BLOCK` | `1200` | бюджет времени на блок |
| `STAGE_COMPARISON_QWEN_TILE_CACHE_ENABLED` | `true` | кеш tiled-результата |

## Как включить

```env
STAGE_COMPARISON_QWEN_PROBLEM_BLOCK_RETRY_ENABLED=true
```
Перезапустить backend:
```bash
pkill -f "uvicorn backend.app.main"
uvicorn backend.app.main:app --host 0.0.0.0 --port 8081 --reload &
```
Запустить (или перезапустить) md-enrichment для нужной пары через UI/API.
Tiled retry сработает автоматически для проблемных блоков.

## Как отключить / откатить

```env
STAGE_COMPARISON_QWEN_PROBLEM_BLOCK_RETRY_ENABLED=false
```
Перезапустить backend. Поведение становится идентичным сборке без этой фичи —
middleware/хук становится no-op, обычные блоки и проблемные блоки обрабатываются
ровно как раньше (baseline). Никаких миграций не требуется.

## Где смотреть diagnostics

Per-item (в `<side>_image_descriptions.json`, `items[].problem_block_retry`):

```json
{
  "block_id": "...", "retry_enabled": true, "retry_attempted": true,
  "retry_reason": "timeout|http_error|invalid_json|not_usable|low_confidence|empty_facts|...",
  "retry_method": "tiled",
  "baseline_status": "error", "baseline_usable_for_diff": false,
  "retry_status": "done|failed|skipped", "retry_improved": true,
  "tiles_count": 12, "tiles_done": 12, "tiles_failed": 0,
  "final_method_used": "tiled_retry|baseline",
  "final_usable_for_diff": true, "cache_hit": false, "errors": []
}
```

Если блок восстановлен — у item появляется `method_used="tiled_retry"`,
`baseline_method="image_crop"`, warning `recovered_by_tiled_retry`, а исходный
ответ лежит в `item["baseline_result"]`.

Per-side/session summary (в payload `<side>_image_descriptions.json` →
`problem_block_retry`):

```json
{"enabled": true, "blocks_checked": 26, "retry_attempted": 4, "retry_done": 4,
 "retry_failed": 0, "retry_skipped": 0, "improved": 4, "cache_hits": 0}
```

## Как читать `method_used=tiled_retry`

Означает: baseline по этому блоку был проблемным, tiled retry его восстановил, и
итоговое `description` — это merged-результат тайлов. Для downstream (Opus
сравнение, IMAGE_DIFF_INDEX) формат `description` полностью совместим с обычным —
меняется только содержимое (богаче), не схема.

## Cache

Ключ tiled-результата учитывает: session/pair/side/block_id, хэш high-res
изображения, `render_long_side`, размер tile, overlap, `max_tiles`,
prompt_version, model. Повторный запуск с теми же параметрами — cache hit, Qwen не
вызывается (`problem_block_retry_cache_hit=true`). Кеш живёт в общем
`text_enrichment/cache` пары (как и обычный image-cache).

## Риски и лимиты

- **Латентность/стоимость:** tiling = до `MAX_TILES` вызовов на блок
  (~15–40 с каждый). Поэтому retry — только для проблемных блоков и под бюджетом
  `MAX_TOTAL_SEC_PER_BLOCK`. Обычные блоки не затрагиваются.
- **ngrok read-timeout ~300 с:** per-tile запрос короткий, в timeout не упирается;
  держите `TILE_TIMEOUT_SEC` ≤ транспортного лимита.
- **Дубли при overlap:** снимаются dedup'ом в merge.
- **Галлюцинации на высоком DPI:** действующие эвристики
  (`analyze_qwen_description_quality`) применяются к baseline; merged-результат —
  union фактов с provenance, без превращения uncertain в confirmed.
- **LM Studio:** retry использует тот же loaded инстанс Qwen, что и baseline;
  `chandra-ocr-2` не трогается.

## Связанные файлы

- [backend/app/services/stage_comparison/problem_block_retry.py](../backend/app/services/stage_comparison/problem_block_retry.py)
- [backend/app/services/stage_comparison/md_image_enrichment.py](../backend/app/services/stage_comparison/md_image_enrichment.py) — хук в `enrich_side`
- [tests/test_stage_comparison_qwen_problem_block_retry.py](../tests/test_stage_comparison_qwen_problem_block_retry.py)
- Эксперимент-обоснование: [experiments/qwen_problem_block_recognition/reports/final_report.md](../experiments/qwen_problem_block_recognition/reports/final_report.md)
- Acceptance smoke: [experiments/qwen_problem_block_recognition/reports/production_patch_smoke.md](../experiments/qwen_problem_block_recognition/reports/production_patch_smoke.md)

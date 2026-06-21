# Stage Comparison — text block equivalence precheck (links-based, mark-only)

**Дата:** 2026-06-08
**Статус:** mark-only, **НЕ подключён** к MD/Opus pipeline (наблюдение/аудит). Без флага влияния на работу нет.
**Модуль:** [backend/app/services/stage_comparison/text_block_equivalence.py](../backend/app/services/stage_comparison/text_block_equivalence.py)

## Зачем

Параллель к [visual_block_equivalence](../backend/app/services/stage_comparison/visual_block_equivalence.py),
но для ТЕКСТОВЫХ/ТАБЛИЧНЫХ блоков. Назначение — найти связанные пары текстовых
блоков OLD↔NEW, чьё содержимое идентично (с точностью до форматного шума), чтобы
в будущем (НЕ сейчас) такие блоки можно было не гонять повторно через Opus
enriched-MD сравнение.

```text
links.json (явные связи блоков)
  → result.json OLD/NEW: ocr_text каждого связанного text/table-блока
  → normalize_block_text (strip HTML + debug-префиксы + NFKC/ё→е + ws/lower)
  → compute_text_metrics (exact / char_ratio / token_jaccard / numbers_changed)
  → статус связи (identical_text / near_identical_text / changed_text / uncertain_text / skipped_*)
  → text_block_equivalence/text_block_equivalence.json (ОТДЕЛЬНЫЙ артефакт)
```

## Ключевой факт о тексте в ПОС (и почему нужна нормализация)

Текст блока в result.json лежит в поле `ocr_text` и в ПОС он **HTML-обёрнут**:
`<div data-bbox="x,y,w,h">…</div>`. Два идентичных по смыслу блока расходятся
ТОЛЬКО из-за разных bbox-координат в разметке. Поэтому `canonicalize_text` из
[block_equivalence_precheck.py](../backend/app/services/stage_comparison/block_equivalence_precheck.py)
(он не снимает HTML, не lower, не убирает префиксы) для этой задачи недостаточен.

`normalize_block_text` снимает ТОЛЬКО форматный шум, сохраняя контент:
1. HTML-теги и сущности (`<div data-bbox=…>` → внутренний текст);
2. debug-префиксы `BLOCK: <id>` в начале строк (OCR иногда вставляет);
3. NFKC, ё→е;
4. схлопывание пробелов/переводов строк, lower, strip.

Числа/даты/марки/сечения **не нормализуются** (они значимы).

## Консервативность (инвариант)

- `identical_text` — ТОЛЬКО при ТОЧНОМ равенстве нормализованного текста обеих
  сторон. Любое расхождение (включая изменившиеся числа/даты/объёмы/марки/ссылки
  на листы) → `near_identical_text` или `changed_text`, но НЕ `identical_text`.
  Точное равенство нормализованного текста гарантирует идентичность чисел по
  построению.
- `near_identical_text` НЕ исключается (mark-only флаг не выставляется).
- Исключающий флаг `exclude_from_opus_md=true` ставится ТОЛЬКО для
  `identical_text`. `exclude_from_qwen` для текстовых блоков **всегда False**
  (Qwen описывает графику, не текст). `enforced` **всегда False** — реального
  skip нет.

## Статусы

| Статус | Когда | exclude_from_opus_md |
|---|---|---|
| `identical_text` | нормализованный текст обеих сторон ТОЧНО равен | **true** |
| `near_identical_text` | `char_ratio ≥ near_threshold` (0.92), но не точно | false |
| `changed_text` | `char_ratio < near_threshold` | false |
| `uncertain_text` | ровно одна сторона пустая после нормализации (markup-only?) | false |
| `skipped_no_text` | обе стороны пусты/коротки (< min_chars) | false |
| `skipped_non_text` | блок не text/table (image/graphic — скоуп visual) | false |
| `skipped_stale_link` | связь `*_stale` | false |
| `skipped_not_one_to_one` | блок в 1↔много / много↔1 | false |
| `skipped_block_missing` | блок не найден в result.json | false |

## Флаги (`.env`)

| Переменная | Default | Назначение |
|---|---|---|
| `STAGE_COMPARISON_TEXT_BLOCK_EQUIVALENCE_ENABLED` | `false` | информационный флаг (модуль никуда не подключён; флаг лишь попадает в артефакт) |
| `STAGE_COMPARISON_TEXT_BLOCK_EQUIVALENCE_MIN_CHARS` | `3` | мин. длина нормализованного текста, чтобы блок считался текстовым |
| `STAGE_COMPARISON_TEXT_BLOCK_EQUIVALENCE_NEAR_THRESHOLD` | `0.92` | порог `near_identical_text` |
| `STAGE_COMPARISON_TEXT_BLOCK_EQUIVALENCE_MAX_LINKS_COMPARED` | `5000` | safety cap |

## Артефакт

`pairs/<pid>/text_block_equivalence/text_block_equivalence.json` — НОВЫЙ
отдельный файл. `comparison_result.json`, `links.json`, `page_alignment.json`,
`visual_block_equivalence.json`, enriched MD НЕ затрагиваются. Поля: `summary`
(счётчики статусов + `potential_opus_blocks_removed` = identical_text),
per-link `status`/`metrics`/`reason`/`exclude_from_opus_md`/`enforced=false`.

## Контролируемая проверка (ПОС, реальная пара)

Session `ba413a93c5754f6c`. Прогон БЕЗ Qwen/Opus (детерминированный разбор
result.json + links.json):

| Пара | links | compared | identical_text | near | changed | skipped |
|---|---:|---:|---:|---:|---:|---:|
| pac34250b (ПОС: ПОС ↔ 6-ПОС) | 54 | 45 | **3** | 29 | 13 | 9 (non_text) |
| p698fce07 (ПЗУ) | 12 | 1 | 0 | 0 | 1 | 11 (stale 7 + non_text 4) |

`identical_text=3` на ПОС — реальный потенциал исключения из Opus (условные
обозначения стройгенплана, формулировки о земельном законодательстве, регламент
работы кранов). 29 near (0.99+) — кандидаты на ручное подтверждение; не
исключаются (часть «уехала» в near из-за просочившегося в OCR id-токена блока —
безопасно: мы не исключаем сомнительное).

Сравните с visual-прогоном той же пары: `identical_visual=0` — графика реально
переработана между ревизиями (минимум 1 minor_render_noise + 7 changed), ложных
identical нет. То есть экономия на ПОС лежит в ТЕКСТЕ, не в графике.

## Безопасность

- mark-only, `enforced=false`, ни Qwen, ни MD, ни Opus не задействованы и не
  изменяются; пишется только новый артефакт;
- модуль НЕ импортирует graphic_llm / enriched_comparison / md_*_jobs (тест это
  проверяет через AST);
- fail-soft на уровне связи (исключение → `uncertain_text`, batch не падает).

## Тесты

[tests/test_stage_comparison_text_block_equivalence.py](../tests/test_stage_comparison_text_block_equivalence.py)
— нормализация (HTML/префикс/ё/ws), метрики (exact/numbers_changed/канонизация
чисел), статусы, инвариант exclude-only-identical, «изменившиеся числа ≠
identical», batch summary/артефакт/fail-soft, отсутствие Qwen/Opus-импортов.

## Связанные файлы

- [text_block_equivalence.py](../backend/app/services/stage_comparison/text_block_equivalence.py)
- [visual_block_equivalence.py](../backend/app/services/stage_comparison/visual_block_equivalence.py) — визуальная параллель (image-блоки)
- [block_equivalence_precheck.py](../backend/app/services/stage_comparison/block_equivalence_precheck.py) — `EqBlock` / `extract_blocks_for_equivalence` (переиспользуются)
- [paths.py](../backend/app/services/stage_comparison/paths.py) — `text_block_equivalence_dir/_report_path`

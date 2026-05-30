# Block crop semantics — code audit

Аудит действующей логики PDF→PNG crop для image-блоков stage 02 на пилотном проекте
`13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf`. Читался фактический код, не только комментарии.

Все ссылки ниже — на [blocks.py](../../../../../../../blocks.py).

## 1. Какой путь сейчас используется

Production crop идёт через [blocks.py:crop_blocks()](../../../../../../../blocks.py#L305).
Для каждого image-блока из `*_result.json`:

1. Пропускаются `block_type != "image"`, `category_code == "stamp"`, и блоки с
   `area_px < MIN_BLOCK_AREA_PX2 = 50_000` (пикселей² в координатах result.json)
   [blocks.py:410](../../../../../../../blocks.py#L410).
2. Предпочтительный путь — cloud download по `crop_url` из OCR-result через
   [blocks.py:download_and_convert()](../../../../../../../blocks.py#L201), который
   декодирует PDF-кроп и рендерит PNG через
   [blocks.py:_render_pdf_bytes_to_png()](../../../../../../../blocks.py#L157).
3. Fallback — локальная вырезка из PDF по `coords_px` через
   [blocks.py:crop_from_pdf()](../../../../../../../blocks.py#L239).

Оба пути обслуживаются одним движком рендера PyMuPDF и принимают одни и те же
параметры: `target_px`, `dpi`, `min_long_side`.

## 2. Как работает скейл

Фактическая формула — в
[blocks.py:_render_pdf_bytes_to_png()](../../../../../../../blocks.py#L180-L191):

```python
if dpi > 0:
    render_scale = dpi / 72
    if min_long_side > 0:
        render_scale = max(render_scale, min_long_side / long_side_pt)
elif target_px > 0:
    render_scale = target_px / long_side_pt
else:
    render_scale = TARGET_DPI / 72

render_scale = max(1.0, min(8.0, render_scale))
```

В production:

- `TARGET_DPI = 100` — fixed density для всех блоков (non-compact).
- `MIN_LONG_SIDE_PX = 800` — действует как **floor**: если геометрия блока при
  `100 DPI` даёт меньше `800 px` по длинной стороне, scale подтягивается вверх.
- Финальный clamp `[1.0, 8.0]` ограничивает масштаб сверху (практически —
  только для сверхмалых блоков, у которых `800/long_side_pt > 8`).

В `crop_from_pdf()` та же логика [blocks.py:283-294](../../../../../../../blocks.py#L283-L294)
с нижним clamp `0.5` вместо `1.0` — это legacy-нюанс и в обычном DPI-режиме не
активен (DPI ≥ 72 гарантирует scale ≥ 1.0).

## 3. Есть ли верхний clamp по длинной стороне?

В обычном non-compact crop **нет верхнего clamp на длинную сторону PNG**. Scale
ограничен `8.0`, но длинная сторона пикселя определяется произведением
`scale × long_side_pt`. Крупные блоки могут рендериться далеко выше 1500 px.

Проверено на пилоте — из 215 блоков 4 блока имеют long side ≥ 1500 px
(max = 2629 px). Для мелких блоков same scale = min_long_side/long_side_pt,
т.е. итог = ровно `MIN_LONG_SIDE_PX` (800 px) по длинной стороне.

Поэтому `TARGET_LONG_SIDE_PX = 1500` для production crop — **legacy константа**,
не ограничивает вывод. Она реально используется только в:

- [blocks.py:_render_full_page()](../../../../../../../blocks.py#L734) — рендер
  полной страницы при консолидации (сейчас отключён, `PAGE_MERGE_MIN_BLOCKS=999`).
- [blocks.py:_render_page_quadrants()](../../../../../../../blocks.py#L770) —
  четверти страницы (тоже отключены, `PAGE_QUADRANT_MIN_BLOCKS=999`).
- [blocks.py:recrop_blocks()](../../../../../../../blocks.py#L1619) — fallback для
  неизвестных `render_size` при recrop итераций.

## 4. Compact-mode

Compact-режим (флаг `--compact` в `blocks.py crop`) управляется отдельной
парой констант `TARGET_DPI_COMPACT=50` и `MIN_LONG_SIDE_PX_COMPACT=500` и
включается только руками. В production по умолчанию **не активен**. В рамках
этого эксперимента **compact специально НЕ трогаем** — иначе смешаем две
независимые оси.

## 5. Что из метаданных влияет на downstream batching

Production batching использует эти поля из `blocks/index.json`:

- `size_kb` — реальный объём PNG на диске
  (размер пакета `MAX_BATCH_SIZE_KB`, solo_kb, heavy classification).
- `render_size` — фактические пиксели `[w, h]` после рендера.
- `ocr_text_len` — длина OCR-текста (для heavy classification).
- `crop_px` — координаты в pixel-space result.json (для heavy classification).
- `is_full_page`, `quadrant`, `merged_block_ids` — флаги для synthetic-блоков
  (страница/четверть/объединённый). В обычном crop всегда False/пусто.

Классификация risk (heavy/normal/light) в
[blocks.py:_classify_block_risk()](../../../../../../../blocks.py#L920) зависит
от `size_kb`, `render_long`, `ocr_text_len`, `crop_long`. Смена `MIN_LONG_SIDE_PX`
меняет `size_kb` и `render_long`, что **может** перевести блок в более тяжёлый
класс → иной `predicted batch plan`.

Claude risk-aware packer
[blocks.py:_pack_blocks_claude_risk_aware()](../../../../../../../blocks.py#L977)
ограничен hard cap `CLAUDE_HARD_CAP = 12` и таргетами `heavy≈5 / normal≈8 /
light≈10`. Hard cap не обходится ни одним override.

## 6. Независимая ось эксперимента и фиксы

| Ось                   | Эксперимент            | Фиксировано           |
|-----------------------|------------------------|------------------------|
| `MIN_LONG_SIDE_PX`    | 800 / 1000 / 1200      | —                     |
| `TARGET_DPI`          | —                      | 100 (production)       |
| `compact` режим       | —                      | false                 |
| `TARGET_LONG_SIDE_PX` | —                      | 1500 (не меняется)    |
| batching policy       | —                      | baseline_p3 (production) |
| parallelism           | —                      | одинаковый на всех profiles (subset=3, full=3) |

Единственная независимая переменная — `MIN_LONG_SIDE_PX` (floor длинной стороны
PNG). Остальное зафиксировано для чистоты эксперимента.

## 7. Текущее состояние пилота

Индекс `_output/blocks/index.json`:

- `total_blocks = 215`
- `compact = false`
- long side min/avg/max: `800 / 842 / 2629`
- size_kb min/avg/max: `12.4 / 44.5 / 605.6`
- long ≥ 800 px: **215** (все — базовая линия упирается в пол 800)
- long ≥ 1000 px: 10
- long ≥ 1500 px: 4
- pages covered: 6–20 (15 страниц)

Т.к. 205 из 215 блоков сейчас имеют ровно 800 px по длинной стороне
(floor-effect), поднятие `MIN_LONG_SIDE_PX` до 1000/1200 затронет практически
весь набор — именно это и нужно измерять.

## 8. Что именно будет overrideить experimental runner

Runner (scripts/run_block_resolution_matrix.py) будет передавать в crop-функцию
явный `render_profile` = dict с полями `target_dpi` и `min_long_side_px`.

- `target_dpi = 100` — одинаково для всех профилей (не ось).
- `min_long_side_px ∈ {800, 1000, 1200}` — независимая ось.
- `compact` не используется (production default).

Production-путь (`python blocks.py crop`) остаётся нетронутым: без ENV и без
явного override модульные константы `TARGET_DPI` и `MIN_LONG_SIDE_PX` сохраняются.

Crop каждого профиля идёт в изолированный shadow-корень
`<exp>/crop_roots/<profile>/blocks/` и никогда не пишет в `_output/blocks/`
пилотного проекта.

## 9. Возможные подводные камни

1. **Cache:** если в shadow-корне уже лежит `index.json` с совпадающим
   `render_profile`, runner обязан переиспользовать его (не recrop'ить).
2. **Dimensions drift:** после поднятия `MIN_LONG_SIDE_PX` `render_size` и
   `size_kb` пересчитываются, что влияет на `_classify_block_risk` и предсказание
   batch-plan. Это **не баг**, а именно тот эффект, который мы измеряем.
3. **Hard cap 12:** даже если новый профиль классифицирует больше блоков как
   heavy, батч-plan всё равно не может превышать 12 block/batch. Отдельная
   метрика — `predicted total batches` (меняется) и `max_heavy_in_batch`.
4. **Subset quality-isolation:** В фазе A subset гоняется в single-block режиме
   (1 блок на LLM-вызов), чтобы изолировать эффект разрешения от
   batch-attention dilution. В фазе B валидация идёт на full document с
   production batching — это измеряет практический эффект в production.

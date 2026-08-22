# Vector 0.3 — сравнение подготовленных графических блоков

Дата исследования: 2026-08-23

Базовые commit: `1619fc3f`, `5e334546`

Статус: research only; Production Stage Comparison не изменён

## Краткий ответ

Нет: generic object layer в текущем виде не даёт надёжный самостоятельный список графических изменений для произвольного уже подготовленного блока П ↔ РД.

Он полезен как evidence/routing layer:

- находит все три подтверждённых локальных graphic-change pair;
- безопасно пропускает четыре узких `GRAPHIC_VECTOR_OK` case без false-safe;
- правильно не сравнивает upstream text/table blocks;
- даёт адресные candidates для одного fused Hybrid call.

Но deterministic ledger непригоден как самостоятельный результат: accuracy `9/37 = 24.3%`, 28 false-positive pair, `TEXT_ONLY` false rate `2/2`, а pair-level precision added/removed candidates только `6.7%/3.3%`. Причина не в одном threshold, а в generic object identity: CAD repacking, outlined glyphs, crop padding и повторяющиеся похожие символы неразличимы без более ранних discipline-specific profiles.

**Вердикт: C. Generic object layer недостаточен; discipline-specific profiles нужны раньше.**

## 1. Реальный input contract

Исследование сначала проверило существующий pipeline, не создавая параллельный detector.

Авторитетные артефакты:

- `<version>/02_work/blocks.json` — schema v1, `coordinate_space=normalized_page_top_left`;
- `<version>/02_work/document.pdf` — source PDF;
- `blocks[].block_id`, `page_index`, `block_type`, `shape_type`, `coords_norm`, `polygon_points`, `crop_url`, `status`, `export_status`;
- `pages[].page_index`, `width_px`, `height_px`, `rotation`;
- опциональный existing `03_analysis/.../block_vector_graphs/<block_id>.json#graph.semantic_ledger` — только prepared label anchors.

Production mapper: `backend/app/services/common/blocks_json.py`. Координатный oracle: `backend/app/services/common/pdf_crop.py::extract_block_crop`.

Benchmark reference содержит только:

```json
{
  "blocks_json": ".../02_work/blocks.json",
  "block_id": "blk_...",
  "block_group_id": "pair-id"
}
```

`bbox`, `bbox_norm`, `coords_norm` и polygon в benchmark reference запрещены. Resolver загружает их из реального upstream row. `crop_url` переиспользуется как существующая access link; отдельный cloud/download contract не изобретался.

Scope уже forward-compatible:

```text
left_blocks[] + right_blocks[] + block_group_id
```

Но block/sheet/1→N matcher не реализован: все пары заданы явно.

Полная фиксация contract: `block_input_contract.json`.

## 2. Координаты, extraction и cache

Comparison unit — только `ALREADY_PREPARED_GRAPHIC_BLOCK`. Вся PDF page читается исключительно как технический источник vector layer:

```text
PDF SHA + page + extractor version cache
    ↓
page.get_drawings() один раз
    ↓
upstream coords_norm / polygon
    ↓
strict clip
    ↓
graphic objects
```

Проверено:

- visual block coordinates переводятся с `page.derotation_matrix`;
- учитываются visible `CropBox ∩ MediaBox` и `cropbox_position`, как в production helper;
- test matrix 0/90/180/270 сохраняет один видимый rectangle и четыре segment на каждой ориентации;
- line clipping — Liang–Barsky, затем polygon clip;
- белая fill-only geometry без stroke удаляется как доказуемо невидимая;
- общего foreground/background или hatch filter нет;
- углы, lengths и object shape нормализуются одной isotropic block-diagonal scale; bbox position остаётся по осям блока;
- segment cap использует round-robin spatial cells, не longest-lines-only;
- dense symbol/relation candidate caps отмечаются как dangerous и запрещают `GRAPHIC_VECTOR_OK`.

Cache выполнен как быстрый локальный pickle. Gzip page cache был отклонён экспериментально: на больших nested PyMuPDF payload decompression добавлял десятки секунд. Cache не коммитится. В полном прогоне он достиг 2.665 GB; это ещё один operational constraint.

## 3. GraphicBlockDescription

Generic types:

- `SYMBOL_OBJECT`;
- `LINEAR_OBJECT`;
- `CLOSED_REGION`;
- `CONNECTED_NETWORK`;
- `COMPOSITE_GRAPHIC`;
- `UNKNOWN_OBJECT`.

Formation использует несколько правил, а не один clustering radius:

- source-path connectivity split;
- endpoint/collinear continuity;
- bounded enclosure/export grouping;
- bounded small-symbol grouping;
- repeated generic family signatures;
- visible style.

Агрессивного background classifier нет. Каждому object сохраняются bbox, isotropic center/size, geometry/family signatures, topology summary, style, label anchors, relation IDs, formation и source drawing provenance.

Relations используются для адресации, не как global score:

- `CONNECTED_TO`;
- `CONTAINS`;
- `ADJACENT_TO`;
- `ALIGNED_WITH`;
- `LABEL_ANCHOR`;
- `REPEATS_WITH`.

Каждая relation имеет confidence/provenance. Candidate generation spatially indexed и bounded; срабатывание cap делает object formation partial.

Schema: `graphic_block_description.schema.json`.

## 4. Comparator и ledger

Alignment — отдельный evidence object:

- translation;
- uniform scale;
- unique geometry/family anchors;
- median residual;
- без affine warp.

Comparator выполняет local greedy correspondence по prepared label anchor, geometry signature, generic family/type, aligned position и size. Он не принимает global `99.5% same` как основание скрыть local event.

Статусы:

- `UNCHANGED`;
- `ADDED`;
- `REMOVED`;
- `GEOMETRY_CHANGED`;
- `STYLE_CHANGED`;
- `CONNECTION_CHANGED`;
- `POSITION_CHANGED`;
- `UNCERTAIN`.

Style сравнивается только внутри matched object. Connection events сравниваются только по адресным graph relations matched objects.

Validator не имеет `TEXT_CHANGED`/`TABLE_CHANGED` event type и удаляет basis `text_only`. Если upstream `block_type` — `text`, `table` или `stamp`, возвращается `GRAPHIC_NOT_APPLICABLE`/skip.

Важно: validator предотвращает явный дубль текста, но не умеет доказать, что vector outline является glyph, а не graphic symbol. Именно поэтому реальный `TEXT_ONLY` всё равно даёт false graphic objects.

Schema: `graphic_change_ledger.schema.json`.

## 5. Benchmark и human ground truth

Собрано 38 реальных explicit block pairs из SS, VK, AR и OV. Все 76 sides разрешаются по существующим `blocks.json`; bbox в manifest отсутствует.

Один baseline case `eom_singleline_changed` исключён: для выбранных версий нет `02_work/blocks.json`, а повторное использование старого manual bbox нарушило бы v0.3 contract.

Распределение GT:

- 3 `GRAPHIC_CHANGE`: `vk_plan`, `vk_nodes`, `ov_plan_floor07`;
- 34 `NO_GRAPHIC_CHANGE`;
- 1 `UNSURE`: `ss_crop_mismatch_page07` из-за разных semantic extents prepared crops.

Подтверждённые events:

- `vk_plan`: в RIGHT добавлены многочисленные красные кольцевые node markers;
- `vk_nodes`: в RIGHT добавлена короткая оранжевая branch/fitting и graphical dimension line;
- `ov_plan_floor07`: в нескольких розовых зонах RIGHT удалены internal equipment contours/symbols.

GT задан manual raster review. После конфликта с Hybrid cases VK были ещё раз вручную adjudicated непосредственно по LEFT/RIGHT raster: видимые graphic additions подтверждены. Утверждение модели не использовалось как ground truth.

Negative controls:

- `TEXT_ONLY`: `ss_scheme_text_changed`, `vk_axono_page17`;
- реальные upstream table/text blocks: `ss_table_page19`, `ov_equipment_table`;
- controlled `table_content_changed` и `text_changed_graphics_same`.

Corpus gaps:

- нет реального style-only positive;
- нет connection-only positive;
- нет eligible raster/vector-empty prepared pair, поэтому `GRAPHIC_VISION_ONLY` на real corpus не валидирован;
- реальные table controls совпадают по содержимому; changed-table-only проверен controlled falsifier;
- per-object exhaustive correspondence annotation отсутствует.

## 6. Результаты deterministic layer

| Метрика | Результат |
|---|---:|
| Scored real pairs | 37 |
| Pair accuracy | 9/37 = 24.3% |
| False-positive pairs | 28 |
| False-negative pairs | 0 |
| Local graphic-change recall | 3/3 = 100% |
| Added pair-level precision / recall | 2/30 = 6.7% / 2/2 = 100% |
| Removed pair-level precision / recall | 1/30 = 3.3% / 1/1 = 100% |
| TEXT_ONLY false graphic change | 2/2 = 100% |
| TABLE controls false graphic change | 0/2 = 0% |
| `GRAPHIC_VECTOR_OK` false-safe | 0/4 = 0% |
| `GRAPHIC_VECTOR_OK` | 4/38 |
| `GRAPHIC_HYBRID` | 34/38 |
| `GRAPHIC_VISION_ONLY` | 0/38 |
| Vision usage | 34/38 = 89.5% |
| Median / max pair latency | 12.05 s / 91.66 s |
| Total deterministic benchmark latency | 941.36 s |

Object-count stability выглядит обманчиво хорошо:

- median no-change object-count ratio: `98.44%`;
- median matched fraction: `90.63%`;
- exact object count: только 5/32 applicable no-change pairs.

Несмотря на эти global-looking цифры, local ledger переполнен false events. Поэтому count/matched fraction нельзя превращать в similarity verdict.

Object matching precision/recall не заявляются: без exhaustive human object correspondences это была бы выдуманная метрика. Pair-level added/removed proxies приведены явно и не выдаются за per-object precision.

Connection precision/recall на real corpus — N/A: нет connection-only positive. Generic comparator создал connection candidates на 32 pairs, что само по себе показывает низкую специфичность; controlled connection case пройден.

## 7. Controlled falsifiers

Пройдено 8/9:

- одно local removal среди 50 000 line primitives — найдено;
- один rectangle, упакованный как path против four lines — unchanged;
- unlabelled object removal — найдено;
- good label anchor при repacking — matched;
- text changed / graphics same — no graphic event;
- table content changed — `GRAPHIC_NOT_APPLICABLE`;
- matched style solid→dashed — `STYLE_CHANGED`;
- removed relation — `CONNECTION_CHANGED`.

Провал:

- два похожих objects поменялись местами;
- generic family + position assignment перекрёстно сопоставил их как unchanged;
- identity/position event потерян и ошибочно получил `GRAPHIC_VECTOR_OK`.

Это конкретный counterexample против production-ready generic identity.

## 8. Fused Hybrid

Выполнен один вызов `gpt-5.6-sol` на 10 hard pairs:

- 20 production-correct raster crops;
- короткий object status count + максимум три candidate samples;
- одна конкретная uncertainty на pair;
- без verify-all;
- без старого L3;
- без повторного text/table comparison.

Результат после manual GT adjudication:

| Метрика | Результат |
|---|---:|
| Scored pairs | 9 (1 UNSURE исключён) |
| Accuracy | 9/9 = 100% |
| Latency | 224.15 s |
| Prompt text | 6 766 chars (~1 692 tokens chars/4) |
| Reported input tokens | 1 450 510 |
| Reported cached input tokens | 1 342 720 |
| Output tokens | 14 267 |
| Total input + output | 1 464 777 |

Изображения доминируют в фактическом token accounting: короткий vector packet сам по себе не делает batch дешёвым. Результат 9/9 перспективен, но выборка мала, содержит только три positives и не покрывает raster-only/style-only/connection-only real cases. Он подтверждает routing architecture, а не production readiness object layer.

## 9. Почему generic layer ломается

### CAD primitive repacking

Одинаковая видимая сущность меняет source paths, component boundaries, endpoints и family membership. Простая node/segment tolerance не делает identity export-invariant.

### Outlined text

Prepared text metadata неполно покрывает glyph outlines. Generic vector geometry не может доказать, что маленький contour — буква, цифра или инженерный symbol. Поэтому итоговый validator не спасает раннее object formation.

### Crop padding и semantic extent

Block-normalized coordinates чувствительны к разной рамке/padding. Translation + uniform scale недостаточны, если upstream blocks включают неодинаковую периферию; affine warp запрещён правильно, но correspondence становится неопределённым.

### Repeated similar objects

Generic family matching теряет identity при перестановке. Label anchor помогает только там, где prepared metadata полно и устойчиво.

### Dense blocks и caps

Spatial caps сохраняют локальное покрытие лучше longest-lines-only, но не восстанавливают semantic grouping. Они честно переводят case в Hybrid, однако Vision usage достигает 89.5%, а page cache — гигабайтов.

## 10. Что нужно до production design

Не ещё один общий threshold research. Нужны ранние discipline-specific profiles:

1. VK/OV network profile: pipes/branches/fittings/node markers, attachment topology и допустимые export variants.
2. AR/SS plan profile: walls/openings/axes/revision graphics и отдельная политика для dimensions/annotations.
3. Provenance-aware glyph/annotation masking, опирающийся на существующий structured text pipeline, без второго text comparator.
4. Profile-specific symbol families и identity rules для repeated/moved objects.
5. Crop-semantic compatibility gate до object correspondence.
6. Отдельный real benchmark с exhaustive annotations для added/removed/connection/style и raster-only cases.

Только после этого имеет смысл повторить узкий v0.3 comparator research. Ничего из эксперимента не следует подключать к production сейчас.

## 11. Итоговый ответ и вердикт

Можем ли мы взять уже подготовленный графический блок П и РД, не трогая повторно text/tables, и получить надёжный список именно графических изменений на уровне объектов?

**Не generic deterministic layer в текущем виде.** Он хорошо работает на exact/simple cases и полезен для адресации Hybrid, но не даёт приемлемую precision на реальных CAD blocks. Один fused Hybrid показывает высокую точность на малой выборке, однако дорог и не закрывает corpus gaps.

**Вердикт C: generic object layer недостаточен; discipline-specific profiles нужны раньше.**

## 12. Трассируемые артефакты

- `artifacts/benchmark_pairs.json` — 38 references без bbox;
- `artifacts/ground_truth.json` — manual graphic-only GT;
- `artifacts/object_descriptions/**` — sampled index + lossless `*.full.json.gz`;
- `artifacts/object_comparisons/**/graphic_change_ledger.json` — sampled ledger index + lossless payload;
- `artifacts/routing_results.json` — pair decisions и metrics;
- `artifacts/hybrid_results.json` — единственный fused call, usage/latency и evaluation;
- `artifacts/controlled_falsifiers.json` — 9 controlled cases;
- `artifacts/human_validation.md` — построчная human/deterministic сверка.

Каждый sampled index содержит filename lossless payload, uncompressed SHA-256, byte count и полные counts. Это сохраняет полный deterministic ledger без многосотмегабайтных pretty-printed JSON.

# Stage Comparison Pipeline V2 — Graphic Vision Grounding

**Дата:** 2026-06-11
**Модуль:** [backend/app/services/stage_comparison/pipeline_v2_graphic_vision_grounding.py](../backend/app/services/stage_comparison/pipeline_v2_graphic_vision_grounding.py)
**Статус:** offline, fail-soft; в dry-run включается автоматически, если был
graphic vision enrichment (отдельный артефакт, сырой vision report не меняется).

## Зачем

Tiled/single-shot vision (Qwen) на плотных схемах читает реальные мелкие
номиналы, которых не видит single-shot high_res — но одновременно
**галлюцинирует**: достраивает типовые ряды и плодит no-op «изменения».

Контролируемый pilot ГРЩ ИОС 1.1 (blocks `7EMD-DT4R-6TN` ↔ `763U-YFTA-DVQ`,
qwen3.6-35b-a3b, tiled 2×2) показал оба эффекта одновременно:

* **реальные чтения** (подтверждены векторным текст-слоем): `QF5 400А→200А`,
  `4х185→4х120`, `SA 1600А`, `QF 2500А`, `T1/T2 1250кВА`;
* **галлюцинации**: фабрикованный стандартный ряд `2P 25…800А`, повтор
  `QF3…QF17 100А`, no-op «изменено … (без изменений)».

Сырой vision-output нельзя сразу подмешивать в delta/critic. Grounding —
второй проход: **подтвердить каждую сущность/число по anchor-тексту блока**,
снять достроенные ряды и no-op, пометить негрунтованное.

## Почему tiled vision галлюцинирует ряды

Реальные номиналы автоматов **всегда** из стандартного ряда МЭК (25, 32, 40,
63, 100, 160…). Когда модель видит часть схемы и «понимает», что это ряд
автоматов, она склонна достроить недостающие рунги стандартной серии или
повторить один номинал на все отходящие линии — даже если на чертеже их нет.
Поэтому «похоже на стандартный ряд» — **слабый** сигнал (он всегда выполняется
для реальных схем). **Сильный** сигнал фабрикации = значение НЕ найдено в
текстовом/векторном слое блока.

## Anchors (источник истины)

Anchors берутся без сети. Приоритет источников (от самого авторитетного):

1. **полный `pdfplumber_text`** блока — векторный текст-слой PDF целиком;
2. **полный OCR `ocr_text`** блока;
3. `pdfplumber_text_excerpt` (600 симв., в normalized model);
4. `ocr_json_summary.key_entities` (`1600А, 2500А, 800А, 1250кВА`…),
   `content_summary` / `detailed_description`, `text_excerpt`;
5. `stamp_data.sheet_name` / `document_code`.

**Полный текст-слой (1–2) не хранится в normalized model** (там только 600-симв.
excerpt'ы — иначе артефакт раздувается). Grounding подтягивает его **лениво из
`source.result_json_path`** (`_load_full_block_texts`, fail-soft, cap 40k симв.)
и использует ТОЛЬКО внутри себя — в grounding report сохраняются лишь короткие
наборы значений + метка `source` (`full_text`/`excerpt`/…), не сам текст.
`use_full_text=False` отключает подтяжку (fallback на excerpt).

`collect_block_text_anchors(block, full_texts=…)` нормализует строки в два
blob'а: **spaced** (для извлечения номиналов — нужны границы токенов) и
**compact** (для подстрочного поиска маркировок). OLD-сущности грунтуются по
LEFT-блоку, NEW — по RIGHT. Достроенные ряды детектируются против
**объединённых** anchors пары (значение, реальное на любой стороне, не считается
фабрикацией).

### Почему excerpt занижает recall (и почему нужен полный текст)

Excerpt (600 символов) обрезает плотный текст-слой: на блоке ГРЩ полный
`pdfplumber_text` = 3029 симв. + `ocr_text` = 4073 симв., из которых excerpt
видит ~15%. Реальные номиналы (`100А, 125А, 315А`…), стоящие дальше 600-го
символа, при excerpt-only давали `ungrounded` — **false negative grounding'а**,
а в худшем случае ложный `rejected_artificial_series` (значение есть на чертеже,
но не в excerpt → выглядит как «достроенное»). Полный текст-слой это снимает.

Live-эффект (pilot ГРЩ 7EMD, тот же vision report): excerpt → full text
повышает grounded `55→81`, weakly `18→33`, и убирает ложные
`artificial_series 31→0` (100А реально присутствует в полном `ocr_text`).

> Если у блока нет текст-слоя (напр. ВРУ-1 LEFT: `pdfplumber=0`, OCR без
> номиналов) — recall остаётся низким честно: grounding занижает доверие, а не
> выдумывает уверенность. Это предел данных, а не алгоритма.

## Нормализация (`normalize_engineering_token`)

Канонизирует ФОРМАТ, но НЕ склеивает разные цифры:

* NFKC + lower; гомоглифы кириллица→латиница (`А`→`a`, `ТА`→`ta`, `Х`→`x`);
* `×`/`х`→`x`, тире `–—−`→`-`; десятичная запятая между цифрами → точка
  (`233,6`→`233.6`); `QF1, QF2` цел;
* единицы: `кВАр`→`kvar`, `кВт`→`kw`, `кВА`→`kva`; метки `Pp/Рр`→`pp`,
  `Ip/Iр`→`ip`;
* пробелы схлопываются в один + склейка «число+единица» (`400 А`→`400a`), но
  пробел между РАЗНЫМ токеном и номиналом сохраняется (`1QF5 400А`→`1qf5 400a`,
  чтобы дизайнатор и номинал не слиплись и номинал извлёкся).

Эквивалентности: `400А`≡`400 А`, `4х185`≡`4x185`, `ТА1–ТА3`≡`TA1-TA3`,
`кВАр`≡`квар`. Различия сохраняются: `400А`≠`4000А`, `4х185`≠`4х95`.

## Статусы

| статус | смысл |
|---|---|
| `grounded` | значение/маркировка найдены в anchor-тексте нужной стороны |
| `weakly_grounded` | частично: маркировка есть, номинал не подтверждён (или наоборот) |
| `ungrounded` | vision сообщил, в anchors не найдено |
| `rejected_artificial_series` | достроенный типовой ряд / искусственный повтор номиналов, не покрытый anchors |
| `rejected_designator_range` | галлюцинированный диапазон дизайнаторов (`QF1…QF100`), не покрытый anchors |
| `rejected_noop` | old==new после нормализации («100А → 100А», «без изменений») |
| `rejected_invalid_format` | пустая/непарсабельная сущность |
| `no_anchor_available` | у блока нет текстового слоя |

Дополнительно каждый grounded/rejected элемент несёт поле **`reason`** (понятный
код): `grounded`, `partial_match`, `not_found_in_anchors`, `no_anchor_available`,
`artificial_rating_ladder`, `repeated_same_rating`, `artificial_designator_range`,
`noop_change`.

### Designator-range галлюцинации

`detect_artificial_designator_range(text, anchors)` ловит достроенные
enumeration-диапазоны дизайнаторов: `QF1…QF100`, `QF1-QF1000`, `KM1…KM10`,
`KA1…KA10`, `SPD1…SPD10`. Правило:

* диапазон с охватом `span ≥ 8`, чей **верхний конец НЕ найден в anchors**, →
  `rejected_designator_range`;
* короткие валидные диапазоны (`TA1-TA3`, `QF1-QF6`, span < 8) — НЕ трогаются;
* реальная длинная серия (`QF1…QF13`, где `QF13` есть в anchors) — НЕ
  отвергается (верхний конец подтверждён);
* второй префикс, если указан, должен совпадать с первым (`QF1…QF100`), иначе
  это не диапазон одной серии.

Это закрывает класс галлюцинаций, который раньше уходил в `ungrounded` (безопасно,
но без явной метки). Live (ВРУ-1+ВРУ-4): `designator_range_rejected = 9–11`
(`QF1…QF100`, `KM1…KM10`, `QF1-QF1000` — теперь явно отклонены).

* **сущность** (`ground_vision_entity`): извлекаются номиналы/сечения/мощности +
  маркировка; все значения найдены → `grounded`; часть/только маркировка →
  `weakly_grounded`; ничего → `ungrounded`.
* **изменение** (`ground_observed_change`): сначала `detect_noop_change`
  (old==new → `rejected_noop`); затем old→LEFT, new→RIGHT; обе стороны
  подтверждены → `grounded`, одна → `weakly_grounded`, ни одной → `ungrounded`.
* **достроенный ряд** (`detect_artificial_series`): среди UNGROUNDED номиналов
  (значения в anchors защищены) ищем (1) повтор одного значения ≥6 раз; (2)
  монотонную стандартную лесенку ≥6 рунгов. Реальные `SA 1600А`/`QF 2500А`,
  присутствующие в anchors, НЕ отвергаются, даже если рядом есть фабрикованный
  ряд.

Comma-joined сущности (`"QF1 (63А), QF2 (400А)"`) атомизируются по запятой-
сепаратору (не десятичной) для пер-сущностного grounding.

## Контракт отчёта

`graphic_vision_grounding_report.json`:

```json
{
  "version": 1,
  "kind": "stage_comparison_pipeline_v2_graphic_vision_grounding",
  "status": "ok|completed_with_warnings|failed",
  "summary": {
    "items_total": 0, "entities_total": 0,
    "entities_grounded": 0, "entities_weakly_grounded": 0, "entities_ungrounded": 0,
    "changes_total": 0, "changes_grounded": 0, "changes_weakly_grounded": 0,
    "changes_rejected": 0,
    "artificial_series_rejected": 0, "designator_range_rejected": 0,
    "noop_changes_rejected": 0,
    "anchor_source_counts": {"full_text": 2}
  },
  "items": [{
    "item_id": "...", "left_block_id": "...", "right_block_id": "...",
    "left_anchors": {"available": true, "ratings": ["400a","1600a"], "sections": ["5x120"]},
    "right_anchors": {...},
    "grounded_entities_old": [{"value":"...","normalized":"...","status":"grounded","matched_values":["400a"],"missing_values":[]}],
    "grounded_entities_new": [...],
    "grounded_changes": [{"value":"...","status":"grounded","old_values":["400a"],"new_values":["200a"]}],
    "rejected_entities": [{"value":"QF7 100А","status":"rejected_artificial_series"}],
    "rejected_changes": [{"value":"... без изменений","status":"rejected_noop"}],
    "artificial_series_reasons": ["repeated_value:100ax31"],
    "warnings": []
  }],
  "warnings": []
}
```

## Сырой vision report НЕ удаляется

`build_graphic_vision_grounding_report` ТОЛЬКО читает
`graphic_vision_enrichment_report.json` + normalized models и пишет **отдельный**
артефакт. Сырой vision report, crops, prompts остаются как есть. Grounding —
это слой доверия НАД vision, а не его замена.

## Зачем это нужно перед delta/critic

Сырой vision-output — это **гипотеза**, а не факт. Пригодный слой = только
`grounded` (и опц. `weakly_grounded` как weak-confirmation для ручной проверки);
`ungrounded` — гипотеза без подтверждения; `rejected_*` подмешивать НЕЛЬЗЯ. То
есть grounding report — обязательный фильтр между сырым vision и любым
downstream'ом, который иначе получил бы галлюцинированные номиналы/диапазоны как
факты.

## Интеграция в dry-run

[3e] после graphic vision enrichment:

```text
graphic_vision_enrichment  ([3d], default OFF)
→ graphic_vision_grounding  ([3e], авто-ON если [3d] дал items с результатами)
```

* включается, только если `gv_report` содержит `items` (нечего грунтовать
  иначе); явно отключается `options.graphic_vision_grounding.enabled=false`;
* пишет `graphic_vision_grounding_report.json`, в манифест, в
  `summary.graphic_vision_grounding` + краткую секцию в `pipeline_v2_summary.md`;
* fail-soft: ошибка не валит конвейер (отражается в `error`/warnings);
* benign-warnings («no anchors for block X», «no vision items») не деградируют
  статус dry-run.

## UI payload

`pipeline_v2_ui_payload.graphic_vision_grounding` (только если слой включён):

```json
{"available": true, "status": "ok",
 "entities_grounded": 0, "entities_weakly_grounded": 0, "entities_ungrounded": 0,
 "changes_grounded": 0, "changes_rejected": 0,
 "artificial_series_rejected": 0, "noop_changes_rejected": 0}
```

Старые payload'ы без секции полностью совместимы (поле не добавляется).

## Controlled validation (offline, реальные pilot reports, без vision-вызовов)

На реальных `graphic_vision_enrichment_report.json` 3 dense-блоков ИОС 1.1.

**Recall (excerpt → full text):**

| блок | grounded | weakly | ungrounded | artificial_series | designator_range |
|---|---|---|---|---|---|
| ГРЩ excerpt | 55 | 18 | 83 | 31 | 1 |
| ГРЩ **full text** | **81** | **33** | 72 | **0** | 1 |
| ВРУ-1+4 excerpt | 3 | 3 | 59 | 0 | 11 |
| ВРУ-1+4 **full text** | **6** | **6** | 54 | 0 | **9** |

Полный текст-слой: (1) поднимает grounded/weakly recall; (2) **снимает ложные
`artificial_series`** на ГРЩ (`31→0`: `100А` реально присутствует в полном
`ocr_text`, excerpt его не видел → ложно считал «достроенным рядом»). Реальные
`SA 1600А`/`QF 2500А`/`FU 125А` НЕ отвергаются. designator-range детектор явно
отклоняет `QF1…QF100`/`KM1…KM10`/`QF1-QF1000` (раньше — silent `ungrounded`).
Артефакты — diagnostics `smoke_ios11_real_tiled_vision_grounded_*` /
`smoke_ios11_vision_grounding_recall_*`.

## Связанные файлы

* [pipeline_v2_graphic_vision_grounding.py](../backend/app/services/stage_comparison/pipeline_v2_graphic_vision_grounding.py)
* [pipeline_v2_graphic_vision_enrichment.py](../backend/app/services/stage_comparison/pipeline_v2_graphic_vision_enrichment.py) — источник vision report
* [pipeline_v2_dry_run.py](../backend/app/services/stage_comparison/pipeline_v2_dry_run.py) — стадия [3e]
* [pipeline_v2_ui_payload.py](../backend/app/services/stage_comparison/pipeline_v2_ui_payload.py) — UI summary
* [tests/test_stage_comparison_pipeline_v2_graphic_vision_grounding.py](../tests/test_stage_comparison_pipeline_v2_graphic_vision_grounding.py)

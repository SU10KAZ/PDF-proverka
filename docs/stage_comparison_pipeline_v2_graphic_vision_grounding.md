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

Anchors берутся из normalized document model блока (`prepared_ingest`), сети
нет:

* `pdfplumber_text_excerpt` — векторный текст-слой PDF (самый авторитетный для
  номиналов: `63А`, `400А`, `ППГнг 2х(5х120)`…);
* `ocr_json_summary.key_entities` — курируемый список сущностей OCR
  (`1600А, 2500А, 800А, 720А, 1250кВА, 200кВАр`…);
* `ocr_json_summary.content_summary` / `detailed_description`, `text_excerpt`;
* `stamp_data.sheet_name` / `document_code`.

`collect_block_text_anchors(block)` нормализует их в два blob'а: **spaced** (для
извлечения номиналов — нужны границы токенов) и **compact** (для подстрочного
поиска маркировок). OLD-сущности грунтуются по LEFT-блоку, NEW — по RIGHT.
Достроенные ряды детектируются против **объединённых** anchors пары (значение,
реальное на любой стороне, не считается фабрикацией).

> Anchors — это ВЫЖИМКИ (excerpt'ы, ~600–1700 символов). Значение, присутствующее
> на чертеже, но не попавшее в excerpt, даст консервативный `ungrounded`
> (не «отвергнуто»). Grounding занижает доверие, а не выдумывает уверенность.

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
| `rejected_artificial_series` | достроенный типовой ряд / искусственный повтор, не покрытый anchors |
| `rejected_noop` | old==new после нормализации («100А → 100А», «без изменений») |
| `rejected_invalid_format` | пустая/непарсабельная сущность |
| `no_anchor_available` | у блока нет текстового слоя |

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
    "artificial_series_rejected": 0, "noop_changes_rejected": 0
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

`grounded`/`weakly_grounded` дельты можно подмешивать в delta explanation как
подтверждённые визуальные изменения; `ungrounded` — только weak confirmation
для ручной проверки; `rejected_*` подмешивать НЕЛЬЗЯ. То есть grounding report —
обязательный фильтр между сырым vision и любым downstream'ом, который иначе
получил бы галлюцинированные номиналы как факты.

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

## Controlled validation (offline, реальный pilot report)

На реальном `graphic_vision_enrichment_report.json` ГРЩ-pilot (без новых vision-
вызовов): из 187 атомарных сущностей 56 grounded / 18 weak / 83 ungrounded;
**31 rejected_artificial_series** (фабрикованный `QF3…QF22 100А`), **1
rejected_noop** (`ППГнг 4x2,5 → 4x2,5`). Реальные `SA 1600А`/`QF 2500А`/`FU 125А`
НЕ отвергнуты (защищены anchor'ами). Артефакты — diagnostics
`smoke_ios11_real_tiled_vision_grsh_*` / grounding smoke.

## Связанные файлы

* [pipeline_v2_graphic_vision_grounding.py](../backend/app/services/stage_comparison/pipeline_v2_graphic_vision_grounding.py)
* [pipeline_v2_graphic_vision_enrichment.py](../backend/app/services/stage_comparison/pipeline_v2_graphic_vision_enrichment.py) — источник vision report
* [pipeline_v2_dry_run.py](../backend/app/services/stage_comparison/pipeline_v2_dry_run.py) — стадия [3e]
* [pipeline_v2_ui_payload.py](../backend/app/services/stage_comparison/pipeline_v2_ui_payload.py) — UI summary
* [tests/test_stage_comparison_pipeline_v2_graphic_vision_grounding.py](../tests/test_stage_comparison_pipeline_v2_graphic_vision_grounding.py)

# Stage Comparison Pipeline V2 — Visual Equivalence Gate

**Дата:** 2026-06-11
**Статус:** offline mark-only этап dry-run; vision НЕ запускается.
**Модуль:** [backend/app/services/stage_comparison/pipeline_v2_visual_equivalence_gate.py](../backend/app/services/stage_comparison/pipeline_v2_visual_equivalence_gate.py)
**Движок:** переиспользован проверенный cascade из
[visual_block_equivalence.py](../backend/app/services/stage_comparison/visual_block_equivalence.py)
(+ рендер блока из [block_equivalence_precheck.py](../backend/app/services/stage_comparison/block_equivalence_precheck.py)).

## Зачем

После real-runner прогонов ИОС 1.1 главный источник неопределённости —
**weak block matching графики**: критик вынужден писать «убедиться, что это
одна и та же цепь». Gate сравнивает matched graphic blocks OLD↔NEW
**попиксельно с выравниванием** ДО vision-этапа:

- блоки визуально идентичны → vision по ним не нужен
  (`exclude_from_vision`): экономия vision-вызовов и НОЛЬ
  description-variance на неизменённых схемах;
- блоки изменились → `send_to_vision` (vision работает только там, где
  есть что описывать);
- алгоритм не уверен / рендер не удался → НЕ исключаем
  (`send_to_vision` / `manual_review`) — лучше переплатить vision-вызовом,
  чем пропустить изменение.

```text
graphic_descriptor
→ visual_equivalence_gate   ← этот этап (mark-only)
→ entity_extraction → entity_diff → delta_explanation → summary/ui
```

## Статусы

| status | Значение | decision |
|---|---|---|
| `identical_visual` | после trim+ECC-выравнивания diff ниже строгих порогов, mask IoU ≥ 0.97, NCC ≥ 0.97 | `exclude_from_vision`; **анти-dilution guard**: при непустом diff_bbox и оценке абсолютного остатка > `identical_max_abs_diff_px` (default 60 px) → `manual_review` с risk-флагом `localized_residual_diff` |
| `minor_visual` | отличие в полосе «шум рендера» (2–5% пикселей); полоса реклассифицируется в gate из engine-`changed` (cascade сам minor не возвращает) | `exclude_from_vision` при confidence ≥ 0.8, иначе `manual_review` (осторожный default) |
| `changed_visual` | видимое изменение после выравнивания | `send_to_vision` |
| `uncertain` | выравнивание не сошлось, fallback не дал решения; сюда же деградирует отсутствие cv2 | `send_to_vision` (лучше не пропустить изменение) |
| `render_failed` | не удалось получить изображение блока (нет crop, битый файл, PDF недоступен) | `manual_review` |
| `skipped` | блок не найден в модели / превышен `max_pairs` cap (cap не молчит — warning) | `manual_review` |

**Почему identical не отправляется в vision:** визуальное совпадение после
выравнивания — самое сильное свидетельство «блок не менялся»; vision на нём
лишь генерирует случайные вариации описания (description-variance), которые
downstream превращаются в ложные дельты.

**Почему uncertain НЕ исключается:** неуверенность алгоритма — это не
«нет изменений»; исключение по uncertain создало бы слепую зону. Правило
gate: ложный vision-вызов дешевле пропущенного изменения.

**Анти-dilution (находка адверсариального ревью):** ratio-пороги identical
на большом блоке растворяют малую реальную правку — замена подписи
«160А»→«250А» на схеме 1000×1000 px это ~50 px (0.005% < порога 2%), движок
честно ставит identical. Поэтому gate смотрит на АБСОЛЮТНЫЙ остаток: если
diff_bbox непуст и `total_diff_ratio × площадь` > `identical_max_abs_diff_px`
(60 px) — решение понижается до `manual_review`, confidence капится 0.95,
diff_bbox сурфейсится в metrics. Рассеянный суб-пиксельный шум рендера
(diff_bbox пуст) исключается как раньше; self-compare ИОС1.1 — 66/66
identical→exclude без ложных manual.

## Mark-only

Этап только помечает (`decision` в отчёте): ничего не удаляется, downstream
(entity diff, delta explanation, vision-пайплайн) пока НЕ читает пометки.
Использование `exclude_from_vision` фактическим vision-этапом — следующий
отдельный шаг.

## Сравнение (engine cascade)

1. grayscale → line-art mask (порог 200) → trim content bbox (белые рамки
   не влияют);
2. ECC Euclidean → ECC Affine (если Euclidean не сошёлся);
3. метрики: diff-ratio по пикселям, цветовой diff (HSV S), mask IoU,
   normalized correlation, foreground ratios;
4. fallback без выравнивания: downscale 256 + IoU/NCC (только для
   уверенного `changed`/`uncertain`, НЕ для identical);
5. debug PNG-панели — опционально (`options.debug_dir`), только diagnostics.

Чистый pixel-perfect compare НЕ используется — решение всегда после
выравнивания и нормализации. Пороги — env
`STAGE_COMPARISON_VISUAL_BLOCK_EQUIVALENCE_*` (общие с движком, см. его доку).

Изображение блока: локальный raster `image_file` (если жив), иначе рендер
из исходного PDF пакета по `coords_norm` (PyMuPDF). `crop_url` НЕ
скачивается — сети нет.

## Отчёт

`visual_equivalence_gate_report.json`: `summary`
(matched/compared/по-статусам/по-решениям + `cv2_available`) и
`block_pairs[]` (pair_key, block ids, страницы, crop paths, status,
decision, confidence, metrics {mask_iou, normalized_correlation,
foreground_ratio_left/right, alignment_method, total_diff_ratio},
risk_flags (+`left/right_readiness_low|not_usable` из graphic descriptor),
reason). Confidence детерминированная: identical = min(IoU, NCC); minor —
линейная позиция в полосе шума; changed 0.9/0.7 (align/fallback);
uncertain 0.3; render_failed/skipped 0.0.

## Интеграция в dry-run

Этап 3b, fail-soft: исключение gate → warning + `visual_equivalence_gate.
status=failed` в summary, этапы 4–6 работают как раньше. Отключение:
`options={"visual_gate": {"enabled": False}}`. `pipeline_v2_summary.json`
получает секцию `visual_equivalence_gate`, `.md` — раздел
«## Visual equivalence gate»; UI payload — под-секцию
`graphic_readiness.visual_equivalence` (только при наличии секции в
summary — старые payload'ы полностью совместимы, frontend не менялся).

## Тесты

[tests/test_stage_comparison_pipeline_v2_visual_equivalence_gate.py](../tests/test_stage_comparison_pipeline_v2_visual_equivalence_gate.py)
— синтетические line-art crops (identical/changed/minor/рамка/разные
размеры), render_failed → manual_review, non-graphic игнор, low-readiness
risk-флаги, counts, max_pairs cap с warning'ом, офлайн-гарантии
(socket-monkeypatch + скан источника), интеграция dry-run (артефакт,
fail-soft, disable, summary/md counts), backward-совместимость UI payload.

## Smoke ИОС 1.1 (no-LLM, diagnostics)

См. финальный отчёт задачи: 47 matched graphic pairs — gate отработал на
реальных схемах; распределение/экономию vision см. в отчёте прогона
(`diagnostics_pipeline_v2/smoke_ios11_visual_equivalence_gate_no_llm_*`).

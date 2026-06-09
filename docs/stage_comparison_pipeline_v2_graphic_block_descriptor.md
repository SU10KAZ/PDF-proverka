# Stage Comparison Pipeline V2 — Graphic Block Descriptor (offline)

**Дата:** 2026-06-10
**Статус:** backend-only offline-слой диагностики графики. НЕ Qwen/Opus/OCR, НЕ
скачивание crop, НЕ diff, НЕ UI, НЕ замена старой логики.
**Модуль:** [backend/app/services/stage_comparison/pipeline_v2_graphic_block_descriptor.py](../backend/app/services/stage_comparison/pipeline_v2_graphic_block_descriptor.py)
**Вход:** `normalized_document_model` (этап 1) (+ опц. `block_matching_report` этапа 2).

## Зачем нужен Graphic Block Descriptor

Текстовые блоки покрываются OCR хорошо, а графика (схемы/планы/узлы) — нет: по
плотным чертежам бывает «мало отличий» не потому, что их нет, а потому что блок
плохо распознан (нет текст-слоя / нет `key_entities` / нужна vision-enrichment).
Этот слой честно диагностирует КАЖДЫЙ графический блок: что это, какой
дисциплины/системы, какие первичные токены видны, и **насколько блок вообще
пригоден для детерминированного diff**. Это даёт «светофор» полноты графики до
того, как подключать дорогой Qwen/Opus.

```text
normalized_document_model (+ optional block_matching_report)
  → для каждого графического блока:
        infer_graphic_type / infer_graphic_discipline / infer_graphic_systems
        extract_graphic_tokens / compute_graphic_geometry_metrics
        assess_graphic_diff_readiness
  → graphic_descriptor_report (descriptors + summary + matched_graphic_blocks)
```

## Человеческое описание vs машинный descriptor

«Человеческое» Qwen-описание — плавный текст («на схеме изображён щит…»). Машинный
descriptor — структурный и диагностический: фиксированные поля (graphic_type,
discipline, systems, buckets токенов), геометрия, источники (`crop_url`/текст-слой/
`key_entities`), оценка `diff_readiness` с `score`/`reasons`/`recommended_next_step`
и `quality_flags`. Он не пересказывает картинку, а отвечает «что это и можно ли с
этим детерминированно работать».

## Какие блоки графические

Блок обрабатывается, если: `block_type == "image"`, ИЛИ `semantic_type ∈
{scheme, large_scheme, plan}`, ИЛИ есть `crop_url`/`has_crop_pdf`, ИЛИ
`ocr_json_summary` (content_summary/key_entities) указывает на схему/план/чертёж.
Обычные текстовые блоки пропускаются.

## Используемые поля normalized model

`block_type`, `semantic_type`, `coords_norm`/`coords_px`, `shape_type`,
`crop_url`/`image_file`/`has_*`, `ocr_json_summary` (`content_summary`,
`detailed_description`, `key_entities`), `text_excerpt`,
`pdfplumber_text_excerpt`, `stamp_data.sheet_name`, `document_code`. Никакие
картинки не читаются, `crop_url` не скачивается.

## graphic_type

`structural_scheme`, `single_line_scheme`, `plan`, `cabinet_scheme`,
`connection_scheme`, `stamp`, `legend`, `table_scheme`, `unknown`. Определяется
по ключевым словам (с нормализацией lower+ё→е) в приоритетном порядке: штамп →
легенда → таблица-схема → шкаф (`шкаф/патч-панель/кросс/RJ45`) → однолинейная
(`ГРЩ/ВРУ/QF/QS/АВР/фидер/шинопровод/однолинейн/кВА/кВт`) → структурная
(`структурная схема/СОВ/СОТ/СКУД/ШК.СВН/УЭРМ/коммутатор`) → подключения
(`подключение/Ethernet/Ввод ~…В`) → план (`план/помещение/этаж`).

## discipline / systems

`discipline` — один код (`EOM|SS|SKUD|SOV|SOT|KR|AR|OV|VK|unknown`); `systems` —
список (`СКУД/СОВ/СОТ/СС/ЭОМ`). Несколько слаботочных систем (или общий «СС») →
discipline `SS`; ровно одна специфичная → `SKUD/SOV/SOT`; электрика → `EOM`;
далее KR/AR/OV/VK по маркерам. Например `discipline=SS, systems=["СОВ","СКУД"]`.

## Токены

Buckets (дедуплицированы внутри блока): `equipment`, `cables`, `power`,
`locations`, `floors`, `systems`, `connection_hints`, `raw_key_entities`. Каждый
`key_entity` классифицируется (`classify_graphic_token`), а текст
(content_summary/detailed/excerpt/sheet_name — **без** key_entities, чтобы не
фрагментировать) дополнительно сканируется regex'ами. Кабели/питание
канонизируются (`220 В`/`220В`→`220в`); локали (`Корпус N`/`Секция N`/`УЭРМ`/
`паркинг`/`ИТП`) и этажи (`-2 этаж`/`16 этаж`/`последний этаж`) — по паттернам;
connection hints — `подключается/Ethernet/Ввод ~…В/«к/от <узел>»`.

## diff_readiness

`assess_graphic_diff_readiness` считает `score` (0..1) по сигналам: есть crop/
image_file (+0.25), есть текст-слой/summary (+0.20), есть key_entities (+0.20),
≥3 значимых токена (+0.25) / ≥1 (+0.10), semantic scheme/plan (+0.10). Бэнды:
`high≥0.70`, `medium≥0.45`, `low` иначе; `not_usable` — image-блок без crop/
текст-слоя/токенов/key_entities. `usable_for_diff` = readiness ∈ {high,medium}.
`recommended_next_step`: high→`deterministic_diff`, medium→`entity_extraction`,
low→`vision_enrichment` (если есть crop) иначе `manual_review`, not_usable→
`vision_enrichment`/`manual_review`.

`quality_flags`: `graphic_without_crop`, `graphic_without_text_layer`,
`graphic_without_key_entities`, `low_token_count`, `stamp_like_graphic`,
`unknown_graphic_type`, `unknown_discipline`, `needs_vision_enrichment`,
`manual_review_recommended`, `large_dense_graphic`.

## matched_graphic_blocks (опц.)

При наличии `block_matching_report` (+ counterpart-модель) `describe_matched_
graphic_blocks` оценивает совместимость пар (НЕ diff): `match_quality` (из
confidence этапа 2), `graphic_type_match`, `discipline_match`, `token_overlap`
(Jaccard по equipment/cables/power/locations), `risk_flags`
(`graphic_type_mismatch`, `discipline_mismatch`, `low_token_overlap`,
`one_side_not_usable`, `missing_descriptor`, `weak_block_match`).

## Формат отчёта

`build_graphic_descriptor_report(model, block_matching_report=None, side=None,
options=None)` → `document` (code/pages_total/blocks_total), `summary`
(`graphic_blocks_total`, `usable_for_diff_total`, `needs_vision_enrichment_total`,
`manual_review_recommended_total`, `by_graphic_type`, `by_discipline`,
`by_readiness`, `warnings_count`), `descriptors[]`, `matched_graphic_blocks[]`
(если в `options["counterpart_model"]` передана вторая модель), `warnings[]`.
Запись — `write_graphic_descriptor_report` (атомарно).

## Что этот этап НЕ делает

- **НЕ** вызывает Qwen/Opus/LLM/OCR и **НЕ** скачивает `crop_url`/не рендерит PDF;
- **НЕ** делает diff (только оценивает готовность к нему);
- **НЕ** ходит в сеть (stdlib only: `json/os/re/tempfile/unicodedata/collections/
  pathlib/typing`);
- **НЕ** трогает старую логику, runtime comparison data, `.env`, deploy, backend.

## Как это помогает понять «мало отличий по плотным схемам»

Если по тяжёлой схеме diff пуст, descriptor покажет причину: `readiness=low/
not_usable` + `graphic_without_key_entities`/`graphic_without_text_layer` +
`large_dense_graphic` + `recommended_next_step=vision_enrichment`. Значит дело не
в «нет изменений», а в слабом распознавании — блок надо прогнать через
vision-enrichment, а не доверять пустому тексту. Это честный сигнал ещё до LLM.

## Тесты

[tests/test_stage_comparison_pipeline_v2_graphic_block_descriptor.py](../tests/test_stage_comparison_pipeline_v2_graphic_block_descriptor.py)
— synthetic модели/отчёт: descriptor для image-схемы, классификация
structural/single_line/cabinet, discipline+systems для СКУД/СОВ/СОТ, дедуп
equipment/cables, нормализация power, locations/floors, connection hints,
geometry-метрики, low/not_usable readiness + флаги, high/medium readiness,
matched token overlap, mismatch risk-флаги, summary-счётчики, атомарная запись,
отсутствие сети/LLM-импортов и сквозная интеграция `result_json → normalize →
match → build_graphic_descriptor_report`.

## Интеграция в Dry Run (готово)

Graphic Descriptor уже подключён к
[Dry Run / Orchestrator](stage_comparison_pipeline_v2_dry_run.md): между block
matching и entity extraction dry-run пишет `left_graphic_descriptor_report.json`,
`right_graphic_descriptor_report.json` и `graphic_descriptor_matched_report.json`,
добавляет секцию `graphic_descriptor` в `pipeline_v2_summary.json` и раздел
«Graphic readiness» со светофором в `pipeline_v2_summary.md`. Подключение
fail-soft: падение descriptor не валит обязательные этапы 1–2/4–5.

## Следующий блок

- **`LLM Delta Explanation / Critic`** — точечный LLM (через `claude -p`,
  fail-soft) объясняет/проверяет уже найденные deterministic deltas (этап 4),
  приоритет дельтам `needs_human_review`; descriptor подсказывает, где источник —
  слабая графика, а не отсутствие изменений.

## Связанные файлы

- [pipeline_v2_graphic_block_descriptor.py](../backend/app/services/stage_comparison/pipeline_v2_graphic_block_descriptor.py)
- [pipeline_v2_prepared_ingest.py](../backend/app/services/stage_comparison/pipeline_v2_prepared_ingest.py) — этап 1 (вход, `ocr_json_summary`)
- [pipeline_v2_block_matching.py](../backend/app/services/stage_comparison/pipeline_v2_block_matching.py) — этап 2 (matched pairs)
- [pipeline_v2_entity_extraction.py](../backend/app/services/stage_comparison/pipeline_v2_entity_extraction.py) — этап 3 (родственная экстракция)
- [pipeline_v2_dry_run.py](../backend/app/services/stage_comparison/pipeline_v2_dry_run.py) — оркестратор (точка будущей интеграции)

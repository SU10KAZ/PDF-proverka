# Stage Comparison Pipeline V2 — Dry Run / Orchestrator (этапы 1–5)

**Дата:** 2026-06-10
**Статус:** backend-only offline-оркестратор. НЕ UI, НЕ Opus/critic, НЕ замена
старой логики Stage Comparison.
**Модуль:** [backend/app/services/stage_comparison/pipeline_v2_dry_run.py](../backend/app/services/stage_comparison/pipeline_v2_dry_run.py)
**Связывает:** [этап 1 — Ingest](stage_comparison_pipeline_v2_prepared_package_ingest.md) · [этап 2 — Block Matching](stage_comparison_pipeline_v2_block_matching.md) · [Graphic Descriptor](stage_comparison_pipeline_v2_graphic_block_descriptor.md) · [этап 3 — Entity Extraction](stage_comparison_pipeline_v2_entity_extraction.md) · [этап 4 — Entity Diff](stage_comparison_pipeline_v2_entity_diff.md)

## Зачем нужен dry-run orchestrator

Этапы Pipeline V2 — изолированные чистые слои. Чтобы прогнать их как единый
offline конвейер на подготовленной паре OLD/NEW и получить все промежуточные
артефакты для инспекции (без UI, без сети, без LLM), нужен один входной вызов.
Dry-run оркестратор именно это и делает: принимает два подготовленных пакета, по
очереди запускает ingest → block matching → **graphic descriptor** → entity
extraction → entity diff, пишет каждый артефакт на диск и собирает сводку +
манифест.

```text
left_package / right_package
  → [1] build_normalized_document_model        → *_normalized_document_model.json
  → [2] match_normalized_documents             → block_matching_report.json
  → [3] build_graphic_descriptor_report (×2)   → left/right_graphic_descriptor_report.json
        describe_matched_graphic_blocks        → graphic_descriptor_matched_report.json
  → [4] extract_entities_for_matched_documents → entity_extraction_report.json
  → [5] diff_entity_extraction_report          → entity_diff_report.json
  → pipeline_v2_summary.json + .md + pipeline_v2_manifest.json
```

Оркестратор **переиспользует существующие функции и writer'ы** этапов — формат их
артефактов не меняется. **Graphic Descriptor** вставлен между block matching и
entity extraction как вспомогательный «светофор» готовности графики (см. ниже);
он fail-soft и НЕ скачивает crop, НЕ вызывает Qwen/Opus.

## Входные пакеты

`left_package` (OLD) и `right_package` (NEW) — словари:

```json
{
  "pdf_path": "optional.pdf",
  "result_json_path": "required_result.json",
  "document_md_path": "optional_document.md",
  "ocr_html_path": "optional_ocr.html"
}
```

Обязателен только `result_json_path`. Bare-строка трактуется как
`result_json_path`. Отсутствующий optional-файл не валит прогон: его «provided/
exists» отражаются в `inputs`, а указанный-но-несуществующий путь даёт warning.

## Артефакты (в `out_dir`)

```text
left_normalized_document_model.json     # этап 1 (OLD)
right_normalized_document_model.json    # этап 1 (NEW)
block_matching_report.json              # этап 2
left_graphic_descriptor_report.json     # graphic descriptor (OLD)
right_graphic_descriptor_report.json    # graphic descriptor (NEW)
graphic_descriptor_matched_report.json  # graphic descriptor (matched pairs)
entity_extraction_report.json           # этап 3
entity_diff_report.json                 # этап 4
pipeline_v2_summary.json                # сводка (машиночитаемая)
pipeline_v2_summary.md                  # сводка (человекочитаемая)
pipeline_v2_manifest.json               # манифест артефактов
```

Все записи атомарны (tmp + `os.replace`) — частично записанного broken JSON не
остаётся. `left/right_graphic_descriptor_report.json` имеют kind
`stage_comparison_pipeline_v2_graphic_block_descriptor`; matched-обёртка —
`stage_comparison_pipeline_v2_graphic_descriptor_matched`.

## Graphic readiness («светофор» графики)

Dry-run пишет графические descriptor-артефакты и добавляет в summary секцию
`graphic_descriptor`: сколько графических блоков с каждой стороны, сколько
пригодно для diff (`*_usable_for_diff_total`), сколько требует vision-enrichment
(`*_needs_vision_enrichment_total`) / ручной проверки
(`*_manual_review_recommended_total`), `by_graphic_type`/`by_discipline`/
`by_readiness`, а также метрики matched-пар (`matched_graphic_blocks_total`,
`matched_low_token_overlap_total`, `matched_one_side_not_usable_total`,
`matched_*_mismatch_total`).

В `pipeline_v2_summary.md` появляется раздел **«## Graphic readiness»** со
светофором:

- 🟢 графика пригодна для deterministic diff;
- 🟡 есть блоки, которым нужен vision enrichment;
- 🔴 есть графические блоки, которые нельзя уверенно сравнить (ручная проверка).

Это честный ответ на вопрос «почему по плотной схеме diff пустой»: если блок
`not_usable`/`needs_vision_enrichment`, пустой diff вызван слабым распознаванием,
а не отсутствием изменений. Graphic descriptor **не скачивает crop и не вызывает
Qwen/Opus** — только диагностика по уже имеющимся полям.

## Summary

`pipeline_v2_summary.json` (`kind=stage_comparison_pipeline_v2_dry_run_summary`):
`status` (`ok|completed_with_warnings|failed`), `artifacts` (имена файлов),
`inputs.left/right` (пути + provided/exists), `stages` с компактными счётчиками
каждого этапа (prepared_ingest / block_matching / entity_extraction /
entity_diff), отдельная секция **`graphic_descriptor`** (см. «Graphic
readiness»), агрегированные `warnings`, `next_recommended_stage`
(`delta_explanation`). При падении — поле `error`.

`pipeline_v2_summary.md` — человекочитаемо: статус, входные файлы, страниц/блоков
обработано и сопоставлено, сущностей извлечено, дельт найдено (added/removed/
changed/uncertain + уверенность), top-10 warnings, список артефактов и вывод
(«✅ готово к LLM explanation/critic» / «⚠ проверьте warnings» / «❌ устраните
ошибку»).

## Manifest

`pipeline_v2_manifest.json` (`kind=stage_comparison_pipeline_v2_manifest`) — для
каждого из 10 артефактов (кроме самого манифеста): `key`, `filename`,
`relative_path`, `exists`, `size_bytes`, `sha256`, `kind` (вычитан из JSON).
Несуществующие (если этап не дошёл / graphic descriptor упал) перечислены с
`exists=false`.

## Статусы и fail-soft

- `failed` — нет/не найден `result_json_path` ИЛИ исключение в обязательном
  этапе (ingest / block matching / entity extraction / entity diff). Уже
  записанные артефакты остаются валидными (атомарность), последующие не пишутся,
  в summary — короткая `error` (`Тип: сообщение`), `status=failed`; summary и
  manifest всё равно записываются.
- `completed_with_warnings` — все обязательные этапы прошли, но есть warnings
  (из артефактов этапов, указанных-но-отсутствующих optional-файлов и/или
  **ошибки graphic descriptor**).
- `ok` — все этапы прошли без warnings.

**Graphic Descriptor fail-soft.** Graphic descriptor — вспомогательный, поэтому
его падение НЕ валит обязательные этапы: ошибка ловится отдельным `try`,
графические артефакты при сбое не пишутся (битого JSON нет), в `warnings` и в
`graphic_descriptor.error` добавляется короткое сообщение, а `status`
понижается до `completed_with_warnings` (если этапы 1–2/4–5 прошли). Обязательные
этапы 4–5 (entity extraction / diff) выполняются даже после сбоя графики.

Оркестратор не роняет процесс: исключение обязательного этапа ловится,
превращается в `status=failed` + `error`.

## Что этот этап НЕ делает

- **НЕ** вызывает Qwen/Opus/LLM, **НЕ** скачивает `crop_url`, **НЕ** ходит в сеть;
- **НЕ** строит UI и не подключается к API;
- **НЕ** меняет старую логику Stage Comparison и форматы артефактов этапов 1–4;
- **НЕ** формулирует строительное/нормативное влияние (это будущий LLM-этап).

Импорты — только stdlib (`hashlib/json/os/tempfile/pathlib/typing`) + чистые
функции этапов 1–4.

## Почему это нужно перед LLM explanation/critic и перед UI

- единая точка прогона даёт воспроизводимый набор детерминированных артефактов,
  которые можно глазами проверить ещё до LLM/UI;
- `entity_diff_report.deltas` + `pipeline_v2_summary` — готовый, заякоренный вход
  для точечного LLM-объяснения/critic (LLM не ищет отличия по всему тому);
- summary/manifest дают «светофор» (`ok`/`warnings`/`failed`): передавать дальше
  в LLM/UI или сначала чинить вход;
- UI впоследствии может просто читать эти артефакты, не вызывая конвейер inline.

## Функции

`run_pipeline_v2_dry_run(left_package, right_package, out_dir, options=None)` —
главный вход (возвращает summary-словарь). Вспомогательные:
`normalize_package_paths`, `build_pipeline_v2_artifact_paths`,
`build_pipeline_v2_summary`, `write_pipeline_v2_summary_json`,
`write_pipeline_v2_summary_md`, `write_pipeline_v2_manifest`. `options` может
нести под-опции `matching`/`extraction`/`diff` для соответствующих этапов.

## Тесты

[tests/test_stage_comparison_pipeline_v2_dry_run.py](../tests/test_stage_comparison_pipeline_v2_dry_run.py)
— synthetic пакеты в `tmp_path`: создание всех 8 артефактов, статус
ok/with-warnings, ключевые счётчики в MD, sha256+размеры в манифесте,
отсутствие optional md/html без падения, `failed` при отсутствующем/несуществующем
`result_json_path`, ожидаемые changed (стадия П→Р) и added (видеорегистратор)
дельты, согласованность счётчиков summary с вложенными отчётами, отсутствие
сети/LLM-импортов и сохранение `kind` артефактов (переиспользование writer'ов).

## Следующий блок

- **`pipeline_v2_delta_explanation` (LLM Delta Explanation / Critic)** — начать
  объяснять уже найденные deterministic deltas с учётом graphic readiness:
  точечный LLM (через `claude -p`, fail-soft) комментирует смысл/влияние
  конкретной дельты и/или critic проверяет её грунтованность — **без поиска
  отличий по всему тому**, приоритет дельтам `needs_human_review`. Секция
  `graphic_descriptor` подсказывает, где пустой diff вызван слабой графикой
  (нужен vision-enrichment), а не отсутствием изменений.

> Graphic Descriptor уже интегрирован в dry-run (этот PR).

## Связанные файлы

- [pipeline_v2_dry_run.py](../backend/app/services/stage_comparison/pipeline_v2_dry_run.py)
- [pipeline_v2_graphic_block_descriptor.py](../backend/app/services/stage_comparison/pipeline_v2_graphic_block_descriptor.py) — Graphic Descriptor
- [pipeline_v2_entity_diff.py](../backend/app/services/stage_comparison/pipeline_v2_entity_diff.py) — этап 4
- [pipeline_v2_entity_extraction.py](../backend/app/services/stage_comparison/pipeline_v2_entity_extraction.py) — этап 3
- [pipeline_v2_block_matching.py](../backend/app/services/stage_comparison/pipeline_v2_block_matching.py) — этап 2
- [pipeline_v2_prepared_ingest.py](../backend/app/services/stage_comparison/pipeline_v2_prepared_ingest.py) — этап 1

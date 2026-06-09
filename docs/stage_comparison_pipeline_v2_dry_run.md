# Stage Comparison Pipeline V2 — Dry Run / Orchestrator (этапы 1–4)

**Дата:** 2026-06-10
**Статус:** backend-only offline-оркестратор. НЕ UI, НЕ Opus/critic, НЕ замена
старой логики Stage Comparison.
**Модуль:** [backend/app/services/stage_comparison/pipeline_v2_dry_run.py](../backend/app/services/stage_comparison/pipeline_v2_dry_run.py)
**Связывает:** [этап 1 — Ingest](stage_comparison_pipeline_v2_prepared_package_ingest.md) · [этап 2 — Block Matching](stage_comparison_pipeline_v2_block_matching.md) · [этап 3 — Entity Extraction](stage_comparison_pipeline_v2_entity_extraction.md) · [этап 4 — Entity Diff](stage_comparison_pipeline_v2_entity_diff.md)

## Зачем нужен dry-run orchestrator

Этапы 1–4 — изолированные чистые слои. Чтобы прогнать их как единый offline
конвейер на подготовленной паре OLD/NEW и получить все промежуточные артефакты
для инспекции (без UI, без сети, без LLM), нужен один входной вызов. Dry-run
оркестратор именно это и делает: принимает два подготовленных пакета, по очереди
запускает этапы 1→2→3→4, пишет каждый артефакт на диск и собирает сводку +
манифест.

```text
left_package / right_package
  → [1] build_normalized_document_model       → *_normalized_document_model.json
  → [2] match_normalized_documents            → block_matching_report.json
  → [3] extract_entities_for_matched_documents → entity_extraction_report.json
  → [4] diff_entity_extraction_report         → entity_diff_report.json
  → pipeline_v2_summary.json + .md + pipeline_v2_manifest.json
```

Оркестратор **переиспользует существующие функции и writer'ы** этапов 1–4 —
формат их артефактов не меняется.

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
entity_extraction_report.json           # этап 3
entity_diff_report.json                 # этап 4
pipeline_v2_summary.json                # сводка (машиночитаемая)
pipeline_v2_summary.md                  # сводка (человекочитаемая)
pipeline_v2_manifest.json               # манифест артефактов
```

Все записи атомарны (tmp + `os.replace`) — частично записанного broken JSON не
остаётся.

## Summary

`pipeline_v2_summary.json` (`kind=stage_comparison_pipeline_v2_dry_run_summary`):
`status` (`ok|completed_with_warnings|failed`), `artifacts` (имена файлов),
`inputs.left/right` (пути + provided/exists), `stages` с компактными счётчиками
каждого этапа (prepared_ingest / block_matching / entity_extraction /
entity_diff), агрегированные `warnings`, `next_recommended_stage`. При падении —
поле `error`.

`pipeline_v2_summary.md` — человекочитаемо: статус, входные файлы, страниц/блоков
обработано и сопоставлено, сущностей извлечено, дельт найдено (added/removed/
changed/uncertain + уверенность), top-10 warnings, список артефактов и вывод
(«✅ готово к LLM explanation/critic» / «⚠ проверьте warnings» / «❌ устраните
ошибку»).

## Manifest

`pipeline_v2_manifest.json` (`kind=stage_comparison_pipeline_v2_manifest`) — для
каждого из 7 артефактов (кроме самого манифеста): `key`, `filename`,
`relative_path`, `exists`, `size_bytes`, `sha256`, `kind` (вычитан из JSON).
Несуществующие (если этап не дошёл) перечислены с `exists=false`.

## Статусы и fail-soft

- `failed` — нет/не найден `result_json_path` ИЛИ исключение в одном из этапов.
  Уже записанные артефакты остаются валидными (атомарность), последующие не
  пишутся, в summary — короткая `error` (`Тип: сообщение`), `status=failed`;
  summary и manifest всё равно записываются.
- `completed_with_warnings` — все этапы прошли, но есть warnings (из артефактов
  этапов и/или указанные-но-отсутствующие optional-файлы).
- `ok` — все этапы прошли без warnings.

Оркестратор не роняет процесс: исключение этапа ловится, превращается в
`status=failed` + `error`.

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

## Следующий блок (на выбор)

- **`pipeline_v2_graphic_block_descriptor`** — усилить графику: детерминированный
  дескриптор image/scheme-блока (геометрия, плотность, наличие текст-слоя/crop,
  «насколько блок пригоден для diff»), чтобы повысить полноту сущностей на
  плотных схемах ещё до LLM.
- **`pipeline_v2_delta_explanation`** — начать объяснять уже найденные
  deterministic deltas: точечный LLM (через `claude -p`, fail-soft) комментирует
  смысл/влияние конкретной дельты и/или critic проверяет её грунтованность —
  **без поиска отличий по всему тому**, приоритет дельтам `needs_human_review`.

## Связанные файлы

- [pipeline_v2_dry_run.py](../backend/app/services/stage_comparison/pipeline_v2_dry_run.py)
- [pipeline_v2_entity_diff.py](../backend/app/services/stage_comparison/pipeline_v2_entity_diff.py) — этап 4
- [pipeline_v2_entity_extraction.py](../backend/app/services/stage_comparison/pipeline_v2_entity_extraction.py) — этап 3
- [pipeline_v2_block_matching.py](../backend/app/services/stage_comparison/pipeline_v2_block_matching.py) — этап 2
- [pipeline_v2_prepared_ingest.py](../backend/app/services/stage_comparison/pipeline_v2_prepared_ingest.py) — этап 1

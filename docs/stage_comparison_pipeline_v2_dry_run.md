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
  → [3b] run_visual_equivalence_gate          → visual_equivalence_gate_report.json
         (mark-only наложение matched graphic blocks ДО vision; fail-soft;
         см. stage_comparison_pipeline_v2_visual_equivalence_gate.md)
  → [3c] build_block_link_preview             → block_link_preview_report.json
         (read-only витрина предложенных связей для UI «Связь блоков»;
         fail-soft; см. stage_comparison_pipeline_v2_block_link_preview.md)
  → [3d] run_graphic_vision_enrichment        → graphic_vision_enrichment_report.json
         (vision-описание send_to_vision/manual_review блоков; default OFF;
         vision_runner ИНЪЕКТИРУЕТСЯ (None → skipped_no_runner); fail-soft;
         см. stage_comparison_pipeline_v2_graphic_vision_enrichment.md)
  → [3e] build_graphic_vision_grounding_report → graphic_vision_grounding_report.json
         (проверка vision-результата по anchor-тексту блока: grounded/weakly/
         ungrounded, снятие достроенных рядов и no-op; авто-ON если [3d] дал
         items с результатами; сырой vision report НЕ меняется; fail-soft;
         см. stage_comparison_pipeline_v2_graphic_vision_grounding.md)
  → [4] extract_entities_for_matched_documents → entity_extraction_report.json
  → [5] diff_entity_extraction_report          → entity_diff_report.json
  → [6] explain_entity_diff_report             → delta_explanation_report.json
  → pipeline_v2_summary.json + .md + pipeline_v2_manifest.json
```

Оркестратор **переиспользует существующие функции и writer'ы** этапов — формат их
артефактов не меняется. **Graphic Descriptor** вставлен между block matching и
entity extraction как вспомогательный «светофор» готовности графики; **Delta
Explanation / Critic** — после entity diff как LLM-объяснение готовых дельт (см.
ниже). Оба fail-soft и НЕ скачивают crop, НЕ вызывают Qwen/Opus.

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
visual_equivalence_gate_report.json     # visual gate (mark-only, до vision)
block_link_preview_report.json          # block link preview (read-only, UI «Связь блоков»)
graphic_vision_enrichment_report.json   # graphic vision enrichment (default OFF)
graphic_vision_grounding_report.json    # vision grounding по anchor-тексту (авто-ON при [3d])
entity_extraction_report.json           # этап 3
entity_diff_report.json                 # этап 4
delta_explanation_report.json           # delta explanation / critic
pipeline_v2_summary.json                # сводка (машиночитаемая)
pipeline_v2_summary.md                  # сводка (человекочитаемая)
pipeline_v2_manifest.json               # манифест артефактов
```

Все записи атомарны (tmp + `os.replace`) — частично записанного broken JSON не
остаётся. `left/right_graphic_descriptor_report.json` имеют kind
`stage_comparison_pipeline_v2_graphic_block_descriptor`; matched-обёртка —
`stage_comparison_pipeline_v2_graphic_descriptor_matched`;
`delta_explanation_report.json` — `stage_comparison_pipeline_v2_delta_explanation`.

## Delta explanation / critic (LLM-слой, offline по умолчанию)

После entity diff dry-run прогоняет `explain_entity_diff_report(...)` по выбранным
дельтам (этап 4) и пишет `delta_explanation_report.json`. **По умолчанию реальный
LLM не подключён:** `run_pipeline_v2_dry_run(..., llm_runner=None)` →
объяснения `skipped_no_runner`. Это нормальное offline-поведение: артефакт
создаётся, но объяснения не выполняются. Реальный LLM запускается **только** через
внешний инъектированный `llm_runner` (controlled smoke), здесь он не создаётся.
Готовый постоянный runner — `build_pipeline_v2_claude_runner()` из
[pipeline_v2_llm_runner.py](../backend/app/services/stage_comparison/pipeline_v2_llm_runner.py)
(см. [stage_comparison_pipeline_v2_llm_runner.md](stage_comparison_pipeline_v2_llm_runner.md));
default dry-run это НЕ меняет.

`llm_runner` — параметр `run_pipeline_v2_dry_run` (backward-compatible, default
`None`). Опции — `options["delta_explanation"]`:

```json
{"enabled": true, "mode": "explain_and_critic",
 "selection_strategy": "priority_only", "max_deltas": 20,
 "include_high_confidence": false}
```

`enabled=false` → этап не запускается, артефакт не пишется, в summary
`delta_explanation.status="disabled"`.

В summary добавляется секция `delta_explanation` (`enabled`, `status`
[`skipped_no_runner|completed|completed_with_warnings|failed|disabled`],
`selected_total`/`explained_total`/`needs_human_review_total`/
`possible_ocr_noise_total`/`possible_weak_graphic_total`/`coverage_notes_total`
+ `by_risk_level`/`by_status`), а в `.md` — раздел **«## Delta explanation /
critic»** с честным выводом «LLM explanation не запускался: runner не передан».

**Инвариант:** LLM НЕ ищет отличия по всему тому и НЕ добавляет дельты — только
объясняет/проверяет уже найденные deterministic deltas (см.
[delta explanation](stage_comparison_pipeline_v2_delta_explanation.md)).
`coverage_notes` по слабой графике строятся даже без runner'а.

## Delta sections (секционирование отчёта)

Summary делит ОБЪЯСНЁННЫЕ дельты на секции для инженера/генподрядчика —
JSON-секция `delta_sections` + раздел **«## Delta sections»** в `.md`
(заголовки ✅/🟡/🟠/⚪/🔴, count + до 10 компактных примеров на секцию).
Это **offline-report, не портал UI**; selection/prompt/diff не меняются,
backend restart не нужен.

Каждая дельта попадает ровно в ОДНУ главную секцию по приоритету:

| Приоритет | Секция | Критерий |
|---|---|---|
| 1 | `llm_failed_or_skipped` 🔴 | status `failed`/`skipped_no_runner`, `model.raw_status != ok` или флаг `llm_response_parse_failed` (нечитаемый ответ LLM = «объяснения нет»); с `llm_runner=None` сюда уходят ВСЕ selected |
| 2 | `likely_noise_hidden_by_default` ⚪ | вердикт `reject` (критик отверг дельту) ИЛИ `possible_ocr_noise` И (show=false ИЛИ risk=none) — типографика/OCR-артефакты/отвергнутое, скрывать по умолчанию |
| 3 | `weak_graphic_review` 🟠 | `possible_weak_graphic` или graphic-контекст блока слабый (needs_vision_enrichment / readiness low/not_usable) — слабая графика бьёт даже accept |
| 4 | `needs_review` 🟡 | `needs_human_review` (вердикт/флаг/статус); сюда же `possible_ocr_noise` с show=true И risk≠none — инженер взглянет |
| 5 | `confirmed_changes` ✅ | `accept` + show=true + groundedness grounded/partially_grounded |

Неклассифицированное — fallback в `needs_review` (safe default). Каждая
секция: `{count, delta_ids, description, examples[≤10]}`; пример: entity/
delta type, subject/field, old→new (обрезка до 60 симв.), critic verdict,
risk, show. `delta_sections.coverage_notes` — счётчики
`{count, weak_graphic, matched_risk}`.

Как читать: **confirmed** — можно показывать как подтверждённые изменения;
**needs_review** — очередь ручной проверки; **weak_graphic** — сперва
vision-доработка/ручной просмотр листа; **noise** — скрыто по умолчанию
(показывать по тумблеру); **failed/skipped** — explanation не выполнялся
(offline-режим) или упал. Валидация на real АР2 changed_only: 3 confirmed
(ИНПАД/Бернер/дата), 1 needs_review (пробелы в шифре, ocr_noise+show=true),
1 noise (кавычки `"…"`→`«…»`, show=false), 0 weak/failed.

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
каждого из 11 артефактов (кроме самого манифеста): `key`, `filename`,
`relative_path`, `exists`, `size_bytes`, `sha256`, `kind` (вычитан из JSON).
Несуществующие (если этап не дошёл / graphic descriptor / delta explanation упали
или delta explanation `disabled`) перечислены с `exists=false`.

## Статусы и fail-soft

- `failed` — нет/не найден `result_json_path` ИЛИ исключение в обязательном
  этапе (ingest / block matching / entity extraction / entity diff). Уже
  записанные артефакты остаются валидными (атомарность), последующие не пишутся,
  в summary — короткая `error` (`Тип: сообщение`), `status=failed`; summary и
  manifest всё равно записываются.
- `completed_with_warnings` — все обязательные этапы прошли, но есть warnings
  (из артефактов этапов, указанных-но-отсутствующих optional-файлов и/или
  **ошибки graphic descriptor / delta explanation**).
- `ok` — все этапы прошли без warnings.

**Graphic Descriptor / Delta Explanation fail-soft.** Оба вспомогательны, поэтому
их падение НЕ валит обязательные этапы: ошибка ловится отдельным `try`, их
артефакты при сбое не пишутся (битого JSON нет), в `warnings` и в
`graphic_descriptor.error` / `delta_explanation.error` добавляется сообщение, а
`status` понижается до `completed_with_warnings` (если обязательные этапы прошли).
Обязательные этапы 4–5 (entity extraction / diff) выполняются даже после сбоя
графики; delta explanation идёт последним. Бенайн `skipped_no_runner` (offline,
runner не передан) НЕ повышается до dry-run warning — это ожидаемое поведение.

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

- **Controlled smoke на одном маленьком реальном prepared package** — прогнать
  полный dry-run на одной небольшой паре `pdf+result.json+md+ocr` (без
  production/deploy): сначала `llm_runner=None` (всё offline, `skipped_no_runner`),
  затем отдельным разрешённым запуском с fake/real runner — проверить качество
  объяснений/critic на живых дельтах и корректность `coverage_notes` по графике.
  Это первый момент реального вызова LLM — выполнять осознанно, вне
  `pf06effb7`/`p9692b6b5`, не трогая runtime comparison data.

> Graphic Descriptor и Delta Explanation уже интегрированы в dry-run (offline,
> `llm_runner=None` по умолчанию).

## Этап [5b] grounded_evidence (2026-06-11)

После entity-diff [5] и перед delta-explanation [6] добавлен опциональный этап
**grounded_evidence** (mark-only):

```text
graphic_vision_grounding [3e] → entity_diff [5] → grounded_evidence [5b] → delta_explanation [6]
```

* связывает дельты с grounded vision (`build_grounded_evidence_report`), пишет
  `grounded_evidence_report.json`, добавлен в `_ARTIFACT_FILENAMES` и манифест;
* включается, только если `gvg_report.items` непуст (нечего связывать иначе);
  отключается `options.grounded_evidence.enabled=false`;
* fail-soft: падение не валит pipeline (`grounded_evidence.error` в summary,
  benign `skipped_no_grounding` не деградирует статус);
* `grounded_evidence_report` прокидывается в `explain_entity_diff_report` →
  per-delta grounded/weak записи попадают в prompt как supporting/weak evidence;
* summary получает секцию `grounded_evidence` (`deltas_with_grounded_evidence`,
  `deltas_with_weak_evidence`, `deltas_without_evidence`,
  `deltas_with_rejected_conflicts`, `*_links`).

`rejected_*` / `ungrounded` НИКОГДА не факт. Подробно —
[stage_comparison_pipeline_v2_grounded_evidence.md](stage_comparison_pipeline_v2_grounded_evidence.md).

## Связанные файлы

- [pipeline_v2_dry_run.py](../backend/app/services/stage_comparison/pipeline_v2_dry_run.py)
- [pipeline_v2_grounded_evidence.py](../backend/app/services/stage_comparison/pipeline_v2_grounded_evidence.py) — этап [5b]
- [pipeline_v2_delta_explanation.py](../backend/app/services/stage_comparison/pipeline_v2_delta_explanation.py) — Delta Explanation / Critic
- [pipeline_v2_graphic_block_descriptor.py](../backend/app/services/stage_comparison/pipeline_v2_graphic_block_descriptor.py) — Graphic Descriptor
- [pipeline_v2_entity_diff.py](../backend/app/services/stage_comparison/pipeline_v2_entity_diff.py) — этап 4
- [pipeline_v2_entity_extraction.py](../backend/app/services/stage_comparison/pipeline_v2_entity_extraction.py) — этап 3
- [pipeline_v2_block_matching.py](../backend/app/services/stage_comparison/pipeline_v2_block_matching.py) — этап 2
- [pipeline_v2_prepared_ingest.py](../backend/app/services/stage_comparison/pipeline_v2_prepared_ingest.py) — этап 1

## Этап [3c2] entity_alignment_preview (2026-06-12)

После block_link_preview [3c] и до graphic_vision [3d] добавлен опциональный
этап **entity_alignment_preview** (mark-only): классифицирует пары графических
блоков по тому, одна ли это сущность — `same_entity_likely` / `possible_rename`
/ `scope_reorganized` / `mismatch_likely` / `link_validation_candidate`. Пишет
`entity_alignment_preview_report.json` (добавлен в `_ARTIFACT_FILENAMES` и
манифест), summary получает секцию `entity_alignment_preview`. Default ON,
fail-soft (`options.entity_alignment_preview.enabled=false` отключает). Ничего не
применяет; downstream selection пока не читает (wiring — следующий шаг). Подробно
— [stage_comparison_pipeline_v2_entity_alignment_preview.md](stage_comparison_pipeline_v2_entity_alignment_preview.md).
- [pipeline_v2_entity_alignment_preview.py](../backend/app/services/stage_comparison/pipeline_v2_entity_alignment_preview.py) — этап [3c2]

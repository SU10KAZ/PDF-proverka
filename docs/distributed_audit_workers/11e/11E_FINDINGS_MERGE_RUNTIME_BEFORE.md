# 11E — production `findings_merge`: путь ДО правки

Восстановлено чтением кода на базе `3b5a9bb2` (tip 11D.2). Названия файлов,
номера этапов и схема НЕ предполагались — всё ниже прочитано в коде.

## 1. Кто зовёт этап

```
PipelineManager
  → backend/app/pipeline/stages/findings_merge/runner.py: run_findings_merge(ctx)
      → backend/app/services/llm/claude_runner.py: run_findings_merge(...)
          ├─ ветка A: is_codex_model(model)   → _run_codex_json_stage
          │                                     + _run_codex_targeted_findings_merge (ВТОРОЙ вызов)
          ├─ ветка B: is_claude_stage(...)    → prepare_findings_merge_task → _run_cli
          └─ ветка C: OpenRouter              → build_findings_merge_messages → llm_runner.run_llm
      → проверка артефакта + post-merge проходы (см. §5)
```

Ключ этапа в `pipeline_log` и в `_run_cli(stage=…)`: `findings_merge`.
Имя каталога audit trail: `03_findings_merge` — это НЕ имя этапа.

## 2. Модель и таймаут

| что | где |
|---|---|
| выбор модели | `get_stage_model("findings_merge")` |
| таймаут | `CLAUDE_FINDINGS_MERGE_TIMEOUT = 3600` (`backend/app/core/config.py:228`) |
| инструменты ветки B | `FINDINGS_MERGE_TOOLS = "Read,Write,Grep,Glob,WebSearch,WebFetch"` (`config.py:243`) |

## 3. Сборка промпта

**Ветка B (Claude CLI — та, которую заменяет 11E).**
`stages/prepare/task_builder.py: prepare_findings_merge_task` →
`load_template_for_llm(FINDINGS_MERGE_TASK_TEMPLATE)` (грузится **EN**-вариант
`prompts/pipeline/en/findings_merge_task.md`) → `_inject_discipline` →
подстановка `{PROJECT_ID}`, `{OUTPUT_PATH}`, `{MD_FILE_PATH}`.
Артефакты в промпт **не вкладываются**: модель получает ПУТИ и обязана прочитать
их сама через `Read`, а результат записать через `Write`.

**Ветка C (OpenRouter — база инженерного содержания для 11E).**
`stages/prepare/prompt_builder.py: build_findings_merge_messages`:

* `system` = `_load_and_clean_template(FINDINGS_MERGE_TASK_TEMPLATE, …)`
  (тот же EN-шаблон + профиль дисциплины + `_clean_template_for_api`);
* `user` = `## 02_text_analysis.json:\n\n<файл целиком>\n\n## 01_blocks_analysis.json:\n\n<файл целиком>`.

Оба файла читает КОНВЕЙЕР (`_read_json_file`, `_read_findings_merge_blocks`).

## 4. Что этап читает перед вызовом модели

| роль | файл | обяз. | кто читает (ветка C) |
|---|---|---|---|
| выход этапа 01 (текст) | `02_text_analysis.json` | да | `_read_json_file` |
| выход этапа 02 (блоки) | `01_blocks_analysis.json` | да | `_read_findings_merge_blocks` |
| шаблон задачи | `prompts/pipeline/en/findings_merge_task.md` | да | `load_template_for_llm` |
| профиль дисциплины | `prompts/disciplines/<SECTION>/role.md` | да | `_inject_discipline` (только `{DISCIPLINE_ROLE}`) |
| MD документа | `{MD_FILE_PATH}` | нет | **только ветка B**, через `Read` модели |
| справочник норм | — | — | **не читается ни одной веткой** (KI-2) |

Резолв имён — `services/storage/stage_artifacts.resolve_existing`: канон сначала,
legacy-имя (`01_text_analysis.json` / `02_blocks_analysis.json`) как fallback.

**Тихая деградация, важная для 11E.** `_read_json_file` при отсутствии файла
возвращает СТРОКУ `"(файл X не найден)"` и кладёт её в промпт. Свод продолжается
по одному источнику и записывает результат как полноценный.

## 5. Что этап делает ПОСЛЕ вызова модели

Всё — детерминированно, в `stages/findings_merge/runner.py`:

1. `validate_and_repair_json(03_findings.json)` — иначе этап FAILED;
2. `normalize_schema.normalize_findings_schema` — канон имён полей;
3. `block_analysis/provenance.backfill_final_findings_provenance` ← читает `01_blocks_analysis.json`;
4. `backfill_text_evidence_in_findings` ← `01_blocks_analysis.json`, `document_graph.json`, `02_work/document.md`;
5. `merge_similar_findings` — объединение похожих + бэкап `03_findings_pre_merge.json`;
6. `apply_phase0_dedup` — за флагом `STAGE01_DEDUP_ENABLED` (по умолчанию OFF);
7. `renumber_findings_sequentially` — сплошная нумерация F-001…F-NNN;
8. `refresh_finding_quality`;
9. `backfill_highlights.backfill_project` ← `01_blocks_analysis.json`;
10. `ground_highlights_textlayer.backfill_textlayer_highlights` — за флагом
    `PIPELINE_TEXTLAYER_HIGHLIGHTS_ENABLED` (по умолчанию OFF, PDF не читается);
11. `attach_stage02_coverage_to_findings`;
12. `block_captions.humanize_findings_file` ← `document_graph.json` + `01_blocks_analysis.json`
    (флаг `FINDINGS_BLOCK_CAPTIONS_ENABLED`, по умолчанию ON).

## 6. Выход

`<output_dir>/03_findings.json`. Схема — в шаблоне: объект с `meta`
(`project_id`, `audit_completed`, `total_findings`, `blocks_analyzed`,
`by_severity`) и массивом `findings` (`id`, `severity`, `category`, `sheet`,
`page`, `problem`, `description`, `norm`, `norm_quote`, `solution`, `risk`,
`source_finding_ids`, `source_block_ids`, `related_block_ids`,
`evidence_text_refs`, `evidence`, `highlight_regions`).

Раннер читает из артефакта ровно `findings` (`len(...get("findings", []))`).

## 7. Сколько вызовов модели делает этап

| ветка | вызовов | почему |
|---|---|---|
| B (Claude CLI) | **1** | один `_run_cli`; повтор только после rate limit, и только если оркестратор разрешил ожидание |
| C (OpenRouter) | 1 | один `run_llm` |
| A (Codex) | 2 | базовый свод + `_run_codex_targeted_findings_merge` |

11E идёт ветками B→provider, где вызов ровно один. Ветка A в provider-режиме
недостижима: развилка провайдера стоит НИЖЕ моста.

## 8. Кто читает выход дальше

`findings_verify`, `norms`, `optimization`, `debt_control`,
`decision_carryover`, `report/generate_excel_report`, `findings_service`,
`knowledge_base`. В 11E ни один из них не запускается (§38).

## 9. Отказы ДО правки

| ситуация | поведение |
|---|---|
| `03_findings.json` не создан | этап FAILED |
| JSON невалиден и не чинится | этап FAILED |
| ненулевой код возврата CLI | этап FAILED |
| rate limit | ожидание сброса и ОДИН повтор (в 11E запрещено) |
| один из входных артефактов отсутствует | **тихо**: в промпт уезжает «(файл … не найден)», этап считается успешным |

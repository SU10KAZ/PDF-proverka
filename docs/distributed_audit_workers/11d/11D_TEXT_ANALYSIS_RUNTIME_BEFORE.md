# 11D — production stage `text_analysis`: путь ДО правки

Снято на базе `feat/distributed-audit-workers-pipeline-provider-e2e`
(HEAD `bc1720f157c5a88ea302314fa5620cf41954dfdf`), до единой строки правки 11D.

## 0. Оговорка о нумерации

В проекте «Этап 01» и «Анализ текста» — разговорные имена. Настоящее имя
production stage, о котором идёт речь, — **`text_analysis`**: этим ключом он
зовётся в `manager.py`, в `pipeline_log.json`, в `stage_models.json` и в
`allowed_stages` привязки провайдера. Артефакт при этом называется
`02_text_analysis.json` (историческое расхождение имени этапа и имени файла).
Ниже везде подразумевается именно `text_analysis`.

## 1. Кто кого зовёт

```
PipelineManager (backend/app/pipeline/manager.py:3872, :5423)
  → stages/text_analysis/runner.py: run_text_analysis(ctx, …)          ← STAGE RUNNER
      → services/llm/claude_runner.py: run_text_analysis(project_info, pid, …)  ← TRANSPORT SWITCH
          ├─ ветка A (codex/*)  : prompt_builder.build_text_analysis_message_sets → _run_codex_json_stage
          ├─ ветка B (claude-*) : task_builder.prepare_text_analysis_task → _run_cli(TEXT_ANALYSIS_TOOLS)
          └─ ветка C (прочее)   : prompt_builder.build_text_analysis_messages → llm_runner.run_llm
      → (возврат в runner.py) проверка выходного файла + валидация + md_prescan
```

Выбор ветки — `get_stage_model("text_analysis")` из `stage_models.json`.
В проде центра там `claude-opus-5` → **ветка B**.

## 2. Ветка B (боевая на центре) — «модель сама читает и сама пишет»

`prepare_text_analysis_task` берёт шаблон `prompts/pipeline/en/text_analysis_task.md`
и подставляет пути. В шаблоне транспортная оболочка живёт прямо в инженерном
тексте:

| строка шаблона | что это |
|---|---|
| `**MD file** … READ via Read tool: {MD_FILE_PATH}` | модель сама читает MD |
| `**Normative reference** — READ via Read tool: {DISCIPLINE_NORMS_FILE}` | модель сама читает нормы |
| `**Block analysis** … READ via Read tool: {BLOCKS_ANALYSIS_PATH}` | модель сама читает Stage 02 |
| `WRITE via Write tool: {OUTPUT_PATH}/02_text_analysis.json` | модель сама пишет результат |
| `Write JSON via Write tool — DO NOT output to chat` | запрет отдавать ответ в stdout |
| `After writing, output a brief summary of what was found` | в stdout уходит человекочитаемая сводка |

Далее `_run_cli(task_text, TEXT_ANALYSIS_TOOLS, …)` запускает `claude -p` c
`--allowedTools <TEXT_ANALYSIS_TOOLS>`, и **файл на диске создаёт модель**.
Стадия-раннер после возврата только проверяет, что файл появился и парсится.

Следствие, ради которого 11D и существует: этот промпт нельзя просто отправить
в `ProviderAdapter`. Адаптер запускает CLI с `--tools=` и полным
`--disallowed-tools`, то есть **инструментов у модели нет ноль**. Модель,
получившая ветку B через мост, физически не может ни прочитать `{MD_FILE_PATH}`,
ни записать `{OUTPUT_PATH}/02_text_analysis.json`; она получила бы инструкции к
инструментам, которых нет, и вернула бы в stdout сводку вместо JSON.

## 3. Что уже сегодня делает pipeline сам — ветка C

Ветка C (`build_text_analysis_messages` → `llm_runner.run_llm`) устроена ровно
так, как требует 11D:

* `_resolve_text_analysis_source` — **pipeline сам читает MD** (`_read_md_file`),
  fallback на `document_graph`/`extracted_text` вырезан, `text_source` всегда `md`;
* `_load_and_clean_template` → `_clean_template_for_api` **снимает транспортную
  оболочку** (строки с `Read tool` / `Write tool` / `DO NOT output to chat` /
  `After writing…`), оставляя инженерное содержание;
* норм-база дисциплины вкладывается **inline** в system;
* `md_prescan.build_prescan_prompt_section` вкладывается **inline** в system;
* `_absence_guard_block()` вкладывается **inline** через `{ABSENCE_GUARD}`;
* `_inject_discipline` подставляет роль / чек-лист / категории дисциплины;
* MD уходит **телом user-сообщения**;
* **output пишет сам pipeline**: `output_path.write_text(json.dumps(result.json_data …))`.

То есть требуемая 11D форма «pipeline читает → prompt inline → модель только
inference → pipeline пишет» в коде уже есть — но она приколочена к транспорту
OpenRouter (`llm_runner.run_llm`, HTTP, платный ключ), а не к `ProviderAdapter`.

**Это и есть точка правки 11D:** взять инженерную часть ветки C и подать её в
`ProviderAdapter`, не изобретая второй text_analysis и не возвращая модели
инструменты.

## 4. Где сегодня стоит перехват моста 11C

`claude_runner._run_cli` (строки 343-362): если активна привязка
(`AUDIT_WORKER_PROVIDER_BINDING` указывает на существующий файл), вызов уходит в
`pipeline_bridge.route_cli_call`. Перехват стоит **ниже** сборки промпта —
поэтому сегодня он получает промпт ветки B со всей транспортной оболочкой.

Это ровно тот разрыв, который 11D закрывает: перехват на уровне `_run_cli`
пригоден для `provider_selfcheck` (его промпт с самого начала пишется под
«ответь JSON в stdout»), но не годится для production stage, чей промпт
рассчитан на файловые инструменты.

## 5. Что делает stage runner после транспорта (не меняется в 11D)

`stages/text_analysis/runner.py` после возврата транспорта:

1. `record_cli_usage`;
2. `is_cancelled` / rate-limit retry (bounded, `rate_limit_retry.py`);
3. `output_dir / TEXT_ANALYSIS_FILENAME` существует? иначе `StageResult.fail`;
4. `validate_and_repair_json` (из `block_analysis.runner`) — парсимость + ремонт кавычек;
5. `json.loads` + обязательный `text_findings: list`, иначе fail;
6. `md_prescan.augment_text_analysis_file` — детерминированное дообогащение файла;
7. `update_pipeline_log(stage, "done")`.

Шаги 3-6 — это и есть «production validator + production output contract»,
который 11D обязан переиспользовать, а не подменять.

## 6. Инвентарь того, что обязано сохраниться (вход для теста semantic preservation)

Инженерная часть (A) промпта ветки C:

| элемент | источник |
|---|---|
| роль дисциплины | `{DISCIPLINE_ROLE}` ← `discipline_service` |
| чек-лист дисциплины | `{DISCIPLINE_CHECKLIST}` |
| категории замечаний дисциплины | `{DISCIPLINE_FINDING_CATEGORIES}` |
| страж отсутствия | `{ABSENCE_GUARD}` ← `_absence_guard_block()` |
| норм-база inline | `_read_norms_reference(project_info)` |
| секция pre-scan | `md_prescan.build_prescan_prompt_section` |
| JSON-схема выхода | блок ```json``` шаблона |
| правила арифметики / кросс-сверки / severity | разделы Task и Rules шаблона |
| `text_source: "md"` | требование в user-сообщении |
| тело MD | user-сообщение |

Транспортная оболочка (B), которой в provider-режиме быть не должно:

| элемент | где |
|---|---|
| `READ via Read tool: …` ×3 | шаблон, снимается `_clean_template_for_api` |
| `WRITE via Write tool: …` | шаблон, снимается |
| `DO NOT output to chat` | шаблон, снимается |
| `After writing, output a brief summary` | шаблон, снимается |
| абсолютные пути `{BLOCKS_ANALYSIS_PATH}` в уцелевших строках 65/128 | **остаётся после `_clean_template_for_api`** — 11D обязан убрать: §14 запрещает давать модели путь проекта |

Последняя строка — единственное, что ветка C не дочищает: `_clean_template_for_api`
убирает строки с `Read tool`, но упоминания `{BLOCKS_ANALYSIS_PATH}` в шагах
задачи (строки 65 и 128 шаблона) остаются и подставляются абсолютным путём.
Для OpenRouter это безвредно (модель без файловой системы), для 11D — прямое
нарушение §14, поэтому в provider-транспорте добавляется детерминированная
зачистка файловых ссылок **только в system-части** (тело документа не трогается).

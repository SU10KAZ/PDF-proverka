# 11D.1 — legacy-путь `text_analysis` (ветка B: Claude CLI + файловые инструменты)

Снято на `537c08a5` (финальный коммит 11D). Всё, что ниже, — код, а не пересказ
отчётов 11D: расхождения с ними отмечены явно.

## 0. Какая ветка и почему именно она — база сравнения

`claude_runner.run_text_analysis` разводит вызов на четыре пути
(`backend/app/services/llm/claude_runner.py:1104-1218`):

| ветка | условие | кто читает MD | кто пишет артефакт |
|---|---|---|---|
| provider (11D) | `pipeline_bridge.active()` — только воркер | конвейер | конвейер |
| A — codex | `is_codex_model(model)` | конвейер | конвейер |
| **B — Claude CLI** | `is_claude_stage("text_analysis")` | **модель через Read** | **модель через Write** |
| C — OpenRouter | иначе | конвейер | конвейер |

Задание 11D.1 называет legacy «старый Claude CLI + файловые tools» — это ветка B.
Она же подтверждена артефактами исторического прогона выбранного документа
(`pipeline_log.json → stages.text_analysis.model = claude-opus-4-6`,
`num_turns = 5`, результат в stdout — сводка «Файл `01_text_analysis.json`
записан»).

**Поправка к 11D.** `11D_TEXT_ANALYSIS_RUNTIME_BEFORE.md:28` утверждает «в проде
центра `claude-opus-5` → ветка B». Это верно для дефолта кода
(`backend/app/core/config.py:277`), но живой рантайм-конфиг центра
`backend/app/data/stage_models.json` (в git не отслеживается, mtime 2026-08-06)
содержит `"text_analysis": "codex/gpt-5.4"` — то есть сегодня центр
сконфигурирован на **ветку A**. На выводы 11D.1 это не влияет (сравниваем с тем
путём, который реально дал исторический результат), но фразу в 11D надо читать
как «дефолт кода», а не «текущий прод».

## 1. Цепочка вызова

```
PipelineManager (manager.py:3872 resume, :5423 полный аудит)
  → stages/text_analysis/runner.py:68  run_text_analysis(ctx, …)
      → services/llm/claude_runner.py:1104  run_text_analysis(project_info, pid, …)
          → :1174  task_text = prepare_text_analysis_task(project_info, project_id)
          → :1175  _run_cli(task_text, TEXT_ANALYSIS_TOOLS, 1800, …)
                     └─ claude -p … (МОДЕЛЬ сама Read MD, Read норм, Write артефакт)
          → :1183  _save_audit_trail(...)
      → runner.py:229-284  файл существует? → validate_and_repair_json →
                            обязательный text_findings → md_prescan.augment
```

## 2. Сборка промпта (`task_builder.prepare_text_analysis_task:546-606`)

| шаг | строка | что делает |
|---|---|---|
| кастомный промпт проекта | `:551-553` | если задан — **возвращается вместо шаблона целиком** |
| шаблон | `:554` | `prompts/pipeline/en/text_analysis_task.md` (EN приоритетнее RU) |
| hard error | `:558-561` | нет MD → `FileNotFoundError` |
| дисциплина | `:563` | `{DISCIPLINE_ROLE}`, `{DISCIPLINE_CHECKLIST}`, `{DISCIPLINE_FINDING_CATEGORIES}`, `{DISCIPLINE_NORMS_FILE}` = **абсолютный путь** |
| блоки | `:567` | `{BLOCKS_ANALYSIS_PATH}` = **путь**, не содержимое |
| подстановки | `:569-576` | `{PROJECT_ID}`, `{OUTPUT_PATH}`, `{MD_FILE_PATH}`, `{BLOCKS_ANALYSIS_PATH}`, `{ABSENCE_GUARD}` |
| pre-scan | `:578-586` | `md_prescan.build_prescan_prompt_section` в конец, fail-soft |
| MD↔вектор врезка | `:588-605` | за флагом `MD_MIRROR_RECONCILE_ENABLED` (default **OFF**) |

`_clean_template_for_api` в ветке B **не вызывается** — транспортные строки
остаются, они здесь и нужны.

Для документа 11D промпт ветки B = **10 894 символа**, 3 строки с `Read tool`,
2 с `Write tool`, 6 абсолютных путей, тела документа нет.

## 3. Шаблон: что инженерное, что транспортное

| секция шаблона | тип | что с ней делает ветка B | ветка C / provider |
|---|---|---|---|
| преамбула (язык вывода, «только JSON») | ENGINEERING | без изменений | без изменений |
| `## Role` → `{DISCIPLINE_ROLE}` | ENGINEERING | подставляется | подставляется |
| `## Input Data` п.1 «MD file … READ via Read tool» | TRANSPORT + заголовок источника | остаётся | **строка удаляется**, компенсирована `INPUT_DATA_NOTE` |
| `## Input Data` п.2 «Normative reference … READ via Read tool» | TRANSPORT + заголовок источника | остаётся | **удаляется**, норм-база вкладывается inline |
| `## Input Data` п.3 «Block analysis … READ via Read tool» | TRANSPORT + заголовок источника | остаётся | **удаляется**, `INPUT_DATA_NOTE` говорит «блоков нет» |
| `### Stage 1` шаги 1-8 (в т.ч. MANDATORY-правила арифметики, кросс-сверки, spec↔[IMAGE], HARD RULE «inflate severity») | ENGINEERING | без изменений | без изменений (только путь → плейсхолдер) |
| `{DISCIPLINE_CHECKLIST}` (6 подсекций ЭОМ) | ENGINEERING | подставляется | подставляется, побайтово так же |
| `{ABSENCE_GUARD}` | ENGINEERING | пусто (флаг OFF) | пусто (флаг OFF) |
| `## Finding Categories` → `{DISCIPLINE_FINDING_CATEGORIES}` | ENGINEERING | подставляется | подставляется, побайтово так же |
| `## Output JSON Schema` | ENGINEERING | без изменений | без изменений (только путь в сноске) |
| `## Normative Accuracy (norm_quote)` | ENGINEERING | без изменений | без изменений |
| `## Output` → «WRITE via Write tool: …» | TRANSPORT | остаётся | **удаляется целиком** |
| `## Rules` 1-3, 6 | ENGINEERING | остаются | остаются |
| `## Rules` 4-5 (Write / сводка в чат) | TRANSPORT | остаются | **удаляются** |
| `## Criteria «ПРОВЕРИТЬ ПО СМЕЖНЫМ»` | ENGINEERING | без изменений | без изменений |

`_clean_template_for_api` удаляет **ровно 6 строк** — все шесть перечислены
выше и все транспортные. Проверено прогоном её регулярок по реальному шаблону с
подставленным профилем ЭОМ (`11D1_PROMPT_SEMANTIC_DIFF.json`).

Единственная инженерная потеря — заголовочные слова «**MD file** (primary text
source)», «**Normative reference**», «**Block analysis (Stage 02, compact
view)**»: они уезжают вместе с «READ via Read tool» в той же строке. 11D это
заметил и компенсировал блоком `INPUT_DATA_NOTE`, который называет источники
точнее (см. `11D1_PROVIDER_RUNTIME.md` §3).

## 4. Рантайм CLI — здесь лежит настоящая асимметрия

`_build_cmd` (`claude_runner.py:209-232`) для text_analysis даёт:

```
claude -p
  --model <stage model>
  --allowedTools Read,Write,Grep,Glob,WebSearch,WebFetch
  --output-format json
  --strict-mcp-config
```

Чего в argv **НЕТ** (грепано по `backend/`): `--system-prompt`,
`--append-system-prompt`, `--setting-sources`, `--add-dir`, `--max-turns`,
`--safe-mode`, `--permission-mode`, `--disallowed-tools`.

* **stdin** — весь `task_text`.
* **cwd** — `text_analysis` вызывает `_run_cli` **без** `clean_cwd`
  (`claude_runner.py:1175-1178`, дефолт `False` на `:326`) ⇒ `cwd_arg = None`
  ⇒ `process_runner.py:293` `work_dir = cwd or str(BASE_DIR)`, где
  `BASE_DIR = ROOT_DIR` = **корень репозитория**.
* **env** — удаляются только переменные, начинающиеся на `CLAUDE`
  (`claude_runner.py:395`). `HOME` остаётся.

Собственный комментарий репозитория (`claude_runner.py:235-238, 247-251`)
описывает следствие дословно:

> «Чистая cwd для запуска `claude -p` **без подгрузки project CLAUDE.md /
> hooks / memory / skills**… Эмпирически (КЖ5.1, 25 блоков) даёт −42%
> input/блок и −36% cli_cost при +35% findings».

То есть проект уже измерил, что ambient-контекст CLI **материально меняет число
находок**, и включил чистую cwd ровно одному этапу — блочному
(`claude_runner.py:1796`). У `text_analysis` она **не включена**: значит модель
ветки B получала в контекст проектный `CLAUDE.md`, `.claude/settings.json`,
хуки, навыки и пользовательский `~/.claude/`.

**Почему это важно именно для severity.** Пять значений `severity` встречаются в
промпте этапа ровно дважды и оба раза без определений: перечнем в JSON-схеме
(`text_analysis_task.md:108`) и строкой «severity — ONLY one of the 5 values»
(`:145`). Развёрнут только критерий «ПРОВЕРИТЬ ПО СМЕЖНЫМ» (`:150-165`). Что
значит «КРИТИЧЕСКОЕ», не сказано ни в шаблоне, ни в одном из 15 профилей
дисциплин (проверено grep'ом по `prompts/disciplines/`). Единственное место
платформы, где это записано, — корневой `CLAUDE.md:211-216`:

```
**Категории:**
- **Критическое** — нельзя строить (нарушения ПУЭ/ГОСТ/СП)
- **Экономическое** — деньги/объёмы/пересортица
- **Эксплуатационное** — будущие проблемы при эксплуатации
- **Рекомендательное** — опечатки, мелкие несоответствия
- **Проверить по смежным** — требует информации из других разделов
```

Ветка B стартует из каталога, где этот файл лежит. Provider — нет.

## 5. Инструменты: что модель МОГЛА сделать сверх чтения MD

`TEXT_ANALYSIS_TOOLS = "Read,Write,Grep,Glob,WebSearch,WebFetch"`
(`backend/app/core/config.py:241`). При cwd = корень репозитория это значит, что
модель ветки B могла:

* прочитать MD (обязана — иначе задача не выполнима);
* прочитать `norms_reference.md` дисциплины по указанному пути (**не обязана** —
  строка помечена `(if available)`, факт чтения нигде не фиксируется);
* прочитать `01_blocks_for_text.json`, если он есть;
* **Grep/Glob по всему репозиторию** — включая `norms_db.json`,
  `norms_paragraphs.json`, артефакты соседних проектов;
* **WebSearch/WebFetch** — проверить актуальность нормы в сети.

Исторический прогон: `num_turns = 5`, то есть ходов с инструментами было
несколько. Какие именно — **UNKNOWN**: содержимое ходов в артефактах не
сохранено, только итоговый JSON-результат CLI.

Возвращать этот доступ 11D.1 не предлагает (§17/§24 задания прямо запрещают).
Зафиксировано как канал, по которому в legacy мог приходить контекст, которого в
provider-режиме нет и не будет.

## 6. Контракт выхода (не менялся ни в 11D, ни в 11D.1)

`stages/text_analysis/runner.py:229-284`:

1. файл `02_text_analysis.json` существует, иначе `StageResult.fail`;
2. `validate_and_repair_json` — парсимость + ремонт кавычек;
3. обязательный `text_findings: list`, иначе fail;
4. `md_prescan.augment_text_analysis_file` — детерминированное дообогащение
   (может **добавить** находки, severity существующих не меняет);
5. `update_pipeline_log(stage, "done")`.

Валидации `severity` нет ни в одном пути — ни в раннере, ни в
`provider_transport.FIELD_TYPES`, ни в 13 проверках `validate_inference`.

# 11D.1 — provider-путь `text_analysis` (после 11D, с правками 11D.1)

## 1. Цепочка

```
PipelineManager → stages/text_analysis/runner.py         ← НЕ МЕНЯЛСЯ
  → claude_runner.run_text_analysis:1122  pipeline_bridge.active()?
      → :873 _run_text_analysis_via_provider
          :920  кастомный промпт проекта? → ОТКАЗ этапа (не подмена)
          :929  prompt_builder.build_text_analysis_messages(...)   ← БОЕВОЙ сборщик ветки C
                  · _read_md_file — MD читает КОНВЕЙЕР, text_source="md"
                  · _inject_discipline — роль / чек-лист / категории
                  · _clean_template_for_api — снятие Read/Write-строк
                  · {ABSENCE_GUARD} (флаг OFF → пусто)
                  · норм-база дисциплины inline
                  · секция md_prescan (для документа 11D пуста)
          :934  provider_transport.build_provider_prompt(messages)
                  · strip_filesystem_references по ИНСТРУКЦИЯМ
                  · INPUT_DATA_NOTE перед «## Input Data»
                  · SEVERITY_SEMANTICS перед «## Output JSON Schema»   ← 11D.1
                  · склейка: инструкции → SOURCE DOCUMENT → OUTPUT TRANSPORT
          :941-966  гейты: путей 0, потолок 600 000 симв., блочный контекст
          :978  pipeline_bridge.run_stage_inference(...)
                  · белый список этапов, потолок вызовов, журнал заявки
          → ProviderAdapter.structured_inference(model из ЛОКАЛЬНОЙ политики)
                  · --tools= --disallowed-tools=… --max-turns 1
                  · --safe-mode --strict-mcp-config --disable-slash-commands
                    --no-session-persistence --setting-sources=
                  · cwd = пустой runtime попытки, env с нуля, промпт в stdin
          :1047 _assert_output_inside_attempt
          :1055 _write_json(text_analysis_provider_run.json)   ← пишется ВСЕГДА
          :1083 _write_json(02_text_analysis.json)             ← только при ok
  → runner.py: validate_and_repair_json + text_findings + md_prescan.augment
```

## 2. Что модель получает и чего не получает

| элемент | как доставлен |
|---|---|
| роль / чек-лист / категории дисциплины | inline, побайтово как в ветке B |
| нормативный справочник дисциплины | **inline целиком** (8 280 симв. для ЭОМ) — в ветке B был только путь |
| правила задачи, схема, `norm_quote`, критерии «ПРОВЕРИТЬ ПО СМЕЖНЫМ» | inline, дословно как в ветке B |
| смысл пяти значений `severity` | **inline (правка 11D.1)** — в ветке B приходил из проектной памяти CLI |
| тело MD | inline целиком, дословно, между маркерами `SOURCE DOCUMENT` |
| секция md_prescan | inline (для документа 11D — пустая, как и в ветке B) |
| страж отсутствия | пусто — флаг `PIPELINE_ABSENCE_GUARD_ENABLED` OFF, как и в ветке B |
| блочный анализ Stage 02 | **нет**; если файл существует — этап ОТКАЗЫВАЕТ, а не работает вслепую |
| Read / Write / Bash / Grep / Glob / WebSearch | **нет**: `--tools=` + поимённый `--disallowed-tools` |
| путь проекта | **нет**: `strip_filesystem_references`, проверка «осталось 0» в рантайме |
| проектный `CLAUDE.md`, `.claude/settings.json`, хуки, навыки, MCP | **нет**: `--safe-mode --setting-sources=`, cwd = пустой каталог |

## 3. Блоки, которые ставит транспорт

**`INPUT_DATA_NOTE`** (11D) — перед покалеченным `## Input Data`. Компенсирует
три заголовка источников, уехавших вместе со строками «READ via Read tool», и
говорит правду о прогоне: MD вложен ниже, норм-база вложена в инструкции,
блочного анализа нет, `[IMAGE]`-описания из MD за визуальное подтверждение не
считать.

**`SEVERITY_SEMANTICS`** (11D.1) — перед `## Output JSON Schema`, то есть
вплотную к перечню значений. Переносит в промпт определения пяти категорий,
которые до 11D доходили до модели только через проектную память CLI. Текст —
дословно канонический раздел «Категории» проектного `CLAUDE.md`; формулировка
намеренно симметрична («не смягчай и не завышай»), потому что проверить влияние
правки прогоном на 11D.1 нельзя. Якорь вставки пишется в карту сборки
(`severity_semantics_anchor`): если шаблон переименуют, отчёт покажет
`end_of_instructions`, а не промолчит.

**`TRANSPORT_CONTRACT`** (11D, уточнён на 11D.1) — последним в промпте. Уточнение:
прежняя фраза «do not report that a file is missing» задумывалась про
инструменты, но читалась шире — как «не сообщай о том, чего не хватает». Она
стояла на позиции максимальной рецентности, а самый частый класс замечаний этого
этапа — ровно «в документации не указано X». Теперь сказано явно: ограничение
касается **доступа к инструментам**, а данные, которых нет в самой документации,
— штатное замечание аудита.

## 4. Отказы: ни одного тихого

Не изменено 11D.1. Полный перечень — `11D_TEXT_ANALYSIS_RUNTIME_AFTER.md` §6.
Возврата «мост не смог → пойдём прежним путём» нет ни в одной ветке.

## 5. Изменённые файлы 11D.1

| файл | что |
|---|---|
| `backend/app/pipeline/stages/text_analysis/provider_transport.py` | `SEVERITY_SEMANTICS` + `_insert_severity_semantics` + два новых поля карты сборки; уточнение одной фразы `TRANSPORT_CONTRACT` |
| `tests/test_11d1_text_analysis_semantic_equivalence.py` | **новый** — 28 тестов A..R |

Ни `prompt_builder`, ни `task_builder`, ни шаблон `text_analysis_task.md`, ни
профили дисциплин, ни раннер этапа 11D.1 **не трогает**: правки живут только в
provider-транспорте, поэтому поведение центра (ветки A/B/C) не меняется ни на
символ. Это закреплено тестами `test_p_legacy_path_unchanged_by_11d1` и
`test_p_shared_template_untouched`.

## 6. Размеры промпта

| | 11D (как ушло в модель 09.08.2026) | после правок 11D.1 |
|---|---|---|
| инструкции | 18 780 | 19 432 (+652) |
| документ | 11 544 | 11 544 (0) |
| всего | 31 175 | 31 990 (+815) |

Сборка 11D воспроизведена офлайн до символа — значит разбор вёлся по тому же
промпту, который реально ушёл в CLI на воркере. Отпечаток после правки с боевым
прогоном не сравним: правка сделана позже и на модели не проверялась.

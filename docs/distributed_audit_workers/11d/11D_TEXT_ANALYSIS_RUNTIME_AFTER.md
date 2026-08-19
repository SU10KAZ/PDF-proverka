# 11D — production stage `text_analysis`: путь ПОСЛЕ правки

Ветка `feat/distributed-audit-workers-text-analysis-provider`, база
`bc1720f157c5a88ea302314fa5620cf41954dfdf` (11C).

## 1. Новая развилка и где она стоит

```
PipelineManager
  → stages/text_analysis/runner.py: run_text_analysis(ctx, …)     ← БЕЗ ИЗМЕНЕНИЙ
      → services/llm/claude_runner.py: run_text_analysis(…)
          ├─ мост активен?  ──► _run_text_analysis_via_provider    ← НОВОЕ
          ├─ ветка A (codex/*)                                      ← БЕЗ ИЗМЕНЕНИЙ
          ├─ ветка B (claude-*, Read/Write tools)                   ← БЕЗ ИЗМЕНЕНИЙ
          └─ ветка C (OpenRouter)                                   ← БЕЗ ИЗМЕНЕНИЙ
      → проверка артефакта + md_prescan                            ← БЕЗ ИЗМЕНЕНИЙ
```

Развилка стоит **выше** выбора codex/claude/OpenRouter. Это не стилистика:
в provider-режиме различается не «каким CLI звать», а **кто делает файловую
работу**. Решать это после сборки промпта было бы поздно — промпт уже был бы
собран под чужой транспорт.

Условие входа — ровно одно: `pipeline_bridge.active()`, то есть переменная
`AUDIT_WORKER_PROVIDER_BINDING` указывает на существующий файл привязки. Её
пишет исполнитель воркера, уже списав разрешение оператора. На центре её нет
никогда, поэтому там код платформы ведёт себя дословно как до 11D
(тест `test_b_legacy_path_untouched_without_binding`).

## 2. Новый путь

```
resolve input MD (боевой резолвер версий)
        ↓
pipeline САМ читает MD          prompt_builder._read_md_file, text_source="md"
        ↓
дисциплина / нормы / pre-scan   _inject_discipline, _read_norms_reference,
        ↓                        build_prescan_prompt_section, {ABSENCE_GUARD}
БОЕВОЙ сборщик промпта          prompt_builder.build_text_analysis_messages
        ↓                        (тот же, что у ветки OpenRouter)
transport adaptation            stages/text_analysis/provider_transport.py
        ↓                        · склейка system+user в один stdin-текст
        ↓                        · зачистка файловых ссылок в ИНСТРУКЦИЯХ
        ↓                        · блок OUTPUT TRANSPORT вместо Read/Write
pipeline_bridge.run_stage_inference
        ↓                        · белый список этапов привязки
        ↓                        · потолок вызовов попытки
        ↓                        · журнал: заявка ДО вызова, запись СРАЗУ после
ProviderAdapter.structured_inference(model=…, accepted_reported_models=…)
        ↓                        · --model из ЛОКАЛЬНОЙ политики воркера
        ↓                        · --tools= , --disallowed-tools=…, --max-turns 1
        ↓                        · --safe-mode --strict-mcp-config --setting-sources=
        ↓                        · cwd = пустой runtime попытки, env с нуля
Claude CLI (подписка владельца VPS, ambient_user)
        ↓
structured JSON
        ↓
БОЕВАЯ проверка результата      validate_inference: exit_code, status,
        ↓                        json_parsed, required_fields, field_types,
        ↓                        expected_semantics(text_source="md"),
        ↓                        no_credential_like, no_private_paths,
        ↓                        no_forbidden_literals, provider/auth/identity,
        ↓                        model_matches_policy
guard пути                      _assert_output_inside_attempt
        ↓
PIPELINE САМ ПИШЕТ              _write_json → <output_dir>/02_text_analysis.json
        ↓                        + <output_dir>/text_analysis_provider_run.json
БОЕВОЙ раннер этапа             validate_and_repair_json, text_findings: list,
                                 md_prescan.augment_text_analysis_file
```

## 3. Что модель НЕ получает

| запрещено §14 | как обеспечено |
|---|---|
| Read / Write / Bash / Grep / Glob | `--tools=` (набор отключён целиком) + `--disallowed-tools=` поимённо |
| путь к проекту как инструкция | `strip_filesystem_references` в system-части; проверка `absolute_paths_remaining_in_instructions == 0` |
| самостоятельный выбор output path | пути в промпте нет вовсе; пишет `_write_json` конвейера |
| выход за каталог попытки | `_assert_output_inside_attempt` — отказ записи |
| личный контекст владельца машины | `--safe-mode --strict-mcp-config --disable-slash-commands --no-session-persistence --setting-sources=`, cwd = пустой каталог |
| соседние проекты / knowledge base / git | процесс работает в изолированных корнях `audit_runner.isolated_roots`; у модели файловой системы нет вовсе |

Замеряно на реальном документе (ЭОМ 133-23-ГК-ЭС): инструкции 18 022 симв.,
документ 11 544 симв., вычищено 2 файловые ссылки, абсолютных путей в
инструкциях осталось **0**.

## 4. Транспортная оболочка: что снято, что поставлено вместо

Снято (и проверяется тестом `test_u2`):

* `READ via Read tool: …` ×3 — снимает `_clean_template_for_api`;
* `WRITE via Write tool: {OUTPUT_PATH}/02_text_analysis.json` — снимает он же;
* `DO NOT output to chat`, `After writing, output a brief summary` — он же;
* абсолютные пути `{BLOCKS_ANALYSIS_PATH}` в уцелевших строках задачи —
  **новое в 11D**: `strip_filesystem_references`.

Поставлено вместо — блок `OUTPUT TRANSPORT`: «инструментов нет; всё нужное уже
вложено выше; верни один JSON-объект в ответе; файл запишет конвейер».

Инженерная часть сверяется автоматически: `semantic_preservation_report`
сравнивает **боевой API-промпт** (ветка OpenRouter) с provider-промптом по
десяти опорным признакам (JSON-схема, перечень severity, правило арифметики,
кросс-сверка, spec↔[IMAGE], `norm_quote`, «ПРОВЕРИТЬ ПО СМЕЖНЫМ», `text_source`,
язык вывода, имя этапа). На реальном документе: потеряно **0**, транспортных
маркеров просочилось **0**.

Базой сравнения выбран API-промпт, а не сырой CLI-шаблон, намеренно: API-промпт
уже боевой и уже прошёл `_clean_template_for_api`, поэтому разница показывает
вклад ровно 11D, а не давно принятое решение о ветке API.

## 5. Модель: локальная политика воркера

Было (11C): `--model` не передавался вовсе; модель выбирал CLI; конфигурация
называла `claude-opus-5`, фактически ответила `claude-opus-4-8[1m]`.

Стало:

```
центр  →  provider_requirement.capability = "strong_audit"   (ЛОГИЧЕСКОЕ)
воркер →  provider_policy.json: strong_audit → "claude-opus-5"
       →  binding.model = "claude-opus-5"
       →  binding.accepted_reported_models = ["claude-opus-5", "claude-opus-5[1m]"]
       →  argv: … --model claude-opus-5 …
       ←  modelUsage → фактическая модель
       →  не совпало ⇒ status=error, error_code=model_mismatch, артефакт НЕ пишется
```

`capability` и `model` в требовании центра **взаимоисключимы** — иначе неясно,
кто решает, а «возьмём точный» вернуло бы центру распоряжение чужой подпиской.

I-P5 при этом сохраняется дословно: в argv попадает строка из файла
администратора машины, а не поле задания.

Список `accepted_reported_models` не «смягчение»: суффикс `[1m]` — вариант той
же модели с окном в миллион токенов (в бинаре CLI 2.1.220 присутствуют обе
формы), и именно суффиксный вид CLI вернул на 11C. Любой другой идентификатор —
отказ; на Sonnet или другое поколение Opus молчаливого перехода нет.

## 6. Отказы: ни одного тихого

| ситуация | поведение |
|---|---|
| этап вне белого списка привязки | этап FAILED, модель не вызывается |
| исчерпан потолок вызовов попытки | этап FAILED, модель не вызывается |
| нет `grant_id` в привязке | этап FAILED |
| журнал в состоянии `indeterminate` | этап FAILED, автоповтора нет (решает оператор) |
| таймаут / ненулевой код CLI / rate limit | этап FAILED с классифицированным кодом |
| ответ не JSON | этап FAILED, артефакт не пишется |
| нет обязательного поля / `text_source ≠ md` | этап FAILED, артефакт не пишется |
| фактическая модель не та | этап FAILED, артефакт не пишется |
| путь записи вне попытки | запись отменена, этап FAILED |

Возврата «мост не смог → пойдём прежним путём» нет ни в одной ветке.
Ветка `except ProviderBridgeError` превращает отказ в код возврата 1 — это
штатный отказ этапа, а не фолбэк: прежний транспорт из provider-режима
недостижим.

## 7. Изменённые и новые файлы

| файл | что |
|---|---|
| `audit_worker/providers/model_policy.py` | **новый** — локальная политика «способность → точная модель» |
| `backend/app/pipeline/stages/text_analysis/provider_transport.py` | **новый** — разделение A/B, склейка промпта, зачистка путей, контракт результата |
| `scripts/run_11d_text_analysis_provider.py` | **новый** — прогон этапа на воркере (fake/real), sandbox, разрешение, отчёт |
| `tests/test_distributed_workers_text_analysis_provider.py` | **новый** — 60 тестов A..AI |
| `audit_worker/providers/resolver.py` | `capability` в требовании, разрешение модели по локальной политике, поля привязки |
| `audit_worker/providers/claude_adapter.py` | `--model` в argv рабочего вызова, fail-closed сверка фактической модели |
| `audit_worker/providers/codex_adapter.py` | принимает те же параметры и ОТКАЗЫВАЕТ на явной модели (не реализовано ⇒ не притворяться) |
| `audit_worker/providers/base.py` | сигнатура `structured_inference` |
| `audit_worker/providers/inference.py` | именованная проверка `model_matches_policy` |
| `audit_worker/providers/pipeline_bridge.py` | передача модели в адаптер и в проверку |
| `audit_worker/providers/errors.py` | код `model_mismatch` |
| `audit_worker/audit_runner.py` | `capability` в форме требования центра |
| `backend/app/services/llm/claude_runner.py` | provider-маршрут `text_analysis`, guard пути записи |

Боевой раннер `stages/text_analysis/runner.py` **не менялся ни на строку**:
проверка артефакта, ремонт JSON, требование `text_findings` и `md_prescan`
остались его работой.

## 8. Что осталось общим с прежним путём

* тот же артефакт `02_text_analysis.json` и то же его имя;
* та же проверка артефакта и то же дообогащение;
* тот же сборщик промпта и тот же профиль дисциплины;
* тот же `pipeline_log`, тот же `StageResult`, тот же учёт токенов.

Единственный новый файл рядом с артефактом — `text_analysis_provider_run.json`:
карта сборки промпта, отпечаток промпта, результат провайдера, проверка,
запись журнала и «мягкая» часть контракта. Он пишется **и при отказе тоже** —
разбирать неудачу без него пришлось бы по журналу вызовов на чужой машине.

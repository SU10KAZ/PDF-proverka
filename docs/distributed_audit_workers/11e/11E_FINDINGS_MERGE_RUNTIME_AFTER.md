# 11E — production `findings_merge`: путь ПОСЛЕ правки

Ветка `feat/distributed-audit-workers-findings-merge-provider`,
база `3b5a9bb277d0c22077f28ee88e3ba39e18a24588` (tip 11D.2).

## 1. Где стоит новая развилка

```
PipelineManager
  → stages/findings_merge/runner.py: run_findings_merge(ctx)        ← БЕЗ ИЗМЕНЕНИЙ
      → services/llm/claude_runner.py: run_findings_merge(…)
          ├─ мост активен? ──► _run_findings_merge_via_provider     ← НОВОЕ
          ├─ ветка A (codex + targeted)                             ← БЕЗ ИЗМЕНЕНИЙ
          ├─ ветка B (claude-*, Read/Write tools)                   ← БЕЗ ИЗМЕНЕНИЙ
          └─ ветка C (OpenRouter)                                   ← БЕЗ ИЗМЕНЕНИЙ
      → validate_and_repair_json + все post-merge проходы           ← БЕЗ ИЗМЕНЕНИЙ
```

Развилка стоит **выше** выбора провайдера. Это не стилистика: в
provider-режиме различается не «каким CLI звать», а **кто делает файловую
работу**, и решать это после сборки промпта было бы поздно. Побочное, но
важное следствие: ветка A с её ВТОРЫМ (targeted) вызовом в provider-режиме
недостижима — а бюджет 11E равен одному вызову.

Условие входа ровно одно: `pipeline_bridge.active()`, то есть переменная
`AUDIT_WORKER_PROVIDER_BINDING` указывает на существующий файл привязки. Её
пишет исполнитель воркера, уже списав разрешение оператора. На центре её нет
никогда (тест `test_c_legacy_path_untouched_without_binding`).

## 2. Новый путь

```
resolve + ПРОВЕРКА входа        provider_transport.resolve_merge_inputs
        ↓                        · оба артефакта обязаны существовать
        ↓                        · оба обязаны быть JSON-ОБЪЕКТАМИ
        ↓                        · иначе отказ ЭТАПА до модели
pipeline САМ читает 02 и 01      prompt_builder.build_findings_merge_messages
        ↓                        (тот же сборщик, что у ветки OpenRouter)
transport adaptation             stages/findings_merge/provider_transport.py
        ↓                        · склейка system+user в один stdin-текст
        ↓                        · честная справка о составе входа
        ↓                        · смысл шкалы severity (импорт из этапа 01)
        ↓                        · блок OUTPUT TRANSPORT вместо Read/Write
        ↓                        · зачистка файловых ссылок в ИНСТРУКЦИЯХ
ПРОВЕРКА ПОЛНОТЫ ВХОДА           input_coverage_report
        ↓                        каждый T-NNN и G-NNN обязан быть в промпте
guard'ы промпта                  абсолютные пути = 0; потолок символов
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
БОЕВАЯ проверка результата       validate_inference: exit_code, status,
        ↓                        json_parsed, required_fields(findings),
        ↓                        field_types, no_credential_like,
        ↓                        no_private_paths, no_forbidden_literals,
        ↓                        provider/auth/identity, model_matches_policy
guard пути                       _assert_output_inside_attempt
        ↓
PIPELINE САМ ПИШЕТ               _write_json → <output_dir>/03_findings.json
        ↓                        + <output_dir>/findings_merge_provider_run.json
БОЕВОЙ раннер этапа              validate_and_repair_json → normalize_schema →
                                 provenance → text_evidence → merge_similar →
                                 phase0_dedup → renumber → finding_quality →
                                 highlights → coverage → block_captions
```

## 3. Что модель НЕ получает

| запрещено §1 задания | как обеспечено |
|---|---|
| Read / Write / Bash / Grep / Glob | `--tools=` (набор отключён целиком) + `--disallowed-tools=` поимённо |
| путь к проекту как инструкция | `strip_filesystem_references` + проверка `absolute_paths_remaining_in_instructions == 0` (в рантайме, не только в тестах) |
| самостоятельный выбор output path | пути в промпте нет вовсе; пишет `_write_json` конвейера |
| выход за каталог попытки | `_assert_output_inside_attempt` — отказ записи |
| исходный PDF, кропы, изображения | в промпт не попадают; `block_analysis` в 11E не запускается |
| соседние проекты, knowledge base, git | процесс работает в изолированных корнях `audit_runner.isolated_roots` |
| личный контекст владельца машины | `--safe-mode --strict-mcp-config --disable-slash-commands --no-session-persistence --setting-sources=`, cwd = пустой каталог |

Замерено на реальном документе: инструкции 12 866 симв., полезная нагрузка
38 814 симв., абсолютных путей в инструкциях **0**, входных замечаний 25
(19 текстовых + 6 блочных), потеряно до модели **0**.

## 4. Транспортная оболочка: что снято, что поставлено вместо

Снято (проверено на дампе реально отправленного stdin):

* `READ via Read tool: …` ×3 (текстовый артефакт, блочный артефакт, MD);
* `WRITE via Write tool: {OUTPUT_PATH}/03_findings.json`;
* `Write JSON via Write tool — DO NOT output to chat`;
* `After writing, output a brief summary of findings`.

Поставлено вместо:

1. **`## Input Data (this run)`** — честный состав входа. Нужен потому, что
   `_clean_template_for_api` вырезает СТРОКИ со словом «Read tool», а в шаблоне
   это как раз строки-ЗАГОЛОВКИ пунктов входных данных: от секции остаются
   висячие подпункты про `text_findings` и `block_analyses` без указания,
   откуда они. Отдельно и явно сказано, чего в прогоне НЕТ: MD-файла и
   нормативного справочника.
2. **`## Severity Semantics`** — смысл пяти значений шкалы, импортируется из
   модуля этапа 01 (один экземпляр на платформу). Вставляется прямо перед
   `### Finding Fields`, то есть вплотную к перечню значений.
3. **`## OUTPUT TRANSPORT`** — «инструментов нет; всё нужное уже вложено выше;
   ни одно входное замечание не теряется молча; верни один JSON-объект».

## 5. Почему смысл severity нужен именно здесь

`text_analysis` severity ВЫБИРАЕТ. Свод его **МЕНЯЕТ**: «Severity elevation»
поднимает уровень при подтверждении чертежом, «Severity reduction» понижает при
опровержении, объединение сводит замечания разных уровней в одно. При этом сам
шаблон свода перечисляет пять значений и ни одного не определяет.

Офлайн-поиск по платформе (`scripts/verify_11e_prompt_semantics.py`, раздел
`hidden_context`) подтвердил вывод 11D.1 и для этого этапа: расшифровка значений
существует ровно в одном месте — корневом `CLAUDE.md`, который `ProviderAdapter`
намеренно подавляет. Ни в `prompts/`, ни в 15 профилях дисциплин её нет.

Возвращается не личный контекст, а одна типизированная константа, и та —
общая с этапом 01: второй экземпляр тех же формулировок разошёлся бы с первым
на первой же правке.

## 6. Отказы: ни одного тихого

| ситуация | поведение |
|---|---|
| нет `02_text_analysis.json` или `01_blocks_analysis.json` | этап FAILED, модель не вызывается |
| вход не разбирается как JSON-объект | этап FAILED, модель не вызывается |
| входное замечание не доехало до промпта | этап FAILED, модель не вызывается |
| в инструкциях остался абсолютный путь | этап FAILED, модель не вызывается |
| промпт больше потолка | этап FAILED (усечения не бывает) |
| у проекта задан кастомный промпт этапа | этап FAILED (тихая подмена правил запрещена) |
| этап вне белого списка привязки | этап FAILED |
| исчерпан потолок вызовов попытки | этап FAILED |
| нет `grant_id` в привязке | этап FAILED |
| журнал в состоянии `indeterminate` | этап FAILED, автоповтора нет |
| таймаут / ненулевой код CLI | этап FAILED с классифицированным кодом |
| ответ не JSON / нет `findings` | этап FAILED, артефакт не пишется |
| фактическая модель не та | этап FAILED, артефакт не пишется |
| путь записи вне попытки | запись отменена, этап FAILED |

Возврата «мост не смог → пойдём прежним путём» нет ни в одной ветке.

## 7. Изменённые и новые файлы

| файл | что |
|---|---|
| `backend/app/pipeline/stages/findings_merge/provider_transport.py` | **новый** — разделение A/B, склейка промпта, контракт входа, полнота входа, сверка смысла |
| `backend/app/services/llm/claude_runner.py` | provider-маршрут `findings_merge`, ключ этапа, потолок промпта |
| `audit_worker/providers/claude_adapter.py` | текст ошибки CLI доезжает в `detail` (найдено боевым прогоном 11E) |
| `scripts/run_11e_findings_merge_provider.py` | **новый** — прогон этапа на воркере (fake/real), sandbox, разрешение, отчёт |
| `scripts/verify_11e_prompt_semantics.py` | **новый** — офлайн-сверка смысла legacy ↔ provider |
| `scripts/verify_11e_pre_inference_gate.py` | **новый** — 93 проверки §23 по ДАМПУ реально отправленного текста |
| `tests/test_distributed_workers_findings_merge_provider.py` | **новый** — 78 тестов A..AO |

Боевой раннер `stages/findings_merge/runner.py` **не менялся ни на строку**:
проверка артефакта, нормализация схемы, провенанс, объединение похожих,
нумерация, подписи блоков остались его работой.

## 8. Что осталось общим с прежним путём

* тот же артефакт `03_findings.json` и то же его имя;
* та же схема и та же проверка артефакта;
* тот же сборщик промпта и тот же профиль дисциплины;
* тот же `pipeline_log`, тот же `StageResult`, тот же учёт токенов.

Единственный новый файл рядом с артефактом —
`findings_merge_provider_run.json`: карта сборки промпта, его отпечаток,
контракт входа, полнота входа, результат провайдера, проверка, запись журнала.
Он пишется **и при отказе тоже** — разбирать неудачу без него пришлось бы по
журналу вызовов на чужой машине.

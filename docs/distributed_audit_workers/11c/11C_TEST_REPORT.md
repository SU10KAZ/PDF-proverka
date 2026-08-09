# 11C_TEST_REPORT — что проверено и чем

## 1. Сводка

| Прогон | Тестов | Прошло | Провалено | Пропущено |
|---|---|---|---|---|
| Новый модуль `tests/test_distributed_workers_pipeline_provider.py` | 75 | **75** | 0 | 0 |
| Вся подсистема `tests/test_distributed_workers_*.py` | 1005 | **1004** | 0 | 1 |
| Смежное на центре `backend/tests -k "claude or llm or runner or remote or pipeline"` | 377 | 368 | 9 | — |

Девять падений в третьей строке — **не регресс 11C**: тот же набор из тех же
девяти падает на нетронутом базовом коммите `85ab2532` (проверено прогоном на
worktree ветки 11b). Причина одна и та же: `backend/app/pipeline/manager.py:4638
TypeError` в контуре `critic_v2`, к провайдерскому слою отношения не имеющий.

**Ни один тест не обращается к настоящей модели.** Везде подставной исполняемый
файл: бюджет реальных вызовов этапа равен двум, и тратить его на регрессии
нельзя.

---

## 2. Покрытие требований §22 задания

| Пункт | Что требовалось | Чем закрыт |
|---|---|---|
| A | pipeline → provider routing | `TestPipelineRouting` (4): мост неактивен без привязки, переменная без файла ≠ активен, `_run_cli` реально запускает адаптер (видно по журналу подделки), этап вне белого списка получает ОТКАЗ |
| B | provider selection | `TestProviderSelection` (5): резолвер выбирает затребованного, неавторизованный отвергается, неизвестные поля/провайдер отвергаются, отсутствие требования — не ошибка |
| C | ambient_user | `TestAmbientUser` (3): режим доезжает в привязку, корень раскладки внутри попытки (каталог данных воркера процессу конвейера не сообщается), публичный вид без абсолютных путей |
| D | grant validation | `TestInferenceGrant`: нет файла, чужое задание, чужой провайдер, просрочка, битый JSON, чужая схема, широкие права, символьная ссылка |
| E | grant atomic consumption | `test_single_use_budget_is_consumed_once`, `test_consumption_survives_a_crash`, `test_concurrent_consumption_yields_a_single_winner` (**шесть отдельных процессов**, ровно один победитель) |
| F | exact-once inference state | `TestExactlyOnceInference`: claim блокирует второй вызов, сохранённый результат воспроизводится, ошибочный результат тоже засчитан, потолок вызовов попытки |
| G | crash-after-inference replay | `test_bridge_replays_instead_of_calling_the_model` (подпроцесс не запускается повторно — по журналу), `test_crash_after_call_forbids_an_automatic_retry` |
| H | EventOutbox persistence | покрыт ранее принятыми тестами подсистемы; 11C добавил `test_ledger_survives_a_process_restart` — состояние журнала читается ДРУГИМ процессом |
| I | result envelope sanitizer | `test_raw_answer_is_not_stored_in_the_contract`, `test_heartbeat_carries_no_paths_or_task_ids`, `test_public_view_has_no_absolute_paths` |
| J | credential scrubber | `test_credential_like_string_fails`, `test_private_path_fails`, `test_report_never_contains_the_secret_itself` |
| K | canary marker exclusion | `test_forbidden_literal_fails`, `test_canary_marker_is_not_stored_in_the_repository` |
| L | invalid JSON | `test_invalid_json_is_an_error_not_an_empty_success`, `test_unparsed_json_fails` |
| M | provider exit != 0 | `test_nonzero_exit_code_is_reported`, `test_nonzero_exit_fails` |
| N | timeout | `test_timeout_kills_the_process_group`, `test_timed_out_call_is_still_recorded_in_the_ledger` |
| O | rate limit | `test_rate_limit_text_is_classified` (отказ по лимиту отличается от «сломался») |
| P | task cancellation | покрыт принятыми тестами подсистемы (`cancel_attempt`, `_was_cancelled`); 11C путь отмены не менял |
| Q | worker restart | `test_ledger_survives_a_process_restart` + runtime-сценарий смоука (шаг «рестарт исполнителя не породил нового вызова») |
| R | duplicate claim | принятые тесты подсистемы (`claim_next`, `execution_token`); 11C добавил гонку за `claim` в журнале |
| S | duplicate transport delivery | принятые тесты (I-06 `UploadSession`); в смоуке — повторное чтение задания и рестарт |
| T | ACK replay | принятые тесты (`rewind_to`, `last_seen_seq`) |
| U | no-tools Claude invocation | `test_tools_are_disabled_and_personal_context_neutralized` |
| V | variadic CLI flag regression | `test_no_variadic_flag_takes_a_separate_value` — ни один вариадический флаг не записан отдельным токеном |
| W | stdin=DEVNULL | `test_stdin_is_devnull_for_status_calls` (подделка читает stdin и получает пусто) |
| X | no SSH inference | `test_runtime_path_has_no_remote_execution` (AST по девяти модулям пути вызова), `test_adapter_runs_a_local_absolute_executable` |

Дополнительно, сверх §22:

* `test_prompt_never_appears_in_argv` — I-P5 дословно, поведенчески: секретная
  строка промпта ищется в argv подделки и не находится, а в stdin — находится;
* `test_worker_secrets_do_not_reach_the_subprocess` — I-P2 на живом процессе и
  по НЕредактированному каналу (выгрузка `/proc/$$/environ` в файл);
* `test_binding_env_name_matches_provider_layer` — литерал в `audit_runner` и
  константа провайдерского слоя не разъедутся;
* `test_pipeline_runner_still_does_not_import_the_provider_layer` — граница 11b
  СУЖЕНА, но не снята;
* `test_worker_runtime_never_issues_its_own_grant` — `issue()` не встречается в
  рантайме воркера;
* `TestSyntheticFixture` (6) — фрагмент извлекается из Markdown версии, его
  отсутствие ошибка, переросший фрагмент отвергается, промпт просит
  противоречие, а не эхо.

---

## 3. Изменённые чужие тесты

Один: `tests/test_distributed_workers_provider_gate.py`, заглушка `Exploding`
получила метод `structured_inference`. Причина механическая — рабочий вызов
стал обязательной частью интерфейса адаптера, и абстрактный класс без него не
инстанцируется. Смысл теста не изменился.

---

## 4. Прогоны сквозного сценария

`scripts/smoke_distributed_audit_pipeline_provider_e2e.py`

| Прогон | Проверок | Провалено | Вызовов модели |
|---|---|---|---|
| `--mode fake --provider claude` (CLI-заглушка) | 37 | 0 | 0 |
| `--mode real --provider claude` | 40 | 0 | **1** |
| `--mode real --provider codex` | 40 | 0 | **1** |

В обоих режимах воркер работает **в режиме настоящих провайдеров** — мост
существует только там. Разница между режимами одна: что стоит по пути адаптера,
настоящий CLI или заглушка, путь к которой задаёт администратор машины
переменной `AUDIT_WORKER_PROVIDER_<X>_EXECUTABLE`.

Прежний вариант «fake = `AUDIT_WORKER_ALLOW_REAL_LLM=false`» проверял бы ДРУГУЮ
ветку кода (подделки через PATH и `enforce_fake_providers`), в которой моста нет
вовсе, — то есть доказывал бы не то.

---

## 5. Команды воспроизведения

```bash
# Модуль этапа
python -m pytest tests/test_distributed_workers_pipeline_provider.py -v

# Вся подсистема воркеров
python -m pytest tests/test_distributed_workers_*.py -q -p no:randomly

# Сквозной прогон БЕЗ обращения к модели
python scripts/smoke_distributed_audit_pipeline_provider_e2e.py \
    --mode fake --provider claude --issue-grant

# Сквозной прогон С ОДНИМ реальным вызовом (требует разрешения оператора)
python scripts/smoke_distributed_audit_pipeline_provider_e2e.py \
    --mode real --provider claude --i-confirm-one-real-inference \
    --canary /home/coder/provider-auth-canary/DO_NOT_READ.txt
```

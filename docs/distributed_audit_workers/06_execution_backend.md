# Этап 5 — ExecutionBackend: подключение конвейера к распределённому audit-worker

**Статус: EXECUTION BACKEND PARTIAL.** Локальный контур работает и доказанно не
изменился. Четыре предварительных ограничения этапа 4 закрыты. Контракт
исполнения, оба backend'а, тип задания `audit_pipeline_v1`, переносимый пакет
реального проекта, изолированный runtime воркера, поддельные провайдеры и
безопасный приём результата — реализованы и покрыты тестами.

**Подключать тестовый VPS на этом основании нельзя.** Причина одна и она
названа прямо: сквозной прогон реального конвейера на живых процессах (§32
задания) НЕ выполнялся, поэтому утверждение «удалённый аудит проходит целиком»
не подтверждено ничем, кроме чтения кода. Полный список того, что осталось
недоказанным, — §36.

Пять адверсариальных проверок нашли **29 подтверждённых дефектов**, включая
четыре, которые прямо обесценивали гарантии этапа: центральные этапы
ВЫПОЛНЯЛИСЬ на воркере, центр НЕ достраивал аудит после приёма результата,
`.env` из корня установленного кода возвращал процессу ключи платных API, а
дефолтная модель этапа ходила в OpenRouter по HTTPS мимо поддельных CLI. Всё
перечисленное исправлено и закреплено тестами (§34) — но сам факт лучше любых
слов объясняет, почему без сквозного прогона разворачивать нельзя.

Флаги `DISTRIBUTED_WORKERS_ENABLED`, `DISTRIBUTED_AUDIT_EXECUTION_ENABLED`,
`AUDIT_WORKER_AUDIT_PIPELINE_ENABLED`, `AUDIT_WORKER_ALLOW_REAL_LLM` — **все
выключены по умолчанию**. Реальные Claude и Codex в этой работе не вызывались.
VPS 176.12.77.31 не подключался.

Ветка: `feat/distributed-audit-workers-execution-backend`
База: `feat/distributed-audit-workers-prepipeline-gate` (HEAD `f06c64d8`)

---

## 1. Цель этапа

Этап 4 закончился вердиктом **PRE-PIPELINE GATE COMPLETE**: тестовый контур
`test_pipeline_v1` управляем, роли операторов есть, два задания на одном воркере
доказаны на настоящих процессах. Не хватало главного — самого аудита: платформа
о подсистеме воркеров не знала, `PipelineManager` не имел точки врезки, а
единственный тип задания был игрушечным.

Задача этапа — ввести абстракцию исполнения, сохранив локальный аудит без
изменения поведения, и довести удалённый путь до состояния, в котором его можно
осмысленно проверять на тестовом VPS.

---

## 2. Исходный PRE-PIPELINE GATE COMPLETE

Что уже работало и здесь не переписывалось: SQLite WAL центра и воркера,
миграции до версии 4, разделение «логическое задание ↔ попытка», операторская
отмена, mark-lost, новая попытка, superseded-результаты, persistent
WorkerCommand, журнал операторских решений, серверные роли (view/operate/admin),
разделение Agent/Executor, доказательство принадлежности процесса, UUID-пути,
Unicode-коды проектов, RetentionManager, предупреждения по диску, безопасный
экран без сборки HTML из строк, два systemd-юнита, 348 тестов и два живых smoke.

Все 348 тестов подсистемы на этой ветке зелёные (§35).

---

## 3. Предварительные исправления (§2 задания)

Все четыре закрыты ДО какой-либо интеграции с `PipelineManager`. Коммит
`f01ccaa0`.

### 3.1. Отмена ещё не выданной попытки

**Что было.** Отмена попытки в состоянии `assigned` шла общим путём: попытка
переводилась в `cancel_requested`, создавался `WorkerCommand`, слот считался
занятым. Для попытки, которую воркер получил, это верно (процесс может идти,
I-06). Для невыданной — нет: три отмены на воркере с лимитом 2 давали
`occupancy_label == "3/2"`, выдача блокировалась, а на офлайн-воркере
держалась до его возвращения (§32.1 п.24 отчёта 05).

**Почему `assigned` — это точно «не выдавали».** Единственный способ передать
работу воркеру — `repositories.claim_next_job_for_worker`, и он атомарно
переводит `assigned → source_uploading` в той же транзакции, что и выборка.
Попытка в `assigned` лежит в очереди центра; процесса нет, отменять некому.

**Что стало.**
`job_service.ALLOWED_TRANSITIONS[JobState.ASSIGNED][JobState.CANCELLED] = (operator,)`
плюс `attempt_service._cancel_undispatched`: прямой переход в `cancelled`,
`WorkerCommand` не создаётся, слот освобождается сразу, действие пишется в
журнал решений, повтор идемпотентен через `Idempotency-Key`.

**Гонка разрешается транзакционно, а не проверкой.** Обе стороны пишут под
`BEGIN IMMEDIATE` через общий writer-лок. Выдача успела первой → отмена видит
не-`assigned` и уходит на обычный путь `cancel_requested` с командой. Отмена
успела первой → условный `UPDATE ... WHERE execution_state='assigned'` не
находит строку. Дополнительно `claim_next_job_for_worker` теперь проверяет
`rowcount` и возвращает «нечего выдавать» вместо того, чтобы отдать воркеру
попытку с фиктивным состоянием.

Тесты: `test_assigned_cancel_is_terminal_and_creates_no_command`,
`test_assigned_cancel_frees_the_slot`, `test_assigned_cancel_is_idempotent`,
`test_dispatched_attempt_still_uses_cancel_requested`,
`test_cancel_loses_race_to_dispatch_and_does_not_lie`,
`test_dispatch_loses_race_and_returns_nothing`.

### 3.2. Свежесть эффективного лимита

**Что было.** `effective_limit` считался один раз в роутере ДО входа в цикл
long-poll и передавался во все итерации как `limit_override`. Возраст снимка
доходил до `wait_sec` (≤60 с): отзыв регистрации, уход воркера в offline и
критический диск, случившиеся внутри окна, текущий вызов не видел.

**Что стало.** `/jobs/next` больше не передаёт `limit_override`. Лимит
вычисляется ВНУТРИ транзакции захвата, на каждой итерации ожидания, по свежей
строке воркера. Дополнительно там же проверяется:

* **связь** — пересчитывается через `worker_registry.compute_connectivity` по
  ТЕКУЩЕМУ времени центра, а не берётся из колонки `connection_status` (её
  двигает только heartbeat);
* **отзыв доступа** — `repositories.worker_access_revoked`: все выданные
  воркеру токены отозваны. Воркер, которому токен никогда не выдавался,
  отозванным не считается — у него нечего отзывать, а на живом пути он и не
  дойдёт до захвата (`require_worker` пускает только по действующему токену);
* **состояние исполнителя** — передаётся подсказкой из запроса
  (`executor_status_hint`), она свежее снимка heartbeat;
* диск, `configured_max_slots`, `max_verified_slots`, занятость — как прежде.

Снимок лимита в роутере остался, но только для диагностики в ответе 409.

Тесты: `test_revoked_worker_gets_no_work_inside_claim`,
`test_offline_agent_gets_no_work_by_current_time`,
`test_executor_offline_hint_blocks_claim`,
`test_jobs_next_no_longer_passes_stale_limit` (машинная проверка того, что
роутер не вернул `limit_override` обратно).

### 3.3. Старт Agent при недоступном центре

**Что было.** `registration.ensure_registered` при старте делал
`PUT /registration`, и `httpx.ConnectError` уходил наружу — процесс завершался
с traceback. Под systemd с `Restart=always` это давало крэш-луп до возвращения
центра, при живых процессах аудита и растущем журнале событий, который никто не
досылал (§32.10 отчёта 05).

**Что стало.** С токеном на диске обновление регистрации перестало быть
условием запуска: агент стартует, открывает локальную базу, находит свои
задания (`_startup_reconcile` → `_adopt_surviving_attempts`), исполнитель
работает независимо, а связь восстанавливается ограниченным exponential backoff
с джиттером (`client.backoff_delays`, 1→30 с, ±20 %). Процессы не убиваются,
EventOutbox не сбрасывается, повторные задания не создаются.

**Диагностические состояния разделены** (`registration.classify_center_failure`):
`center_unreachable` (DNS, connect, таймаут, 5xx) — повод ждать;
`center_tls_error`, `center_auth_error` (401/403), `center_protocol_error` (426)
— ожиданием не лечатся и требуют человека. Состояние пишется в
`worker_state.json` и в лог агента при смене. Чужое исключение классификатор не
глотает, а пробрасывает.

Тесты: `test_center_failures_are_classified_separately`,
`test_unknown_exception_is_not_swallowed_as_network`,
`test_agent_starts_when_center_is_unreachable`,
`test_agent_records_recovery_of_connection`.

### 3.4. Ограничение частоты заявок на регистрацию

**Готового механизма в проекте нет** — проверено: в `backend/app` четыре
middleware (`CurrentObjectMiddleware`, gzip, `PortalAuthMiddleware`,
`ActionLogMiddleware`), ни один не ограничивает частоту; внешних библиотек вроде
`slowapi` в зависимостях нет.

**Счётчик в памяти не годится** по двум причинам, и обе не гипотетические: его
обнуляет рестарт backend (а рестарт делает вотчдог), и он не общий для
нескольких воркеров uvicorn. Поэтому состояние живёт в `workers.db` —
миграция 5, таблица `registration_rate_limit`
(`backend/app/services/distributed_workers/rate_limit.py`).

Модель: фиксированное окно, два независимых счётчика — по паре
(IP, `instance_id`) и по IP целиком. Ключи хранятся ХЭШАМИ: в базе не должно
лежать ни адреса, ни `instance_id` открытым текстом, иначе таблица сама станет
оракулом существования воркера.

Свойства:

* **списание идёт ДО проверки bootstrap-секрета** — иначе перебор секрета не
  ограничивался бы вовсе. Обратная сторона названа честно: поток неверных
  заявок с одного адреса временно закрывает регистрацию и легальному воркеру с
  того же адреса. Сторона ошибки выбрана осознанно;
* адрес берётся из фактического соединения (`request.client.host`), не из
  `X-Forwarded-For`: заголовок подконтролен тому, кого мы ограничиваем;
* ответ **429 + `Retry-After`**, текст одинаков для известного и неизвестного
  `instance_id`;
* обе корзины ПРОВЕРЯЮТСЯ до инкремента: отказ по второй не съедает квоту
  первой;
* повторная регистрация того же `instance_id` внутри лимита остаётся
  идемпотентной (возвращается та же запись, claim-secret не перевыпускается);
* очистка — удаление окон старше четырёх окон на любой записи;
* нули в обоих порогах выключают ограничитель ЯВНОЙ настройкой.

Конфигурация: `DISTRIBUTED_WORKERS_REGISTRATION_RATE_WINDOW_SEC` (3600),
`..._MAX_PER_INSTANCE` (10), `..._MAX_PER_IP` (30).

Тесты: восемь, включая `test_registration_rate_limit_survives_restart`,
`test_registration_rate_limit_counts_wrong_secret`,
`test_registration_rate_limit_does_not_leak_existence`,
`test_registration_rate_limit_stores_only_hashes`,
`test_registration_rate_limit_does_not_double_charge_one_request`.

---

## 4. Что реализовано

| Требование задания | Где | Проверка |
|---|---|---|
| Абстракция ExecutionBackend | `pipeline/execution/contracts.py` | `test_contract_declares_eight_operations` |
| Типизированные модели запроса/итога | там же | `test_request_forbids_command_and_path_fields` |
| LocalExecutionBackend как делегат | `pipeline/execution/local.py` | `test_local_backend_delegates_with_identical_arguments` |
| RemoteWorkerExecutionBackend | `pipeline/execution/remote.py` | `test_remote_backend_never_calls_local_dispatch` |
| Выбор backend'а и флаги | `pipeline/execution/registry.py` | `test_remote_item_without_flag_is_refused_not_run_locally` |
| Врезка в PipelineManager | `manager.py::_execute_item` | `test_local_path_goes_straight_to_dispatch_action` |
| Persisted backend state | `models/audit.py::BatchQueueItem` | `test_queue_item_roundtrips_execution_handle` |
| Совместимость старой очереди | там же | `test_old_queue_json_reads_as_local` |
| backend-specific liveness | `manager._remote_items`, `remote.liveness` | `test_cleanup_zombies_never_touches_remote_items` |
| Тип задания `audit_pipeline_v1` | `models/distributed_workers.py::JobType` | `test_job_type_enum_is_closed` |
| Строгая нагрузка задания | `AuditPipelineParams` (`extra="forbid"`) | `test_audit_params_model_forbids_execution_fields` |
| Профиль `remote_audit_pilot_v1` | `REMOTE_AUDIT_PILOT_V1` + список этапов | `test_worker_rejects_unknown_profile_and_action` |
| Пакет реального проекта | `services/distributed_workers/project_package.py` | `test_package_scan_excludes_secrets_and_regenerables` |
| Сохранение хардлинков | там же | `test_package_preserves_hardlinks` |
| Снимок prompts | `collect_prompt_snapshot` | `test_prompt_snapshot_hash_is_stable_and_content_sensitive` |
| Снимок stage_models | `collect_model_config_snapshot` | `test_package_manifest_has_required_fields` |
| Снимок feature flags без секретов | `collect_feature_flags_snapshot` | `test_feature_flags_snapshot_drops_secrets` |
| Сканер секретов как рубеж | `find_secrets_in_files` | `test_source_package_contains_no_secrets` |
| Создание задания аудита | `audit_job_service.create_audit_job` | §14 ниже |
| Проверка совместимости воркера | `audit_job_service.compatibility_report` | `test_audit_targets_explains_incompatibility` |
| Изолированный runtime воркера | `audit_worker/audit_runner.py` | `test_worker_env_is_an_allowlist_and_points_inside_job_dir` |
| Фиксированный argv | `audit_runner.build_argv` | `test_worker_builds_fixed_argv` |
| Повторная валидация на воркере | `audit_runner.validate_params` | шесть тестов §5 файла |
| Использование существующих stage runners | `pipeline/remote_audit_runner.py` | `test_runner_refuses_norms_and_unknown_profile` |
| Поддельные провайдеры | `pipeline/execution/fake_providers.py` | `test_fake_providers_are_marked_and_executable` |
| Fail-closed без подделок | `executor._provider_dir` | `test_executor_fails_closed_without_fake_providers` |
| Один реальный аудит на воркер | `executor.audit_slot_conflict` | `test_real_audit_and_test_jobs_never_mix` |
| Аудит и тесты не смешиваются | `executor.test_slot_conflict` | `test_running_test_job_blocks_real_audit` |
| Хардлинки в распаковщике воркера | `package_io.verify_and_unpack` | `test_worker_unpacker_allows_hardlinks_but_only_inside_payload` |
| Result package реального аудита | `package_io.build_result_package` | `test_worker_package_never_returns_source_files` |
| Безопасный import через staging | `services/distributed_workers/result_import.py` | `test_result_import_applies_only_generated_paths` |
| Разделение source/worker/central | `result_import.classify_path` | `test_path_classification_matches_the_contract` |
| Откат применения по журналу | `result_import.rollback_applied` | `test_result_import_rolls_back_on_failure` |
| Идемпотентность и конфликт | `import_result_for_attempt` | `test_result_import_is_idempotent_and_detects_conflict` |
| Usage один раз | `result_import.apply_usage_report` | `test_usage_report_applies_exactly_once` |
| Ручной запуск (API) | `POST /api/workers/audit/launch` | `test_audit_launch_requires_operator_and_intent` |
| Совместимые воркеры (API) | `GET /api/workers/audit/targets` | `test_audit_targets_explains_incompatibility` |
| UI: готовность и запуск | `audit-workers.{html,js}` | оба прежних теста на запрет сборки HTML зелёные |

---

## 5. Что намеренно НЕ реализовано

Граница этапа (§3.2 задания). Каждый пункт — явное «нет»:

* **подключение VPS 176.12.77.31** — не выполнялось;
* **SSH-развёртывание** — нет;
* **реальные вызовы Claude Code и Codex** — не выполнялись ни в одном прогоне;
  подписочные лимиты не расходовались;
* **автоматический выбор воркера**, выбор по близости сброса лимита,
  QuotaAdapter — нет; воркер выбирает оператор;
* **нормативный этап на воркере** — запрещён конструкцией (§20);
* **до 5 реальных аудитов**, более одного `audit_pipeline_v1` на VPS,
  распределение этапов одного проекта между VPS — нет;
* **S3, Redis, RabbitMQ, Kubernetes** — нет;
* **произвольные shell-команды**, передача исходного кода через пакет,
  автообновление воркера — нет;
* **автоматическое переназначение remote audit** после потери связи и
  автоматическое слияние результата superseded-попытки — нет;
* **промышленный rollout** — нет.

Двухслотовая поддержка сохранена для `test_pipeline_v1`; для
`audit_pipeline_v1` максимум — один.

---

## 6. ExecutionBackend

`backend/app/pipeline/execution/contracts.py`.

Восемь операций: `prepare`, `start`, `status`, `wait`, `cancel`, `liveness`,
`reattach`, `collect_result`. `run()` — не девятая операция, а композиция
остальных; локальный backend её переопределяет, чтобы остаться ОДНИМ вызовом
прежнего `_dispatch_action`.

Модели:

| Модель | Что несёт |
|---|---|
| `ExecutionRequest` | project_id, version_id, object_id, job_id, `AuditExecutionOptions`, execution_mode, assigned_worker_id, execution_profile, pipeline_revision, correlation_id |
| `ExecutionHandle` | backend_type, handle_id, project_id, version_id, attempt_id, remote_job_id, worker_id, execution_profile, created_at |
| `ExecutionSnapshot` | execution_state, connectivity_state, stage, progress, last_event_at, liveness + причина, error |
| `ExecutionResult` | success, cancelled, package_id, package_hash, returned_artifacts, resume_stage, usage_report, error |
| `LivenessVerdict` | state (`alive`/`unknown`/`dead`), **обязательная причина**, last_signal_at, connectivity |

**Через интерфейс не проходит** shell-команда, argv, имя или путь исполняемого
файла, переменные окружения, произвольный callable, абсолютный путь. Не «не
принято передавать» — соответствующих полей нет, а `extra="forbid"` превращает
попытку в ошибку валидации (`test_request_forbids_command_and_path_fields`).

Живые объекты платформы (`item`, `job`) вынесены в отдельный `ExecutionContext`,
который НИКОГДА не сериализуется и не уходит на воркер: запрос обязан быть
сериализуемым и не содержать ссылок на объекты процесса.

`Liveness` имеет три значения, и «не знаю» — полноценное из них. Для удалённой
работы это прямой инвариант: отсутствие сигнала никогда не означает `DEAD`.

---

## 7. LocalExecutionBackend

`backend/app/pipeline/execution/local.py`.

Содержательная часть `run()` — одна строка:

```python
await self._manager._dispatch_action(
    ctx.item, ctx.job,
    default_action=ctx.default_action,
    action_override=ctx.action_override,
)
```

Аргументы и их порядок совпадают с прежним вызовом из `_batch_slot_worker`
дословно. Всё, что было до и после (регистрация job в `active_jobs`, ContextVar
`bind_object`/`bind_version`, статусы item'а, cleanup, `_persist_queue`,
broadcast) осталось у менеджера и сюда не переехало: **перенос кода ради
красоты интерфейса и есть тот способ, которым ломают работающие конвейеры.**

Больше того: локальный путь вообще не создаёт объект backend'а —
`_execute_item` для `execution_mode == local` вызывает `_dispatch_action`
напрямую (`test_local_path_goes_straight_to_dispatch_action`). Класс существует
для контракта: `status`, `liveness`, `cancel`, `reattach` дают менеджеру
единообразный способ спрашивать, а отвечают по УЖЕ существующим сигналам
платформы (`has_live_processes`, живой asyncio-таск, in-memory job). Новых
источников истины локальный backend не заводит.

`reattach` возвращает `None` осознанно: локальное исполнение рестарт центра не
переживает, и прежний механизм (`load_persisted_queue` → `interrupted` →
авто-resume с места обрыва) остаётся единственным.

Регрессионные тесты локального контура — `tests/test_batch_queue_*.py`,
`test_pipeline_cancel_propagation.py`, `test_pipeline_queue_single_flight.py`,
`test_resume_detector.py`: 58 passed, без изменений.

---

## 8. RemoteWorkerExecutionBackend

`backend/app/pipeline/execution/remote.py`.

Что делает:

1. `prepare` — проверяет воркер и совместимость, проверяет отсутствие активного
   исполнения того же проекта и версии, собирает immutable source package,
   создаёт логическое задание и попытку в `workers.db`, назначает воркер,
   сохраняет `ExecutionHandle` в элемент очереди и на диск;
2. `start` — ничего не запускает. Воркер забирает задание сам, опросом: порт на
   VPS наружу не открыт, и это свойство архитектуры;
3. `wait` — опрашивает центральное хранилище и транслирует события в
   СУЩЕСТВУЮЩИЕ WebSocket-сообщения (`WSMessage.progress`, `WSMessage.log`).
   Своего формата не заводится: фронтенд не должен отличать локальный аудит от
   удалённого;
4. `collect_result` — передаёт результат в импортёр (§25), который применяет его
   ровно один раз.

Чего НЕ делает — важнее:

* **не зовёт `_dispatch_action`** ни при каких обстоятельствах. Проверяется
  машинно по дереву разбора: ни одного обращения к атрибуту, ни импорта
  `subprocess`/`paramiko`/`pexpect`, ни упоминания `read_token`/`worker_token`
  в AST (`test_remote_backend_never_calls_local_dispatch`);
* **не удерживает HTTP-запрос** до конца аудита: работа идёт в корутине слота
  очереди, состояние живёт в `workers.db`;
* **не считает отсутствие heartbeat доказательством остановки**;
* **не создаёт новую попытку сам** и не переносит задание другому воркеру.

**Идемпотентность `prepare`** — не удобство, а инвариант: повторный HTTP-запуск,
рестарт центра и авто-resume прерванного элемента возвращают СУЩЕСТВУЮЩИЙ
handle, если попытка ещё есть в базе
(`test_remote_backend_reuses_existing_handle`: после второго `prepare` заданий
у воркера по-прежнему одно).

---

## 9. Интеграция с PipelineManager

Точка врезки одна, ровно там, где её нашёл первый аудит архитектуры
(§6.1 отчёта 01): в `_batch_slot_worker`, перед фактическим вызовом
`_dispatch_action`. Изменения в `manager.py`:

| Место | Что изменилось |
|---|---|
| `_batch_slot_worker` | `await self._dispatch_action(...)` → `await self._execute_item(...)` |
| `_execute_item` (новый) | локально — прежний вызов; удалённо — backend + маппинг итога в `job.status` |
| `_remote_items` (новый) | элементы очереди, исполняющиеся удалённо, по project_id |
| `_protected_pids` | удалённые задания защищены ВСЕГДА, независимо от живости batch-worker |
| `cleanup_zombies` | явный пропуск удалённых элементов (дублирует защиту намеренно) |
| `_has_live_project_audit` | наличие удалённых элементов = живой аудит |
| `cancel` | маршрутизация в `_cancel_remote_item` ДО всего остального |
| `_cancel_remote_item` (новый) | команда воркеру вместо убийства локальных процессов |
| `_enqueue_single` | три новых необязательных аргумента |
| `start_remote_audit` (новый) | одиночный удалённый запуск с проверкой батча |

`manager.py` НЕ импортирует ни подсистему воркеров, ни пакет `audit_worker`:
проверяется машинно (`test_pipeline_manager_knows_nothing_about_the_worker_subsystem`).
Всё идёт через `backend.app.pipeline.execution`.

**Почему `_protected_pids` и `cleanup_zombies` защищают remote дважды.** Это не
избыточность. Первый аудит назвал ложный зомби самым опасным классом дефекта
всей затеи: все три существующих гейта живости опираются на ЛОКАЛЬНЫЕ сигналы
(живые дочерние процессы, живой asyncio-таск, живой batch-worker), которых у
удалённого задания на центре нет по определению. Если кто-то однажды изменит
состав `protected`, remote-задание не должно молча стать зомби и уехать в
resume, где `_clean_stage_files` удаляет `03_findings.json`.

**Batch остаётся локальным.** `add_to_batch` не принимает ни `execution_mode`,
ни `worker_id` (проверяется чтением исходника в тесте). Попытка удалённо
запустить проект, участвующий в активной групповой очереди, отклоняется понятной
ошибкой в `start_remote_audit`.

---

## 10. Persisted backend state

`BatchQueueItem` получил четыре поля, все с дефолтами:

```
execution_mode: str = "local"          # local | remote_worker
worker_id: Optional[str] = None
execution_profile: Optional[str] = None
execution_handle: dict = {}
```

Старый `batch_queue.json` читается без изменений и трактуется как локальный:
элемент, записанный прошлой версией, не может внезапно оказаться удалённым
(`test_old_queue_json_reads_as_local`). Битое поле `execution_handle` не ломает
очередь — `registry.handle_from_item` возвращает `None`
(`test_broken_handle_does_not_break_the_queue`). `execution_mode=remote_worker`
без `worker_id` трактуется как локальный, а не падает.

`workers.db` — источник истины для удалённого задания, попытки, назначения,
состояния исполнения, disposition, событий, upload, команд и связи.
`worker.db` — для локальной очереди воркера, реестра процессов, EventOutbox,
локальных пакетов и подтверждения приёма. Третьей базы не создавалось.
Миграция 5 добавила в центральную схему `logical_jobs.execution_profile`,
`logical_jobs.pipeline_revision` и пять полей приёма результата в
`job_attempts`; представление `remote_jobs` пересоздано, запрет записи через
него сохранён.

---

## 11. cleanup_zombies

Правила для удалённого задания:

* **запрещено** считать его зомби из-за отсутствия локального процесса;
* **запрещено** переводить `running` в `failed` по heartbeat timeout;
* **запрещено** вызывать `_clean_stage_files` для живой удалённой попытки;
* **запрещено** автоматически возобновлять его локально.

`RemoteWorkerExecutionBackend.liveness` возвращает `DEAD` ровно в одном случае —
попытка достигла терминального состояния своим ходом. `operator_declared_lost`
даёт `UNKNOWN` с текстом «процесс на VPS мог остаться жив». `offline` даёт
`UNKNOWN` с текстом «это НЕ основание считать работу остановленной»; связь при
этом считается по строке ВОРКЕРА (`last_seen_at`), а не по колонке
`job_attempts.connectivity_state` — у той нет ни одного писателя, и опора на неё
означала «связь есть всегда» (§34.1 п. 15).

Живым удалённым исполнением считается элемент со статусом `running` либо
`interrupted` И непустым `execution_handle`. Оба условия существенны:
`pending`-элемент в этом множестве парализовывал очередь целиком, а элемент без
handle ничего на воркере не занимает (§34.1 п. 7).

Тесты: `test_cleanup_zombies_never_touches_remote_items`,
`test_local_zombie_detection_still_works` (обратная сторона: локальный
протухший job по-прежнему снимается),
`test_remote_liveness_never_reports_dead_on_offline`,
`test_remote_liveness_dead_only_on_terminal_state`.

---

## 12. Restart и reattach

| Сценарий | Что происходит |
|---|---|
| Рестарт центра | `load_persisted_queue` переводит running → interrupted; `execution_mode` и `execution_handle` сохранены; авто-resume отдаёт элемент в `_execute_item`; remote backend в `prepare` находит существующую попытку и возвращает ТОТ ЖЕ handle — второго задания не создаётся |
| Рестарт PipelineManager | то же |
| Рестарт Agent | без изменений с этапа 4: исполнитель и процесс живут, агент делает reconciliation, `_adopt_surviving_attempts` заводит контекст, события досылаются |
| Рестарт Executor | pid + тик старта + отпечаток + `completed.marker`, а для реального аудита ещё и `work/process_exit.json`, который пишет САМ процесс конвейера последним действием. Без него завершённый многочасовой прогон объявлялся прерванным: `completed.marker` пишет наблюдатель, а он рестартом и умер (§34.1 п. 25). Автоповтора нет |
| Потеря связи | процесс работает, EventOutbox растёт, пакет результата создаётся; после связи всё уезжает |

`reattach` проверен тестами `test_remote_reattach_finds_attempt_and_creates_nothing`
(число заданий до и после равно одному) и
`test_remote_reattach_returns_none_for_missing_attempt`.

**Честная граница.** Перечисленное проверено НА УРОВНЕ КОДА И ЮНИТ-ТЕСТОВ.
Прогона на живых процессах с реальным конвейером не было — см. §36.

---

## 13. Ручной remote launch

`GET /api/workers/audit/targets` (право `view`) — список воркеров с отчётом
совместимости. Несовместимый воркер не «не показывается», а показывается с
точным кодом причины: `center_revision_missing`, `code_revision_mismatch`,
`protocol_mismatch`, `missing_capability`, `not_approved`, `agent_offline`,
`executor_offline`, `disk_critical`, `audit_slot_busy`, `test_jobs_running`.
Молчаливое «недоступен» оператор не может ни понять, ни исправить.

`POST /api/workers/audit/launch` (право `operate`, гейт намерения
`X-Requested-With: audit-workers`, обязательный `Idempotency-Key`) — постановка
в очередь через `pipeline_manager.start_remote_audit`.

Ответ говорит правду о последствиях: профиль, что нормативный этап останется на
центре, и включены ли на воркере НАСТОЯЩИЕ Claude/Codex.

UI (`frontend/audit-workers.{html,js}`): карточка VPS получила строку «Реальный
аудит» (capability, режим провайдеров, слот аудита, ревизия, причина
несовместимости), плюс панель ручного запуска с выбором только из совместимых
воркеров и подтверждением фразой. Разметка — только `createElement` +
`textContent`; оба прежних машинных теста на запрет сборки HTML из строк
зелёные.

---

## 14. audit_pipeline_v1

Второй и последний тип задания. `JobType` закрыт двумя ИМЕНАМИ реализаций;
машинная проверка убеждается, что ни одно значение не содержит `shell`, `exec`,
`eval`, `script`, `argv`, `command`, `python`.

`AuditPipelineParams` (`extra="forbid"`):

| Поле | Тип | Смысл |
|---|---|---|
| `execution_profile` | `Literal["remote_audit_pilot_v1"]` | профиль один |
| `action` | `Literal["full","audit","resume"]` | закрытый набор |
| `retry_stage` | этап из `REMOTE_AUDIT_PILOT_STAGES` | валидатор отвергает остальное |
| `include_optimization` | bool | |
| `include_norms` | `Literal[False]` | «случайно передать true» невозможно |
| `project_layout_version` | int | |
| `pipeline_revision` | str | сверяется воркером |
| `expected_source_tree_hash`, `prompt_bundle_hash`, `model_config_hash`, `feature_flags_hash` | str | сверяются на воркере |
| `required_result_artifacts` | list[str] | воркер берёт пересечение со СВОИМ |

**Чего в модели нет:** command, argv, executable, script, module, cwd, env,
hook, tool list, любой путь. Поля просто нет — значит его нельзя ни прислать,
ни «случайно поддержать».

---

## 15. remote_audit_pilot_v1

Профиль один и фиксированный. Разрешённые этапы перечислены В КОДЕ
(`REMOTE_AUDIT_PILOT_STAGES`), а не приходят произвольным JSON от центра:
`crop_blocks`, `document_graph`, `block_context`, `block_analysis`,
`text_analysis`, `findings_merge`, `findings_review`, `optimization`,
`optimization_review`.

`CENTRAL_ONLY_STAGES` — `norm_verify`, `debt_control`, `decision_carryover`,
`excel`. Запрет стоит в ЧЕТЫРЁХ местах, и четвёртое — единственное, которое
реально работает для `action="full"`:

1. валидатор `retry_stage` в модели задания;
2. `FORBIDDEN_STAGES` в `remote_audit_runner.load_spec` — тоже только для
   явного `retry_stage`;
3. `CENTRAL_ONLY_ARTIFACTS` в импортёре: приход норм-артефакта из пакета
   воркера отклоняет ВЕСЬ пакет;
4. **процессный гейт `AUDIT_PIPELINE_CENTRAL_STAGES_DISABLED`** —
   `PipelineManager._central_stage_blocked` в `_run_norm_verification`,
   `_run_debt_control`, `_run_decision_carryover` и на обеих точках Excel.

Четвёртый добавлен по итогам адверсариальной проверки: первых трёх было
НЕДОСТАТОЧНО, и `action="full"` спокойно выполнял все четыре центральных этапа
на воркере (§34.1 п. 1). Единица запрета — процесс, потому что процесс
`remote_audit_runner` целиком является удалённой ногой одного аудита; на центре
переменная не выставлена никогда.

Семантика (§5 задания): центр формирует пакет → воркер прогоняет допустимые
этапы в изолированном каталоге → воркер собирает result package → центр
принимает, проверяет и применяет → существующий resume detector определяет
следующий этап → нормативный этап и финальная генерация идут на центре.

---

## 16. Source package

`backend/app/services/distributed_workers/project_package.py`.

**TAR, а не ZIP.** 18 % файлов корпуса (36 673 из 199 016) — хардлинки, из них
34 932 кропа блоков после дедупликации. ZIP не имеет типа записи «жёсткая
ссылка» вовсе, и пакет раздувается на 40 %. Здесь хардлинки сохраняются явной
картой инодов: первый файл каждого инода кладётся обычной записью, остальные —
типом `link`, а карта групп уезжает в манифест.

**Сканирование дерева, а не список путей.** Раскладка версий НЕОДНОРОДНА: у
одной версии `pipeline_log.json` в `99_service/`, у другой каталога
`99_service/` нет вовсе. Фиксированный список путей на таком корпусе молча
теряет артефакты, а resume-детектор на воркере после этого начинает конвейер не
с того этапа.

Исключается: `.git`, `.env*`, `.venv`, `__pycache__`, `node_modules`, `.claude`,
`.codex`, `.ssh`, токены и claim-секреты, `workers.db`/`worker.db`,
`batch_queue.json`, `paid_cost*`, `usage_data.json`, `decisions_log.json`,
`norms_paragraphs.json`, PID/lock/sock/WAL/SHM, ключи и сертификаты,
восстановимый `_stage02_paid_response_cache`, `.evicted`. Симлинки не
переносятся и не разыменовываются.

Отдельно исключены **центральные артефакты** (`norm_checks.json`,
`03a_norms_verified.json`, `decision_carryover_report.json`,
`migrated_findings_report.json`) и **`expert_review.json`**. Причина не
секретность, а асимметрия: сборщик пакета результата возвращает всё дерево
`03_analysis/`, а импортёр отклоняет ВЕСЬ пакет, увидев центральный артефакт —
то есть любой повторный аудит версии, где нормы уже проходили, выбрасывал
многочасовой прогон целиком (§34.1 п. 18). Следствие, названное прямо: на
воркере вердиктов эксперта не видно; вернёт их `decision_carryover` на центре.

Манифест содержит: `manifest_version`, `package_id`, `package_type=source`,
`job_id`, `attempt_id`, `project_id`, `project_external_id`, `version_id`,
`execution_profile`, `pipeline_revision`, `worker_protocol_version`,
`project_layout_version`, `created_at`, `compression`, `source_tree_hash`,
`prompt_bundle_hash`, `model_config_hash`, `feature_flags_hash`,
`global_snapshot_hash`, `required_inputs`, `excluded_regenerable_paths`,
`files`, `hardlinks`, `total_size`, `uncompressed_size`, `limits`, `archive`.

Пути внутри архива — только относительные POSIX, под `payload/project/` и
`payload/snapshot/`.

**Что НЕ сделано:** переписывание абсолютных путей внутри артефактов
(`pipeline_log.artifacts_dir`, `block_context_summary.project_dir`,
`stage01_meta.runtime_plan_path`). Первый аудит перечислил эти поля как
требующие обработки при распаковке. Здесь их не трогали — см. §36.

---

## 17. Prompt/config snapshot

`audit_job_service.build_snapshot` делает immutable-снимок на КОНКРЕТНУЮ попытку:

* `prompts/` — только `.md`/`.txt`/`.json`, относительные пути;
* `stage_models.json` — файл вне git, и без него прогон пошёл бы на других
  моделях: прод на codex-ногах, дефолты в коде на Claude Opus;
* профиль флагов — только известные префиксы (`AUDIT_`, `PIPELINE_`, `STAGE01_`,
  `STAGE02_`, `FINDINGS_`, `BLOCK_`, `BUDGET_`, `PAID_API_`, `CRITIC_`,
  `NORMS_`), и каждый ключ проходит фильтр по имени: `secret|token|password|
  api_key|credential|cookie|bootstrap` в снимок не попадает.

Хэши всех трёх едут в манифесте и в нагрузке задания. Воркер после распаковки
пересчитывает их (`remote_audit_runner.verify_snapshot`) и при расхождении
завершается с явной ошибкой. Изменение конфигурации центра после старта на
текущую попытку не влияет.

Модель задаётся ЛОГИЧЕСКИМ именем через снимок; имя бинарника центр не
передаёт никогда.

---

## 18. Изолированный worker runtime

`audit_worker/audit_runner.py` — вторая и последняя точка запуска процесса в
пакете воркера (третья, `__main__.py`, — dev-самозапуск исполнителя). Машинный
тест `test_only_one_subprocess_spawn_point` перечисляет все три поимённо.

Каталог попытки: `WORKER_DATA_DIR/jobs/<job_uuid>/<attempt_uuid>/` с
подкаталогами `source_package`, `unpack_staging`, `project`, `snapshot`, `work`,
`result`, `logs`, `metadata`, `package_output`, `usage`.

**argv фиксирован:** `[python, "-u", "-m", PIPELINE_ENTRYPOINT_MODULE, spec]`.
Имя модуля — константа файла воркера. Интерпретатор — тот же, что у
исполнителя, либо `AUDIT_WORKER_PIPELINE_PYTHON`. Путь к установленному коду —
`AUDIT_WORKER_PIPELINE_ROOT`, задаёт администратор VPS: путь к исполняемому
коду не может приходить из задания.

**env — белый список** `PATH`, `LANG`, `LC_ALL`, `HOME`, `TMPDIR`, `TZ` плюс
вычисленные корни данных, каждый ВНУТРИ каталога попытки:
`AUDIT_DATA_DIR`, `AUDIT_APP_DATA_DIR`, `AUDIT_PROJECTS_DIR`,
`AUDIT_PROJECTS_V2_DIR`, `AUDIT_PROMPTS_DIR`, `AUDIT_ACTION_LOG_DIR`, `TMPDIR`.
Ни одна переменная не приходит из задания; секретов воркера здесь нет —
исполнитель их и не знает.

`remote_audit_runner.apply_runtime_paths` ПРОВЕРЯЕТ все шесть корней (раньше —
три, и среди пропущенных была рабочая для legacy-раскладки
`AUDIT_PROJECTS_DIR`), а также требует, чтобы `AUDIT_ROOT_DIR`/`AUDIT_BASE_DIR`
не были заданы вовсе. Смысл проверки не в недоверии к воркеру, а в том, что
процесс, запущенный руками с неполным окружением, не должен писать в чужие
каталоги.

**Белый список env бесполезен без `AUDIT_DISABLE_DOTENV=1`.** Конфигурация
платформы вызывает `load_dotenv()` на импорте, а тот ищет `.env` вверх от
своего файла и находит его в корне УСТАНОВЛЕННОГО кода — возвращая процессу
ключи платных провайдеров и `PAID_API_ENABLED=true`. Переменную выставляют оба:
воркер в `build_env` и сам runner первым действием, до любого импорта
`backend.app` (§34.1 п. 3).

**`project_id` проверяется как ЧАСТЬ ПУТИ** (`validate_project_id`): он
приходит в задании, а `resolve_project_dir` делает `projects_dir / project_id`.
Непроверенное значение с `..` или абсолютным путём выводило и запись, и ЧТЕНИЕ
за каталог попытки, а прочитанное уезжало в `03_findings.json` — то есть в
пакет результата и на центр (§34.1 п. 23).

Отмена адресуется группе процессов, созданной через `start_new_session=True`;
механизм тот же, что для `test_pipeline_v1`, с четырьмя доказательствами
принадлежности.

**Отдельный системный пользователь** — свойство развёртывания (systemd-юниты
`User=audit-worker`, `ProtectSystem=strict`, `ReadWritePaths`), а не кода. В
тестах изоляция проверяется временными каталогами и проверкой значений env; под
отдельным пользователем прогонов не было.

---

## 19. Fake providers

`backend/app/pipeline/execution/fake_providers.py`. Живут на стороне конвейера,
а не в пакете воркера: там запрещены строковые литералы с именами реальных CLI,
и запрет полезен.

Подделки принимают тот же минимальный контракт (промпт через stdin, JSON на
stdout), детерминированы и умеют симулировать: `ok`, `rate_limit` (в формате,
который распознаёт `cli_utils.is_rate_limited`), `auth_error`, `timeout`,
`broken_json`. Поведение задаётся переменной `AUDIT_WORKER_FAKE_BEHAVIOUR`,
которую ставит ТЕСТ.

**Режим — свойство конфигурации воркера, не поля задания.** При
`AUDIT_WORKER_ALLOW_REAL_LLM=false` и отсутствии каталога подделок задание
отвергается fail-closed (`test_executor_fails_closed_without_fake_providers`):
молча уйти к настоящему CLI нельзя.

Каталог обязан быть помечен `PROVIDERS.json` с `mode: "fake"`, и проверка
маркера теперь СТОИТ НА БОЕВОМ ПУТИ (`audit_runner.provider_dir_is_fake` в
`executor._provider_dir`). Раньше достаточно было существования каталога:
пустой каталог префиксует PATH, ничего не перекрывая, и настоящий CLI
находился обычным резолвом — при том, что центру рапортовалось
`provider_mode="fake"` (§34.1 п. 6).

**Подделка CLI сама по себе НЕ закрывает вызов модели** — и это оказалось
главным дефектом раздела. Дефолт этапа `block_batch` (`ensemble/gpt-codex`)
ходит в OpenRouter по HTTPS, а `CLAUDE_CLI_BIN`/`AUDIT_CODEX_CLI_PATH`
резолвят бинарь мимо PATH. Поэтому `remote_audit_runner.enforce_fake_providers`
дополнительно: выключает `PAID_API_ENABLED`, удаляет из окружения ключи девяти
провайдеров, связывает переменные резолва с подделками и отказывает при их
отсутствии (§34.1 п. 4).

**Оркестрация не подменяется.** Этапы, их порядок, запись артефактов и сборка
результата идут настоящие; подделан только последний метр, где процесс
обращается к внешней модели.

---

## 20. Norm handoff

На воркере: `include_norms` — литеральный `False` в модели, отдельная проверка в
`audit_runner.validate_params`, отдельная проверка в
`remote_audit_runner.load_spec` и — главное — процессный гейт §15, который
единственный останавливает `action="full"`. Норм-база не устанавливается,
норм-сервер не поднимается, `norms_paragraphs.json` не пишется.

После возврата: центр валидирует пакет → применяет разрешённые артефакты →
вызывает СУЩЕСТВУЮЩИЙ `detect_resume_stage` (своей логики «какой этап
следующий» импортёр не содержит) → `_run_central_tail_after_remote` выполняет
норм-этап, контроль долгов, перенос вердиктов и Excel на центре, повторяя
хвост `_run_ocr_pipeline` дословно. Раньше этого блока не было вовсе: элемент
помечался `COMPLETED` сразу после приёма, и обещание «нормы на центре» было
ложным (§34.1 п. 2).

Приход норм-артефакта (`norm_checks.json`, `03a_norms_verified.json`,
`decision_carryover_report.json`, `migrated_findings_report.json`) в пакете
воркера отклоняет ВЕСЬ пакет, а не игнорируется
(`test_result_import_rejects_central_only_artifact`, с проверкой, что проект при
этом не тронут ни на один файл).

**Что НЕ проверено:** сценарии §18 задания на живом центре (проект, где
норм-этап требуется; повторный central resume; рестарт центра между import и
norm stage). Юнит-тестами закреплены только гейт этапов и то, что импортёр
получает `resume_stage` из существующего детектора с правильной сигнатурой.

---

## 21. Глобальные mutable-файлы

| Ресурс | Стратегия | Как обеспечено |
|---|---|---|
| `decisions_log.json` | запрет на воркере + central-only writer | нет в пакете (чёрный список имён), нет в разрешённых путях приёма |
| `paid_cost.json`, `paid_cost_events.jsonl` | запрет на воркере + central-only writer | то же; воркер возвращает `usage_report`, применяет центр |
| `usage_data.json` | central-only writer | `result_import.apply_usage_report` пишет через существующий `usage_tracker` ровно один раз; вызывается из `import_result_for_attempt` (раньше не вызывался в проде ни откуда) |
| `norms_paragraphs.json` | запрет на воркере | норм-этап на воркере не выполняется вовсе |
| `missing_norms_vault.json` | запрет на воркере | следствие того же |
| `objects.json`, `users.json`, `batch_queue.json` | не переносятся | чёрный список имён |
| `prompts/`, `stage_models.json` | read-only snapshot попытки | §17 |

Ни один воркер не меняет общий центральный файл напрямую: пути `01_input/`,
`02_work/`, `04_review/`, `discussions/`, `05_export/`, `version.json`,
`document.json`, `project_info.json`, `current_version.txt` классифицируются как
`source` и не применяются; всё вне `03_analysis/` и `99_service/` —
`unknown` и отклоняет пакет.

**Честная оговорка про paid_cost.** Дневной лимит платного API остаётся
локальным для центра: `usage_report` воркера применяется в `usage_tracker`, но
НЕ в `paid_cost.json`. Причина — стоимость удалённого прогона на подписке
Claude/Codex не выражается в тех же единицах, что платный API, и придумывать
конверсию без измерения было бы хуже отсутствия данных. Это ограничение, а не
решение (§36).

---

## 22. Progress и logs

События воркера транслируются в СУЩЕСТВУЮЩИЕ WebSocket-сообщения:
`stage_progress` → `WSMessage.progress` (только при известном `total`),
`stage_started`/`stage_completed`/`job_started` и `log_line` →
`WSMessage.log`, `job_failed`/`quota_warning`/`resource_warning` → лог уровня
error. Дедупликация — по курсору `last_seq`.

**Выдуманного процента нет.** Процент отдаётся только когда воркер прислал
`total`; иначе показываются этап, длительность, последнее событие и число
завершённых операций.

`stdout`/`stderr` процесса конвейера пишутся ПРЯМО В ФАЙЛЫ каталога попытки
(дескрипторы принадлежат самому процессу — тот же урок, что в `test_runner`:
пока трубы держал наблюдатель, его уход убивал работу SIGPIPE'ом). В пакет
результата логи попадают с видимой обрезкой хвостом по 8 МиБ на файл.

---

## 23. Cancel

Удалённая отмена: `manager.cancel(project_id)` видит проект в `_remote_items`
→ `_cancel_remote_item` → `backend.cancel` → `attempt_service.request_cancel` →
persistent `WorkerCommand` → Agent → Executor отменяет только свою группу
процессов → ACK → состояние обновляется. `kill_all_processes` для remote не
вызывается: на центре нечего убивать, а вызов снял бы job с учёта и создал
видимость отмены.

Исходные и уже созданные артефакты не удаляются автоматически. Если попытка
ещё не создана на стороне подсистемы — элемент очереди просто помечается
`cancelled`. Если пакет уже выдан — путь `cancel_requested` + команда, с
честным текстом про офлайн-VPS. Если работа уже закончилась — `already_final`,
результат сохраняется, задним числом попытка не отменяется.

---

## 24. Result package

`audit_worker/package_io.build_result_package` с `job_type="audit_pipeline_v1"`
собирает разделы `project` (только `03_analysis/` и `99_service/`), `work`
(белый список, включая `pipeline_log.json`), `result`, `usage`, `logs`.

**Исходники обратно не едут** — и это первый рубеж защиты §25: сборщик воркера
физически не кладёт `01_input/` и `02_work/` в пакет
(`test_worker_package_never_returns_source_files`).

Манифест: `package_type=result`, `source_package_hash`, `job_id`, `attempt_id`,
`worker_id`, `job_type`, `pipeline_revision`, `execution_profile`,
`stage_completion`, `resume_hint`, `files` с хэшами, `hardlinks`,
`required_artifacts`, `generated_artifacts`, `excluded_artifacts`, `exit_code`,
`cancellation_state`, `created_at`.

**Пакет не помечается успешным при отсутствии обязательного артефакта.**
`audit_runner.missing_required_artifacts` проверяет
`work/pipeline_log.json`, `result/03_findings.json`,
`result/audit_manifest.json`, `usage/usage_report.json`; при недостаче попытка
уходит в `failed` с перечислением. «Успех без 03_findings.json» проходит все
проверки транспорта и потому опаснее явного провала.

---

## 25. Безопасный import

`backend/app/services/distributed_workers/result_import.py`. Пакет НИКОГДА не
распаковывается поверх проекта.

Порядок: upload staging (уже был) → полный SHA-256 и безопасная TAR-проверка
(`package_service.safe_extract`) → распаковка в отдельный staging попытки →
валидация манифеста → сверка `source_package_hash` с тем, что центр отправлял →
сверка `pipeline_revision` → проверка карты завершённых этапов → построение
плана изменений → отказ при недопустимых путях → резервные копии заменяемого →
атомарные замены с `fsync` и журналом → фиксация `result_import_state=applied` →
central resume через существующий детектор.

Разделение путей сделано машинно (`classify_path`): `source` пропускается,
`central` отклоняет пакет, `unknown` отклоняет пакет, `worker` применяется.

Идемпотентность: повторный приём того же пакета отвечает `replayed=True` и
ничего не меняет. Другой hash для уже применённой попытки — `ResultImportConflict`.

Отклонённый пакет пишется в `rejected_results/<job>/<attempt>/import_rejection.json`.

---

## 26. Rollback import

Журнал `apply_journal.json` в staging попытки: для каждого применённого пути —
существовал ли файл до применения и где лежит его резервная копия.
`rollback_applied` идёт в ОБРАТНОМ порядке: заменённое восстанавливается из
копии, созданное удаляется. Результат откака (`restored`/`removed`/`failed`)
дописывается в тот же журнал.

При сбое посреди применения: исходный проект не повреждён, staging сохраняется
для диагностики, попытка не получает `completed`, `result_import_state` не
становится `applied`, оператор может повторить import.

Проверено тестом с инъекцией отказа посреди применения
(`test_result_import_rolls_back_on_failure`): три новых файла удалены,
заменённый `03_findings.json` восстановлен побайтово.

**Честная граница.** Откат срабатывает на ИСКЛЮЧЕНИИ. Убийство процесса центра
(`kill -9`) посреди применения оставит журнал в состоянии `in_progress` и
частично применённые файлы на диске; автоматического подхвата такого журнала при
старте центра НЕТ. Факт виден (журнал сохранён и помечен), но разбор — ручной.
Это ограничение (§36).

---

## 27. Usage merge

Воркер возвращает `usage/usage_report.json` с записями
`{stage, model, input_tokens, output_tokens, cost_usd, cost_usd_notional,
calls, duration_ms, source: "worker"}` — из `stages_summary`, а не из
несуществующего ключа `stages` (по нему отчёт был пуст ВСЕГДА, §34.1 п. 17).

Центр применяет их через существующий `usage_tracker.record_usage`, передавая
объект `UsageRecord` (вызов по именованным аргументам давал `TypeError`, тихо
глушился, и расход терялся навсегда — §34.1 п. 16). РОВНО ОДИН РАЗ: признак
`job_attempts.usage_applied_at` ставится после цикла, повтор отвечает
`already_applied`, а провал записи отметку НЕ ставит — иначе повторить приём
было бы нельзя.

В центральный `paid_cost.json` воркер не пишет никогда, и центр по его отчёту
тоже — см. оговорку §21.

---

## 28. Compatibility

Строгая политика: `protocol_version` совпадает, `package_manifest_version`
поддерживается, `execution_profile` поддерживается, `pipeline_revision` воркера
СОВПАДАЕТ с `AUDIT_PIPELINE_REVISION` центра, хэши промптов и моделей совпадают
с пакетом, capability содержит `audit_pipeline_v1`, режим провайдеров объявлен.

Если `AUDIT_PIPELINE_REVISION` на центре не задана — удалённый запуск запрещён
(`center_revision_missing`): сверять ревизии не с чем, а «наверное совпадает» на
этом рубеже неприемлемо.

Несовместимый воркер не предлагается для запуска и показывается с точной
причиной (§13).

---

## 29. Resource model

`AUDIT_WORKER_REAL_AUDIT_MAX_SLOTS=1`, и это доказанный предел этапа, а не
настройка: значение больше зажимается в `audit_worker/config.py`. Вывод «два
`test_pipeline_v1` работают одновременно» на реальный аудит не переносится — у
него другой профиль RAM, диска и длительности, и он не измерялся.

Правила (`executor.audit_slot_conflict` / `test_slot_conflict`):

* один `audit_pipeline_v1` на воркер;
* `test_pipeline_v1` не стартует, пока идёт аудит;
* аудит не стартует, пока идут тестовые задания;
* ожидание слота возвращает попытку в `queued`, а НЕ в `failed`;
* критический диск блокирует старт (наследуется с этапа 4);
* текущий аудит не убивается из-за роста нагрузки.

Центральная сторона учитывает то же: `compatibility_report` возвращает
`audit_slot_used`, `audit_slot_limit` и `audit_slot_label` («0/1» или «1/1»), и
UI показывает именно их.

**Пять реальных аудитов НЕ заявляются.** RAM-профиль реального удалённого
аудита не измерялся вовсе.

---

## 30. Feature flags

| Флаг | Дефолт | Что включает |
|---|---|---|
| `DISTRIBUTED_WORKERS_ENABLED` | false | подсистему воркеров (маршруты, БД) |
| `DISTRIBUTED_AUDIT_EXECUTION_ENABLED` | false | удалённое исполнение аудита |
| `AUDIT_WORKER_AUDIT_PIPELINE_ENABLED` | false | приём `audit_pipeline_v1` воркером |
| `AUDIT_WORKER_ALLOW_REAL_LLM` | false | настоящие Claude/Codex на воркере |
| `AUDIT_WORKER_REAL_AUDIT_MAX_SLOTS` | 1 | зажимается до 1 |
| `AUDIT_PIPELINE_REVISION` | пусто | пусто = удалённый запуск запрещён |

Свойства: включение подсистемы воркеров удалённый аудит НЕ включает; воркер без
своего флага `audit_pipeline_v1` не объявляет и не принимает; настоящие LLM
запрещены независимо от флага удалённого аудита; локальный аудит работает при
всех новых флагах `false` (регрессия §35 это подтверждает); отсутствие
обязательной конфигурации (ревизия) — fail-closed.

---

## 31. UI

См. §13. Права: `operate` запускает и отменяет, `viewer` только смотрит,
`admin` управляет регистрацией. Проверка серверная, гейт намерения и
`Idempotency-Key` обязательны, разметка строится DOM-API.

**Пробел, названный явно:** на странице ПРОЕКТА (`frontend/index.html`,
`app.js`) кнопки «Отправить на audit-worker» НЕТ. Ручной запуск живёт только на
экране «Аудит-воркеры». §30.1 задания просил его на проекте — не сделано (§36).

---

## 32. Автоматические тесты

Новый файл `tests/test_distributed_workers_execution_backend.py` — **129 тестов**
(99 по разделам задания + 30 на исправления адверсариальных находок):

| Раздел | Про что | Тестов |
|---|---|---|
| §1.1 | отмена невыданной попытки, гонки | 6 |
| §1.2 | свежесть лимита, отзыв, offline, исполнитель | 4 |
| §1.3 | классификация отказов центра, старт без центра | 4 |
| §1.4 | лимит частоты регистрации | 8 |
| §2 | контракт, оба backend'а, идемпотентность, liveness, reattach | 14 |
| §3 | врезка в менеджер, персистентность, зомби, batch-only-local | 8 |
| §4 | пакет проекта, хардлинки, исключения, снимки, секреты | 8 |
| §5 | строгий `audit_pipeline_v1`, argv, env, подделки, слоты | 22 |
| §6 | приём результата, план, конфликты, откат, usage | 12 |
| §7 | безопасность: секреты, traversal, symlink, hardlink, права | 13 |
| §8 | исправления по §34: гейт центральных этапов, изоляция окружения, поддельные провайдеры, `project_id` как путь, маркер выхода, паралич очереди, маршрутизация отмены, выход из ожидания, учёт расхода, центральные артефакты, сканер секретов, `worker_state`, протухшие команды | 30 |

Прогон подсистемы целиком:
`python -m pytest tests/test_distributed_workers_*.py -q` → **477 passed**
(348 старых + 129 новых), 79 с.

---

## 33. E2E

**НЕ ВЫПОЛНЕН.** Сквозной прогон §32 задания на живом межпроцессном стенде
(uvicorn + `python -m audit_worker executor` + `python -m audit_worker agent` +
настоящий конвейер под поддельными провайдерами) не проводился.

Что это значит практически: путь «пакет → воркер → изолированный запуск →
result package → upload → import → central resume» проверен по частям
юнит-тестами и чтением кода, но НИ ОДНОГО РАЗА не был пройден целиком. Ни один
шаг сценария §32 (50 пунктов) не отмечен как пройденный, потому что ни один не
запускался.

Существующие smoke этапов 3.5 и 4 (`scripts/smoke_distributed_workers_*.py`)
проверяют только `test_pipeline_v1` и на этой ветке не перезапускались.

Следствие для вердикта — §39.

---

## 34. Адверсариальные находки

Пять независимых проверок, только на чтение, ни одна не правила файлы. Темы:
(1) врезка в менеджер и durable-состояние, (2) пакеты и приём результата,
(3) изоляция воркера и запрет настоящих LLM, (4) отмена/лимиты/старт без
центра, (5) сверка заявлений документа с кодом.

Итог: **они нашли то, чего не нашли 447 тестов** — включая четыре дефекта,
которые прямо обесценивали заявленные гарантии этапа. Это и есть главный
результат раздела, и он важнее любой формулировки о готовности.

### 34.1. Исправлено (коммит `f88a7c32`, +30 тестов)

**Гарантии профиля, которые не выполнялись:**

1. **Центральные этапы ВЫПОЛНЯЛИСЬ на воркере.** Проверка `FORBIDDEN_STAGES`
   сверяла только явный `retry_stage`, а `action="full"` через
   `_run_ocr_pipeline` гнал `norm_verify`, `debt_control`,
   `decision_carryover` и Excel: `include_norms` из спеки в `BatchQueueItem`
   не переносился вовсе, а решение принимает флаг платформы. Введён процессный
   гейт `AUDIT_PIPELINE_CENTRAL_STAGES_DISABLED` (§15), который выставляет
   `remote_audit_runner.harden_process_env`; четыре этапа менеджера выходят
   сразу с записью в лог. Тест `test_central_stages_are_blocked_in_the_remote_process`.
2. **Центр НЕ достраивал аудит после приёма результата.** `_execute_item`
   ставил `COMPLETED` и выбрасывал `resume_stage`: проект помечался
   «завершён» вообще без `norm_checks.json`, а UI и ответ API обещали
   обратное. Добавлен `_run_central_tail_after_remote` — хвост
   `_run_ocr_pipeline` дословно, включая `PIPELINE_NORMS_AFTER_MERGE_ENABLED`.
3. **`.env` из корня установленного кода пробивал белый список.**
   `load_dotenv()` на импорте конфигурации искал файл вверх от `config.py` и
   находил его в `pipeline_root`, возвращая процессу ключи провайдеров и
   `PAID_API_ENABLED=true`. Добавлен `AUDIT_DISABLE_DOTENV`; воркер выставляет
   его всегда, runner — до первого импорта backend. Тесты
   `test_config_honours_dotenv_kill_switch`,
   `test_remote_runner_hardens_environment_before_config_import`.
4. **Настоящая платная модель вызывалась ПО УМОЛЧАНИЮ.** Дефолт этапа
   `block_batch` — `ensemble/gpt-codex`, то есть HTTPS в OpenRouter; подделка
   двух CLI такой путь не закрывает вовсе, а `PAID_API_DAILY_LIMIT_USD=0`
   означает «лимита нет». `enforce_fake_providers` гасит платный API, удаляет
   ключи девяти провайдеров из окружения и связывает `CLAUDE_CLI_BIN`,
   `AUDIT_CODEX_CLI_PATH`, `CODEX_CLI_PATH` с подделками; отсутствие
   подделок — отказ запуска.
5. **Снимок `stage_models.json` хэшировался, но не применялся** — прогон шёл
   на дефолтах кода, а проверка хэша давала ложную уверенность «та же
   конфигурация». `apply_model_snapshot` кладёт снимок в `AUDIT_APP_DATA_DIR`.
6. **`_provider_dir` считал поддельным любой существующий каталог.** Пустой
   каталог префиксует PATH, ничего не перекрывая, — и `which` находит
   настоящий CLI, пока центру отрапортовано `provider_mode="fake"`. Теперь
   требуется маркер (`provider_dir_is_fake`, имена бинарей берутся ИЗ маркера,
   чтобы не появиться в пакете воркера).

**Очередь, отмена, живость:**

7. `_remote_items` включал `pending`: один такой элемент парализовывал ВСЮ
   очередь после рестарта — `_has_live_project_audit` вечно «выполняется»,
   resume 409, `_ensure_batch_worker` возвращал очередь без worker'а, и любой
   следующий локальный запуск молча не исполнялся. Теперь требуется
   непустой `execution_handle`, а `pending` в живые не входит.
8. `cancel()` по голому `project_id` мог пометить отменённым удалённый элемент
   ДРУГОЙ версии, не тронув живой локальный аудит, и вернуть `True`.
9. `wait()` не имел выхода из нетерминального состояния: `mark_lost` и
   `create_attempt` намеренно не трогают `execution_state`, поэтому штатный
   путь «признать потерянной → новая попытка» вешал слот навсегда.
10. Смерть/отмена слота помечала живое удалённое задание `cancelled`/`failed`
    — снимая защиту от `cleanup_zombies` и подталкивая к локальному
    перезапуску поверх идущего прогона. Теперь `interrupted`.
11. Повторный запуск в окне рестарта создавал ВТОРОЙ полный платный аудит
    того же проекта (дедуп работал только при `queue.status == "running"`).
12. `clear_queue_history` разрешалась при живой удалённой попытке и стирала
    единственную ссылку на неё.
13. Pre-crop кропал проект, который поедет на воркер со своим пакетом:
    лишний CPU центра и невоспроизводимое содержимое пакета.
14. Элемент с сохранённым handle, но потерянным `worker_id`, исполнялся
    ЛОКАЛЬНО — с `_clean_stage_files` поверх живой удалённой работы.
15. Связь в `liveness` читалась из `job_attempts.connectivity_state`, у
    которой нет ни одного писателя: вердикт был `ALIVE` даже для VPS,
    молчащего сутки. Теперь считается по строке воркера.

**Приём результата и учёт расхода:**

16. `usage_tracker.record_usage` вызывался по именованным аргументам вместо
    объекта `UsageRecord`; `TypeError` глушился, а `usage_applied_at` при этом
    ставился — расход терялся НАВСЕГДА без возможности повтора. Плюс сам
    `apply_usage_report` не вызывался в проде ни откуда: теперь он часть
    `import_result_for_attempt`, а провал записи отметку не ставит.
17. `collect_usage` читал `stages` вместо `stages_summary` — отчёт был пуст
    всегда, при том что файл присутствовал и проверку артефактов проходил.
18. Центр ОТПРАВЛЯЛ воркеру центральные артефакты, а импортёр отклонял ВЕСЬ
    пакет, увидев их обратно: любой повторный аудит версии, где нормы уже
    проходили, выбрасывал многочасовой прогон целиком. Асимметрия закрыта с
    обеих сторон.
19. `expert_review.json` из `03_analysis/latest` классифицировался как
    worker-файл и применялся — устаревшая копия возвращала снятые вердикты
    эксперта. Защита теперь по ИМЕНИ, а не только по префиксу.
20. `_detect_resume_stage` вызывался с путём вместо `project_id` — подсказка
    была `None` всегда (исключение глушилось).
21. `forbidden_prefixes` в `validate_result_package` не совпадали с
    раскладкой пакета аудита (`payload/project/…`) — первый рубеж был мёртв.
22. Сканер секретов не ловил `sk-ant-…`, `sk-or-v1-…`, PEM, JWT, AWS, GitHub,
    DSN с паролем; блоб `feature_flags.json` не сканировался вовсе, а сама
    проверка шла ПОСЛЕ записи архива и sidecar-манифеста.

**Изоляция воркера:**

23. **`project_id` из задания — это ПУТЬ, и он не проверялся нигде.**
    `resolve_project_dir` делает `projects_dir / project_id`, поэтому `..`
    поднимался вверх, а абсолютный путь при join отбрасывал левую часть: это
    и запись вне изоляции, и чтение с эксфильтрацией — прочитанное уезжало в
    `03_findings.json`, то есть в пакет результата и на центр. Добавлен
    `validate_project_id`.
24. `apply_runtime_paths` проверяла три корня из шести — и пропускала как раз
    рабочую для legacy-раскладки `AUDIT_PROJECTS_DIR`.
25. Реальный конвейер не писал `work/process_exit.json`: перезапущенный
    исполнитель объявлял ЗАВЕРШЁННЫЙ многочасовой аудит прерванным, потому
    что `completed.marker` писал наблюдатель, а он рестартом и умер.
26. Отмена в `result_uploading`/`result_received`/`validating` давала HTTP 500
    (ребра в `cancel_requested` нет), при том что интерфейс кнопку показывал.
    Теперь `already_finishing` + `can_cancel=false` в этих состояниях.
27. Гонка «отмена против выдачи» выбирала ветку по снимку, прочитанному ВНЕ
    транзакции, и роняла запрос: отмена не выполнялась ни как факт, ни как
    просьба, в журнале не было ничего. Теперь перечитка и обычный путь с
    командой. Прежний комментарий в коде утверждал обратное — он был неверен.
28. Протухшая команда (`status='expired'`) переиспользовалась как
    «незавершённая»: `enqueue_command` возвращал старую строку, которую
    `pending_commands` воркеру уже не отдаёт.
29. `worker_state` (`draining`/`degraded`) не участвовал в `effective_limit`:
    воркер, уходящий в плавный останов, получал новую работу.

### 34.2. Не исправлено, с причиной

* **Потеря `_action_override` на главном пути авто-resume** (проверка 1
  п. 13). Дефект подтверждён и относится к ЛОКАЛЬНОМУ конвейеру: `resume_interrupted_batch`
  переводит `interrupted → pending` до того, как `_batch_slot_worker` вычислит
  `was_interrupted`, поэтому прерванный элемент гонится целиком. Не тронуто
  намеренно: правка меняет поведение локального resume, а это ровно тот класс
  регрессии, от которого этап защищается. Отдельная задача.
* **Подхват журнала применения после `kill -9`** (§26): требует старта
  центра, сканирующего `result_staging/*/apply_journal.json`. Факт виден,
  разбор ручной.
* **`paid_cost.json` по отчёту воркера** (§21, §27): конверсию подписочной
  стоимости в единицы платного API без измерения придумывать не следует.
* **Кнопка удалённого запуска на странице проекта** (§31).
* **Холостая гонка «захватил → конфликт слота → вернул в очередь»** (проверка
  3, В-7): при `AUDIT_WORKER_MAX_SLOTS=2` и идущем аудите тестовое задание
  крутится по кругу без задержки. Дефект реален, но проявляется только в
  конфигурации «аудит + тесты одновременно», которая на этом этапе не
  используется.
* **Отмена подтверждает смерть только главного pid, а не пустоту группы**
  (проверка 3, В-8): осиротевший дочерний CLI может пережить отмену.
* **`AUDIT_PROJECTS_V2_WRITE_MODE` на воркер не передаётся** (проверка 2, B2):
  конвейер там работает в legacy write-mode. Это ровно тот класс дефекта,
  который может обнаружить только сквозной прогон, — а он не выполнялся
  (§33). Оставлено как есть с явной записью здесь: угадывать раскладку без
  прогона хуже, чем признать пробел.
* **`/claim` не ограничен по частоте** (только `/register`).
* **Единая корзина лимита за обратным прокси**: адрес берётся из соединения
  намеренно (заголовку доверять нельзя), но за nginx без PROXY protocol все
  воркеры схлопнутся в один бакет. Схема развёртывания не проверялась.

### 34.3. Что проверки признали корректным

Флаги и их независимость (включение подсистемы воркеров удалённый аудит не
включает; локальный аудит при всех новых флагах `false` идёт прежним путём);
отсутствие канала «выполни произвольную команду» (argv фиксирован, `cwd` —
корень установленного кода, `shell=False`, неизвестное поле в `params`
ОТВЕРГАЕТСЯ, `extra="forbid"` вторым рубежом); невозможность импортировать код
из архива (`PYTHONPATH`/`sys.path[0]` — корень установленного кода; ни
`pickle`, ни `yaml.load` в `backend/app` нет); поимка симлинков и `..` в обеих
распаковках; отсутствие второго процесса конвейера после рестарта исполнителя
(pid + тик старта + отпечаток + `cmdline` из `/proc` против второго источника);
адресация отмены только в подтверждённую группу процессов; невозможность двух
одновременных реальных аудитов; идемпотентность приёма результата и откат по
журналу на исключении; недостижимость глобальных mutable-файлов через пакет;
транзакционность выдачи; отсутствие утечки существования `instance_id` в
ограничителе регистрации; аддитивность миграции 5; отсутствие сборки HTML из
строк в новом UI; чистота коммитов (только `.py`, ни PDF, ни баз, ни логов).

Отдельно: проверка 5 сообщила, что UI не закоммичен. Это ошибка проверки —
она смотрела `git log` в основном рабочем каталоге, где HEAD другой; в ветке
UI лежит в `811e99c6`.

---

## 35. Регрессия

Прогон подсистемы: **477 passed** (348 старых + 129 новых).

Локальный контур очереди (`test_batch_queue_*`, `test_pipeline_cancel_propagation`,
`test_pipeline_queue_single_flight`, `test_resume_detector`): **63 passed**, без
изменений — и это существенно, потому что исправления §34 трогали `manager.py`
в девяти местах.

Полный прогон `tests` + `backend/tests` выполнен ДВАЖДЫ одной командой
(`pytest tests backend/tests -q --continue-on-collection-errors`): на этой ветке
и на базовом `f06c64d8` в отдельном worktree с тем же `.env` и
`stage_models.json`.

```
база f06c64d8 : 79 failed, 7009 passed, 109 skipped, 42 errors
эта ветка     : 79 failed, 7138 passed, 109 skipped, 42 errors
```

Сравнивались не числа, а МНОЖЕСТВА: `comm -13` / `comm -23` по отсортированным
спискам `FAILED`/`ERROR` (по 118 строк с каждой стороны) — обе разности ПУСТЫ.
Ни одного нового падения и ни одного «случайно починившегося». Разница
`+129 passed` — ровно новые тесты этапа. Прогон повторён ПОСЛЕ исправлений §34
(они трогали `manager.py` в девяти местах); результат тот же.

Унаследованные падения (геометрические тесты, тесты, требующие боевых данных
`projects_v2` и внешних сервисов; девять файлов падают на СБОРЕ, им нужны
корпуса из `experiments/`) одинаковы на обеих сторонах и к подсистеме отношения
не имеют.

**Изменённые старые тесты — восемь, все с обоснованием:**

| Тест (файл) | Было | Стало | Почему |
|---|---|---|---|
| `test_migration_is_idempotent` (`step35`) | `== 4` | `== 5` | добавлена миграция 5 |
| `test_step0_database_migrates_without_data_loss` (`step35`) | `== 4` | `== 5` | то же |
| `test_migration_2_upgrades_existing_database` (`hardening`) | `== 4` | `== 5` | то же |
| `test_operator_manages_attempts` (`prepipeline_gate`) | ждал `cancel_requested` | ждёт `cancelled` | §3.1: отмена невыданной попытки терминальна |
| `test_occupancy_counter_may_exceed_limit_but_only_conservatively` (`prepipeline_gate`) | закреплял «3/2» как фактическое поведение | проверяет, что «3/2» больше нет и выдача не заблокирована | §3.1: закрыт §32.1 п.24 отчёта 05 |
| `test_operator_cannot_declare_cancelled_once_worker_has_the_package` (`review_fixes`) | запрещал `assigned → cancelled` | запрещает начиная с `source_uploading`, требует наличия ребра у `assigned`, добавлены проверки `running`/`accepted_by_worker` | проверка была ШИРЕ обоснования |
| `test_pipeline_manager_untouched` → `..._knows_nothing_about_the_worker_subsystem` (`flag_off`) | «в manager.py нет слова ExecutionBackend» | нет импортов подсистемы и `audit_worker`, врезка идёт через абстракцию | врезка появилась; запрет переехал туда, где содержателен |
| `test_only_one_subprocess_spawn_point` (`flag_off`) | две точки запуска | три, все поимённо, у каждой ровно один вызов | реальный аудит не мог использовать точку тестового процесса |
| `test_job_type_enum_is_closed` (`center`) | ровно один тип | ровно два + запрет исполняемых слов в значениях | добавлен `audit_pipeline_v1` |

Ни один старый тест не удалён и не переименован в сторону ослабления;
`git diff f06c64d8..HEAD -- tests | grep '^-def test_'` даёт только
переименование `test_pipeline_manager_untouched`, объяснённое выше.

Реальные LLM в прогонах не участвовали.

---

## 36. Известные ограничения

Названы прямо, без смягчений. Первые три — причина вердикта PARTIAL.

1. **Сквозной прогон НЕ выполнялся.** E2E §32 задания на живых процессах не
   проводился ни разу. Ни «удалённый аудит проходит целиком», ни «результат
   применяется», ни «central resume работает» не подтверждены прогоном —
   только чтением кода, юнит-тестами по частям и пятью адверсариальными
   проверками. Последние нашли **29 подтверждённых дефектов**, четыре из
   которых обесценивали заявленные гарантии, — это прямая оценка того,
   сколько ещё может найти первый настоящий прогон.
2. **Реальный конвейер на воркере ни разу не запускался.** Связка
   `audit_runner` → `remote_audit_runner` → `_dispatch_action` написана и
   покрыта тестами на уровне валидации, argv, env, гейтов и отказов, но САМ
   прогон конвейера (даже с поддельными провайдерами, даже на фикстуре) не
   выполнялся.
3. **Семантическое сравнение локального и удалённого результата не
   проводилось.** Контракт эквивалентности (§33 задания) не определён в коде и
   не проверен.
4. **`AUDIT_PROJECTS_V2_WRITE_MODE` на воркер не передаётся** — конвейер там
   работает в legacy write-mode, тогда как центр в `projects_v2_primary`.
   Проверка 2 показала два возможных исхода (падение резолва путей либо запись
   в каталог, который сборщик результата не возвращает), и они различаются
   ТОЛЬКО прогоном. Не угадывалось намеренно.
5. **Абсолютные пути внутри артефактов не переписываются.**
   `pipeline_log.artifacts_dir`, `block_context_summary.project_dir`,
   `stage01_meta.runtime_plan_path` уедут на воркер как есть.
6. **Откат применения работает на исключении, а не на убийстве процесса.**
   `kill -9` центра посреди применения оставит журнал `in_progress` и частично
   применённые файлы; автоматического подхвата при старте нет. Плюс `*.tmp`
   файлы применения в каталоге проекта никто не убирает.
7. **`paid_cost.json` не пополняется по отчёту воркера.** `usage_data.json`
   теперь пополняется (§27); дневной лимит платного API остаётся локальным для
   центра.
8. **Кнопки удалённого запуска на странице ПРОЕКТА нет** — только на экране
   «Аудит-воркеры» (§31).
9. **`_action_override` теряется на главном пути авто-resume** — дефект
   ЛОКАЛЬНОГО конвейера, унаследованный от предыдущих этапов (§34.2). Не
   тронут намеренно.
10. **Отдельный системный пользователь для процесса конвейера** — свойство
    systemd-юнитов, в тестах не проверялся; сами юниты под реальный аудит не
    обновлялись (нет новых переменных, `ProtectHome=true` при
    `ALLOW_REAL_LLM=true` отрежет авторизацию настоящих CLI).
11. **Отмена подтверждает смерть главного pid, а не пустоту группы** —
    осиротевший дочерний CLI может пережить отмену.
12. **Холостая гонка «захватил → конфликт слота → вернул в очередь»** при
    `AUDIT_WORKER_MAX_SLOTS=2` и смешивании аудита с тестовыми заданиями.
13. **`/claim` не ограничен по частоте**; за обратным прокси без PROXY protocol
    лимит регистрации схлопывается в одну корзину.
14. **Хардлинки в пакете РЕЗУЛЬТАТА не поддерживаются** центральным
    распаковщиком. Сегодня это не проблема (сборщик их не создаёт), но если
    появятся — пакет будет отклонён.
15. **Все ограничения этапов 3.5 и 4 остаются в силе**: окно идемпотентности
    500 записей и без фильтра по заданию, отсутствие TTL у `idempotency_keys`,
    аренда сборки архива 30 минут, long-poll держит поток обработчика,
    `mark_lost` не снимает `current_attempt_id`, миграции вперёд-только,
    `lease_expires_at` локальной очереди пишется но не читается, `instance_id`
    не уникален в схеме, CSP на портале нет, TTL команды 7 суток.

## 37. Готовность к тестовому VPS

**Не готово.** Причина одна и она в §36 п. 1–2: код, который ни разу не был
запущен целиком, нельзя разворачивать на машине с чужой инфраструктурой и
живыми подписками. Адверсариальные проверки эту оценку подтвердили численно: 29
подтверждённых дефектов на коде, который проходил 447 тестов, причём четыре из
них означали «настоящая модель вызывается, хотя заявлено обратное».

Что нужно сделать ДО подключения тестового VPS, в этом порядке:

1. собрать межпроцессный стенд §32 и прогнать сценарий целиком с поддельными
   провайдерами — на фикстуре проекта, локально, без сети;
2. закрыть дефекты, которые этот прогон покажет (их будет несколько: связка
   ещё не работала, и §36 п. 4 — первый кандидат);
3. определить и проверить контракт семантической эквивалентности локального и
   удалённого результата;
4. решить вопрос абсолютных путей в артефактах (§36 п.4);
5. только после этого — развёртывание на 176.12.77.31 с
   `AUDIT_WORKER_ALLOW_REAL_LLM=false` и одним проектом.

---

## 38. План отката

1. **Выключить удалённое исполнение:** `DISTRIBUTED_AUDIT_EXECUTION_ENABLED=false`,
   рестарт backend. Единственным активным backend'ом остаётся
   `LocalExecutionBackend`; элемент очереди с `execution_mode=remote_worker`
   локально НЕ исполняется, а отклоняется понятной ошибкой (это защита от
   двойного исполнения, а не дефект).
2. **Выключить подсистему целиком:** `DISTRIBUTED_WORKERS_ENABLED=false`. Из
   маршрутов остаются `/api/workers/status`, `/api/workers/me` и HTML-страница.
3. **Старые remote attempts сохраняются**, `workers.db` сохраняется, пакеты на
   воркере не удаляются (`validated_results/`, `superseded_results/`,
   `rejected_results/`, `source_packages/`, `result_staging/`).
4. **Применённый результат откатывается по журналу**
   `result_staging/<job>/<attempt>/apply_journal.json` через
   `result_import.rollback_applied`. Резервные копии заменённых файлов лежат
   рядом, в `backup/`.
5. **Batch-очередь работает локально** — она и не менялась.
6. **Авто-resume не запускает remote локально** — см. п. 1.
7. **UI удалённого аудита отключается сам**: панель показывает причину
   («удалённое исполнение выключено») и блокирует кнопку.
8. **Центральные результаты не удаляются** ни одним шагом откака.
9. **Откат схемы центра:** миграция 5 аддитивна для таблиц (`ALTER TABLE ADD
   COLUMN` + новая таблица), но ПЕРЕСОЗДАЁТ представление `remote_jobs`.
   Прежний код с новой базой работает: представление содержит все старые
   колонки. Если откат схемы всё же нужен — остановить backend, заменить
   `workers.db` копией `workers.db.before_v4_to_v5`, удалить `-wal`/`-shm`.
10. **Откат кода:** `git revert` четырёх коммитов ветки в обратном порядке.
    `PipelineManager` при этом возвращается к прямому вызову
    `_dispatch_action` — точка врезки локализована в одном методе.

---

## 39. Точные границы следующего этапа

Следующий этап — **межпроцессный E2E реального конвейера**, и только он:

* собрать стенд §32 задания: настоящий `backend/app/main.py`, настоящий
  `PipelineManager`, настоящие workers API, настоящие Agent и Executor,
  настоящий дочерний процесс конвейера, поддельные CLI;
* подготовить фикстуру проекта, на которой конвейер реально проходит
  (подготовленный `result.json`, MD, блоки);
* пройти сценарий целиком, включая обрывы: убийство агента посреди этапа,
  остановку центра, обрыв upload, применение результата, central resume;
* определить и проверить контракт семантической эквивалентности;
* закрыть найденные дефекты.

В него НЕ входит: подключение VPS 176.12.77.31, реальные Claude/Codex,
автоматический выбор воркера, повышение числа реальных аудитов выше одного,
batch с удалённым исполнением.

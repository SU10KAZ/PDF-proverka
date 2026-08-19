# 11C_RUNTIME_PATH — фактическая цепочка «задание → модель → результат»

Карта снята **по коду**, а не по проектным намерениям: для каждого звена указан
файл, символ, входной и выходной контракты, побочные эффекты на диске и в БД,
семантика повтора и идемпотентности. Состояние — ветка
`feat/distributed-audit-workers-pipeline-provider-e2e` от HEAD `85ab2532`.

Разделение на «было до 11C» и «стало» дано там, где 11C менял звено; остальное
описано как есть.

---

## 0. Схема в одну строку

```
оператор → create_audit_job → SQLite центра + пакет исходников
  → GET/POST /api/v1/worker/jobs/next (агент воркера, HTTP)
  → LocalJobStore + worker.db (очередь попыток)
  → Executor.claim_next → run_audit_attempt
  → [11C] ProviderResolver + inference_grant → provider_binding.json
  → audit_runner.run_audit_job (argv/env строит ВОРКЕР)
  → python -m backend.app.pipeline.remote_audit_runner <run_spec.json>
  → bind_providers → apply_runtime_snapshot → apply_discipline_profile
  → run_provider_selfcheck | _dispatch_action
  → stages/provider_selfcheck → claude_runner._run_cli
  → [11C] pipeline_bridge.route_cli_call → InferenceLedger → ProviderAdapter
  → claude | codex (локальный подпроцесс, stdin=промпт)
  → ProviderInferenceResult → validate_inference → артефакты
  → package_io.build_result_package (TAR + манифест)
  → EventOutbox → POST /api/v1/worker/events → ACK
  → POST /api/v1/worker/jobs/{id}/result (чанки) → валидация → completed
```

---

## 1. Центр: постановка задания

| Что | Где |
|---|---|
| Файл | `backend/app/services/distributed_workers/audit_job_service.py` |
| Функция | `create_audit_job(...)` (строка ~340) |
| Вход | `worker_id`, `project_id`, `version_id`, `version_dir`, `action`, `include_optimization`, `retry_stage`, **`provider_requirement` (11C)**, `actor`, `settings` |
| Выход | `{job_id, attempt_id, execution_profile, …}` |
| Побочные эффекты | строка в `logical_jobs`/`attempts` (SQLite `workers.db`), архив исходников в `<data>/source_packages/`, снимки runtime-конфигурации и профиля дисциплины |
| Retry | отсутствует: повторный запуск того же проекта отбивается уникальным индексом `ux_logical_jobs_active_project` (409) |
| Идемпотентность | по паре (проект, версия) через активный logical_job; ключ идемпотентности операторского HTTP — в `idempotency_keys` |

**Что изменил 11C.** Добавлено действие `provider_selfcheck` и поле
`provider_requirement` в `AuditPipelineParams`
(`backend/app/models/distributed_workers.py`, класс `ProviderRequirementPayload`,
`extra="forbid"`). Требование содержит только смысл — провайдер, ожидаемая
модель, белый список этапов, потолок вызовов. Пути, аргументы, окружение и
промпт отсутствуют по построению, как и во всей остальной нагрузке.

Обязательные артефакты стали **зависеть от действия**:
`audit_job_service.required_artifacts_for(action)` и
`job_service.required_artifacts_for(job)` (читает `action` из payload задания).
До 11C список был один, и синтетическая проверка была бы обречена на
«пакет неполон: нет 03_findings.json» независимо от того, что произошло.

**Ingress центра.** Выключатель — `DISTRIBUTED_WORKERS_ENABLED`
(`backend/app/core/config.py:1260`, по умолчанию **False**). Он снимает сразу
три вещи: регистрацию роутера `/api/v1/worker/*` (`backend/app/main.py:254`),
операторский `/api/workers/*` (кроме `status_router`) и создание SQLite
(`database.ensure_ready` → `settings.require_enabled()`). Роутеры регистрируются
**на импорте**, поэтому включение требует рестарта процесса. Второй, независимый
флаг — `DISTRIBUTED_AUDIT_EXECUTION_ENABLED` (`config.py:1309`): он не про
ingress, а про право СОЗДАВАТЬ задания `audit_pipeline_v1`.

---

## 2. Выдача задания воркеру

| Что | Где |
|---|---|
| Файл | `backend/app/api/routers/audit_worker_agent.py` |
| Маршрут | `POST /api/v1/worker/jobs/next` |
| Вход | `{free_slots, accepts, wait_sec, executor_status, busy_slots}` + `Authorization: Bearer <worker_token>`, `Idempotency-Key` |
| Выход | `204` либо назначение с полем `params` (нагрузка `AuditPipelineParams`) и свежим execution-token |
| Побочные эффекты | `claim_next_job_for_worker` (транзакция), перевыпуск `execution_token_sha256` у попытки |
| Retry | long-poll до `wait_sec`; ключ идемпотентности НОВЫЙ на обычный опрос и ТОТ ЖЕ после обрыва |
| Идемпотентность | I-05: повтор с тем же ключом возвращает то же назначение, но перевыпускает execution-token (в кэше секрет не хранится) |

Портальная cookie к агентским маршрутам не применяется:
`portal_auth.EXEMPT_PREFIXES = ("/api/v1/worker/",)`.

---

## 3. Агент воркера

| Что | Где |
|---|---|
| Файлы | `audit_worker/agent.py`, `audit_worker/job_poller.py`, `audit_worker/client.py` |
| Функции | `WorkerAgent.run_forever` → `JobPullClient.poll` → `_prepare_ctx` → `execute_job` |
| Вход | назначение центра |
| Выход | запись в `LocalJobStore` (`jobs/<job>/<attempt>/metadata.json`) и в `worker.db` (`db.enqueue`) |
| Побочные эффекты | скачивание пакета исходников с докачкой по `Range`, распаковка в `unpack_staging`, `os.replace` секций |
| Retry | `backoff_delays()` 1→2→4→…→30 с с джиттером ±20 %, бесконечно; сетевые ошибки не меняют состояние |
| Идемпотентность | `db.enqueue` идемпотентен по `attempt_id`; `accept` идёт с ключом `accept:{job}:{attempt}` |

Агент **не запускает процессов аудита** и не знает провайдерского слоя как
исполнителя: он опрашивает `ProviderManager` только ради heartbeat.

---

## 4. Захват попытки и решение о провайдере (изменено 11C)

| Что | Где |
|---|---|
| Файл | `audit_worker/executor.py` |
| Функции | `Executor._tick` → `db.claim_next` → `run_attempt` → `run_audit_attempt` → **`prepare_provider_binding` (11C)** |
| Вход | строка очереди `worker.db`, `params_json` |
| Выход | `metadata/provider_binding.json` в каталоге попытки либо `None` |
| Побочные эффекты | атомарное списание `<worker_root>/config/allow_synthetic_inference`; создание раскладки попытки |
| Retry | нет: разрешение тратится за ПОПЫТКУ, а не за успех |
| Идемпотентность | вызов модели защищён журналом (см. §7), само разрешение — счётчиком `used/max_uses` под `flock` |

Порядок в `prepare_provider_binding` обязателен и не переставляется:

1. рубеж машины — `AUDIT_WORKER_PIPELINE_PROVIDER_ENABLED`;
2. режим провайдеров — `AUDIT_WORKER_ALLOW_REAL_LLM` (в fake-режиме привязка
   бессмысленна и запрещена);
3. **резолв** — `ProviderResolver.resolve`: провайдер установлен, не заблокирован
   политикой, режим авторизации не `unavailable`, `auth_state == logged_in`.
   Отказ здесь ничего не стоит: разрешение ещё цело;
4. **списание** разрешения оператора — последним из проверок и ДО запуска
   процесса;
5. запись привязки.

Проверка слота (`audit_slot_conflict`) стоит **до** подготовки привязки: иначе
ожидание слота списывало бы разрешение.

---

## 5. Запуск процесса конвейера

| Что | Где |
|---|---|
| Файл | `audit_worker/audit_runner.py` |
| Функции | `validate_params`, `build_argv`, `build_env`, `run_audit_job` |
| Вход | `params_json` задания, каталог попытки, путь привязки |
| Выход | `AuditRunOutcome`, `metadata/run_spec.json`, `logs/stdout.log`, `logs/stderr.log` |
| Побочные эффекты | подпроцесс `python -u -m backend.app.pipeline.remote_audit_runner <spec>` в своей сессии |
| Retry | нет: после `executor_interrupted` автоповтора не бывает (§8.6) |
| Идемпотентность | `completed.marker` + `process_exit.json` различают «отработал» и «исчез» |

argv фиксирован; переменная часть одна — путь к спецификации. Окружение
собирается **с нуля** белым списком (`_ENV_WHITELIST`), корни данных уводятся
внутрь каталога попытки (`isolated_roots`), `HOME` и `TMPDIR` в белый список не
входят.

**11C добавил ровно одну переменную** — `AUDIT_WORKER_PROVIDER_BINDING`
(литерал `PROVIDER_BINDING_ENV`). Модуль по-прежнему **не импортирует**
провайдерский слой: ему нужно только имя переменной. Совпадение литерала с
`resolver.BINDING_ENV` закреплено тестом
`test_binding_env_name_matches_provider_layer`, а сама граница — тестом
`test_pipeline_runner_still_does_not_import_the_provider_layer`.

---

## 6. Код платформы: точка входа конвейера

| Что | Где |
|---|---|
| Файл | `backend/app/pipeline/remote_audit_runner.py` |
| Функции | `main` → `harden_process_env` → `load_spec` → `apply_runtime_paths` → `apply_runtime_snapshot` → `apply_discipline_profile` → `enforce_fake_providers` → **`bind_providers` (11C)** → `apply_model_snapshot` → `verify_snapshot` → `run` |
| Вход | `run_spec.json`, снимки центра в каталоге попытки |
| Выход | NDJSON-события на stdout, `result/*`, `work/pipeline_log.json`, `usage/usage_report.json`, `work/process_exit.json` |
| Побочные эффекты | запись только внутрь каталога попытки (проверяется `apply_runtime_paths`) |
| Retry | нет; исход фиксируется маркером |
| Идемпотентность | обеспечивается уровнем выше (попытка) |

`bind_providers` — **проверка**, а не установка: переменную ставит исполнитель.
Три утверждения: fake-режим и привязка несовместимы; `job_id`/`attempt_id`
привязки совпадают со спекой (две независимые записи одной строки очереди);
привязанный провайдер — тот, которого потребовал центр.

---

## 7. Вызов модели (новое звено 11C)

| Что | Где |
|---|---|
| Этап | `backend/app/pipeline/stages/provider_selfcheck/__init__.py` — `run_stage` |
| Точка выбора CLI | `backend/app/services/llm/claude_runner.py` — `_run_cli` |
| Мост | `audit_worker/providers/pipeline_bridge.py` — `route_cli_call` → `run_stage_inference` |
| Журнал | `audit_worker/providers/inference_ledger.py` — `InferenceLedger` |
| Адаптер | `audit_worker/providers/{base,claude_adapter,codex_adapter}.py` — `structured_inference` |
| Вход | промпт (данные задания) |
| Выход | `ProviderInferenceResult` + `ValidationReport` |
| Побочные эффекты | `<job_dir>/inference/<key>.claim.json`, `<key>.result.json`; подпроцесс CLI |
| Retry | **запрещён** при неизвестном исходе; разрешён только повтор ЧТЕНИЯ сохранённого результата |
| Идемпотентность | **I-P9** — exactly once per attempt (см. ниже) |

**Почему разрыв существовал.** До 11C `_run_cli` резолвил бинарь через
`config._find_claude_cli` → `shutil.which("claude")` и запускал его с `HOME`
внутри каталога попытки — то есть находил **неавторизованный** CLI. Провайдерский
слой умел авторизовать, но конвейер о нём не знал; граница была объявлена в
доке 11b §13 и закреплена AST-тестом.

**Как разрыв закрыт.** `_run_cli` получил преамбулу: при активной привязке
вызов уходит в мост. Это единственная развилка «claude или codex» во всём
конвейере, поэтому перехват стоит именно перед ней. На центре ветки не
существует: `active()` смотрит на переменную, которую ставит только исполнитель
воркера.

**Промпт передаётся через stdin, а не argv.** Инвариант I-P5 («ни один argv не
приходит извне») остаётся дословным: argv состоит только из констант модуля.
Побочно закрыт и другой канал — argv видно в `ps` любому пользователю машины.

**I-P9 — inference exactly-once per attempt.** Протокол журнала:

| состояние на входе | что делает мост |
|---|---|
| ничего нет | создаёт `claim` через `O_CREAT|O_EXCL` **до** вызова, зовёт модель, сохраняет результат |
| есть `result` | возвращает сохранённое, модель не зовёт |
| есть `claim` без `result` | **отказ**: исход неизвестен, повтор запрещён, решение за оператором |

Единственная примитивная операция с одним победителем и на локальной ФС, и
между процессами — `O_CREAT|O_EXCL`; поэтому именно она, а не блокировка.

---

## 8. Проверка результата

| Что | Где |
|---|---|
| Файл | `audit_worker/providers/inference.py` |
| Функция | `validate_inference` → `ValidationReport` |
| Утверждений | 12 именованных: `exit_code`, `status`, `json_parsed`, `required_fields`, `field_types`, `expected_semantics`, `no_credential_like`, `no_private_paths`, `no_forbidden_literals`, `provider_matches_task`, `auth_mode_matches_task`, `identity_matches_claim` |

Плюс независимая сверка на стороне этапа: числа и цитаты ответа обязаны
встречаться **в самом фрагменте**; проверка считает по тексту и модель ни о чём
не спрашивает.

Контрольные литералы (canary) приходят файлом оператора
(`AUDIT_WORKER_PROVIDER_FORBIDDEN_LITERALS_FILE`) и в репозитории не хранятся:
иначе «в ответе её не нашли» было бы утверждением о репозитории.

---

## 9. Сборка результата и транспорт

| Что | Где |
|---|---|
| Пакет | `audit_worker/package_io.py` — `build_result_package` → `result/<attempt>.tar.gz` + манифест |
| Журнал событий | `audit_worker/event_outbox.py` — `EventOutbox.append` (санитайзер ПРИ ЗАПИСИ, I-12) |
| Нумерация | `local_db.allocate_event_sequence` — номер выдаёт SQLite, PK `(job, attempt, seq)` |
| Отправка | `audit_worker/agent.py::_flush_outbox_locked` → `POST /api/v1/worker/events` |
| Загрузка | `POST /api/v1/worker/jobs/{id}/result` чанками по 32 МиБ, `X-Chunk-SHA256`, `Range` |
| Транспорт | `audit_worker/client.py::CenterClient` поверх `httpx.Client(verify=True, follow_redirects=False)` |
| Retry | `409 sequence_gap` → `rewind_to(expected_seq)` и повтор; сеть — молча копим |
| Идемпотентность | I-04 (`last_seen_seq`), I-06 (`UploadSession` + `result_package_hash`) |

11C добавил одно событие — сводку журнала вызовов
(`Executor._announce_inference_ledger`): числа и состояния ключей, **без**
промптов и ответов. Сырой ответ модели живёт только в журнале внутри каталога
попытки и уезжает пакетом, а не heartbeat'ом.

---

## 10. Где именно был разрыв и почему его нельзя было «просто подключить»

1. `ProviderAdapter` — слой identity/quota/probe: у него не было метода «выполни
   работу», единственный промпт был константой `PROBE_PROMPT`, инференс по
   умолчанию запрещён, метод синхронный.
2. Резолв бинаря **противоположный**: адаптер намеренно не ищет по `PATH` (там
   первым стоит каталог подделок), конвейер ищет именно по `PATH`.
3. Окружение: у процесса конвейера `HOME` внутри попытки — найденный CLI не
   авторизован; авторизацию даёт только `ProviderAdapter.build_env()`.
4. Интеграция провайдеров была сделана **переменными окружения**, а не объектом
   (так работает fake-режим).

Поэтому 11C добавил ровно то, чего не хватало: рабочий вызов в адаптере
(`structured_inference`), точку решения (`ProviderResolver`), документ решения
(`ProviderBinding`), журнал оплат (`InferenceLedger`) и один перехват в штатной
точке выбора CLI. Второго провайдерского фреймворка не появилось: авторизация,
изоляция окружения, отключение инструментов, убийство группы по таймауту и
редакция вывода остались там, где были.

---

## 11. Что осталось НЕ подключённым (честная граница)

Через мост ходит только этап из белого списка привязки. Настоящие этапы аудита
(`findings_merge`, `text_analysis`, `block_analysis`, `optimization`) через него
**не ходят и не могут**: они полагаются на инструменты CLI (`Write`, `Read`) для
записи JSON-артефактов, а рабочий вызов адаптера идёт с полностью отключёнными
инструментами. Перевод этих этапов — отдельная работа: либо контракт «модель
возвращает JSON в stdout» вместо «модель пишет файл», либо отдельный профиль
инструментов в адаптере. Пока этап не в белом списке, мост отвечает **отказом**,
а не молчаливым обходом.

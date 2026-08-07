# Этап 0: вертикальный срез распределённых audit-worker

**Ветка:** `feat/distributed-audit-workers-step0`
**Базовый HEAD:** `bdc5c87f0a15aced0b5ef766d96d911d44b0b016` (ветка `feature/block-vector-graphs`)
**Статус:** реализовано и покрыто тестами; **выключено по умолчанию**
**Основание:** [01_current_architecture_audit.md](01_current_architecture_audit.md), [02_technical_design.md](02_technical_design.md)

---

## 1. Что это и чего здесь нет

Проверена **вся инфраструктурная цепочка** распределённого аудита на безопасной тестовой задаче — до какого-либо вмешательства в `PipelineManager`:

```
регистрация воркера → ручное одобрение → выдача токена → heartbeat с ресурсами
→ ручная выдача тестового задания → скачивание TAR-пакета → sha256 + манифест
→ безопасная распаковка → фиксированный тестовый процесс → события и полные логи
→ сборка результата → чанкованная загрузка → четыре проверки → скачивание оператором
```

**Чего в этом этапе нет и не должно быть** (проверяется тестами, а не обещаниями):

| Не реализовано | Как это гарантировано |
|---|---|
| Реальный аудит, интеграция с `PipelineManager` | `test_pipeline_manager_untouched` — грепом по `manager.py` |
| Вызовы Claude Code / Codex | `test_no_llm_invocation_in_worker_package` |
| Произвольные shell-команды от центра | `test_command_enum_has_no_shell`, `test_only_one_subprocess_spawn_point` |
| Нормативный этап, запись в `decisions_log.json` | подсистема к ним не обращается вовсе |
| Автовыбор воркера, «потратить до сброса», учёт лимитов | не реализовано; поля протокола заложены |
| Автоудаление через 30 дней, команды удаления | `retention_until` передаётся, но чистильщика нет |
| Автообновление воркера | `GET /update/manifest` отвечает `204` — контракт без реализации |
| S3, Redis, RabbitMQ, Kubernetes, внешняя БД, постоянный WS центр↔воркер | не введены |

---

## 2. Быстрый старт (пилот на одной машине)

### 2.1. Центр

```bash
# 1. Секрет регистрации — обязателен, дефолта нет намеренно
python3 -c "import secrets; print(secrets.token_urlsafe(32))"

# 2. В .env центра
DISTRIBUTED_WORKERS_ENABLED=true
DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET=<вставить сгенерированное>

# 3. Запуск как обычно
uvicorn backend.app.main:app --host 0.0.0.0 --port 8081
```

В логе старта появится `[startup] распределённые audit-worker: ВКЛЮЧЕНЫ`.
Экран: **`http://<host>:8081/audit-workers`**.

Проверка конфигурации без захода в UI:

```bash
curl -s localhost:8081/api/workers/status | python3 -m json.tool
# enabled: true, config_error: null  → всё готово
# enabled: true, config_error: "...BOOTSTRAP_SECRET не задан..." → секрет не выставлен
```

### 2.2. Воркер

```bash
# На стороннем VPS, под ОТДЕЛЬНЫМ системным пользователем (не root)
python3 -m venv /opt/audit-worker/venv
/opt/audit-worker/venv/bin/pip install -r requirements-worker.txt

export AUDIT_WORKER_DISPATCHER_URL=https://auditmanager.app
export AUDIT_WORKER_ROOT=/var/lib/audit-worker
export AUDIT_WORKER_NAME="VPS-2 Hetzner FSN1"
export AUDIT_WORKER_MAX_SLOTS=1

# Проверка окружения БЕЗ центра: прогнать тестовый процесс локально
python -m audit_worker selftest --steps 3

# Регистрация (секрет переносится на VPS по SSH — вне протокола)
python -m audit_worker register --bootstrap-secret '<секрет центра>'

# ← здесь оператор одобряет воркер на экране «Аудит-воркеры»

python -m audit_worker run
```

`register` печатает `worker_id`, статус `pending` и путь состояния. Токен
сохраняется в `<root>/token` с правами `0600` и **больше нигде не появляется** —
повторная регистрация того же `instance_id` нового токена не выдаёт.

### 2.3. Systemd-юнит (рекомендуемый профиль изоляции)

```ini
[Unit]
Description=Audit Manager worker
After=network-online.target

[Service]
User=audit-worker
Group=audit-worker
Environment=AUDIT_WORKER_DISPATCHER_URL=https://auditmanager.app
Environment=AUDIT_WORKER_ROOT=/var/lib/audit-worker
ExecStart=/opt/audit-worker/venv/bin/python -m audit_worker run
Restart=always
RestartSec=10

NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=/var/lib/audit-worker

[Install]
WantedBy=multi-user.target
```

---

## 3. Ручной smoke-сценарий

Проверяет то же, что `tests/test_distributed_workers_e2e.py`, но руками и на живой связке.

| # | Действие | Ожидаемый результат |
|---|---|---|
| 1 | `python -m audit_worker selftest --steps 3` | код 0, создан `_selftest/result/summary.json`; в выводе виден **фиксированный argv** |
| 2 | `python -m audit_worker register --bootstrap-secret <секрет>` | `registration_status: pending`, файл `token` с правами 600 |
| 3 | `python -m audit_worker register --bootstrap-secret <неверный>` | `HTTP 401` — регистрация закрыта |
| 4 | Открыть `/audit-workers` | карточка VPS в статусе `pending`, кнопка «Одобрить» |
| 5 | `python -m audit_worker run` **до** одобрения | в логе `heartbeat не прошёл: HTTP 403` — и агент не падает |
| 6 | Нажать «Одобрить» | статус `approved`; heartbeat проходит; появляются RAM/CPU/диск и слоты с пояснением «ограничивает …» |
| 7 | Заполнить форму (5 шагов × 0.5 с) → «Отправить» | задание появляется в списке, статус «Назначено, ожидает воркер» |
| 8 | Наблюдать экран | статусы сменяются: пакет передаётся → принято воркером → **Выполняется** с полосой `k/5` → «Завершён на воркере, ожидается передача» → «Результат передаётся» → «Результат принят и проверен» |
| 9 | Кнопка «Логи» | полный stdout тестового процесса построчно, с номерами `seq` |
| 10 | Кнопка «Скачать результат» | `.tar.gz`; внутри `package_manifest.json`, `payload/result/summary.json`, `payload/result/run_log.txt` |
| 11 | **Обрыв связи:** `iptables -A OUTPUT -d <центр> -j DROP` посреди прогона | конвейер на воркере **не останавливается**; на экране «Выполняется, связь потеряна N мин назад»; **состояние задания не меняется** |
| 12 | Вернуть связь | события догоняются пакетами, порядок сохранён, дублей нет; статус доходит до «Результат принят и проверен» |
| 13 | Убить агент (`kill`) во время прогона и запустить снова | процесс задания пережил рестарт → агент его не трогает; `seq` продолжается, а не начинается с 1 |
| 14 | `python -m audit_worker status` | список заданий и счётчик `retention_unconfirmed` |
| 15 | Отправить второе задание на **тот же** `project_id`, пока первое активно | `409` — одно активное задание на проект |

**Проверка выключения:** поставить `DISTRIBUTED_WORKERS_ENABLED=false`, перезапустить центр →
`/api/workers` отдаёт `404`, `/api/workers/status` отдаёт `enabled:false`, файл `workers.db` не создаётся, существующий аудит работает как раньше.

---

## 4. Карта файлов

### Центр

| Файл | Назначение |
|---|---|
| `backend/app/core/config.py` | блок `DISTRIBUTED_WORKERS_*` (10 переменных) |
| `backend/app/core/portal_auth.py` | `EXEMPT_PREFIXES` — `/api/v1/worker/` живёт в своём контуре |
| `backend/app/main.py` | условная регистрация роутеров + маршрут `/audit-workers` |
| `backend/app/models/distributed_workers.py` | закрытые enum (`JobType`, `JobState`, `WorkerCommandType`) и pydantic-контракты |
| `services/distributed_workers/schema.py` | DDL + нумерованные миграции |
| `services/distributed_workers/database.py` | WAL, один writer под локом, `run_db` → `to_thread` |
| `services/distributed_workers/repositories.py` | CRUD, транзакционный приём батча событий |
| `services/distributed_workers/settings.py` | настройки с перечитыванием env, fail-fast по секрету |
| `services/distributed_workers/auth.py` | bearer-токен воркера, `execution_token` попытки |
| `services/distributed_workers/registration_service.py` | заявка → одобрение → отзыв → ротация |
| `services/distributed_workers/worker_registry.py` | heartbeat и **только** ось связи |
| `services/distributed_workers/job_service.py` | машина состояний (единственный писатель `state`), сборка source-пакета, финализация |
| `services/distributed_workers/event_service.py` | непрерывность батча, дедуп, побочные эффекты, файл логов |
| `services/distributed_workers/package_service.py` | TAR, манифест, безопасная распаковка, четыре проверки |
| `services/distributed_workers/upload_service.py` | чанкованная возобновляемая загрузка |
| `services/distributed_workers/progress_service.py` | честный прогресс и ETA |
| `services/distributed_workers/redaction.py` | очистка секретов |
| `api/routers/audit_worker_agent.py` | `/api/v1/worker/*` — 16 эндпоинтов |
| `api/routers/audit_workers_admin.py` | `/api/workers/*` — оператор; `status_router` работает и при выключенном флаге |
| `frontend/audit-workers.html`, `static/js/audit-workers.js`, `static/css/audit-workers.css` | экран |

### Воркер (`audit_worker/`)

| Файл | Назначение |
|---|---|
| `__main__.py` | CLI: `register` / `run` / `status` / `selftest` |
| `config.py` | настройки из env, `capabilities()` |
| `client.py` | HTTP-клиент, backoff, типизированные ошибки (`SequenceGapError`, `AttemptSupersededError`) |
| `registration.py` | личность воркера, `instance_id` на каждый запуск |
| `heartbeat.py` | фоновый поток живости |
| `job_poller.py` | long-poll, только при свободном слоте |
| `local_store.py` | файловое состояние (не БД — так решено в §7.3 техпроекта) |
| `event_outbox.py` | дисковый outbox, монотонный `seq`, уплотнение, прореживание |
| `package_io.py` | проверка и безопасная распаковка source, сборка result |
| `test_runner.py` | валидация параметров, **построение argv**, запуск, разбор вывода |
| `test_process.py` | сам тестовый процесс `test_pipeline_v1` |
| `process_registry.py` | pid + метка старта из `/proc`, переживает рестарт |
| `resource_monitor.py` | снимок ресурсов, формула слотов, гистерезис |
| `uploader.py` | чанки с докачкой |
| `reconciliation.py` | сверка после рестарта любой стороны |
| `agent.py` | супервизор |

> **Отклонение от предложенной структуры:** модуль назван `local_store.py`, а не
> `local_database.py`, потому что базы там нет — состояние файловое, как решено в
> техпроекте §7.3. Имя не должно врать о содержимом.

---

## 5. API

### Контур воркера — `/api/v1/worker/*` (bearer-токен машины)

| Метод | Путь | Назначение | Идемпотентность |
|---|---|---|---|
| POST | `/register` | заявка на регистрацию | по `instance_id`; повтор не выдаёт новый токен |
| PUT | `/registration` | обновление возможностей | естественная |
| POST | `/heartbeat` | живость + ресурсы + активные задания | естественная |
| POST | `/jobs/next` | long-poll задания; `204` = нет | атомарный claim |
| POST | `/jobs/{id}/accept` | подтверждение принятия | повтор → тот же ответ |
| POST | `/jobs/{id}/reject` | отказ от задания | по `attempt_id` |
| GET | `/jobs/{id}/source` | скачать пакет (`Range` → докачка) | естественная |
| POST | `/events` | пакет событий | `first_seq` + курсор |
| POST | `/resources` | внеочередной снимок ресурсов | естественная |
| POST | `/uploads` | создать сессию загрузки | по `(job, attempt, hash)` |
| GET | `/uploads/{id}` | состояние сессии (докачка) | естественная |
| PUT | `/uploads/{id}/chunks/{idx}` | чанк | по `(upload_id, idx, sha256)` |
| POST | `/uploads/{id}/complete` | завершить и провалидировать | повтор → тот же ответ |
| GET | `/commands` | забрать команды | естественная |
| POST | `/commands/{id}/ack` | подтвердить | `noop` при повторе |
| GET | `/update/manifest` | контракт обновления | всегда `204` на этом этапе |
| POST | `/reconcile` | сверка после рестарта | чистое чтение |

### Контур оператора — `/api/workers/*` (портальная cookie)

`GET /status` (работает всегда) · `GET ""` · `GET /{id}` · `POST /{id}/approve|revoke|rotate-token` ·
`GET /jobs/list` · `POST /jobs` · `GET /jobs/{id}` · `GET /jobs/{id}/events|logs|result`

---

## 6. Безопасность тестового задания

Требование §4 задания выполнено **конструкцией**, а не проверкой.

| Что центр НЕ передаёт | Почему это невозможно |
|---|---|
| shell-команду | в схеме `TestJobParams` таких полей нет (`extra="forbid"`) |
| имя исполняемого файла | путь берётся из `Path(test_process.__file__)` |
| аргументы | `build_argv()` возвращает ровно 4 элемента фиксированной формы |
| путь | `result_dir` вычисляется от каталога задания на воркере |
| переменные окружения | `build_env()` — белый список из 5 переменных |

Разрешённая нагрузка: `label` (только `[A-Za-z0-9._-]`, ≤64), `steps` (1..100),
`step_seconds` (0..10), `result_bytes` (0..8 МиБ), `fail_at_step`.

**Три рубежа зажима**, каждый держит оборону сам:
1. центр — pydantic + проверка суммарной длительности против `DISTRIBUTED_WORKERS_TEST_JOB_MAX_SEC`;
2. воркер — `test_runner.validate_params` (отвергает неизвестные поля, зажимает диапазоны);
3. сам процесс — повторный clamp в `test_process.main`.

---

## 7. Инварианты и где они закреплены

| Инвариант | Реализация | Тест |
|---|---|---|
| I-01 потеря heartbeat ≠ остановка | конвейер пишет в файловый outbox, сети на его пути нет | `test_offline_run_then_late_delivery` |
| I-02 не признавать зомби по локальным сигналам | ребра `running → failed` для роли `center` нет в таблице | `test_center_cannot_fail_running_job` |
| I-03 нет авто-переназначения | ребра `running → assigned` нет вовсе | `test_no_auto_reassign_edge` |
| I-04 идемпотентность событий | `UNIQUE(job_id, attempt_id, sequence)` + курсор в одной транзакции | `test_event_batch_idempotent` |
| I-05 одно исполнение | частичный уникальный индекс + `execution_token` | `test_double_assignment_blocked_by_index`, `test_execution_token_mismatch_rejected` |
| I-06 нет дубля результата | сессия по хэшу, чанк по `(upload_id, idx)` | `test_upload_session_is_idempotent` |
| I-07 не публиковать до проверок | `completed` только из `validating`; провал → `rejected_results/` | `test_failed_validation_does_not_publish` |
| I-08 не удалять без подтверждения | `retention_until` = NULL до `validated_at` | `test_retention_unconfirmed_is_computed_not_a_state` |
| I-09 переживание рестартов | SQLite WAL на центре, файлы на воркере, `seq` не сбрасывается | `test_outbox_seq_is_monotonic_and_survives_restart` |
| I-10/I-11 нет произвольных команд | закрытый enum на обеих сторонах | `test_command_enum_has_no_shell` |
| I-12 очистка секретов | редакция **при записи** в outbox + повтор на центре | `test_outbox_redacts_secrets_on_write` |

---

## 8. Тесты

```bash
python -m pytest tests/test_distributed_workers_e2e.py \
                 tests/test_distributed_workers_center.py \
                 tests/test_distributed_workers_agent.py \
                 tests/test_distributed_workers_flag_off.py -v
```

**82 теста, все зелёные.** Разбиение:

| Файл | Тестов | Тип |
|---|---|---|
| `test_distributed_workers_e2e.py` | 3 | end-to-end: настоящий агент против настоящего приложения через `SyncASGITransport` (без сокетов и портов) |
| `test_distributed_workers_center.py` | 31 | unit + integration центра |
| `test_distributed_workers_agent.py` | 37 | unit агента |
| `test_distributed_workers_flag_off.py` | 11 | regression: платформа при выключенном флаге + контроль границ |

Вспомогательный `tests/distributed_workers_helpers.py` — синхронный мост к ASGI
(агент по проекту синхронный и живёт в потоках, а `httpx.ASGITransport` асинхронный).

---

## 9. Известные ограничения этапа

1. **Ротация токена без grace-периода.** Старый гасится сразу, воркер надо перезапустить с новым. Grace на сутки (§20.3 техпроекта) появится вместе с автообновлением.
2. **`reconcile` при недоступности центра возвращает пустой вердикт** — агент продолжает по локальному состоянию. Это следствие I-01, а не недоделка.
3. **Отмена задания** реализована на стороне агента (`cancel_job` → `terminate_job`), но операторской кнопки на экране пока нет: команда ставится только через API.
4. **Компрессия — gzip.** `zstandard` в окружении отсутствует; `zstd` объявлен полем манифеста и читается, если пакет установлен.
5. **Исходный пакет синтетический** — описание задания, а не дерево `projects_v2`. Сборка настоящего пакета проекта — шаг 2 из §25 техпроекта.
6. **Слоты считаются по «лёгкому» профилю** (`RAM_PER_JOB_LIGHT = 1 ГБ`): норм-базы на воркере нет. Профиль с норм-этапом (6,5 ГБ) объявлен рядом и включится вместе с ним.
7. **Геометрические тесты не собираются в worktree** — им нужен каталог `projects_v2/` с рабочими данными, которого в worktree нет. К этим изменениям отношения не имеет (в основном дереве собираются).

---

## 10. Что делать дальше

Порядок из §25 техпроекта, шаги 1–2:

1. **`ExecutionBackend` без удалённого режима** — ввести абстракцию, ничего не изменив в поведении: `LocalExecutionBackend.run()` = однострочный делегат в `_dispatch_action`. Критерий: `test_execution_backend_local_parity` + суточный прогон реальных аудитов без отличий.
2. **Настоящий пакет проекта** — сборка дерева версии по `is_source_file()`, переписывание абсолютных путей, гидрация кропов офлайн. Критерий: `detect_resume_stage` на распакованном пакете даёт ту же точку, что на исходном дереве.

**Чего делать НЕ нужно** до этих двух шагов: подключать реальный аудит к удалённому исполнению, трогать `cleanup_zombies`, вводить автовыбор воркера.

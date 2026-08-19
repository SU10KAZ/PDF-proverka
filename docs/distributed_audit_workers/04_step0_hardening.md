# Этап 3.5 — усиление вертикального среза распределённых audit-worker

**Статус: HARDENING PARTIAL.** Тестовый контур `test_pipeline_v1` управляем,
переживает перезапуск сетевого агента, безопасен для повторных попыток и
готов к контролируемому хранению данных. Подключать реальный аудит **ещё
нельзя** — см. §29 «Известные ограничения» и §30 «Готовность к следующему
этапу»: часть обязательных для реального пайплайна вещей намеренно не сделана.

Флаг `DISTRIBUTED_WORKERS_ENABLED` по-прежнему **выключен по умолчанию**.

Ветка: `feat/distributed-audit-workers-hardening`
База: `feat/distributed-audit-workers-step0` (HEAD `d498932c`)

---

## 1. Причина промежуточного этапа

Повторная состязательная проверка этапа 0 нашла четыре ограничения, каждое из
которых делало подключение реального аудита опасным:

1. **Операторского управления попыткой не было вовсе.** Ни отмены, ни
   признания попытки потерянной, ни создания новой попытки, ни просмотра
   результата старой. Застрявшее задание чинилось правкой БД руками.
2. **Выполняемый процесс не был отделён от сетевого.** Перезапуск агента убивал
   работу, а второй запуск агента порождал второй процесс аудита.
3. **Внешний идентификатор проекта был ограничен ASCII.** Реальные коды в этом
   репозитории — `13АВ/РД-АР3-К7`, `ЖК «Событие 6.2» / корпус 3`: кириллица,
   пробелы, кавычки и `/`. Такой код нельзя использовать как путь.
4. **RetentionManager отсутствовал.** Подтверждённые пакеты на воркере не
   чистились, диск рос без ограничений, а неподтверждённый результат удалять
   нельзя ни при каких условиях.

---

## 2. Исходное состояние Step 0

Что уже работало и на этом этапе не переписывалось: центральная подсистема,
SQLite WAL, двухэтапная регистрация VPS (`register` → одобрение → `claim`),
worker-token, heartbeat, ручная выдача `test_pipeline_v1`, безопасная передача
TAR, локальный EventOutbox, работа при потере связи, возобновляемая загрузка
результата, экран «Аудит-воркеры», 156 тестов и smoke.

Ограничение схемы этапа 0, из которого следовало всё остальное: **попытка была
колонками одной строки `remote_jobs`**. Создать вторую попытку = затереть
первую. Истории, отдельного результата старой попытки и признака «оператор
больше не считает попытку текущей» выразить было негде.

---

## 3. Что реализовано

| Требование задания | Где | Проверка |
|---|---|---|
| Логическое задание и попытки | `schema.py` миграция 3, `repositories.py` | `test_step0_database_migrates_without_data_loss` |
| Сохранение всех старых попыток | `repositories.create_next_attempt` | `test_attempt_numbers_are_never_reused` |
| Ручной запрос отмены | `attempt_service.request_cancel` | `test_cancel_creates_persistent_command_and_does_not_fake_cancelled` |
| Подтверждение отмены воркером | `attempt_service.apply_cancel_ack` | `test_cancel_ack_moves_attempt_to_cancelled_only_on_proof` |
| Признание попытки потерянной | `attempt_service.mark_lost` | `test_mark_lost_does_not_claim_process_stopped` |
| Безопасное создание нового attempt_id | `attempt_service.create_attempt` | `test_new_attempt_after_mark_lost` |
| Защита от двух активных попыток | индекс `ux_attempts_one_active` | `test_only_one_active_attempt_per_job` |
| Результат старой попытки отдельно | `job_service.store_unpublished_result` | `test_old_attempt_result_goes_to_superseded_storage` |
| История попыток | `GET /api/workers/jobs/{id}/attempts` | `test_attempt_history_endpoint` |
| Журнал административных действий | таблица `worker_admin_actions` | `test_admin_action_log_is_append_only` |
| Persistent WorkerCommand | `worker_commands` + `/commands/next` | `test_command_delivered_only_to_owner_and_survives_redelivery` |
| Локальная очередь команд воркера | `local_db.local_commands` | `test_cancel_terminates_only_verified_process` |
| Разделение agent/executor | `audit_worker/agent.py`, `audit_worker/executor.py` | `test_killing_agent_does_not_stop_the_audit` |
| Продолжение работы при рестарте агента | процесс в своей сессии, файлы вместо пайпов | там же |
| Защита от повторного запуска | `local_db.claim_next` + `recover_after_restart` | `test_two_executors_never_start_two_processes` |
| Безопасная отмена своего процесса | `process_control.py` | `test_cancel_refuses_when_fingerprint_does_not_match` |
| Кириллица, пробелы и `/` в коде проекта | `identifiers.py`, `paths.py` | `test_external_project_codes_are_accepted_and_never_hit_paths` |
| Хранение только по UUID | `identifiers.attempt_dir` | `test_no_module_builds_path_from_project_id` |
| RetentionManager | `audit_worker/retention.py` | `test_retention_dry_run_is_default` |
| Dry-run по умолчанию | `AUDIT_WORKER_RETENTION_DELETE_ENABLED=false` | там же |
| Ручная команда удаления | `POST …/request-deletion` | `test_manual_deletion_request_requires_acknowledged_result` |
| Предупреждения по диску | `RetentionManager.disk_snapshot` | `test_disk_critical_blocks_new_jobs_but_not_running` |
| Новые состояния в UI | `frontend/static/js/audit-workers.js` | `test_frontend_builds_dom_without_innerhtml` |
| Тесты, smoke, документация | `tests/…`, `scripts/smoke_…py`, этот файл | — |

---

## 4. Что намеренно НЕ реализовано

Это не «забыли», а граница этапа (§2.2 задания). Каждый пункт — явное «нет»:

* **реальный аудит, ExecutionBackend, интеграция с PipelineManager.**
  `backend/app/pipeline/manager.py` не изменён ни одной строкой (проверяется
  тестом `test_pipeline_manager_untouched`);
* **автоматическое сравнение и продвижение результата старой попытки.** Пакет
  сохраняется, показывается и скачивается — и всё. Решение принимает человек;
* **принудительное удаление НЕподтверждённого результата.** Отдельного
  эндпоинта нет намеренно (§12.4 задания);
* **централизованное обновление воркера.** `GET /update/manifest` по-прежнему
  отвечает 204 «обновлений нет» и притворяться не должен;
* **автоматический выбор VPS и авто-переназначение задания.** Назначение
  ручное, ребра `running → assigned` в машине состояний нет;
* **пять параллельных реальных аудитов, S3, Redis, RabbitMQ, Kubernetes,
  Claude Code, Codex, нормативный этап.** Ничего из этого не появилось;
* **подключение боевого VPS 176.12.77.31.** На этом этапе не выполнялось.

---

## 5. Миграция центральной БД

`backend/app/services/distributed_workers/schema.py`, `SCHEMA_VERSION = 3`.

**Порядок применения.** Миграции нумерованные, вперёд-только. Каждая идёт
ПООПЕРАТОРНО внутри одной явной транзакции вместе с записью в
`schema_migrations` (`schema.migrate`). `executescript` не используется: он
коммитит текущую транзакцию перед запуском, и миграция под ним не может быть
атомарной. Сбой на любом операторе откатывает шаг целиком — базы, где половина
миграции применена, а отметки нет, не существует
(`test_failed_migration_leaves_no_half_applied_schema`).

**Резервная копия.** `database._backup_before_migration` снимает снимок через
`VACUUM INTO` в файл `workers.db.before_v<N>_to_v<M>` до применения миграций.
Копия не снимается для пустой базы и если мигрировать нечего
(`test_backup_is_taken_before_migration`).

**Что делает миграция 3:**

1. заводит `logical_jobs` (что делаем) и `job_attempts` (кто и когда делал);
2. переносит каждую строку `remote_jobs` в пару «логическое задание + попытка
   №1» без потери полей, результатов и журнала переходов;
3. заводит `worker_admin_actions`;
4. расширяет `worker_commands` полями `job_id`, `attempt_id`, `status`,
   `expires_at`;
5. **удаляет таблицу** `remote_jobs` и создаёт **представление** с тем же
   именем — «текущая попытка задания». Весь читающий код этапа 0 работает без
   изменений, а писать в него физически нельзя: SQLite отвергает UPDATE по
   представлению. Гарантия здесь ровно одна и не больше: **через представление
   состояние не изменить**. Утверждать «единственный писатель — `transition`»
   нельзя: `claim_next_job_for_worker`, `update_job_fields` и
   `update_attempt_fields` пишут `execution_state` напрямую в `job_attempts`.
   Первый делает это осознанно (захват в той же транзакции, что и выборка, с
   ручной записью в `job_state_transitions`), остальные два — служебные
   апдейтеры полей. Проверку ролей и таблицу рёбер применяет только
   `transition`; всё, что идёт мимо, обязано быть точечным и разобранным.

Соответствие колонок представления и `repositories.get_attempt` закреплено
тестом `test_view_columns_match_attempt_projection` — расхождение поймается
сразу, а не через полгода.

`workers.db` удалять не требуется.

---

## 6. Миграция worker DB

`audit_worker/local_db.py`, `SCHEMA_VERSION = 1` — база создаётся с нуля при
первом запуске агента или исполнителя (`worker.db` рядом с `worker_state.json`).
Каталоги заданий этапа 0 продолжают читаться: `audit_worker/paths.py`
допускает ключи вида `att_1a2b3c4d` при чтении существующих путей, но новые
ключи всегда UUID.

---

## 7. Логическое задание и попытки

```
logical_jobs                       job_attempts
  job_id (UUID, PK)   ◄────────────  job_id (FK)
  project_external_id                attempt_id (UUID, PK)
  project_display_name               attempt_number      UNIQUE(job_id, №)
  project_version_id                 assignment_generation
  job_type                           assigned_worker_id
  payload                            execution_token_hash
  current_attempt_id  ─────────────► execution_state     ← ось ИСПОЛНЕНИЯ
  overall_state                      attempt_disposition ← ось РАСПОЛОЖЕНИЯ
  created_at / created_by            result_storage_class
                                     retention_until, …
```

**Две оси ортогональны, и это главное свойство модели.**
`execution_state = 'running'` вместе с
`attempt_disposition = 'operator_declared_lost'` читается как «центр больше не
считает попытку текущей», но **не** как «процесс на VPS остановлен» (I-06).
Смешивать их запрещено: именно смешение и порождает ложь «система сказала
failed, значит там всё кончилось».

`attempt_disposition ∈ {active, completed, cancelled, operator_declared_lost,
superseded}`. `completed` означает «попытка закончилась своим ходом» — в том
числе провалом: `failed` это законченная попытка, а не отозванная.

Ограничения обеспечены схемой, а не дисциплиной кода:

* `UNIQUE(attempt_id)` — первичный ключ;
* `UNIQUE(job_id, attempt_number)` — номера не переиспользуются;
* частичный `ux_attempts_one_active ON job_attempts(job_id) WHERE
  attempt_disposition='active'` — ровно одна активная попытка (I-04);
* частичный `ux_logical_jobs_active_project` — одно активное задание на
  (проект, версия);
* `current_attempt_id` всегда указывает на активную либо последнюю попытку.

Старые строки при создании новой попытки **не перезаписываются**
(`create_next_attempt`).

---

## 8. Операторская отмена

`POST /api/workers/jobs/{job_id}/attempts/{attempt_id}/cancel`

Тело: `reason` (обязательно), `confirmation` (ровно «ОТМЕНИТЬ»),
`grace_period_sec` (0…600). Заголовки: `X-Requested-With: audit-workers` и
`Idempotency-Key` — оба обязательны.

Что происходит:

1. попытка переводится в `cancel_requested` (не `cancelled`!);
2. создаётся **persistent** `worker_commands` со `status='pending'`, адресная
   к паре (job, attempt);
3. ответ прямо говорит: «Если VPS сейчас офлайн, команда будет доставлена
   после восстановления связи — мгновенная остановка не гарантируется».

`cancelled` появляется **только** после подтверждения воркера с исходом
`cancelled`, `already_cancelled` или `not_running_locally`
(`attempt_service.apply_cancel_ack`). Ответы `ownership_mismatch` и
`ambiguous_not_running` состояние не меняют — попытка остаётся видимой
оператору. Роли `center` на ребре `cancel_requested → cancelled` в машине
состояний нет вовсе.

**Гонка «успел закончиться» (§15.1):** если попытка уже терминальна, ответ —
`already_final`, результат не уничтожается, попытка не становится `cancelled`
задним числом.

Повторный запрос с тем же `Idempotency-Key` возвращает записанный результат.
Запрос с новым ключом после неразрешающего ACK создаёт **новую** команду —
иначе попытка навсегда застревала бы в `cancel_requested`.

---

## 9. Признание попытки потерянной

`POST /api/workers/jobs/{job_id}/attempts/{attempt_id}/mark-lost`

Тело: `mandatory_reason`, `typed_confirmation` (ровно «ПОПЫТКА ПОТЕРЯНА»),
`observed_worker_state`, `optional_operator_note`.

Что меняется: **только ось disposition**. `execution_state` остаётся тем, чем
был — `running` остаётся `running`. Выдуманного `failed` здесь нет, потому что
центр не знает, остановился ли процесс (I-06). Ответ содержит дословно:
«Удалённый процесс может продолжать работу. После создания новой попытки
результаты старой будут считаться устаревшими».

`execution_token` старой попытки **не отзывается**: по нему вернувшийся воркер
попадёт в контур своей попытки и сможет сдать события и архив. Права менять
актуальное состояние задания у него при этом нет (I-07).

Автоматического удаления старого результата нет.

---

## 10. Создание новой попытки

`POST /api/workers/jobs/{job_id}/attempts`

Тело: `worker_id`, `reason`, `source_attempt_id`, `confirmation` («НОВАЯ
ПОПЫТКА»).

Разрешено только поверх попытки с disposition из
`{completed, cancelled, operator_declared_lost, superseded}`. Поверх обычного
`running` — 409 с текстом «Новую попытку нельзя создать поверх работающей».

Новая попытка получает новый `attempt_id` (UUID), новый `execution_token`,
новый `assignment_generation`, свежесобранный исходный пакет с новым
`attempt_id` в манифесте. Старая помечается `superseded_by_attempt` и
`superseded_at`, но остаётся целиком.

Два одинаковых запроса (двойной клик) → одна попытка. Два разных запроса
одновременно → один 200 и один 409 по частичному уникальному индексу.

---

## 11. Superseded result

Если отозванная или вытесненная попытка возвращается с готовым архивом:

* архив принимается, sha256 и манифест проверяются;
* кладётся в `superseded_results/<job_uuid>/<attempt_uuid>/`;
* рядом пишется `unpublished_reason.json` с `published: false` и пометкой
  «Результат устаревшей попытки — автоматически не используется»;
* попытка переходит в `superseded_result_received`, `result_storage_class`
  становится `superseded`;
* прогресс актуальной попытки не меняется, `overall_state` задания не
  становится `completed`;
* скачивание — `GET …/attempts/{attempt_id}/result`, имя файла в заголовке
  начинается с `УСТАРЕВШАЯ-ПОПЫТКА_`;
* на экране блок подписан «Не является актуальным результатом задания».

Машинная гарантия: ребро в `superseded_result_received` отвергается, если
disposition попытки всё ещё `active` (`transition`). Похоронить нормальный
результат текущей работы этим ребром нельзя.

---

## 12. WorkerCommand

Таблица `worker_commands`: `command_id`, `worker_id`, `job_id`, `attempt_id`,
`command_type`, `payload`, `idempotency_key` (UNIQUE), `created_at`,
`delivered_at`, `acknowledged_at`, `status`, `result`, `expires_at`.

Разрешённые типы на этом этапе — ровно два: `cancel_attempt`,
`delete_attempt_data`. У каждого своя pydantic-модель с `extra="forbid"`
(`COMMAND_PAYLOAD_MODELS`). Значений `run_shell` / `exec` / `eval` / `script` /
`argv` в enum нет и быть не может (I-10).

API воркера: `POST /api/v1/worker/commands/next` (long-poll, допустим
`wait_sec`) и `POST /api/v1/worker/commands/{id}/ack`. Исторический
`GET /commands` оставлен для совместимости.

* воркер получает **только свои** команды;
* повторная доставка безопасна: `delivered` не выпадает из выдачи, потому что
  доставка ≠ исполнение;
* повторный ACK с тем же результатом — `replayed: true`; **с другим
  результатом — 409**: переписать историю исполнения нельзя;
* истёкшая команда (`expires_at`) не выдаётся;
* команда с неизвестным типом или лишним полем нагрузки воркеру не выдаётся, а
  гасится машинным ACK `unsupported_command_type` — иначе она висела бы в
  очереди вечно и `has_pending_commands` навсегда остался бы `true`;
* команды переживают перезапуск центра.

---

## 13. Audit log оператора

Таблица `worker_admin_actions`: `action_id`, `actor_id`, `actor_display_name`,
`action_type`, `worker_id`, `job_id`, `attempt_id`, `previous_state_json`,
`requested_state_json`, `reason`, `idempotency_key`, `request_id`, `source_ip`,
`user_agent`, `created_at`, `result_status`, `result_json`.

Типы: `approve_worker`, `reject_worker`, `rotate_worker_token`, `create_job`,
`cancel_attempt`, `mark_attempt_lost`, `create_attempt`,
`request_worker_data_deletion`.

* журнал append-only через `repositories.record_admin_action`; функции
  удаления записей нет ни в сервисе, ни в API (проверяется тестом: в роутере
  нет ни одного метода DELETE);
* `actor` берётся **из аутентификации**, а не из тела запроса;
* секреты проходят redaction (в журнал попадают причина и запрошенное
  состояние, но не токены);
* повтор идемпотентного действия не создаёт вторую запись: частичный
  уникальный индекс `ux_admin_actions_idem`;
* чтение: `GET /api/workers/admin-actions`, на экране — только просмотр.

---

## 14. Agent / executor

```
                 сеть                          локально
┌──────────────────────────┐        ┌────────────────────────────┐
│ audit-worker-agent       │        │ audit-worker-executor      │
│  регистрация, claim      │        │  захват попытки из очереди │
│  heartbeat               │ worker │  запуск test_pipeline_v1   │
│  long-poll заданий       │◄──.db─►│  реестр процессов          │
│  скачивание пакета       │        │  stdout/stderr → файлы     │
│  EventOutbox → центр     │        │  completed.marker          │
│  upload результата       │        │  сборка архива             │
│  приём WorkerCommand     │        │  отмена, RetentionManager  │
└──────────────────────────┘        └────────────────────────────┘
   знает токен                          токена не знает
   процессов не запускает               к центру не ходит
```

Локальные таблицы (`audit_worker/local_db.py`): `executor_instances`,
`execution_queue`, `local_commands`, `process_registry`.

Команды: `python -m audit_worker agent`, `python -m audit_worker executor`.
`python -m audit_worker run` оставлена **только для разработки**: она печатает
предупреждение и поднимает исполнителя рядом; в проде это два systemd-юнита.

Машинные гарантии:

* в `audit_worker/agent.py` нет ни `kill`, ни `killpg`, ни `SIGTERM/SIGKILL`,
  ни импорта `test_runner`/`process_control` — проверяется тестом по дереву
  разбора, без учёта комментариев;
* в `audit_worker/executor.py` нет `httpx`, `CenterClient`, `read_token`,
  `WorkerStateStore` — тем же способом;
* точек запуска процесса ровно две и обе известны поимённо: `test_runner.py`
  (сам аудит) и `__main__.py` (dev-самозапуск исполнителя фиксированным argv).

---

## 15. Process ownership

`audit_worker/process_control.py`. Сигнал уходит только процессу, чья
принадлежность доказана **четырьмя** совпадениями:

1. запись `process_registry` относится к нужной паре (job_id, attempt_id);
2. pid жив;
3. тик старта из `/proc/<pid>/stat` совпадает с записанным — pid
   переиспользуется системой (I-17);
4. отпечаток команды из `metadata.json` совпадает с отпечатком в реестре —
   **второй независимый источник**; сверять реестр сам с собой бессмысленно.

Дополнительно проверяется группа процессов: сигнал уходит `killpg` по группе,
созданной нами через `start_new_session=True`, и только если фактический
`getpgid(pid)` совпадает с записанным.

`pkill`, `killall`, `pgrep`, `os.system`, поиск по строке команды — отсутствуют
и запрещены тестом.

Исходы (закрытый набор, уезжают в ACK): `cancelled`, `already_completed`,
`already_cancelled`, `not_running_locally`, `ownership_mismatch`,
`ambiguous_not_running`.

---

## 16. Перезапуск агента

Сценарий подтверждён **автоматическим** тестом
`test_killing_agent_does_not_stop_the_audit` (настоящий uvicorn + настоящий
`python -m audit_worker agent` + настоящий `python -m audit_worker executor`):

1. агент получает задание и кладёт попытку в `execution_queue`;
2. исполнитель захватывает её и запускает процесс;
3. тест проверяет, что агент **не является родителем** процесса и что процесс
   в **другой группе процессов**;
4. агент убивается `SIGKILL`;
5. исполнитель жив, процесс аудита жив;
6. журнал событий продолжает расти (проверяется ростом `outbox-*.jsonl`);
7. агент поднимается заново;
8. `process_registry` по-прежнему содержит **один** pid — второго процесса не
   появилось (I-03);
9. работа доводится до конца, центр принимает результат.

Ключевая деталь, без которой это не работало: процесс аудита пишет
stdout/stderr **прямо в файлы**, а не в пайпы наблюдателя. Пока трубы держал
исполнитель, его смерть закрывала пайп и процесс падал от SIGPIPE на первой же
строке вывода — то есть наблюдатель убивал работу самим фактом своего ухода.

---

## 17. Перезапуск исполнителя

`Executor.recover_after_restart` разбирает три случая раздельно:

| Что видит | Что делает |
|---|---|
| pid жив, тик старта совпал | не трогает; поднимает наблюдателя и доводит до конца |
| pid мёртв, есть отметка завершения | доупаковывает результат, работа не теряется |
| pid мёртв, отметки нет | состояние `executor_interrupted`, **автоповтора нет** |

Отметок завершения две и это не избыточность: `completed.marker` пишет
исполнитель, дождавшийся выхода, а `work/process_exit.json` — **сам процесс**
последним действием. Второй источник и есть ответ на «что если наблюдателя
перезапустили посреди работы»: только процесс знает исход достоверно.

`executor_interrupted` — диагностическое состояние. Исполнитель ничего не
перезапускает, агент передаёт событие `job_failed` с кодом
`executor_interrupted`, решение о повторе принимает оператор (§8.6 задания).
`running` без подтверждённой живости не изображается никогда.

---

## 18. Безопасные project identifiers

Три разные сущности:

| Поле | Что это | Может ли попасть в путь |
|---|---|---|
| `project_external_id` | код проекта: `13АВ/РД-АР3-К7` | **никогда** |
| `project_display_name` | название для экрана | **никогда** |
| `job_id`, `attempt_id` | UUID | только они |

`identifiers.normalize_external_id`: NFC-нормализация (иначе «й» в двух
кодировках даёт два разных проекта), обрезка пробелов по краям; запрещены NUL,
управляющие символы, пустая строка и длина > 200. Кириллица, пробелы, кавычки,
скобки и `/` разрешены — это легальная часть кода, а не попытка обхода.

`identifiers.attempt_dir` / `paths.attempt_dir` строят путь **только** из UUID
и отвергают всё остальное до того, как строка попадёт в `Path`. Проверено на
кодах из задания: `13АВ/РД-АР3-К7`, `АР — 001 план потолка`,
`ЖК «Событие 6.2» / корпус 3`, пробелы, кавычки, HTML-теги, длинное имя, NUL,
`../../etc/passwd`.

Имя файла для скачивания (`safe_download_filename`) чистится от разделителей и
схлопывает `..`, но **читается файл всегда по UUID из БД**.

Отдельный тест грепает исходники подсистемы на склейку пути из
`project_id`/`project_external_id`/`project_display_name`.

---

## 19. RetentionManager

`audit_worker/retention.py`. Отвечает **только** за локальные копии на
воркере; центральные пакеты он не трогает и не может (I-14).

**Начало срока (§12.1).** До подтверждения приёма центром: `retention_until`
пуст, признак `retention_unconfirmed` взведён, автоматическое удаление
запрещено. После подтверждения `retention_until = confirmed_at +
AUDIT_WORKER_RETENTION_DAYS` (по умолчанию 30 дней).

**Сухой прогон по умолчанию.** При `AUDIT_WORKER_RETENTION_DELETE_ENABLED=false`
менеджер считает кандидатов, показывает ожидаемый освобождаемый объём, пишет
`runtime/retention_report.json` — и **не стирает ничего**, а записи `deleted`
в БД не появляется.

**Запреты удаления** (проверяются и в центре, и на воркере — каждый рубеж
держит оборону сам): активная попытка; попытка ещё в локальной очереди; процесс
помечен работающим; нет подтверждённого hash; `retention_unconfirmed`; в outbox
остались неотправленные события; срок не наступил (для автоматического прохода).

**Физическое удаление** при явно включённом флаге: tombstone → атомарное
переименование каталога в локальную корзину → стирание содержимого. Повтор
после сбоя безопасен, `purge_trash` дочищает корзину. `realpath` проверяется на
принадлежность `WORKER_DATA_DIR/jobs`; симлинк удаляется **как ссылка**, цель
остаётся нетронутой. Запись о попытке и её hash сохраняются в
`runtime/tombstones/<attempt_id>.json` навсегда.

**Ручная команда** `delete_attempt_data` идёт тем же путём: центр →
WorkerCommand → агент → `local_commands` → RetentionManager. Агент её не
исполняет. Неподтверждённый результат этой командой удалить нельзя.

Конфигурация:

```
AUDIT_WORKER_RETENTION_ENABLED=true
AUDIT_WORKER_RETENTION_DELETE_ENABLED=false
AUDIT_WORKER_RETENTION_DAYS=30
AUDIT_WORKER_RETENTION_SCAN_INTERVAL_SEC=3600
AUDIT_WORKER_DISK_WARNING_FREE_BYTES=5368709120
AUDIT_WORKER_DISK_CRITICAL_FREE_BYTES=1073741824
```

Ручной осмотр: `python -m audit_worker retention` — печатает кандидатов и
разрез диска, ничего не удаляя.

---

## 20. Disk warnings

Heartbeat несёт `disk`: `total_bytes`, `used_bytes`, `free_bytes`,
`jobs_bytes`, `confirmed_results_bytes`, `unconfirmed_results_bytes`,
`cleanup_candidates_bytes`, `cleanup_candidates`, `level`.

* `warning` — экран показывает предупреждение;
* `critical` — `worker_registry.can_receive_jobs` возвращает отказ, heartbeat
  перестаёт отдавать `has_available_work`, а `POST /jobs/next` отвечает 409.
  **Текущие задания не убиваются, неподтверждённые результаты не удаляются**
  даже при нехватке места.

Значения чистятся `worker_registry.sanitize_disk`: не число — значит `None`,
`level` вне закрытого набора — `unknown`.

---

## 21. Изменения UI

`frontend/audit-workers.html`, `frontend/static/js/audit-workers.js`,
`frontend/static/css/audit-workers.css`.

* карточка VPS показывает **раздельно** связь агента и состояние исполнителя
  (`online / stale / offline / interrupted / unknown`), его
  `executor_instance_id`, число процессов и неоднозначных процессов;
* блок хранения: уровень диска, свободно, кандидаты на очистку и их объём,
  объём неподтверждённых результатов;
* секция «История попыток»: номер, VPS, состояние исполнения, расположение,
  «текущая/устаревшая», начало, длительность, прогресс, результат, hash,
  retention, команды с их доставкой и ответом, операторские действия;
* четыре опасные кнопки, каждая требует причину и **ввод подтверждающей фразы
  руками** — `ОТМЕНИТЬ`, `ПОПЫТКА ПОТЕРЯНА`, `НОВАЯ ПОПЫТКА`,
  `УДАЛИТЬ ДАННЫЕ`; тексты предупреждений говорят правду про офлайн-VPS и про
  возможный живой процесс;
* результат устаревшей попытки — отдельным блоком с подписью «Не является
  актуальным результатом задания»;
* секция «Журнал операторских действий» — только чтение.

**Безопасность разметки.** Карточки и списки строятся DOM-API (`createElement`
+ `textContent`). Слова `innerHTML`, `outerHTML`, `insertAdjacentHTML` в
скрипте отсутствуют — проверяется тестом. Опасные вызовы уходят с
`X-Requested-With: audit-workers` и `Idempotency-Key`.

---

## 22. Безопасность

| Что | Как закрыто |
|---|---|
| Операторский API fail-closed | при `PORTAL_AUTH_ENABLED=false` и без `DISTRIBUTED_WORKERS_ALLOW_INSECURE_ADMIN` роутер не монтируется вовсе |
| CSRF | `SameSite=lax` у портальной cookie + обязательный `X-Requested-With` на опасных ручках |
| Idempotency-Key | обязателен на всех опасных операторских действиях |
| Разделение контуров | `/api/v1/worker/*` — bearer-токен; `/api/workers/*` — портальная cookie; токен воркера в операторский API не пускают |
| Execution-token | попытка ищется ПО ХЭШУ токена; старый токен не может менять новую попытку |
| Хранение токенов | только sha256; колонки с открытым токеном в схеме нет |
| Rotate-token | требует операторской авторизации и `X-Requested-With`, пишется в журнал, старый токен отзывается атомарно, новый показывается один раз |
| Redaction | секреты чистятся при записи в outbox и повторно на центре |
| TAR safety | без изменений с этапа 0: symlink/hardlink/устройства запрещены, префикс `payload/`, дубли путей, потолок степени сжатия, per-file sha256 |
| Path traversal | пути только из UUID; `?attempt=../../secret` → 400 |
| Symlink deletion escape | симлинк удаляется как ссылка, цель не трогается |
| Process ownership | четыре независимых совпадения, `killpg` только по своей группе |
| Command allowlist | закрытый enum + `extra="forbid"` + повторная валидация при выдаче |
| XSS | DOM-API вместо строк; снимки воркера чистятся по типам и длинам |
| Секреты исполнителю | не передаются: в `executor.py` нет ни клиента, ни чтения токена |

---

## 23. Systemd

`docs/distributed_audit_workers/systemd/audit-worker-agent.service`
`docs/distributed_audit_workers/systemd/audit-worker-executor.service`

Ключевые решения и почему именно так:

* **никакой связи между юнитами.** Ни `Requires=`, ни `PartOf=`, ни
  `BindsTo=`: любая из них означала бы, что рестарт агента останавливает
  работу, ради разделения с которой всё и делалось;
* `KillMode=process` в обоих юнитах. При `control-group` systemd прибил бы всё
  порождённое; процессы аудита живут в своих сессиях (`setsid`) и уходить
  вместе с наблюдателем не должны;
* `Restart=always` у обоих;
* один системный пользователь `audit-worker`, общий `WORKER_DATA_DIR`,
  раздельные лог-файлы;
* секретов **нет в командной строке**: `/proc/<pid>/cmdline` читается кем
  угодно. Токен и claim-secret — файлы 0600, которые создаёт и читает только
  агент; в окружении исполнителя нет ни адреса центра, ни секретов;
* `ProtectSystem=strict` + `ReadWritePaths=/var/lib/audit-worker`.

`python -m audit_worker run` в проде не используется: команда печатает
предупреждение, что это режим разработки.

---

## 24. API

**Воркер** (`/api/v1/worker/*`, bearer-токен), новое на этапе 3.5:

| Метод | Путь | Назначение |
|---|---|---|
| POST | `/commands/next` | long-poll за командами |
| POST | `/commands/{id}/ack` | подтверждение; 409 при конфликте результата |

Расширены: `/heartbeat` (поля `executor`, `disk`), `/reconcile` (поля
`agent_instance_id`, `executor`, `disk`, расширенный вердикт).

**Оператор** (`/api/workers/*`, портальная cookie), новое:

| Метод | Путь | Назначение |
|---|---|---|
| GET | `/jobs/{job_id}/attempts` | история попыток |
| POST | `/jobs/{job_id}/attempts` | новая попытка |
| POST | `/jobs/{job_id}/attempts/{attempt_id}/cancel` | запрос отмены |
| POST | `/jobs/{job_id}/attempts/{attempt_id}/mark-lost` | признать потерянной |
| POST | `/jobs/{job_id}/attempts/{attempt_id}/request-deletion` | удалить данные с VPS |
| GET | `/jobs/{job_id}/attempts/{attempt_id}/result` | скачать пакет попытки |
| GET | `/admin-actions` | журнал операторских действий |

Все семь операторских ручек требуют заголовок `X-Requested-With:
audit-workers`. Пять из них (`rotate-token`, `cancel`, `mark-lost`, `attempts`,
`request-deletion`) требуют дополнительно `Idempotency-Key` — там повтор
потребляется журналом. Подтверждающие фразы в теле: `ОТМЕНИТЬ`,
`ПОПЫТКА ПОТЕРЯНА`, `НОВАЯ ПОПЫТКА`, `УДАЛИТЬ ДАННЫЕ` — поля `confirmation`
(cancel, attempts, request-deletion) и `typed_confirmation` (mark-lost).

> Задание предлагало префикс `/api/audit-workers/…`. Оставлен существующий
> `/api/workers/…`: он уже принадлежит операторскому контуру, вынесен в
> `portal_auth`, используется экраном и всеми тестами этапа 0. Заводить второй
> префикс ради одного этапа — плодить путаницу, а не порядок.

---

## 25. Машина состояний

Ось исполнения — 15 значений этапа 0 без изменений. Что добавилось:

* `cancel_requested` достижимо из `created`, `assigned`, `source_uploading`,
  `source_ready`, `accepted_by_worker`, `running`, `completed_locally` — но
  только для роли `operator`;
* `cancel_requested → cancelled` доступно **только роли worker**;
* ребро `→ superseded_result_received` добавлено из 10 состояний оси
  исполнения (всех, кроме `created`, `result_received`, `validating`,
  `completed` и `superseded_result_received`) и разрешено **только** для
  попытки с `attempt_disposition != active`; проверка машинная, в `transition`;
* прямого операторского ребра `→ cancelled` начиная с `assigned` НЕТ: как
  только пакет у воркера, «отменено» ставится только по его подтверждению.
  Ребро осталось у `created`, где исполнителя ещё нет;
* при достижении терминального состояния `transition` сам выставляет
  disposition, **не перебивая** назначенное оператором
  (`OPERATOR_DISPOSITIONS`);
* `overall_state` логического задания ведётся только по ТЕКУЩЕЙ попытке: хвост
  отозванной попытки не вправе объявить задание завершённым.

---

## 26. Гонки и их разрешение

| Гонка | Правило | Тест |
|---|---|---|
| Cancel против завершения (§15.1) | `already_final`, результат цел, задним числом не отменяем | `test_cancel_of_completed_attempt_keeps_result` |
| Mark-lost, старый воркер вернулся (§15.2) | новая попытка current; старые события — в историю старой; результат — в superseded; overall не меняется | `test_old_attempt_events_do_not_touch_the_new_one`, `test_old_attempt_result_goes_to_superseded_storage` |
| Двойное создание attempt (§15.3) | тот же ключ → та же попытка; разные ключи → 409 по индексу | `test_two_identical_create_attempt_requests_make_one`, `test_two_different_create_attempt_requests_conflict` |
| Cancel и mark-lost вместе (§15.4) | подтверждённый cancel делает mark-lost ненужным; при pending cancel mark-lost допустим и pending-команда остаётся, чтобы остановить старый процесс; новая попытка — только после явного mark-lost; поздний ACK относится к старой попытке | `test_cancel_ack_moves_attempt_to_cancelled_only_on_proof` |
| Удаление против upload (§15.5) | пока upload не подтверждён и в outbox есть события — удалять нельзя; повтор → `already_deleted` | `test_retention_refuses_active_attempt_and_pending_outbox` |
| Два исполнителя | `BEGIN IMMEDIATE` + условный UPDATE; `claim_generation` остаётся 1 | `test_two_executors_never_start_two_processes` |

---

## 27. Автоматические тесты

Числа — по фактическому сбору `pytest --collect-only`, не по памяти.

| Файл | Про что | Тестов |
|---|---|---|
| `tests/test_distributed_workers_hardening.py` | дефекты, закрытые повторной сверкой этапа 0 | 70 |
| `tests/test_distributed_workers_step35.py` | миграции, операторские действия, старый результат, WorkerCommand, идентификаторы, retention, UI-безопасность | 60 |
| `tests/test_distributed_workers_agent.py` | агент этапа 0 | 37 |
| `tests/test_distributed_workers_center.py` | центр этапа 0 | 31 |
| `tests/test_distributed_workers_review_fixes.py` | дефекты адверсариальной проверки этапа 3.5 (§33) | 27 |
| `tests/test_distributed_workers_flag_off.py` | выключенный флаг, настоящий `main.py`, отсутствие LLM | 15 |
| `tests/test_distributed_workers_executor.py` | agent/executor настоящими процессами, ownership, отмена | 13 |
| `tests/test_distributed_workers_e2e.py` | сквозной срез через ASGI | 3 |
| **Итого** | | **256** |

Тесты, поднимающие несколько процессов, помечены `@pytest.mark.slow`; маркер
зарегистрирован в `pytest.ini`. По умолчанию они ЗАПУСКАЮТСЯ — отключаются
явным `-m "not slow"`.

---

## 28. E2E smoke

`scripts/smoke_distributed_workers_step35.py` — 25 групп проверок (нумерация
шагов сценария 1–48, сгруппированы по 1–3) на настоящих
процессах: uvicorn + `python -m audit_worker executor` + `python -m
audit_worker agent`. Проходит регистрацию, одобрение, выдачу задания, убийство
агента посреди работы, повторный подъём агента, отмену, признание потерянной,
новую попытку, возврат старой, кириллический код проекта, подтверждение
приёма, сухой прогон retention, физическое удаление локальной копии,
перезапуск всех трёх сторон и проверку журнала.

Последний шаг проверяет логи всех трёх процессов на traceback, bootstrap-secret
и токены (`wtk_`, `etk_`).

Claude/Codex/LLM в smoke не участвуют.

```bash
python scripts/smoke_distributed_workers_step35.py           # временный каталог
python scripts/smoke_distributed_workers_step35.py --keep    # оставить артефакты
```

---

## 29. Известные ограничения

Честно и без смягчений:

1. **Реального аудита нет.** Единственный тип задания — `test_pipeline_v1`.
   Всё, что описано выше, проверено на нём и только на нём.
2. **Ролей у операторов нет.** Любой аутентифицированный пользователь портала
   может отменить попытку, признать её потерянной и заказать удаление данных.
   Журнал фиксирует, кто это сделал, но не ограничивает.
3. **Журнал append-only на уровне API, а не хранилища.** Кто имеет доступ к
   файлу `workers.db`, может его изменить. Настоящая неизменяемость требует
   внешнего хранилища и в этот этап не входила.
4. **`max_slots > 1` не проверялся.** Исполнитель механически поддерживает
   несколько одновременных попыток, но агент в основном цикле ведёт одну, и
   параллельная работа не тестировалась.
5. **Ограничения регистрации по частоте нет** — унаследовано с этапа 0.
   Знающий bootstrap-secret может создавать заявки без ограничений.
6. **Миграция вперёд-только.** Откат — восстановление резервной копии
   `workers.db.before_v2_to_v3` (см. §31).
7. **Мигрированные попытки этапа 0 имеют не-UUID `attempt_id`.** Их каталоги
   читаются по послаблению `allow_legacy`; новые ключи всегда UUID.
8. **Long-poll `/commands/next` держит поток обработчика.** При десятках
   воркеров это станет заметно; на пилоте из одного-двух VPS — нет.
9. **Автоматической сверки старого и нового результата нет** (и не
   планировалась на этом этапе).
10. **Корзина удаления чистится только при проходе менеджера.** Если
    исполнитель не запускается, освободившееся место не возвращается.
11. **Smoke требует свободного порта и ~2 минут.** В CI он не включён; это
    ручной прогон.
12. **Окно поиска повтора операторского действия — 500 последних записей**
    журнала. За его пределами повтор ключа перестаёт распознаваться и действие
    выполняется заново; от последствий защищают вторичные проверки состояния,
    но это компромисс, а не гарантия.
13. **`mark_lost` не снимает `current_attempt_id`.** Признанная потерянной
    попытка остаётся «текущей» строкой представления, пока оператор не создал
    новую. Опасные следствия закрыты фильтром `attempt_disposition = 'active'`
    в выдаче и ре-предложении, но сама модель остаётся двусмысленной.
14. **Аренда сборки архива — 30 минут.** Дольше этого распаковка идти не
    должна; если пойдёт, вторая попытка `complete` займёт сессию у ещё живого
    сборщика. Порог подобран, а не измерен на больших пакетах.
15. **`Idempotency-Key` на `/jobs/next` не сверяется с телом запроса** (в
    отличие от `_idempotent`). Повтор с чужим телом отдаст кэшированное
    задание. Опасное следствие — перевыпуск токена чужой попытки — закрыто, но
    сверка тела не добавлена.
16. **Записи `idempotency_keys` не чистятся.** Ни TTL, ни фонового прохода;
    таблица растёт монотонно.
17. **Межпроцессный замок счётчика событий — `flock`.** На не-POSIX
    (или на ФС без поддержки flock) он деградирует до потокового: дубли между
    процессами снова становятся возможны. Целевая платформа — Linux.

---

## 30. Готовность к следующему этапу

Что закрыто из четырёх ограничений, названных в §1: **все четыре**.

Чего не хватает, чтобы подключать реальный аудит:

* **ExecutionBackend и контракт с PipelineManager** — их нет вовсе, и это
  главный оставшийся шаг;
* **сборка настоящего пакета проекта** (`projects_v2`) вместо синтетического;
* **роли операторов** (п. 2 §29) — реальный аудит стоит дороже тестового, и
  «любой вошедший может отменить» перестаёт быть приемлемым;
* **проверка `max_slots > 1`** — реальные аудиты идут по несколько часов, и
  однослотовый воркер бесполезен;
* **лимит частоты регистрации** перед выставлением наружу.

Поэтому вердикт этапа — **HARDENING PARTIAL**: тестовый контур работает,
реальный аудит подключать нельзя.

---

## 31. План отката

1. **Выключить флаг:** `DISTRIBUTED_WORKERS_ENABLED=false`, перезапустить
   backend. Из 384 маршрутов подсистеме остаются два: `/api/workers/status` и
   HTML-страница. База не создаётся, фоновых задач нет.
2. **Остановить воркер:** `systemctl stop audit-worker-agent
   audit-worker-executor`. Процессы аудита при этом **не** останавливаются —
   это осознанно; чтобы остановить их, нужна операторская отмена либо ручное
   вмешательство на VPS.
3. **Восстановить БД:** остановить backend, заменить `workers.db` файлом
   `workers.db.before_v2_to_v3`, удалить `-wal`/`-shm`, запустить backend.
   Схема вернётся к версии 2, попытки — к модели этапа 0.
4. **Результаты сохраняются:** каталоги `validated_results/`,
   `superseded_results/`, `rejected_results/`, `source_packages/` при откате не
   трогаются. `superseded_results` появился на этом этапе — старый код его
   просто не читает.
5. **PipelineManager не менялся** — откатывать нечего.
6. **Удаление роутеров** (`audit_worker_agent`, `audit_workers_admin` из
   `main.py`) на локальный аудит не влияет: точек врезки нет.
7. **Старые попытки не теряются** ни при откате БД (они в копии), ни при
   удалении роутеров (данные на диске).
8. **Случайное удаление центральных данных невозможно:** RetentionManager
   живёт на воркере, доступа к хранилищу центра у него нет, а операторская
   команда удаления адресована только локальной копии.

Проверка отката без риска: скопировать `workers.db` и прогнать
`schema.current_version` на копии — она покажет 2 до миграции и 3 после.

---

## 32. Точные точки подключения ExecutionBackend

Куда именно врезаться на следующем этапе (сейчас этих врезок нет):

| Точка | Файл | Что подставить |
|---|---|---|
| Тип задания | `models/distributed_workers.py::JobType` | добавить `real_audit_v1` рядом с `test_pipeline_v1` |
| Сборка исходного пакета | `job_service.build_source_package` | заменить синтетические `job.json` + `README.txt` на дерево `projects_v2/<version>` |
| Обязательные артефакты | `job_service.TEST_JOB_REQUIRED_ARTIFACTS` | список файлов `_output/` реального аудита |
| Запуск работы | `executor.run_attempt` → `test_runner.run_test_job` | вызов ExecutionBackend вместо `test_process`; argv по-прежнему строит воркер |
| Валидация параметров | `test_runner.validate_params` | схема параметров реального аудита с теми же тремя рубежами зажима |
| Приём результата | `job_service.finalize_result` | распаковка `_output/` в версию проекта |
| Точка вызова из платформы | **не создана намеренно** | `PipelineManager` о подсистеме не знает; врезка появится отдельным решением |

Всё остальное — очередь, попытки, команды, отмена, retention, журнал — от типа
задания не зависит и переиспользуется как есть.

---

## 33. Адверсариальная проверка этапа 3.5 и что она нашла

Четыре независимые проверки читали готовый код против §3 этого документа:
безопасность; состояния и попытки; agent/executor; регрессия и документация.
Проверяющие ничего не правили — только предъявляли путь, строку и сценарий.
Ниже — то, что подтвердилось по коду и закрыто. Тесты — в
`tests/test_distributed_workers_review_fixes.py` (27 шт.).

### 33.1 Принадлежность процесса (I-17) — три дыры в главном инварианте

| Что было | Чем это плохо | Как закрыто |
|---|---|---|
| `is_alive(pid, None)` возвращал `True` | Незаписанная метка старта = «считаем живым», то есть разрешение слать сигнал по одному pid. Ровно то, что I-17 запрещает буквально | Неизвестность = «не доказано», `False`. Отдельная `pid_exists()` для диагностики |
| `verify_ownership` пропускал проверку отпечатка при `expected_fingerprint=None` | Четвёртое из четырёх «совпадений подряд» молча не выполнялось | Пустой отпечаток → отказ |
| «Второй независимый источник» сверял `local_db` с `metadata.json` | Оба поля пишет ОДИН вызов из ОДНОЙ переменной. Совпадение гарантировано и не значит ничего | `live_command_fingerprint(pid)` читает `/proc/<pid>/cmdline` — спрашивает ядро, а не нашу же запись |

### 33.2 Центр: адресация попыткой вместо задания

* **`/jobs/next` с повтором `Idempotency-Key` перевыпускал токен ЧУЖОЙ
  попытки.** Запись шла `update_job_fields(cached["job_id"], …)` — то есть в
  «текущую попытку задания». Вернувшийся воркер, чью попытку признали
  потерянной, повтором старого ключа окирпичивал НОВУЮ: её законный
  исполнитель получал 409 на всех ручках и не мог сдать готовый результат.
  Теперь: попытка из кэша, проверка «активна + закреплена за этим воркером +
  не терминальна», иначе `409 idempotency_key_stale`.
* **`accept` и `reject` авторизовались по попытке, а писали по заданию.** Окно
  между двумя `await` позволяло старому воркеру перевести в `failed` чужую
  активную попытку. Теперь `transition(attempt_id=…)`.
* **Признанная потерянной попытка выдавалась воркеру ПОВТОРНО.**
  `claim_next_job_for_worker` и `reoffer_unknown_jobs` фильтровали только по
  состоянию исполнения, а `mark_lost` его не меняет. Добавлен фильтр
  `attempt_disposition = 'active'` в обоих местах.

### 33.3 Центр: доставка результата

* **Готовый результат было не сдать, если доехала отмена.** Словарь путей
  `catch_up_to_result_received` не знал `cancel_requested` — ребро
  `cancel_requested → completed_locally` в таблице было заведено именно под эту
  гонку, но догон им не пользовался: 409 по кругу. Добавлен путь.
* **Результат отозванной попытки мог опубликоваться как актуальный.** Между
  «проверили, что попытка активна» и записью проходят минуты распаковки и
  валидации. Теперь `finalize_result` перечитывает попытку непосредственно
  перед записью и при отзыве бросает `AttemptNoLongerActive`; роутер кладёт
  архив на хранение вместо публикации.
* **Мигрированные попытки этапа 0 не могли сдать результат вообще.** Ключи вида
  `att_legacy1` проходили на путях ЧТЕНИЯ и падали на путях ЗАПИСИ —
  `UnsafeIdentifier` это `ValueError`, его не ловил `except JobError`, наружу
  шёл HTTP 500, а сессия навсегда залипала в `assembling`. `allow_legacy=True`
  добавлен и на записи.
* **Сессия сборки залипала навсегда.** Смерть процесса-сборщика не откатить
  никаким `try/finally`. Введена аренда `assembly_started_at` (30 минут):
  по истечении сессию разрешено занять заново.
* **`store_unpublished_result` выдавал `retention_until` без проверки
  содержимого** — воркеру разрешалось удалить локальную копию пакета, который
  центр не открывал. Теперь содержимое проверяется, отчёт кладётся рядом.

### 33.4 Центр: операторские действия

* **`POST /resources` минует санитайзер.** `{"executor": "PWNED"}` от
  одобренного воркера ронял операторский экран 500-й НАВСЕГДА и не для одного
  воркера, а для всего списка. Теперь тот же `sanitize_resource_snapshot`, что
  и в heartbeat.
* **Идемпотентность не была привязана к адресу.** Тот же ключ на другой попытке
  возвращал результат первой и не выполнял ничего — с успешным HTTP 200.
  Теперь поиск идёт по (тип, ключ, job, attempt).
* **Эффект ACK не применялся на повторе.** Падение центра между записью ACK и
  применением эффекта оставляло попытку в `cancel_requested` навсегда: воркер
  отмену подтвердил, повтор возвращал `replayed=True`, эффекта не наступало.
  Гард `if not replayed` снят — обе функции идемпотентны по состоянию.
* **Фиксированный ключ команды удаления.** Повтор возвращал старую строку
  (в том числе протухшую) с ответом «команда поставлена в очередь». Теперь
  счётчик, как у отмены.
* **`approve`/`reject`/`revoke`/`POST /jobs` были без гейта намерения**, а
  `rotate-token` — с ним. Введён `_require_intent_header` (только CSRF-рубеж,
  без ключа: повтор этих действий второго эффекта не создаёт).
* **Прямое операторское `→ cancelled` из `assigned`/`source_*` убрано.** Пакет
  уже у воркера; «отменено» без его подтверждения — то самое враньё, которое
  запрещает критерий готовности 6.

### 33.5 Воркер

* **Дубли `seq` между процессами.** С этапа 3.5 в один каталог событий пишут
  ДВА процесса — исполнитель и агент. Потокового лока для этого мало: у
  каждого свой `last_written_seq` в памяти. Добавлен межпроцессный `flock` с
  перечитыванием курсора под замком.
* **Курсор чинился только в одну сторону.** «Курсор позади файлов» (потерян
  `cursor.json`, сегменты целы) приводил к переиспользованию занятых номеров.
* **`pending_batch` не видел уплотнённые сегменты**, и запрошенный после 409
  номер не находился нигде.
* **Команда, застрявшая в `processing`,** не возвращалась в очередь никогда и
  никогда не подтверждалась центру → `requeue_orphan_commands` при старте.
* **Восстановление забирало попытки чужого ЖИВОГО исполнителя** — второй
  наблюдатель за тем же процессом и второй сборщик того же архива. Добавлена
  проверка `executor_alive` (pid + тик старта).
* **Захваченная, но не начатая попытка** объявлялась `executor_interrupted`;
  теперь возвращается в очередь — работа не начиналась, терять нечего.
* **`already_packaged` не эмитил `job_completed_locally`** — собранный архив
  оставался центру неизвестен.
* **Наблюдатель затирал исход отмены** значением `executor_interrupted`.
* **`grace_period_sec: 1e9` вешал главный цикл**, нечисловое значение — роняло
  исполнителя целиком. Введён кламп `_grace_period` и `try/except` вокруг
  разбора команды и вокруг оборота цикла.
* **`_safe_remove` разрешал удаление самого корня `jobs/`** — `target == root`
  стояло в разрешающей части условия.
* **`metadata.json` с execution-токеном имел права по umask** (обычно 0644) —
  теперь 0600, как у остальных файлов с секретами.
* **`LocalJobStore.job_dir` склеивал путь напрямую**, минуя `paths.attempt_dir`.
* **Досылка результата безусловно ставила `finished`** даже без подтверждения
  центра — задание исчезало и из досылки, и из сверки.
* **`--with-executor` под systemd плодил исполнителей** при каждом рестарте
  агента. Добавлена проверка живого исполнителя и явное предупреждение.

### 33.6 Что проверки признали чистым

XSS на операторском экране (сборка DOM только через `textContent`); закрытый
enum `WorkerCommandType` без `run_shell`/`argv`; построение путей через
`identifiers`/`paths` (кроме одного места, исправленного); распаковка tar с
обеих сторон; хранение секретов только хэшами и их вычистка из логов, событий
и архивов; невмешательство в `backend/app/pipeline/**` (диффа нет); fail-closed
монтирование операторского роутера при `PORTAL_AUTH_ENABLED=false`; запрет
записи в представление `remote_jobs`; план отката §31 — выполним буквально.

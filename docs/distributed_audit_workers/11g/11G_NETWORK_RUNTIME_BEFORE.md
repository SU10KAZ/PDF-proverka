# 11G. Сетевой рантайм ДО правок: как цепочка выглядела и где она рвалась

Базовый коммит: **09afc6b9** («fix(воркеры 11F): потолок кропа под предел провайдера + сборщик доказательств»).
Машиночитаемая версия того же содержания — `11G_NETWORK_RUNTIME_BEFORE.json`.

---

## 1. Что это за документ и почему он про состояние ДО правок

11G — первый этап, который обязан провести **штатное** распределённое задание по всей цепочке:
центр создаёт задание → воркер сам его забирает по HTTPS → выполняет участок конвейера на настоящих
моделях → возвращает пакет чанками → центр принимает, подтверждает и достраивает нормативный хвост.
Всё предыдущее (11C…11F) доказывало **куски** этого пути, и почти каждый кусок держался на операторе.

Чтобы правки 11G можно было оценивать, а не принимать на веру, зафиксировано состояние ровно на
09afc6b9 — до первого коммита этапа. Это важно физически: первый коммит 11G (`2aef0564`) уже изменил
десять файлов, включая `ProviderRequirementPayload`, `RemoteWorkerExecutionBackend.prepare`,
`inference_grant.py` и `audit_worker/config.py`. Поэтому все ссылки на эти файлы ниже сняты через
`git show 09afc6b9:<файл>`, а не из рабочего дерева; остальные файлы на базе и в дереве совпадают.

Документ отвечает на один вопрос: **что именно мешало обычному распределённому заданию доехать от
кнопки «запустить на воркере» до `norm_verify` на центре**. Не «что хотелось бы улучшить», а где цепь
физически разомкнута — с указанием файла и строки, где требование теряется, где падает валидация,
где обязан вмешаться человек.

Терминология: **центр** — 176.12.77.128 (`adm.hlab.kz`), **воркер** — 176.12.77.31
(`andrewuzun3.hlab.kz`). «Мост» — `pipeline_bridge`, канал «конвейер → ProviderAdapter → CLI модели».
«Привязка» — `metadata/provider_binding.json` попытки. «Разрешение» — файл
`<worker_root>/config/allow_synthetic_inference`.

---

## 2. Цепочка по шагам

Девятнадцать звеньев в порядке прохождения. Столбец «состояние» — что переживает рестарт.

| # | шаг | функция | файл:строка | вход | выход | состояние | endpoint | идемпотентность |
|---|---|---|---|---|---|---|---|---|
| 1 | Очередь берёт элемент | `_batch_slot_worker` → `_execute_item` | `backend/app/pipeline/manager.py:6147-6151` → `:6288-6336` | `BatchQueueItem` + `AuditJob` | `backend.run(request, ctx)` либо локальный `_dispatch_action` | `batch_queue.json` (`status`, `execution_mode`, `worker_id`, `execution_handle`) | — | дедуп при постановке: `manager.py:6934-6949` (project+version) и `:6951-6974` (любой remote, включая `interrupted`) → 409 |
| 2 | Выбор backend | `item_execution_mode` → `select_backend` → `build_request` | `backend/app/pipeline/execution/registry.py:78-102`, `:105-126`, `:129-154` | элемент очереди | `RemoteWorkerExecutionBackend` + `ExecutionRequest` | нет; флаги перечитываются каждый вызов (`:36-38`) | — | чистые функции; `REMOTE_WORKER` без `worker_id` → тихо LOCAL (`:90-93`); LOCAL с разбираемым handle → REMOTE, чтобы отказать громко (`:94-101`) |
| 3 | Создание удалённого задания | `RemoteWorkerExecutionBackend.prepare` | `backend/app/pipeline/execution/remote.py:135-196` (вызов — `:171-183`) | `ExecutionRequest` + `ctx.extra['actor']` | `ExecutionHandle` | `item.execution_handle` → `_persist_queue()` (`:198-211`, fail-soft) | — | `:152-160` — живой и не отозванный handle возвращается как есть, второго задания нет (E-04, E-05) |
| 4 | Задание на центре | `audit_job_service.create_audit_job` | `backend/app/services/distributed_workers/audit_job_service.py:340-503` (пакет — `:506-614`) | worker_id, project_id, version_dir, action, actor, settings | строка `remote_jobs` + токен + манифест | `logical_jobs.payload`, `job_attempts`, `job_state_transitions`, архив в `source_packages/<job>/<attempt>/` | — | `schema.py:261-263` `ux_logical_jobs_active_project` → `ActiveJobExists` (I-18); `schema.py:311-312` одна активная попытка |
| 5 | Контракт требования к провайдеру | `ProviderRequirementPayload` / `AuditPipelineParams.provider_requirement` | `backend/app/models/distributed_workers.py:442-471`, `:524` | dict от `create_audit_job` (`audit_job_service.py:441`) | валидированная модель → `model_dump()` | вложенный ключ в `logical_jobs.payload` (`schema.py:252`); отдельной колонки нет | — | н/п (модель данных) |
| 6 | Реестр воркеров | `register_worker` / `claim_token` / `record_heartbeat` / `compatibility_report` | `registration_service.py:32-122`, `:125-178`; `worker_registry.py:39-101`, `:314-333`; `audit_job_service.py:99-207` | bootstrap-секрет → claim-secret → одобрение → `wtk_` | `worker_id`, токен, отчёт совместимости | `workers`, `worker_tokens`, `resource_snapshots`, `worker_provider_states` | `POST /register`, `POST /claim`, `POST /heartbeat`, `POST /api/workers/{id}/approve` | повтор регистрации не перевыпускает claim-secret (`:73-87`); claim одноразовый, проверка+сжигание в одной транзакции (`:139-177`) |
| 7 | Опрос и захват | `JobPullClient.poll` → `jobs_next` → `claim_next_job_for_worker` | `audit_worker/job_poller.py:22-73`; `audit_worker_agent.py:493-703`; `repositories.py:591-736` | `{free_slots, accepts, wait_sec}` + Bearer + `X-Worker-Id` | `JobAssignment` / 204 / 409 | `job_attempts.execution_state` assigned→source_uploading, хэш execution-токена, `idempotency_keys` | `POST /api/v1/worker/jobs/next` (long-poll 25 с) | ключ = свежий uuid4, переиспользуется только после транспортного сбоя (`job_poller.py:60,68-71`); кэш ответа с перевыпуском токена (`:517-577`); CAS в `BEGIN IMMEDIATE` (`repositories.py:715-727`) (I-05) |
| 8 | Скачивание пакета | `download_source` → `verify_and_unpack` → `_download_and_verify` | `client.py:201-229`; `package_io.py:203-361`; `agent.py:855-907` | URL + `X-Execution-Token` + ожидаемый sha256 | секции `AUDIT_PACKAGE_SECTIONS` в каталоге попытки | `jobs/<job>/<attempt>/{source_package,project,snapshot,runtime,discipline_profile,metadata}` | `GET /api/v1/worker/jobs/{job_id}/source` (Range, ETag) | `.part` + `Range`, дописывание только на 206 (`client.py:201-229`); распаковка в `.staging-<pid>-<ms>` → `os.replace` (`package_io.py:250,358-360`) |
| 9 | Исполнитель | `_tick` → `local_db.claim_next` → `run_attempt` → `run_audit_attempt` | `audit_worker/executor.py:280-314`, `:485-501`, `:900+`, `:682-707` | строка `execution_queue` | запуск конвейера, маркер, пакет | `worker.db` (WAL): `execution_queue`, `process_registry`, `executor_instances` | — | ёмкость считается внутри транзакции (`local_db.py:366-417`); стартовый CAS `expect_states=(CLAIMED,)` (`executor.py:939-947`) |
| 10 | Попытка | `claim_next`/`set_queue_state`/`adopt_claim`; `job_service.transition` | `local_db.py:366-417,419-440,450-498`; `job_service.py:230-326` | `job_id`+`attempt_id` (только UUID, `audit_worker/paths.py:45-52`) | переходы состояний + аудит-строка | `job_attempts` (+`central_handoff_*`, `schema.py:600-604`); `execution_queue` | — | уход из терминального состояния запрещён на уровне SQL (`local_db.py:450-498`); на центре — `ALLOWED_TRANSITIONS` и роль актора (I-02, I-03) |
| 11 | Привязка провайдера | `Executor.prepare_provider_binding` | `audit_worker/executor.py:782-898` (вызов `:921`); форма — `audit_runner.py:210-272`; разбор — `resolver.py:84-159`; резолв — `resolver.py:358-438` | `params.provider_requirement` | `metadata/provider_binding.json` (0600) либо `None` | файл привязки в каталоге попытки | — | единица разрешения тратится за попытку, а не за успех (`:883-889`) |
| 12 | Разрешение (grant) | `inference_grant.consume` | `audit_worker/providers/inference_grant.py:293-351`; списание — `executor.py:852-855` | provider + `task_id=job_id` | `GrantRecord`, `grant_id` в привязку | JSON разрешений, атомарная перезапись под `fcntl` (`:219-243`) | — | атомарное уменьшение; exactly-once самого ВЫЗОВА даёт журнал, а не grant (I-P9, `pipeline_bridge.py:221-340`) |
| 13 | Дочерний процесс конвейера | `audit_runner.run_audit_job` → `remote_audit_runner` | `audit_runner.py:538-695`, argv `:419-424`, env `:427-487`, спека `:555-588`; `remote_audit_runner.py:440-479`, `:29`, `:876-882` | `metadata/run_spec.json` | артефакты этапов, JSONL-события, `work/process_exit.json` | `03_analysis/*`, `work/pipeline_log.json`, `usage/usage_report.json` | — | два независимых маркера: `completed.marker` (`executor.py:95-119`) и `process_exit.json` (`:122-146`) |
| 14 | Журнал событий | `EventOutbox` → `_flush_outbox_locked` → `ingest_batch` | `event_outbox.py:189-259`, `:425-469`, `:471-481`; `agent.py:1250-1295`; `audit_worker_agent.py:824-878` | события двух процессов | `EventBatchResponse` с курсором | сегменты `outbox-%04d.jsonl`, раздельные `cursor.json`/`ack.json` (`:71-75`) | `POST /api/v1/worker/events` | `seq` выдаёт SQLite (`local_db.py:733-771`); на центре курсор + `UNIQUE(job,attempt,sequence)` (I-04); 409 `sequence_gap` → `rewind_to` |
| 15 | Загрузка результата | `_package` → `upload_result` → `_upload_archive` | `executor.py:1119-1234`; `uploader.py:25-102`, `:105-127`; `package_io.py:422-677`; `agent.py:1039-1114` | архив + sha256 + манифест | `UploadCompleteResponse` c `retention_until` | `uploads/<upload_id>/state.json` (пишется ДО отправки); `upload_sessions`, `upload_chunks` | `POST /uploads` → `PUT /uploads/{id}/chunks/{i}` → `POST /uploads/{id}/complete` | сессия по `(job, attempt, expected_hash)` (`upload_service.py:65-69`); тот же чанк — `replayed`, другой — 409; повтор `complete` при `verified` — тот же ответ (I-06) |
| 16 | Приём и валидация | `upload_service.assemble` → `validate_result_package` | `upload_service.py:152-203`; `package_service.py:355-460` | все чанки `range(total)` + заявленные хэш/размер | `result_staging/<job>/<attempt>/result.tar.gz` | `result_staging/`, при отказе `rejected_results/` | часть `POST /uploads/{id}/complete` | лизинг сборки `claim_upload_for_assembly` (`repositories.py:835-869`) |
| 17 | ACK | `job_service.finalize_result` / `store_unpublished_result` | `job_service.py:772-886` (`retention_until` — только `:874-877`), `:674-768` | архив + заявленные хэш/размер | `COMPLETED`, `validated_at`, `retention_until = now+30д` | `job_attempts.retention_until / retention_state / result_package_hash` | отдельного маршрута НЕТ; три канала: ответ `complete`, `heartbeat.retention_updates` (`:432-445`), `reconcile` (`:1456-1479`) | повтор отдаёт тот же `retention_until`; на воркере приём = непустой `retention_until` (`agent.py:1090-1106`) |
| 18 | Импортёр | `wait` → `collect_result` → `import_result_for_attempt` | `remote.py:328-376`, `:456-498`; `result_import.py:89-168`, `:240-278`, `:326-478`, `:481-531` | попытка в `completed` + архив | `ExecutionResult(resume_stage, resume_hint, usage_report, …)` | файлы в `version_dir`; `apply_journal.json` + `backup/`; `result_import_state` | — | `applied` + тот же хэш → `replayed`; другой хэш → `ResultImportConflict`; любое `rejected` обрывает импорт целиком (I-07) |
| 19 | Центральный хвост | `_run_central_tail_after_remote` → `_detect_central_resume_stage` | `backend/app/pipeline/manager.py:6338-6470` | `ExecutionResult` + `ExecutionHandle` | `norm_verify → debt_control → decision_carryover → excel` | артефакты центральных этапов; ось `central_handoff` | — | `central_handoff_state == 'completed'` закрывает элемент без второго прохода (`:6358-6371`) — импорт идемпотентен, норм-этап и Excel нет |

Три опоры цепочки, которые полезно держать в голове при чтении таблицы:

- **Воркер только исходящий.** Ни одного входящего сокета в пакете `audit_worker` нет; единственный
  сетевой импорт — `httpx`. Всё, что видно в столбце endpoint, инициирует воркер (C-01, C-05).
- **Задание не несёт исполняемого.** В `ExecutionRequest` и `AuditPipelineParams` нет ни argv, ни путей,
  ни переменных окружения, ни промпта (`contracts.py:3-14`, `models/distributed_workers.py:443-450`).
  Окружение дочернего процесса строится с нуля белым списком (`audit_runner.py:427-487`).
- **Две оси состояния.** `JobState` доходит до `completed` в момент приёмки архива; всё, что происходит
  после, живёт на отдельной оси `central_handoff` (`central_handoff.py:1-19`, `:32-58`). Без второй оси
  рестарт центра между приёмом и норм-этапом был бы невидим.

---

## 3. Где цепочка рвалась

Двенадцать разрывов. Первые четыре и седьмой — блокеры: без них штатное задание не проходит вообще.

### G-01. Центр не формулировал требование к провайдеру

`backend/app/pipeline/execution/remote.py:171-183` — список аргументов `create_audit_job` заканчивается
`settings=settings`. Параметра `provider_requirement` там нет, значит берётся значение по умолчанию
`None` (`audit_job_service.py:349`). Дальше по прямой: `wants_inference = False`
(`audit_job_service.py:406-408`) → `build_runtime_snapshot(provider_mode='fake')` (`:414`).

Каждое боевое задание уезжало на воркер со словами «модель не нужна». На воркере это выражается одной
строкой: `executor.py:801-803` возвращает `None` до всех проверок, привязка не пишется,
`AUDIT_WORKER_PROVIDER_BINDING` не выставляется, `pipeline_bridge.active()`
(`pipeline_bridge.py:98-125`) отдаёт `False` — и `claude_runner` уходит на прямой `claude -p`, то есть
на транспорт **до** 11C. Весь результат 11C/11D/11E/11F на штатном пути был недостижим.

### G-02. Контракт центра не знает слова `capability`

`backend/app/models/distributed_workers.py:442-471`: поля `capability` нет, при `extra="forbid"` (`:452`).
Отказ №1 — на центре: `ProviderRequirementPayload(provider='claude', capability='strong_audit')` даёт
`ValidationError: Extra inputs are not permitted`. Отказ №2, независимый — при выдаче задания:
`audit_worker_agent.py:706-716` заново строит `AuditPipelineParams(**raw)` из сохранённой нагрузки,
так что впрыск поля прямо в JSON уронил бы `/jobs/next` пятисоткой.

Обойти это «точной моделью» нельзя: `resolver.py:128-140` отвергает любой непустой
`provider_requirement.model` безусловно — «центру идентификатор модели не принадлежит». А послать
требование **без** способности хуже, чем не послать вовсе: `resolver.py:406-417` оставляет
`binding.model = None`, и `pipeline_bridge._preflight` (`pipeline_bridge.py:387-410`) отказывает
каждому рабочему вызову — «в привязке нет назначенной модели». Оба конца тупиковые.

Асимметрия, которую стоит зафиксировать: точные идентификаторы моделей **уже** ездят с центра на
воркер — через `runtime_config.stage_model_mapping` и байты `snapshot/stage_models.json`
(`audit_job_service.py:233-235`, `:306-325`; применяются `remote_audit_runner.py:366-399`). Правило
«центр не называет модель» верно только для ноги ProviderAdapter, где `--model` берётся из привязки
(I-P5), а не для конфигурации этапов.

### G-03. Ни одной операторской поверхности для требования

`RemoteAuditLaunchRequest` (`models/distributed_workers.py:877-889`, `extra="forbid"`) знает четыре
поля: `worker_id`, `project_id`, `version_id`, `action`. `AuditExecutionOptions`
(`contracts.py:81-95`, `extra="forbid"`) — пять, все про этапы. `build_request`
(`registry.py:129-154`) провайдерского не заполняет ничего. Грепы по
`audit_workers_admin.py` на `provider_requirement|capability|max_inferences` дают ноль.

Единственный производитель требования во всём дереве — `scripts/smoke_distributed_audit_pipeline_provider_e2e.py:905-919`,
и он **явно** обходит HTTP, вызывая `create_audit_job` в подпроцессе: докстринг `:890-897` объясняет,
что продовый операторский маршрут не знает синтетического действия `provider_selfcheck` и расширять
продовую поверхность ради проверки канала этап 11C не должен. Само действие при этом принимается и
моделью параметров (`models:490`), и сервисом (`audit_job_service.py:417`) — но не маршрутом.

### G-04. Разрешение мог выписать только человек

`inference_grant.issue` на базе снабжена докстрингом «ТОЛЬКО для оператора и тестов, не для воркера»
(`inference_grant.py:365-369`), функции `issue_for_job` не существует (grep = 0), а структурный тест
`tests/test_distributed_workers_pipeline_provider.py:965-979` грепает `executor.py`, `agent.py`,
`pipeline_bridge.py` и `manager.py` на строку `inference_grant.issue`. Воркер вправе только `consume`
(`executor.py:852-855`).

Значит задание с `max_inferences > 0`, приехавшее по сети, встаёт в `_GrantPending`
(`executor.py:856-864`) и по истечении `pipeline_provider_grant_wait_sec` (по умолчанию 0.0)
отклоняется (`:865-867`). Опереть автовыдачу было не на что: потолка машины
(`AUDIT_WORKER_PIPELINE_PROVIDER_MAX_INFERENCES`) в `audit_worker/config.py` не было.

### G-05. Воркер не объявлял свои способности

В `heartbeat_payload` (`audit_worker/providers/manager.py:260-346`) ключа `provider_capabilities` нет.
Центр видит `provider_mode`, `real_inference_allowed`, `pipeline_bridge_enabled` — но не знает,
покрывает ли локальная политика машины (`model_policy.py:80-81`, `KNOWN_CAPABILITIES = ("strong_audit",)`)
требуемую способность. Проверить это до создания задания нечем, и расхождение всплывало на воркере
**после** списания разрешения: `consume` на `executor.py:852-855`, `resolve` на `:872-889`, возврат
явно не делается (`:883-889`).

### G-06. Повторная попытка audit-задания невозможна

`attempt_service.create_attempt` (`attempt_service.py:570`) берёт параметры через
`job_service.job_params` (`job_service.py:343-350`), а тот безусловно строит
`TestJobParams(**payload["params"])` при `extra="forbid"` (`models:432`) → `ValidationError` на любом
`audit_pipeline_v1`. Даже если бы разбор прошёл, следом собрался бы **тестовый** пакет
(`job_service.build_source_package`, `job_service.py:432-437`). Операторское действие «создать новую
попытку» к аудиту неприменимо, а сохранённое `provider_requirement` при пересоздании попытки не
перечитывается никогда.

### G-07. Ingress не включён

`DISTRIBUTED_WORKERS_ENABLED` по умолчанию `False` (`config.py:1260`) и на боевом центре не задан;
роутер `/api/v1/worker/*` регистрируется только под ним (`main.py:254`). Поэтому любой запрос воркера
даёт **404, а не 403** — маршрута нет в приложении. `DISTRIBUTED_AUDIT_EXECUTION_ENABLED` тоже `False`
(`config.py:1309`), и `select_backend` (`registry.py:113-122`) при отказе бросает `ExecutionError`,
не откатываясь в локальный режим.

Постоянного HTTPS-входа нет с этапа 10: прямой порт режет провайдер (проходят 22/80/443/3389),
`location` в nginx требует sudo (его нет ни на одной машине), SSH как транспорт запрещён условием
(C-01/C-02), TLS отключить нельзя (`audit_worker/config.py:464-467`). Фактически использовался
quick-туннель cloudflared, у которого URL меняется при каждом перезапуске. Статус —
`PERSISTENT_CENTRAL_INGRESS = not_ready` (`11_provider_auth_quota_gate.md:1055-1064`), план включения
`11c/11C_CENTER_INGRESS_CUTOVER.md` — НЕ ПРИМЕНЁН.

### G-08. Пустая `AUDIT_PIPELINE_REVISION` запирает аудит дважды

`config.py:1318` по умолчанию пустая строка. Отсюда причина `center_revision_missing` в отчёте
совместимости (`audit_job_service.py:113-127`) → 409 `worker_incompatible` на запуске
(`audit_workers_admin.py:416-426`); и независимо — `AuditPipelineParams.pipeline_revision` с
`min_length=1` (`models:497`). Ревизия должна совпасть с `AUDIT_WORKER_PIPELINE_REVISION` воркера
(`audit_runner.py:297-308`), причём подгонять центр под воркера прямо запрещено (§13 плана ingress).

### G-09. В пакет уезжает плейсхолдер вместо хэша дерева

`audit_job_service.py:433` ставит `"sha256:" + "0"*64`; этим же объектом собирается пакет (`:462-473`),
и `snapshot/job.json` пишет `params.model_dump()` (`:526-543`). Настоящий хэш подставляется только
после сборки (`:476-483`) и переписывает `logical_jobs.payload`. Итог: копия параметров внутри пакета
и копия в БД навсегда расходятся по `expected_source_tree_hash`.

### G-10. Опции удалённого запуска молча теряются

Сверка `remote.py:171-183` с `contracts.py:98-112`: на удалённом пути выброшены `object_id`,
`execution_profile`, `pipeline_revision`, `correlation_id` и `options.start_from`. «Продолжить с этапа N»
удалённо выразить нечем. Часть потерь осмысленна (сервис жёстко ставит профиль, свою ревизию и
`include_norms=False`), но происходит это молча. Плюс `build_request` никогда не заполняет
`include_optimization`/`include_norms` — они всегда остаются значениями по умолчанию.

### G-11. Сетевые звенья не имеют ни одного боевого прогона

Во всём `main()` харнеса 11F (`scripts/run_11f_worker_slice.py:669-811`) нет ни одного сетевого шага:
ни `EventOutbox`, ни `POST /events`, ни чанковой загрузки, ни ACK, ни `retention_until`. Пакет
исходников ехал `scp` (`11f/11F_DEPLOYMENT_REPORT.md:57-61`), пакет результата — тоже `scp`
(`11f/11F_RESULT_PACKAGE.json:31-35`), приём делал локальный `scripts/verify_11f_handoff_import.py`.
Шаги 7, 8, 14, 15, 17, 18 на реальном `audit_pipeline_v1` не исполнялись ни разу; доказаны они только
смоуками с поддельными CLI (09 и 11C) и тестовыми заданиями. Сам 11F это и признаёт
(`11f/11F_CHECKLIST.md:49-55`, OQ-1).

### G-12. Исполнитель и агент в боевом прогоне не участвовали

Харнес заменил обе роли: раскладку попытки (`:285-361`), спеку (`:438-493` плюс патч `:758-766`),
запуск (`:496-544`), сборку пакета результата (`:600-664`). systemd-юниты не ставились и не
перезапускались (`11f/11F_DEPLOYMENT_REPORT.md:59-61`), прогон шёл в отдельном корне
`~/audit-worker-11f` со своей `provider_policy.json`. Следовательно слот-контроль
(`executor.py:682-707`), стартовый CAS (`:939-947`), маркеры завершения (`:1011-1022`, `:122-146`),
правило «нулевой код при отсутствующем артефакте = FAILED» (`:1040-1067`) и восстановление после
рестарта (`:362-446`) реальным аудитом не проверялись.

**Что при этом работало и не требует пересмотра.** Само ядро конвейера на воркере доказано: 11F прошёл
`13` ключей `pipeline_log`, 8 реальных вызовов `claude-opus-5`, exit 0 за 1401 с, 31 замечание, и центр
принял пакет локальным импортёром с планом `apply 131 / rejected 0` и пустым списком нарушений границы
(`11f/11F_LOCAL_IMPORT_TEST.json`). Разрывы 11G — это именно **обвязка**: формулировка требования,
выдача разрешения, транспорт и подтверждение.

---

## 4. Что оператору приходилось делать руками на 11D–11F

Двадцать один ручной шаг харнеса 11F (M1…M21 в исходной карте) распадается на восемь классов.
Ниже — классы, а не пересказ таблицы.

| класс | что делал человек/харнес вместо кода | пункты | чем это должно быть |
|---|---|---|---|
| **K1. Пакет исходников собирал не центр** | отдельный скрипт `scripts/build_11f_source_package.py` вместо `audit_job_service.build_audit_source_package` | M3 | шаг 4 цепочки |
| **K2. Транспорта не было ни в одну сторону** | `scp` туда и обратно, ни событий, ни чанков, ни ACK, ни `retention_until`; приём — локальный импортёр; сбор доказательств — отдельный скрипт | M4, M15, M16, M17, M18 | шаги 8, 14, 15, 17, 18 |
| **K3. Роль агента исполнял харнес** | раскладка каталога попытки, распаковка, перенос секций `os.replace`, сохранение `metadata/source_manifest.json` (`run_11f_worker_slice.py:285-361`) | M5 | `WorkerAgent._download_and_verify` + `audit_runner.prepare_job_dir` |
| **K4. Провайдерский контур выписывался руками** | разрешение выписывал сам прогон (`:391-400`), `ProviderBinding` конструировался напрямую в обход `ProviderResolver` (`:405-422`), `capability` резолвилась локально (`:403-404`), `allow_real_llm` проставлял скрипт (`:468`) | M6, M7, M8, M10 | шаги 11 и 12: порядок «рубеж машины → режим → резолв → списание → запись» |
| **K5. Роль исполнителя тоже исполнял харнес** | `run_spec.json` писался целиком и патчился после записи, окружение собиралось вручную, конвейер запускался `subprocess.run`, пакет результата собирал скрипт | M9, M11, M12, M13, M14 | шаги 9, 13, 15 |
| **K6. Конфигурация задания зашита в харнес** | таблица моделей этапов (`:73-95`) и белый список этапов привязки (`:63-71`) — константами | M1, M2 | `snapshot/stage_models.json` из пакета и `provider_requirement.allowed_stages` от центра |
| **K7. Эксплуатация мимо штатной установки** | systemd-юниты не ставились; отдельный корень воркера `~/audit-worker-11f` со своей `provider_policy.json` | M19, M21 | продовый корень + `audit-worker-agent.service` / `audit-worker-executor.service` |
| **K8. Остатки лесов** | мёртвый `write_model_snapshot()`, служебное поле `_11f_fake_cli` в спеке | M20, M11 | удаляется вместе с харнесом |

Существенная оговорка: харнес **не переписывал конвейер**. Он звал ту же точку входа
`backend.app.pipeline.remote_audit_runner`, которая зовёт тот же `PipelineManager._dispatch_action`,
что и центр (`run_11f_worker_slice.py:2-9`, `:97`). Поэтому 11F — честное доказательство участка
конвейера и нечестное доказательство обвязки.

Харнесы 11D и 11E (`run_11d_text_analysis_provider.py`, `run_11e_findings_merge_provider.py`) той же
формы, с двумя отличиями в лучшую сторону и одним в худшую: разрешение там выписывалось **отдельным**
операторским запуском `--issue-grant` (в отличие от K4), привязка выписывалась через настоящий
`ProviderResolver`; зато пакета исходников не было вовсе — ни манифеста, ни `runtime_snapshot_hash`,
ни `discipline_profile_hash`, а обязательные входы свода стейджились руками из `--inputs-dir`.

---

## 5. Флаги, которые всё это гейтят

### Центр — три решения, принимаемых независимо

| флаг | файл | по умолчанию | что выключает |
|---|---|---|---|
| `DISTRIBUTED_WORKERS_ENABLED` | `backend/app/core/config.py:1260` | `False` | сразу три вещи: роутер `/api/v1/worker/*` (`main.py:254` — при выключенном флаге **404, не 403**), операторский `/api/workers/*` кроме `status_router`, и создание SQLite с каталогами (`database.ensure_ready → settings.require_enabled`). Двойной рубеж: `require_worker` сам проверяет флаг и отдаёт 404 |
| `DISTRIBUTED_AUDIT_EXECUTION_ENABLED` | `config.py:1309` | `False` | **не ingress**, а право СОЗДАВАТЬ задания `audit_pipeline_v1`. `remote_execution_available` (`registry.py:47-55`) требует оба флага; `select_backend` (`:113-122`) при отказе бросает `ExecutionError` и никогда не падает в локальный режим. Включать одновременно с ingress прямо запрещено (§13 плана) |
| `DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET` | `config.py:1273-1275` | `""` | регистрацию НОВЫХ воркеров; `require_bootstrap_secret` требует ≥16 символов (`settings.py:115-133`). Пустой секрет — способ включить ingress, не открывая приём машин: выданные токены продолжают работать |

Рядом, формально не флаги, но такие же жёсткие рубежи: `AUDIT_PIPELINE_REVISION` (`config.py:1318`,
по умолчанию пустая — см. G-08) и `DISTRIBUTED_WORKERS_ALLOW_INSECURE_ADMIN` (`config.py:1301-1303`),
без которого при выключенной портальной авторизации операторский контур не поднимается (`main.py:265-278`).

### Воркер — рубежи машины, а не задания

Все они принадлежат **администратору VPS**; задание не может ни одного из них ослабить.

| рубеж | файл | по умолчанию | смысл |
|---|---|---|---|
| `AUDIT_WORKER_AUDIT_PIPELINE_ENABLED` | `audit_worker/config.py:396` | `false` | право вообще исполнять `audit_pipeline_v1` (`audit_runner.py:309-312`) |
| `AUDIT_WORKER_ALLOW_REAL_LLM` | `config.py:397` | `false` | настоящие модели; при `false` обязателен каталог подделок с маркером — fail-closed (`executor.py:723-754`) |
| `AUDIT_WORKER_PIPELINE_PROVIDER_ENABLED` | `config.py:450-452` | `false` | рубеж машины для моста; проверяется ПЕРВЫМ (`executor.py:816-821`) |
| `AUDIT_WORKER_PIPELINE_PROVIDER_GRANT_WAIT_SEC` | `config.py:453-455` | `0.0` | ноль = отказ сразу при отсутствии разрешения (`executor.py:856-867`) |
| `AUDIT_WORKER_PROVIDER_BINDING` | `audit_runner.py:59` | не задана | единственный активатор моста; задана и файла нет → `pipeline_bridge.active()` бросает, а не молча уходит на несанкционированный CLI (`pipeline_bridge.py:98-125`). На центре её не бывает никогда |
| `AUDIT_WORKER_PIPELINE_REVISION` | `config.py:385-387` | `None` | обязана совпасть с ревизией задания (`audit_runner.py:297-308`) |
| `AUDIT_WORKER_RETENTION_DELETE_ENABLED` | `config.py:371-373` | `false` | физическое удаление; без него `sweep` только отчитывается (`retention.py:102-127`) |
| `verify_tls` (константа) | `config.py:464-467` | `true` | переменной для отключения нет намеренно; послабление только для localhost под `AUDIT_WORKER_ALLOW_INSECURE_LOCALHOST` (`:468-470`, правила `:484-511`) |
| `<worker_root>/config/allow_synthetic_inference` (файл) | `inference_grant.py:62,115-116` | нет файла | право на оплачиваемые вызовы: владелец = текущий uid, режим ⊆ 0600 (`:123-146`), списание атомарно под `fcntl` (`:293-351`) |

---

## 6. Инварианты, которые нельзя нарушить при закрытии разрывов

Только те, что реально касаются этой цепочки. Ссылки — на место формулировки.

### Базовые (`02_technical_design.md:144-157`; реализация — `03_step0_implementation.md:312-325`)

| код | инвариант | почему он в этом списке |
|---|---|---|
| I-01 | потеря heartbeat ≠ остановка аудита | шаг 6 и 14: heartbeat-поток никогда не валит задание (`heartbeat.py:60-73`); недоступный центр не останавливает работу (`reconciliation.py:149-156`) |
| I-02 | remote-задание нельзя признать зомби по локальным сигналам центра | шаг 10: ребра `running → failed` для роли `center` нет в `ALLOWED_TRANSITIONS` |
| I-03 | центр не переназначает задание из-за недоступности воркера | шаги 3 и 10: `prepare` не создаёт вторую попытку и не двигает задание (`remote.py:11-12`), ребра `running → assigned` нет вовсе (ADR-004) |
| I-04 | повторная отправка события не применяет последствия дважды | шаг 14: `UNIQUE(job_id, attempt_id, sequence)` + `last_seen_seq` в одной транзакции |
| I-05 | одно задание не исполняется на двух воркерах | шаг 7: частичный уникальный индекс + `execution_token` + CAS-захват |
| I-06 | повторная загрузка не создаёт дубликат | шаг 15: сессия по `(job, attempt, expected_hash)` + `result_package_hash` |
| I-07 | пакет не публикуется до полной загрузки, sha256, манифеста и обязательных артефактов | шаги 16 и 18: staging → атомарный `os.replace`, провал → `rejected_results/` |
| I-08 | воркер не удаляет пакет до подтверждения приёма | шаг 17: `retention_until IS NULL` до `validated_at`; `retention.py:185-201` отказывает даже ручной команде |
| I-09 | состояния переживают рестарт центра и воркера, обрыв сети, повтор HTTP | вся цепочка: SQLite WAL, `LocalJobStore`, `EventOutbox`, `seq` не сбрасывается |
| I-11 | воркер выполняет только команды протокола (закрытый enum) | шаг 4 и 13: в задании нет ни argv, ни путей; каталоги строятся из UUID (`identifiers.py:113-125`, `audit_worker/paths.py:45-52`) |
| I-12 | секреты вычищаются ПРИ ЗАПИСИ в outbox, а не перед отправкой | шаг 14: `event_outbox.py:242` → `redaction.redact_mapping` |

### Приём и центральный хвост (`02_technical_design.md:3631-3640`)

| код | инвариант | где касается |
|---|---|---|
| I-16 | после приёма пакета `detect_resume_stage` на центре возвращает `norm_verify`, continuation ставится, `latest` публикуется | шаг 19; источник истины — ЦЕНТРАЛЬНЫЙ детектор, `resume_hint` воркера остаётся подсказкой (`manager.py:6338+`) |
| I-18 | второе назначение на тот же `(project_id, version_id)` отбивается уникальным индексом | шаг 4 |
| I-19 | вызов со старым `X-Execution-Token` → `409 attempt_superseded` | шаги 7, 8, 14, 15 (`_load_job_for_worker`, `audit_worker_agent.py:207-222`) |
| I-21 | до `validated_at` воркер не удаляет ничего; `retention_until` приходит только после валидации | шаг 17 |

Оговорка: у I-14 и I-17 в разных документах разные трактовки (`04_step0_hardening.md:488` и `:873-877`
против `02_technical_design.md`); разбор расхождения — `05_pre_pipeline_gate.md:907-919`. В контексте
этой цепочки I-17 читается как «принадлежность процесса»: `is_alive(pid, None)` обязан давать `False`
при незаписанной метке старта (`process_registry.py:53-67`).

### Провайдерский слой (`11_provider_auth_quota_gate.md:312-320`; I-P8 — `11b:866`; I-P9 — `11c/11C_RUNTIME_PATH.md:211-220`)

| код | инвариант | где касается |
|---|---|---|
| I-P1 | окружение строится С НУЛЯ, а не копированием `os.environ` с чисткой | шаг 13 (`audit_runner.py:427-487`) и провайдерский подпроцесс (`providers/base.py:66-77`) |
| I-P2 | worker-токен, адрес центра, execution-токен и ключи платных API физически не доходят до CLI: их имён нет в белом списке | шаги 11–13 |
| I-P5 | ни один `argv` не приходит извне — только константы модуля | шаг 5: именно поэтому `provider_requirement.model` отвергается (`resolver.py:128-140`), а модель берётся из локальной политики |
| I-P7 | таймаут обязателен, по нему убивается ГРУППА процессов | шаг 13 (`process_control.terminate`, `:107-184`) |
| I-P9 | **inference exactly-once per attempt**: claim через `O_CREAT\|O_EXCL` ДО вызова → модель → сохранение; есть результат — отдаётся сохранённый; есть claim без результата — ОТКАЗ, решение за оператором | шаги 12–15 и особенно 18: повторная доставка результата после вызова модели не должна оплачиваться второй раз |

### Архитектурные ограничения (`02_technical_design.md:129-138`)

`C-01` — воркер сам устанавливает исходящее соединение, HTTPS/443, входящего канала для обработки
заданий нет. `C-02` — SSH только для установки и диагностики, не транспорт. `C-05` — постоянный WS
центр↔воркер не вводится без доказанной необходимости. `C-09` — прямая одновременная запись нескольких
воркеров в глобальные файлы (`decisions_log.json`, `norms_paragraphs.json`, `paid_cost.json`) запрещена.
Любое решение по закрытию G-07 обязано остаться внутри C-01/C-02.

### Модельная политика (11D, не номерована как I-)

Центр присылает **логическую способность**; точную модель назначает `provider_policy.json`
администратора машины (`model_policy.py`, файл 0600). `capability` и `model` взаимоисключимы
(`audit_runner.py:261-265`), `model` от центра отвергается вовсе (`resolver.py:128-140`). Отсутствующая
политика, неизвестная способность и пустая политика — это `ProviderPolicyError`, а не значение по
умолчанию: **никаких дефолтных моделей** (`model_policy.py:188-200`, `:308-329`).

---

### Сноски (расхождения отчётов с исходником — принят исходник)

1. `11c/11C_CENTER_INGRESS_CUTOVER.md:59` и `11c/11C_RUNTIME_PATH.md:31,250` называют маршрут
   `POST /api/v1/worker/jobs/{id}/result`. **Такого маршрута нет.** Загрузка результата — это
   `POST /api/v1/worker/uploads` (`audit_worker_agent.py:881`), `PUT …/chunks/{idx}` (`:970`) и
   `POST …/complete` (`:1019`). `GET /jobs/{job_id}/result` существует только в операторском роутере
   (`audit_workers_admin.py:978`). Для reverse-proxy это не критично (правило пишется на префикс
   `/api/v1/worker/*`), но список маршрутов в плане включения ingress неточен.
2. В картах сегментов указаны строки декораторов маршрутов; определения функций на строку ниже
   (`jobs_next` — `:494` при декораторе `:493`, `download_source` — `:720`, `accept_job` — `:739`).
   В таблицах выше даны диапазоны, включающие и декоратор, и тело.
3. `ExecutionResult` **не объявляет** поле `resume_hint` (`contracts.py:152-164`); оно проходит только
   благодаря `model_config extra="allow"`, а читается через `getattr(result, "resume_hint", None)`
   (`manager.py:6338+`). То есть разделение «подсказка воркера / решение центра» держится на нестрогой
   модели, а не на контракте.

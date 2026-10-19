# План: регистрация, одобрение и раздельные права в AuditManager

**Дата:** 2026-10-19 · **Статус:** согласован с поправками владельца (раздел 0),
реализуется в ветке `feature/portal-access`
**Код проверен на:** `main` @ `268b1eea` (после предварительного аудита на
`c93305db` вышло 18 коммитов, все в сравнении версий; выводы аудита по ним
перепроверены и остаются в силе, номера строк ниже — по `268b1eea`).

Цель — простая модель из двух ролей:

- **admin** делает всё, что можно сделать на портале сейчас;
- **viewer** только смотрит и скачивает готовые результаты разрешённых
  разделов и ничего не запускает и не меняет.

Самостоятельная регистрация создаёт заявку, администратор одобряет её на сайте.

## 0. Поправки владельца (2026-10-19) — имеют приоритет над остальным текстом

1. **Роли — вариант Б из § 10.1.** При миграции admin — только явно
   подтверждённый аккаунт владельца. Все остальные действующие аккаунты —
   viewer с явно назначенными правами по разделам. Флага
   `can_manage_accounts` нет: управление пользователями входит в роль admin.
   Бывшие роли `users.json` и списки субъектов подсистемы исполнителей viewer'у
   ничего не добавляют. Вопрос 10.1 закрыт.
2. **История и авторство.** Прежние решения, авторы, даты, идентификаторы
   сотрудников, сопоставления и график не переподписываются. Бывшие эксперты
   после перехода сохраняют разрешённый просмотр; новые экспертные действия
   выполняет admin. Экспертное действие без связи аккаунта с сотрудником
   **не сохраняется** — ответ `409 account_not_linked_to_staff` (а не пустой
   автор). Автора и время определяет сервер; id аккаунта пишется в журнал
   действий (`account_id`) и в новые записи решений (`expert_account_id`,
   аддитивное поле). Глобальный «текущий сотрудник» и автор из тела запроса
   не используются.
3. **Сессии.** Межзапросного кэша нет: аккаунт и права читаются из SQLite
   один раз на каждый HTTP-запрос (в потоке, не в event loop). WebSocket
   перепроверяет статус, роль, `session_version`, срок и
   `must_change_password` по БД каждые 5 с (это покрывает правки через CLI);
   правки через API закрывают соединения сразу.
   `GET /api/auth/me` — PUBLIC с тремя ответами: аноним → `200
   {authenticated:false}`; действующая сессия → `200 {authenticated:true, …}`;
   отозванная/истёкшая/недействительная cookie → `200 {authenticated:false,
   reason:"session_revoked"|"session_expired"}` с удалением cookie.
   При `must_change_password` доступны только `GET /api/auth/me`,
   `POST /api/auth/password`, `POST /api/auth/logout` и страница
   `/change-password`; разделы, `/admin/*`, `/static` приложения и WebSocket
   закрыты.
4. **Объект.** Единое определение объекта из параметра пути, `X-Object-Id` и
   `?object_id=`: конфликт значений → `400 object_conflict`, неизвестный
   объект → `404 unknown_object`, документа нет в явно выбранном объекте →
   `404` без перехода к глобальному объекту или первому совпадению. Без
   явного объекта неоднозначный шифр → `409 ambiguous_document`.
5. **Read-only и выгрузки.** viewer не меняет исходные данные, результаты,
   решения, очереди и статусы и ничего не запускает. Разрешены только
   перечисленные операции кэша (5.3) и детерминированное формирование файлов
   из сохранённых результатов; разрешение генератора выгрузки не разрешает
   запуск произвольных процессов (единственный подпроцесс — фиксированный
   `generate_excel_report.py` с фиксированными аргументами). В разделах без
   выгрузок («База знаний», «График работ») флажок скачивания отключён с
   пояснением, новые выгрузки не разрабатываются.
6. **Откат** (заменяет 8.6): не восстанавливает отозванные cookie, старые
   пароли, доступ заблокированных и прежние права viewer; старый секрет
   сессий не возвращается; точка отката содержит исправления этапа 0.
   Аварийный режим — только владелец с новым секретом и новым паролем,
   регистрация и вход остальных закрыты.
7. **Измерения** — только на временных данных и заглушках; настоящий прогон
   моделей в рамках задачи не выполняется. Реальные модели, платные API и
   анализ — только по отдельной явной команде владельца.

---

## 1. Текущее состояние (подтверждено по коду и боевой конфигурации)

### 1.1. Вход и сотрудники разделены

| Что | Где | Факт |
|---|---|---|
| Логины и хеши | `backend/app/core/portal_auth.py: _parse_users()`, `get_settings()` | Строка `PORTAL_AUTH_USERS=login:hash,…` из окружения. На проде она лежит в `/etc/auditmanager/backend.phaseb.env` (root, 0600) |
| Сессия | `issue_token()` / `verify_token()` | Подписанная HMAC cookie `{"u": login, "exp": …}`. Серверного хранилища нет. Проверяется только подпись, срок и наличие логина в env |
| Middleware | `PortalAuthMiddleware.dispatch()` | Требует только факта входа. Ставит `request.state.portal_user` |
| Сотрудники | `services/common/user_service.py`, `APP_DATA_DIR/users.json` | Поля id, login, surname, initials, name, role (`expert`/`admin`), created_at. Паролей нет |
| Канонизация | `services/common/employee_identity.py` | Алиасы сотрудников для `decisions_log` (`reviewer_aliases`); `canonicalize_user_record()` не меняет файл |
| Сопоставление | `user_service.get_user_by_login()` | Без учёта регистра, по login или id. В portal_auth логин регистрозависим |

Боевые факты, собранные только счётчиками, без вывода значений:

- `PORTAL_AUTH_ENABLED=true`.
- 8 портальных логинов, все `pbkdf2-sha256` (passlib, 29000 раундов по
  умолчанию), все ASCII в нижнем регистре, коллизий без учёта регистра нет.
- Все 8 сопоставляются с 8 сотрудниками `users.json`: 7 с ролью `expert`,
  1 с ролью `admin`.
- Сотрудник с ролью `admin` — тот же субъект, что единственный
  `DISTRIBUTED_WORKERS_ADMIN_SUBJECTS`. В `…_VIEWER_SUBJECTS` 5 субъектов, в
  `…_OPERATOR_SUBJECTS` ни одного.
- `passlib` установлен в боевом venv (донор релизов), но **отсутствует в
  `requirements.txt` центра**: он есть только в `requirements-worker-pipeline.txt`.

### 1.2. Единой системы прав нет

Все проверки прав в бэкенде, кроме «вошёл в портал»:

| Проверка | Где | На чём основана |
|---|---|---|
| Правка плана работ | `routers/schedule.py: _require_admin()` (:96) | `users.json role == "admin"` |
| Ответы Human Contour | `routers/stage_comparison.py: _authorized_human_contour_author()` (:256), `human_contour.py:496` | role ∈ {admin, expert} |
| Подсистема исполнителей | `services/distributed_workers/authorization.py: require_permission()` | Отдельные списки субъектов `DISTRIBUTED_WORKERS_{VIEWER,OPERATOR,ADMIN}_SUBJECTS` |
| Восстановление после очистки | `routers/projects.py: _require_restore_clean_auth()` (:921) | Только факт входа |
| Решения по превью ProjectChange | `routers/project_change_preview.py` (:108) | Всегда 403 |

**API сотрудников не защищён вовсе** (`routers/users.py`): POST, PUT, DELETE
и `/switch` доступны любому вошедшему. Через
`PUT /api/users/{id} {"role":"admin"}` любой сотрудник может назначить себе
роль admin. Тогда он проходит `schedule._require_admin` и проверку Human
Contour. Ещё он может переписать себе чужой login, и его сессия начнёт
сопоставляться с чужой записью. `GET /api/users` отдаёт всем полный список с
ролями.

**Запуск обработки не защищён:** ни одного маршрута `audit.py` (62), сравнения
версий (107), сравнения стадий (117 вместе со смежными модулями),
оптимизации, обсуждений и импорта реестров не проверяет роль.

### 1.3. Автор экспертного решения может прийти от клиента

- `routers/knowledge_base.py: _resolve_reviewer()` берёт автора из сессии, но
  `knowledge_base_service.py:614` пишет `expert_reviewer=dec.reviewer or
  reviewer`. При этом `models/expert_review.py: ExpertDecision.reviewer` и
  `timestamp` приходят в теле запроса, так что автора и дату конкретного
  решения можно подменить.
- `user_service.get_current_user()` — глобальный `current_id` из `users.json`,
  общий для всех. Именно им подписываются решения в
  `routers/optimization.py:328, 367, 404, 439, 467`,
  `routers/version_comparison_ai.py:52 (_user)` и запасной ветке
  `stage_comparison.py:250`.
- Сравнение стадий берёт автора из тела запроса: `scProductionAuthor()`
  в `app.js:15604`.

### 1.4. Просмотр, который на деле пишет или запускает

Подтверждено по коду:

| Экран / вызов | Что происходит | Где |
|---|---|---|
| Открытие «Сравнения стадий» | `POST /api/stage-comparison/sessions`: «найти или создать». При совпадении подписи `_inherit_pair_work()` переписывает все пары, иначе создаётся новая сессия и `index.json` | `app.js:14719-14798` (`scRefreshSession`, POST на :14749), `store.py:197-272` |
| Открытие пары | `POST /sessions/{sid}/pairs` (найти или создать pair.json с копированием работы) | `app.js:15256-15320`, `store.py:368-405` |
| Обработка пары | `POST …/sheet-match-suggestions` всегда переписывает файл | `app.js:17535`, `store.py:711-769` |
| Доска пар | `PUT …/document-pairing` с задержкой 600 мс после перетаскивания, автосопоставления и опроса групп | `app.js:14122-14150, 14384` |
| Опрос групп документов (каждые 4 с) | `GET …/document-groups` при готовой сборке вызывает attach/detach и переписывает `document_groups.json` | `app.js:14280`, `document_groups.py:206-221` |
| «Отчёт с ИИ-разбором» | `makeReport()` при отсутствии результата вызывает `startRun(false)`: запуск Claude на 30–40 мин | `project-change-consolidation.js:472-476` |
| Открытие проекта в «Сравнении версий» | `GET …/matrix` → `jobs.matrix()` → `ensure_profiles()` и `_threads.submit(_lineage_job)` | `version-comparison.js:648`, `version_comparison/jobs.py:187-223` |
| Статусы прогонов и серий | GET-запросы «чинят» состояние: переписывают run.json, series.json, job.json и отменяют задания в sqlite-очереди, если считают процесс мёртвым | `version_comparison/jobs.py:927-955`, `series.py:200-227`, `version_comparison_ai/jobs.py:364-381`, `md_report/runner.py:208-227` |
| Просмотр ИИ-описания | `verify.build` пишет `verification.json` и кропы (запись не атомарная) | `version_comparison_ai/verify.py:531-563`, `images.py:135-141` |
| Миниатюры страниц | Кэш PNG на диске (атомарно), но ширина любая от 64 до 2400 px, поэтому диск растёт | `version_comparison/profile.py:233-249` |
| Оптимизация раздела | При отсутствии снимка GET строит его и пишет `snapshot.json` и `history/initial-<uuid>` | `routers/optimization.py:484` |
| База знаний: недостающие нормы | GET выполняет `_reconcile()` и `_save()` | `missing_norms_service.py:235, 276` |
| Решение по замечанию | Каждый клик «принять/отклонить» сразу шлёт `POST /api/discussions/…/resolve` (ещё до «Сохранить») | `app.js:12100-12133` |
| Переключение объекта | `POST /api/objects/switch` меняет общий `objects.json → current_id` | `app.js:4945`, `object_service.switch_object()` :226-242 |
| Глобальные опросы | `live-status` (15 с) вызывает `cleanup_zombies()` и может переписать `batch_queue.json`; usage и paid-cost каждые 60 с | `app.js:2221-2232, 12511`, `routers/audit.py:700` |

### 1.5. Скачивания

- **`GET /api/export/download/{filename}`** (`export.py:77-93`) ищет файл в
  `REPORTS_DIR`, затем в `BASE_DIR = ROOT_DIR`. Единственная защита — проверка
  `startswith(BASE_DIR)`. На проде `ROOT_DIR` — каталог релиза, поэтому:
  - любой вошедший может скачать файлы верхнего уровня релиза (`.mcp.json`,
    `CLAUDE.md`, `*.py`, `requirements*.txt`). `.env` в релизе нет, но в
    git-чекауте он лежит в корне, так что экземпляр, запущенный из чекаута,
    отдал бы и его;
  - настоящие отчёты из `REPORTS_DIR = DATA_DIR/отчет` лежат вне каталога
    релиза и получают 403.

  Генераторы `generateExcel()` и `exportSectionExcel()` в `index.html` нигде
  не вызываются.
- **`GET /api/export/audit-package/{pid}`** (`export.py:252`):
  - ZIP собирается в `io.BytesIO` прямо в `async def` (:157, :271), и туда же
    синхронный `subprocess.run(generate_excel_report.py, timeout=60)`
    (:218, :344). Event loop при этом блокируется до 60 с.
  - В v2-ветке `find_document_by_project_id(project_id)` вызывается без
    объекта (:143).
  - В архив входят исходные PDF и MD, все артефакты, `discussions/*.json`
    (история LLM-чатов) и `expert_review.json` с именами экспертов.
- **Сравнение версий:**
  - `report.xlsx` и `report_html.zip` прогона пишутся через `write_bytes` с
    фиксированными именами (`jobs.py:1102-1103`). Пока они пересобираются,
    скачивающий может получить обрезанный файл.
  - `report.md` ИИ-описания пишется неатомарно (`version_comparison_ai/jobs.py:764`).
  - PDF MD-отчёта атомарен (`report.new.pdf` → `os.replace`), но имя
    временного файла общее (`md_report/runner.py:725-737`).
- **Сравнение стадий:**
  - `report.zip` собирается во временный файл с уникальным именем, удаляется
    через BackgroundTask.
  - `report.xlsx` строится в памяти.
- **`GET /api/document/{pid}/pdf` и картинки блоков** грузятся через
  `<iframe>`/`<img>`. Ни `X-Object-Id`, ни `?object_id=` они не передают.

### 1.6. Привязка к объекту

- **Изоляция выбора объекта** работает так:
  - `CurrentObjectMiddleware` (`core/current_object.py`) привязывает к
    ContextVar значение `X-Object-Id` (или `?object_id=`);
  - фронт добавляет заголовок ко всем `fetch` на `/api/` (`app.js:4866-4886`)
    и хранит выбор по вкладкам в sessionStorage (`app.js:4777-4796`).

  Механизм сохраняем.
- **Пробел:** на проде основной путь чтения (`read_canary._resolve_doc_or_404`,
  `read_canary.py:161-187`) берёт объект **только из `?object_id=`**, а
  заголовок игнорирует. Без параметра срабатывает
  `ProjectsV2Adapter.find_document_by_project_id(...)` и возвращает первое
  совпадение среди всех объектов (`projects_v2_adapter.py:217-231`). Так же,
  без объекта, ищут документ:
  - `version_service._resolve_projects_v2_version_context`;
  - `project_service._get_project_status_v2`;
  - `findings_service._v2_doc_version`;
  - `kb_validation_service`;
  - `optimization._read_v2_optimization`;
  - экспорт архива.
- **Сейчас** в 6 объектах 858 документов, и **ни один код документа не
  повторяется в разных объектах**. Смешение пока скрытое, но первая же
  загрузка совпадающего шифра в другой объект его включит.
- **Ключи в памяти без объекта:**
  - `active_jobs`, очередь и `prepare_state` (`project_id[:version_id]`);
  - кэш разбора MD (`"pid::vid"`);
  - `hidden_projects.json` (голый pid).
- **Сводки по всем объектам сразу:**
  - `GET /api/findings/summary`;
  - `GET /api/optimization/summary/all`;
  - `running` в `live-status`;
  - `GET /api/stage-comparison/sessions` (с путями);
  - `GET /api/project-comparison/catalog`;
  - `GET /api/version-comparison/objects` (с `storage_root`).
- **Сравнение стадий:** все маршруты `/sessions/{sid}/…` работают только по
  session_id, принадлежность сессии объекту не проверяется
  (`scope.sessions_for_object()` для этого есть, но не используется).

### 1.7. WebSocket и служебные сведения

- **WebSocket** (`main.py:330-364`): `_ws_authorized()` проверяет cookie
  только при подключении.
  - Отозвать уже открытое соединение нельзя.
  - Комнату `/ws/audit/{project_id}` можно открыть для любого id.
  - Глобальные подписчики получают все события всех проектов
    (`ws/manager.py: broadcast_to_project`).
  - Фронт не разбирает код закрытия и переподключается бесконечно
    (`app.js:11203-11235`).
- **`/api/info` открыт без входа** (нужен watchdog'у и деплою) и отдаёт
  `base_dir`, `projects_dir`, `claude_cli`, `data_roots`, `disk`. Watchdog
  (`~/bin/webapp-watchdog.sh`), монитор и `deploy_center_release._health()`
  проверяют только HTTP 200.
- **Журнал действий** (`core/action_log.py: ActionLogMiddleware`) пишет
  логин из cookie, путь, query (до 500 символов) и IP, тело запроса не читает.
  `GET /api/action-log` отдаёт всё это любому вошедшему.
- **`/api/usage/*`** (расход подписки, деньги, разбивка по сотрудникам) и
  `/api/audit/account` (данные учётной записи Claude) открыты любому вошедшему.

### 1.8. Инфраструктура

- **Процесс:** один процесс uvicorn, `127.0.0.1:8081`
  (`/etc/systemd/system/auditmanager-backend.service`).
- **Прокси:** перед ним nginx с https, передаёт `X-Forwarded-Proto`,
  `X-Forwarded-For` и `X-Real-IP`. При `PORTAL_COOKIE_SECURE=auto` cookie
  получает флаг Secure.
- **Данные:** служба видит `/home/coder/projects/PDF-proverka` через
  `BindPaths=/srv/auditmanager/data`.
  - `AUDIT_APP_DATA_DIR=…/backend/app/data`, физически
    `/srv/auditmanager/data/backend/app/data` — **вне каталогов релизов**,
    переживает переключение релизов и доступен на запись.
  - `.gitignore` уже исключает `users.json`, но `*.sqlite3` в этом каталоге
    не исключён.
- **Песочница:** `ProtectHome=read-only`. Новые записываемые пути под
  `/home/coder` требуют drop-in с `ReadWritePaths`, а его ставит только
  инженер. Каталог `APP_DATA_DIR` уже записываем.
- **SQLite:** готовый образец есть у подсистемы исполнителей
  (`services/distributed_workers/database.py`): WAL, `busy_timeout`,
  соединение на поток, `run_db()` через to_thread, бэкап перед миграцией схемы.
- **Резервное копирование:** в crontab пользователя копирования
  `APP_DATA_DIR` и `knowledge_base` не найдено.
- **Потоки:**
  - пул `asyncio.to_thread`: `max(32, 4×CPU)` = 48 на 12 ядрах
    (`main.py:78-108`);
  - синхронные `def`-эндпоинты идут в отдельный пул anyio: 40 потоков по
    умолчанию, не настраивается.

---

## 2. Рекомендуемая минимальная архитектура

Оставляем FastAPI, текущий фронтенд, подписанную cookie и middleware.
Добавляем пять частей.

1. **Хранилище аккаунтов на SQLite** — `APP_DATA_DIR/portal_accounts.sqlite3`,
   один файл вне релизов, по образцу `distributed_workers/database.py`.
   Отдельный сервис не нужен: 10–15 человек, запись редкая.
2. **Cookie второй версии.** Полезная нагрузка
   `{"v":2, "a":account_id, "sv":session_version, "iat", "exp"}`.
   - На каждом защищённом запросе сервер достаёт аккаунт из хранилища (через
     кэш процесса, см. 3.5) и сверяет статус и `session_version`.
   - Блокировка, отклонение, сброс пароля и понижение роли увеличивают
     `session_version`, и все открытые сессии и сокеты сразу перестают
     действовать.
3. **Единый контекст запроса `AccountContext`** (id, login, role, status,
   staff_id, права по разделам). Его ставит middleware, остальные слои читают
   только его: журнал действий, атрибуция решений, подсистема исполнителей,
   график.
4. **Центральная таблица политик маршрутов** —
   `backend/app/core/access_policy.py`, по ключу
   `(метод, шаблон пути) → политика`.
   - Проверку выполняет глобальная зависимость
     `FastAPI(dependencies=[Depends(enforce_route_policy)])`.
   - Неизвестный маршрут доступен только admin (fail closed).
   - Тест перебирает `app.routes` и падает, если у маршрута нет явной политики.

   Так мы не опираемся на правило «GET можно» и не расставляем проверки
   по 30 роутерам вручную.
5. **Отдельные read-only варианты** для тех экранов, у которых просмотр сейчас
   пишет (раздел 5.3). viewer вызывает только их.

Почему не другой вариант:

- **Серверная таблица сессий вместо `session_version`** даёт выход «с одного
  устройства», но добавляет запись на каждый вход и очистку старых сессий.
  Для 15 человек хватит «выйти» (удалить cookie) и «завершить все сеансы»
  (увеличить `session_version`).
- **Несколько воркеров uvicorn не нужны и сейчас небезопасны**, состояние
  живёт в процессе:
  - `BOOT_ID` и токены отмены в `version_comparison*` (при нескольких
    процессах GET с чужого процесса пометит живое ИИ-задание как прерванное);
  - очередь `series_ai` без flock;
  - фиксированные `.tmp` в `store._atomic_write_json` и `atomic_write_json`;
  - `pipeline_manager.active_jobs`;
  - `ws_manager`.

  Сначала убираем то, что блокирует event loop, и мерим (этап 8).
- **Внешний IdP, Redis, отдельная БД** при этом объёме не обоснованы.

---

## 3. Модель аккаунтов, статусов, ролей, прав и сессий

### 3.1. Схема (SQLite, `schema_version = 1`)

```sql
CREATE TABLE accounts (
  id               INTEGER PRIMARY KEY,
  login            TEXT NOT NULL,                 -- как ввёл пользователь
  login_norm       TEXT NOT NULL UNIQUE,          -- NFC + casefold + strip
  display_name     TEXT NOT NULL,                 -- ФИО из заявки
  contact          TEXT NOT NULL DEFAULT '',      -- e-mail/телефон для связи, не логин
  note             TEXT NOT NULL DEFAULT '',      -- «кто вы и зачем доступ»
  password_hash    TEXT NOT NULL,                 -- pbkdf2_sha256 (passlib)
  role             TEXT NOT NULL CHECK (role IN ('admin','viewer')),
  status           TEXT NOT NULL CHECK (status IN ('pending','active','rejected','blocked')),
  staff_id         TEXT UNIQUE,                   -- id из users.json или NULL
  session_version  INTEGER NOT NULL DEFAULT 1,
  must_change_password INTEGER NOT NULL DEFAULT 0,
  status_reason    TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
  decided_at TEXT, decided_by INTEGER REFERENCES accounts(id),
  last_login_at TEXT, registration_ip TEXT,
  legacy_login     TEXT UNIQUE                    -- логин из PORTAL_AUTH_USERS, если перенесён
);
CREATE TABLE account_sections (
  account_id   INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  section      TEXT NOT NULL CHECK (section IN
               ('audit_results','version_comparison','stage_comparison','knowledge_base','schedule')),
  can_view     INTEGER NOT NULL CHECK (can_view IN (0,1)),
  can_download INTEGER NOT NULL CHECK (can_download IN (0,1)),
  CHECK (can_download = 0 OR can_view = 1),       -- скачивание только при просмотре
  PRIMARY KEY (account_id, section)
);
CREATE TABLE auth_events (                        -- журнал действий с аккаунтами
  id INTEGER PRIMARY KEY, at TEXT NOT NULL,
  actor_account_id INTEGER, target_account_id INTEGER,
  event TEXT NOT NULL, details_json TEXT NOT NULL DEFAULT '{}' -- без паролей и хешей
);
CREATE TABLE auth_throttle (
  key TEXT PRIMARY KEY,                           -- 'login:<login_norm>' | 'ip:<ip>' | 'reg:<ip>'
  failures INTEGER NOT NULL, window_start REAL NOT NULL, locked_until REAL NOT NULL DEFAULT 0
);
CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
```

Флага `can_manage_accounts` нет (поправка 0.1): управление пользователями
входит в роль admin.

### 3.2. Статусы и переходы

| Из \ в | active | rejected | blocked | pending |
|---|---|---|---|---|
| pending | одобрить (admin) | отклонить (admin) | — | — |
| active | — | — | заблокировать (admin), `sv+1` | — |
| blocked | разблокировать (admin), `sv+1` | — | — | — |
| rejected | одобрить (admin, если передумали) | — | — | — |

Правила:

- **Сессию получает только `active`.** pending, rejected и blocked при верном
  пароле получают 403 с кодом `account_pending` / `account_rejected` /
  `account_blocked`, cookie не выдаётся. Статус сообщается только после
  проверки пароля, поэтому по ответам нельзя узнать, есть ли такой аккаунт.
- **Самостоятельная регистрация** всегда создаёт
  `role='viewer', status='pending'`, `account_sections` пуст. Сервер
  принимает только `login, display_name, contact, note, password`, остальные
  поля тела отклоняются схемой (`extra="forbid"`).
- **Одобрение не открывает разделы.** Права администратор назначает отдельно
  на той же карточке. Одобренный viewer без разделов видит экран
  «Доступ к разделам ещё не назначен».
- **Защита последнего администратора.** В той же транзакции, что и изменение,
  проверяется, что после него останется хотя бы один `role='admin' AND
  status='active'`. Себя заблокировать, понизить или удалить нельзя.
- **Удаление аккаунта** — только для pending и rejected (освобождает логин).
  Активных блокируют, история не теряется.

### 3.3. Роли и права

- **admin** — все маршруты, включая управление аккаунтами.
- **viewer** — маршруты с политикой `SECTION_VIEW(s)` при `can_view` для
  раздела `s`, `SECTION_DOWNLOAD(s)` при `can_download`, плюс `SELF` и `COMMON`
  (см. 4.1).
- **Права администратора в разделах не хранятся:** у admin есть всё.
- **Разделы первой версии:** `audit_results`, `version_comparison`,
  `stage_comparison`, `knowledge_base`, `schedule`. Ограничений по объектам,
  дисциплинам и шифрам нет (допущение Д-1).

### 3.4. Аккаунт и сотрудник

- `accounts.staff_id` — явная ссылка на `users.json → id`. Ставит её только
  администратор (при миграции — из таблицы сопоставления). Уникальна: один
  сотрудник — один аккаунт.
- **Атрибуция действий** (`expert_reviewer`, `updated_by` плана, `user` в
  заданиях сравнения версий, `author` в сравнении стадий) идёт только через
  новую функцию `user_service.staff_for_request(request)`:
  `AccountContext.staff_id → запись users.json → name` (с канонизацией
  `employee_identity`).
  - Нет `staff_id` — экспертное действие не сохраняется, ответ
    `409 account_not_linked_to_staff` (поправка 0.2). Глобальный
    `current_id` не используется.
  - Значения автора и даты из тела запроса сервер игнорирует.
- `users.json`, `employee_identity.py` и `decisions_log.json` не меняются.
  Исторические имена и алиасы продолжают сопоставляться так же
  (`reviewer_matches_user`).
- **Поле `users.json → role` перестаёт участвовать в правах.** Его читают
  `schedule._require_admin` и `human_contour` — обе проверки переводятся на
  `AccountContext.role`. Поле остаётся описательным (эксперт / ГИП и т. п.).
- **`GET /api/users`** для admin отдаёт как сейчас (хешей там нет и не будет —
  они в другой БД). Для viewer этот маршрут закрыт: графику работ достаточно
  имён, которые уже есть в ответе `GET /api/schedule`.

### 3.5. Сессии и отзыв доступа

- **Cookie.**
  - Имя `PORTAL_SESSION_COOKIE_NAME`, с переходом меняется на `am_session`
    (см. 8.4).
  - Флаги `HttpOnly`, `SameSite=Lax`, `Secure` (на проде явно
    `PORTAL_COOKIE_SECURE=true`), `Path=/`.
  - Срок `PORTAL_SESSION_TTL_HOURS` (24 ч, как сейчас).
- **Проверка на каждом запросе** (кроме PUBLIC): подпись, срок, `v==2`, затем
  аккаунт по `a`:
  - `status=='active'`, иначе 401 и cookie сбрасывается;
  - `sv == session_version`, иначе 401 и cookie сбрасывается.
- **Без межзапросного кэша** (поправка 0.3). Аккаунт и права читаются из
  SQLite один раз на каждый HTTP-запрос, в `asyncio.to_thread` (синхронный
  sqlite в event loop уже ронял бэкенд, см. `distributed_workers/database.py`).
  Изменения через API и через CLI действуют со следующего запроса.
- **Изменение прав по разделам** `sv` не меняет: права и так читаются на каждом
  запросе. Заблокированный в момент открытия раздела пользователь получает 403
  на следующем запросе.
- **Выход** (`POST /api/auth/logout`) удаляет cookie у текущего браузера.
- **«Завершить все сеансы»** (сам пользователь или admin) делает `sv+1`.
- **Смена пароля самим пользователем** (`POST /api/auth/password`, нужен
  текущий пароль) делает `sv+1` и сразу выдаёт новую cookie текущему браузеру.
- **Сброс пароля администратором.** Генерируется временный пароль, он
  показывается администратору один раз. Ставится `must_change_password=1` и
  `sv+1`. Пока флаг стоит, пользователю доступны только `SELF`-маршруты и
  страница смены пароля. Восстановления по почте нет: SMTP на сервере не
  настроен (допущение Д-4).
- **WebSocket.**
  - Тот же разбор cookie при подключении.
  - Первая версия пускает к `/ws/*` только admin: viewer'у живые журналы
    аудита не нужны, а глобальная комната транслирует события всех проектов.
    viewer получает закрытие `4403`.
  - `ws_manager` хранит соединения вместе с `account_id` и `sv`.
  - Новый `ws_manager.revoke(account_id)` закрывает их с кодом `4401`; его
    вызывает каждая операция, меняющая `sv` или роль.
  - Раз в 5 с проверка по БД закрывает сокеты с изменённым статусом, ролью, `sv`, истёкшим `exp` или `must_change_password` (так ловятся и правки через CLI).
  - Фронт на `4401/4403/1008` не переподключается, на `4401` уводит на
    `/login`.

### 3.6. Защита запросов и учётных данных

- **Изменяющие методы** (POST, PUT, PATCH, DELETE) на `/api/*`:
  - `Origin` должен совпадать с хостом запроса (с учётом `X-Forwarded-Host`
    nginx);
  - обязателен заголовок `X-Requested-With: AuditManager`. Его добавляют
    обёртка `fetch` в `portal_auth.js` и патч `XMLHttpRequest.prototype`
    (XHR используется в `version-comparison.js:48`,
    `version-comparison-batch.js:254`, `app.js:15005`).

  Исключения: `/api/v1/worker/*` (bearer, без cookie) и `/api/auth/login`
  (только Origin).
- **`Sec-Fetch-Site: cross-site`** на любом `/api/*` даёт отказ. Так
  закрываются и admin-маршруты, которые пишут на GET (например `matrix`):
  при `SameSite=Lax` их можно было бы вызвать переходом по ссылке с чужого
  сайта.
- **Ограничение попыток** хранится в таблице `auth_throttle`, чтобы
  переживать рестарт. IP берётся из `request.client.host`: uvicorn доверяет
  `X-Forwarded-For` от 127.0.0.1.
  - Вход: 5 неудач на логин за 15 мин блокируют логин на 15 мин; 30 неудач с
    одного IP за 15 мин блокируют IP. Ответ одинаковый — «Неверный логин или
    пароль».
  - Регистрация: 3 заявки с одного IP в час, не более 20 открытых pending
    всего, сверх лимита 429.
- **Пароли:**
  - не короче 10 символов, не равны логину;
  - новые хеши — `pbkdf2_sha256` с 600 000 раундов, через
    `passlib.CryptContext`;
  - существующие хеши на 29 000 раундов проверяются как есть и
    перехешируются при первом успешном входе (`needs_update`);
  - проверка идёт в `asyncio.to_thread`, сейчас она синхронно выполняется в
    `async def login`;
  - при неизвестном логине по-прежнему сверяемся с dummy-хешем;
  - `passlib==1.7.4` добавить в `requirements.txt`.
- **Пароли и хеши не попадают в ответы и логи:**
  - API аккаунтов отдаёт явную проекцию без `password_hash`;
  - журнал действий тело не читает (уже так);
  - для `/api/auth/*` отдельный обработчик ошибок валидации не эхо-возвращает
    `input` (FastAPI по умолчанию кладёт его в ответ 422);
  - временный пароль администратору показывается в ответе один раз и в
    `auth_events` не пишется.
- **`/api/info`:**
  - аноним получает `{"app","version","status":"ok"}` — для watchdog,
    монитора и деплоя этого достаточно, они проверяют только код 200;
  - полный ответ — только admin.
- **`/docs`, `/openapi.json`, `/redoc`** — только admin.

---

## 4. Матрица доступа

### 4.1. Классы политик

| Политика | Кто | Примеры |
|---|---|---|
| `PUBLIC` | все, без входа | `/login`, `/register`, `POST /api/auth/login`, `POST /api/auth/register`, `POST /api/auth/logout`, `GET /api/auth/me`, `GET /api/info` (сокращённый), `/favicon.ico` |
| `MACHINE` | свой контур | `/api/v1/worker/*` (bearer-токен исполнителя, `audit_worker_agent.py`). gRPC/mTLS Gateway вне FastAPI — не затрагивается |
| `SELF` | любой `active` (и с `must_change_password`) | `GET /api/auth/me`, `POST /api/auth/password`, `POST /api/auth/sessions/revoke` |
| `COMMON` | любой `active` без `must_change_password` | `GET /` (SPA), `/static/*`, `GET /api/objects` (для viewer без `projects_dir`), `GET /api/objects/stats` (без `force`) |
| `SECTION_VIEW(s)` | admin; viewer с `can_view(s)` | см. 5.1 |
| `SECTION_DOWNLOAD(s)` | admin; viewer с `can_download(s)` | см. 5.1 |
| `ADMIN` | только admin. Политика по умолчанию для всего, что не перечислено | запуск, загрузка, правки, решения, очереди, настройки, служебные сведения |
| `ADMIN` (аккаунты) | admin | `/api/admin/accounts/*`, `/admin/users` |

### 4.2. Разделы и действия

| Раздел | Просмотр (`can_view`) | Скачивание (`can_download`) | Всегда только admin |
|---|---|---|---|
| Результаты аудита | список проектов объекта, карточка, версии и файлы (имена), замечания, оптимизация (проектная и по разделу, если снимок уже построен), блоки и картинки блоков, страницы документа и PDF во встроенном просмотре, решения экспертов (`GET expert-review`), отчёт о перенесённых замечаниях | ZIP-пакет аудита (профиль viewer, см. 5.4), Excel «решённые замечания» | запуск/повтор/продолжение/отмена этапов, подготовка данных и кропы, очередь, модели, шаблоны промптов, журналы аудита, загрузка/удаление/переименование/скрытие, решения эксперта, обсуждения (чат с моделью), проверка по базе знаний, `llm-text`, `?storage=legacy` |
| Сравнение версий | каталог объекта, обзор, матрица (только готовая), прогоны, серии, листы, превью, тайлы, миниатюры (фиксированные ширины), готовые ИИ-описания и «Изменения в листе», статусы MD-отчётов | `report.xlsx`/`report_html.zip` прогона (только готовые), xlsx серии, ИИ `report`/`report.md`/`view.md`, `changes.md`, `summary.md` серии, PDF и JSON MD-отчёта (только `done`) | загрузки и пакеты, подготовка, прогоны, серии, ИИ-задания и оценки, «Луна», очередь этапов, связи листов, правка каталога, настройки и runtime, `worker-status`, `packet`/`check-crop` (что отправлено модели) |
| Сравнение стадий | объекты (без путей), группы документов (только чтение), текущая сессия объекта (только чтение), пара, все GET-результаты (production state/changes/questions/reports/evidence, text/graphic/high-level результаты, сводные результаты, ProjectChange по объекту, каталог результатов без `diagnostics`, Human Mapping `ui-data`/assets/чтение, prelinks GET, block-mapping GET кроме `bridge-check`), рендер страниц | `report.xlsx` и `report.zip` сводного результата | создание сессий и пар, загрузка стадий, группы и сборки, сопоставление документов и листов, все запуски (production, сводка, ИТ-анализ моделью, text/graphic/AI), ответы и решения, Human Mapping записи, prelinks POST/PUT/DELETE, `GET /sessions` (всех объектов), `production/ai-modes`, `production/batch` |
| База знаний | записи, статистика, паттерны, недостающие нормы (чтение) | — (выгрузок сейчас нет) | подтверждения, отзыв, паттерны (detect/approve/dismiss/edit), импорт Excel, отметки норм, backfill |
| График работ | `GET /api/schedule`, `GET /api/schedule/plan` | — (выгрузок сейчас нет) | `PUT /api/schedule/plan` |
| Вне разделов | — | — | пользователи (`/api/users*`), аккаунты, объекты (создание/правка/удаление, `switch`), расход и деньги (`/api/usage/*`), учётная запись Claude, журнал действий, исполнители (`/api/workers/*`, `/audit-workers`), критик v2, внешние реестры, shadow API, дисциплины (правка), `/docs` |

У «Базы знаний» и «Графика работ» сейчас нет выгрузок, поэтому право
скачивания там ничего не открывает. Флаг в интерфейсе оставляем — на случай,
если выгрузка появится (допущение Д-5).

---

## 5. Карта маршрутов, скачиваний и неявных записей

Всего в приложении 537 маршрутов. Полный перечень с политиками будет в
`access_policy.py` (этап 3), а тест `test_access_policy_coverage.py`
гарантирует, что ни один не останется без политики. Ниже — то, что открывается
viewer'у; всё остальное получает `ADMIN`.

### 5.1. Что открывается viewer'у (allowlist по разделам)

**`audit_results` — VIEW**

- `GET /api/projects`, `GET /api/projects/{pid:path}` (catch-all, карточка),
  `GET /api/projects/{pid}/versions`, `…/versions/{vid}/files`,
  `GET /api/projects/disciplines`, `GET /api/audit/disciplines`,
  `GET /api/project-groups`.
- `GET /api/findings/{pid}`, `…/finding/{fid}`, `…/block-map`,
  `…/textlayer-highlights-shadow`, `…/kb-validation` (чтение готового);
  `GET /api/findings/summary` (после этапа 6 — по объекту).
- `GET /api/tiles/{pid}/blocks`, `…/blocks/analysis`, `…/blocks/image/{bid}`,
  `…/blocks/region-image/{bid}`.
- `GET /api/document/{pid}/pages`, `…/page/{n}`, `…/pdf`. PDF показывается во
  встроенном просмотре, поэтому увидеть его без возможности сохранить нельзя.
  Это честно фиксируем: исходный PDF доступен при праве просмотра.
- `GET /api/optimization/{pid}`, `…/block-map`, `…/status`;
  `GET /api/optimization/summary/all` (после этапа 6 — по объекту);
  `GET /api/optimization/section/{code}` (только готовый снимок);
  `GET …/replications`, `…/replications/{rid}` (без «ремонта» статуса).
- `GET /api/knowledge-base/expert-review/{pid}`.
- `GET /api/projects/{pid}/versions/{vid}/migrated-findings/report`.

**`audit_results` — DOWNLOAD**

- `GET /api/export/audit-package/{pid}` (профиль viewer, 5.4).
- `GET /api/discussions/{pid}/resolved/excel`.

**`version_comparison` — VIEW** (префикс `/api/version-comparison`)

- `GET /objects` (проекция без `storage_root`), `GET /objects/{oid}/catalog`,
  `…/overview`.
- **Новый** `GET /objects/{oid}/projects/{pid}/matrix-view` (5.3).
- `GET /sources/{sha}/pages/{page}/thumb` (ширина округляется вверх до
  ближайшей из 160, 320, 640, 1280, 2400), `…/canvas`, `…/tile`.
- `GET /projects/{pid}/runs`, `/runs/{rid}`, `/runs/{rid}/permit`,
  `/runs/{rid}/spec`, `/runs/{rid}/sheets/{spid}`, `…/preview.png`, `…/tile`.
- `GET /projects/{pid}/series`, `/series/{sid}`.
- `GET /runs/{rid}/ai/jobs`, `…/jobs/{jid}`, `/runs/{rid}/ai/latest`,
  `…/{jid}/view` (без построения слоёв), `…/{jid}/sheets/{spid}`, `…/focus`,
  `…/ref-focus` (без `verify.build`).
- `GET /runs/{rid}/sheets/{spid}/changes`, `…/changes/focus`.
- `GET /series/{sid}/ai-sheet-changes`, `…/whole-specifications/status`.
- `GET /objects/{oid}/md-reports`, `…/md-reports/{rid}`.

Все GET выше вызывают сервисы в режиме `repair=False` (5.3).

**`version_comparison` — DOWNLOAD**

- `GET /runs/{rid}/reports/{name}` (только `ready`), `/series/{sid}/report.xlsx`.
- `GET …/ai/jobs/{jid}/report`, `…/report.md`, `…/view.md`, `…/changes.md`,
  `/series/{sid}/ai-sheet-changes/summary.md`.
- `GET /objects/{oid}/md-reports/{rid}/report.pdf`, `…/report.json` (только
  `state=done`).

**`stage_comparison` — VIEW** (префикс `/api/stage-comparison`, `PP` =
`/sessions/{sid}/pairs/{pid}`)

- `GET /objects` (без путей), `GET /objects/{oid}/document-groups?readonly=1`
  (5.3).
- **Новые** `GET /objects/{oid}/session` и
  `GET /objects/{oid}/sessions/{sid}/pair?left=…&right=…` (5.3).
- `GET /sessions/{sid}` (для viewer — с проверкой принадлежности объекту и без
  абсолютных путей), `GET PP`.
- `GET PP/production/state`, `…/stages/{stage}/result`, `…/text-evidence`,
  `…/changes`, `…/questions` (без создания реестра), `…/questions/{dk}`,
  `…/preliminary-report`, `…/human-review`, `…/final-report`,
  `…/changes/{tid}/evidence`.
- `GET PP/runs/{rid}/project-changes`, `GET PP/consolidated`,
  `…/consolidated/{src}/{con}`, `…/it-analysis`, `…/it-analysis/model-run`
  (статус), `…/evidence/{eid}/crop`.
- `GET PP/sheet-matches`, `…/sheet-link-repairs`, `…/text-comparison`,
  `…/text-exclusions`, `…/graphic-comparison`, `…/text-differences`,
  `…/text-ai-review`, `…/text-final-comparison`, `…/text-change-summary`,
  `…/high-level-project-changes`, `…/text-entities`.
- `GET PP/page-thumb`, `…/page-info`, `…/text-search`, `…/page-preview`,
  `…/page-tile`, `…/page-svg`.
- `GET PP/block-mapping/*` (кроме `bridge-check`), `GET PP/prelinks`.
- `GET /objects/{oid}/project-changes` и его `evidence/*`, все GET
  `/api/project-change-preview/*`, `GET /api/project-comparison/catalog`
  (без `diagnostics=true`, после этапа 6 — по объекту),
  `GET /api/project-comparison/catalog/entries/{eid}`.
- GET `/api/human-mapping/*` (`ui-data`, `assets`, чтение `reviews` и
  `block-links`).

**`stage_comparison` — DOWNLOAD**

- `GET PP/consolidated/{src}/{con}/report.xlsx`, `…/report.zip`.

**`knowledge_base` — VIEW**

- `GET /api/knowledge-base/entries`, `/stats`, `/patterns`, `/missing-norms`
  (в режиме `reconcile=False`).

**`schedule` — VIEW**

- `GET /api/schedule`, `GET /api/schedule/plan`.

### 5.2. Ловушки, которые закрывает таблица политик

- **Пишут или запускают на GET** — остаются `ADMIN`, а viewer получает
  read-only двойник:
  - `vc …/matrix`;
  - `vc /ai/settings` (запускает `codex --version` и `codex exec --help`);
  - `vc /md-reports/runtime`;
  - `vc /ai-sheet-changes/queue` (очередь всех объектов);
  - `vc /worker-status`;
  - `sc /sessions` (все объекты с путями);
  - `sc …/production/batch` (переписывает файл);
  - `audit live-status` (`cleanup_zombies`) и `batch/status` (`_persist_queue`);
  - `optimization …/pipeline/status` (mkdir и копирование);
  - `blocks …/llm-text` (раскрывает промпты);
  - `audit …/prompts`, `…/templates`, `…/log`.
- **POST, которые только читают** (`detect-discipline`, `routing-preview`,
  `document-pairing/suggest`, `upload-folder/precheck`), остаются `ADMIN`:
  они нужны только в админских сценариях.
- **Маршруты с «ремонтом» статуса.** Для admin поведение не меняется. Для
  viewer — режим без записи (5.3).

### 5.3. Read-only двойники для неявных записей

| Сейчас | Для viewer | Функции |
|---|---|---|
| `POST /api/stage-comparison/sessions` (найти или создать) | `GET /api/stage-comparison/objects/{oid}/session`. Сервер сам берёт пути стадий объекта (клиент их не передаёт), считает подпись `_source_signature()` без записи и отдаёт сессию с той же подписью. Если подпись не совпала — последнюю сессию тех же путей с `stale: true` («Есть новые документы, администратор ещё не обновил сравнение»). Сессии нет — `404 not_prepared` | вынести сканирование и подсчёт подписи из `store.create_session()` в `store.find_session(stage_a, stage_b) -> (session or None, latest, stale)`. `create_session()` вызывает её же, `_inherit_pair_work()` остаётся только в админском пути |
| `POST …/sessions/{sid}/pairs` (найти или создать) | `GET …/objects/{oid}/sessions/{sid}/pair?left=&right=`, только поиск. Нет пары — «Результат ещё не подготовлен администратором» | `store.find_pair(session_id, left_pdf, right_pdf)` |
| `POST PP/sheet-match-suggestions` при обработке пары | не вызывается | фронт (этап 7) |
| `PUT …/document-pairing` (автосохранение) | не вызывается: доска в режиме только чтения | `pcReadOnlySources` (`app.js:12774`) дополняется `|| !canAdmin` |
| `GET …/document-groups` (attach/detach) | `?readonly=1` → `list_groups(attach=False)` | `stage_comparison/document_groups.py: list_groups(), _attach_ready()` |
| `GET …/production/questions` (создание sqlite при первом обращении) | при отсутствии реестра — пустой список | `human_contour.build_read_model(create=False)`, `DecisionRegistry(create=False)` |
| `GET /api/version-comparison/…/matrix` (профили и сборка линий) | `GET …/matrix-view`: готовый `lineage` при совпадении подписи, иначе `{"state":"not_prepared"}`. Если admin сейчас строит — `{"state":"building"}` из `_lineage_state`, без новых задач | `version_comparison/jobs.py: matrix_view()`: те же шаги, что в `matrix()`, без `ensure_profiles()` и `_threads.submit()` |
| `get_run` / `series.get` / `ai get_job` / `md_report.get` / `sheet_match.status` («ремонт» статуса) | параметр `repair: bool = True`; маршруты viewer передают `repair=False`. Ремонт делают админские чтения и фоновые `vc-recovery`/опросы очередей | `version_comparison/jobs.py:927-955`, `series.py:200-227`, `version_comparison_ai/jobs.py:364-381`, `md_report/runner.py:208-227`, `sheet_match.py:397-406` |
| AI `view`/`view.md`/`ref-focus` (`verify.build` пишет слои) | `materialize=False`: берутся готовые слои; нет слоя — показ без проверки и пометка «проверка не подготовлена» | `version_comparison_ai/view.py: job_view()`, `verify.py: build()`, `summary.py:255-264` |
| `GET …/thumb` (кэш любой ширины) | ширина округляется до фиксированного набора | `routers/version_comparison.py: page_thumb()` |
| `GET /api/optimization/section/{code}` (строит снимок) | нет снимка — `{"state":"not_prepared"}` | `section_optimization_service`: `build_if_missing=False` |
| `GET …/replications*` (`_mark_interrupted_if_needed`) | `repair=False` | `section_optimization_replication_service` |
| `GET /api/knowledge-base/missing-norms` (`_reconcile`/`_save`) | `reconcile=False` | `missing_norms_service.get_missing_norms()`, `get_stats()` |
| `GET /api/project-groups` (одноразовая миграция формата) | миграция выполняется при старте или admin-чтении, viewer только читает | `group_service.load_groups(migrate=False)` |
| `GET /api/tiles/…/blocks/image/{bid}` (восстановление вытесненного кропа) | оставляем: это восстановление кэша того же артефакта, а не анализ. Ветку v2 переносим в `to_thread` | `routers/blocks.py:1261`, `block_crop_store.resolve_block_image()` |
| `makeReport()` → `startRun(false)` | ветка viewer: «Результат ещё не подготовлен администратором» | `project-change-consolidation.js:472-476` |
| `POST /api/objects/switch` | не вызывается. Выбор объекта viewer хранится только во вкладке (sessionStorage + `X-Object-Id`), как уже сделано | `app.js: switchObject()` :4937-4960 |
| Мгновенный `POST …/resolve` при клике по решению | у viewer кнопок решений нет | `app.js: setExpertDecision()` |

В интерфейсе различаются три вида действий:

1. **Скачать готовый файл.** Файл уже лежит на диске, viewer с
   `can_download` его получает.
2. **Детерминированно сформировать файл из сохранённых данных.** Это xlsx
   серии, сводный `report.xlsx`/`report.zip` стадий, `changes.md`/`view.md`,
   ZIP-пакет аудита, Excel решённых замечаний. Модели и запуска нет, всё
   строится в памяти или во временном файле с уникальным именем. viewer с
   `can_download` это получает.
3. **Новый анализ** — только admin. Даже «если результата нет — посчитаем»
   для viewer не делается никогда.

### 5.4. Скачивания: что исправить

1. **`GET /api/export/download/{filename}`:**
   - убрать fallback в `BASE_DIR`;
   - искать только в `REPORTS_DIR`, имя по шаблону `^[\w.-]+\.xlsx$`, без
     ведущей точки, `resolve().is_relative_to(REPORTS_DIR)`;
   - политика `ADMIN` (генераторы глобальные, по всем проектам, из интерфейса
     не вызываются);
   - в `POST /api/export/excel*` не отдавать абсолютный `path` и формировать
     уникальное имя `…_<YYYYmmdd_HHMMSS>_<6 hex>.xlsx`.
2. **ZIP-пакет аудита** (`routers/export.py: download_audit_package()`,
   `_download_audit_package_v2()`):
   - объект — явно из привязанного ContextVar или `?object_id=`
     (`find_document_by_project_id(pid, object_id=…)`, `v2_source_pdf(…, object_id)`);
   - сборка целиком в `asyncio.to_thread`: временный файл в `tempfile`
     вместо `BytesIO`, `FileResponse` и `BackgroundTask(unlink)`;
     `subprocess.run` внутри потока;
   - не больше двух сборок одновременно (`asyncio.Semaphore(2)`), сверх
     лимита 429 «Подождите»;
   - **согласованность:** если по этой версии идёт обработка
     (`pipeline_manager.is_running(project_id, version_id)`,
     `pipeline/manager.py:1207`), ответ 409 «Идёт
     обработка, архив будет доступен после завершения». Дополнительно перед
     сборкой фиксируются `mtime_ns`/`size` включаемых артефактов, после сборки
     сверяются; при расхождении — одна повторная сборка, затем 409;
   - **профиль viewer:** без `discussions/*.json` (история чатов с моделью —
     admin, Д-3) и без промптов. Исходные PDF/MD, замечания, оптимизация, граф,
     `expert_review.json` и `audit_report.xlsx` остаются — viewer и так видит
     их в разделе. Состав задаётся явным списком в коде, а не «всё из
     `_output`».
3. **Отчёты прогона сравнения версий** (`version_comparison/jobs.py:1095-1108`):
   - запись `tmp(уникальное имя) → os.replace`;
   - `GET …/reports/{name}` отдаёт файл только при состоянии `ready` и
     наличии обоих файлов, во время пересборки — 409.
4. **`report.md` ИИ-описания** (`version_comparison_ai/jobs.py:764`) пишется
   через `atomic_write_text`.
5. **PDF MD-отчёта** (`md_report/runner.py:721-743`):
   - уникальное временное имя;
   - замок по `report_id`, чтобы пересборка не шла дважды;
   - при неудачной пересборке прежний `report.pdf` остаётся доступным (не
     переводить готовый отчёт в `failed`, хранить ошибку отдельным полем).
6. **`common/atomic_json.atomic_write_json`** и `store._atomic_write_json`:
   имя временного файла `.<name>.<pid>.<uuid>.tmp` вместо фиксированного
   `.tmp`. Это дешёвое и полезное исправление, даже если процесс останется
   один.
7. **`discussions …/resolved/excel`:** `Content-Disposition` по RFC 5987
   (`filename*=UTF-8''…`), иначе кириллический шифр даёт 500.
8. **`<iframe>`/`<img>`/`<a>` к PDF, кропам и файлам** получают `?object_id=`
   через общий помощник `withObject(url)` в `app.js` (этап 6).
9. **`packet`/`check-crop`** (то, что отправлено модели) — `ADMIN`.

---

## 6. Интерфейс и пользовательские сценарии

### 6.1. Вход и регистрация

- **`frontend/login.html`** (автономная страница, без внешних ресурсов):
  - сообщения по кодам `account_pending` («Заявка ожидает одобрения
    администратором»), `account_rejected`, `account_blocked`, 429 «Слишком
    много попыток, попробуйте через N минут»;
  - ссылка «Зарегистрироваться».
- **Новая `frontend/register.html`** (тоже автономная):
  - поля: логин, ФИО, контакт, «кто вы и зачем нужен доступ», пароль дважды;
  - ответ: «Заявка отправлена. Доступ появится после одобрения администратором»;
  - занятый логин сообщается явно. Это осознанный компромисс удобства:
    логины сотрудников и так короткие и угадываемые, а перебор ограничен
    лимитами 3.6.
- **Страница смены пароля:** при `must_change_password` пользователь видит
  только её.

### 6.2. Управление пользователями (admin)

- **Отдельная страница `/admin/users`:** `frontend/admin-users.html` и
  `frontend/static/js/admin-users.js`. Отдаёт её `main.py` по образцу
  `serve_audit_workers()`. 21-тысячестрочный `app.js` не трогаем.
- **Вкладка «Заявки»:** pending с ФИО, контактом, комментарием, датой и IP.
  Кнопки «Одобрить» (сразу открывает матрицу разделов) и «Отклонить» (с
  причиной).
- **Вкладка «Пользователи»:** роль, статус, сотрудник (`staff_id`, выбор из
  `users.json`), матрица 5 разделов × {просмотр, скачивание} (при снятии
  «просмотра» «скачивание» снимается само), «Заблокировать/Разблокировать»,
  «Сбросить пароль» (временный показывается один раз), «Завершить сеансы»,
  последний вход.
- **Вкладка «Журнал»:** `auth_events`.
- **Пункт меню «Пользователи»** в боковой панели `index.html` — только при
  `caps.role == "admin"`.
- **API** (`backend/app/api/routers/accounts_admin.py`, политика `ADMIN`):
  - `GET /api/admin/accounts?status=`, `GET /api/admin/accounts/{id}`;
  - `POST /api/admin/accounts/{id}/approve|reject|block|unblock|reset-password|revoke-sessions`;
  - `PUT /api/admin/accounts/{id}/sections`, `PUT /api/admin/accounts/{id}/role`,
    `PUT /api/admin/accounts/{id}/staff`;
  - `DELETE /api/admin/accounts/{id}` (только pending и rejected);
  - `GET /api/admin/auth-events`.

  Ответы — явная проекция без хешей.

### 6.3. Портал для viewer

- **Возможности страницы:** `main.py: serve_spa()` подставляет
  `{{portal_caps}}` в `<script type="application/json" id="portal-caps">`
  (так же, как сейчас `prelink_caps` и `assembly_caps`):
  `{role, sections:{s:{view,download}}, accounts, staff_id, must_change_password}`.
  `GET /api/auth/me` отдаёт то же для обновления без перезагрузки. Модули
  `version-comparison-*.js` и `project-change-*.js` читают `window.__portalCaps`
  (computed из `app.js` им недоступны).
- **`app.js`:**
  - `canAdmin`, `canView(s)` и `canDownload(s)` рядом с `schedIsAdmin`
    (:11844), сам `schedIsAdmin` выражается через `canAdmin`;
  - навигация показывает только разрешённые разделы, hash-маршрутизатор
    уводит с запрещённого на первый разрешённый или на экран «Доступ к
    разделам ещё не назначен»;
  - кнопки запуска, правки, загрузки и решений обёрнуты в
    `v-if="canAdmin"`. Это около 150 элементов в `index.html` и около 40 в
    шаблонах JS-модулей; большая часть сгруппирована в панели, поэтому правок
    примерно 60–80;
  - кнопки скачивания — `v-if="canDownload(s)"`.
- **Отключается у viewer при загрузке:**
  - `startPolling()` (live-status каждые 15 с);
  - `usagePollTimer` (6 запросов денег и подписки каждые 60 с);
  - `GET /api/audit/account`, `GET /api/users`;
  - `connectGlobalWS()` и `connectProjectWS()`;
  - `fetchPrepareQueue()` и `refreshBatchQueue()`;
  - опрос пакетов загрузки в сравнении версий (`version-comparison-batch.js:161-179`).

  Статусы проектов viewer получает из `GET /api/projects`.
- **Сравнение стадий:**
  - `scLoadObjects()`/`scRefreshSession()` у viewer идут в
    `GET …/objects/{oid}/session`;
  - `scOpenPairRowResult()` идёт в `GET …/pair`;
  - `pcReadOnlySources` включает `!canAdmin`, этим разом отключаются
    перетаскивание, выбор и автосохранение доски (уже реализованы по этому
    флагу);
  - `stage-block-mapping.js` (`writeAccess`/`draftWritable`) и `pcDecide`
    смотрят на серверные `capabilities`, а сервер для viewer отдаёт `false`.
- **«Отчёт с ИИ-разбором»** (`project-change-consolidation.js`) у viewer
  делает так:
  - результат готов — скачивание;
  - нет — сообщение «Результат ещё не подготовлен администратором»;
  - кнопки «Пересчитать», «Отменить» и «Загрузить» скрыты;
  - опрос `model-run` остаётся только для чтения статуса.
- **Сравнение версий:** `loadMatrix()` у viewer идёт в `matrix-view`,
  состояние `not_prepared` показывается как «Сравнение ещё не подготовлено
  администратором». Кнопки запуска в `acts()` (`version-comparison-projects.js:443-512`)
  скрыты, остаются «Скачать PDF» и «Скачать MD».
- **`switchObject()`:** у viewer `POST /objects/switch` не отправляется; у
  admin его ошибка не должна прерывать `refreshProjects()`.
- **`portal_auth.js`:**
  - заголовок `X-Requested-With` для fetch и XHR;
  - разбор `401 {code}` (истёкшая или отозванная сессия → `/login?reason=…`)
    и `403 {code:"forbidden_section"}` (сообщение, а не редирект).
- **Обработчики `onclose` сокетов** читают `event.code` и не переподключаются
  на `4401/4403/1008`.

---

## 7. Этапы реализации

Порядок: 0 → 1 → 2 → 3 → (4 и 6 параллельно) → 5 → 7 → 8 → 9 → 10.
Порядок обязателен там, где указана зависимость. До этапа 9 viewer'ов на
проде нет: регистрация выключена флагом `PORTAL_REGISTRATION_ENABLED=false`,
режим `PORTAL_AUTH_MODE=env_users` (как сейчас). Каждый этап — отдельные
коммиты в `main` с тестами. Выкатывать можно по этапам, поведение сотрудников
до этапа 9 не меняется (кроме этапа 0).

### Этап 0. Срочные исправления, не зависящие от новой модели

- **Цель:** закрыть найденные дыры уже в текущей модели.
- **Файлы и функции:**
  - `routers/export.py: download_file()` — 5.4 п.1;
  - `main.py: api_info()` — сокращённый ответ для анонима;
  - `routers/users.py: add_user/switch_user/update_user/delete_user` — пока
    через проверку `schedule._require_admin()`, вынесенную в
    `user_service.require_staff_admin(request)` (users.json `role=admin`, его
    сейчас нельзя повысить себе самому, раз PUT закрыт);
  - `knowledge_base_service.py:614` — при включённой авторизации автор и
    время решения только серверные;
  - `routers/usage.py`, `routers/action_log.py`, `GET /api/audit/account` —
    та же проверка.
- **Поведение:**
  - файлы релиза больше не скачиваются;
  - эксперты не могут менять сотрудников, видеть деньги и журнал;
  - автор решения не подменяется;
  - watchdog и деплой работают как прежде.
- **Зависимости:** нет.
- **Проверки:** `tests/test_export_download_guard.py` (`.mcp.json`,
  `.env`, `..`, точка в начале имени дают 404/403; отчёт из `REPORTS_DIR`
  отдаётся); `tests/test_users.py` (эксперт получает 403, admin — 200);
  `tests/test_portal_auth.py` (`/api/info` для анонима без путей);
  `tests/test_kb_reviewer_attribution.py`; `scripts/ci_regression_gate.py`.
- **Критерий готовности:** тесты зелёные, гейт без новых падений; на стенде
  `curl /api/export/download/.mcp.json` даёт 404.

### Этап 1. Хранилище аккаунтов, пароли, CLI

- **Цель:** иметь источник аккаунтов и прав, ещё не подключённый к входу.
- **Файлы:**
  - новые `backend/app/services/accounts/{__init__,db.py,store.py,passwords.py,throttle.py}`;
  - `backend/app/core/config.py`: `PORTAL_ACCOUNTS_DB` (по умолчанию
    `APP_DATA_DIR/portal_accounts.sqlite3`), `PORTAL_AUTH_MODE`
    (`env_users`|`accounts`), `PORTAL_REGISTRATION_ENABLED`;
  - `requirements.txt` (`passlib==1.7.4`);
  - `.gitignore` (`/backend/app/data/*.sqlite3*`);
  - новый `backend/scripts/portal_accounts.py` (CLI: `init`, `import-legacy`,
    `list`, `set-admin`, `reset-password`, `backup`).
- **Функции:**
  - `db.ensure_ready()`: WAL, `busy_timeout`, `foreign_keys`, права 0600,
    бэкап перед миграцией схемы, по образцу `distributed_workers/database.py`;
  - `store.create_pending()`, `approve()`, `reject()`, `block()`, `unblock()`,
    `set_sections()`, `set_role()`, `set_staff()`, `reset_password()`,
    `revoke_sessions()`, `get_context(account_id)`, `authenticate(login, pw)`;
  - каждая изменяющая функция — одна транзакция с проверкой «последнего
    администратора» и записью `auth_events`;
  - `passwords.verify_and_update()` через `CryptContext(schemes=["pbkdf2_sha256"],
    pbkdf2_sha256__rounds=600000)`;
  - `normalize_login()`: NFC + casefold + strip, допустимы `[a-z0-9._-]{3,32}`
    после нормализации.
- **Поведение:** никакого, модуль не подключён.
- **Зависимости:** нет.
- **Проверки:** `tests/test_portal_accounts_store.py`:
  - ограничения CHECK (скачивание без просмотра, неизвестный раздел, роль,
    статус);
  - уникальность `login_norm` (`Ivan`/`ivan`/`ivan` в NFD) и `staff_id`;
  - последний admin, самоблокировка;
  - каждая операция пишет `auth_events`, хешей в событиях нет;
  - старые хеши на 29 000 раундов принимаются и перехешируются;
  - throttle переживает переоткрытие БД.

  Всё на временной БД в `tmp_path`.
- **Критерий готовности:** тесты зелёные, CLI `init` и `list` работают на
  временной БД.

### Этап 2. Контекст аккаунта, сессии v2, вход, регистрация

- **Цель:** при `PORTAL_AUTH_MODE=accounts` входить по аккаунтам и иметь
  единый `AccountContext`.
- **Файлы и функции:**
  - `core/portal_auth.py`:
    - `issue_token_v2(ctx)`, `verify_token_v2()`;
    - `request_account(request) -> AccountContext | None` (кэш, 3.5);
    - `PortalAuthMiddleware.dispatch()`: режимы `env_users`/`accounts`,
      `request.state.account`, `scope["state"]["account"]`, коды
      `401 {code}`, проверки Origin, `X-Requested-With` и `Sec-Fetch-Site`
      (3.6), новые PUBLIC-пути `/register`, `/api/auth/register`;
    - `request_username()` остаётся как совместимость: возвращает
      `ctx.login`;
  - `routers/auth.py`: `login` (`to_thread`, throttle, коды статусов),
    `logout`, `me` (роль, разделы, флаги), новые `register`, `password`,
    `sessions/revoke`, обработчик ошибок валидации без эха;
  - `core/action_log.py: ActionLogMiddleware._resolve_actor()` — из
    `scope["state"]`, без повторной проверки cookie;
  - `services/common/user_service.py`: `staff_for_request(request)`;
    `get_current_user()` помечается «только при выключенной авторизации»;
  - замена атрибуции:
    - `routers/optimization.py:328, 367, 404, 439, 467`;
    - `routers/version_comparison_ai.py: _user()`;
    - `routers/stage_comparison.py: _engineer_author()`;
    - `knowledge_base._resolve_reviewer()`;
    - `schedule._require_admin()`;
    - `human_contour` (роль и автор).

    Поля автора из тела игнорируются.
  - `routers/users.py: list_users()` — `current_id` из `ctx.staff_id`;
    `/switch` — `ADMIN` и не влияет на атрибуцию при включённой авторизации;
  - `services/distributed_workers/authorization.py: resolve_actor()` —
    субъект `ctx.login`. Если `ctx.role != 'admin'` — прав нет (диагностика
    «подсистема доступна только администратору портала»). Списки субъектов
    продолжают сужать права admin;
  - `main.py: _ws_authorized()` → `ws_account()`; `ws/manager.py`: учёт
    `account_id`/`sv`, `revoke(account_id)`, периодическая проверка срока;
    `/ws/*` только для admin;
  - `main.py: serve_login()`, новый `serve_register()`.
- **Поведение:** при `env_users` всё как сейчас. При `accounts` вход по БД,
  старые cookie не принимаются, отзыв срабатывает сразу.
- **Зависимости:** этап 1.
- **Проверки:**
  - `tests/test_portal_auth_accounts.py`: вход active, pending, rejected,
    blocked; неверный пароль; throttle 5/15 мин; `sv+1` при блокировке,
    сбросе и смене пароля даёт 401 на старой cookie; смена пароля выдаёт
    новую cookie; `must_change_password` пускает только к SELF; регистрация
    создаёт pending/viewer без разделов, лишние поля (`role`, `status`,
    `sections`, `staff_id`) дают 422 и в БД не попадают; 429 по лимитам;
    cookie v1 и cookie с другим секретом отвергаются; ответы и журнал без
    паролей и хешей;
  - `tests/test_ws_revocation.py`: блокировка закрывает открытый сокет
    `4401`; viewer получает `4403`; истёкший `exp` закрывается проверкой;
  - `tests/test_csrf_origin.py`: POST без Origin или с чужим Origin, без
    `X-Requested-With` и с `Sec-Fetch-Site: cross-site` получают 403;
    `/api/v1/worker/*` не затронут;
  - `tests/test_distributed_workers_*` — прежние зелёные, плюс
    «viewer портала из списка DW-viewer прав не имеет»;
  - атрибуция: решение viewer невозможно; admin без `staff_id` пишет
    пустого автора; подмена `reviewer`/`timestamp`/`author` в теле
    игнорируется.
- **Критерий готовности:** все тесты зелёные в обоих режимах; в режиме
  `env_users` прежние тесты `test_portal_auth.py` и `test_users.py` проходят
  без изменений.

### Этап 3. Политика маршрутов

- **Цель:** сервер сам не пускает viewer к запрещённому, ничего не зависит от
  интерфейса.
- **Файлы:**
  - новый `backend/app/core/access_policy.py`;
  - `main.py` (`FastAPI(dependencies=[Depends(enforce_route_policy)])`);
  - обработка `/docs`/`/openapi.json`/`/redoc`/`/static` в middleware
    (смонтированные приложения глобальной зависимости не получают).
- **Функции:**
  - `POLICY: dict[(method, path_template), Policy]` с явными записями по 5.1
    и 4.1;
  - `enforce_route_policy(request)` берёт `request.scope["route"].path`,
    неизвестное трактует как `ADMIN`;
  - ответ 403 — `{"code":"forbidden_section","section":…}` или
    `{"code":"admin_only"}`;
  - для разделов с объектным параметром (`/objects/{oid}/…`) — проверка, что
    объект существует (без ACL).
- **Поведение:** viewer получает 403 на всё вне allowlist, admin — как
  раньше.
- **Зависимости:** этап 2.
- **Проверки:**
  - `tests/test_access_policy_coverage.py`: у каждого из `app.routes` есть
    политика; снимок allowlist viewer совпадает с эталонным файлом (любое
    расширение видно в diff); ни один маршрут из списка «пишет/запускает»
    (эталон из раздела 5, ведётся в тесте) не получил `SECTION_*`;
  - `tests/test_access_matrix.py`: параметризованный обход всех маршрутов
    тремя клиентами — аноним, viewer без разделов, viewer со всеми
    разделами без скачивания, viewer с полным доступом, admin. Ожидание —
    401/403/не-403 по таблице. Обработчики подменяются заглушками, реальные
    сервисы не вызываются.
- **Критерий готовности:** покрытие 100 % маршрутов; матрица зелёная.

### Этап 4. Read-only двойники

- **Цель:** открытие разрешённого экрана не пишет и не запускает.
- **Файлы и функции:** таблица 5.3:
  - `stage_comparison/store.py: find_session(), find_pair()`;
  - `document_groups.list_groups(attach=)`;
  - `production_batch.status(repair=)`;
  - `human_contour.build_read_model(create=)`;
  - `version_comparison/jobs.py: matrix_view()`, `get_run(repair=)`;
  - `series.get(repair=)`;
  - `version_comparison_ai/jobs.py: get_job(repair=)`, `view.job_view(materialize=)`;
  - `md_report/runner.get(repair=)`;
  - `sheet_match.status(repair=)`;
  - `section_optimization_*` (`build_if_missing=`, `repair=`);
  - `missing_norms_service.get_missing_norms(reconcile=)`;
  - `group_service.load_groups(migrate=)`;
  - новые маршруты `GET /api/stage-comparison/objects/{oid}/session`,
    `…/sessions/{sid}/pair`, `GET /api/version-comparison/…/matrix-view`;
  - выбор режима в маршрутах по `ctx.role`.
- **Поведение:** для admin без изменений, для viewer — только чтение и
  состояния `not_prepared`/`stale`.
- **Зависимости:** этап 3 (маршруты должны быть в таблице).
- **Проверки:** `tests/test_viewer_no_side_effects.py`.
  - Временное хранилище с фикстурами: два объекта, сессия стадий с парой,
    прогон и серия сравнения версий, готовое ИИ-задание, MD-отчёт, снимок
    оптимизации, база знаний.
  - Для каждого маршрута allowlist запрос делается от имени viewer.
  - До и после сравниваются хеши, `mtime_ns` и список всех файлов дерева
    данных и sqlite.
  - Через `monkeypatch` запрещены `subprocess.Popen/run`,
    `asyncio.create_subprocess_exec`, `threading.Thread.start`,
    `ThreadPoolExecutor.submit` (кроме пула starlette и `to_thread`),
    `pipeline_manager.start_*` и `*_service.start*`: при вызове тест падает.
  - Отдельно: admin-путь по-прежнему «ремонтирует» статус (регрессия).
- **Критерий готовности:** ноль записей и ноль запусков на всём allowlist.

### Этап 5. Скачивания и архивы

- **Цель:** скачиваются только завершённые файлы нужного объекта, архивы не
  открывают лишнего, сборка не держит event loop.
- **Файлы и функции:** раздел 5.4:
  - `routers/export.py`;
  - `version_comparison/jobs.py: _report_job()`, `report_file()`;
  - `version_comparison_ai/jobs.py:764`;
  - `md_report/runner.py: _pdf_stage()`;
  - `common/atomic_json.py`;
  - `stage_comparison/store.py: _atomic_write_json()`;
  - `production_batch._write()`;
  - `routers/discussions.py: download_resolved_excel()`.
- **Поведение:**
  - ZIP собирается в потоке, во время обработки версии отдаётся 409;
  - у viewer в ZIP нет `discussions/`;
  - отчёты сравнения версий не отдаются наполовину записанными.
- **Зависимости:** этапы 3 и 6 (передача `object_id`).
- **Проверки:**
  - `tests/test_download_integrity.py`:
    - сборка отчёта с искусственной задержкой между блоками записи и
      параллельное скачивание: всегда либо 409, либо целый файл (zip
      открывается, хеш совпадает с итоговым);
    - 10 одновременных ZIP-пакетов разных версий: архивы не смешиваются,
      временные файлы удалены;
    - 3-й одновременный запрос пакета получает 429;
    - пакет во время «идущей обработки» (фикстура `pipeline_manager`)
      получает 409;
    - состав ZIP для admin и для viewer совпадает с эталонным списком;
    - кириллический шифр в `resolved/excel` не даёт 500;
  - `tests/test_export_event_loop.py`: во время сборки ZIP параллельный
    `GET /api/info` отвечает быстрее 200 мс (подмена генератора Excel
    задержкой 2 с).
- **Критерий готовности:** тесты зелёные, на стенде пакет на реальном
  документе совпадает по составу с эталоном.

### Этап 6. Привязка к объекту

- **Цель:** одинаковые `project_id`/`version_id` в разных объектах не
  смешиваются. ACL по объектам здесь не вводится.
- **Файлы и функции:**
  - `storage/read_canary.py: _resolve_doc_or_404()` — объект из
    `?object_id=`, иначе из привязанного ContextVar;
  - если объекта нет, а код встречается в нескольких объектах — 409
    `ambiguous_document`, первого совпадения больше нет;
  - то же для мест поиска без объекта из 1.6 — передавать новый
    `project_service.bound_object_id()` (чтение существующего ContextVar
    `_bound_object_id`, `project_service.py:55`);
  - кэш `document_service` по ключу `"oid::pid::vid"`;
  - `findings_service.get_all_summaries()` и
    `get_all_optimization_summaries()` — по привязанному объекту;
  - `GET /api/project-comparison/catalog`, `GET /api/stage-comparison/sessions`
    и `GET /api/version-comparison/objects` для viewer — проекция без путей;
  - `sessions/{sid}` для маршрутов viewer — проверка
    `sid ∈ scope.sessions_for_object(oid)`;
  - `?storage=legacy` — только admin;
  - `project_service.resolve_project_dir()` — отказ на `..` и абсолютные
    пути;
  - `routers/objects.py: switch_object()` — `ADMIN`;
  - фронт: `app.js` — помощник `withObject(url)` для `<img>`, `<iframe>`,
    `<a href>` и `window.open` (`app.js:97`, `:9183`, `:4768`, `:5201` и
    другие найденные `grep`'ом места); `switchObject()` по 6.3.
- **Поведение:** при отсутствии дубликатов (как сейчас) для пользователя
  ничего не меняется. При дубликате ответ правильный или явная ошибка, но не
  чужие данные.
- **Зависимости:** этап 2 (контекст); можно вести параллельно с этапом 4.
- **Проверки:** `tests/test_object_scoping_duplicates.py`:
  - временный `projects_v2` с двумя объектами, в каждом документ
    `ЭМ-1 v001` с разным содержимым замечаний и PDF;
  - карточка, замечания, блоки, картинка блока, страница, PDF, оптимизация,
    ZIP-пакет и экспорт с `X-Object-Id` A и B возвращают данные своего
    объекта;
  - запрос без объекта получает 409;
  - `<img>`-URL с `?object_id=` получает правильный объект;
  - сессия стадий объекта A по маршруту объекта B получает 404.
- **Критерий готовности:** тест зелёный; существующие тесты
  `test_current_object_per_request.py` и тесты read_canary без регрессий.

### Этап 7. Интерфейс

- **Цель:** viewer видит только разрешённое и не вызывает запрещённого;
  admin управляет пользователями на сайте.
- **Файлы:**
  - `frontend/login.html`, новые `frontend/register.html`,
    `frontend/admin-users.html`, `frontend/static/js/admin-users.js`;
  - `frontend/static/js/portal_auth.js`;
  - `frontend/index.html` (`{{portal_caps}}`, `v-if` по 6.3, пункт
    «Пользователи»);
  - `frontend/static/js/app.js`: `canAdmin`/`canView`/`canDownload`,
    маршрутизатор, опросы, WS, `scLoadObjects`/`scRefreshSession`,
    `scOpenPairRowResult`, `switchObject`, `pcReadOnlySources`, `withObject`;
  - `project-change-consolidation.js` (`makeReport`);
  - `version-comparison.js` (`loadMatrix`);
  - `version-comparison-projects.js` (`acts`);
  - `version-comparison-batch.js` (опрос);
  - остальные `version-comparison-*.js` (кнопки запуска);
  - `main.py: serve_spa()`, `serve_admin_users()`, `serve_register()`.
- **Поведение:** 6.1–6.3.
- **Зависимости:** этапы 2–4 (серверные коды и двойники).
- **Проверки:**
  - `frontend/tests/*.test.js` (vitest):
    - `makeReport` у viewer не вызывает POST и показывает сообщение;
    - `scRefreshSession` у viewer не шлёт POST;
    - доска при `!canAdmin` не автосохраняет;
    - `onclose` с `4401/4403` не переподключается;
    - `portal_auth.js` ставит заголовок для fetch и XHR;
  - `npm run lint` и `npm run typecheck` в своём объёме;
  - ручной сценарий в браузере (Playwright) на стенде: регистрация,
    одобрение, назначение двух разделов, вход viewer, обход всех разделов
    с журналом сетевых запросов. Ни одного не-GET, кроме `/api/auth/*`, и
    ни одного 403 на разрешённых экранах.
- **Критерий готовности:** у viewer в журнале сети при обходе разрешённых
  разделов нет изменяющих запросов; admin видит портал как раньше плюс
  страницу «Пользователи».

### Этап 8. Многопользовательская работа и измерение

- **Цель:** 15 читателей и длительная задача администратора не мешают друг
  другу. Подтверждается измерением, а не чтением кода.
- **Файлы и функции:**
  - вынести из event loop то, что держит его на путях viewer и на
    длительных admin-путях:
    - `routers/findings.py: run_kb_validation()` — `subprocess.run claude`
      на минуты прямо в `async def`;
    - `routers/stage_comparison.py` — undo, `text-ai-review`,
      `text-change-summary`, `high-level-project-changes`: захват
      `store._lock` (RLock) в event loop, пока поток держит тот же замок при
      сравнении;
    - `routers/blocks.py` — v2-ветки `image`, `region-image`, `analysis`,
      `blocks`;
    - `routers/projects.py: list_projects()`;
    - `GET /api/stage-comparison/objects` (rglob);
    - `routers/human_mapping.py: result_scope` (sha256 артефактов на каждом
      запросе) — кэш по `mtime_ns`;
    - `routers/audit.py:807` (`readlines` всего журнала);
  - `main.py`: размер пула anyio через
    `anyio.to_thread.current_default_thread_limiter().total_tokens =
    THREADPOOL_TOKENS` (по умолчанию 64);
  - отдельный `Semaphore` для рендера страниц, тайлов и миниатюр
    (по умолчанию 6), чтобы просмотр не выбирал все потоки;
  - новый `scripts/load/portal_viewers_load.py` (httpx async, не в CI).
- **Поведение:** без функциональных изменений.
- **Зависимости:** этапы 3–5.
- **Проверки:** см. 9.3, сценарий нагрузки.
- **Критерий готовности:** пороги 9.3 выполнены на стенде с копией данных.
  При невыполнении — разбор по профилю, и только затем вопрос о
  нескольких процессах (с предварительным устранением состояния в процессе
  из раздела 2).

### Этап 9. Миграция и переключение на проде

Выполняет инженер, по шагам раздела 8.

- **Зависимости:** этапы 0–8 выкачены в режиме `env_users`.
- **Критерий готовности:** все 8 сотрудников вошли по новым cookie; admin —
  только владелец; у 7 бывших экспертов роль viewer и ровно те разделы,
  что в утверждённом `mapping.json`; `auth_events` содержит импорт; прежние
  записи `decisions_log.json` и график не изменились (сверка хеша файла до и
  после); новое решение admin подписано его сотрудником; решение viewer
  отклоняется; healthcheck зелёный; подсистема исполнителей доступна только
  admin.

### Этап 10. Снятие старого контура

- **Цель:** один источник аккаунтов.
- **Действия:**
  - через 14 дней без отката инженер удаляет `PORTAL_AUTH_USERS` из env;
  - следующий релиз удаляет режим `env_users`, `_parse_users()`,
    `backend/scripts/hash_portal_password.py` и чтение `users.json → role`
    в правах;
  - `docs/portal_auth.md` переписывается под новую модель, ссылка
    добавляется в `docs/INDEX.md` и `CLAUDE.md`.
- **Критерий готовности:** `grep PORTAL_AUTH_USERS` по коду пуст (кроме
  истории в docs).

---

## 8. Миграция и откат

### 8.1. Резервное копирование (перед этапом 9)

- `sqlite3 .backup` новой БД (если создана заранее), копии `users.json`,
  `objects.json`, `knowledge_base/decisions_log.json`,
  `stage_comparison_saved_config.json`, `schedule`-файлов в
  `/home/coder/auditmanager/backups/portal-access-<дата>/`.
- Копия `/etc/auditmanager/backend.phaseb.env` (root, инженер).
- БД подсистемы исполнителей не трогается.
- После переключения — ежедневный `python backend/scripts/portal_accounts.py
  backup` (cron, хранить 30 копий). Это единственная новая регулярная задача:
  аккаунты нельзя восстановить из других данных.

### 8.2. Таблица сопоставления

- Перенос хешей — `import-legacy --dry-run`. Его запускает инженер, потому
  что строка с хешами доступна только root.

  ```bash
  sudo grep '^PORTAL_AUTH_USERS=' /etc/auditmanager/backend.phaseb.env \
    | sudo -u coder /home/coder/auditmanager/current/venv/bin/python \
        backend/scripts/portal_accounts.py import-legacy --stdin --dry-run \
        --db /srv/auditmanager/data/backend/app/data/portal_accounts.sqlite3
  ```

  Строка уходит через stdin и не попадает в argv и историю shell. Скрипт
  выводит таблицу без хешей:
  `legacy_login | login_norm | staff_id (по users.json login/id) | ФИО | предложенная роль | разделы`.
  Роль предлагается только из явного файла `mapping.json`, никаких догадок
  по имени.
- Инженер правит `mapping.json`: для каждого логина роль, `staff_id` и
  разделы. Файл кладётся рядом с бэкапом. Администратор назначается явно
  (`"role":"admin"` у конкретного логина). Первый зарегистрировавшийся
  администратором не становится, а аккаунтов из `users.json` без логина в
  env скрипт не создаёт.
- Запуск без `--dry-run` с `--mapping mapping.json`:
  - перенос хеша как есть;
  - `legacy_login` заполняется;
  - статус `active`;
  - событие `legacy_import` в `auth_events`.
- **Отказ всего импорта, если:**
  - `login_norm` двух записей совпадает;
  - `staff_id` занят;
  - в mapping нет ни одного admin;
  - хеш не распознан.

  На сегодня, по подсчёту из 1.1, ни одного конфликта нет.

### 8.3. Переключение

1. **Выкатить релиз** с этапами 0–8 (режим `env_users`).
2. **Импорт** (8.2). Проверить, что `portal_accounts.py list` показывает
   ожидаемую таблицу.
3. **Инженер в env:**
   - `PORTAL_AUTH_MODE=accounts`;
   - `PORTAL_SESSION_COOKIE_NAME=am_session` (старые cookie просто не
     читаются);
   - новый `PORTAL_SESSION_SECRET`;
   - `PORTAL_COOKIE_SECURE=true`;
   - `PORTAL_REGISTRATION_ENABLED=false`.

   Перезапуск — разрешённым `sudo -n systemctl restart`, перед этим проверить
   отсутствие идущих аудитов, как в CLAUDE.md.
4. **Все входят заново.** Сотрудников предупредить заранее: старые сессии
   завершатся.
5. **Проверки после переключения:**
   - вход каждого сотрудника;
   - экспертное решение подписывается тем же именем;
   - график работ показывает тех же людей;
   - `/audit-workers` открывается admin;
   - `/api/info` отвечает 200;
   - watchdog молчит.
6. **Включить регистрацию** `PORTAL_REGISTRATION_ENABLED=true` (следующий
   рестарт). Новые пользователи появляются только отсюда.

### 8.4. Старые сессии

- Cookie v1 (`{"u":…}`) в режиме `accounts` не принимаются.
- Смена имени cookie и секрета гарантирует, что ни одна старая подпись не
  сработает даже при ошибке в разборе.
- Переходного периода с двумя форматами нет: при 8 сотрудниках один
  повторный вход проще и безопаснее.

### 8.5. Сохранность БД между релизами

- БД лежит в `APP_DATA_DIR` (`/srv/auditmanager/data/backend/app/data`) —
  вне `releases/`, запись разрешена существующим `ReadWritePaths`. Новый
  drop-in не нужен.
- При старте в режиме `accounts` без БД или без активного admin служба не
  падает (watchdog не должен зацикливаться):
  - `/api/info` отвечает;
  - вход отвечает 503 «Нет активного администратора. Обратитесь к инженеру»;
  - это же пишется в журнал.

  Автоматического создания администратора нет.
- `build_center_release.py` и `deploy_center_release.py` БД не копируют и не
  трогают.

### 8.6. Откат (поправка 0.6)

Откат не должен возвращать отозванные cookie, старые пароли, доступ
заблокированных и прежние права viewer. Поэтому возврат к прежнему релизу
(до этапа 0) и к прежнему `PORTAL_SESSION_SECRET` **запрещён**.

- **Точка отката по коду** — релиз этой ветки (или релиз только с этапом 0,
  если сломан сам код аккаунтов). Оба содержат исправления этапа 0.
- **Откат при сбое модуля аккаунтов без смены релиза:** аварийный режим
  `PORTAL_AUTH_MODE=emergency_owner`.
  - Вход разрешён ровно одному логину из `PORTAL_EMERGENCY_OWNER`
    (`login:hash`, новый пароль владельца, хеш делает
    `backend/scripts/hash_portal_password.py`). Строка `PORTAL_AUTH_USERS` в
    этом режиме не читается.
  - Требуются новый `PORTAL_SESSION_SECRET` (не равный ни одному прежнему) и
    новое имя cookie `PORTAL_SESSION_COOKIE_NAME` (например `am_emergency`):
    ни одна выданная ранее cookie не проходит проверку.
  - Регистрация, вход остальных, `/admin/users` и API аккаунтов закрыты
    (503 `emergency_mode`). Владелец имеет права admin.
  - БД аккаунтов не изменяется; после исправления — обратно
    `PORTAL_AUTH_MODE=accounts` с ещё одним новым секретом (все входят
    заново, блокировки и права в БД сохранены).
- **Если сломан и аварийный режим** — релиз только с этапом 0 в режиме
  `env_users`, где `PORTAL_AUTH_USERS` содержит **только** владельца с новым
  хешем, новый секрет и новое имя cookie. В коде этапа 0 регистрации нет, а
  других логинов в строке нет — значит, никто, кроме владельца, не войдёт.
- **Чего не делать:** возвращать прежнюю строку `PORTAL_AUTH_USERS` с восемью
  логинами, прежний секрет или релиз до этапа 0.
- **Проверка** (выполнена на копии данных, см. отчёт о реализации):
  `tests/test_portal_rollback.py` — cookie, выданная в `accounts`, не
  принимается в `emergency_owner`; заблокированный аккаунт не входит;
  старый пароль из legacy-строки не действует; после возврата в `accounts`
  с новым секретом старые cookie отвергаются, права viewer сохранены.

---

## 9. Проверки

### 9.1. Автоматические тесты (временные данные, без реальных моделей)

| № | Сценарий | Тест |
|---|---|---|
| 1 | Регистрация создаёт только pending/viewer, без разделов; лишние поля отвергаются | `test_portal_auth_accounts.py::test_register_*` |
| 2 | pending, rejected и blocked не получают сессию и данные; отозванная cookie даёт 401 | `test_portal_auth_accounts.py::test_status_*`, `test_access_matrix.py` |
| 3 | Admin одобряет, отклоняет, меняет права и роль через API страницы; последний admin защищён | `test_accounts_admin_api.py` |
| 4 | Viewer открывает и скачивает разрешённое, скачивание без просмотра невозможно | `test_access_matrix.py`, `test_portal_accounts_store.py::test_download_requires_view` |
| 5 | Прямые API, ссылки на файлы и WS не обходят политику | `test_access_policy_coverage.py`, `test_access_matrix.py`, `test_ws_revocation.py`, `test_export_download_guard.py` |
| 6 | Viewer ничего не запускает и не меняет ни в одном разделе | `test_access_matrix.py` (все не-allowlist маршруты дают 403 до вызова обработчика) |
| 7 | Открытие разрешённого экрана не создаёт задач и записей | `test_viewer_no_side_effects.py` |
| 8 | Отзыв прав и блокировка действуют на открытые сессии и сокеты | `test_portal_auth_accounts.py::test_revoke_*`, `test_ws_revocation.py` |
| 9 | Два объекта с одинаковыми идентификаторами не смешиваются | `test_object_scoping_duplicates.py` |
| 10 | Параллельные скачивания не отдают неполных или чужих файлов | `test_download_integrity.py` |
| 11 | 15 читателей одновременно с длительной задачей admin | нагрузочный сценарий 9.3 (стенд) и `test_export_event_loop.py` (CI) |
| 12 | Сохраняются сотрудники, история решений, админ-доступ, машинный контур, healthcheck | `test_users.py`, `test_schedule*.py`, тесты `distributed_workers` и `audit_worker_agent`, `test_portal_auth.py::test_api_info_*`, новый `test_attribution_continuity.py` (исторические алиасы `employee_identity` продолжают совпадать) |

Все тесты используют `tmp_path`, временную SQLite и фикстуры хранилища.
Длительная задача admin имитируется заглушкой этапа (поток с `sleep` и
нагрузкой CPU), модели и Codex не вызываются. Запуск — двумя корнями с
`--continue-on-collection-errors` и регресс-гейт
`scripts/ci_regression_gate.py`.

### 9.2. Ручная проверка на стенде

- Стенд с копией данных (не прод), режим `accounts`.
- Playwright: регистрация, одобрение, разделы, вход viewer, обход всех
  разделов.
- Журнал сети без изменяющих запросов.
- Попытки прямых URL к запрещённым маршрутам и файлам.
- Блокировка в момент открытого экрана: следующий запрос даёт выход на
  `/login`.

### 9.3. Сценарий измерения производительности

Скрипт `scripts/load/portal_viewers_load.py` (этап 8), стенд с копией данных:

- 15 независимых виртуальных пользователей, у каждого свой аккаунт и своя
  cookie, у части — разные объекты в `X-Object-Id`. Сценарий каждого с
  паузами 1–5 с в течение 15 минут:
  1. вход;
  2. список проектов;
  3. карточка и замечания;
  4. 20 картинок блоков и 10 страниц документа;
  5. матрица сравнения версий (готовая) и 30 тайлов;
  6. пара сравнения стадий и 20 `page-svg`;
  7. скачивание одного ZIP-пакета и одного отчёта.
- Параллельно admin запускает длительную задачу. Сначала — заглушка этапа в
  потоке на 10 минут с нагрузкой CPU и блокировкой `store._lock`.
  Настоящие модели и анализ в измерении не участвуют (поправка 0.7).
- Отдельный зонд раз в 0,5 с меряет задержку event loop (время ответа
  `GET /api/info`).
- **Метрики:** p50, p95 и p99 по маршрутам; доля ошибок; задержка цикла;
  занятость пулов (`to_thread` и anyio); RSS процесса; число открытых
  файлов.
- **Предлагаемые пороги приёмки** (утверждаются инженером):
  - JSON-чтения p95 < 1,5 с;
  - картинки и тайлы p95 < 3 с;
  - ZIP-пакет < 30 с;
  - 0 ответов 5xx;
  - задержка цикла p99 < 200 мс;
  - ни одного 409 и неполного файла у скачиваний, кроме случаев, когда
    admin пересобирает именно этот отчёт.

---

## 10. Допущения и блокирующие вопросы

### 10.1. Роли текущих сотрудников — решено (вариант Б)

**Факт.** За последние 30 дней в `decisions_log.json` записано 12 606
экспертных решений, все — от 6 сотрудников с ролью `expert`.

**Решение владельца (2026-10-19):** вариант Б. Admin — только явно
подтверждённый аккаунт владельца; остальные 7 — viewer с явно назначенными
разделами. Экспертные решения, сопоставления и запуски после перехода делает
admin. История решений и график работ сохраняются без переподписи; бывшие
эксперты видят разрешённые разделы. Варианты А и В отклонены.

Следствие, о котором владелец предупреждён: новые записи в «Графике работ»
появляются только от действий admin.

### 10.2. Допущения (не блокируют, при несогласии меняются точечно)

- **Д-1. Ограничений по объектам, дисциплинам и шифрам нет:** viewer с
  правом на раздел видит все объекты. Привязка к объекту при этом
  исправляется (этап 6), так что позже объектная ACL добавляется как ещё
  одна таблица и проверка в `enforce_route_policy`.
- **Д-2. Исходный PDF доступен при праве просмотра «Результатов аудита»:**
  его показывает встроенный просмотрщик.
- **Д-3. viewer не видит обсуждения** (история чатов с моделью), промпты,
  `llm-text`, пакеты, отправленные модели (`packet`, `check-crop`), журналы
  аудита, расход и деньги. В ZIP-пакете viewer нет `discussions/`.
- **Д-4. Восстановления пароля по почте нет** (SMTP не настроен): сброс
  делает администратор временным паролем.
- **Д-5. Флаг «скачивание» в «Базе знаний» и «Графике работ»** отключён в
  интерфейсе с пояснением «в разделе нет выгрузок», сервер его не принимает
  (поправка 0.5).
- **Д-6. viewer не пользуется WebSocket.** Живые журналы аудита ему не нужны.
- **Д-7. Процесс backend остаётся один** до результатов этапа 8. Внешние
  сервисы (Redis, IdP, отдельная БД) не вводятся.
- **Д-8. DW-viewer субъекты (5 человек) после переключения экран
  исполнителей не видят:** их портальная роль — viewer, а подсистема
  доступна только admin (поправка 0.1).
- **Д-9. Ключи очередей конвейера по голому `project_id`** (`active_jobs`,
  batch) остаются: это админский контур. Для viewer статусы идут из
  `GET /api/projects` и защищены от неоднозначности 409 (этап 6).
  Переделка ключей — отдельная задача.
- **Д-10. Занятый логин при регистрации сообщается явно** (6.1).

## 11. Состояние реализации (2026-10-19)

Этапы 0–8 сделаны в ветке `feature/portal-access` (база `268b1eea`).
Этапы 9–10 (переключение прода и снятие старого контура) выполняет владелец
по `docs/portal_access_migration.md`.

### 11.1. Нагрузка (этап 8, стенд на срезе данных, без моделей)

Стенд — `scripts/load/`:

* срез данных в отдельный каталог, абсолютные пути переписаны на стенд;
* отдельная база аккаунтов, свободный порт;
* один процесс uvicorn, как на проде;
* 15 viewer с разными cookie и объектами, скачивание пакета у каждого раза в
  несколько циклов;
* заглушка задачи admin.

| Показатель | Без задачи admin | С задачей admin |
|---|---|---|
| Запросов за ~300 с | 9 451 (31,5/с) | 3 237 (10,8/с) |
| 5xx / исключения | 0 / 1 (`ReadError` клиента) | 0 / 0 |
| Задержка цикла событий p50 / p99 / max | 0,03 / 0,54 / 0,73 с | 0,39 / 1,82 / 2,53 с |
| Просмотр (findings, page, sheet, svg) p95 | 0,2–0,8 с | 1,6–2,2 с |
| Плитки листа p95 | 1,35 с | 4,16 с |
| Пакет аудита p50 / p95 | 1,0 / 16,4 с (7 из 68 — 429) | 4,1 / 32,5 с |
| RSS процесса max | 1 568 МБ | 1 597 МБ |

* 404 по PDF и блокам — объект 213, у которого PDF и кропы утрачены и в
  исходных данных.
* 400 по плитке листа — листы без результата сравнения (добавленные или
  удалённые); на базовом коммите так же.

Заглушка admin пессимистична. Поток на чистом Python держит GIL и
`store._lock` сравнения стадий 75 % времени, ещё один поток хеширует, 4 потока
пула заняты весь прогон. Замедление цикла событий (он замка не берёт) —
конкуренция за GIL, а не нехватка потоков. Поэтому семафор на рендер не
введён: он бы не помог. Реальные стадии с моделями идут подпроцессами (codex,
claude) и GIL процесса не держат. Если чтение под тяжёлой задачей admin
окажется медленным на проде, вынести пересчёты сравнения стадий в отдельный
процесс — отдельная задача.

### 11.2. Отклонения от плана и оставшиеся ограничения

* viewer в данных сессии сравнения стадий ещё видит абсолютные пути PDF
  (`pdf_path` в парах). В списке объектов пути убраны. Файлы viewer получает
  только через проверенные маршруты.
* Ветка выгрузки пакета для legacy-хранилища (не `projects_v2`) не
  переписана: она доступна только admin (`storage=legacy` — admin).
* Ключи очередей конвейера по голому `project_id` — как Д-9.
* Один процесс backend (Д-7); сессии и WebSocket проверяются по базе, поэтому
  второй процесс не потребует переделки, но не измерялся.

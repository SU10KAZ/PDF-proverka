# Журнал действий (action log)

Сквозная летопись «кто, что, когда сделал и чем закончилось» для последующего
разбора ошибок: какие действия предшествовали сбою, какие запросы падали, какие
этапы конвейера рушились и почему, когда рестартовал сервер.

## Что пишется

| kind | Источник | Содержимое |
|------|----------|------------|
| `api` | `ActionLogMiddleware` (`backend/app/core/action_log.py`) | каждый HTTP-запрос: `actor` (логин портала из session-cookie), `method`, `path`, `route`, `query`, `project_id` (из path-параметров), `status`, `dur_ms`, `ip`; при исключении — `error` + `traceback` |
| `pipeline` | хук в `audit_logger.update_pipeline_log()` — единая воронка всех stage-статусов (manager, stage runner'ы через ctx, prepare_service) | `project_id`, `stage`, `status` (running/done/partial/skipped/error/interrupted), `message`, `error`, `duration_sec` |
| `app_log` | мост `install_logging_bridge()` на root-логгере | WARNING/ERROR/CRITICAL из **всех** модулей backend: `level`, `logger`, `message`, `exc` |
| `system` | lifespan в `main.py` | `startup` / `shutdown` — по ним видны рестарты и падения сервера |

## Шум-фильтр HTTP

Поллинговые GET фронта (live-status, `*/status`, `/api/usage/*`, картинки
страниц/блоков, статусы job'ов и т.п.) **не пишутся**, иначе журнал распухает
на мегабайты в час. Правила:

- фильтр применяется ТОЛЬКО к успешным GET/HEAD/OPTIONS (<400);
- любой POST/PUT/PATCH/DELETE пишется всегда;
- любой ответ >=400 и любое исключение пишутся всегда (даже на шумовом пути);
- встроенный список — `_NOISE_PATTERNS` в `action_log.py`; расширение без
  правки кода — env `ACTION_LOG_NOISE_EXTRA` (CSV из regex).

## Хранилище

Суточные append-only JSONL: `DATA_DIR/logs/actions/actions-YYYY-MM-DD.jsonl`
(в prod данные прибиты к MAIN через `AUDIT_DATA_DIR`, поэтому журнал живёт в
`logs/actions/` рядом с `server.log` независимо от deploy-worktree кода).
Одна строка = одно событие, `ts` — локальное время с таймзоной. Файлы старше
`ACTION_LOG_RETENTION_DAYS` (default 180) удаляются при первой записи нового
дня. Запись потокобезопасна (lock) и fail-soft: ошибка журнала никогда не
ломает запрос/конвейер (однократное предупреждение в stderr).

## Как читать

```bash
# Сводка за 7 дней: объёмы, ошибки, активность инженеров, топ путей
python scripts/analyze_action_log.py

# Только ошибки за 3 дня
python scripts/analyze_action_log.py --errors --days 3 --limit 50

# Действия конкретного инженера / поиск
python scripts/analyze_action_log.py --user andrey
python scripts/analyze_action_log.py --kind pipeline --q block_analysis
```

REST (за portal-auth):

- `GET /api/action-log?date_from=&date_to=&kind=&actor=&q=&errors_only=&limit=&offset=`
  — события новые→старые; в ответе `persons` — маппинг логин→ФИО из users.json;
- `GET /api/action-log/stats?days=7` — сводка по дням.

Чтение файлов — в `asyncio.to_thread` (не блокирует loop → не дразнит watchdog).

## Флаги

| Env | Default | Что делает |
|-----|---------|-----------|
| `ACTION_LOG_ENABLED` | `true` | мастер-выключатель |
| `ACTION_LOG_HTTP_ENABLED` | `true` | контур HTTP-запросов |
| `ACTION_LOG_PIPELINE_ENABLED` | `true` | контур этапов конвейера |
| `ACTION_LOG_APPLOG_ENABLED` | `true` | мост logging (WARNING+) |
| `ACTION_LOG_RETENTION_DAYS` | `180` | глубина хранения в днях |
| `AUDIT_ACTION_LOG_DIR` | `DATA_DIR/logs/actions` | путь к журналу |
| `ACTION_LOG_NOISE_EXTRA` | `[]` | доп. шумовые regex (CSV) |
| `ACTION_LOG_MAX_DAY_BYTES` | `256 МБ` | потолок суточного файла: сверх — дроп до следующего дня + маркер `day_cap_reached` (защита диска от штормов) |
| `ACTION_LOG_APPLOG_MAX_PER_MIN` | `600` | потолок app_log-событий в минуту: сверх — дроп + агрегат о числе подавленных |

## Реализация и гарантии

- `backend/app/core/action_log.py` — ядро: писатель, шум-фильтр, pure-ASGI
  middleware (НЕ BaseHTTPMiddleware — не трогает тело запроса/ответа, безопасен
  для стриминга `chat/stream` и загрузок), мост logging, чтение.
- Middleware добавлен ПОСЛЕДНИМ в `main.py` → внешний: видит 401 от PortalAuth
  (неавторизованные попытки — тоже действия). Событие пишется после полного
  ответа (известны статус и длительность).
- Мост logging добавляет к root ещё и `StreamHandler(stderr, WARNING)`:
  добавление хендлера отключает `logging.lastResort`, а прежнее поведение
  (WARNING+ → stderr → `server.err.log`) должно сохраниться.
- Тело запроса НЕ логируется (пароль из `POST /api/auth/login` не попадает в
  журнал; плюс никакого риска для стриминга/загрузок).
- Писатель самолечится: если `logs/actions/` удалили посреди дня, следующая
  запись пересоздаёт директорию (сброс кэш-ключа в except).
- На shutdown lifespan вызывает `uninstall_logging_bridge()` — мост не
  переживает `with TestClient(app)` в тестах.
- Чтение потоковое (`deque`, память O(limit), `errors="replace"` против битого
  UTF-8); `stats(days=N)` — N календарных дней; кривые `date_from/date_to` → 422.
- CLI экранирует управляющие символы недоверенных полей (`\x1b` из path не
  исполняется терминалом).
- Тестовая изоляция двухуровневая: process-lifetime песочница
  `AUDIT_ACTION_LOG_DIR` в module-level conftest (базовое значение никогда не
  прод) + autouse-фикстуры `_isolate_action_log` per-test.
- Тесты: `backend/tests/test_action_log.py`, `tests/test_action_log_api.py`.

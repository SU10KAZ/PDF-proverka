# Portal Auth — простая защита портала логином/паролем

**Дата:** 2026-05-28

Лёгкая аутентификация веб-портала для 3-4 сотрудников. Без БД пользователей,
ролей, регистрации, восстановления пароля и внешних auth-провайдеров.

По умолчанию **выключена** — портал работает как раньше. Включается одним
флагом в `.env`, без изменения кода.

## Как это работает

* **Хеши паролей** — `pbkdf2_sha256` (passlib, чистый Python). Пароли в открытом
  виде нигде не хранятся и не логируются.
* **Сессия** — self-contained подписанный cookie (HMAC-SHA256, stdlib). Без
  серверного стораджа. Cookie: `HttpOnly`, `SameSite=Lax`, `Secure` (по схеме
  запроса), срок жизни — `PORTAL_SESSION_TTL_HOURS` (default 24 ч).
* **Middleware** (`PortalAuthMiddleware`) блокирует всё, кроме exempt-списка,
  пока пользователь не вошёл:
  * `/api/...` без сессии → `401 Unauthorized`;
  * HTML-страницы без сессии → редирект на `/login`;
  * WebSocket (`/ws/...`) проверяет cookie в самом обработчике и закрывает
    соединение с кодом 1008, если сессии нет.
* Swagger/OpenAPI (`/docs`, `/openapi.json`, `/redoc`) и `/static/*` тоже
  закрыты — отдаются только после входа.

### Exempt-пути (доступны без входа)

| Путь | Зачем |
|---|---|
| `/login` | страница входа (self-contained, без внешних ассетов) |
| `/api/auth/login` | проверка логина/пароля |
| `/api/auth/logout` | выход (idempotent) |
| `/api/auth/me` | статус сессии (фронт опрашивает до входа) |
| `/api/info` | healthcheck, который дёргает cron-watchdog (`curl -f`) |
| `/favicon.ico` | иконка вкладки на странице входа |

> `/api/info` оставлен открытым намеренно: `~/bin/webapp-watchdog.sh` каждую
> минуту делает `curl -fsS http://127.0.0.1:8081/api/info`; если закрыть его
> под auth — watchdog решит, что портал упал, и будет рестартить процесс в
> цикле.

## Endpoints

| Метод | Путь | Назначение |
|---|---|---|
| `GET` | `/login` | страница входа (редирект на `/`, если auth выключен или уже вошёл) |
| `POST` | `/api/auth/login` | `{username, password}` → ставит cookie, `{authenticated, username}` |
| `POST` | `/api/auth/logout` | очищает cookie, `{ok:true}` |
| `GET` | `/api/auth/me` | `{authenticated, username, auth_enabled}` |

## Включить auth (production)

1. Сгенерировать хеши паролей для каждого сотрудника:
   ```bash
   python backend/scripts/hash_portal_password.py --user ivan
   # Пароль: ********  (вводится скрытно, не попадает в историю shell)
   # → ivan:$pbkdf2-sha256$29000$...
   ```
2. Сгенерировать секрет подписи cookie:
   ```bash
   python -c "import secrets; print(secrets.token_urlsafe(48))"
   ```
3. Прописать в `.env` (значение `PORTAL_AUTH_USERS` — **без кавычек**, одной
   строкой; см. раздел «Кавычки и `$` в .env» ниже):
   ```env
   PORTAL_AUTH_ENABLED=true
   PORTAL_AUTH_USERS=ivan:$pbkdf2-sha256$29000$...,petr:$pbkdf2-sha256$29000$...,olga:$pbkdf2-sha256$29000$...
   PORTAL_SESSION_SECRET=<вывод token_urlsafe>
   PORTAL_SESSION_TTL_HOURS=24
   PORTAL_COOKIE_SECURE=auto
   ```
4. Перезапустить backend:
   ```bash
   pkill -f "uvicorn backend.app.main"
   uvicorn backend.app.main:app --host 0.0.0.0 --port 8081 --reload &
   ```

## Добавить нового сотрудника

```bash
python backend/scripts/hash_portal_password.py --user newname
```
Допишите `,newname:$pbkdf2-sha256$...` в конец `PORTAL_AUTH_USERS` и
перезапустите backend. Удаление сотрудника — убрать его запись из строки
(его действующие сессии перестают проходить проверку, т.к. `verify_token`
сверяется со списком логинов).

## Отключить auth (локальная разработка)

```env
PORTAL_AUTH_ENABLED=false
```
Либо просто не задавать переменную. Middleware становится no-op, кнопка «Выйти»
в сайдбаре скрыта, поведение портала идентично сборке без этой фичи.

## Кавычки и `$` в .env

Значение `PORTAL_AUTH_USERS` пишите **без кавычек**, одной строкой:

```env
PORTAL_AUTH_USERS=igor:$pbkdf2-sha256$...,alexey:$pbkdf2-sha256$...,andrey:$pbkdf2-sha256$...
```

Причина — кастомный загрузчик `.env` в `backend/app/main.py`: он делает
`os.environ.setdefault(k.strip(), v.strip())`, то есть **не снимает обрамляющие
кавычки** и **не интерполирует `$`**, и срабатывает раньше `load_dotenv()`
(а тот с `override=False` уже не перезаписывает значение). Поэтому:

* `$` в pbkdf2-хеше безопасен и без кавычек — интерполяции `$VAR` не происходит;
* одинарные кавычки **сломают** парсинг: ведущая `'` прилипнет к первому логину
  (`'igor` вместо `igor`), а хвостовая `'` — к хешу последнего пользователя,
  и такие входы будут молча отклоняться.

## Безопасность — что покрыто

* пароли только в виде хешей; login endpoint не логирует пароль;
* cookie `HttpOnly` + `SameSite=Lax` + `Secure` (на https);
* прямые запросы к `/api/...` без сессии → `401`;
* подпись cookie HMAC-SHA256 — подделать токен без секрета нельзя;
* истёкшие/удалённые пользователи отклоняются (`verify_token` сверяет exp и
  наличие логина в конфиге).

## Связанные файлы

* [backend/app/core/portal_auth.py](../backend/app/core/portal_auth.py) — хеши, подписанный cookie, middleware
* [backend/app/api/routers/auth.py](../backend/app/api/routers/auth.py) — login/logout/me
* [backend/scripts/hash_portal_password.py](../backend/scripts/hash_portal_password.py) — генератор хеша
* [frontend/login.html](../frontend/login.html) — self-contained страница входа
* [frontend/static/js/portal_auth.js](../frontend/static/js/portal_auth.js) — 401-интерсептор + logout
* [tests/test_portal_auth.py](../tests/test_portal_auth.py) — тесты

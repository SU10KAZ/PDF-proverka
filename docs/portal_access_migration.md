# Доступ к порталу: перенос, миграция, выкладка, откат

Инструкция к ветке `feature/portal-access` (план — `docs/portal_access_plan.md`).
Выполняет владелец/инженер. Всё, что меняет прод (push, выкладка, правка
`/etc/auditmanager/*.env`, рестарт службы, база аккаунтов на проде), — только
вручную по этой инструкции.

## 0. Что получится

* **Режим `env_users` (после выкладки, до миграции).** Вход по прежней строке
  `PORTAL_AUTH_USERS`, сотрудники работают как сейчас. Действуют исправления
  этапа 0:
  * изменение сотрудников, расход и деньги, журнал действий, учётная запись
    модели — только сотрудник с `users.json role=admin`;
  * download экспорта больше не отдаёт файлы релиза;
  * экспертные решения подписывает сервер; повторное «Сохранить» больше не
    переподписывает чужие решения.
* **Режим `accounts` (после миграции).** Аккаунты в
  `APP_DATA_DIR/portal_accounts.sqlite3`.
  * admin — только владелец; 7 сотрудников — viewer с назначенными разделами.
  * Регистрация создаёт pending/viewer без разделов.
  * Права проверяются сервером на каждом запросе; отзыв действует сразу, в том
    числе для открытых WebSocket.
* **Режим `emergency_owner`** — аварийный: входит только владелец с новым
  паролем.

## 1. Перенос в `main`

```bash
cd /home/coder/projects/PDF-proverka
git status                      # убедиться, что в main нет чужой незакоммиченной работы
git log --oneline -1 main       # должен совпадать с commit, проверенным в отчёте
git merge --no-ff feature/portal-access      # или: git rebase/cherry-pick по вкусу
python -m pytest tests backend/tests --continue-on-collection-errors -m "not slow" -q
cd frontend && npm test && npm run lint && npm run typecheck && cd ..
```

Новых падений против эталона быть не должно. Давний долг —
`scripts/ci_known_failures.txt`. Сверка эталона на чистом базовом коммите
описана в отчёте о реализации.

Конфликты возможны в `frontend/static/js/app.js`, `frontend/index.html` и
`backend/app/api/routers/stage_comparison.py`, если в `main` их правили
параллельно. Разрешать в пользу обеих правок. Затем повторить тесты и
`tests/test_access_policy_coverage.py`. Если новый маршрут `main` не попал в
таблицу политик, этот тест упадёт: новый маршрут нужно явно отнести к
viewer-перечню или оставить admin.

## 2. Выкладка кода (режим `env_users`)

Порядок из `CLAUDE.md` («Выкладка на живой портал»):

1. push;
2. сборка релиза;
3. проверка, что не идут аудиты;
4. `--dry-run`;
5. deploy.

Переменную `PORTAL_AUTH_MODE` пока НЕ задавать: значение по умолчанию —
`env_users`.

Проверка после выкладки:

* вход сотрудников по старым паролям;
* экспертное решение сохраняется с прежним автором;
* «График работ» на месте;
* `/api/info` анониму отвечает `{"app","version","status"}`;
* `curl -s -o /dev/null -w '%{http_code}' https://…/api/export/download/.mcp.json`
  с cookie сотрудника даёт `404`;
* панель расхода и журнал у сотрудника без `role=admin` возвращают 403.

`passlib==1.7.4` теперь в `requirements.txt`. В venv-доноре он уже есть.

## 3. Миграция аккаунтов (режим `accounts`)

Выполнять в окне, когда не идут аудиты (`GET /api/audit/batch/status`,
`pstree` по PID службы), и только после шага 2: CLI
`backend/scripts/portal_accounts.py` появляется в релизе с этой веткой.

Служба читает три env-файла (`backend.secrets.env`, `backend.phaseb.env`,
`migration.env`; у всех права root 0600). Ниже `$ENVF` — тот файл, где сейчас
лежит `PORTAL_AUTH_USERS`. Переменные `PORTAL_*` правятся там же.

```bash
REL=/home/coder/auditmanager/current
DB=/srv/auditmanager/data/backend/app/data/portal_accounts.sqlite3   # = APP_DATA_DIR службы
PY="$REL/venv/bin/python"; CLI="$REL/app/backend/scripts/portal_accounts.py"
BK=/home/coder/auditmanager/backups/portal-access-$(date +%Y%m%d-%H%M)
mkdir -p "$BK"
ENVF=$(sudo grep -l '^PORTAL_AUTH_USERS=' /etc/auditmanager/*.env); echo "$ENVF"   # ровно один файл

# 3.1 Резервные копии (данные — без секретов; env — root, sudo)
cp -a /srv/auditmanager/data/backend/app/data/users.json "$BK/"
cp -a /srv/auditmanager/data/backend/app/data/objects.json "$BK/"
cp -a /srv/auditmanager/data/knowledge_base/decisions_log.json "$BK/"
sha256sum "$BK"/*.json > "$BK/SHA256SUMS"
sudo cp -a "$ENVF" "$BK/$(basename "$ENVF").root"     # копия остаётся root 0600

# 3.2 Шаблон сопоставления: роли НЕ угадываются (role пустой у всех)
sudo grep '^PORTAL_AUTH_USERS=' "$ENVF" \
  | "$PY" "$CLI" --db "$DB" propose-mapping --stdin > "$BK/mapping.json"
```

**3.3. Заполнить `$BK/mapping.json` вручную:**

* у своего логина — `"role": "admin"`;
* у остальных — `"role": "viewer"` и `"sections"`, например
  `{"audit_results": {"view": true, "download": true}, "version_comparison":
  {"view": true, "download": true}, "stage_comparison": {"view": true,
  "download": true}, "knowledge_base": {"view": true}, "schedule": {"view":
  true}}`;
* `staff_id` уже подставлен по `users.json`, проверить его.

Скачивание в «Базе знаний» и «Графике работ» не назначается: в этих
разделах нет выгрузок, сервер такое право отклонит.

```bash
# 3.4 Пробный импорт: любой конфликт отменяет весь импорт
sudo grep '^PORTAL_AUTH_USERS=' "$ENVF" \
  | "$PY" "$CLI" --db "$DB" import-legacy --stdin --mapping "$BK/mapping.json" --dry-run
# 3.5 Импорт (хеши переносятся как есть; при первом входе — перехеширование)
sudo grep '^PORTAL_AUTH_USERS=' "$ENVF" \
  | "$PY" "$CLI" --db "$DB" import-legacy --stdin --mapping "$BK/mapping.json"
"$PY" "$CLI" --db "$DB" list        # таблица без хешей: 1 admin, 7 viewer
"$PY" "$CLI" --db "$DB" backup --dest "$BK/accounts"
```

**3.6. Переключение.** В `$ENVF` (sudo):

```env
PORTAL_AUTH_MODE=accounts
PORTAL_SESSION_SECRET=<новый: python -c "import secrets;print(secrets.token_urlsafe(48))">
PORTAL_SESSION_COOKIE_NAME=am_session
PORTAL_COOKIE_SECURE=true
PORTAL_REGISTRATION_ENABLED=false
```

Кавычки вокруг значения `PORTAL_AUTH_USERS` CLI снимает сам. Строку
`PORTAL_AUTH_USERS` на этом шаге оставить: она понадобится только для
сверки и после стабилизации удаляется (шаг 6). В режиме `accounts` она не
читается.

Затем выполнить `sudo -n systemctl --system restart auditmanager-backend.service`.

**3.7. Проверка:**

* каждый сотрудник входит прежним паролем;
* у владельца есть пункт «Пользователи»;
* у остальных нет кнопок запуска, а прямой `POST` даёт 403;
* `sha256sum -c "$BK/SHA256SUMS"` для `decisions_log.json` и `users.json` —
  без изменений;
* график работ на месте;
* `/audit-workers` открывается только владельцу;
* `/api/info` отвечает 200.

**3.8. Включить регистрацию.** `PORTAL_REGISTRATION_ENABLED=true`, рестарт.

Ежедневная копия базы аккаунтов (crontab пользователя `coder`):

```cron
30 3 * * * /home/coder/auditmanager/current/venv/bin/python /home/coder/auditmanager/current/app/backend/scripts/portal_accounts.py --db /srv/auditmanager/data/backend/app/data/portal_accounts.sqlite3 backup --dest /home/coder/auditmanager/backups/portal-accounts >/dev/null 2>&1
```

Старше 30 копий удалять вручную или `find … -mtime +30 -delete`.

## 4. Откат (не возвращает старые cookie, пароли и права)

Запрещено возвращать:

* прежний `PORTAL_SESSION_SECRET` и имя cookie;
* строку `PORTAL_AUTH_USERS` с восемью логинами в режиме `env_users`;
* релиз до этапа 0.

**4.1. Сбой модуля аккаунтов, релиз исправен — аварийный режим владельца.**

```bash
"$PY" "$REL/app/backend/scripts/hash_portal_password.py" --user <ваш логин>   # НОВЫЙ пароль
```

В env:

```env
PORTAL_AUTH_MODE=emergency_owner
PORTAL_EMERGENCY_OWNER=<ваш логин>:<хеш>
PORTAL_SESSION_SECRET=<новый>
PORTAL_SESSION_COOKIE_NAME=am_emergency
```

Хеш содержит `$`. Значение записывается в той же форме (с кавычками или без),
в какой сейчас записана `PORTAL_AUTH_USERS` в `$ENVF` и с какой служба
работает. Кавычки снимает systemd; загрузчик `.env` проекта их не снимает (см.
`docs/portal_auth.md`, «Кавычки и `$` в .env»).

Затем рестарт. Входит только владелец; регистрация, остальные и
`/admin/users` закрыты. База аккаунтов не меняется.

Возврат: `PORTAL_AUTH_MODE=accounts`, ещё один новый секрет,
`PORTAL_SESSION_COOKIE_NAME=am_session`, рестарт. Все входят заново,
блокировки и права сохранены.

**4.2. Сломан сам код этой ветки.** `deploy_center_release.py --release <id
релиза этапа 0>` — релиз, собранный из коммита этапа 0, или более поздний
исправный. В env:

```env
PORTAL_AUTH_MODE=env_users
PORTAL_AUTH_USERS=<только ваш логин>:<новый хеш>
PORTAL_SESSION_SECRET=<новый>
PORTAL_SESSION_COOKIE_NAME=am_rollback
```

Регистрации в этом коде нет, других логинов в строке нет — войдёт только
владелец.

**4.3. Не делать:** `deploy … --release ui-real-c93305db` (до этапа 0) с
прежним env. Это вернуло бы старые пароли, старые cookie и полный доступ
всем 8 логинам.

Проверено на копии данных: `scripts/portal_access/rehearse_migration.py` и
`tests/test_portal_rollback.py`.

## 5. Восстановление из резервной копии

```bash
sudo systemctl --system stop auditmanager-backend.service        # нужен пароль sudo
cp "$BK/accounts/portal_accounts.<stamp>.sqlite3" "$DB" && rm -f "$DB-wal" "$DB-shm"
sudo -n systemctl --system restart auditmanager-backend.service
```

`users.json` и `decisions_log.json` миграцией не меняются; их копии в `$BK` —
страховка.

## 6. После стабилизации (через 14 дней без отката)

* Удалить `PORTAL_AUTH_USERS` из env.
* Следующим релизом убрать режим `env_users` и `users.json role` из проверок
  (план, этап 10).

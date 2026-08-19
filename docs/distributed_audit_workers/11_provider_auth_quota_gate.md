# Этап 11. Provider auth & quota gate

> **Ветка:** `feat/distributed-audit-workers-provider-gate`
> **База:** `9b56bd3f` (HEAD задачи №10, REAL VPS TRANSPORT COMPLETE)
> **Дата:** 2026-08-09
> **Push:** не выполнялся
> **Реальных обращений к моделям:** см. раздел 22 — по умолчанию ноль

---

## 1. Цель этапа

Этап 10 доказал транспорт между двумя настоящими VPS. Модели при этом
оставались поддельными: `claude` и `codex` на воркере не были установлены
вовсе, а конвейер звал заглушки из каталога подделок.

Этот этап отвечает на другой вопрос: **можно ли подключить к воркеру настоящие
Claude Code и Codex так, чтобы (а) это было законно, (б) их учётные данные не
утекли и не размазались по машинам, и (в) центр честно видел, сколько лимита
осталось.**

Ключевое слово — «честно». Экран, который показывает «остаток 62 %», когда
никакого измерения не было, хуже пустого экрана: по нему принимают решения.
Поэтому главным результатом этапа стало не столько подключение, сколько
разделение двух ситуаций, которые легко слить в одну:

* лимит **известен** — потому что провайдер отдал его официальным
  машиночитаемым интерфейсом;
* лимит **неизвестен** — и это не сбой, а свойство провайдера.

Реальный аудит пользовательских документов на этом этапе не запускался.

---

## 2. Исходное состояние: REAL VPS TRANSPORT COMPLETE

Из документа 10:

```
центр 176.12.77.128 ──HTTPS(cloudflared)──▶ воркер 176.12.77.31
Agent + Executor доказаны, fake-аудит через две реальные машины доказан.
```

Ограничение того этапа, снимаемое здесь: моделей не было ни одной.

**Уточнение к документу 10.** Там сказано, что «`claude` и `codex` на воркере
физически отсутствуют». Инвентаризация этого этапа показала, что утверждение
верно лишь в той форме, в какой оно проверялось (`command -v` в
неинтерактивной сессии). Фактически на машине уже жили:

| что | где | откуда |
|---|---|---|
| `claude` 2.1.126 | `~/.npm-global/bin/claude` | глобальный npm-пакет |
| `claude` 2.1.222 | внутри `~/.vscode-server/extensions/anthropic.claude-code-*` | расширение VS Code |
| скрипт-обёртка | `~/.local/bin/claude` | ручная, ищет бинарь в расширениях |
| `~/.claude/.credentials.json` | 0600, mtime 2026-08-05 | личная работа пользователя VPS |
| `~/.codex/auth.json` | 0600, mtime 2026-08-03 | то же |

Ни один из этих путей не был в `PATH` неинтерактивной сессии, поэтому
`command -v` их не находил.

**Меняет ли это вывод этапа 10 о том, что настоящие модели не звались?** Нет,
и вот почему: доказательство там строилось не только на отсутствии бинарей.
`audit_runner.build_env` собирает окружение конвейера с нуля, `HOME` уводится
в `job_dir/work/home` (личный `~/.claude` конвейеру не виден), а `PATH`
префиксуется каталогом подделок, помеченным `PROVIDERS.json`. Плюс журнал
`fake_provider_calls.jsonl`. Но одно из пяти свидетельств было слабее, чем
считалось, и это записано здесь, а не замолчано.

---

## 3. Официальные ограничения провайдеров

Раздел собран **до** написания кода, по официальным источникам. Ни одна
строчка кода не опирается на форум, блог или реверс.

### 3.1 Claude Code (Anthropic)

| вопрос | ответ | источник |
|---|---|---|
| установка | нативный установщик `curl -fsSL https://claude.ai/install.sh \| bash`; также Homebrew, WinGet, apt/dnf/apk, npm | docs → Advanced setup |
| куда ставится | `~/.local/bin/claude` → `~/.local/share/claude/versions/<версия>` | там же |
| проверка целостности | `manifest.json` с SHA256 + отделённая подпись GPG, ключ `31DD DE24 DDFA B679 F42D 7BD2 BAA9 29FF 1A7E CACE` | docs → Binary integrity |
| авторизация | Pro/Max/Team/Enterprise через браузер; Console; Bedrock/Vertex/Foundry; `apiKeyHelper`; `ANTHROPIC_API_KEY` | docs → Authentication |
| авторизация для CI/скриптов | `claude setup-token` → одногодичный OAuth-токен в `CLAUDE_CODE_OAUTH_TOKEN`. Официальная формулировка: «For CI pipelines, scripts, or other environments where interactive browser login isn't available» | docs → Authentication → Generate a long-lived token |
| где лежат учётные данные | Linux: `~/.claude/.credentials.json`, режим `0600`. При заданном `CLAUDE_CONFIG_DIR` — внутри него | docs → Authentication → Credential management |
| перенос конфига | `CLAUDE_CONFIG_DIR`: «every `~/.claude` path lives under that directory instead» | docs → Explore the .claude directory |
| **машиночитаемый auth status** | **`claude auth status`** — JSON по умолчанию, `--text` для человека, exit 0 = вошли, 1 = нет | docs → CLI reference |
| **машиночитаемый остаток лимита** | **официального способа без обращения к модели НЕТ** | см. 3.3 |
| какой лимит расходуется | Pro/Max: «usage limits that are shared across Claude and Claude Code, meaning all activity in both tools counts against the same usage limits» | Help Center → Use Claude Code with your Pro or Max plan |
| `--bare` | «Anthropic auth is strictly `ANTHROPIC_API_KEY` or `apiKeyHelper` (OAuth and keychain are never read)» — то есть bare-режим подписку НЕ использует | `claude --help` 2.1.226, docs → headless |
| страна | Казахстан входит в список поддерживаемых стран Anthropic (проверено) | anthropic.com/supported-countries |

### 3.2 Codex CLI (OpenAI)

| вопрос | ответ | источник |
|---|---|---|
| установка | `curl -fsSL https://chatgpt.com/codex/install.sh \| sh`; также npm, Homebrew | docs → Codex CLI |
| куда ставится | `$CODEX_INSTALL_DIR` (по умолчанию `~/.local/bin`), пакет — в `$CODEX_HOME/packages/standalone` | install.sh, docs → Environment variables |
| проверка целостности | `codex-package_SHA256SUMS`, сверка в установщике | install.sh |
| авторизация | `codex login` (браузер), `codex login --device-auth` (без браузера, beta), `codex login --with-api-key` (stdin) | docs → Authentication |
| проверка состояния | `codex login status` — «exits with `0` when credentials are present» | docs → Developer commands |
| где лежат учётные данные | `$CODEX_HOME/auth.json`. Документация прямо предупреждает: «Treat `~/.codex/auth.json` like a password: it contains access tokens» | docs → Authentication |
| перенос состояния | `CODEX_HOME`: «Sets the root for Codex state, including config, auth, logs, sessions, skills». Каталог должен существовать заранее | docs → Environment variables |
| **машиночитаемый auth status** | `codex login status` (код возврата) + `codex app-server` → `account/read` (`type`, `email`, `planType`, `requiresOpenaiAuth`) | docs → App server |
| **машиночитаемый остаток лимита** | **ЕСТЬ:** `codex app-server` → `account/rateLimits/read` | docs → App server, раздел «6) Rate limits (ChatGPT)» |
| формат окна | `usedPercent` — «current usage within the quota window»; `windowDurationMins` — «the quota window length»; `resetsAt` — «a Unix timestamp (seconds) for the next reset»; есть `primary` и `secondary` | там же |
| стабильность | «The app-server command and WebSocket transport are experimental and aren't supported for production workloads»; в `codex --help` подкоманда помечена `[experimental]` | docs → App server; сам CLI 0.147.0 |
| рекомендация для автоматизации | «Use API key authentication for programmatic Codex CLI workflows, such as CI/CD jobs»; при этом `CODEX_ACCESS_TOKEN` описан как «a ChatGPT or Codex access token for **trusted automation**» | docs → Authentication, Environment variables |

### 3.3 Почему у Claude нет zero-inference источника остатка

Утверждение проверено четырьмя независимыми способами, потому что от него
зависит вся конструкция этапа.

1. **Подкоманд нет.** `claude --help` версии 2.1.226 на самом воркере:
   `agents, auth, auto-mode, doctor, gateway, import, install, mcp, plugin,
   project, setup-token, ultrareview, update`. Ничего про usage/limits/quota.
2. **`/usage`, `/cost`, `/status` — команды интерактивного сеанса.** В `-p`
   они недоступны, а их вывод — текст терминала, не контракт.
3. **OpenTelemetry-экспорт не содержит лимитов.** Полный список метрик:
   `session.count`, `lines_of_code.count`, `pull_request.count`,
   `commit.count`, `cost.usage`, `token.usage`, `code_edit_tool.decision`,
   `active_time.total`. Это РАСХОД, а не остаток.
4. **Единственный официальный машиночитаемый вид остатка — статусная строка.**
   Поля `rate_limits.five_hour.{used_percentage,resets_at}` и
   `rate_limits.seven_day.{…}` подаются на stdin скрипту statusLine. И там же
   написано ключевое:

   > `rate_limits`: appears only for Claude.ai subscribers (Pro/Max) **after
   > the first API response in the session**.

Отсюда прямое следствие по §17 задания: **узнать остаток можно только ценой
запроса к модели**, а значит автоматический опрос квоты Claude ЗАПРЕЩЁН — он
расходовал бы подписку ради телеметрии. Код это не обходит: он возвращает
`quota_state="unknown"`, `estimated_remaining_pct=None`,
`raw_remaining_supported=False` и текстовое объяснение оператору.

---

## 4. Inventory Claude

| параметр | значение |
|---|---|
| версия | **2.1.226 (Claude Code)** |
| способ установки | официальный нативный установщик `https://claude.ai/install.sh`, аргумент `latest` |
| SHA256 бинаря | `4e9bec1177ce9690e8bd988b710ac24105e70da428dd094c5adcbbe786a55555` |
| сверка с манифестом | совпало с `platforms["linux-x64"].checksum` из `manifest.json` релиза 2.1.226 |
| SHA256 установщика | `cde4f1702d3b1695f92b73d26888364e17bca476e17f0fd676484c951d36c125` |
| путь бинаря | `<providers>/claude/home/.local/share/claude/versions/2.1.226` |
| лаунчер | `<providers>/claude/home/.local/bin/claude` (символьная ссылка) |
| sudo | не использовался |
| авторизация | **не выполнена** (`claude auth status` → `{"loggedIn": false, "authMethod": "none", "apiProvider": "firstParty"}`, exit 1) |
| non-interactive режимы | `-p/--print`, `--output-format text\|json\|stream-json`, `--json-schema`, `--max-turns`, `--permission-mode`, `--bare` |
| автообновление | выключено (`DISABLE_AUTOUPDATER=1` в окружении адаптера) — версия входит в контракт разбора |

## 5. Inventory Codex

| параметр | значение |
|---|---|
| версия | **codex-cli 0.147.0** |
| способ установки | официальный установщик `https://chatgpt.com/codex/install.sh`, `CODEX_NON_INTERACTIVE=1` |
| SHA256 установщика | `ba92dd27e5c06f0d3bbc58bfa4b9cfb6599cd2742fbb1f92a2765e6c07dedb5a` |
| проверка пакета | выполняет сам установщик по `codex-package_SHA256SUMS` |
| путь бинаря | `<providers>/codex/home/.codex/packages/standalone/current/bin/codex` |
| лаунчер | `<providers>/codex/home/.local/bin/codex` (символьная ссылка) |
| sudo | не использовался |
| авторизация | **не выполнена** (`codex login status` → `Not logged in`, exit 1; `account/read` → `{"account": null, "requiresOpenaiAuth": true}`) |
| non-interactive режимы | `codex exec` (+`--json`, `--sandbox`, `--ephemeral`, `--output-schema`, `--skip-git-repo-check`) |
| structured interface | `codex app-server` (JSON-RPC 2.0 по stdio) |

---

## 6. Installation

Установка выполнена **под пользователем `coder`, без sudo**, в изолированные
каталоги. Ни системный Node, ни Plesk, ни почтовые сервисы, ни nginx, ни
firewall не затронуты; портов не открыто.

Приём, ради которого установка получилась полностью изолированной: обоим
официальным установщикам подменён `HOME` на provider home. Установщики кладут
всё относительно `$HOME`, поэтому и бинарь, и лаунчер, и конфиг оказались
внутри каталога воркера, а личные `~/.local/bin/claude`, `~/.claude` и
`~/.codex` пользователя VPS остались нетронутыми.

```bash
PROV=/home/coder/audit-worker/data/providers

# Claude
HOME=$PROV/claude/home \
CLAUDE_CONFIG_DIR=$PROV/claude/home/.claude \
DISABLE_AUTOUPDATER=1 \
  bash install.sh latest

# Codex
HOME=$PROV/codex/home \
CODEX_HOME=$PROV/codex/home/.codex \
CODEX_INSTALL_DIR=$PROV/codex/home/.local/bin \
CODEX_NON_INTERACTIVE=1 \
  sh install.sh
```

Перед установкой Claude была снята резервная копия личного лаунчера
`~/.local/bin/claude` и сверен его SHA256 после установки:
`dcdf965a…a10b4` до и после — файл не тронут. Личные
`~/.claude/.credentials.json` и `~/.codex/auth.json` сохранили исходные mtime.

---

## 7. Provider homes

```
/home/coder/audit-worker/data/providers/          0700
├── claude/                                       0700
│   ├── home/       ← HOME процесса CLI           0700
│   │   ├── .claude/            ← CLAUDE_CONFIG_DIR, здесь же .credentials.json
│   │   ├── .claude.json
│   │   └── .local/{bin,share,state}/claude
│   ├── runtime/    ← cwd подпроцессов, ПУСТОЙ    0700
│   └── metadata/   ← соль отпечатка              0700
└── codex/                                        0700
    ├── home/                                     0700
    │   ├── .codex/             ← CODEX_HOME, здесь же auth.json и пакет
    │   └── .local/bin/codex
    ├── runtime/                                  0700
    └── metadata/                                 0700
```

Четыре свойства раскладки и причина каждого:

1. **Каталог ДАННЫХ, а не кода.** Код воркера живёт в `app/<релиз>/` со
   ссылкой `current`; данные — в `data/`. Обновление и откат релиза
   авторизацию не трогают. Артефакт деплоя собирается из git-tracked файлов
   репозитория и `providers/` в него не попадает физически.
2. **Менеджер удержания сюда не заглядывает.** `RetentionManager` сканирует
   только `jobs_dir`; `providers/` вне его зоны.
3. **`runtime/` пуст и не является git-репозиторием.** Это не
   перестраховка: `codex app-server`, запущенный с `cwd=/home/coder`,
   обнаружил там личный `~/.codex` как project-local конфигурацию и выдал
   предупреждение. С пустым `runtime/` этого не происходит.
4. **0700.** На VPS живут посторонние сервисы под другими пользователями.
   Единственная надёжная граница здесь — права файловой системы.

Каталог `providers/` намеренно исключён из `WorkerConfig.ensure_dirs()`:
общий режим 0750 остальных каталогов слишком широк для учётных данных, и
`providers/` создаёт `ProviderManager` с 0700.

---

## 8. Authentication

Состояние на момент написания документа: **вход не выполнен ни у одного
провайдера**. Это осознанно: §7 задания требует, чтобы интерактивную
авторизацию выполнял оператор, а не автоматика.

Команды для оператора (выполнять на VPS под `coder`):

```bash
PROV=/home/coder/audit-worker/data/providers

# ── Claude Code ──────────────────────────────────────────────────────────
HOME=$PROV/claude/home CLAUDE_CONFIG_DIR=$PROV/claude/home/.claude \
  $PROV/claude/home/.local/bin/claude auth login
# Откроется браузер (по SSH — будет предложено скопировать ссылку клавишей c
# и вставить код обратно в терминал).
# Проверка ПОСЛЕ входа (ноль обращений к модели):
HOME=$PROV/claude/home CLAUDE_CONFIG_DIR=$PROV/claude/home/.claude \
  $PROV/claude/home/.local/bin/claude auth status

# ── Codex ────────────────────────────────────────────────────────────────
HOME=$PROV/codex/home CODEX_HOME=$PROV/codex/home/.codex \
  $PROV/codex/home/.local/bin/codex login --device-auth
# device-auth не требует браузера на самой машине: показывается код,
# который вводится на любом устройстве. Обычный `codex login` тоже работает.
HOME=$PROV/codex/home CODEX_HOME=$PROV/codex/home/.codex \
  $PROV/codex/home/.local/bin/codex login status
```

Чего в этой процедуре нет и не будет: копирования `~/.claude` или `~/.codex`
между машинами, переноса `.credentials.json`/`auth.json`, передачи токена
через чат или файл, чтения содержимого credential-файлов.

---

## 9. Credential isolation

| требование | как обеспечено | чем проверено |
|---|---|---|
| Claude и Codex имеют разные provider home | `ProviderHome` вычисляет путь от имени провайдера | тест `test_provider_homes_are_separate_and_narrow`; smoke шаг 2 |
| job HOME остаётся отдельным | `audit_runner.isolated_roots` не изменён: `HOME=job_dir/work/home` | тест `test_provider_layer_is_the_only_place_naming_real_clis` |
| provider credentials не попадают в attempt root | `providers/` вне `jobs/`; конвейер не импортирует `audit_worker.providers` | тот же тест (AST-проверка импортов) |
| result package credentials не содержит | пакет собирается из `job_dir`, `providers/` вне его | наследуется с этапа 10 |
| source package credentials не содержит | собирается центром из проекта | наследуется |
| центр credential не получает | `as_center_payload` — ПЕРЕЧИСЛЕНИЕ разрешённых полей, не вычитание запрещённых | тесты `test_email_never_reaches_the_center_payload`, `test_center_payload_has_no_absolute_paths`, `test_stored_snapshot_contains_no_credentials`; smoke шаг 7 |
| Executor не читает credential напрямую | Executor вызывает конвейер; провайдерский слой живёт в агенте и в CLI-подкоманде | AST-проверка импортов |
| ProviderAdapter запускает CLI с изолированным окружением | `build_env` собирается с нуля; `FORBIDDEN_ENV_NAMES` — рубеж против будущей правки | тесты `test_environment_is_built_from_scratch`, `test_forbidden_env_names_are_rejected_even_if_added_later`, `test_no_token_in_process_argv_or_env` |
| права каталогов минимальные | 0700 | smoke шаг 2 (живая проверка на VPS) |
| provider home не попадает в deployment artifact | артефакт из git-tracked файлов репозитория; `providers/` — каталог данных на VPS | `deploy_audit_worker.BUNDLE_INCLUDE` |
| provider home переживает обновление воркера | лежит в `data/`, не в `app/<релиз>/` | деплой нового релиза выполнен, каталог на месте |
| откат кода не удаляет provider auth | то же | по построению |

**Содержимое credential-файлов не читается нигде.** `credential_file_facts`
использует только `os.stat`: существование, режим, владелец, размер. Функции
`open` в `identity.py` нет.

---

## 10. ProviderAdapter

Единственная точка запуска CLI провайдера. Инварианты слоя (`base.py`):

| код | инвариант |
|---|---|
| I-P1 | окружение строится С НУЛЯ, а не копированием `os.environ` с чисткой |
| I-P2 | worker-token, адрес центра, execution-token и ключи платных API не доходят физически: их имён нет в белом списке, а адаптер их и не получает |
| I-P3 | у каждого провайдера свой `HOME` и своя переменная конфига; Claude никогда не видит `CODEX_HOME` |
| I-P4 | `cwd` — пустой `providers/<p>/runtime` |
| I-P5 | ни один `argv` не приходит извне: только константы модуля |
| I-P6 | вывод редактируется СРАЗУ, а не «перед отправкой» |
| I-P7 | таймаут обязателен, по нему убивается ГРУППА процессов |

Интерфейс (§9 задания) реализован полностью: `installed()`, `version()`,
`auth_status()`, `quota_status()`, `minimal_probe()`, `classify_error()`,
`executable_path()`, `capability_snapshot()`.

**`PATH` фиксированный** (`/usr/local/bin:/usr/bin:/bin`), а исполняемый файл
выбирается по абсолютному пути. Поиска по `PATH` нет намеренно: в `PATH`
воркера первым идёт каталог ПОДДЕЛЬНЫХ провайдеров, и «найти `claude` в
`PATH`» означало бы опросить подделку и отрапортовать центру её версию как
настоящую. Закреплено тестом
`test_wrong_executable_is_not_silently_replaced_by_path_lookup`.

**Диалог JSON-RPC вынесен в отдельный метод** `run_jsonrpc_stdio`, потому что
`communicate()` закрывает stdin сразу, а сервер с stdin в EOF вправе
завершиться, не ответив. Проверено вживую: `printf … | codex app-server`
отдавал только ответ на `initialize`, а `account/read` и
`account/rateLimits/read` терялись.

---

## 11. Claude quota source

**Источник: отсутствует.** `supports_zero_inference_quota() → False`,
`quota_source_name() → "unavailable"`.

`quota_status()` возвращает:

| ситуация | `quota_state` | остаток |
|---|---|---|
| вход не выполнен | `auth_required` | `None` |
| вход истёк | `auth_required` | `None` |
| ошибка/неизвестно | `unknown` | `None` |
| вошли | `unknown` | `None` + объяснение оператору |

Объяснение, которое видит оператор:

> у Claude Code нет официального машиночитаемого способа узнать остаток лимита
> без обращения к модели: поля `rate_limits` публикуются только скрипту
> статусной строки и только после первого ответа API в сеансе. Автоматический
> опрос запрещён — он расходовал бы подписку ради телеметрии.

Что остаётся вместо опроса (реализуемо на следующих этапах, здесь не сделано):
наблюдённые отказы по лимиту из настоящих прогонов (`observed_rate_limit_response`),
собственная статистика вызовов (`local_usage_statistics`) и ручные даты
сброса от оператора (`operator_manual`) — все три источника уже есть в
перечислении приоритетов.

---

## 12. Codex quota source

**Источник: официальный structured JSON-RPC.**

```
codex app-server                      # stdio, JSON-RPC 2.0
  → initialize (БЕЗ capabilities — остаёмся на стабильной поверхности)
  → initialized
  → account/read            → {account:{type,email,planType}, requiresOpenaiAuth}
  → account/rateLimits/read → {rateLimits:{limitId,primary,secondary,
                                           rateLimitReachedType},
                               rateLimitsByLimitId:{…}}
```

Живая проверка на воркере в разлогиненном состоянии:

```json
{"id": 2, "result": {"account": null, "requiresOpenaiAuth": true}}
{"id": 3, "error": {"code": -32600,
  "message": "codex account authentication required to read rate limits"}}
```

То есть интерфейс работает, а «нет данных» отличается от «не вошли»: первое
даёт `unknown`, второе — `auth_required`.

**Честная оговорка, которая доехала до кода и до экрана.** Подкоманда
`app-server` помечена `[experimental]` в собственном `codex --help`, а
документация говорит «aren't supported for production workloads». Поэтому
снимок несёт `source_stability="experimental"`, и это ОТДЕЛЬНАЯ ось от
`confidence`:

* `confidence=high` — числу можно верить: его отдала первая сторона структурой;
* `source_stability=experimental` — контракт может измениться в следующей
  версии CLI.

Смешать их значило бы либо занизить достоверность точного числа, либо скрыть
риск смены контракта. При смене контракта разбор падает в `UNKNOWN`, а не
выдаёт мусор — это закреплено тестом.

**`capabilities.experimentalApi` не используется**: оба нужных метода входят в
стабильную часть поверхности app-server.

---

## 13. Quota normalization

Единый тип `ProviderQuotaSnapshot` (`audit_worker/providers/quota.py`).
Главное свойство — **выдуманный процент нельзя сконструировать**:

```python
QuotaWindow(window_id="w", source=SOURCE_UNAVAILABLE,
            confidence=CONFIDENCE_HIGH, used_pct=10.0)
# → QuotaContractError: процент указан при source=unavailable

ProviderQuotaSnapshot(..., estimated_remaining_pct=50.0,
                      raw_remaining_supported=False)
# → QuotaContractError: процент, не подтверждённый источником, показывать нельзя
```

Три независимые оси, которые легко перепутать:

| ось | вопрос | значения |
|---|---|---|
| `quota_state` | что сейчас с лимитом | `ready`, `low`, `limited`, `cooldown`, `auth_required`, `unknown`, `stale`, `error`, `policy_blocked` |
| `source` | откуда сведения | 8 значений, см. ниже |
| `confidence` | насколько верить | `high`, `medium`, `low`, `none` |

Приоритет источников (§11 задания), от надёжного к слабому:

```
1. official_structured_api
2. official_app_server_rpc          ← Codex сейчас здесь
3. official_machine_readable
4. official_documented_text
5. observed_rate_limit_response
6. local_usage_statistics
7. operator_manual
—  unavailable                      ← Claude сейчас здесь
```

Правило выбора остатка при нескольких окнах — **худшее**: пятичасовое окно
может быть свободно, когда недельное почти выбрано, и «свободно» в этом случае
неправда.

Санитайзер на центре (`provider_accounts.sanitize_quota`) **пересобирает**
объект из разрешённых значений, а не проверяет присланный. Процент
отбрасывается при любом из трёх условий: нет `raw_remaining_supported`,
`source=unavailable`, `confidence=none`. Значение вне 0..100 не зажимается, а
отбрасывается — зажатое выглядело бы достоверным, не будучи им. `ready`/`low`
без остатка честно понижается до `unknown`.

---

## 14. SubscriptionAccount

Новая центральная сущность (миграции схемы 6 → 8).

| поле | назначение |
|---|---|
| `account_id` | UUID |
| `provider` | `claude` / `codex` |
| `display_name` | имя для человека |
| `account_group_id` | ключ объединения (см. 15) |
| `account_kind` | `subscription_personal` / `_team` / `_enterprise` / `commercial_api` / `unknown` |
| `policy_state` | `allowed` / `review_required` / `policy_blocked` — комплаенс-решение оператора |
| `manual_reset_label`, `manual_next_reset_at`, `manual_reset_recurrence`, `reset_timezone` | ручные сведения оператора |
| `warning_days` | пороги предупреждения, по умолчанию `[7, 3, 1]` |
| `operator_marked_unused` | отметка «лимит почти не использован» |
| `notes`, `created_at`, `updated_at` | |

**Чего в таблице нет и не появится:** колонок для токена, пароля,
refresh-token, cookie и API-ключа. Это не «мы решили не хранить» — колонки
нет, записать некуда. Закреплено тестом `test_center_schema_has_no_token_column`.

Новая запись заводится с `policy_state="review_required"`: комплаенс по
учётной записи — решение человека, и молчаливое `allowed` было бы этим
решением вместо него.

Наблюдаемые значения (`observed_remaining_pct`, `observed_next_reset_at`,
`quota_state`, `quota_source`, `quota_confidence`, `last_checked_at`) НЕ хранятся
в таблице аккаунта: они вычисляются сведением снимков воркеров, чтобы не было
двух источников истины.

---

## 15. account_group_id

Одна подписка может обслуживать несколько VPS. Тогда два воркера присылают ДВА
снимка ОДНОГО лимита.

```
VPS-A · Claude ─┐
                ├─ account_group claude-account-01 ─ ОДИН ресурс
VPS-B · Claude ─┘
```

**Складывать их нельзя.** 40 % и 40 % — это не 80 %, это одни и те же 40 %,
увиденные дважды. `reconcile_group` выбирает ОДИН снимок по объявленной
политике `most_trustworthy_then_freshest_single_snapshot`:

1. надёжность источника (индекс в списке приоритетов);
2. при равной надёжности — достоверность;
3. при равной достоверности — свежесть.

Политика называется в ответе API и на экране, чтобы решение можно было
оспорить, а не угадывать. Поле `aggregated: false` присутствует во всех ветвях
специально: будущая правка, решившая «а давайте суммировать», сломает тест
`test_two_workers_one_account_are_not_summed`.

**Группу назначает оператор.** Автоматически сопоставить два VPS «по одному
аккаунту» нельзя — для этого пришлось бы сверять секретные данные, что прямо
запрещено. Способы задать группу:

* на воркере: `AUDIT_WORKER_PROVIDER_CLAUDE_ACCOUNT_GROUP_ID=claude-account-01`;
* на центре: `PUT /api/workers/{worker_id}/providers/{provider}/account-group`
  (право `operate`).

`account_fingerprint` — солёный отпечаток с ПОВОРКЕРНОЙ солью. Он отвечает
ровно на один вопрос — «на ЭТОМ воркере сменилась учётная запись?» — и
намеренно НЕ позволяет сопоставить два воркера: это работа оператора.

---

## 16. Manual reset dates

Оператор задаёт вручную: метку, дату/время сброса, часовой пояс,
периодичность (`daily`/`weekly`/`monthly`/`every_5_hours`), заметки и пороги
предупреждения.

**Две даты живут отдельно и никогда не перетирают друг друга:**

| поле | кто пишет |
|---|---|
| `manual_next_reset_at` | только оператор |
| наблюдаемая (`observed_next_reset_at`) | только провайдер через снимок |

При расхождении больше часа экран показывает обе и предупреждение
«ручная и наблюдаемая даты сброса расходятся; автоматика ручную дату не
меняет». Закреплено тестами `test_manual_and_observed_reset_coexist` и
`test_observation_never_overwrites_manual_reset`.

Отдельная тонкость: в `upsert_account` переданное `None` означает «не
трогать», а не «стереть». Иначе форма, не заполнившая поле, молча удаляла бы
дату, которую человек ставил руками. Для явного стирания есть отдельная
операция `clear_manual_reset`.

---

## 17. Warning thresholds

По умолчанию `[7, 3, 1]` дней, настраивается на каждую учётную запись
(1..365, до 10 значений).

Сработавшие пороги показываются с указанием ИСТОЧНИКА даты: «за 3 дня по
ручной дате» и «за 3 дня по наблюдаемой» — разные утверждения с разной
надёжностью.

**`reset_soon_unused`** («лимит сгорит неиспользованным») зажигается только
при одном из двух условий:

* сброс близко **И** остаток ДЕЙСТВИТЕЛЬНО известен и выше порога `low`;
* оператор сам пометил учётную запись как почти не использованную.

Если остаток неизвестен, предупреждения НЕТ, и экран объясняет почему:
«остаток неизвестен: предупреждать не по чему. Отметьте учётную запись
вручную, если знаете, что лимит не израсходован». Ложная тревога здесь дороже
пропуска: оператор, которого позвали зря дважды, перестанет реагировать на
третий раз.

Порог `low` не вычисляется без настройки:
`DISTRIBUTED_WORKERS_QUOTA_LOW_THRESHOLD_PCT`, по умолчанию **25 %**
(консервативно: 25 % пятичасового окна Codex — это уже мало для полного
аудита раздела). Значение `0` явно выключает состояние `low`.

---

## 18. Heartbeat

Расширен полем `providers` — список безопасных снимков.

**Heartbeat провайдеров НЕ опрашивает.** Он отдаёт последний известный снимок.
Опрашивать в такте heartbeat значило бы поднимать процессы CLI 2880 раз в
сутки ради данных, которые меняются раз в час, и подвешивать сигнал живости на
время каждого запуска.

Три разные частоты:

| что | по умолчанию | переменная |
|---|---|---|
| heartbeat | 30 с | `AUDIT_WORKER_HEARTBEAT_SEC` |
| проверка авторизации | 300 с | `AUDIT_WORKER_PROVIDER_AUTH_CHECK_INTERVAL_SEC` |
| опрос лимита | 900 с | `PROVIDER_QUOTA_PROBE_INTERVAL_SEC` |
| контрольный запрос к модели | **никогда автоматически** | — |

Опрос лимита зажимается снизу частотой проверки авторизации: он тяжелее и
ходит в сеть провайдера.

Тип поля в модели — `list[Any]`, а не `list[dict]`, и это поведенческое
решение. Heartbeat — сигнал ЖИВОСТИ; отбить его с 422 из-за одного
неразобранного элемента снимка значит превратить исправный воркер в «пропал со
связи». Дефект найден собственным тестом
`test_heartbeat_carries_providers_and_survives_bad_snapshot`, а не рассуждением.
Строгую форму задаёт санитайзер на центре.

Сырой ответ провайдера остаётся ЛОКАЛЬНО (и проходит редактор секретов); центр
получает только нормализованное значение.

---

## 19. UI

Экран «Аудит-воркеры» расширен аддитивно.

**На карточке VPS** — блок «Провайдеры моделей», по строке на провайдера:
установка и версия CLI, состояние авторизации, состояние лимита и остаток,
источник и достоверность, время последней проверки, экспериментальность
контракта, привязка к учётной записи, предупреждение о слишком широких правах
файла учётных данных.

Блок отдельный намеренно: **состояние провайдера — не состояние машины**.
Воркер остаётся `online`, когда Claude не авторизован.

**Отдельная секция «Учётные записи подписок»** — потому что одна подписка
может обслуживать несколько VPS, и её лимит один ресурс. На карточке:
состояние, остаток, источник, достоверность, наблюдаемый и ручной сброс с
днями до каждого, пороги предупреждения, число привязанных VPS, комплаенс.
При двух и более VPS карточка прямо пишет, чей снимок принят и что остатки не
складываются.

**Неизвестный остаток пишется словом «неизвестен».** Ни «0 %», ни «100 %», ни
прочерк: они читаются как измеренные значения.

Роли: просмотр — `view`, правка ручных полей и привязка группы — `operate`.
Viewer получает 403 на прямой HTTP-запрос, а не просто не видит кнопку.
CSRF-заголовок `X-Requested-With: audit-workers` требуется и на `PUT`.
Изменения пишутся в неизменяемый журнал операторских действий.

Разметка из данных не собирается: только `textContent` (проверяется тестом на
отсутствие `innerHTML` в файле экрана).

---

## 20. Quota history

Таблица `provider_quota_snapshots`. Запись происходит **только при смене
значимых полей** (состояние, остаток, дата сброса, источник) либо по истечении
минимального интервала (`DISTRIBUTED_WORKERS_QUOTA_HISTORY_MIN_INTERVAL_SEC`,
900 с). Каждая 30-секундная запись heartbeat в историю не идёт.

Два независимых предела очистки, и один без другого дырявый:

* по времени — `DISTRIBUTED_WORKERS_QUOTA_HISTORY_RETENTION_DAYS`, 120 дней;
* по числу строк — `DISTRIBUTED_WORKERS_QUOTA_HISTORY_MAX_ROWS_PER_ACCOUNT`,
  5000 на пару воркер+провайдер (иначе сбойный воркер, меняющий значение
  каждые полминуты, раздул бы таблицу за сутки).

Чтение: `GET /api/workers/providers/accounts/{id}/history`.

---

## 21. Error handling

Классификатор `audit_worker/providers/errors.py`, закрытый набор кодов:
`auth_required`, `rate_limited`, `cooldown`, `network_error`,
`provider_unavailable`, `cli_missing`, `incompatible_cli`, `malformed_status`,
`policy_blocked`, `timeout`, `unknown`.

Порядок распознавания в тексте важен: `rate limit` проверяется до общего
`limit`, иначе безобидное «context limit exceeded» уехало бы в `rate_limited`,
и оператор увидел бы исчерпанную подписку там, где её нет. Закреплено тестом.

**Отказ провайдера не имеет права:** уронить Agent, уронить Executor, сделать
воркер offline, изменить тестовое задание, удалить авторизацию. Обеспечено
тремя рубежами:

1. `ProviderManager.refresh` ловит любое исключение и пишет в кеш честное
   состояние `error`;
2. агент оборачивает и сбор снимка, и сбор предупреждений;
3. обработчик heartbeat на центре оборачивает запись состояния провайдеров.

Проверено тестами `test_provider_failure_does_not_raise_out_of_manager`,
`test_broken_adapter_is_isolated_from_the_other_provider`,
`test_cli_disappears_between_probes` и живым шагом 8 smoke-скрипта, где
исполняемый файл Claude временно уводится в сторону.

---

## 22. Minimal inference probes

**По умолчанию запрещены.** Флаг `AUDIT_WORKER_ALLOW_REAL_PROVIDER_PROBE=false`
— значение по умолчанию, и оно НЕ зависит от `AUDIT_WORKER_ALLOW_REAL_LLM`:
контрольный запрос этого этапа и боевой аудит — разные решения.

Два независимых разрешения, оба обязательны:

1. переменная окружения — решение администратора VPS, живёт в конфигурации;
2. `--i-confirm-single-real-request` — решение оператора здесь и сейчас.

Одного из двух недостаточно; проверено тестом
`test_probe_needs_two_independent_permissions`.

Запрос: ровно один на провайдера, промпт фиксирован в коде —
`Reply exactly: PROVIDER_PROBE_OK`. Документов проекта, путей, репозитория в
нём нет. Инструменты запрещены (`--permission-mode dontAsk` + явный список
запрещённых для Claude; `--sandbox read-only --ephemeral --ignore-rules` для
Codex), `cwd` — пустой runtime-каталог.

Сохраняется: время, провайдер, модель, код возврата, длительность, usage,
квота до и квота после. Текст учётных данных не сохраняется.

**Выполнено на этом этапе: НОЛЬ запросов.** Ни к Claude, ни к Codex.

Разрешение оператора получено 2026-08-09: **по одному запросу к каждому
провайдеру, после того как оператор выполнит вход**. Вход — действие человека
(браузер или device-code), автоматика его не выполняет и выполнить не может.
Поэтому контрольные запросы переносятся на момент после авторизации; команда
для них готова и приведена в приложении Б.

---

## 23. Claude subscription quota semantics

Что удалось установить официальными средствами:

| вопрос | ответ | обоснование |
|---|---|---|
| какой bucket расходует non-interactive `claude -p` | **тот же лимит подписки**, что и интерактивный Claude Code и Claude в вебе | Help Center: «usage limits that are shared across Claude and Claude Code, meaning all activity in both tools counts against the same usage limits» |
| это отдельный лимит Agent SDK? | **нет**, отдельного лимита Agent SDK в документации не существует | документация Agent SDK описывает тот же CLI и те же учётные данные |
| подтверждается ли косвенно | да: `--bare` описан как режим, где «OAuth and keychain are never read» и нужен `ANTHROPIC_API_KEY`. Значит НЕ-bare `-p` подписку как раз использует | `claude --help` 2.1.226, docs → headless |
| `CLAUDE_CODE_OAUTH_TOKEN` | «authenticates with your Claude subscription and requires a Pro, Max, Team, or Enterprise plan» | docs → Authentication |

**`CLAUDE_UNUSED_SUBSCRIPTION_LIMIT_REUSE = confirmed`** — в том смысле, что
техническая предпосылка бизнес-идеи подтверждена документацией: неиспользованный
лимит подписки действительно доступен неинтерактивному вызову, и отдельного
«ещё одного» лимита для скриптов не существует.

Оговорки, которые обязаны стоять рядом с этим «confirmed»:

* **живого измерения не проводилось** — inference-запросы на этапе запрещены,
  и вывод сделан по документации, как и требует §19 («не делать вывод по
  одному изменившемуся проценту без подтверждения документации»);
* вывод **технический**, не юридический. Допустимость конкретного сценария
  зависит от типа учётной записи — см. раздел 25.

---

## 24. Codex subscription quota semantics

| вопрос | ответ |
|---|---|
| тип авторизации | Sign in with ChatGPT (план Plus/Pro/Business/Enterprise) либо API-ключ |
| что использует `codex exec` | «Reuses saved CLI authentication by default» — то есть тот же вход, что и интерактивный |
| общий ли лимит | да: «ChatGPT Work usage inside ChatGPT uses the same pricing, credits, and usage limits as Codex»; локальные сообщения и облачные чаты делят 5-часовое окно |
| окна | первичное и вторичное (`primary`/`secondary`), 5-часовое и недельное |
| machine-readable percentage/reset | **да** — `account/rateLimits/read` |
| что при упоре в лимит | `rateLimitReachedType` в ответе; агент может дорабатывать текущий ход «subject to fair use limits» |
| официальная рекомендация для автоматизации | API-ключ для CI/CD; `CODEX_ACCESS_TOKEN` — «for trusted automation» |

**`CODEX_UNUSED_SUBSCRIPTION_LIMIT_REUSE = confirmed`** технически: `codex exec`
переиспользует сохранённый вход и расходует тот же лимит плана, а остаток
наблюдаем официальным интерфейсом без обращения к модели.

Оговорка: OpenAI **рекомендует** для программных сценариев API-ключ. Это
рекомендация, а не запрет, и `CODEX_ACCESS_TOKEN` для «trusted automation»
описан отдельно. Живого измерения также не проводилось.

---

## 25. Compliance result

### 25.1 Claude

Автоматизированный/скриптовый сценарий с подпиской **официально
поддерживается Anthropic**: `claude setup-token` существует именно «for CI
pipelines, scripts, or other environments where interactive browser login
isn't available» и требует плана Pro/Max/Team/Enterprise. Отдельно
`ANTHROPIC_API_KEY` и `apiKeyHelper` работают в `-p` без ограничений.

Поэтому **`CLAUDE_PROVIDER_POLICY_BLOCKED` не возвращается**.

Но есть ограничение, которое обязано быть проговорено, потому что оно
касается сути бизнес-идеи. Consumer Terms Anthropic, раздел «Account creation
and access»:

> You may not share your Account login information, Anthropic API key, or
> Account credentials with anyone else. You also may not make your Account
> available to anyone else.

И раздел «Use of our Services» запрещает доступ «through automated or
non-human means… **except when you are accessing our Services via an Anthropic
API Key or where we otherwise explicitly permit it**». Существование
`claude setup-token` и есть это «explicitly permit it» для подписки.

Отсюда практический вывод:

| сценарий | вердикт |
|---|---|
| личная подписка Pro/Max одного человека, воркеры выполняют ЕГО собственную работу | допустимо |
| личная подписка Pro/Max, воркеры обслуживают запросы других сотрудников компании | **не допустимо** — это «making the Account available to anyone else» |
| Claude for Teams / Enterprise (место на сотрудника) | допустимо, и это рекомендованный Anthropic путь для организаций |
| Claude Console (API-биллинг) | допустимо всегда |

Документация Claude Code прямо называет Teams/Enterprise «the best experience
for organizations using Claude Code».

**Решение о типе учётной записи принято оператором 2026-08-09:**

| провайдер | выбранный тип | `account_kind` |
|---|---|---|
| Claude | личная подписка Pro/Max, воркеры выполняют собственную работу владельца | `subscription_personal` |
| Codex | личный ChatGPT Plus/Pro, то же условие | `subscription_personal` |

Это допустимый случай по обеим политикам: запрещено *делиться* учётной
записью, а не пользоваться ею с нескольких своих машин. Граница, которую
нужно помнить при росте системы: как только воркеры начнут обслуживать
запросы ДРУГИХ людей — общую очередь компании, — тот же аккаунт станет
«made available to anyone else», и потребуется Teams/Enterprise либо
API-ключ. Техника этого не заметит; это решение человека.

Значение проставляется оператором на экране «Аудит-воркеры» (диалог правки
учётной записи); до этого запись живёт в состоянии `review_required`.

### 25.2 Codex

Запрета нет. `codex exec` официально «reuses saved CLI authentication», а
`CODEX_ACCESS_TOKEN` описан для trusted automation; при этом для CI/CD OpenAI
рекомендует API-ключ. Ограничение по совместному использованию учётной записи
аналогично: план ChatGPT — на пользователя, для организаций существуют
Business/Enterprise с местами.

**`CODEX_PROVIDER_POLICY_BLOCKED` не возвращается.**

---

## 26. Security

| контроль | состояние |
|---|---|
| секретов в центральной БД | нет: колонок не существует |
| секретов в heartbeat | нет: payload собирается перечислением разрешённых полей |
| секретов в логах | редактор применяется сразу при получении вывода подпроцесса |
| секретов в артефакте деплоя | `providers/` — каталог данных на VPS, в артефакт не входит |
| `argv` | только константы модуля; путь бинаря в журнале сжимается до имени |
| окружение | белый список + явный запрет имён + проверка при сборке |
| права каталогов | 0700, живая проверка smoke |
| права credential-файлов | наблюдаются, слишком широкие дают предупреждение уровня `error` |
| e-mail учётной записи | не покидает воркер; используется только как вход солёного отпечатка |
| абсолютные пути чужой машины | не уезжают в центр (проверено smoke и тестом) |
| роли | `view`/`operate`/`admin`, fail-closed |
| CSRF | `X-Requested-With` на всех изменяющих, включая `PUT` |
| XSS | экран строится `textContent`; `innerHTML` в файле отсутствует |
| SQL | параметризованные запросы; проверено вводом с `'; DROP TABLE` |

---

## 27. Real VPS smoke

`scripts/smoke_distributed_audit_provider_gate.py`.

По умолчанию READ-ONLY и **ноль обращений к моделям**. Шаги: preflight →
изоляция provider home → неприкосновенность личных каталогов → исполняемые
файлы и версии → права учётных данных → авторизация и лимиты → скан на
секреты → изоляция отказа провайдера → центральный контур → (по двум явным
флагам) контрольный запрос.

Живой прогон против **176.12.77.31** от 2026-08-09 (дважды: до и после
адверсариальных правок):

```
ПРОВЕРОК: 34, ВСЕ ПРОШЛИ
```

Ключевые строки:

```
✔ claude: раскладка home/runtime/metadata — home:700 runtime:700 metadata:700
✔ provider home двух провайдеров различны
✔ личный ~/.claude/.credentials.json: только метаданные — 600 coder 2026-08-05 15:43:16
✔ claude: --version отвечает — 2.1.226 (Claude Code)
✔ codex: --version отвечает — codex-cli 0.147.0
✔ контрольный запрос к модели по умолчанию ЗАПРЕЩЁН
✔ опрос лимита реже проверки авторизации — auth=300.0с, quota=900.0с
✔ claude: остаток без источника не показывается — остаток=неизвестен · источник=unavailable
✔ codex: остаток без источника не показывается — остаток=неизвестен · источник=unavailable
✔ в снимке ДЛЯ ЦЕНТРА нет абсолютных путей чужой машины
✔ в снимке ДЛЯ ЦЕНТРА нет e-mail учётной записи
✔ сломанный провайдер помечен как missing
✔ второй провайдер опрошен как обычно
✔ обращений к моделям не выполнялось
```

Первый прогон нашёл два собственных дефекта скрипта (запуск из каталога данных
вместо каталога кода; скан секретов спотыкался о путь в шапке диагностики) —
оба исправлены отдельным коммитом, а не подгонкой ожиданий.

Шаг «изоляция отказа провайдера» переписан после адверсариальной проверки:
он больше НЕ перемещает исполняемый файл на диске (обрыв ssh между `mv` и
восстановлением оставлял бы провайдера сломанным), а имитирует отказ
переменной окружения `AUDIT_WORKER_PROVIDER_CLAUDE_EXECUTABLE`. Обещание
«ничего не меняет» и поведение теперь совпадают, и это проверяется
отдельной строкой отчёта «файлы на воркере не тронуты».

---

## 28. Automated tests

Новый файл `tests/test_distributed_workers_provider_gate.py` — **83 проверки**
(67 по чек-листу §31 плюс 16, закрепляющих найденные адверсариальной
проверкой дефекты).

| раздел задания | покрытие |
|---|---|
| 31.1 адаптеры | installed / missing / version / logged in / logged out / timeout / malformed status / redaction / wrong executable |
| 31.2 разбор квоты | полные данные / нет процента / только reset / rate limited / stale / unknown / malformed / версия разборщика / многоведёрный вид / порог `low` |
| 31.3 учётные записи | создание / группа / две машины в одной группе / ручной сброс / наблюдаемый сброс / расхождение / пороги 7-3-1 / отсутствие выдуманного процента |
| 31.4 безопасность | нет токена в БД/heartbeat/логах / provider home не в пакете / Claude не читает Codex и наоборот / viewer не может менять / CSRF / XSS-строки |
| 31.5 устойчивость | сбой опроса / Agent продолжает / heartbeat продолжает / устаревание / истечение авторизации / исчезновение CLI |

Прогон всей подсистемы после этапа и адверсариальных правок:
**772 passed, 1 skipped**.

Прогон `backend/tests`: **68 failed, 1665 passed** — и ровно столько же на
базовом коммите `9b56bd3f` в СО-ЛОКАЛЬНОМ worktree. Новых падений ноль.

`scripts/ci_regression_gate.py` в этом worktree красный, и это честно нужно
назвать: он сообщает о девяти «новых» падениях `test_*_geometry`. Все девять —
ошибки СБОРА (`ValueError` при вычислении id параметризации по корпусу
проектов), потому что в worktree нет каталога `projects/`. Ни один из этих
файлов диффом этапа не затронут, и те же девять ошибок воспроизведены на
базовом коммите в том же каталоге. Baseline гейта записан в окружении с
корпусом проектов — отсюда расхождение.

---

## 29. Adversarial findings

Пять независимых проверок только на чтение нашли **15 подтверждённых
дефектов**. Все закрыты и закреплены тестами (`TestAdversarialFindings`,
`TestAdversarialCenterFindings`). Ниже — что именно было сломано.

### 29.1 Provider credentials

| # | дефект | почему это важно |
|---|---|---|
| 1 | `run_jsonrpc_stdio` не редактировал `responses`/`notifications` | инвариант I-P6 обещает редакцию всего вывода подпроцесса. `limitId` из ответа доезжает до центра в `window_id` и `detail` на КАЖДОМ успешном опросе; текст ошибки обновления токена — в `detail` при сбое. Единственный барьер отсутствовал |
| 2 | `str(exc)` из `ProviderManager.refresh` шёл в снимок без редакции | сообщение чужой библиотеки может содержать путь или URL с учётными данными |
| 3 | `runtime/` пересоздавался по umask (0755), а служит `TMPDIR` подпроцесса | заявленные 0700 не обеспечивались после сноса каталога |

### 29.2 Корректность квоты

| # | дефект | что видел бы оператор |
|---|---|---|
| 4 | поле неправильного ТИПА молча превращалось в `None` | при `usedPercent="97.5"` и вторичном окне 10 % — «готов, остаток 90 %» вместо UNKNOWN |
| 5 | `bool` принимался как процент | `usedPercent: true` → «остаток 99 %», достоверность high |
| 6 | окна ЧУЖИХ вёдер участвовали в `min()` и в выборе сброса | `code_review` на 90 % подменял остаток основного лимита Codex |
| 7 | ведро без `limitId` подписывалось именем `codex` | число относилось не к тому лимиту, чьё имя стояло рядом |
| 8 | порог `low` показывался центром, применялся только воркером | экран писал «Порог: 25 %», которого никто не применил |
| 9 | `observed_at` принимался без границ | метка `1e18` делала снимок вечно свежим |
| 10 | протухший снимок в строке провайдера на карточке VPS | карточка аккаунта писала «устарело», строка провайдера — «готов, 62 %» |
| 11 | предпросмотр ранжирования считал протухший остаток измеренным | воркер с трёхдневным снимком поднимался наверх как «остаток известен» |

### 29.3 Учётные записи и изоляция

| # | дефект | последствие |
|---|---|---|
| 12 | heartbeat СТИРАЛ ручную привязку оператора | эндпоинт §15 жил ровно один такт heartbeat: воркер без заданной переменной шлёт `NULL`, и он затирал назначенное человеком |
| 13 | воркер мог объявить себя участником ЧУЖОЙ учётной записи и стать по ней источником истины | введена ось происхождения (миграция 8): привязка оператора старше заявления воркера, а самопривязка остаётся ВИДНОЙ с пометкой — прятать её значило бы лишить оператора шанса заметить чужака |
| 14 | воркер мог неограниченно плодить учётные записи; `capability` без ограничения размера | 50 heartbeat = 50 записей, каждая требует решения человека; 2 МБ ложились в базу и возвращались браузеру дословно |
| 15 | `_kill_group` выходил при смерти ЛИДЕРА | потомок в той же группе переживал обе итерации; при самостоятельном выходе лидера группе не уходило ничего |

### 29.4 Комплаенс

Проверка подтвердила фактическую верность всех утверждений кода про
официальные интерфейсы (`claude auth status`, `codex login status`,
`app-server` с `account/read` и `account/rateLimits/read`, имена полей
`usedPercent`/`windowDurationMins`/`resetsAt`, `CLAUDE_CONFIG_DIR`,
`CODEX_HOME`) и **не нашла опровержения** ключевого вывода: у Claude Code нет
zero-inference источника остатка. Проверялись четыре направления: подкоманда
`claude usage`, OTel-метрики, иные JSON-выводы, административные API
(Rate Limits API отдаёт RPM/TPM Console-организации и помечен «unavailable for
individual accounts» — это не 5-часовое окно подписки).

Найдено три фактических неточности, все исправлены: `CODEX_HOME` указывал на
несуществующий каталог (документация прямо требует «the directory must already
exist», и цитата этого требования стояла в шапке модуля, который его не
выполнял); поле организации в `claude auth status` называется `orgId`, а не
`organizationUuid`; комментарий приписывал `CODEX_NON_INTERACTIVE` подавление
автообновления — это переменная установщика.

### 29.5 UI и регрессия

| # | дефект |
|---|---|
| 16 | экран объявлял «воркер не сообщал о провайдерах», когда не ответил СЕРВЕР |
| 17 | диалог правки сдвигал ручную дату на смещение часового пояса при КАЖДОМ открытии (UTC на подстановке против локального времени на разборе) |
| 18 | правка имени молча снимала отметку «лимит почти не использован» — единственный источник предупреждения при неизвестном остатке |
| 19 | три ручки не имели вызова с фронта; `account_kind` нельзя было задать с экрана |
| 20 | стирание ручной даты попадало в неизменяемый журнал как «поля: —» |
| 21 | шаг smoke «изоляция отказа» физически перемещал исполняемый файл вопреки обещанию «ничего не меняет» |

Роли, CSRF, порядок регистрации маршрутов, отсутствие `innerHTML`,
совместимость со старым воркером и миграция живой базы v6→v8 проверены
отдельно — дефектов не найдено.

### 29.6 Регрессия

Первый замер показал 36 «новых» падений `backend/tests`. Причина —
**дрейф окружения, а не код**: worktree этапа лежит внутри главного
репозитория и через поиск вверх видит его `.env`, а базовый worktree в `/tmp`
— нет. Со-локальный базовый worktree на `9b56bd3f` дал ровно те же
**68 падений**, что и HEAD. Новых падений — ноль.

---

## 30. Known limitations

1. **Остаток лимита Claude неизвестен и будет неизвестен**, пока Anthropic не
   даст zero-inference интерфейс. Это не дефект реализации, а свойство
   продукта; обходные пути запрещены заданием и не сделаны.
2. **Контракт источника Codex экспериментальный.** `app-server` помечен
   `[experimental]`. При смене контракта разбор даст `UNKNOWN`, а не мусор,
   но данные пропадут.
3. **Авторизация не выполнена ни у одного провайдера.** Требует действия
   оператора; до этого `quota_state=auth_required` у обоих.
4. **Живого heartbeat с провайдерским снимком через настоящую сеть не было.**
   Путь доказан юнит- и интеграционными тестами против настоящего FastAPI и
   живым `python -m audit_worker providers` на VPS, но воркер после этапа 10
   остановлен, а поднимать туннель ради этого этапа не требовалось.
5. **Источники 5–7 приоритета не реализованы**: наблюдённые отказы по лимиту,
   собственная статистика вызовов. Перечисление и места под них есть.
6. **Периодичность `recurrence` хранится, но не применяется**: ручная дата не
   сдвигается автоматически после наступления. Сдвиг — решение оператора.
7. **Один аккаунт на несколько провайдеров не поддерживается** и не нужен:
   ключ уникальности — пара (провайдер, группа).
8. **Официально санкционированные токены автоматизации сейчас недоступны
   воркеру.** `FORBIDDEN_ENV_NAMES` не пропускает в подпроцесс
   `CLAUDE_CODE_OAUTH_TOKEN`, `CODEX_ACCESS_TOKEN`, `ANTHROPIC_API_KEY` и
   `OPENAI_API_KEY` — это правильная защита секретов ВОРКЕРА, но побочно она
   закрывает все три официально разрешённые схемы автоматизации, оставляя
   единственным путём интерактивный вход человека в provider home. Развилка
   договорная, а не техническая, и решать её нужно ДО подключения моделей к
   конвейеру — см. раздел 33.

---

## 31. Persistent ingress status

**`PERSISTENT_CENTRAL_INGRESS = not_ready`.**

Постоянный HTTPS-вход для центра остаётся отдельным operational blocker с
этапа 10: боевой nginx и DNS на этом этапе не менялись (и по условию задания
не должны были). Для проверок использовался тот же временный проверенный
туннель, что и раньше.

Это не блокирует вердикт этапа: перед production scheduler persistent ingress
станет обязательным, но provider gate от него не зависит.

---

## 32. Readiness for first real audit

Что готово:

* оба CLI установлены официальным способом, изолированы, версии зафиксированы
  и сверены с подписанным манифестом (Claude);
* авторизация опрашивается официальной командой без обращений к моделям;
* лимит Codex наблюдаем структурно; лимит Claude честно помечен неизвестным;
* центр видит состояние, экран показывает источник и достоверность;
* реальный вызов модели закрыт двумя независимыми разрешениями.

Что нужно сделать ПЕРЕД первым малым реальным аудитом:

1. оператору войти в обоих провайдеров (раздел 8; тип учётной записи уже
   выбран — личная подписка у обоих);
2. оператору задать `account_group_id` и `account_kind` на экране;
3. выполнить по одному контрольному запросу на провайдера с явным
   подтверждением;
4. поднять транспорт до центра (туннель либо persistent ingress);
5. включить `AUDIT_WORKER_AUDIT_PIPELINE_ENABLED` и `AUDIT_WORKER_ALLOW_REAL_LLM`
   — это ОТДЕЛЬНОЕ решение, не входящее в этот этап.

---

## 33. Next stage boundaries

Следующий этап — **один малый реальный аудит на одном воркере**. Что в него
входит и что нет:

**Входит:** подключение провайдерского слоя к `audit_pipeline_v1`; учёт
фактического расхода как источника `local_usage_statistics`; обработка
наблюдённого `rate_limited` как источника квоты; сверка артефактов реального
прогона.

**Не входит:** автоматическая выдача заданий (scheduler остаётся выключенным);
режим «потратить до сброса»; пять параллельных аудитов; production rollout.

**Развилка, которую нужно закрыть решением человека, а не кодом.** Сейчас
воркер может говорить с моделью только после интерактивного входа человека в
provider home на чужом VPS. Официально санкционированные схемы —
`claude setup-token` → `CLAUDE_CODE_OAUTH_TOKEN`, `CODEX_ACCESS_TOKEN` для
trusted automation, API-ключ — заблокированы списком запрещённых переменных,
и это сделано ради защиты секретов воркера. Если следующим этапом выбирается
токен или API-ключ, потребуется отдельный, узкий и документированный канал
его доставки: не наследование из окружения воркера, а собственное хранилище
provider home с правами 0600, которое читает ТОЛЬКО адаптер. Молча ослаблять
`FORBIDDEN_ENV_NAMES` нельзя — он защищает от совсем другой утечки.

---

## Приложение А. Переменные окружения этапа

### Воркер

| переменная | по умолчанию | назначение |
|---|---|---|
| `AUDIT_WORKER_PROVIDER_GATE_ENABLED` | `true` | наблюдение за провайдерами (ноль обращений к моделям) |
| `AUDIT_WORKER_PROVIDER_AUTH_CHECK_INTERVAL_SEC` | `300` | частота проверки авторизации |
| `PROVIDER_QUOTA_PROBE_INTERVAL_SEC` | `900` | частота опроса лимита |
| `AUDIT_WORKER_PROVIDER_QUOTA_STALE_AFTER_SEC` | `1800` | когда снимок перестаёт быть действующим |
| `AUDIT_WORKER_PROVIDER_TIMEOUT_SEC` | `60` | таймаут запуска CLI |
| `AUDIT_WORKER_PROVIDER_<CLAUDE\|CODEX>_ACCOUNT_GROUP_ID` | — | привязка к учётной записи |
| `AUDIT_WORKER_PROVIDER_<CLAUDE\|CODEX>_POLICY_BLOCKED` | `false` | комплаенс-стоп на воркере |
| `AUDIT_WORKER_PROVIDER_<CLAUDE\|CODEX>_EXECUTABLE` | — | явный путь к CLI |
| **`AUDIT_WORKER_ALLOW_REAL_PROVIDER_PROBE`** | **`false`** | **разрешение на реальный вызов модели** |

### Центр

| переменная | по умолчанию | назначение |
|---|---|---|
| `DISTRIBUTED_WORKERS_QUOTA_LOW_THRESHOLD_PCT` | `25` | порог `low`; `0` выключает состояние |
| `DISTRIBUTED_WORKERS_QUOTA_STALE_SEC` | `3600` | когда снимок протух на центре |
| `DISTRIBUTED_WORKERS_QUOTA_HISTORY_RETENTION_DAYS` | `120` | глубина истории |
| `DISTRIBUTED_WORKERS_QUOTA_HISTORY_MAX_ROWS_PER_ACCOUNT` | `5000` | предел строк |
| `DISTRIBUTED_WORKERS_QUOTA_HISTORY_MIN_INTERVAL_SEC` | `900` | минимальный интервал повторной записи |

## Приложение Б. Команды оператора

```bash
# Состояние провайдеров на воркере (локально, без сети к центру, без моделей)
cd /home/coder/audit-worker/current
AUDIT_WORKER_ROOT=/home/coder/audit-worker/data \
  /home/coder/audit-worker/venv/bin/python -m audit_worker providers

# То же с локальными путями (в центр не уходит)
… -m audit_worker providers --local

# Живая проверка с центра (read-only, ноль обращений к моделям)
python scripts/smoke_distributed_audit_provider_gate.py \
    --worker-host 176.12.77.31 --worker-user coder

# ОДИН контрольный запрос к модели — два независимых разрешения
AUDIT_WORKER_ALLOW_REAL_PROVIDER_PROBE=true \
  … -m audit_worker provider-probe codex --i-confirm-single-real-request
```

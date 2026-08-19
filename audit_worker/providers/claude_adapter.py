"""ClaudeProviderAdapter — Claude Code CLI.

Что здесь официально доступно и что из этого следует.

АВТОРИЗАЦИЯ — есть машиночитаемый ответ без единого обращения к модели:

    claude auth status            # JSON, по умолчанию
    claude auth status --text     # то же человеку
    # exit 0 = вошли, exit 1 = не вошли

Документация: Claude Code → CLI reference, строка `claude auth status`
(«Show authentication status as JSON… Exits with code 0 if logged in, 1 if
not»). Проверено на 2.1.226: в разлогиненном состоянии возвращается
`{"loggedIn": false, "authMethod": "none", "apiProvider": "firstParty"}`.

ЛИМИТ — официального способа узнать его БЕЗ обращения к модели не существует.

Проверено по официальным источникам и по самому CLI:
  * `claude --help` (2.1.226) не содержит ни одной подкоманды про usage, limits
    или quota: `agents, auth, auto-mode, doctor, gateway, import, install, mcp,
    plugin, project, setup-token, ultrareview, update`;
  * `/usage`, `/cost` и `/status` — команды ИНТЕРАКТИВНОГО сеанса; в `-p` они
    недоступны, а их вывод — текст терминала, не контракт;
  * OpenTelemetry-экспорт Claude Code (docs → Monitoring) содержит
    `claude_code.cost.usage` и `claude_code.token.usage` — это РАСХОД, а не
    остаток лимита; метрик и событий про rate limits в нём нет;
  * единственный официальный машиночитаемый вид остатка — поля
    `rate_limits.five_hour.{used_percentage,resets_at}` и
    `rate_limits.seven_day.{…}` в JSON, который Claude Code подаёт на stdin
    скрипту статусной строки (docs → Customize your status line). Там же
    сказано ключевое: эти поля «appear only for Claude.ai subscribers (Pro/Max)
    **after the first API response in the session**».

Отсюда прямое следствие по §17 задания: ОПРАШИВАТЬ остаток нечем, и
автоматический опрос запрещён. Адаптер не пытается обойти это ни статусной
строкой в фоне, ни разбором интерактивного вывода, ни недокументированными
эндпоинтами, ни запросом к модели ради телеметрии.

ЧТО ВСЁ-ТАКИ ДАЁТ ОСТАТОК — и почему это не противоречит сказанному выше.

Claude Code по ходу ОБЫЧНОЙ работы сам сохраняет последнюю известную
утилизацию в свой конфигурационный файл (`cachedUsageUtilization`). Прочитать
его — значит открыть локальный файл: ни одного подпроцесса, ни одного запроса,
ни одного токена. Разбор живёт в `claude_local_usage` (жёсткий allowlist полей,
см. его шапку), а собранный отсюда снимок несёт оба окна — пятичасовое и
недельное — с `source=local_usage_statistics`, `confidence=medium` и
`source_stability=undocumented`.

Источник недокументирован, поэтому: он не заменяет официальный API, его нет у
пользователя, который ещё не работал через Claude Code, и он НЕ является
основанием для автоматического выбора воркера планировщиком (§6 задания 12J).
Когда кеша нет или его форма незнакома, метод возвращает прежнее честное
«неизвестно» — и это закрывает КВОТУ, а не провайдера: установка и авторизация
живут отдельными полями.
"""
from __future__ import annotations

import json
import time
from typing import Any, Optional, Sequence

from audit_worker.providers import claude_local_usage, errors, quota
from audit_worker.providers.base import (
    PROBE_EXPECTED,
    PROBE_PROMPT,
    AuthStatus,
    ProbeResult,
    ProviderAdapter,
)
from audit_worker.providers.inference import (
    STATUS_ERROR,
    STATUS_SUCCESS,
    ProviderInferenceResult,
    sha256_text,
)
from audit_worker.providers.identity import (
    AUTH_ERROR,
    AUTH_EXPIRED,
    AUTH_LOGGED_IN,
    AUTH_LOGGED_OUT,
    AUTH_UNKNOWN,
)
from audit_worker.providers.paths import PROVIDER_CLAUDE

#: Инструменты, которые контрольный запрос НЕ получает ПОИМЁННО.
#:
#: Список остаётся, но главным барьером больше не является: `--tools=` ниже
#: отключает встроенный набор целиком, и это качественно другая гарантия.
#: Перечисление закрывает только известные имена, а новый инструмент в будущей
#: версии CLI по умолчанию оказался бы РАЗРЕШЁН — то есть список защищает от
#: прошлого, а не от будущего. Оставлен вторым рубежом на случай, если
#: `--tools=` когда-нибудь сменит семантику.
_PROBE_DISALLOWED_TOOLS = (
    "Bash", "Read", "Edit", "Write", "Glob", "Grep", "WebFetch", "WebSearch",
    "Task", "NotebookEdit", "TodoWrite", "AskUserQuestion",
)

#: Флаги, которыми контрольный запрос отключает ЛИЧНЫЙ контекст владельца
#: машины. В ambient-режиме `HOME=/home/<user>`, и без этих флагов запрос
#: получил бы вместе с авторизацией всё остальное содержимое чужого каталога.
#:
#: Измерено на пилотном воркере до включения (док 11b, находка 11):
#: `~/.claude/settings.json` — 91 646 байт с хуком `Stop`, запускающим
#: `python3 ~/.claude/hooks/export_session.py`; `~/.claude/CLAUDE.md` —
#: инструкция вызывать навык `graphify`; `~/.claude/skills/` — сам навык;
#: `settings.local.json` — ещё один слой разрешений. То есть «минимальный
#: запрос с фиксированным промптом» без нейтрализации выполнял бы чужие
#: команды и получал бы чужие инструкции — и результат зависел бы от машины.
#:
#: `--safe-mode`, а НЕ `--bare`: последний тоже выключает лишнее, но вместе с
#: тем требует `ANTHROPIC_API_KEY`/`apiKeyHelper` и никогда не читает OAuth —
#: то есть в ambient-режиме, где авторизация как раз OAuth-подписочная, он
#: сделал бы контрольный запрос невозможным. Проверено на 2.1.220:
#: `claude --safe-mode auth status` → `loggedIn: true`, `authMethod: claude.ai`.
_PROBE_NEUTRALIZE_PERSONAL_CONTEXT = (
    # CLAUDE.md, навыки, плагины, хуки, MCP-серверы, агенты, стили вывода.
    # Авторизация, выбор модели и встроенные инструменты при этом работают.
    "--safe-mode",
    # Второй рубеж по MCP: брать серверы только из `--mcp-config`, которого нет.
    "--strict-mcp-config",
    "--disable-slash-commands",
    # Не оставлять следов сессии в личном каталоге человека.
    "--no-session-persistence",
    # Ни одного слоя настроек: ни user, ни project, ни local.
    "--setting-sources=",
)


def _inference_argv(model: Optional[str] = None) -> list[str]:
    """argv РАБОЧЕГО вызова: константы модуля плюс модель локальной политики.

    Отличие от `_probe_argv()` ровно одно: нет позиционного промпта. Промпт
    уходит через stdin, поэтому argv здесь не содержит ни байта данных задания —
    инвариант I-P5 выполняется дословно, а не «почти».

    Всё остальное совпадает намеренно: нейтрализация личного контекста, полное
    отключение инструментов и один ход. Рабочий вызов не имеет права быть
    «мягче» контрольного — иначе доказанное на probe перестало бы что-то
    говорить о боевом пути.

    ПОЧЕМУ ТЕПЕРЬ ЕСТЬ `--model`, хотя на 11C его не было. Тогда флага не было
    по букве I-P5: идентификатор модели пришёл бы ИЗ ЗАДАНИЯ. Прогон 11C показал
    цену этого решения — конфигурация называла `claude-opus-5`, а ответила
    `claude-opus-4-8[1m]`, модель учётной записи по умолчанию. На 11D источник
    строки другой: её задаёт ЛОКАЛЬНАЯ политика воркера (`model_policy`), файл
    администратора машины рядом с `worker.env`. Для воркера «извне» — это центр
    и задание; собственная конфигурация машины внешним источником не является,
    поэтому I-P5 сохраняется, а слепота «какая модель ответила» — устраняется.

    Флаг `--model` НЕ вариадический (`--model <model>`, один аргумент), поэтому
    форма с пробелом безопасна; вариадические флаги ниже по-прежнему пишутся
    только через `=` (см. докстринг `_probe_argv`).
    """
    argv: list[str] = [*_PROBE_NEUTRALIZE_PERSONAL_CONTEXT]
    if model:
        # Форма с `=`, как у вариадических флагов ниже. `--model` не
        # вариадический (проверено по объявлению CLI: `--model <model>`), то
        # есть поглотить соседний токен он не может. Но форма с `=` снимает
        # ещё и класс «значение начинается с дефиса и разбирается как флаг», а
        # заодно не заставляет читателя помнить, какие флаги здесь какие.
        argv += [f"--model={model}"]
    argv += [
        "--tools=",
        "--disallowed-tools=" + ",".join(_PROBE_DISALLOWED_TOOLS),
        "--permission-mode", "dontAsk",
        "--max-turns", "1",
        "--output-format", "json",
        # Промпт НЕ позиционный: `claude -p` без позиционного аргумента читает
        # его со стандартного ввода (docs → CLI reference, print mode).
        "-p",
    ]
    return argv


def _inference_argv_multimodal(model: Optional[str] = None) -> list[str]:
    """argv рабочего вызова С ИЗОБРАЖЕНИЕМ. Те же константы плюс два флага.

    Почему вообще понадобился отдельный argv. У `claude` 2.1.220 нет флага
    `--image` (проверено по `--help` на пилотном воркере). Единственный
    официальный способ отдать модели картинку, не включая ей ни одного
    инструмента, — подать на stdin сообщение в формате `stream-json` с
    content-блоком `type=image`. Тогда изображение уходит БАЙТАМИ в теле
    запроса: ни `Read`, ни каталог вложений, ни доступ к файловой системе не
    нужны вовсе — это строго сильнее того, что допускал §12 задания 11F.

    Проверено живым вызовом на 176.12.77.31 (11F, capability probe): синтет
    PNG 420×160 с числом, `--tools=`, ответ модели — ровно это число.

    Два вынужденных отличия от `_inference_argv`:

      * `--input-format stream-json` ТРЕБУЕТ `--output-format stream-json`.
        Проверено: с `--output-format json` CLI отказывается ещё до обращения
        к модели («--input-format=stream-json requires
        output-format=stream-json»). Поэтому разбор ответа — построчный NDJSON,
        а не одиночный конверт;
      * `--verbose` обязателен для потокового вывода в print-режиме.

    Разбор строки stdin происходит ЛОКАЛЬНО до обращения к модели (проверено:
    битый JSON даёт `SyntaxError` и нулевой расход), то есть ошибка сборки
    сообщения не стоит вызова.
    """
    argv: list[str] = [*_PROBE_NEUTRALIZE_PERSONAL_CONTEXT]
    if model:
        argv += [f"--model={model}"]
    argv += [
        "--tools=",
        "--disallowed-tools=" + ",".join(_PROBE_DISALLOWED_TOOLS),
        "--permission-mode", "dontAsk",
        "--max-turns", "1",
        "--input-format", "stream-json",
        "--output-format", "stream-json",
        "--verbose",
        "-p",
    ]
    return argv


def _probe_argv() -> list[str]:
    """argv контрольного запроса. Только константы модуля (I-P5).

    ПОРЯДОК И ФОРМА ЗАПИСИ ЗДЕСЬ — ЧАСТЬ КОНТРАКТА, а не стиль.

    `--tools`, `--disallowed-tools`, `--allowedTools`, `--add-dir` объявлены в
    CLI как ВАРИАДИЧЕСКИЕ (`<tools...>`): они забирают все последующие токены
    до следующего флага. Записанные как `--tools ""` они съели бы соседний
    аргумент, и промпт перестал бы быть промптом.

    Это не гипотеза. При подготовке этого этапа команда `claude --tools ""
    doctor` поглотила `doctor` как второе имя инструмента, осталась без
    подкоманды, ушла в print-режим и прочитала промпт из stdin — то есть
    выполнила НЕЗАПЛАНИРОВАННЫЙ запрос к модели (opus-5, 4224 входных и 732
    выходных токена). Поэтому все вариадические флаги записываются
    исключительно в форме `--флаг=значение`, а промпт стоит ПОСЛЕДНИМ.
    """
    return [
        *_PROBE_NEUTRALIZE_PERSONAL_CONTEXT,
        # Форма с `=` обязательна: см. докстринг.
        "--tools=",
        "--disallowed-tools=" + ",".join(_PROBE_DISALLOWED_TOOLS),
        "--permission-mode", "dontAsk",
        "--max-turns", "1",
        "--output-format", "json",
        # `-p` — булев флаг печати; сам промпт идёт позиционным и ПОСЛЕДНИМ,
        # чтобы его нечем было поглотить.
        "-p", PROBE_PROMPT,
    ]


class ClaudeProviderAdapter(ProviderAdapter):
    provider = PROVIDER_CLAUDE
    #: v1 — разбор `claude auth status` (JSON) и `--version`.
    parser_version = "claude-1"

    def provider_env(self) -> dict[str, str]:
        return {
            # Официальный способ перенести ВСЁ, что иначе жило бы в ~/.claude,
            # включая `.credentials.json` на Linux (docs → Authentication →
            # Credential management).
            "CLAUDE_CONFIG_DIR": str(self.home.config_dir),
            # Версия обязана быть стабильной: она входит в контракт разбора и
            # уезжает в историю квот. Фоновое самообновление сделало бы
            # `cli_version` в истории недостоверным задним числом.
            "DISABLE_AUTOUPDATER": "1",
            # Чужой VPS: лишний фоновый трафик с него не наш.
            "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
            # Никакой телеметрии с воркера.
            "DISABLE_TELEMETRY": "1",
            "DISABLE_ERROR_REPORTING": "1",
        }

    # ─── Версия ──────────────────────────────────────────────────────────────
    def version(self) -> Optional[str]:
        result = self.run(["--version"], timeout_sec=min(30.0, self.timeout_sec))
        if not result.ok:
            return None
        # Формат: «2.1.226 (Claude Code)». Берём первый токен — остальное
        # человекочитаемая подпись, на которую опираться нельзя.
        head = (result.stdout or "").strip().split()
        return head[0] if head else None

    # ─── Авторизация (0 обращений к модели) ──────────────────────────────────
    def auth_status(self) -> AuthStatus:
        result = self.run(["auth", "status"], timeout_sec=min(45.0, self.timeout_sec))
        if result.executable_missing:
            return AuthStatus(
                auth_state=AUTH_UNKNOWN,
                auth_method="none",
                error_code=errors.ERR_CLI_MISSING,
                detail="claude не установлен",
            )
        if result.timed_out:
            return AuthStatus(
                auth_state=AUTH_UNKNOWN,
                auth_method="none",
                error_code=errors.ERR_TIMEOUT,
                detail="claude auth status не ответил вовремя",
            )
        try:
            payload = result.json_stdout()
        except (json.JSONDecodeError, ValueError):
            # Ненулевой код с неразобранным выводом — не «сломан формат», а
            # чаще всего понятная ошибка текстом. Классифицируем её.
            code = result.error_code()
            if code == errors.ERR_AUTH_REQUIRED:
                return AuthStatus(
                    auth_state=AUTH_LOGGED_OUT,
                    auth_method="none",
                    error_code=None,
                    detail="не выполнен вход",
                )
            return AuthStatus(
                auth_state=AUTH_ERROR,
                auth_method="none",
                error_code=errors.ERR_MALFORMED_STATUS,
                detail="ответ claude auth status не является JSON",
            )
        if not isinstance(payload, dict):
            return AuthStatus(
                auth_state=AUTH_ERROR,
                auth_method="none",
                error_code=errors.ERR_MALFORMED_STATUS,
                detail="ответ claude auth status не объект",
            )
        return _auth_from_payload(payload, exit_code=result.exit_code)

    # ─── Лимит: официального опроса нет, локальный кеш есть ──────────────────
    def supports_zero_inference_quota(self) -> bool:
        """Да — но не потому, что появился официальный опрос.

        Способ узнать остаток БЕЗ обращения к модели существует ровно один:
        прочитать кеш, который Claude Code сам пишет в свой конфигурационный
        файл по ходу обычной работы (`claude_local_usage`). Это открытие
        локального файла — ноль подпроцессов, ноль запросов, ноль токенов.

        Значение влияет на поведение `ProviderManager`: с `False` снимок
        пересобирался только вместе с проверкой авторизации, и свежий кеш
        доезжал до центра с задержкой в интервал auth-проверки.
        """
        return True

    def quota_source_name(self) -> str:
        return quota.SOURCE_LOCAL_USAGE_STATS

    def quota_source_stability(self) -> str:
        # Ключ `cachedUsageUtilization` не описан ни в одном документе и может
        # исчезнуть в любом обновлении CLI. Ось `stability` заведена ровно для
        # таких случаев: число правдоподобно, контракт — нет.
        return quota.STABILITY_UNDOCUMENTED

    def quota_status(self, *, auth: Optional[AuthStatus] = None) -> quota.ProviderQuotaSnapshot:
        """Остаток из локального кеша Claude Code — либо честное «неизвестно».

        Чего этот метод не делает ни при каких условиях: не обращается к
        модели, не запускает интерактивный сеанс, не читает учётные данные и
        не ходит в сеть. Отсутствие кеша закрывает КВОТУ и только квоту —
        провайдер остаётся установленным и авторизованным (§8 задания).
        """
        now = time.time()
        auth = auth or self.auth_status()
        if self.policy_blocked:
            return quota.unknown_snapshot(
                self.provider,
                auth_state=auth.auth_state,
                quota_state=quota.QUOTA_POLICY_BLOCKED,
                reason="провайдер отключён политикой на этом воркере",
                observed_at=now,
                probe_error_code=errors.ERR_POLICY_BLOCKED,
            )
        if auth.auth_state == AUTH_LOGGED_OUT:
            return quota.unknown_snapshot(
                self.provider,
                auth_state=auth.auth_state,
                quota_state=quota.QUOTA_AUTH_REQUIRED,
                reason="вход не выполнен: лимит подписки неизвестен",
                observed_at=now,
                probe_error_code=errors.ERR_AUTH_REQUIRED,
                cli_version=None,
            )
        if auth.auth_state == AUTH_EXPIRED:
            return quota.unknown_snapshot(
                self.provider,
                auth_state=auth.auth_state,
                quota_state=quota.QUOTA_AUTH_REQUIRED,
                reason="срок действия входа истёк — требуется повторный вход",
                observed_at=now,
                probe_error_code=errors.ERR_AUTH_REQUIRED,
            )
        if auth.auth_state in (AUTH_ERROR, AUTH_UNKNOWN):
            return quota.unknown_snapshot(
                self.provider,
                auth_state=auth.auth_state,
                quota_state=quota.QUOTA_UNKNOWN,
                reason=auth.detail or "состояние авторизации не определено",
                observed_at=now,
                probe_error_code=auth.error_code,
            )
        reading = claude_local_usage.read_local_usage(
            config_dir=self.home.config_dir,
            home_dir=self.home.home,
            now=now,
        )
        if not reading.ok:
            return quota.unknown_snapshot(
                self.provider,
                auth_state=auth.auth_state,
                quota_state=quota.QUOTA_UNKNOWN,
                reason=_quota_unavailable_reason(reading),
                reason_code=reading.reason,
                observed_at=now,
            )
        return _snapshot_from_local_usage(
            reading,
            provider=self.provider,
            auth_state=auth.auth_state,
            account_group_id=self.account_group_id,
            stale_after_sec=self.stale_after_sec,
            low_threshold_pct=self.low_threshold_pct,
        )

    # ─── Контрольный запрос (§18): по умолчанию запрещён ─────────────────────
    def minimal_probe(self, *, confirmed_by_operator: bool = False) -> ProbeResult:
        if self.policy_blocked:
            return ProbeResult(
                provider=self.provider, allowed=False, performed=False,
                error_code=errors.ERR_POLICY_BLOCKED,
                detail="провайдер отключён политикой",
            )
        if not self.inference_allowed:
            return ProbeResult(
                provider=self.provider, allowed=False, performed=False,
                error_code=errors.ERR_POLICY_BLOCKED,
                detail=(
                    "реальный вызов модели запрещён: "
                    "AUDIT_WORKER_ALLOW_REAL_PROVIDER_PROBE=false"
                ),
            )
        if not confirmed_by_operator:
            # Второй рубеж намеренно не совпадает с первым. Флаг окружения
            # выставляет администратор VPS один раз; подтверждение — решение
            # оператора здесь и сейчас. Одного из двух недостаточно.
            return ProbeResult(
                provider=self.provider, allowed=True, performed=False,
                error_code=errors.ERR_POLICY_BLOCKED,
                detail="нет подтверждения оператора на конкретный запуск",
            )
        argv = _probe_argv()
        started = time.time()
        result = self.run(argv, timeout_sec=max(60.0, self.timeout_sec), purpose="probe")
        duration = result.duration_sec
        if result.timed_out:
            return ProbeResult(
                provider=self.provider, allowed=True, performed=True,
                started_at=started, duration_sec=duration,
                exit_code=result.exit_code, error_code=errors.ERR_TIMEOUT,
                detail="контрольный запрос не завершился вовремя",
            )
        payload: Any = None
        try:
            payload = result.json_stdout()
        except (json.JSONDecodeError, ValueError):
            payload = None
        text = ""
        usage: dict[str, Any] = {}
        model: Optional[str] = None
        if isinstance(payload, dict):
            text = str(payload.get("result") or "")
            raw_usage = payload.get("usage")
            if isinstance(raw_usage, dict):
                usage = {k: v for k, v in raw_usage.items() if isinstance(v, (int, float))}
            if isinstance(payload.get("total_cost_usd"), (int, float)):
                usage["total_cost_usd"] = payload["total_cost_usd"]
            model = _model_from_envelope(payload)
        else:
            text = result.stdout or ""
        # Отказ САМОГО ПРОВАЙДЕРА виден только в конверте ответа: CLI может
        # завершиться нулём, при этом внутри стоять `is_error: true` и
        # `api_error_status: 403`. Раньше сюда смотрел лишь код возврата, и
        # 403 «организация отключила доступ» доезжал как `unknown` — то есть
        # неотличимо от сетевого сбоя.
        envelope_code = _provider_refusal_code(payload)
        failed = bool(envelope_code) or not result.ok
        return ProbeResult(
            provider=self.provider,
            allowed=True,
            performed=True,
            started_at=started,
            duration_sec=duration,
            exit_code=result.exit_code,
            model=model,
            matched_expected=PROBE_EXPECTED in text,
            usage=usage,
            error_code=(envelope_code or result.error_code()) if failed else None,
            detail=(
                "провайдер отказал в доступе учётной записи"
                if envelope_code == errors.ERR_ENTITLEMENT_BLOCKED
                else ("контрольный запрос завершился ошибкой" if failed else None)
            ),
        )

    # ─── Рабочий вызов (этап 11C) ────────────────────────────────────────────
    def structured_inference(
        self,
        prompt: str,
        *,
        purpose: str,
        timeout_sec: Optional[float] = None,
        model: Optional[str] = None,
        accepted_reported_models: Sequence[str] = (),
        model_report: str = "required",
    ) -> ProviderInferenceResult:
        blocked = self._inference_gate(confirmed_by_caller=True, purpose=purpose)
        if blocked is not None:
            return blocked
        text = str(prompt or "")
        if not text.strip():
            # Пустой stdin у `claude -p` — не «пустой запрос», а зависание:
            # CLI ждёт ввода. Отказ до запуска дешевле любого таймаута.
            return ProviderInferenceResult(
                provider=self.provider, model=None, status=STATUS_ERROR,
                auth_mode=self.home.auth_mode, error_code=errors.ERR_UNKNOWN,
                detail="пустой промпт: рабочий вызов не выполняется",
            )
        requested_model = str(model).strip() if model else ""
        accepted = tuple(str(x).strip() for x in accepted_reported_models if str(x).strip())
        if requested_model and not accepted:
            # Назначить модель и не назначить, с чем сверять ответ, — значит
            # получить приказ без проверки. Отказ ДО запуска: он бесплатен,
            # а вызов на непроверяемых условиях — нет.
            return ProviderInferenceResult(
                provider=self.provider, model=None, status=STATUS_ERROR,
                auth_mode=self.home.auth_mode,
                error_code=errors.ERR_MODEL_MISMATCH,
                detail=(
                    f"модель {requested_model!r} назначена, но список допустимых "
                    "фактических идентификаторов пуст: сверять ответ не с чем"
                ),
            )
        result = self.run(
            _inference_argv(requested_model or None),
            # Явный срок вызывающего ПОБЕЖДАЕТ. У контрольного запроса стоит
            # пол в 120 с, потому что там срок ничей: команду даёт человек и
            # бюджета этапа не существует. У рабочего вызова бюджет есть, его
            # знает этап конвейера, и подменять его нижней границей значило бы
            # держать процесс живым дольше, чем этап готов ждать.
            timeout_sec=(
                float(timeout_sec) if timeout_sec
                else max(120.0, float(self.timeout_sec))
            ),
            stdin_text=text,
            purpose=purpose,
        )
        envelope: Any = None
        try:
            envelope = result.json_stdout()
        except (json.JSONDecodeError, ValueError):
            envelope = None
        return self._finalize_inference(
            result, envelope,
            requested_model=requested_model, accepted=accepted,
        )

    # ─── Рабочий вызов С ИЗОБРАЖЕНИЕМ (этап 11F) ─────────────────────────────
    def structured_inference_multimodal(
        self,
        prompt: str,
        *,
        images: Sequence[tuple[str, bytes]],
        purpose: str,
        timeout_sec: Optional[float] = None,
        model: Optional[str] = None,
        accepted_reported_models: Sequence[str] = (),
        model_report: str = "required",
    ) -> ProviderInferenceResult:
        """То же, что `structured_inference`, но с изображениями в теле запроса.

        `images` — последовательность пар `(media_type, raw_bytes)`. Байты
        кодируются в base64 и уходят content-блоками `type=image` в ОДНОМ
        сообщении вместе с текстом задания. Файловая система при этом не
        участвует ни с одной стороны: у модели по-прежнему `--tools=`, никакого
        каталога вложений не создаётся, и путь к кропу модели не сообщается.

        Порядок блоков — сначала изображения, потом текст: инструкция, стоящая
        после материала, надёжнее удерживает формат ответа.
        """
        import base64

        blocked = self._inference_gate(confirmed_by_caller=True, purpose=purpose)
        if blocked is not None:
            return blocked
        text = str(prompt or "")
        if not text.strip():
            return ProviderInferenceResult(
                provider=self.provider, model=None, status=STATUS_ERROR,
                auth_mode=self.home.auth_mode, error_code=errors.ERR_UNKNOWN,
                detail="пустой промпт: рабочий вызов не выполняется",
            )
        if not images:
            return ProviderInferenceResult(
                provider=self.provider, model=None, status=STATUS_ERROR,
                auth_mode=self.home.auth_mode, error_code=errors.ERR_UNKNOWN,
                detail=(
                    "мультимодальный вызов без изображений: молчаливый переход "
                    "на текстовый путь запрещён — этап получил бы анализ чертежа "
                    "без чертежа"
                ),
            )
        requested_model = str(model).strip() if model else ""
        accepted = tuple(str(x).strip() for x in accepted_reported_models if str(x).strip())
        if requested_model and not accepted:
            return ProviderInferenceResult(
                provider=self.provider, model=None, status=STATUS_ERROR,
                auth_mode=self.home.auth_mode,
                error_code=errors.ERR_MODEL_MISMATCH,
                detail=(
                    f"модель {requested_model!r} назначена, но список допустимых "
                    "фактических идентификаторов пуст: сверять ответ не с чем"
                ),
            )
        content: list[dict[str, Any]] = []
        for media_type, blob in images:
            if not blob:
                return ProviderInferenceResult(
                    provider=self.provider, model=None, status=STATUS_ERROR,
                    auth_mode=self.home.auth_mode, error_code=errors.ERR_UNKNOWN,
                    detail="пустое изображение во вложении: вызов не выполняется",
                )
            content.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": str(media_type),
                    "data": base64.b64encode(blob).decode("ascii"),
                },
            })
        content.append({"type": "text", "text": text})
        line = json.dumps(
            {"type": "user", "message": {"role": "user", "content": content}},
            ensure_ascii=False,
        )
        result = self.run(
            _inference_argv_multimodal(requested_model or None),
            timeout_sec=(
                float(timeout_sec) if timeout_sec
                else max(120.0, float(self.timeout_sec))
            ),
            stdin_text=line + "\n",
            purpose=purpose,
        )
        envelope = parse_stream_json(result.stdout or "")
        if envelope is None:
            # В потоке нет итогового объекта `{"type":"result"}`. Уйти дальше с
            # СЫРЫМ NDJSON нельзя: `_first_json_object` подобрал бы первое
            # событие потока — служебную инициализацию CLI с `session_id` и
            # составом инструментов — и при нулевом коде возврата это было бы
            # записано в журнал как успешный ответ модели.
            return ProviderInferenceResult(
                provider=self.provider, model=None, status=STATUS_ERROR,
                duration_ms=int(result.duration_sec * 1000),
                exit_code=result.exit_code,
                auth_mode=self.home.auth_mode,
                error_code=errors.ERR_MALFORMED_STATUS,
                detail=(
                    "потоковый вывод оборван: итогового объекта result нет. "
                    "Служебное событие CLI не выдаётся за ответ модели"
                ),
                raw_sha256=sha256_text(result.stdout or ""),
                raw_bytes=len((result.stdout or "").encode("utf-8", "replace")),
            )
        return self._finalize_inference(
            result, envelope,
            requested_model=requested_model, accepted=accepted,
        )

    def _finalize_inference(
        self,
        result: Any,
        envelope: Any,
        *,
        requested_model: str,
        accepted: Sequence[str],
    ) -> ProviderInferenceResult:
        """Разбор ответа CLI, общий для текстового и мультимодального вызова.

        Вынесен из `structured_inference` без изменения поведения: два вызова
        отличаются только тем, КАК собран stdin и КАК разобран stdout, а всё, что
        идёт после — сверка модели, извлечение JSON, классификация ошибки — обязано
        быть буквально одним и тем же кодом. Иначе строгий гейт модели существовал
        бы на одном пути и отсутствовал на другом.
        """
        duration_ms = int(result.duration_sec * 1000)
        if result.timed_out:
            return ProviderInferenceResult(
                provider=self.provider, model=None, status=STATUS_ERROR,
                duration_ms=duration_ms, exit_code=result.exit_code,
                auth_mode=self.home.auth_mode, error_code=errors.ERR_TIMEOUT,
                detail="рабочий вызов не завершился вовремя",
                raw_sha256=sha256_text(result.stdout or ""),
                raw_bytes=len((result.stdout or "").encode("utf-8", "replace")),
            )
        answer_text = ""
        usage: dict[str, Any] = {}
        model: Optional[str] = None
        if isinstance(envelope, dict):
            answer_text = str(envelope.get("result") or "")
            usage = _usage_from_payload(envelope)
            model = _model_from_envelope(envelope)
        else:
            # Конверт не разобран — работаем с сырым выводом. Это законный
            # случай (например ошибка CLI текстом), и он обязан дойти до
            # проверки как ошибка, а не как пустой успех.
            answer_text = result.stdout or ""
        payload = self._first_json_object(answer_text)
        # `is_error` в конверте — независимый от кода возврата признак отказа.
        # Проверять его обязательно: CLI умеет завершиться нулём, сообщив об
        # ошибке полем, и без этой строки такой ответ прошёл бы как успех.
        envelope_error = bool(
            isinstance(envelope, dict) and envelope.get("is_error") is True
        )
        ok = result.ok and payload is not None and not envelope_error
        # Сверка фактической модели — ПОСЛЕДНИЙ гейт и самый строгий (этап 11D).
        # Стоит после разбора намеренно: результат уже есть, вызов уже оплачен,
        # и его надо ЗАПИСАТЬ в журнал (это делает мост), но объявить успехом
        # ответ чужой модели нельзя. Отсутствующий идентификатор тоже считается
        # несовпадением: «не знаем, кто ответил» — не то же самое, что «ответила
        # назначенная».
        model_mismatch = ""
        if requested_model:
            reported = (model or "").strip()
            if not reported:
                model_mismatch = (
                    f"CLI не сообщил фактическую модель; назначена {requested_model!r}"
                )
            elif reported not in accepted:
                model_mismatch = (
                    f"фактическая модель {reported!r} не входит в допустимые "
                    f"{list(accepted)} для назначенной {requested_model!r}"
                )
        if model_mismatch:
            ok = False
        return ProviderInferenceResult(
            provider=self.provider,
            model=model,
            status=STATUS_SUCCESS if ok else STATUS_ERROR,
            result=payload or {},
            usage=usage,
            duration_ms=duration_ms,
            exit_code=result.exit_code,
            auth_mode=self.home.auth_mode,
            error_code=(
                None if ok
                else (
                    # Отказ провайдера — первым: он объясняет причину точнее
                    # любого кода возврата. 403 «организация отключила доступ»
                    # иначе доехал бы как `unknown` (если CLI вернул ненулевой
                    # код) или как `malformed_status` (если нулевой), и в обоих
                    # случаях оператор искал бы поломку не там.
                    _provider_refusal_code(envelope)
                    or (
                        result.error_code() if not result.ok
                        else (
                            errors.ERR_MODEL_MISMATCH if model_mismatch
                            else errors.ERR_MALFORMED_STATUS
                        )
                    )
                )
            ),
            detail=(
                None if ok
                else (
                    _cli_failure_detail(result, answer_text) if not result.ok
                    else (
                        model_mismatch if model_mismatch
                        else "ответ модели не содержит JSON-объекта"
                    )
                )
            ),
            raw_sha256=sha256_text(answer_text),
            raw_bytes=len(answer_text.encode("utf-8", "replace")),
        )


#: Сколько символов сообщения CLI попадает в `detail`. Не «на всякий случай», а
#: рубеж: ошибка провайдера — короткая служебная строка, а вот
#: `invalid_request_error` теоретически способна процитировать кусок входа.
#: Обрезка держит диагностику полезной и не превращает поле в канал утечки.
_CLI_FAILURE_DETAIL_MAX_CHARS = 400


#: Человекочитаемое пояснение к каждому исходу чтения кеша. Ключ — код причины
#: из `claude_local_usage`; свободного текста «как получилось» здесь нет, и это
#: намеренно: оператор должен видеть одну из немногих понятных ситуаций, а не
#: сообщение, которое каждый раз выглядит по-новому.
_QUOTA_REASON_TEXT: dict[str, str] = {
    claude_local_usage.REASON_MISSING: (
        "Claude Code ещё не сохранил локальные данные об использовании. "
        "Официального машиночитаемого остатка у него нет, а кеш появляется "
        "только после реальных обращений к модели этим пользователем"
    ),
    claude_local_usage.REASON_SCHEMA_UNSUPPORTED: (
        "локальный кеш использования Claude Code имеет неизвестную форму — "
        "остаток не читается (источник недокументирован и мог измениться)"
    ),
    claude_local_usage.REASON_NO_SOURCE: (
        "у Claude Code нет поддерживаемого способа сообщить остаток лимита "
        "без обращения к модели"
    ),
}


def _quota_unavailable_reason(reading: claude_local_usage.LocalUsageReading) -> str:
    """Причина отсутствия остатка. Код причины остаётся в тексте дословно.

    Код нужен потому, что до браузера доезжает не этот текст (проводной снимок
    его не несёт), а состояние снимка; текст же читает тот, кто смотрит
    диагностику воркера, и ему нужна опора, по которой можно грепать.
    """
    base = _QUOTA_REASON_TEXT.get(
        reading.reason, _QUOTA_REASON_TEXT[claude_local_usage.REASON_NO_SOURCE]
    )
    return f"[{reading.reason}] {base}"


def _snapshot_from_local_usage(
    reading: claude_local_usage.LocalUsageReading,
    *,
    provider: str,
    auth_state: str,
    account_group_id: Optional[str],
    stale_after_sec: float,
    low_threshold_pct: Optional[float],
) -> quota.ProviderQuotaSnapshot:
    """Чтение кеша → нормализованный снимок с ДВУМЯ окнами.

    Три решения, каждое из которых легко принять неправильно.

    `observed_at` — метка САМОГО кеша, а не момент чтения файла. Иначе снимок
    восьмичасовой давности выглядел бы сделанным только что, и весь механизм
    просроченности (`stale_after`) стал бы декорацией.

    Остаток берётся по САМОМУ ОГРАНИЧИВАЮЩЕМУ окну: пятичасовое бывает
    свободно при почти выбранном недельном, и «осталось 84 %» в этом случае
    неправда. Дата сброса берётся у ТОГО ЖЕ окна — процент и дата обязаны
    относиться к одному лимиту.

    Достоверность — `medium`. Число сообщил сам CLI (не мы его вывели), но
    сообщил недокументированным полем и с задержкой кеша, поэтому `high`,
    которое стоит у Codex с его структурным RPC, здесь было бы завышением.
    """
    confidence = quota.CONFIDENCE_MEDIUM
    source = quota.SOURCE_LOCAL_USAGE_STATS
    windows = tuple(
        quota.QuotaWindow(
            window_id=item.window_id,
            source=source,
            confidence=confidence,
            used_pct=item.used_pct,
            remaining_pct=item.remaining_pct,
            reset_at=item.reset_at,
            duration_sec=item.duration_sec,
        )
        for item in reading.windows
    )
    primary = min(windows, key=lambda w: (w.remaining_pct if w.remaining_pct is not None else 101.0))
    secondaries = tuple(w for w in windows if w is not primary)

    next_reset = primary.reset_at
    if next_reset is None:
        others = [w.reset_at for w in secondaries if w.reset_at is not None]
        next_reset = min(others) if others else None

    observed_at = float(reading.fetched_at or 0.0)
    snapshot = quota.ProviderQuotaSnapshot(
        provider=provider,
        quota_state=quota.QUOTA_READY,
        observed_at=observed_at,
        source=source,
        confidence=confidence,
        auth_state=auth_state,
        account_group_id=account_group_id,
        stale_after=observed_at + float(stale_after_sec),
        primary_window=primary,
        secondary_windows=secondaries,
        next_reset_at=next_reset,
        estimated_remaining_pct=primary.remaining_pct,
        raw_remaining_supported=True,
        source_stability=quota.STABILITY_UNDOCUMENTED,
        parser_version=claude_local_usage.PARSER_VERSION,
        detail=(
            f"[{claude_local_usage.REASON_AVAILABLE}] локальный кеш Claude Code, "
            f"окно {primary.window_id}"
        ),
        reason_code=quota.REASON_LOCAL_CACHE_AVAILABLE,
    )
    return quota.apply_low_threshold(snapshot, low_threshold_pct=low_threshold_pct)


def _provider_refusal_code(payload: Any) -> Optional[str]:
    """Код отказа ПРОВАЙДЕРА из конверта ответа CLI.

    Два поля, и оба нужны. `api_error_status` говорит, что ответил не CLI, а
    сервер; текст в `result` говорит, ЧТО именно ответил. Судить по одному
    статусу нельзя: 403 бывает и от истёкшего входа, и от запрета организации,
    а действия оператора в этих случаях противоположные — перелогиниться либо
    идти к администратору.

    Возвращает `None`, когда отказа провайдера в конверте нет: тогда исход
    решает код возврата процесса, как и раньше.
    """
    if not isinstance(payload, dict):
        return None
    status = payload.get("api_error_status")
    is_error = bool(payload.get("is_error")) or str(
        payload.get("terminal_reason") or ""
    ) == "api_error"
    if not is_error and status is None:
        return None
    text = " ".join(
        str(payload.get(key) or "")
        for key in ("result", "error", "message", "terminal_reason")
    )
    code = errors.classify_text(text)
    if code:
        return code
    if isinstance(status, (int, float)) and not isinstance(status, bool):
        number = int(status)
        if number in (401, 403):
            # Сервер отказал, но словами, которых мы не знаем. `auth_required`
            # здесь честнее `unknown`: он хотя бы указывает на учётную запись.
            return errors.ERR_AUTH_REQUIRED
        if number == 429:
            return errors.ERR_RATE_LIMITED
        if number >= 500:
            return errors.ERR_PROVIDER_UNAVAILABLE
    return errors.ERR_UNKNOWN if is_error else None


def _cli_failure_detail(result: Any, answer_text: str) -> str:
    """Почему CLI завершился ошибкой — СЛОВАМИ САМОГО CLI, а не константой.

    Раньше здесь стояла строка «CLI завершился ошибкой», а текст ответа нигде
    не сохранялся: в журнал попытки уезжал только его `sha256`. Цена вскрылась
    на 11E — единственный оплаченный вызов этапа вернул ошибку в 99 байт, и
    разобрать её оказалось нечем: бюджет вызовов исчерпан, повтор запрещён,
    подбор строки по хэшу ничего не дал. Диагностическое сообщение провайдера —
    не данные заказчика, и терять его на границе, где деньги уже потрачены,
    нельзя.

    Источники берутся в порядке информативности: разобранный `result` конверта,
    затем сырой stdout, затем stderr. Оба потока приходят сюда УЖЕ пройдя
    `redaction.redact` в `run()`, поэтому учётные данные в них не попадают.
    """
    parts: list[str] = []
    for chunk in (answer_text, getattr(result, "stdout", ""), getattr(result, "stderr", "")):
        text = " ".join(str(chunk or "").split())
        if text and text not in parts:
            parts.append(text)
    joined = " | ".join(parts)
    if not joined:
        return "CLI завершился ошибкой (вывод пуст)"
    if len(joined) > _CLI_FAILURE_DETAIL_MAX_CHARS:
        joined = joined[:_CLI_FAILURE_DETAIL_MAX_CHARS] + "…"
    return f"CLI завершился ошибкой: {joined}"


def _usage_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Числовой расход из конверта `--output-format json`."""
    usage: dict[str, Any] = {}
    raw = payload.get("usage")
    if isinstance(raw, dict):
        usage = {k: v for k, v in raw.items() if isinstance(v, (int, float))}
    if isinstance(payload.get("total_cost_usd"), (int, float)):
        usage["total_cost_usd"] = payload["total_cost_usd"]
    if isinstance(payload.get("num_turns"), int):
        usage["num_turns"] = payload["num_turns"]
    return usage


def parse_stream_json(stdout: str) -> Optional[dict[str, Any]]:
    """Свести NDJSON потокового вывода к тому же конверту, что даёт `json`.

    `--output-format stream-json` печатает по объекту в строке: события системы,
    сообщения ассистента и ПОСЛЕДНИМ — объект `{"type":"result", …}` с текстом
    ответа, расходом, стоимостью и `modelUsage`. Он и есть конверт: остальные
    строки нужны только чтобы достать фактическую модель из сообщения
    ассистента, если в итоговом объекте её нет.

    Возвращает `None`, если ни одной валидной строки нет — вызывающий обязан
    обработать это как ошибку CLI, а не как пустой успех.
    """
    envelope: Optional[dict[str, Any]] = None
    assistant_model: Optional[str] = None
    for raw_line in (stdout or "").splitlines():
        line = raw_line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            continue
        if not isinstance(event, dict):
            continue
        kind = event.get("type")
        if kind == "result":
            envelope = event
        elif kind == "assistant" and assistant_model is None:
            message = event.get("message")
            if isinstance(message, dict) and isinstance(message.get("model"), str):
                assistant_model = message["model"].strip() or None
    if envelope is None:
        return None
    # Модель из сообщения ассистента приоритетнее: в итоговом объекте поля
    # `model` нет вовсе, а `modelUsage` смешивает рабочую модель со служебными.
    if assistant_model and not isinstance(envelope.get("model"), str):
        envelope = {**envelope, "model": assistant_model}
    return envelope


#: Модели, которые Claude Code запускает ДЛЯ СЕБЯ и которые не имеют отношения
#: к заданию этапа: служебная классификация, оценка «thinking tokens», заголовки
#: сессии. Они появляются в `modelUsage` рядом с рабочей моделью.
#:
#: Это не гипотеза. Живой вызов 11F (capability probe на .31) вернул
#: `modelUsage` с ДВУМЯ ключами: `claude-haiku-4-5-20251001` (543 входных,
#: 19 выходных, $0,000638) и `claude-opus-5` (запрошенная). Служебный ключ
#: стоял ПЕРВЫМ.
_AUXILIARY_MODEL_PREFIXES = ("claude-haiku-",)


def _model_from_envelope(payload: dict[str, Any]) -> Optional[str]:
    """Какая модель ФАКТИЧЕСКИ отвечала на задание.

    Прежняя реализация брала `next(iter(modelUsage))` — первый ключ словаря.
    Пока в `modelUsage` была одна запись, это работало; но CLI кладёт туда и
    СВОИ служебные модели (см. `_AUXILIARY_MODEL_PREFIXES`), и порядок ключей
    определяется порядком вставки, а не важностью. То есть строгий гейт
    «сообщённая модель == запрошенной» мог отвергнуть совершенно нормальный
    ответ Opus только потому, что CLI успел до него сходить в Haiku за
    заголовком сессии — и это выглядело бы как подмена модели.

    Порядок разрешения, от надёжного к запасному:

      1. поле `model` верхнего уровня, если оно есть;
      2. запись `modelUsage` с наибольшим числом ВЫХОДНЫХ токенов среди
         неслужебных: рабочий ответ длиннее служебного на порядок;
      3. первая неслужебная запись;
      4. первая запись вообще — чтобы «не знаю, кто ответил» не превращалось
         в `None` там, где ответ на самом деле есть.
    """
    top = payload.get("model")
    if isinstance(top, str) and top.strip():
        return top.strip()
    model_usage = payload.get("modelUsage")
    if not isinstance(model_usage, dict) or not model_usage:
        return None
    names = [str(k) for k in model_usage]
    primary = [n for n in names if not n.startswith(_AUXILIARY_MODEL_PREFIXES)]
    pool = primary or names

    def _out_tokens(name: str) -> int:
        row = model_usage.get(name)
        if not isinstance(row, dict):
            return 0
        value = row.get("outputTokens", row.get("output_tokens", 0))
        return int(value) if isinstance(value, (int, float)) else 0

    return max(pool, key=_out_tokens)


def _auth_from_payload(payload: dict[str, Any], *, exit_code: Optional[int]) -> AuthStatus:
    """Разбор `claude auth status`.

    Схема documented как «Show authentication status as JSON» без перечисления
    полей, поэтому разбор ЗАЩИТНЫЙ: неизвестные поля игнорируются, отсутствие
    ожидаемых не считается ошибкой формата, а код возврата используется как
    независимый признак (он документирован явно: 0 = вошли, 1 = нет).
    """
    logged_in = payload.get("loggedIn")
    if logged_in is None:
        logged_in = payload.get("logged_in")
    method = str(payload.get("authMethod") or payload.get("auth_method") or "unknown")
    provider_kind = payload.get("apiProvider") or payload.get("api_provider")
    plan = payload.get("planType") or payload.get("plan") or payload.get("subscriptionType")
    # `email` и `organization` НЕ попадают ни в `raw_public`, ни к центру:
    # первое — персональные данные, второе — сведения об организации.
    # `orgId` — фактическое имя поля в ответе CLI (проверено на живом
    # выводе). Прежний `organizationUuid` был выдумкой: ветка-фолбэк не
    # срабатывала никогда, и у учётной записи без e-mail (Console/SSO)
    # отпечаток молча оставался пустым.
    identifier = payload.get("email") or payload.get("orgId")

    if logged_in is True:
        state = AUTH_LOGGED_IN
    elif logged_in is False:
        state = AUTH_LOGGED_OUT
    elif exit_code == 0:
        state = AUTH_LOGGED_IN
    elif exit_code == 1:
        state = AUTH_LOGGED_OUT
    else:
        state = AUTH_UNKNOWN

    expired = payload.get("expired")
    if state == AUTH_LOGGED_IN and expired is True:
        state = AUTH_EXPIRED

    return AuthStatus(
        auth_state=state,
        auth_method=method if state != AUTH_LOGGED_OUT else "none",
        plan_type=str(plan) if plan else None,
        stable_identifier=str(identifier) if identifier else None,
        raw_public={
            "authMethod": method,
            "apiProvider": str(provider_kind) if provider_kind else None,
        },
        detail=None if state == AUTH_LOGGED_IN else "вход не выполнен",
    )

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

Отсюда прямое следствие по §17 задания: получить остаток можно только ЦЕНОЙ
запроса к модели, а значит автоматический опрос квоты ЗАПРЕЩЁН. Адаптер не
пытается обойти это ни статусной строкой в фоне, ни разбором интерактивного
вывода, ни недокументированными эндпоинтами. Он честно возвращает
`quota_state="unknown"`, `estimated_remaining_pct=None`,
`raw_remaining_supported=False` — и центр показывает «неизвестен».

Что остаётся вместо опроса (и это реализуемо на следующих этапах, а не здесь):
наблюдённые отказы по лимиту из настоящих прогонов, собственная статистика
вызовов и ручные даты сброса от оператора.
"""
from __future__ import annotations

import json
import time
from typing import Any, Optional

from audit_worker.providers import errors, quota
from audit_worker.providers.base import (
    PROBE_EXPECTED,
    PROBE_PROMPT,
    AuthStatus,
    ProbeResult,
    ProviderAdapter,
)
from audit_worker.providers.identity import (
    AUTH_ERROR,
    AUTH_EXPIRED,
    AUTH_LOGGED_IN,
    AUTH_LOGGED_OUT,
    AUTH_UNKNOWN,
)
from audit_worker.providers.paths import PROVIDER_CLAUDE

#: Инструменты, которые контрольный запрос НЕ получает. Список явный, а не
#: «всё, кроме»: новый инструмент в будущей версии CLI должен быть запрещён по
#: умолчанию, поэтому вместе со списком идёт `--permission-mode dontAsk`.
_PROBE_DISALLOWED_TOOLS = (
    "Bash", "Read", "Edit", "Write", "Glob", "Grep", "WebFetch", "WebSearch",
    "Task", "NotebookEdit", "TodoWrite", "AskUserQuestion",
)


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

    # ─── Лимит: официального zero-inference способа нет ──────────────────────
    def supports_zero_inference_quota(self) -> bool:
        return False

    def quota_source_name(self) -> str:
        return quota.SOURCE_UNAVAILABLE

    def quota_source_stability(self) -> str:
        return quota.STABILITY_NOT_APPLICABLE

    def quota_status(self, *, auth: Optional[AuthStatus] = None) -> quota.ProviderQuotaSnapshot:
        """Всегда «неизвестно» — и это ЕДИНСТВЕННЫЙ честный ответ.

        Метод намеренно не имеет ветки, которая при каких-то условиях вернула
        бы процент: любая такая ветка потребовала бы обращения к модели, то
        есть автоматического расхода подписки на телеметрию (запрещено §17).
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
        return quota.unknown_snapshot(
            self.provider,
            auth_state=auth.auth_state,
            quota_state=quota.QUOTA_UNKNOWN,
            reason=(
                "у Claude Code нет официального машиночитаемого способа узнать "
                "остаток лимита без обращения к модели: поля rate_limits "
                "публикуются только скрипту статусной строки и только после "
                "первого ответа API в сеансе. Автоматический опрос запрещён — "
                "он расходовал бы подписку ради телеметрии"
            ),
            observed_at=now,
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
        argv = [
            "-p", PROBE_PROMPT,
            "--output-format", "json",
            "--max-turns", "1",
            # Ничего не разрешено сверх явного списка; список пуст.
            "--permission-mode", "dontAsk",
            "--disallowed-tools", " ".join(_PROBE_DISALLOWED_TOOLS),
        ]
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
            model_usage = payload.get("modelUsage")
            if isinstance(model_usage, dict) and model_usage:
                model = next(iter(model_usage))
        else:
            text = result.stdout or ""
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
            error_code=None if result.ok else result.error_code(),
            detail=None if result.ok else "контрольный запрос завершился ошибкой",
        )


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
    identifier = payload.get("email") or payload.get("organizationUuid")

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

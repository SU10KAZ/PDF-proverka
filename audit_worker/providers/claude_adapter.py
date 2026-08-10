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
from typing import Any, Optional, Sequence

from audit_worker.providers import errors, quota
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

    # ─── Рабочий вызов (этап 11C) ────────────────────────────────────────────
    def structured_inference(
        self,
        prompt: str,
        *,
        purpose: str,
        timeout_sec: Optional[float] = None,
        model: Optional[str] = None,
        accepted_reported_models: Sequence[str] = (),
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
        envelope: Any = None
        try:
            envelope = result.json_stdout()
        except (json.JSONDecodeError, ValueError):
            envelope = None
        answer_text = ""
        usage: dict[str, Any] = {}
        model: Optional[str] = None
        if isinstance(envelope, dict):
            answer_text = str(envelope.get("result") or "")
            usage = _usage_from_payload(envelope)
            model_usage = envelope.get("modelUsage")
            if isinstance(model_usage, dict) and model_usage:
                model = next(iter(model_usage))
            if not model and isinstance(envelope.get("model"), str):
                model = envelope["model"]
        else:
            # Конверт не разобран — работаем с сырым выводом. Это законный
            # случай (например ошибка CLI текстом), и он обязан дойти до
            # проверки как ошибка, а не как пустой успех.
            answer_text = result.stdout or ""
        payload = self._first_json_object(answer_text)
        ok = result.ok and payload is not None
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
                    result.error_code() if not result.ok
                    else (
                        errors.ERR_MODEL_MISMATCH if model_mismatch
                        else errors.ERR_MALFORMED_STATUS
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

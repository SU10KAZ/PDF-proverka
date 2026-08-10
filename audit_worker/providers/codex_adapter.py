"""CodexProviderAdapter — Codex CLI.

Логика Claude сюда НЕ переносится (прямое требование §20 задания): у Codex
другой способ авторизации, другая единица лимита и — в отличие от Claude —
официальный машиночитаемый интерфейс остатка, не требующий обращения к модели.

АВТОРИЗАЦИЯ:

    codex login status     # exit 0, когда учётные данные есть
    codex login            # браузерный вход
    codex login --device-auth   # вход по коду для машин без браузера (beta)
    codex login --with-api-key  # ключ через stdin

Документация: Codex → Authentication. Учётные данные лежат в
`$CODEX_HOME/auth.json`; `CODEX_HOME` официально описан как «root for Codex
state, including config, auth, logs, sessions» (Codex → Environment variables).

ЛИМИТ — есть официальный structured-интерфейс БЕЗ обращения к модели:

    codex app-server        # JSON-RPC 2.0 по stdio
      → initialize / initialized
      → account/read              {account:{type,email,planType}, requiresOpenaiAuth}
      → account/rateLimits/read   {rateLimits:{limitId,primary,secondary,…},
                                   rateLimitsByLimitId:{…}}

Поля окна документированы дословно: `usedPercent` — «current usage within the
quota window», `windowDurationMins` — «the quota window length», `resetsAt` —
«a Unix timestamp (seconds) for the next reset». Есть и первичное, и вторичное
окно (`primary`/`secondary`) — то самое различие, о котором спрашивает §20.

Честная оговорка, которая обязана дойти до оператора: подкоманда `app-server`
в собственном `codex --help` помечена `[experimental]`, а документация прямо
говорит «The app-server command and WebSocket transport are experimental and
aren't supported for production workloads». Поэтому снимок несёт
`source_stability="experimental"`. Достоверность САМОГО ЧИСЛА при этом высокая
(его отдаёт первая сторона структурой), а «экспериментальность» означает риск
смены контракта — и на этот случай разбор защитный: не разобрали → UNKNOWN, а
не выдуманное значение.

Мы НЕ используем `capabilities.experimentalApi`: остаёмся на стабильной части
поверхности app-server. Оба используемых метода в неё входят.

Чего здесь нет и не будет: обращения к `GET /api/codex/usage` и любым другим
недокументированным HTTP-эндпоинтам, разбора веб-кабинета и cookie (§11).
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Optional, Sequence

from audit_worker.providers import errors, quota
from audit_worker.providers.base import (
    PROBE_EXPECTED,
    PROBE_PROMPT,
    AuthStatus,
    JsonRpcResult,
    ProbeResult,
    ProviderAdapter,
)
from audit_worker.providers.identity import (
    AUTH_ERROR,
    AUTH_LOGGED_IN,
    AUTH_LOGGED_OUT,
    AUTH_UNKNOWN,
)
from audit_worker.providers.inference import (
    STATUS_ERROR,
    STATUS_SUCCESS,
    ProviderInferenceResult,
    sha256_text,
)
from audit_worker.providers.paths import PROVIDER_CODEX

#: Хвост argv рабочего вызова. ТОЛЬКО константы (I-P5): промпт уходит через
#: stdin, а `-` — документированный способ Codex сказать «читай инструкции со
#: стандартного ввода» (`codex exec --help`: «If not provided as an argument (or
#: if `-` is used), instructions are read from stdin»).
#:
#: Набор флагов совпадает с контрольным запросом дословно и по той же причине,
#: что у Claude: рабочий путь не имеет права быть мягче проверенного.
_INFERENCE_ARGV: tuple[str, ...] = (
    "exec",
    "--json",
    "--sandbox", "read-only",
    "--skip-git-repo-check",
    "--ephemeral",
    "--ignore-rules",
    "--ignore-user-config",
    "-",
)

#: Хвост argv БЕЗ терминатора `-`: между флагами нейтрализации и `-` вставляются
#: значения, вычисленные самим адаптером (модель локальной политики, пути
#: вложений). Порядок обязателен: `codex exec` берёт промпт позиционно, и любой
#: флаг со значением обязан стоять ДО него.
_INFERENCE_ARGV_HEAD: tuple[str, ...] = _INFERENCE_ARGV[:-1]


def _inference_argv(
    model: Optional[str] = None,
    image_paths: Sequence[Path] = (),
) -> list[str]:
    """argv рабочего вызова: константы модуля + значения локальной политики.

    ПОЧЕМУ ПОЯВИЛСЯ `--model`, хотя до 11H его у Codex не было. До этого этапа
    адаптер ОТКАЗЫВАЛ на любом явном `model`: «реализовано и проверено только
    для Claude». Отказ был честнее молчаливого игнорирования, но он же делал
    Codex непригодным для конвейера — мост (`pipeline_bridge._preflight`)
    требует назначенной модели у ЛЮБОГО провайдера, иначе ответила бы модель
    учётной записи по умолчанию и ни одна проверка этого не заметила бы.

    Источник строки тот же, что у Claude на 11D: ЛОКАЛЬНАЯ политика воркера
    (`model_policy`), файл администратора машины. Данные задания в argv
    по-прежнему не попадают, то есть I-P5 сохраняется дословно.

    ФОРМА ЗАПИСИ. `--model` у Codex не вариадический (`-m, --model <MODEL>`),
    но пишется через `=` по тому же правилу, что у Claude: форма `--флаг=значение`
    снимает класс «значение начинается с дефиса и разбирается как флаг».

    `--image` же вариадический ДОСЛОВНО (`-i, --image <FILE>...`), и вот здесь
    форма с `=` не стилистика, а обязательное условие: `--image /a/b.png -` съел
    бы терминатор `-` как второе имя файла, и промпт перестал бы читаться со
    стандартного ввода. Ровно этот класс ошибки уже дал незапланированный запрос
    к модели на подготовке 11b (см. `claude_adapter._probe_argv`).
    """
    argv: list[str] = [*_INFERENCE_ARGV_HEAD]
    if model:
        argv.append(f"--model={model}")
    for path in image_paths:
        argv.append(f"--image={path}")
    argv.append("-")
    return argv

#: Типы вложений, которые адаптер соглашается записать на диск, и расширение
#: файла для каждого. Список ЗАКРЫТ: расширение уходит в имя файла, а имя — в
#: argv, поэтому «возьмём из media_type всё после слэша» означало бы позволить
#: вызывающему влиять на имя файла в командной строке.
_SUPPORTED_IMAGE_MEDIA_TYPES: dict[str, str] = {
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/webp": ".webp",
    "image/gif": ".gif",
}

#: Имя клиента в `initialize`. Документация просит идентифицировать интеграцию
#: (`clientInfo.name`) — так на стороне провайдера видно, кто спрашивает.
_CLIENT_INFO = {
    "name": "audit_manager_worker",
    "title": "AuditManager distributed audit worker",
    "version": "1",
}

_ID_ACCOUNT = 2
_ID_RATE_LIMITS = 3

#: Сколько живёт общий результат одного диалога с app-server. Нужен, чтобы
#: `auth_status()` и `quota_status()` в одном цикле опроса не поднимали
#: app-server дважды: это лишний процесс на чужой машине и лишний сетевой вызов.
_APP_SERVER_MEMO_SEC = 10.0


class CodexProviderAdapter(ProviderAdapter):
    provider = PROVIDER_CODEX
    #: v1 — разбор `account/read` и `account/rateLimits/read` app-server 0.147.x
    #: плюс `codex login status` как независимый признак.
    parser_version = "codex-appserver-1"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._memo: Optional[tuple[float, JsonRpcResult]] = None

    def provider_env(self) -> dict[str, str]:
        return {
            # Официальная переменная: корень ВСЕГО состояния Codex, включая
            # auth.json, конфиг, логи и сессии.
            "CODEX_HOME": str(self.home.config_dir),
            # Переменная УСТАНОВЩИКА («skip installer prompts»), а не
            # выключатель автообновления: официального аналога
            # `DISABLE_AUTOUPDATER` у Codex нет. Ставим её, чтобы ни одна
            # ветка CLI не ушла в интерактивный диалог на машине без
            # терминала. Следствие, которое нужно знать: версия Codex НЕ
            # закреплена так же жёстко, как версия Claude, поэтому
            # `parser_version` привязан к разбору app-server 0.147.x, и при
            # расхождении контракта разбор честно даёт UNKNOWN.
            "CODEX_NON_INTERACTIVE": "1",
        }

    # ─── Версия ──────────────────────────────────────────────────────────────
    def version(self) -> Optional[str]:
        result = self.run(["--version"], timeout_sec=min(30.0, self.timeout_sec))
        if not result.ok:
            return None
        # Формат: «codex-cli 0.147.0».
        parts = (result.stdout or "").strip().split()
        for token in reversed(parts):
            if token and token[0].isdigit():
                return token
        return parts[-1] if parts else None

    # ─── Диалог с app-server ─────────────────────────────────────────────────
    def _app_server(self, *, force: bool = False) -> JsonRpcResult:
        now = time.monotonic()
        if not force and self._memo is not None:
            stamp, cached = self._memo
            if now - stamp < _APP_SERVER_MEMO_SEC:
                return cached
        result = self.run_jsonrpc_stdio(
            ["app-server"],
            messages=[
                # Без `capabilities` вовсе: это официальный способ остаться на
                # СТАБИЛЬНОЙ части API (docs: «Omit capabilities … and the
                # server rejects experimental methods/fields»).
                {"method": "initialize", "id": 1, "params": {"clientInfo": _CLIENT_INFO}},
                {"method": "initialized"},
                {"method": "account/read", "id": _ID_ACCOUNT,
                 "params": {"refreshToken": False}},
                {"method": "account/rateLimits/read", "id": _ID_RATE_LIMITS},
            ],
            await_ids=[_ID_ACCOUNT, _ID_RATE_LIMITS],
            timeout_sec=min(60.0, self.timeout_sec),
        )
        self._memo = (now, result)
        return result

    # ─── Авторизация (0 обращений к модели) ──────────────────────────────────
    def auth_status(self) -> AuthStatus:
        if not self.installed():
            return AuthStatus(
                auth_state=AUTH_UNKNOWN, auth_method="none",
                error_code=errors.ERR_CLI_MISSING, detail="codex не установлен",
            )
        # Независимый дешёвый признак: документировано «exits with 0 when
        # credentials are present». Он не даёт ни плана, ни аккаунта, зато
        # работает и тогда, когда app-server не поднялся.
        login = self.run(["login", "status"], timeout_sec=min(30.0, self.timeout_sec))
        rpc = self._app_server()
        account = rpc.result_of(_ID_ACCOUNT)

        if account is not None:
            return _auth_from_account(account, login_exit_code=login.exit_code)

        # app-server не ответил: опираемся на код возврата `login status`.
        if login.executable_missing:
            return AuthStatus(
                auth_state=AUTH_UNKNOWN, auth_method="none",
                error_code=errors.ERR_CLI_MISSING, detail="codex не установлен",
            )
        if login.timed_out:
            return AuthStatus(
                auth_state=AUTH_UNKNOWN, auth_method="none",
                error_code=errors.ERR_TIMEOUT,
                detail="codex login status не ответил вовремя",
            )
        if login.exit_code == 0:
            return AuthStatus(
                auth_state=AUTH_LOGGED_IN, auth_method="unknown",
                error_code=rpc.error_code,
                detail=(
                    "вход выполнен, но app-server не ответил — план и лимиты "
                    "недоступны"
                ),
            )
        if login.exit_code == 1:
            return AuthStatus(
                auth_state=AUTH_LOGGED_OUT, auth_method="none",
                detail="вход не выполнен",
            )
        return AuthStatus(
            auth_state=AUTH_ERROR, auth_method="none",
            error_code=login.error_code(),
            detail="состояние авторизации Codex не определено",
        )

    # ─── Лимит: официальный structured-источник ──────────────────────────────
    def supports_zero_inference_quota(self) -> bool:
        return True

    def quota_source_name(self) -> str:
        return quota.SOURCE_OFFICIAL_APP_SERVER_RPC

    def quota_source_stability(self) -> str:
        # Не «на всякий случай»: подкоманда помечена [experimental] в самом CLI.
        return quota.STABILITY_EXPERIMENTAL

    def quota_status(self, *, auth: Optional[AuthStatus] = None) -> quota.ProviderQuotaSnapshot:
        now = time.time()
        cli_version = None
        if self.policy_blocked:
            return quota.unknown_snapshot(
                self.provider, auth_state=AUTH_UNKNOWN,
                quota_state=quota.QUOTA_POLICY_BLOCKED,
                reason="провайдер отключён политикой на этом воркере",
                observed_at=now, probe_error_code=errors.ERR_POLICY_BLOCKED,
            )
        if not self.installed():
            return quota.unknown_snapshot(
                self.provider, auth_state=AUTH_UNKNOWN,
                reason="codex не установлен", observed_at=now,
                probe_error_code=errors.ERR_CLI_MISSING,
            )
        auth = auth or self.auth_status()
        rpc = self._app_server()

        rpc_error = rpc.error_of(_ID_RATE_LIMITS)
        payload = rpc.result_of(_ID_RATE_LIMITS)

        if payload is None:
            # Разделяем три разные новости: «не вошли», «не ответил» и
            # «ответил ошибкой». Свалить их в одну означало бы показать
            # оператору «ошибка» там, где нужно просто войти.
            message = str((rpc_error or {}).get("message") or "")
            code = errors.classify_text(message) or rpc.error_code
            if auth.auth_state == AUTH_LOGGED_OUT or code == errors.ERR_AUTH_REQUIRED:
                return quota.unknown_snapshot(
                    self.provider, auth_state=auth.auth_state,
                    quota_state=quota.QUOTA_AUTH_REQUIRED,
                    reason="вход в Codex не выполнен — лимиты недоступны",
                    observed_at=now, probe_error_code=errors.ERR_AUTH_REQUIRED,
                    cli_version=cli_version,
                )
            return quota.unknown_snapshot(
                self.provider, auth_state=auth.auth_state,
                quota_state=quota.QUOTA_ERROR if code else quota.QUOTA_UNKNOWN,
                reason=(
                    f"app-server не отдал лимиты: {message or rpc.detail or 'нет ответа'}"
                ),
                observed_at=now, probe_error_code=code or errors.ERR_UNKNOWN,
                cli_version=cli_version,
            )

        try:
            return _snapshot_from_rate_limits(
                payload,
                provider=self.provider,
                auth_state=auth.auth_state,
                account_group_id=self.account_group_id,
                observed_at=now,
                stale_after=now + self.stale_after_sec,
                parser_version=self.parser_version,
                low_threshold_pct=self.low_threshold_pct,
            )
        except (quota.QuotaContractError, TypeError, ValueError, KeyError) as exc:
            # Контракт разъехался — это ровно тот случай, когда §11 требует
            # UNKNOWN, а не «разберём как получится».
            return quota.unknown_snapshot(
                self.provider, auth_state=auth.auth_state,
                quota_state=quota.QUOTA_UNKNOWN,
                reason=f"ответ app-server не разобран разборщиком {self.parser_version}: {exc}",
                observed_at=now, probe_error_code=errors.ERR_MALFORMED_STATUS,
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
            return ProbeResult(
                provider=self.provider, allowed=True, performed=False,
                error_code=errors.ERR_POLICY_BLOCKED,
                detail="нет подтверждения оператора на конкретный запуск",
            )
        argv = [
            "exec",
            "--json",
            # Политика песочницы для команд, которые запросила бы модель.
            # ВАЖНО и измерено: на пилотном воркере эта политика не может быть
            # применена — Codex приводит её в исполнение через `bwrap`, а на
            # Ubuntu 24.04 при `kernel.apparmor_restrict_unprivileged_userns=1`
            # непривилегированный `bwrap` не стартует вовсе («setting up uid
            # map: Permission denied»). Флаг оставлен как объявление намерения
            # и как рабочая защита на машинах, где песочница поднимается; на
            # ЭТОЙ машине безопасность контрольного запроса держится не на нём,
            # а на том, что модель не просят выполнять команды (см. док 11b).
            "--sandbox", "read-only",
            # runtime — не git-репозиторий; без этого codex откажется стартовать.
            "--skip-git-repo-check",
            # Не оставлять на чужом VPS файлов сессии.
            "--ephemeral",
            # Не читать проектные и пользовательские execpolicy-правила.
            "--ignore-rules",
            # Не читать `$CODEX_HOME/config.toml` — авторизация при этом
            # по-прежнему берётся из `CODEX_HOME` (документировано в
            # `codex exec --help`). Это парный к `--ignore-rules` флаг, и без
            # него ambient-режим тянул бы личные настройки владельца машины.
            # Измерено на пилотном воркере: `model = "gpt-5.6-sol"`,
            # `model_reasoning_effort = "ultra"`, `service_tier = "priority"`,
            # шесть доверенных проектов и MCP-сервер `openaiDeveloperDocs`.
            # То есть «минимальный» запрос без этого флага поднимал бы ещё и
            # внешний MCP-сервер, а модель и усилие зависели бы от машины.
            "--ignore-user-config",
            # Промпт — последним: у `codex exec` он позиционный, и любой
            # флаг, принимающий значение, обязан стоять до него.
            PROBE_PROMPT,
        ]
        started = time.time()
        result = self.run(argv, timeout_sec=max(60.0, self.timeout_sec), purpose="probe")
        if result.timed_out:
            return ProbeResult(
                provider=self.provider, allowed=True, performed=True,
                started_at=started, duration_sec=result.duration_sec,
                exit_code=result.exit_code, error_code=errors.ERR_TIMEOUT,
                detail="контрольный запрос не завершился вовремя",
            )
        usage: dict[str, Any] = {}
        matched = False
        for line in (result.stdout or "").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                # В stdout может быть и обычный текст ответа — он тоже годится
                # для сверки с ожидаемой фразой.
                if PROBE_EXPECTED in line:
                    matched = True
                continue
            if not isinstance(event, dict):
                continue
            if PROBE_EXPECTED in json.dumps(event, ensure_ascii=False):
                matched = True
            if event.get("type") == "turn.completed":
                raw = event.get("usage")
                if isinstance(raw, dict):
                    usage = {k: v for k, v in raw.items() if isinstance(v, (int, float))}
        return ProbeResult(
            provider=self.provider,
            allowed=True,
            performed=True,
            started_at=started,
            duration_sec=result.duration_sec,
            exit_code=result.exit_code,
            matched_expected=matched,
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
        model_report: str = "required",
    ) -> ProviderInferenceResult:
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
        requested_model = str(model).strip() if model else ""
        accepted = tuple(
            str(x).strip() for x in accepted_reported_models if str(x).strip()
        )
        if requested_model and not accepted:
            # Назначить модель и не назначить, с чем сверять ответ, — значит
            # получить приказ без проверки. Отказ ДО запуска бесплатен, вызов на
            # непроверяемых условиях — нет. Дословно то же правило, что у Claude.
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
        return self._finalize_inference(
            result, requested_model=requested_model, accepted=accepted,
            model_report=model_report,
        )

    # ─── Рабочий вызов С ИЗОБРАЖЕНИЕМ (этап 11H) ─────────────────────────────
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
        """То же, что `structured_inference`, но с вложенными изображениями.

        ПОЧЕМУ ЧЕРЕЗ ФАЙЛ, А НЕ БАЙТАМИ В ТЕЛЕ ЗАПРОСА, как у Claude. Это не
        выбор из двух равных: у `codex exec` официальный способ отдать картинку
        ровно один — `-i, --image <FILE>...`. Формата потокового ввода с
        content-блоком `type=image` у него нет вовсе, а `--output-schema`
        описывает форму ОТВЕТА и к вложениям отношения не имеет.

        Раз путь неизбежен, вопрос становится другим: КАКОЙ каталог видит CLI.
        Ответ — только каталог этого вызова и ничего больше:

          * файлы кладутся в свежий `mkdtemp` ВНУТРИ `home.runtime` (0700,
            владелец — воркер). `runtime` уже служит и `cwd`, и `TMPDIR`
            подпроцесса, то есть новых мест, куда CLI имеет доступ, не
            появляется;
          * в каталоге лежат ТОЛЬКО вложения этого вызова. Ни соседних кропов,
            ни артефактов задания, ни репозитория, ни личного каталога человека
            там нет — вложение попадает туда копией байтов, а не ссылкой на
            файл задания;
          * записанное СВЕРЯЕТСЯ ПО SHA256 с тем, что передал вызывающий, до
            запуска CLI. Расхождение — отказ без обращения к модели: анализ
            чертежа по чужому файлу хуже, чем несостоявшийся анализ;
          * каталог удаляется в `finally` — и после успеха, и после отказа, и
            после исключения.

        Песочница `--sandbox read-only` остаётся: она ограничивает КОМАНДЫ,
        которые модель попросила бы выполнить, и к чтению вложения отношения не
        имеет. Инструментов модели не даётся ни одного — то есть «прочитать
        соседний файл» она может только через команду, а команду ей выполнять
        нечем.
        """
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
        accepted = tuple(
            str(x).strip() for x in accepted_reported_models if str(x).strip()
        )
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
        for media_type, blob in images:
            if not blob:
                return ProviderInferenceResult(
                    provider=self.provider, model=None, status=STATUS_ERROR,
                    auth_mode=self.home.auth_mode, error_code=errors.ERR_UNKNOWN,
                    detail="пустое изображение во вложении: вызов не выполняется",
                )
            if str(media_type) not in _SUPPORTED_IMAGE_MEDIA_TYPES:
                return ProviderInferenceResult(
                    provider=self.provider, model=None, status=STATUS_ERROR,
                    auth_mode=self.home.auth_mode, error_code=errors.ERR_UNKNOWN,
                    detail=(
                        f"неподдерживаемый тип вложения {media_type!r}; "
                        f"допустимы {sorted(_SUPPORTED_IMAGE_MEDIA_TYPES)}"
                    ),
                )
        try:
            self.home.ensure_dirs()
        except OSError:
            pass
        workspace: Optional[Path] = None
        try:
            try:
                workspace = Path(
                    tempfile.mkdtemp(prefix="attach-", dir=str(self.home.runtime))
                )
                os.chmod(workspace, 0o700)
            except OSError as exc:
                return ProviderInferenceResult(
                    provider=self.provider, model=None, status=STATUS_ERROR,
                    auth_mode=self.home.auth_mode, error_code=errors.ERR_UNKNOWN,
                    detail=f"не создать каталог вложений вызова: {exc}",
                )
            paths: list[Path] = []
            for index, (media_type, blob) in enumerate(images):
                suffix = _SUPPORTED_IMAGE_MEDIA_TYPES[str(media_type)]
                # Имя файла порядковое и НЕ несёт данных задания: ни block_id,
                # ни имени проекта в нём нет. Путь всё равно уезжает в argv, а
                # argv видно в `ps` любому пользователю машины.
                path = workspace / f"attachment-{index:03d}{suffix}"
                try:
                    path.write_bytes(blob)
                    os.chmod(path, 0o600)
                except OSError as exc:
                    return ProviderInferenceResult(
                        provider=self.provider, model=None, status=STATUS_ERROR,
                        auth_mode=self.home.auth_mode, error_code=errors.ERR_UNKNOWN,
                        detail=f"вложение не записано: {exc}",
                    )
                expected = hashlib.sha256(blob).hexdigest()
                actual = hashlib.sha256(path.read_bytes()).hexdigest()
                if actual != expected:
                    return ProviderInferenceResult(
                        provider=self.provider, model=None, status=STATUS_ERROR,
                        auth_mode=self.home.auth_mode,
                        error_code=errors.ERR_MALFORMED_STATUS,
                        detail=(
                            "хэш записанного вложения не совпал с переданным: "
                            "вызов не выполняется"
                        ),
                    )
                paths.append(path)
            result = self.run(
                _inference_argv(requested_model or None, paths),
                timeout_sec=(
                    float(timeout_sec) if timeout_sec
                    else max(120.0, float(self.timeout_sec))
                ),
                stdin_text=text,
                purpose=purpose,
            )
        finally:
            if workspace is not None:
                shutil.rmtree(workspace, ignore_errors=True)
        return self._finalize_inference(
            result, requested_model=requested_model, accepted=accepted,
            model_report=model_report,
        )

    def _finalize_inference(
        self,
        result: Any,
        *,
        requested_model: str,
        accepted: Sequence[str],
        model_report: str = "required",
    ) -> ProviderInferenceResult:
        """Разбор потока `codex exec --json`, общий для обоих рабочих вызовов.

        Вынесен намеренно: текстовый и мультимодальный путь отличаются только
        тем, как собран argv, а сверка модели обязана быть буквально одним и тем
        же кодом. Иначе строгий гейт существовал бы на одном пути и отсутствовал
        на другом — ровно тот дефект, который 11F закрывал у Claude.
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
        messages, usage, reported_model = _collect_exec_stream(result.stdout or "")
        payload = None
        # Ответы перебираются С КОНЦА: последнее сообщение агента и есть его
        # итог, а более ранние могут содержать рассуждение с примером JSON.
        for candidate in reversed(messages):
            payload = self._first_json_object(candidate)
            if payload is not None:
                break
        answer_text = messages[-1] if messages else (result.stdout or "")
        ok = result.ok and payload is not None
        # Сверка фактической модели — последний и самый строгий гейт (11D у
        # Claude, 11H у Codex). Стоит ПОСЛЕ разбора: вызов уже оплачен, его
        # обязан записать журнал, но объявлять успехом ответ чужой модели
        # нельзя. Отсутствующий идентификатор — тоже несовпадение: «не знаем,
        # кто ответил» и «ответила назначенная» — разные утверждения.
        model_mismatch = ""
        if requested_model:
            reported = (reported_model or "").strip()
            if not reported:
                # Молчание CLI — несовпадение, ПОКА политика машины не объявила
                # обратное. `unsupported` ставит администратор VPS, и ставит он
                # его не «чтобы прошло», а потому что у CLI такого поля нет:
                # поток `codex exec --json` 0.147.0 состоит из `thread.started`
                # (только thread_id), `turn.started`, `item.completed` и
                # `turn.completed` (только usage) — идентификатора модели нет ни
                # в одном событии (измерено на .31, диагностический вызов 11H).
                #
                # Что при этом НЕ теряется: сам факт назначения модели.
                # `--model` у Codex не декоративен — с неизвестным значением CLI
                # получает от сервера 400 `invalid_request_error` и выходит с
                # кодом 1 (проверено там же). То есть «модель назначена и
                # принята» остаётся доказанным; недоказуемо ровно одно — что
                # ответила именно она.
                if model_report != "unsupported":
                    model_mismatch = (
                        "CLI не сообщил фактическую модель; назначена "
                        f"{requested_model!r}"
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
            model=reported_model,
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


def _collect_exec_stream(stdout: str) -> tuple[list[str], dict[str, Any], Optional[str]]:
    """Разобрать JSONL-поток `codex exec --json`.

    Разбор ЗАЩИТНЫЙ, как и у квоты: контракт `exec --json` в самом CLI помечен
    развивающимся, поэтому распознаются несколько известных форм сообщения
    агента, а нераспознанное просто не мешает. Строки, не являющиеся JSON,
    тоже попадают в кандидаты: часть версий печатает итог обычным текстом.
    """
    messages: list[str] = []
    plain: list[str] = []
    usage: dict[str, Any] = {}
    model: Optional[str] = None
    for line in (stdout or "").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            plain.append(line)
            continue
        if not isinstance(event, dict):
            continue
        kind = str(event.get("type") or "")
        item = event.get("item")
        if isinstance(item, dict):
            item_type = str(item.get("type") or "")
            if item_type in ("agent_message", "assistant_message", "message"):
                value = item.get("text") or item.get("content")
                if isinstance(value, str) and value.strip():
                    messages.append(value)
            if not model and isinstance(item.get("model"), str):
                model = item["model"]
        legacy = event.get("msg")
        if isinstance(legacy, dict):
            value = legacy.get("message") or legacy.get("text")
            if isinstance(value, str) and value.strip():
                messages.append(value)
            if not model:
                model = _model_from_event(legacy)
        if kind in ("turn.completed", "turn.failed"):
            raw = event.get("usage")
            if isinstance(raw, dict):
                usage.update(
                    {k: v for k, v in raw.items() if isinstance(v, (int, float))}
                )
        if not model:
            model = _model_from_event(event)
    if not messages and plain:
        messages.append("\n".join(plain))
    return messages, usage, model


#: Ключи, под которыми `codex exec --json` называет фактически применённую
#: модель. Поиск ведётся ТОЛЬКО по этому списку и только на ограниченной
#: глубине: «найдём любое поле, похожее на модель» рано или поздно подобрало бы
#: чужое значение (имя модели из текста ответа, конфигурацию MCP-сервера), и
#: гейт сверки молча начал бы проходить на чём попало.
_MODEL_KEYS: tuple[str, ...] = ("model", "model_slug", "modelSlug", "model_id")

#: Контейнеры, внутрь которых имеет смысл заглянуть. Опять же закрытый список.
_MODEL_CONTAINERS: tuple[str, ...] = (
    "thread", "session", "turn", "config", "configuration", "payload", "data",
)


def _model_from_event(event: Any, *, depth: int = 0) -> Optional[str]:
    """Фактическая модель из одного события потока. `None`, если её там нет.

    Зачем понадобился отдельный разбор. До 11H поиск смотрел только верхний
    уровень события и поле `item.model`, а `codex exec` объявляет модель в
    служебном событии начала нити (`codex.thread.started` и родня), где она
    лежит на уровень глубже. Пустой результат здесь означает `model=None`, а
    `None` в сверке — отказ вызова, то есть цена «не нашли» максимальна.

    Глубина ограничена двумя уровнями намеренно: этого достаточно для всех
    известных форм события и мало для того, чтобы дотянуться до содержимого
    ответа модели.
    """
    if not isinstance(event, dict) or depth > 2:
        return None
    for key in _MODEL_KEYS:
        value = event.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    for key in _MODEL_CONTAINERS:
        nested = event.get(key)
        if isinstance(nested, dict):
            found = _model_from_event(nested, depth=depth + 1)
            if found:
                return found
    return None


#: Сколько символов сообщения CLI попадает в `detail`. Тот же рубеж, что у
#: Claude, и по той же причине: ошибка провайдера — короткая служебная строка, а
#: развёрнутая ошибка запроса теоретически способна процитировать кусок входа.
_CLI_FAILURE_DETAIL_MAX_CHARS = 400


def _cli_failure_detail(result: Any, answer_text: str) -> str:
    """Почему CLI завершился ошибкой — СЛОВАМИ САМОГО CLI.

    Константа «CLI завершился ошибкой» на её месте стояла до 11H, и цена этой
    экономии уже измерена на 11E: боевой вызов отказал за 32 секунды, а текст
    ошибки был выброшен адаптером — причина отказа осталась неустановимой
    навсегда. Текст уже прошёл редактор секретов в `ProviderAdapter.run`,
    поэтому здесь остаётся только выбрать источник и обрезать длину.
    """
    for candidate in (
        getattr(result, "stderr", "") or "",
        answer_text or "",
        getattr(result, "stdout", "") or "",
    ):
        text = " ".join(str(candidate).split())
        if text:
            if len(text) > _CLI_FAILURE_DETAIL_MAX_CHARS:
                text = text[:_CLI_FAILURE_DETAIL_MAX_CHARS] + "…"
            return f"CLI завершился ошибкой: {text}"
    return "CLI завершился ошибкой без диагностики"


# ─── Разбор ответов app-server ───────────────────────────────────────────────
def _auth_from_account(
    account_payload: dict[str, Any], *, login_exit_code: Optional[int]
) -> AuthStatus:
    """Разбор `account/read`.

    Документированные формы: `{"account": null, "requiresOpenaiAuth": true|false}`
    и `{"account": {"type": "chatgpt"|"apiKey"|"amazonBedrock", "email": …,
    "planType": …}}`.
    """
    account = account_payload.get("account")
    if account is None:
        # `requiresOpenaiAuth=false` означает, что провайдер вообще не требует
        # учётных данных OpenAI (например, сторонний провайдер) — это не «не
        # вошли», и путать эти два состояния нельзя.
        if account_payload.get("requiresOpenaiAuth") is False:
            return AuthStatus(
                auth_state=AUTH_LOGGED_IN, auth_method="external_provider",
                detail="учётные данные OpenAI не требуются для активного провайдера",
            )
        return AuthStatus(
            auth_state=AUTH_LOGGED_OUT, auth_method="none",
            detail="вход не выполнен",
        )
    if not isinstance(account, dict):
        return AuthStatus(
            auth_state=AUTH_ERROR, auth_method="none",
            error_code=errors.ERR_MALFORMED_STATUS,
            detail="account/read: поле account не объект",
        )
    kind = str(account.get("type") or "unknown")
    plan = account.get("planType")
    # e-mail используется ТОЛЬКО как вход солёного отпечатка и никуда больше
    # не уходит: `as_center_payload` его не перечисляет.
    email = account.get("email")
    state = AUTH_LOGGED_IN
    if login_exit_code == 1 and kind == "unknown":
        state = AUTH_LOGGED_OUT
    return AuthStatus(
        auth_state=state,
        auth_method=kind,
        plan_type=str(plan) if plan else None,
        stable_identifier=str(email) if email else None,
        raw_public={"type": kind, "requiresOpenaiAuth": account_payload.get("requiresOpenaiAuth")},
    )


def _number(value: Any) -> Optional[float]:
    """Число или None. `bool` числом НЕ считается.

    `isinstance(True, int)` истинно, и без этой проверки `usedPercent: true`
    превращался бы в «использован 1 %», то есть в остаток 99 % — выдуманное
    значение с высокой достоверностью. Ровно тот случай, ради которого весь
    модуль и написан.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _window(
    raw: Any, *, window_id: str, source: str, confidence: str
) -> Optional[quota.QuotaWindow]:
    """Одно окно из ответа app-server.

    Ключевое отличие от «мягкого» разбора: поле НЕПРАВИЛЬНОГО типа — это не
    «поля нет», а расхождение контракта. Раньше оно молча превращалось в
    `None`, и снимок уходил дальше с процентом соседнего окна: при
    `primary.usedPercent = "97.5"` (строка вместо числа) и
    `secondary.usedPercent = 10` оператор видел «готов, остаток 90 %», хотя
    пятичасовое окно было выбрано на 97,5 %. Теперь такой ответ поднимает
    `QuotaContractError`, и `quota_status` честно отдаёт UNKNOWN.
    """
    if not isinstance(raw, dict):
        return None
    used_raw = raw.get("usedPercent")
    minutes_raw = raw.get("windowDurationMins")
    resets_raw = raw.get("resetsAt")

    used = _number(used_raw)
    if used_raw is not None and used is None:
        raise quota.QuotaContractError(
            f"{window_id}: usedPercent не число ({type(used_raw).__name__})"
        )
    resets_at = _number(resets_raw)
    if resets_raw is not None and resets_at is None:
        raise quota.QuotaContractError(
            f"{window_id}: resetsAt не число ({type(resets_raw).__name__})"
        )
    minutes = _number(minutes_raw)
    if minutes_raw is not None and minutes is None:
        raise quota.QuotaContractError(
            f"{window_id}: windowDurationMins не число ({type(minutes_raw).__name__})"
        )

    if used is None and resets_at is None:
        return None
    return quota.QuotaWindow(
        window_id=window_id,
        source=source,
        confidence=confidence,
        used_pct=used,
        reset_at=resets_at,
        duration_sec=int(minutes) * 60 if minutes is not None else None,
    )


def _snapshot_from_rate_limits(
    payload: dict[str, Any],
    *,
    provider: str,
    auth_state: str,
    account_group_id: Optional[str],
    observed_at: float,
    stale_after: float,
    parser_version: str,
    low_threshold_pct: Optional[float],
) -> quota.ProviderQuotaSnapshot:
    """`account/rateLimits/read` → нормализованный снимок.

    Правило выбора остатка: берётся ХУДШЕЕ из известных окон. Пятичасовое окно
    может быть свободно, когда недельное почти выбрано, — и «свободно» в этом
    случае неправда. Ошибиться в сторону осторожности здесь дешевле.
    """
    source = quota.SOURCE_OFFICIAL_APP_SERVER_RPC
    confidence = quota.CONFIDENCE_HIGH

    buckets = payload.get("rateLimitsByLimitId")
    single = payload.get("rateLimits")
    chosen: Optional[dict[str, Any]] = None
    chosen_key: Optional[str] = None
    if isinstance(buckets, dict) and buckets:
        # Многоведёрный вид. Основное ведро Codex документировано как `codex`.
        if isinstance(buckets.get("codex"), dict):
            chosen, chosen_key = buckets["codex"], "codex"
        else:
            for key in sorted(buckets):
                if isinstance(buckets[key], dict):
                    chosen, chosen_key = buckets[key], key
                    break
    if not isinstance(chosen, dict) and isinstance(single, dict):
        chosen, chosen_key = single, None
    if not isinstance(chosen, dict):
        raise quota.QuotaContractError("ни rateLimits, ни rateLimitsByLimitId не разобраны")

    # Имя ведра берётся из САМОГО ведра, а при его отсутствии — из ключа, под
    # которым оно лежало. Подстановка «codex» по умолчанию была ошибкой: она
    # подписывала измерение чужого лимита именем основного, и на экране
    # «limit_id=codex» относилось не к codex.
    limit_id = str(chosen.get("limitId") or chosen_key or "unknown")
    primary = _window(
        chosen.get("primary"), window_id=f"{limit_id}:primary",
        source=source, confidence=confidence,
    )
    secondary = _window(
        chosen.get("secondary"), window_id=f"{limit_id}:secondary",
        source=source, confidence=confidence,
    )
    # Окна ВЫБРАННОГО ведра — те, по которым принимается решение.
    own: list[quota.QuotaWindow] = [w for w in (primary, secondary) if w is not None]
    # Окна прочих вёдер — справочные. Они НЕ участвуют ни в расчёте остатка,
    # ни в выборе ближайшего сброса: у чужого ведра свой лимит и свои окна, и
    # смешивать их означало бы подписать число не тем лимитом. Раньше `min()`
    # шёл по объединённому списку — при `code_review` на 90 % основной остаток
    # Codex подменялся чужим.
    foreign: list[quota.QuotaWindow] = []
    if isinstance(buckets, dict):
        for key in sorted(buckets):
            bucket = buckets[key]
            if not isinstance(bucket, dict) or bucket is chosen:
                continue
            for field_name in ("primary", "secondary"):
                extra = _window(
                    bucket.get(field_name), window_id=f"{key}:{field_name}",
                    source=source, confidence=confidence,
                )
                if extra is not None:
                    foreign.append(extra)

    secondaries: list[quota.QuotaWindow] = (
        ([secondary] if secondary is not None else []) + foreign
    )

    known = [w.remaining_pct for w in own if w.remaining_pct is not None]
    remaining = min(known) if known else None
    raw_supported = remaining is not None

    resets = [w.reset_at for w in own if w.reset_at is not None]
    next_reset = min(resets) if resets else None

    reached = chosen.get("rateLimitReachedType")
    if reached:
        state = quota.QUOTA_LIMITED
    elif remaining is None:
        state = quota.QUOTA_UNKNOWN
    else:
        state = quota.QUOTA_READY

    snapshot = quota.ProviderQuotaSnapshot(
        provider=provider,
        quota_state=state,
        observed_at=observed_at,
        source=source,
        confidence=confidence if raw_supported or reached else quota.CONFIDENCE_NONE,
        auth_state=auth_state,
        account_group_id=account_group_id,
        stale_after=stale_after,
        primary_window=primary,
        secondary_windows=tuple(secondaries),
        next_reset_at=next_reset,
        estimated_remaining_pct=remaining,
        raw_remaining_supported=raw_supported,
        source_stability=quota.STABILITY_EXPERIMENTAL,
        parser_version=parser_version,
        detail=(
            f"limit_id={limit_id}"
            + (f", достигнут лимит типа {reached}" if reached else "")
        ),
    )
    return quota.apply_low_threshold(snapshot, low_threshold_pct=low_threshold_pct)

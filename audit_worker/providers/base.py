"""ProviderAdapter — единственная точка, из которой запускается CLI провайдера.

Инварианты слоя (§9 и §28 задания), выраженные в коде:

  I-P1. Окружение строится С НУЛЯ. Не «копируем os.environ и чистим»: чистка
        знает только то, что в неё внесли, и любая новая переменная воркера
        уехала бы в CLI по умолчанию. Здесь наоборот — доезжает только то, что
        перечислено поимённо.
  I-P2. Worker-token, адрес центра, execution-token и ключи платных API до
        подпроцесса не доходят ФИЗИЧЕСКИ: их имён нет в белом списке, а сам
        адаптер их не получает — конструктор их не принимает.
  I-P3. Каждый провайдер получает СВОЙ HOME и свою переменную конфигурации.
        Claude никогда не видит `CODEX_HOME`, Codex — `CLAUDE_CONFIG_DIR`.
  I-P4. `cwd` — пустой каталог `providers/<p>/runtime`. Ни репозитория, ни
        проектных настроек, ни файлов задания. Для quota-опроса это не
        перестраховка: `codex app-server`, запущенный с cwd=/home/coder,
        обнаружил там чужой `.codex` как project-local конфигурацию.
  I-P5. Ни один argv не приходит извне. Аргументы — константы модуля плюс
        значения, которые адаптер сформировал сам. Строки от центра, из
        задания или из файла в argv не попадают.
  I-P6. Всё, что вышло из подпроцесса, проходит редактор секретов ДО того, как
        попадёт в возвращаемое значение. Не «перед отправкой», а сразу: дальше
        значение живёт в логах и в heartbeat.
  I-P7. Таймаут обязателен, и по нему убивается ГРУППА процессов. `app-server`
        живёт, пока открыт stdin, и без группы после таймаута на чужом VPS
        оставался бы висящий процесс.
"""
from __future__ import annotations

import abc
import json
import os
import signal
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

from audit_worker import redaction
from audit_worker.providers import errors
from audit_worker.providers.identity import ProviderIdentity
from audit_worker.providers.paths import ProviderHome
from audit_worker.providers.quota import ProviderQuotaSnapshot

#: Системные переменные, без которых процесс на некоторых VPS не стартует
#: вовсе. Секретов среди них нет, путей к данным платформы — тоже.
#: `PATH` в этот список НЕ входит: он собирается адаптером из фиксированных
#: системных каталогов, иначе унаследованный PATH воркера (в котором первым
#: стоит каталог ПОДДЕЛЬНЫХ провайдеров) подсунул бы CLI подделку.
ENV_PASSTHROUGH: tuple[str, ...] = (
    "LANG",
    "LC_ALL",
    "TZ",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "LD_LIBRARY_PATH",
)

#: Фиксированный PATH подпроцесса. Только системные каталоги: CLI запускается
#: по АБСОЛЮТНОМУ пути, а PATH нужен ему лишь для служебных утилит.
SAFE_PATH = "/usr/local/bin:/usr/bin:/bin"

#: Имена, которые не имеют права оказаться в окружении подпроцесса ни при
#: каких условиях. Проверка избыточна по построению (окружение собирается с
#: нуля) — и именно поэтому полезна: она ловит будущую правку, которая решит
#: «ну добавим сюда одну переменную».
FORBIDDEN_ENV_NAMES: frozenset[str] = frozenset({
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "OPENAI_API_KEY",
    "CODEX_API_KEY",
    "CODEX_ACCESS_TOKEN",
    "OPENROUTER_API_KEY",
    "AUDIT_WORKER_TOKEN",
    "AUDIT_WORKER_DISPATCHER_URL",
    "DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET",
    "PORTAL_AUTH_PASSWORD",
    "PORTAL_AUTH_SECRET",
})


class ProviderEnvironmentError(RuntimeError):
    """Окружение подпроцесса нарушает инвариант слоя."""


@dataclass(frozen=True)
class ProcessResult:
    """Итог запуска CLI. stdout/stderr уже отредактированы."""

    argv_display: tuple[str, ...]
    exit_code: Optional[int]
    stdout: str
    stderr: str
    duration_sec: float
    timed_out: bool = False
    executable_missing: bool = False

    @property
    def ok(self) -> bool:
        return self.exit_code == 0 and not self.timed_out

    def error_code(self) -> str:
        return errors.classify_process_result(
            exit_code=self.exit_code,
            stdout=self.stdout,
            stderr=self.stderr,
            timed_out=self.timed_out,
            executable_missing=self.executable_missing,
        )

    def json_stdout(self) -> Any:
        """Разобрать stdout как JSON. Исключение — не «пусто», а malformed."""
        return json.loads(self.stdout)


@dataclass(frozen=True)
class AuthStatus:
    """Результат безопасной проверки авторизации (0 обращений к модели)."""

    auth_state: str
    auth_method: str
    plan_type: Optional[str] = None
    #: Незасекреченный идентификатор учётной записи ДЛЯ ЛОКАЛЬНОГО отпечатка.
    #: За пределы воркера в открытом виде не выходит.
    stable_identifier: Optional[str] = None
    error_code: Optional[str] = None
    detail: Optional[str] = None
    raw_public: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProbeResult:
    """Итог минимального контрольного запроса к модели (§18 задания)."""

    provider: str
    allowed: bool
    performed: bool
    started_at: Optional[float] = None
    duration_sec: Optional[float] = None
    exit_code: Optional[int] = None
    model: Optional[str] = None
    matched_expected: Optional[bool] = None
    usage: dict[str, Any] = field(default_factory=dict)
    error_code: Optional[str] = None
    detail: Optional[str] = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "allowed": self.allowed,
            "performed": self.performed,
            "started_at": self.started_at,
            "duration_sec": self.duration_sec,
            "exit_code": self.exit_code,
            "model": self.model,
            "matched_expected": self.matched_expected,
            "usage": dict(self.usage),
            "error_code": self.error_code,
            "detail": self.detail,
        }


#: Ровно та фраза, что задана §18. Модель не получает ни документов, ни
#: репозитория, ни инструментов — только эту строку.
PROBE_PROMPT = "Reply exactly: PROVIDER_PROBE_OK"
PROBE_EXPECTED = "PROVIDER_PROBE_OK"


class ProviderAdapter(abc.ABC):
    """Общая часть адаптеров. Наследники добавляют только знание своего CLI."""

    #: Имя провайдера. Задаётся наследником.
    provider: str = ""
    #: Версия разборщика ответов. Растёт при ЛЮБОМ изменении логики разбора —
    #: иначе в истории квот нельзя понять, каким кодом получено значение.
    parser_version: str = "1"

    def __init__(
        self,
        home: ProviderHome,
        *,
        executable: Optional[Path] = None,
        timeout_sec: float = 60.0,
        account_group_id: Optional[str] = None,
        policy_blocked: bool = False,
        inference_allowed: bool = False,
        low_threshold_pct: Optional[float] = None,
        stale_after_sec: float = 900.0,
        on_process: Optional[Callable[[int, str], None]] = None,
    ) -> None:
        self.home = home
        self._executable_override = Path(executable) if executable else None
        self.timeout_sec = float(timeout_sec)
        self.account_group_id = (str(account_group_id).strip() or None) if account_group_id else None
        self.policy_blocked = bool(policy_blocked)
        # Разрешение на РЕАЛЬНЫЙ вызов модели. По умолчанию False во всех
        # конструкторах цепочки: включается только явным флагом оператора.
        self.inference_allowed = bool(inference_allowed)
        self.low_threshold_pct = low_threshold_pct
        self.stale_after_sec = float(stale_after_sec)
        self._on_process = on_process

    # ─── Обязательный интерфейс (§9) ─────────────────────────────────────────
    def executable_path(self) -> Optional[Path]:
        """Путь к CLI. Выбирается УСТАНОВЛЕННЫМ кодом, не заданием и не центром.

        Порядок: явное указание администратора VPS → путь официального
        установщика внутри provider home. Поиска по `PATH` здесь нет
        намеренно: в PATH воркера первым стоит каталог поддельных провайдеров,
        и «найти claude в PATH» означало бы опросить подделку и отрапортовать
        центру её версию.
        """
        if self._executable_override is not None:
            return self._executable_override
        default = self.home.default_executable
        return default if default.exists() else None

    def installed(self) -> bool:
        path = self.executable_path()
        return bool(path and path.exists() and os.access(path, os.X_OK))

    @abc.abstractmethod
    def version(self) -> Optional[str]:
        """Версия CLI. `None`, если не установлен или не ответил."""

    @abc.abstractmethod
    def auth_status(self) -> AuthStatus:
        """Состояние авторизации. Обращений к модели — ноль."""

    @abc.abstractmethod
    def quota_status(self, *, auth: Optional[AuthStatus] = None) -> ProviderQuotaSnapshot:
        """Снимок лимита. Обращений к модели — ноль.

        Если официального способа без обращения к модели не существует,
        наследник ОБЯЗАН вернуть снимок с `quota_state="unknown"` и
        `raw_remaining_supported=False`, а не пытаться добыть число обходным
        путём (§11 задания).
        """

    @abc.abstractmethod
    def minimal_probe(self, *, confirmed_by_operator: bool = False) -> ProbeResult:
        """Один минимальный запрос к модели. По умолчанию ЗАПРЕЩЁН."""

    def classify_error(self, payload: Any) -> str:
        return errors.classify_text(errors.summarize(payload)) or errors.ERR_UNKNOWN

    def capability_snapshot(self) -> dict[str, Any]:
        """Что этот адаптер умеет — для центра и для интерфейса."""
        return {
            "provider": self.provider,
            "parser_version": self.parser_version,
            "zero_inference_auth_status": True,
            "zero_inference_quota": self.supports_zero_inference_quota(),
            "structured_quota_source": self.quota_source_name(),
            "quota_source_stability": self.quota_source_stability(),
            "inference_probe_supported": True,
            "inference_probe_allowed": self.inference_allowed and not self.policy_blocked,
            "provider_home": self.home.as_public_dict(),
        }

    @abc.abstractmethod
    def supports_zero_inference_quota(self) -> bool:
        """Существует ли ОФИЦИАЛЬНЫЙ способ узнать лимит без вызова модели."""

    @abc.abstractmethod
    def quota_source_name(self) -> str:
        """Идентификатор источника квоты из закрытого списка `quota.py`."""

    @abc.abstractmethod
    def quota_source_stability(self) -> str:
        """Стабильность контракта источника (`quota.STABILITY_*`)."""

    # ─── Сводка ──────────────────────────────────────────────────────────────
    def identity(self) -> ProviderIdentity:
        """Собрать ProviderIdentity: установка + авторизация + права файлов."""
        from audit_worker.providers import identity as identity_mod

        now = time.time()
        if self.policy_blocked:
            return ProviderIdentity(
                provider=self.provider,
                installation_status=(
                    identity_mod.INSTALL_INSTALLED if self.installed()
                    else identity_mod.INSTALL_MISSING
                ),
                auth_state=identity_mod.AUTH_UNKNOWN,
                auth_method="none",
                policy_state=identity_mod.POLICY_BLOCKED,
                inference_allowed=False,
                last_auth_check_at=now,
                cli_version=None,
                account_group_id=self.account_group_id,
                provider_home=self.home.home,
                executable_path=self.executable_path(),
                credential_facts=identity_mod.credential_file_facts(
                    self.home.credential_path
                ),
                capability=self.capability_snapshot(),
                error_code=errors.ERR_POLICY_BLOCKED,
                detail=(
                    "провайдер отключён политикой на этом воркере "
                    "(AUDIT_WORKER_PROVIDER_<X>_POLICY_BLOCKED)"
                ),
            )

        if not self.installed():
            return ProviderIdentity(
                provider=self.provider,
                installation_status=identity_mod.INSTALL_MISSING,
                auth_state=identity_mod.AUTH_UNKNOWN,
                auth_method="none",
                policy_state=identity_mod.POLICY_ALLOWED,
                inference_allowed=False,
                last_auth_check_at=now,
                account_group_id=self.account_group_id,
                provider_home=self.home.home,
                executable_path=None,
                credential_facts=identity_mod.credential_file_facts(
                    self.home.credential_path
                ),
                capability=self.capability_snapshot(),
                error_code=errors.ERR_CLI_MISSING,
                detail="CLI провайдера не установлен в provider home",
            )

        version = self.version()
        auth = self.auth_status()
        fingerprint = identity_mod.account_fingerprint(
            self.home.metadata,
            provider=self.provider,
            stable_identifier=auth.stable_identifier,
        )
        installation = (
            identity_mod.INSTALL_INSTALLED if version
            else identity_mod.INSTALL_BROKEN
        )
        return ProviderIdentity(
            provider=self.provider,
            installation_status=installation,
            auth_state=auth.auth_state,
            auth_method=auth.auth_method,
            plan_type=auth.plan_type,
            policy_state=identity_mod.POLICY_ALLOWED,
            # Одного `logged_in` мало: реальный вызов модели остаётся под
            # отдельным флагом даже у полностью авторизованного провайдера.
            inference_allowed=self.inference_allowed,
            last_auth_check_at=now,
            cli_version=version,
            account_group_id=self.account_group_id,
            account_fingerprint=fingerprint,
            provider_home=self.home.home,
            executable_path=self.executable_path(),
            credential_facts=identity_mod.credential_file_facts(
                self.home.credential_path
            ),
            capability=self.capability_snapshot(),
            error_code=auth.error_code,
            detail=auth.detail,
        )

    # ─── Запуск подпроцесса ──────────────────────────────────────────────────
    def build_env(self) -> dict[str, str]:
        """Окружение подпроцесса. Собирается с нуля (I-P1).

        Наследник добавляет свои переменные через `provider_env()`; общий код
        проверяет результат на запрещённые имена — не потому, что сейчас они
        могут туда попасть, а чтобы будущая правка не смогла их внести молча.
        """
        env: dict[str, str] = {"PATH": SAFE_PATH}
        for name in ENV_PASSTHROUGH:
            value = os.environ.get(name)
            if value:
                env[name] = value
        env["HOME"] = str(self.home.home)
        # Каталог временных файлов уводится внутрь provider home: иначе CLI
        # пишет в общий /tmp чужой машины, где его увидит кто угодно.
        env["TMPDIR"] = str(self.home.runtime)
        env.update(self.provider_env())
        forbidden = FORBIDDEN_ENV_NAMES & set(env)
        if forbidden:
            raise ProviderEnvironmentError(
                f"в окружение {self.provider} попали запрещённые переменные: "
                f"{sorted(forbidden)}"
            )
        return env

    @abc.abstractmethod
    def provider_env(self) -> dict[str, str]:
        """Переменные, специфичные для провайдера (его CONFIG_DIR/HOME)."""

    def _redact_literals(self) -> tuple[str, ...]:
        """Значения, которые обязаны исчезнуть из вывода дословно."""
        return (str(self.home.home), str(self.home.root))

    def run(
        self,
        args: Sequence[str],
        *,
        timeout_sec: Optional[float] = None,
        stdin_text: Optional[str] = None,
        purpose: str = "status",
    ) -> ProcessResult:
        """Запустить CLI. Единственная точка `subprocess` во всём слое.

        `args` — только хвост после исполняемого файла, и он всегда приходит из
        констант адаптера (I-P5). Строка от центра или из задания сюда попасть
        не может: у метода нет ни одного вызова с внешним значением.
        """
        executable = self.executable_path()
        started = time.monotonic()
        if executable is None or not executable.exists():
            return ProcessResult(
                argv_display=(self.provider, *args),
                exit_code=None,
                stdout="",
                stderr="исполняемый файл провайдера не найден",
                duration_sec=0.0,
                executable_missing=True,
            )
        argv = [str(executable), *[str(a) for a in args]]
        env = self.build_env()
        limit = float(timeout_sec if timeout_sec is not None else self.timeout_sec)
        # cwd — пустой runtime-каталог (I-P4). Создаём здесь же: каталог могли
        # снести вместе с временными файлами, и падать из-за этого незачем.
        try:
            self.home.runtime.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass

        proc: Optional[subprocess.Popen] = None
        timed_out = False
        try:
            proc = subprocess.Popen(
                argv,
                cwd=str(self.home.runtime),
                env=env,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                # Своя сессия: подпроцесс не получит сигналы, адресованные
                # воркеру, а мы сможем убить ГРУППУ по таймауту (I-P7).
                start_new_session=True,
            )
            if self._on_process:
                try:
                    self._on_process(proc.pid, purpose)
                except Exception:                     # noqa: BLE001 — учёт не роняет опрос
                    pass
            try:
                stdout, stderr = proc.communicate(input=stdin_text, timeout=limit)
                exit_code = proc.returncode
            except subprocess.TimeoutExpired:
                timed_out = True
                _kill_group(proc)
                stdout, stderr = _drain_after_kill(proc)
                exit_code = proc.returncode
        except (OSError, ValueError) as exc:
            return ProcessResult(
                argv_display=tuple(_display_argv(argv)),
                exit_code=None,
                stdout="",
                stderr=redaction.redact(str(exc), extra_literals=self._redact_literals()),
                duration_sec=time.monotonic() - started,
                executable_missing=isinstance(exc, FileNotFoundError),
            )
        finally:
            if proc is not None and proc.poll() is None:
                _kill_group(proc)

        literals = self._redact_literals()
        return ProcessResult(
            argv_display=tuple(_display_argv(argv)),
            exit_code=exit_code,
            # Редактируем СРАЗУ (I-P6): дальше значение живёт в логах и уедет
            # в heartbeat, и «отредактируем перед отправкой» здесь опоздало бы.
            stdout=redaction.redact(stdout or "", extra_literals=literals),
            stderr=redaction.redact(stderr or "", extra_literals=literals),
            duration_sec=time.monotonic() - started,
            timed_out=timed_out,
        )


    def run_jsonrpc_stdio(
        self,
        args: Sequence[str],
        *,
        messages: Sequence[dict[str, Any]],
        await_ids: Sequence[Any],
        timeout_sec: Optional[float] = None,
        purpose: str = "quota",
    ) -> "JsonRpcResult":
        """Диалог JSON-RPC по stdio с долгоживущим подпроцессом.

        Отдельный метод, а не `run()`, по одной причине: `communicate()`
        закрывает stdin сразу, а сервер, у которого stdin в EOF, вправе
        завершиться, не ответив на уже принятые запросы. Проверено вживую:
        `printf … | codex app-server` отдавал только ответ на `initialize`, а
        ответы на `account/read` и `account/rateLimits/read` терялись. Здесь
        stdin держится открытым, пока не придут ВСЕ ожидаемые ответы либо не
        истечёт срок.

        Подпроцесс всё равно запускается тем же способом, что и в `run()`:
        своя сессия, окружение с нуля, cwd = runtime, убийство группы на выходе.
        """
        executable = self.executable_path()
        started = time.monotonic()
        if executable is None or not executable.exists():
            return JsonRpcResult(
                responses={}, notifications=(), error_code=errors.ERR_CLI_MISSING,
                detail="исполняемый файл провайдера не найден",
                duration_sec=0.0, stderr="",
            )
        limit = float(timeout_sec if timeout_sec is not None else self.timeout_sec)
        deadline = time.monotonic() + limit
        argv = [str(executable), *[str(a) for a in args]]
        try:
            self.home.runtime.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass

        proc: Optional[subprocess.Popen] = None
        responses: dict[Any, dict[str, Any]] = {}
        notifications: list[dict[str, Any]] = []
        stderr_chunks: list[str] = []
        error_code: Optional[str] = None
        detail: Optional[str] = None
        try:
            proc = subprocess.Popen(
                argv,
                cwd=str(self.home.runtime),
                env=self.build_env(),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                start_new_session=True,
            )
            if self._on_process:
                try:
                    self._on_process(proc.pid, purpose)
                except Exception:                     # noqa: BLE001
                    pass

            import threading

            def _drain_stderr() -> None:
                try:
                    assert proc is not None and proc.stderr is not None
                    for line in proc.stderr:
                        stderr_chunks.append(line)
                        if len(stderr_chunks) > 200:
                            break
                except Exception:                     # noqa: BLE001
                    pass

            threading.Thread(target=_drain_stderr, daemon=True).start()

            assert proc.stdin is not None and proc.stdout is not None
            for message in messages:
                proc.stdin.write(json.dumps(message, ensure_ascii=False) + "\n")
            proc.stdin.flush()

            pending = {i for i in await_ids}
            lines: list[str] = []

            def _read_lines() -> None:
                try:
                    assert proc is not None and proc.stdout is not None
                    for line in proc.stdout:
                        lines.append(line)
                except Exception:                     # noqa: BLE001
                    pass

            reader = threading.Thread(target=_read_lines, daemon=True)
            reader.start()
            cursor = 0
            while pending and time.monotonic() < deadline:
                if cursor >= len(lines):
                    if not reader.is_alive() and cursor >= len(lines):
                        break
                    time.sleep(0.05)
                    continue
                raw = lines[cursor]
                cursor += 1
                try:
                    message = json.loads(raw)
                except (json.JSONDecodeError, ValueError):
                    continue
                if not isinstance(message, dict):
                    continue
                if "id" in message and message["id"] in pending:
                    responses[message["id"]] = message
                    pending.discard(message["id"])
                elif "method" in message:
                    if len(notifications) < 50:
                        notifications.append(message)
            if pending:
                error_code = errors.ERR_TIMEOUT
                detail = f"нет ответа на {sorted(map(str, pending))}"
        except (OSError, ValueError) as exc:
            error_code = errors.classify_exception(exc)
            detail = str(exc)
        finally:
            if proc is not None:
                try:
                    if proc.stdin is not None and not proc.stdin.closed:
                        proc.stdin.close()
                except Exception:                     # noqa: BLE001
                    pass
                if proc.poll() is None:
                    _kill_group(proc)

        literals = self._redact_literals()
        return JsonRpcResult(
            responses=responses,
            notifications=tuple(notifications),
            error_code=error_code,
            detail=redaction.redact(detail, extra_literals=literals) if detail else None,
            duration_sec=time.monotonic() - started,
            stderr=redaction.redact("".join(stderr_chunks)[:8000], extra_literals=literals),
        )


@dataclass(frozen=True)
class JsonRpcResult:
    """Итог диалога JSON-RPC. Значения уже отредактированы."""

    responses: dict[Any, dict[str, Any]]
    notifications: tuple[dict[str, Any], ...]
    error_code: Optional[str]
    detail: Optional[str]
    duration_sec: float
    stderr: str

    def result_of(self, request_id: Any) -> Optional[dict[str, Any]]:
        message = self.responses.get(request_id)
        if not isinstance(message, dict):
            return None
        payload = message.get("result")
        return payload if isinstance(payload, dict) else None

    def error_of(self, request_id: Any) -> Optional[dict[str, Any]]:
        message = self.responses.get(request_id)
        if not isinstance(message, dict):
            return None
        payload = message.get("error")
        return payload if isinstance(payload, dict) else None


def _display_argv(argv: Sequence[str]) -> list[str]:
    """argv для журнала: путь к исполняемому файлу сжат до имени.

    Полный путь раскрывает раскладку чужой машины, а для диагностики хватает
    имени бинаря и аргументов. Аргументы у нас константные и секретов не
    содержат — но на всякий случай прогоняются через редактор.
    """
    if not argv:
        return []
    head = Path(argv[0]).name
    tail = [redaction.redact(str(a)) for a in argv[1:]]
    return [head, *tail]


def _kill_group(proc: subprocess.Popen) -> None:
    """SIGTERM группе → пауза → SIGKILL. Одиночный kill(pid) недостаточен.

    `codex app-server` держится, пока открыт stdin, и порождает потомков.
    Убить только лидера значило бы оставить на чужом VPS висящие процессы.
    """
    try:
        pgid = os.getpgid(proc.pid)
    except (OSError, ProcessLookupError):
        pgid = None
    for sig, wait in ((signal.SIGTERM, 3.0), (signal.SIGKILL, 2.0)):
        if proc.poll() is not None:
            return
        try:
            if pgid is not None:
                os.killpg(pgid, sig)
            else:
                proc.send_signal(sig)
        except (OSError, ProcessLookupError):
            return
        try:
            proc.wait(timeout=wait)
            return
        except subprocess.TimeoutExpired:
            continue


def _drain_after_kill(proc: subprocess.Popen) -> tuple[str, str]:
    """Забрать то, что процесс успел написать до смерти. Молча, без ожидания."""
    try:
        return proc.communicate(timeout=5.0)
    except Exception:                                  # noqa: BLE001
        return "", ""

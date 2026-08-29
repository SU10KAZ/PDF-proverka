"""Шлюз ИИ-сессий сравнения — единственная точка запуска моделей.

Сравнение документации не ходит в HTTP-API моделей. Разрешены ровно два
транспорта, оба по подписке:

    CLAUDE_SESSION — `claude -p`
    CODEX_SESSION  — `codex exec`

Шлюз отвечает за всё, чего не должен знать оркестратор: выбор семейства
провайдера и модели, уровень рассуждения, таймаут, отмену, изоляцию сессии,
разбор структурированного вывода, повтор транзиентных отказов, учёт вызовов.
Оркестратор произвольный CLI не запускает.

Изоляция здесь — не гигиена, а цена. Замер на claude: обычный вызов стоит
33 802 входных токена (описания инструментов и системный промпт по умолчанию),
тот же вызов с `--tools ""`, собственным `--system-prompt` и
`--setting-sources ""` — 240. На четырёхстах элементах это разница между
0,1 млн и 13,5 млн токенов. Поэтому у модели физически нет инструментов, нет
доступа к репозиторию и нет ключей в окружении: она читает переданный ей
пакет доказательств и отвечает по схеме.

Отмена сделана честно: процесс запускается в собственной сессии, и убивается
вся группа. Иначе после Ctrl+C остаются жить сотни CLI-процессов, каждый со
своим соединением к провайдеру.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import signal
import subprocess
import tempfile
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable, Sequence

from . import settings

#: Помечает каждый порождённый процесс, чтобы осиротевшие можно было найти.
RUN_MARKER_ENV = "STAGE_COMPARISON_AI_RUN"

#: Окружение сессии модели собирается по БЕЛОМУ списку, а не вычищается по
#: чёрному. Чёрный список защищает ровно от тех имён, которые кто-то успел в
#: него внести: `DATABASE_URL`, `JWT_SECRET`, пароль Redis и любой ключ,
#: появившийся в проде на неделю позже правки этого файла, проезжали в дочерний
#: процесс без единого предупреждения. Белый список ошибается в обратную
#: сторону: незнакомая переменная просто не доедет.
#:
#: Здесь только то, без чего CLI не запустится или не найдёт свою подписку.
_ENV_ALLOWLIST = frozenset({
    "PATH",            # без него не найдётся ни node, ни сам CLI
    "HOME",            # ~/.claude.json, ~/.codex — там лежит подписка
    "USER", "LOGNAME",
    "LANG", "LC_ALL", "LC_CTYPE",
    "TMPDIR",
    "TZ",
    "TERM", "NO_COLOR",
    "CODEX_HOME",      # каталог авторизации Codex, если он переопределён
    # Корпоративные корневые сертификаты: без них TLS до провайдера не встанет.
    "SSL_CERT_FILE", "SSL_CERT_DIR", "NODE_EXTRA_CA_CERTS",
    "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE",
})

#: Прокси — это инфраструктура, а не секрет, но ровно до тех пор, пока в URL
#: нет `логин:пароль@`. С учётными данными переменная не едет вовсе: молча
#: отдать их в чужой процесс хуже, чем не достучаться до провайдера.
_PROXY_ENV = (
    "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY",
    "http_proxy", "https_proxy", "no_proxy",
)

#: Имена, которые не попадут в окружение НИКОГДА, даже если оператор явно
#: перечислил их в расширении белого списка. Расширение существует для
#: «не хватило переменной среды», а не для «протащить ключ».
_SECRET_NAME_RE = re.compile(
    r"(TOKEN|SECRET|PASSWORD|PASSWD|CREDENTIAL|API_?KEY|PRIVATE_KEY|"
    r"DATABASE_URL|_DSN|AUTH|SESSION_KEY|COOKIE|SALT|SIGNING)",
    re.I,
)

#: Расширение белого списка для конкретной машины: имена через запятую.
ENV_ALLOWLIST_EXTENSION = "STAGE_COMPARISON_AI_ENV_ALLOWLIST"

#: Возможности Codex, которых у аналитика сравнения быть не должно. Песочница
#: `-s read-only` ограничивает то, ЧТО команда может сделать, но не отменяет
#: саму возможность её выполнить: с включённым `shell_tool` модель по-прежнему
#: читает репозиторий, `.env`, чужие артефакты прогона и историю git. Аналитику
#: не нужен ни один инструмент: он получает пакет доказательств и отвечает по
#: схеме. Картинки визуального резерва передаются явно через `-i`, поэтому
#: `view_image` (чтение файла с диска по решению модели) тоже снимается.
CODEX_DISABLED_FEATURES = (
    "shell_tool",
    "unified_exec",
    "view_image",
    "browser_use",
    "browser_use_external",
    "browser_use_full_cdp_access",
    "computer_use",
    "code_mode_host",
    "hooks",
    "plugins",
    "plugin_sharing",
    "remote_plugin",
    "apps",
    "multi_agent",
    "skill_search",
    "skill_mcp_dependency_install",
    "image_generation",
    "in_app_local_automation",
    "shell_snapshot",
    "standalone_web_search",
    "tool_suggest",
    "workspace_dependencies",
)

#: Возможности, которые обязаны быть ВЫКЛЮЧЕНЫ, иначе прогон не стартует.
#: Проверка среды смотрит не на то, передали ли мы флаг, а на состояние после
#: его применения: переименованная в новой версии возможность иначе осталась бы
#: включённой, а `--disable` молча создал бы неиспользуемый ключ конфигурации.
CODEX_REQUIRED_OFF = (
    "shell_tool",
    "view_image",
    "browser_use",
    "computer_use",
    "hooks",
    "plugins",
)

#: Возможности, которые CLI обязан ЗНАТЬ, но чьё состояние гейтом не является.
#:
#: `unified_exec` на этой версии CLI не выключается ни `--disable`, ни
#: `-c features.unified_exec=false` — он остаётся `true` всегда. Он выбирает
#: РЕАЛИЗАЦИЮ выполнения команд, а не факт её наличия: при `shell_tool=false`
#: инструмента выполнения не предлагается вовсе. Проверено поведением, а не
#: чтением флага — на реальном вызове с этим набором ключей модель на прямую
#: просьбу выполнить `ls` ответила «инструмента нет».
#:
#: Имя всё равно проверяется на существование: если оно исчезнет из CLI,
#: значит устройство выполнения команд поменялось, и вывод выше надо
#: перепроверять, а не наследовать.
CODEX_MUST_BE_KNOWN = ("unified_exec",)

#: Отказы провайдера, которые проходят сами. Повторять их можно.
_TRANSIENT_MARKERS = (
    "at capacity", "overloaded", "rate limit", "rate_limit",
    "too many requests", "service unavailable", "temporarily unavailable",
    "bad gateway", "internal server error", "stream error",
    "stream disconnected", "connection reset", "connection refused",
    "connection closed", "econnreset", "etimedout",
)
#: Отказы, повторять которые — значит втрое быстрее сжечь остаток подписки.
_PERMANENT_MARKERS = (
    "usage limit reached", "quota", "insufficient_quota", "billing",
    "unauthorized", "not authenticated", "invalid api key",
    "model_not_found", "unsupported model", "permission denied",
)


class GatewayError(RuntimeError):
    """Ошибка конфигурации шлюза, а не отказ модели."""


class GatewayCancelled(RuntimeError):
    """Прогон отменён снаружи."""


@dataclass
class CallResult:
    provider_family: str
    model: str
    reasoning_level: str | None
    ok: bool
    parsed: dict | None = None
    error: str = ""
    error_kind: str = ""
    duration_ms: int = 0
    exit_code: int | None = None
    attempts: int = 1
    session_id: str | None = None
    usage: dict = field(default_factory=dict)
    raw_excerpt: str = ""

    def as_dict(self) -> dict:
        value = asdict(self)
        value["raw_excerpt"] = self.raw_excerpt[:2000]
        return value


class CancelToken:
    """Общий сигнал отмены для всех вызовов одного прогона."""

    def __init__(self) -> None:
        self._event = threading.Event()

    def cancel(self) -> None:
        self._event.set()

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()

    def raise_if_cancelled(self) -> None:
        if self._event.is_set():
            raise GatewayCancelled("stage comparison AI run cancelled")

    def wait(self, seconds: float) -> bool:
        return self._event.wait(seconds)


# ── Разбор ответа ──────────────────────────────────────────────────────────

def extract_json(text: str) -> dict | None:
    """Достать объект JSON из вывода: сначала честный разбор, затем скобки.

    Регулярка по свободному тексту как основной контракт запрещена. Сюда
    попадаем, только если нативный структурированный вывод не сработал.
    """
    text = (text or "").strip()
    if not text:
        return None
    try:
        value = json.loads(text)
        return value if isinstance(value, dict) else None
    except json.JSONDecodeError:
        pass
    if "```" in text:
        for chunk in text.split("```"):
            chunk = chunk.strip()
            if chunk.startswith("json"):
                chunk = chunk[4:].strip()
            if chunk.startswith("{"):
                try:
                    value = json.loads(chunk)
                    if isinstance(value, dict):
                        return value
                except json.JSONDecodeError:
                    continue
    start = text.find("{")
    while start != -1:
        depth, in_string, escaped = 0, False, False
        for index in range(start, len(text)):
            char = text[index]
            if in_string:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == '"':
                    in_string = False
                continue
            if char == '"':
                in_string = True
            elif char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    try:
                        value = json.loads(text[start:index + 1])
                        if isinstance(value, dict):
                            return value
                    except json.JSONDecodeError:
                        break
        start = text.find("{", start + 1)
    return None


def classify_failure(text: str) -> str:
    low = (text or "").lower()
    if any(marker in low for marker in _PERMANENT_MARKERS):
        return "PERMANENT"
    if any(marker in low for marker in _TRANSIENT_MARKERS):
        return "TRANSIENT"
    return "UNKNOWN"


# ── Окружение и процессы ───────────────────────────────────────────────────

def _proxy_carries_credentials(value: str) -> bool:
    """True, когда в URL прокси есть `логин:пароль@`."""
    head = (value or "").split("/")[-1] if "//" not in value else value.split("//", 1)[1]
    return "@" in head.split("/")[0]


def allowed_env_names() -> frozenset[str]:
    """Белый список этой машины: фиксированный плюс разрешённое расширение."""
    extra = {
        name.strip()
        for name in (os.environ.get(ENV_ALLOWLIST_EXTENSION) or "").split(",")
        if name.strip() and not _SECRET_NAME_RE.search(name.strip())
    }
    return frozenset(_ENV_ALLOWLIST | extra)


def _clean_env(run_id: str) -> dict[str, str]:
    """Окружение дочернего процесса: только разрешённое, ничего лишнего."""
    allowed = allowed_env_names()
    env = {
        key: value for key, value in os.environ.items()
        if key in allowed and not _SECRET_NAME_RE.search(key)
    }
    for name in _PROXY_ENV:
        value = os.environ.get(name)
        if value and not _proxy_carries_credentials(value):
            env[name] = value
    env.setdefault("LANG", "ru_RU.UTF-8")
    # CLI в неинтерактивном режиме не должен рисовать рамки и цвета: их
    # управляющие последовательности попадают в разбираемый поток.
    env.setdefault("TERM", "dumb")
    env.setdefault("NO_COLOR", "1")
    env[RUN_MARKER_ENV] = run_id
    return env


def _resolve_codex_binary() -> str:
    configured = settings.codex_binary()
    if configured and Path(configured).exists():
        return configured
    found = shutil.which("codex")
    if found:
        return found
    for candidate in sorted(
        Path("/home/coder/.vscode-server/extensions").glob(
            "openai.chatgpt-*/bin/*/codex"
        ),
        reverse=True,
    ):
        if candidate.exists():
            return str(candidate)
    raise GatewayError("codex CLI не найден: задайте STAGE_COMPARISON_AI_CODEX_BIN")


def _resolve_claude_binary() -> str:
    configured = settings.claude_binary()
    found = shutil.which(configured) or (
        configured if Path(configured).exists() else None
    )
    if not found:
        raise GatewayError("claude CLI не найден: задайте STAGE_COMPARISON_AI_CLAUDE_BIN")
    return found


def _run_process(
    command: Sequence[str],
    *,
    cwd: str,
    env: dict[str, str],
    timeout_s: int,
    stdin_text: str | None,
    cancel: CancelToken | None,
    run_id: str = "",
) -> tuple[int | None, str, str, str]:
    """Запустить процесс в своей сессии; вернуть (код, stdout, stderr, отказ).

    Четвёртое значение — вид отказа: "" при нормальном завершении,
    ``TIMEOUT`` или ``CANCELLED``.
    """
    process = subprocess.Popen(  # noqa: S603 — команда собрана здесь же
        list(command),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=cwd,
        env=env,
        start_new_session=True,
    )
    _REGISTRY.add(process, run_id)
    try:
        # stdin закрывает сам communicate(): без этого CLI ждёт ввод три
        # секунды и печатает предупреждение прямо в разбираемый поток.
        deadline = time.monotonic() + timeout_s
        reader: dict[str, str] = {}

        def communicate() -> None:
            try:
                out, err = process.communicate(input=stdin_text)
            except Exception as exc:  # pragma: no cover — защитный путь
                out, err = "", str(exc)
            reader["stdout"] = out or ""
            reader["stderr"] = err or ""

        worker = threading.Thread(target=communicate, daemon=True)
        worker.start()
        while worker.is_alive():
            if cancel is not None and cancel.cancelled:
                _kill_process_group(process)
                worker.join(timeout=5)
                return None, reader.get("stdout", ""), reader.get("stderr", ""), "CANCELLED"
            if time.monotonic() > deadline:
                _kill_process_group(process)
                worker.join(timeout=5)
                return None, reader.get("stdout", ""), reader.get("stderr", ""), "TIMEOUT"
            worker.join(timeout=0.2)
        return (
            process.returncode,
            reader.get("stdout", ""),
            reader.get("stderr", ""),
            "",
        )
    finally:
        _REGISTRY.discard(process)


def _kill_process_group(process: subprocess.Popen) -> None:
    """Убить всю группу: CLI поднимает дочерние процессы, и они переживают его."""
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(os.getpgid(process.pid), sig)
        except (ProcessLookupError, PermissionError, OSError):
            break
        try:
            process.wait(timeout=3)
            return
        except subprocess.TimeoutExpired:
            continue


class _ProcessRegistry:
    """Живые процессы прогона: нужны, чтобы отмена не оставила сирот.

    Процессы помечены идентификатором прогона. Без этого отмена одной пары
    сносила бы вызовы соседней: `kill_all()` убивал всё, что породил шлюз в
    этом процессе бэкенда, а параллельные пары в очереди — обычный режим.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._items: dict[subprocess.Popen, str] = {}

    def add(self, process: subprocess.Popen, run_id: str = "") -> None:
        with self._lock:
            self._items[process] = run_id

    def discard(self, process: subprocess.Popen) -> None:
        with self._lock:
            self._items.pop(process, None)

    def kill_all(self, run_id: str = "") -> int:
        with self._lock:
            items = [
                process for process, owner in self._items.items()
                if not run_id or owner == run_id
            ]
        for process in items:
            _kill_process_group(process)
        return len(items)

    def size(self, run_id: str = "") -> int:
        with self._lock:
            return sum(
                1 for owner in self._items.values()
                if not run_id or owner == run_id
            )


_REGISTRY = _ProcessRegistry()


def kill_live_processes(run_id: str = "") -> int:
    """Убить процессы шлюза. Без ``run_id`` — все, иначе только этого прогона."""
    return _REGISTRY.kill_all(run_id)


def find_orphaned_processes() -> list[dict[str, Any]]:
    """Найти CLI-сессии прошлых прогонов, оставшиеся без родителя.

    Процесс запускается в собственной сессии — это цена честной отмены: убить
    можно всю группу. Обратная сторона в том, что упавший бэкенд не уносит их
    за собой. Метка в окружении делает такие сироты находимыми: без неё
    отличить их от чужого `codex exec` нечем, а убивать чужое нельзя.
    """
    orphans: list[dict[str, Any]] = []
    proc = Path("/proc")
    if not proc.is_dir():
        return orphans
    for entry in proc.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            environ = (entry / "environ").read_bytes()
        except (OSError, PermissionError):
            continue
        if RUN_MARKER_ENV.encode() not in environ:
            continue
        try:
            status = (entry / "status").read_text(encoding="utf-8", errors="replace")
        except (OSError, PermissionError):
            continue
        parent = 0
        for line in status.splitlines():
            if line.startswith("PPid:"):
                parent = int(line.split()[1] or 0)
                break
        if parent != 1:
            continue
        run_id = ""
        for chunk in environ.split(b"\0"):
            if chunk.startswith(RUN_MARKER_ENV.encode() + b"="):
                run_id = chunk.split(b"=", 1)[1].decode("utf-8", "replace")
                break
        orphans.append({"pid": int(entry.name), "run_id": run_id})
    return orphans


def reap_orphaned_processes(*, keep_run_id: str = "") -> int:
    """Убить осиротевшие сессии прошлых прогонов. Текущий прогон не трогаем."""
    killed = 0
    for orphan in find_orphaned_processes():
        if keep_run_id and orphan["run_id"] == keep_run_id:
            continue
        try:
            os.killpg(os.getpgid(orphan["pid"]), signal.SIGKILL)
            killed += 1
        except (ProcessLookupError, PermissionError, OSError):
            continue
    return killed


def live_process_count(run_id: str = "") -> int:
    return _REGISTRY.size(run_id)


# ── Вызовы ─────────────────────────────────────────────────────────────────

def call_codex(
    prompt: str,
    *,
    model: str,
    schema: dict | None = None,
    reasoning_level: str | None = None,
    timeout_s: int | None = None,
    images: Iterable[str] = (),
    retries: int = 1,
    cancel: CancelToken | None = None,
    run_id: str = "",
) -> CallResult:
    """Один изолированный вызов Codex. Песочница только на чтение."""
    binary = _resolve_codex_binary()
    timeout_s = timeout_s or settings.call_timeout_seconds()
    run_id = run_id or uuid.uuid4().hex
    workdir = Path(tempfile.mkdtemp(prefix="sc_ai_codex_"))
    image_paths = [str(value) for value in images]
    try:
        out_file = workdir / "last_message.txt"
        command = [
            binary, "exec",
            "-m", model,
            "-s", "read-only",           # даже без инструментов — второй рубеж
            "--skip-git-repo-check",
            "--ephemeral",
            "--ignore-user-config",      # без ~/.codex/config.toml и его MCP
            "--ignore-rules",            # без пользовательских execpolicy
            "-C", str(workdir),          # пустой временный каталог, не репозиторий
            "-o", str(out_file),
        ]
        for feature in CODEX_DISABLED_FEATURES:
            command += ["--disable", feature]
        if reasoning_level:
            command += ["-c", f"model_reasoning_effort={reasoning_level}"]
        if schema is not None:
            schema_file = workdir / "schema.json"
            schema_file.write_text(
                json.dumps(schema, ensure_ascii=False), encoding="utf-8"
            )
            command += ["--output-schema", str(schema_file)]
        # Промпт всегда уходит через stdin, а на месте позиционного аргумента
        # стоит «-». Две причины. Пакет доказательств на партию из десяти
        # элементов — это десятки килобайт: в argv он рискует упереться в
        # ARG_MAX и виден любому в `ps`. И `-i/--image` объявлен как <FILE>...,
        # то есть переменной длины: позиционный промпт после него был бы
        # проглочен как ещё один файл.
        if image_paths:
            command += ["-i", *image_paths]
        command.append("-")
        stdin_text: str | None = prompt

        last: CallResult | None = None
        for attempt in range(retries + 1):
            if cancel is not None and cancel.cancelled:
                return CallResult(
                    settings.CODEX_SESSION, model, reasoning_level, False,
                    error="отменено", error_kind="CANCELLED", attempts=attempt + 1,
                )
            started = time.perf_counter()
            code, stdout, stderr, failure = _run_process(
                command, cwd=str(workdir), env=_clean_env(run_id),
                timeout_s=timeout_s, stdin_text=stdin_text, cancel=cancel,
                run_id=run_id,
            )
            duration_ms = int((time.perf_counter() - started) * 1000)
            if failure:
                return CallResult(
                    settings.CODEX_SESSION, model, reasoning_level, False,
                    error=("превышен таймаут" if failure == "TIMEOUT" else "отменено"),
                    error_kind=failure, duration_ms=duration_ms, attempts=attempt + 1,
                )
            combined = f"{stdout}\n{stderr}"
            payload = None
            if out_file.exists():
                payload = extract_json(out_file.read_text(encoding="utf-8"))
            if payload is None:
                payload = extract_json(stdout)
            if payload is not None:
                return CallResult(
                    settings.CODEX_SESSION, model, reasoning_level, True,
                    parsed=payload, duration_ms=duration_ms, exit_code=code,
                    attempts=attempt + 1, raw_excerpt=combined[-2000:],
                )
            kind = classify_failure(combined)
            last = CallResult(
                settings.CODEX_SESSION, model, reasoning_level, False,
                error=(combined.strip()[-500:] or "пустой ответ"),
                error_kind=kind, duration_ms=duration_ms, exit_code=code,
                attempts=attempt + 1, raw_excerpt=combined[-2000:],
            )
            if attempt < retries and kind == "TRANSIENT":
                if cancel is not None and cancel.wait(3 * (attempt + 1)):
                    break
                elif cancel is None:
                    time.sleep(3 * (attempt + 1))
                continue
            break
        assert last is not None
        return last
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def call_claude(
    prompt: str,
    *,
    model: str,
    schema: dict | None = None,
    reasoning_level: str | None = None,
    timeout_s: int | None = None,
    system_prompt: str | None = None,
    retries: int = 1,
    cancel: CancelToken | None = None,
    run_id: str = "",
) -> CallResult:
    """Один изолированный вызов Claude без права трогать файловую систему."""
    binary = _resolve_claude_binary()
    timeout_s = timeout_s or settings.call_timeout_seconds()
    run_id = run_id or uuid.uuid4().hex
    workdir = Path(tempfile.mkdtemp(prefix="sc_ai_claude_"))
    try:
        # Промпт уходит через stdin — по тем же причинам, что и у Codex:
        # ARG_MAX и `ps`. Симметрия здесь не косметика: контракт безопасности
        # у двух семейств моделей обязан быть одинаковым, иначе «изолировано»
        # означает «изолировано у одного из двух».
        command = [
            binary, "-p",
            "--model", model,
            "--output-format", "json",
            "--tools", "",              # инструментов физически нет
            "--setting-sources", "",    # без ~/.claude/settings.json
            "--strict-mcp-config",      # без внешних MCP-серверов
            "--system-prompt",
            system_prompt or "Ты — точный аналитик. Отвечай строго по схеме.",
            "--no-session-persistence",
            "--disable-slash-commands",
        ]
        if schema is not None:
            command += ["--json-schema", json.dumps(schema, ensure_ascii=False)]
        if reasoning_level:
            command += ["--effort", reasoning_level]

        last: CallResult | None = None
        for attempt in range(retries + 1):
            if cancel is not None and cancel.cancelled:
                return CallResult(
                    settings.CLAUDE_SESSION, model, reasoning_level, False,
                    error="отменено", error_kind="CANCELLED", attempts=attempt + 1,
                )
            started = time.perf_counter()
            code, stdout, stderr, failure = _run_process(
                command, cwd=str(workdir), env=_clean_env(run_id),
                timeout_s=timeout_s, stdin_text=prompt, cancel=cancel,
                run_id=run_id,
            )
            duration_ms = int((time.perf_counter() - started) * 1000)
            if failure:
                return CallResult(
                    settings.CLAUDE_SESSION, model, reasoning_level, False,
                    error=("превышен таймаут" if failure == "TIMEOUT" else "отменено"),
                    error_kind=failure, duration_ms=duration_ms, attempts=attempt + 1,
                )
            envelope = extract_json(stdout)
            usage: dict[str, Any] = {}
            session_id = None
            payload = None
            if envelope and "result" in envelope:
                usage = dict(envelope.get("usage") or {})
                session_id = envelope.get("session_id")
                if envelope.get("is_error"):
                    text = str(envelope.get("result"))
                    return CallResult(
                        settings.CLAUDE_SESSION, model, reasoning_level, False,
                        error=text[:500], error_kind=classify_failure(text),
                        duration_ms=duration_ms, exit_code=code,
                        attempts=attempt + 1, session_id=session_id, usage=usage,
                    )
                payload = envelope.get("structured_output") or extract_json(
                    str(envelope.get("result"))
                )
                usage["total_input_tokens"] = sum(
                    usage.get(key) or 0 for key in (
                        "input_tokens",
                        "cache_creation_input_tokens",
                        "cache_read_input_tokens",
                    )
                )
            else:
                payload = envelope
            if payload is not None:
                return CallResult(
                    settings.CLAUDE_SESSION, model, reasoning_level, True,
                    parsed=payload, duration_ms=duration_ms, exit_code=code,
                    attempts=attempt + 1, session_id=session_id, usage=usage,
                    raw_excerpt=stdout[-2000:],
                )
            combined = f"{stdout}\n{stderr}"
            kind = classify_failure(combined)
            last = CallResult(
                settings.CLAUDE_SESSION, model, reasoning_level, False,
                error=(combined.strip()[-500:] or "не удалось получить JSON"),
                error_kind=kind, duration_ms=duration_ms, exit_code=code,
                attempts=attempt + 1, session_id=session_id, usage=usage,
                raw_excerpt=combined[-2000:],
            )
            if attempt < retries and kind == "TRANSIENT":
                if cancel is not None and cancel.wait(3 * (attempt + 1)):
                    break
                elif cancel is None:
                    time.sleep(3 * (attempt + 1))
                continue
            break
        assert last is not None
        return last
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def call(
    provider_family: str,
    prompt: str,
    **kwargs: Any,
) -> CallResult:
    """Единственная функция, которую зовёт слой разрешения."""
    if provider_family == settings.CLAUDE_SESSION:
        kwargs.pop("images", None)
        return call_claude(prompt, **kwargs)
    if provider_family == settings.CODEX_SESSION:
        kwargs.pop("system_prompt", None)
        return call_codex(prompt, **kwargs)
    raise GatewayError(f"неизвестное семейство провайдера: {provider_family}")


def _cli_probe(command: Sequence[str], *, timeout_s: int = 30) -> str:
    """Спросить у CLI его собственную справку/состояние. Без сети и без модели."""
    try:
        finished = subprocess.run(  # noqa: S603 — команда собрана здесь же
            list(command),
            capture_output=True,
            text=True,
            timeout=timeout_s,
            cwd=tempfile.gettempdir(),
            env=_clean_env("validate"),
            start_new_session=True,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    return f"{finished.stdout}\n{finished.stderr}"


def _codex_feature_states(binary: str) -> dict[str, str]:
    """Состояние флагов возможностей ПОСЛЕ применения наших `--disable`.

    `codex features list` принимает те же `--disable`, что и `codex exec`,
    поэтому проверка отвечает не на вопрос «передали ли мы флаг», а на вопрос
    «выключено ли оно на самом деле».
    """
    command = [binary, "features", "list"]
    for feature in CODEX_DISABLED_FEATURES:
        command += ["--disable", feature]
    states: dict[str, str] = {}
    for line in _cli_probe(command).splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[-1] in {"true", "false"}:
            states[parts[0]] = parts[-1]
    return states


def validate_runtime(
    *,
    require_vision: bool = False,
    deep: bool | None = None,
    mode: str | None = None,
) -> dict[str, Any]:
    """Проверить среду ДО прогона: транспорт, изоляция, структурный вывод.

    Модель не хардкодится: если CLI на этой машине её не знает, честнее узнать
    об этом при старте этапа, чем на четырёхсотом элементе. То же и с
    изоляцией: `--disable shell_tool` на переименованной в новой версии
    возможности молча превращается в неиспользуемый ключ конфигурации, и
    аналитик получает shell обратно, ничем этого не показав.

    Проверка офлайн: ни одного обращения к провайдеру, ни одного токена.
    """
    report: dict[str, Any] = {
        "ok": True,
        "problems": [],
        "binaries": {},
        "checks": {},
        # Режим ЭТОГО прогона, а не установки: аудитный след обязан объяснять
        # тот прогон, к которому приложен. Без параметра остаётся прежний
        # путь — настройка установки.
        "mode": settings.normalize_mode(mode) if mode else settings.mode(),
    }

    def fail(problem: str) -> None:
        report["ok"] = False
        report["problems"].append(problem)

    codex_binary = ""
    try:
        codex_binary = _resolve_codex_binary()
        report["binaries"]["CODEX_SESSION"] = codex_binary
    except GatewayError as exc:
        fail(str(exc))

    if codex_binary:
        help_text = _cli_probe([codex_binary, "exec", "--help"])
        report["checks"]["codex_help_readable"] = bool(help_text.strip())
        if not help_text.strip():
            fail("codex CLI не отвечает на `exec --help`")
        else:
            required_flags = {
                "structured_output": "--output-schema",
                "sandbox": "--sandbox",
                "ignore_user_config": "--ignore-user-config",
                "feature_switch": "--disable",
                "reasoning_level": "--config",
            }
            if require_vision:
                required_flags["vision"] = "--image"
            for name, flag in required_flags.items():
                present = flag in help_text
                report["checks"][f"codex_{name}"] = present
                if not present:
                    fail(f"codex CLI не поддерживает {flag} ({name})")
        states = _codex_feature_states(codex_binary) if help_text.strip() else {}
        report["checks"]["codex_features_probed"] = bool(states)
        if help_text.strip() and not states:
            fail("codex CLI не отвечает на `features list`: изоляция не проверена")
        observed: dict[str, str] = {}
        for feature in CODEX_REQUIRED_OFF:
            state = states.get(feature)
            observed[feature] = state or "UNKNOWN"
            if state is None:
                fail(
                    f"codex CLI не знает возможности {feature!r}: "
                    "изоляция сессии не подтверждена"
                )
            elif state != "false":
                fail(f"codex CLI оставляет {feature!r} включённой")
        for feature in CODEX_MUST_BE_KNOWN:
            state = states.get(feature)
            observed[feature] = state or "UNKNOWN"
            if state is None:
                fail(
                    f"codex CLI больше не знает {feature!r}: устройство "
                    "выполнения команд изменилось, изоляцию надо перепроверить"
                )
        report["checks"]["codex_isolation_features"] = observed

    needs_critic = settings.deep() if deep is None else bool(deep)
    if needs_critic:
        claude_binary = ""
        try:
            claude_binary = _resolve_claude_binary()
            report["binaries"]["CLAUDE_SESSION"] = claude_binary
        except GatewayError as exc:
            fail(str(exc))
        if claude_binary:
            help_text = _cli_probe([claude_binary, "--help"])
            report["checks"]["claude_help_readable"] = bool(help_text.strip())
            if not help_text.strip():
                fail("claude CLI не отвечает на `--help`")
            else:
                for name, flag in (
                    ("structured_output", "--json-schema"),
                    ("tools_switch", "--tools"),
                    ("setting_sources", "--setting-sources"),
                    ("strict_mcp", "--strict-mcp-config"),
                    ("system_prompt", "--system-prompt"),
                ):
                    present = flag in help_text
                    report["checks"][f"claude_{name}"] = present
                    if not present:
                        fail(f"claude CLI не поддерживает {flag} ({name})")

    leaked = sorted(
        name for name in _clean_env("validate")
        if _SECRET_NAME_RE.search(name)
    )
    report["checks"]["environment_names"] = sorted(_clean_env("validate"))
    report["checks"]["environment_leaked_secrets"] = leaked
    if leaked:
        fail(f"в окружение сессии попали секреты: {', '.join(leaked)}")
    return report


__all__ = [
    "CallResult",
    "CancelToken",
    "GatewayCancelled",
    "GatewayError",
    "RUN_MARKER_ENV",
    "call",
    "call_claude",
    "call_codex",
    "classify_failure",
    "extract_json",
    "find_orphaned_processes",
    "kill_live_processes",
    "live_process_count",
    "reap_orphaned_processes",
    "validate_runtime",
]

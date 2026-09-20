"""Шлюз ИИ-сессий сравнения — единственная точка запуска моделей.

Сравнение документации не ходит в HTTP-API моделей. Разрешены ровно два
транспорта, оба по подписке:

    CLAUDE_SESSION — `claude -p`
    CODEX_SESSION  — `codex exec`; текст длиннее одного хода Codex —
                     `codex app-server` (история треда + последний кусок)

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

import base64
import hashlib
import json
import os
import queue
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
    json_events: bool = False,
) -> CallResult:
    """Один изолированный вызов Codex. Песочница только на чтение.

    `json_events=True` добавляет `--json`: stdout становится потоком событий
    JSONL, и расход токенов хода берётся из события `turn.completed` в
    `CallResult.usage`. Ответ тогда читается ТОЛЬКО из `-o` (последнее
    сообщение) — поток событий как ответ не разбирается никогда.
    """
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
        if json_events:
            command.append("--json")
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
            usage = exec_event_usage(stdout) if json_events else {}
            payload = None
            if out_file.exists():
                payload = extract_json(out_file.read_text(encoding="utf-8"))
            if payload is None and not json_events:
                payload = extract_json(stdout)
            if payload is not None:
                return CallResult(
                    settings.CODEX_SESSION, model, reasoning_level, True,
                    parsed=payload, duration_ms=duration_ms, exit_code=code,
                    attempts=attempt + 1, raw_excerpt=combined[-2000:], usage=usage,
                )
            kind = classify_failure(combined)
            last = CallResult(
                settings.CODEX_SESSION, model, reasoning_level, False,
                error=(combined.strip()[-500:] or "пустой ответ"),
                error_kind=kind, duration_ms=duration_ms, exit_code=code,
                attempts=attempt + 1, raw_excerpt=combined[-2000:], usage=usage,
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


def exec_event_usage(stdout: str) -> dict[str, Any]:
    """Расход токенов из событий `codex exec --json` (последнее `turn.completed`)."""
    usage: dict[str, Any] = {}
    for line in (stdout or "").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict) and event.get("type") == "turn.completed" and isinstance(event.get("usage"), dict):
            usage = dict(event["usage"])
    return usage


def _app_server_wire_text(lines: Sequence[bytes]) -> str:
    """User text exactly as the serialized JSON-RPC lines carry it, in order."""
    texts: list[str] = []
    for line in lines:
        message = json.loads(line)
        params = message.get("params") or {}
        if message.get("method") == "thread/inject_items":
            for item in params.get("items") or []:
                texts.extend(c["text"] for c in item.get("content") or [] if c.get("type") == "input_text")
        elif message.get("method") == "turn/start":
            texts.extend(i["text"] for i in params.get("input") or [] if i.get("type") == "text")
    return "".join(texts)


def call_codex_app_server(
    history: Sequence[str],
    final_text: str,
    *,
    model: str,
    expected_text_sha256: str,
    schema: dict | None = None,
    reasoning_level: str | None = None,
    timeout_s: int | None = None,
    images: Iterable[str] = (),
    cancel: CancelToken | None = None,
    run_id: str = "",
    cyber_access_program: str | None = None,
) -> tuple[CallResult, dict[str, Any]]:
    """Один вызов Codex через `codex app-server` для текста длиннее одного хода.

    Третий транспорт по подписке (HTTP-API по-прежнему нет). Ход Codex не
    принимает больше 1 048 576 символов текста, поэтому ведущие куски кладутся
    в историю свежего эфемерного треда (`thread/inject_items`), а последний
    кусок вместе с картинками запускает ход. Перед запуском текст собирается
    обратно из уже сериализованных строк JSON-RPC; если он не совпал с
    `expected_text_sha256` побайтно, процесс не запускается вовсе.
    Возвращает результат и квитанцию провода (без текста промпта).
    """
    binary = _resolve_codex_binary()
    timeout_s = timeout_s or settings.call_timeout_seconds()
    run_id = run_id or uuid.uuid4().hex
    image_paths = [str(Path(value).resolve()) for value in images]
    workdir = Path(tempfile.mkdtemp(prefix="sc_ai_codex_app_"))
    history_items = [
        {"type": "message", "role": "user", "content": [{"type": "input_text", "text": chunk}]}
        for chunk in history
    ]
    turn_input: list[dict[str, Any]] = [{"type": "text", "text": final_text}]
    turn_input += [{"type": "localImage", "path": path} for path in image_paths]

    def encode(message: dict[str, Any]) -> bytes:
        return (json.dumps(message, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")

    def planned(thread_id: str) -> list[bytes]:
        turn_params: dict[str, Any] = {
            "threadId": thread_id, "input": turn_input, "model": model, "cwd": str(workdir),
            "approvalPolicy": "never",
        }
        if reasoning_level:
            turn_params["effort"] = reasoning_level
        if schema is not None:
            turn_params["outputSchema"] = schema
        if cyber_access_program:
            turn_params["cyberAccessProgram"] = cyber_access_program
        lines = ([encode({"method": "thread/inject_items", "id": 3,
                          "params": {"threadId": thread_id, "items": history_items}})] if history_items else [])
        lines.append(encode({"method": "turn/start", "id": 4 if history_items else 3, "params": turn_params}))
        return lines

    wire_text = _app_server_wire_text(planned("pending"))
    wire = {
        "transport": "codex_app_server",
        "wire_text_sha256": hashlib.sha256(wire_text.encode("utf-8")).hexdigest(),
        "wire_text_chars": len(wire_text),
        "history_items": len(history_items),
        "turn_text_chars": len(final_text),
        "images": len(image_paths),
        "cyber_access_program": cyber_access_program,
    }
    if wire["wire_text_sha256"] != expected_text_sha256:
        shutil.rmtree(workdir, ignore_errors=True)
        raise GatewayError("app-server: собранный текст не совпал с полезной нагрузкой — вызов не выполнен")

    command = [
        binary, "app-server", "--stdio",
        "-c", 'model_provider="openai"',
        "-c", 'approval_policy="never"',
        "-c", 'sandbox_mode="read-only"',
        "-c", 'web_search="disabled"',
        "-c", "mcp_servers={}",
        "-c", "project_doc_max_bytes=0",
    ]
    if reasoning_level:
        command += ["-c", f'model_reasoning_effort="{reasoning_level}"']
    for feature in CODEX_DISABLED_FEATURES:
        command += ["--disable", feature]
    started = time.perf_counter()
    deadline = time.monotonic() + timeout_s
    stderr_file = (workdir / "stderr.txt").open("wb")
    process = subprocess.Popen(  # noqa: S603 — команда собрана здесь же
        command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=stderr_file,
        cwd=str(workdir), env=_clean_env(run_id), start_new_session=True,
    )
    _REGISTRY.add(process, run_id)
    inbox: "queue.Queue[bytes | None]" = queue.Queue()

    def pump() -> None:
        assert process.stdout is not None
        for raw in iter(process.stdout.readline, b""):
            inbox.put(raw)
        inbox.put(None)

    threading.Thread(target=pump, daemon=True).start()
    state: dict[str, Any] = {"messages": [], "usage": {}, "tool_items": set(), "turn": None}

    def result(ok: bool, **kwargs: Any) -> CallResult:
        return CallResult(settings.CODEX_SESSION, model, reasoning_level, ok,
                          duration_ms=int((time.perf_counter() - started) * 1000),
                          exit_code=process.poll(), usage=state["usage"], **kwargs)

    def send(data: bytes) -> None:
        assert process.stdin is not None
        process.stdin.write(data)
        process.stdin.flush()

    def receive(request_id: int | None) -> dict[str, Any]:
        while True:
            if cancel is not None and cancel.cancelled:
                raise GatewayCancelled("stage comparison AI run cancelled")
            if time.monotonic() > deadline:
                raise TimeoutError("app-server timeout")
            try:
                raw = inbox.get(timeout=0.2)
            except queue.Empty:
                continue
            if raw is None:
                raise RuntimeError("app-server закрыл поток до завершения хода")
            message = json.loads(raw)
            method = message.get("method")
            if method and "id" in message:  # server request: nothing may be approved
                send(encode({"id": message["id"], "error": {"code": -32601, "message": "not supported"}}))
                state["tool_items"].add(f"request:{method}")
                continue
            if request_id is not None and message.get("id") == request_id:
                if "error" in message:
                    raise RuntimeError(f"app-server {request_id}: {json.dumps(message['error'], ensure_ascii=False)}")
                return message.get("result") or {}
            if method == "thread/tokenUsage/updated":
                last = ((message.get("params") or {}).get("tokenUsage") or {}).get("last") or {}
                state["usage"] = {k: last.get(k, 0) for k in (
                    "inputTokens", "cachedInputTokens", "outputTokens", "reasoningOutputTokens")}
            elif method == "item/completed":
                item = (message.get("params") or {}).get("item") or {}
                if item.get("type") == "agentMessage":
                    state["messages"].append(item.get("text") or "")
                elif item.get("type") not in {"reasoning", "userMessage"}:
                    state["tool_items"].add(str(item.get("type")))
            elif method == "turn/completed":
                state["turn"] = (message.get("params") or {}).get("turn") or {}
                if request_id is None:
                    return state["turn"]

    try:
        send(encode({"method": "initialize", "id": 1, "params": {
            "clientInfo": {"name": "projectchange-v3", "version": "1"},
            "capabilities": {"experimentalApi": True}}}))
        receive(1)
        send(encode({"method": "initialized", "params": {}}))
        send(encode({"method": "thread/start", "id": 2, "params": {
            "model": model, "cwd": str(workdir), "approvalPolicy": "never",
            "sandbox": "read-only", "ephemeral": True}}))
        lines = planned(str(receive(2)["thread"]["id"]))
        if hashlib.sha256(_app_server_wire_text(lines).encode("utf-8")).hexdigest() != expected_text_sha256:
            raise GatewayError("app-server: собранный текст не совпал с полезной нагрузкой — вызов не выполнен")
        for index, line in enumerate(lines):
            send(line)
            receive(3 + index)
        turn = state["turn"] or receive(None)
        wire["thread_ephemeral"] = True
        if turn.get("status") != "completed" or turn.get("error"):
            text = json.dumps(turn.get("error"), ensure_ascii=False)
            return result(False, error=text[-500:] or str(turn.get("status")),
                          error_kind=classify_failure(text), raw_excerpt=text[-2000:]), wire
        if state["tool_items"]:
            return result(False, error=f"инструменты в ответе: {sorted(state['tool_items'])}",
                          error_kind="PERMANENT"), wire
        payload = extract_json(state["messages"][-1]) if state["messages"] else None
        if payload is None:
            return result(False, error="пустой ответ", error_kind="UNKNOWN"), wire
        return result(True, parsed=payload, raw_excerpt=state["messages"][-1][-2000:]), wire
    except GatewayCancelled:
        return result(False, error="отменено", error_kind="CANCELLED"), wire
    except TimeoutError:
        return result(False, error="превышен таймаут", error_kind="TIMEOUT"), wire
    except (RuntimeError, ValueError, KeyError, OSError) as exc:
        text = str(exc)
        return result(False, error=text[-500:], error_kind=classify_failure(text), raw_excerpt=text[-2000:]), wire
    finally:
        try:
            if process.stdin is not None:
                process.stdin.close()
        except OSError:
            pass
        if process.poll() is None:
            _kill_process_group(process)
        _REGISTRY.discard(process)
        stderr_file.close()
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


# ── Claude с изображениями: stream-json ────────────────────────────────────
#
# `call_claude` отдаёт модели только текст: в текстовом режиме `claude -p`
# изображению некуда деться. Вход `--input-format stream-json` принимает одно
# сообщение пользователя с блоками содержимого, в том числе с изображениями в
# base64 (тот же вид, что у Agent SDK). CLI при этом требует и вывод
# `stream-json` (иначе отказывается стартовать) вместе с `--verbose`.
#
# Факты прочитаны в самом CLI 2.1.270, а не в документации, — она про этот
# режим молчит. Всё, что из них следует для целостности доказательств
# (пределы изображений, молчаливое вырезание медиа сверх лимита), проверяет
# ВЫЗЫВАЮЩИЙ до обращения: см. `project_change_v3/transport.py`.

#: Уровни усилия, которые знает CLI. Незнакомое значение он НЕ отвергает, а
#: молча заменяет уровнем по умолчанию, напечатав предупреждение в stderr.
#: Поэтому значение проверяется здесь и до запуска процесса.
CLAUDE_EFFORT_LEVELS = ("low", "medium", "high", "xhigh", "max")

#: Системный промпт вызова с изображениями. `--system-prompt` ЗАМЕНЯЕТ
#: встроенный промпт агента-программиста. Текст виден модели, поэтому он
#: короткий, без предметного содержания, а его хеш уходит в квитанцию.
CLAUDE_MULTIMODAL_SYSTEM_PROMPT = (
    "Выполни задание пользователя. Ответ верни строго по заданной JSON-схеме."
)

#: Потолок выходных токенов одного запроса (вместе с рассуждением). У CLI по
#: умолчанию он вдвое ниже, а упёршийся в потолок ответ — это отказ вызова.
CLAUDE_MAX_OUTPUT_TOKENS = 128_000

_SYNTHETIC_MODEL = "<synthetic>"
_IMAGE_MAGIC = (
    (b"\x89PNG\r\n\x1a\n", "image/png"),
    (b"\xff\xd8\xff", "image/jpeg"),
    (b"GIF87a", "image/gif"),
    (b"GIF89a", "image/gif"),
)


def image_media_type(data: bytes) -> str | None:
    """Тип изображения по сигнатуре файла, а не по расширению."""
    for magic, media_type in _IMAGE_MAGIC:
        if data.startswith(magic):
            return media_type
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    return None


def same_claude_model(requested: str, reported: str) -> bool:
    """Тот ли это идентификатор модели.

    `[1m]` — псевдоним окна контекста на стороне CLI, не другая модель. API
    может вернуть идентификатор с датой выпуска (`…-20260801`). Любое иное
    отличие — другая модель.
    """
    def base(value: str) -> str:
        return re.sub(r"(\[1m\])+$", "", (value or "").strip(), flags=re.IGNORECASE)

    want, got = base(requested), base(reported)
    if not want or not got:
        return False
    return got == want or re.fullmatch(re.escape(want) + r"-\d{8}", got) is not None


def build_claude_user_message(
    prompt: str,
    images: Sequence[str | os.PathLike[str]] = (),
) -> tuple[str, dict[str, Any]]:
    """Одна строка stream-json: изображения по порядку, затем ТОЧНЫЙ текст.

    Перед каждым изображением стоит его порядковый номер («Image N:») — тот же
    номер, которым на изображение ссылается текст. Это принятый у провайдера
    способ нумеровать несколько изображений; нового содержания он не несёт.

    Строка кодируется в чистый ASCII: читатель строк в CLI режет ввод и по
    U+2028/U+2029, а такие символы в распознанном тексте встречаются.
    Собранная строка тут же разбирается обратно и сверяется с исходным
    текстом и байтами файлов — расхождение означает, что ничего не уйдёт.
    """
    if not isinstance(prompt, str) or not prompt:
        raise GatewayError("пустой текст запроса к claude")
    content: list[dict[str, Any]] = []
    image_sha256: list[str] = []
    image_bytes: list[int] = []
    for index, image in enumerate(images, start=1):
        data = Path(image).read_bytes()
        media_type = image_media_type(data)
        if media_type is None:
            raise GatewayError(f"изображение {index}: неизвестный формат файла {Path(image).name}")
        image_sha256.append(hashlib.sha256(data).hexdigest())
        image_bytes.append(len(data))
        content.append({"type": "text", "text": f"Image {index}:"})
        content.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": media_type,
                "data": base64.b64encode(data).decode("ascii"),
            },
        })
    content.append({"type": "text", "text": prompt})
    line = json.dumps(
        {"type": "user", "message": {"role": "user", "content": content}},
        ensure_ascii=True,
    )
    echoed = json.loads(line)["message"]["content"]
    sent_images = [block for block in echoed if block["type"] == "image"]
    if echoed[-1].get("text") != prompt or len(sent_images) != len(image_sha256) or any(
        hashlib.sha256(base64.b64decode(block["source"]["data"])).hexdigest() != digest
        for block, digest in zip(sent_images, image_sha256)
    ):
        raise GatewayError("сообщение для claude отличается от исходного текста или изображений")
    return line + "\n", {
        "text_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
        "text_chars": len(prompt),
        "images": len(image_sha256),
        "image_sha256": image_sha256,
        "image_bytes": image_bytes,
        "image_ordinal_labels": "Image {n}:",
        "content_order": "images_then_text",
        "stdin_bytes": len(line) + 1,
    }


def claude_stream_events(stdout: str) -> list[dict[str, Any]]:
    """Поток `--output-format stream-json`: по объекту JSON на строку."""
    events: list[dict[str, Any]] = []
    for raw in (stdout or "").splitlines():
        raw = raw.strip()
        if not raw.startswith("{"):
            continue
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            events.append(value)
    return events


def _claude_wire(events: Sequence[dict[str, Any]], *, requested_model: str) -> dict[str, Any]:
    """Что CLI сам сообщил о вызове: версия, модель каждого ответа, расход."""
    init = next((e for e in events if e.get("type") == "system" and e.get("subtype") == "init"), {})
    result = next((e for e in reversed(events) if e.get("type") == "result"), {})
    assistant_models: list[str] = []
    content_types: dict[str, int] = {}
    synthetic_text: list[str] = []
    for event in events:
        if event.get("type") != "assistant":
            continue
        message = event.get("message") or {}
        model = str(message.get("model") or "")
        blocks = message.get("content") if isinstance(message.get("content"), list) else []
        if model == _SYNTHETIC_MODEL:
            synthetic_text += [str(b.get("text") or "") for b in blocks if isinstance(b, dict)]
            continue
        assistant_models.append(model)
        for block in blocks:
            if isinstance(block, dict):
                kind = str(block.get("type") or "unknown")
                content_types[kind] = content_types.get(kind, 0) + 1
    model_usage = result.get("modelUsage") if isinstance(result.get("modelUsage"), dict) else {}
    own = {name: row for name, row in model_usage.items() if same_claude_model(requested_model, name)}
    return {
        "cli_version": init.get("claude_code_version"),
        "init_model": init.get("model"),
        "tools": list(init.get("tools") or []),
        "mcp_servers": [s.get("name") for s in init.get("mcp_servers") or [] if isinstance(s, dict)],
        "api_key_source": init.get("apiKeySource"),
        "permission_mode": init.get("permissionMode"),
        "assistant_models": sorted(set(assistant_models)),
        "assistant_messages": len(assistant_models),
        "assistant_content_types": content_types,
        "synthetic_messages": [text[:300] for text in synthetic_text if text][:5],
        "result_subtype": result.get("subtype"),
        "is_error": bool(result.get("is_error")),
        "stop_reason": result.get("stop_reason"),
        "num_turns": result.get("num_turns"),
        "duration_api_ms": result.get("duration_api_ms"),
        "total_cost_usd": result.get("total_cost_usd"),
        "usage_raw": dict(result.get("usage") or {}),
        "model_usage": model_usage,
        "context_window": next((row.get("contextWindow") for row in own.values()), None),
        "max_output_tokens": next((row.get("maxOutputTokens") for row in own.values()), None),
        "auxiliary_models": sorted(name for name in model_usage if name not in own),
    }


def call_claude_multimodal(
    prompt: str,
    *,
    model: str,
    schema: dict,
    reasoning_level: str,
    images: Sequence[str | os.PathLike[str]] = (),
    timeout_s: int | None = None,
    system_prompt: str | None = None,
    max_output_tokens: int = CLAUDE_MAX_OUTPUT_TOKENS,
    cancel: CancelToken | None = None,
    run_id: str = "",
) -> tuple[CallResult, dict[str, Any]]:
    """Один изолированный вызов Claude с текстом и изображениями. Без повторов.

    Возвращает результат и «провод» — то, что CLI сам сообщил о вызове.
    Ответ принимается, только если его дала запрошенная модель: у CLI есть
    собственный переход на другую модель после отказа (`refusal_fallback`),
    и снаружи он ничем, кроме поля `model` в ответе, не виден.
    """
    if reasoning_level not in CLAUDE_EFFORT_LEVELS:
        raise GatewayError(
            f"claude CLI не знает уровень усилия {reasoning_level!r}: он молча взял бы "
            f"уровень по умолчанию; допустимы {', '.join(CLAUDE_EFFORT_LEVELS)}"
        )
    if not isinstance(schema, dict) or not schema:
        raise GatewayError("вызов claude с изображениями требует JSON-схему ответа")
    binary = _resolve_claude_binary()
    timeout_s = timeout_s or settings.call_timeout_seconds()
    run_id = run_id or uuid.uuid4().hex
    system_prompt = system_prompt or CLAUDE_MULTIMODAL_SYSTEM_PROMPT
    stdin_text, sent = build_claude_user_message(prompt, images)
    wire: dict[str, Any] = {
        "binary": binary,
        "sent": sent,
        "system_prompt_sha256": hashlib.sha256(system_prompt.encode("utf-8")).hexdigest(),
        "max_output_tokens_requested": int(max_output_tokens),
        "fallback_model_flag": False,
    }
    if cancel is not None and cancel.cancelled:
        return CallResult(
            settings.CLAUDE_SESSION, model, reasoning_level, False,
            error="отменено", error_kind="CANCELLED",
        ), wire
    workdir = Path(tempfile.mkdtemp(prefix="sc_ai_claude_mm_"))
    try:
        command = [
            binary, "-p",
            "--model", model,
            "--input-format", "stream-json",
            "--output-format", "stream-json",
            "--verbose",                # CLI требует его для вывода stream-json
            "--tools", "",              # инструментов физически нет
            "--setting-sources", "",    # без ~/.claude/settings.json и хуков
            "--strict-mcp-config",      # без внешних MCP-серверов
            "--system-prompt", system_prompt,
            "--no-session-persistence",
            "--disable-slash-commands",
            "--effort", reasoning_level,
            "--json-schema", json.dumps(schema, ensure_ascii=False),
        ]
        env = _clean_env(run_id)
        env.update({
            # Сжатие контекста переписало бы доказательства пересказом.
            "DISABLE_AUTO_COMPACT": "1",
            # Вызов модели не должен обновлять CLI на этой машине.
            "DISABLE_AUTOUPDATER": "1",
            "CLAUDE_CODE_MAX_OUTPUT_TOKENS": str(int(max_output_tokens)),
        })
        started = time.perf_counter()
        code, stdout, stderr, failure = _run_process(
            command, cwd=str(workdir), env=env, timeout_s=timeout_s,
            stdin_text=stdin_text, cancel=cancel, run_id=run_id,
        )
        duration_ms = int((time.perf_counter() - started) * 1000)

        def failed(kind: str, text: str, usage: dict | None = None, session_id: str | None = None) -> CallResult:
            return CallResult(
                settings.CLAUDE_SESSION, model, reasoning_level, False,
                error=(text or "").strip()[-500:] or kind, error_kind=kind,
                duration_ms=duration_ms, exit_code=code, session_id=session_id,
                usage=usage or {}, raw_excerpt=f"{stdout[-1500:]}\n{stderr[-500:]}",
            )

        if failure:
            return failed(failure, "превышен таймаут" if failure == "TIMEOUT" else "отменено"), wire
        events = claude_stream_events(stdout)
        wire.update(_claude_wire(events, requested_model=model))
        wire["stderr_excerpt"] = (stderr or "").strip()[-500:]
        result = next((e for e in reversed(events) if e.get("type") == "result"), None)
        usage = dict((result or {}).get("usage") or {})
        if usage:
            usage["total_input_tokens"] = sum(
                usage.get(key) or 0 for key in (
                    "input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens",
                )
            )
        session_id = (result or {}).get("session_id")
        if "--effort" in (stderr or "") and "ignoring" in (stderr or "").lower():
            return failed("EFFORT_IGNORED", stderr, usage, session_id), wire
        if result is None:
            combined = f"{stdout}\n{stderr}"
            return failed(classify_failure(combined), combined, usage, session_id), wire
        if result.get("is_error") or result.get("subtype") != "success":
            text = str(result.get("result") or "; ".join(map(str, result.get("errors") or []))
                       or result.get("subtype") or "")
            kind = classify_failure(text)
            if kind == "UNKNOWN":
                # `is_error` бывает и при subtype=success (исчерпанный лимит подписки):
                # «SUCCESS» как код отказа читался бы как успех.
                subtype = str(result.get("subtype") or "")
                kind = subtype.upper() if subtype and subtype != "success" else "PROVIDER_ERROR"
            return failed(kind, text, usage, session_id), wire
        foreign = [name for name in wire["assistant_models"] if not same_claude_model(model, name)]
        if foreign or not wire["assistant_models"]:
            return failed(
                "MODEL_MISMATCH",
                f"ответ дала не запрошенная модель {model}: {foreign or 'ответов модели нет'}",
                usage, session_id,
            ), wire
        if not any(same_claude_model(model, name) for name in wire["model_usage"]):
            return failed(
                "MODEL_MISMATCH",
                f"в расходе вызова нет запрошенной модели {model}: {sorted(wire['model_usage'])}",
                usage, session_id,
            ), wire
        unexpected_tools = [name for name in wire["tools"] if name != "StructuredOutput"]
        if unexpected_tools or wire["mcp_servers"]:
            return failed(
                "ISOLATION_BREACH",
                f"у сессии оказались инструменты {unexpected_tools} / MCP {wire['mcp_servers']}",
                usage, session_id,
            ), wire
        payload = result.get("structured_output")
        if not isinstance(payload, dict):
            return failed("NO_STRUCTURED_OUTPUT", "CLI не вернул structured_output", usage, session_id), wire
        return CallResult(
            settings.CLAUDE_SESSION, model, reasoning_level, True,
            parsed=payload, duration_ms=duration_ms, exit_code=code,
            session_id=session_id, usage=usage, raw_excerpt=stdout[-2000:],
        ), wire
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
    require_json_events: bool = False,
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
        version_text = _cli_probe([codex_binary, "--version"]).strip()
        report["checks"]["codex_version"] = version_text.splitlines()[0] if (
            version_text
        ) else "UNKNOWN"
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
            if require_json_events:
                required_flags["json_events"] = "--json"
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
            version_text = _cli_probe([claude_binary, "--version"]).strip()
            report["checks"]["claude_version"] = (
                version_text.splitlines()[0] if version_text else "UNKNOWN"
            )
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


#: Ключи CLI, без которых вызов с изображениями не собрать. Проверяются по
#: собственной справке CLI, без обращения к провайдеру.
CLAUDE_MULTIMODAL_FLAGS = (
    ("structured_output", "--json-schema"),
    ("tools_switch", "--tools"),
    ("setting_sources", "--setting-sources"),
    ("strict_mcp", "--strict-mcp-config"),
    ("system_prompt", "--system-prompt"),
    ("input_format", "--input-format"),
    ("output_format", "--output-format"),
    ("effort", "--effort"),
    ("no_session_persistence", "--no-session-persistence"),
    ("verbose", "--verbose"),
)


def validate_claude_runtime(*, reasoning_level: str | None = None) -> dict[str, Any]:
    """Готовность `claude -p` к вызову с изображениями. Ноль обращений к модели."""
    report: dict[str, Any] = {"ok": True, "problems": [], "binaries": {}, "checks": {}}

    def fail(message: str) -> None:
        report["ok"] = False
        report["problems"].append(message)

    try:
        binary = _resolve_claude_binary()
    except GatewayError as exc:
        fail(str(exc))
        return report
    report["binaries"]["CLAUDE_SESSION"] = binary
    version_text = _cli_probe([binary, "--version"]).strip()
    report["checks"]["claude_version"] = version_text.splitlines()[0] if version_text else "UNKNOWN"
    if not version_text:
        fail("claude CLI не отвечает на `--version`")
    help_text = _cli_probe([binary, "--help"])
    report["checks"]["claude_help_readable"] = bool(help_text.strip())
    if not help_text.strip():
        fail("claude CLI не отвечает на `--help`")
        return report
    for name, flag in CLAUDE_MULTIMODAL_FLAGS:
        present = flag in help_text
        report["checks"][f"claude_{name}"] = present
        if not present:
            fail(f"claude CLI не поддерживает {flag} ({name})")
    stream_json = "stream-json" in help_text
    report["checks"]["claude_stream_json"] = stream_json
    if not stream_json:
        fail("claude CLI не знает формат stream-json: изображения передать нечем")
    if reasoning_level is not None:
        known = reasoning_level in CLAUDE_EFFORT_LEVELS and reasoning_level in help_text
        report["checks"]["claude_effort_level"] = known
        if not known:
            fail(f"claude CLI не знает уровень усилия {reasoning_level!r}")
    leaked = sorted(name for name in _clean_env("validate") if _SECRET_NAME_RE.search(name))
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
    "call_claude_multimodal",
    "call_codex",
    "classify_failure",
    "extract_json",
    "find_orphaned_processes",
    "kill_live_processes",
    "live_process_count",
    "reap_orphaned_processes",
    "validate_claude_runtime",
    "validate_runtime",
]

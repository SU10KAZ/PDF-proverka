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

#: Переменные окружения, которым нечего делать в сессии модели.
_STRIP_ENV_PREFIXES = (
    "ANTHROPIC_", "OPENAI_", "OPENROUTER_", "AWS_", "GOOGLE_", "GEMINI_",
    "AZURE_", "HF_", "HUGGINGFACE_",
)
_STRIP_ENV_EXACT = (
    "CLAUDE_CODE_SSE_PORT", "CLAUDECODE", "CLAUDE_CODE_ENTRYPOINT",
    "CLAUDE_CODE_SESSION_ID",
)

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

def _clean_env(run_id: str) -> dict[str, str]:
    env = {
        key: value for key, value in os.environ.items()
        if not key.startswith(_STRIP_ENV_PREFIXES) and key not in _STRIP_ENV_EXACT
    }
    env.setdefault("LANG", "ru_RU.UTF-8")
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
    _REGISTRY.add(process)
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
    """Живые процессы прогона: нужны, чтобы отмена не оставила сирот."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._items: set[subprocess.Popen] = set()

    def add(self, process: subprocess.Popen) -> None:
        with self._lock:
            self._items.add(process)

    def discard(self, process: subprocess.Popen) -> None:
        with self._lock:
            self._items.discard(process)

    def kill_all(self) -> int:
        with self._lock:
            items = list(self._items)
        for process in items:
            _kill_process_group(process)
        return len(items)

    def size(self) -> int:
        with self._lock:
            return len(self._items)


_REGISTRY = _ProcessRegistry()


def kill_live_processes() -> int:
    """Аварийная уборка: убить все процессы, порождённые шлюзом."""
    return _REGISTRY.kill_all()


def live_process_count() -> int:
    return _REGISTRY.size()


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
            "-s", "read-only",
            "--skip-git-repo-check",
            "--ephemeral",
            "-C", str(workdir),
            "-o", str(out_file),
        ]
        if reasoning_level:
            command += ["-c", f"model_reasoning_effort={reasoning_level}"]
        if schema is not None:
            schema_file = workdir / "schema.json"
            schema_file.write_text(
                json.dumps(schema, ensure_ascii=False), encoding="utf-8"
            )
            command += ["--output-schema", str(schema_file)]
        # `-i/--image` объявлен как <FILE>..., то есть переменной длины:
        # позиционный промпт после него будет проглочен как ещё один файл.
        # Поэтому при картинках промпт всегда уходит через stdin, а вместо
        # него ставится «-».
        stdin_text: str | None = None
        if image_paths:
            command += ["-i", *image_paths, "-"]
            stdin_text = prompt
        else:
            command.append(prompt)

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
        command = [
            binary, "-p", prompt,
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
                timeout_s=timeout_s, stdin_text="", cancel=cancel,
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


def validate_runtime() -> dict[str, Any]:
    """Проверить, что заявленные в настройках транспорты вообще существуют.

    Модель не хардкодится: если CLI на этой машине её не знает, честнее узнать
    об этом при старте этапа, чем на четырёхсотом элементе.
    """
    report: dict[str, Any] = {"ok": True, "problems": [], "binaries": {}}
    try:
        report["binaries"]["CODEX_SESSION"] = _resolve_codex_binary()
    except GatewayError as exc:
        report["ok"] = False
        report["problems"].append(str(exc))
    if settings.deep():
        try:
            report["binaries"]["CLAUDE_SESSION"] = _resolve_claude_binary()
        except GatewayError as exc:
            report["ok"] = False
            report["problems"].append(str(exc))
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
    "kill_live_processes",
    "live_process_count",
    "validate_runtime",
]

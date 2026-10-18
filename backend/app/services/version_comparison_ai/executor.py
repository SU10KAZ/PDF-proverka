"""Исполнитель ИИ-описаний — заменяемый адаптер (план §7).

* ``CodexExecDescriber`` — серверный ``codex exec`` на авторизации пользователя
  службы (``~/.codex``), модель «Астра» ``gpt-6-astra``. Изоляция — та же, что у
  шлюза «Сравнения стадий»: пустой временный каталог, ``-s read-only``,
  ``--ephemeral``, без пользовательского конфига и правил, 22 возможности
  выключены, окружение по белому списку, отмена убивает группу процессов.
  Аргументы собираются здесь, а не ``gateway.call_codex``: картинки идут формой
  ``--image=<путь>``, потому что вариадический ``-i p1 p2 -`` может принять
  терминатор ``-`` за ещё один файл (``audit_worker/providers/codex_adapter.py``).
* ``FakeDescriber`` — подмена для тестов: ответы по сценарию, ни одного процесса.

Ограничитель Астры аудитов (``llm/astra_parallelism.limit_astra``) этот путь не
использует: у модуля свой семафор ``VC_AI_CONCURRENCY`` (план §13). Фактические
ограничения аккаунта (квота подписки, авторизация) распознаются по ответу CLI
и не повторяются.
"""
from __future__ import annotations

import json
import re
import shutil
import tempfile
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Protocol

from backend.app.services.stage_comparison.ai import gateway

from . import settings

CancelToken = gateway.CancelToken

QUOTA_MARKERS = ("usage limit", "quota", "insufficient_quota", "rate limit reached", "you've hit your",
                 "limit has been reached", "billing")
AUTH_MARKERS = ("unauthorized", "not authenticated", "not logged in", "please log in", "login required",
                "invalid api key")
# Код ответа 401 — только отдельным числом: внутри счётчиков токенов («24013»), идентификаторов и номеров листов
# «401» встречается постоянно (18.10.2026 любой временный сбой из 1 000 вызовов Луны выглядел как «нет авторизации»).
_STATUS_401 = re.compile(r"(?<![\w.])401(?![\w.])")
ERROR_TEXT = {
    "QUOTA": "исчерпана квота подписки Codex — повторите после сброса лимита",
    "AUTH": "Codex не авторизован у пользователя службы",
    "TIMEOUT": "превышено время ожидания ответа",
    "CANCELLED": "отменено",
    "TRANSIENT": "временный отказ провайдера",
    "INVALID_JSON": "ответ не является JSON",
    "CONTEXT": "пакет не поместился в контекст модели",
    "RUNTIME": "среда исполнителя не готова",
    "IMAGE_UNAVAILABLE": "изображение части недоступно — часть не отправлена",
    "UNKNOWN": "неизвестная ошибка исполнителя",
}
NO_RETRY = ("QUOTA", "AUTH", "CANCELLED", "RUNTIME", "CONTEXT")


@dataclass
class DescribeTask:
    prompt: str
    schema: dict
    image_paths: list[str]
    timeout_s: int
    marker: str
    packet_id: str = ""
    chunk_id: str = ""
    raw_path: str = ""     # куда побайтно сохранить файл ответа исполнителя ДО разбора


@dataclass
class DescribeResult:
    ok: bool
    parsed: dict | None = None
    error_kind: str = ""
    error: str = ""
    duration_ms: int = 0
    attempts: int = 1
    usage: dict = field(default_factory=dict)
    raw_excerpt: str = ""

    def as_dict(self) -> dict:
        d = asdict(self)
        d["raw_excerpt"] = self.raw_excerpt[-2000:]
        return d


class ChangeDescriber(Protocol):
    executor_id: str

    def describe_runtime(self) -> dict[str, Any]: ...

    def validate_runtime(self) -> dict[str, Any]: ...

    def run(self, task: DescribeTask, cancel: CancelToken) -> DescribeResult: ...


def classify(text: str) -> str:
    low = (text or "").lower()
    if any(m in low for m in QUOTA_MARKERS):
        return "QUOTA"
    if any(m in low for m in AUTH_MARKERS) or _STATUS_401.search(low):
        return "AUTH"
    kind = gateway.classify_failure(text)
    return {"CONTEXT_WINDOW_EXCEEDED": "CONTEXT", "CONTEXT_COMPACTED": "CONTEXT"}.get(kind, kind)


def failure_text(stdout: str, stderr: str) -> str:
    """Текст для распознавания отказа: stderr, строки вывода не в формате JSON и события ошибок ``--json``.
    Ответы модели, идентификаторы и счётчики токенов не входят: слова «quota», «401» в них — не отказ."""
    out = [stderr or ""]
    for line in (stdout or "").splitlines():
        s = line.strip()
        if not s:
            continue
        try:
            ev = json.loads(s)
        except ValueError:
            out.append(s)
            continue
        if not isinstance(ev, dict):
            continue
        item = ev.get("item") if isinstance(ev.get("item"), dict) else {}
        if ev.get("type") in ("error", "turn.failed") or item.get("type") == "error":
            out.append(json.dumps(ev, ensure_ascii=False))
    return "\n".join(out)


class CodexExecDescriber:
    executor_id = "codex_exec"

    def __init__(self, model: str | None = None, effort: str | None = None, binary: str | None = None,
                 service_tier: str | None = None):
        self.model = model or settings.model()
        self.effort = effort or settings.effort()
        # Тариф скорости Codex (``priority`` = «Fast»): только если задан явно; по умолчанию — обычный.
        self.service_tier = service_tier or None
        # Явный путь CLI — только у способа «Изменения в листе» (settings.sc_codex_bin); общий выбор шлюза
        # для остальных путей не меняется.
        self.binary = binary or None
        self._cli_version: str | None = None

    def resolve_binary(self) -> str:
        if self.binary:
            if not Path(self.binary).is_file():
                raise gateway.GatewayError(f"заданный CLI исполнителя не найден: {self.binary}")
            return self.binary
        return gateway._resolve_codex_binary()

    def cli_version(self) -> str:
        if self._cli_version is None:
            try:
                text = gateway._cli_probe([self.resolve_binary(), "--version"]).strip()
                self._cli_version = text.splitlines()[0] if text else "UNKNOWN"
            except gateway.GatewayError:
                self._cli_version = "UNKNOWN"
        return self._cli_version

    def describe_runtime(self) -> dict[str, Any]:
        rt = {"executor": self.executor_id, "model": self.model, "effort": self.effort,
              "account": "подписка Codex пользователя службы (~/.codex)", "transport": "codex exec"}
        if self.binary:
            rt.update(cli=self.binary, cli_version=self.cli_version(), service_tier=self.service_tier)
        return rt

    def validate_runtime(self) -> dict[str, Any]:
        """Офлайн-проверка CLI: флаги, изоляция, окружение без секретов. Ни одного вызова модели."""
        if not self.binary:
            return gateway.validate_runtime(require_vision=True, deep=False, require_json_events=True)
        return validate_cli(self.binary)

    def command(self, binary: str, workdir: Path, out_file: Path, schema_file: Path,
                image_paths: list[str]) -> list[str]:
        cmd = [binary, "exec", f"--model={self.model}", "--sandbox=read-only", "--skip-git-repo-check",
               "--ephemeral", "--ignore-user-config", "--ignore-rules", "--json"]
        for feature in gateway.CODEX_DISABLED_FEATURES:
            cmd += ["--disable", feature]
        if self.effort:
            cmd += ["-c", f"model_reasoning_effort={self.effort}"]
        if self.service_tier:
            cmd += ["-c", f'service_tier="{self.service_tier}"']
        cmd += [f"--output-schema={schema_file}"]
        cmd += [f"--image={p}" for p in image_paths]
        # Невариадические ключи между картинками и «-»: терминатор не может стать файлом.
        cmd += [f"--cd={workdir}", f"--output-last-message={out_file}", "-"]
        return cmd

    def run(self, task: DescribeTask, cancel: CancelToken) -> DescribeResult:
        import json
        binary = self.resolve_binary()
        workdir = Path(tempfile.mkdtemp(prefix="vc_ai_codex_"))
        try:
            # Копии изображений с нейтральными именами: в процесс не уходят пути хранилища.
            local = []
            for k, src in enumerate(task.image_paths, 1):
                dst = workdir / f"img{k:03d}{Path(src).suffix}"
                shutil.copyfile(src, dst)
                local.append(str(dst))
            schema_file = workdir / "schema.json"
            schema_file.write_text(json.dumps(task.schema, ensure_ascii=False), encoding="utf-8")
            out_file = workdir / "last_message.json"
            cmd = self.command(binary, workdir, out_file, schema_file, local)
            started = time.perf_counter()
            code, stdout, stderr, failure = gateway._run_process(
                cmd, cwd=str(workdir), env=gateway._clean_env(task.marker), timeout_s=task.timeout_s,
                stdin_text=task.prompt, cancel=cancel, run_id=task.marker)
            ms = int((time.perf_counter() - started) * 1000)
            usage = gateway.exec_event_usage(stdout)
            if failure:
                return DescribeResult(False, error_kind=failure, error=ERROR_TEXT.get(failure, failure),
                                      duration_ms=ms, usage=usage, raw_excerpt=f"{stdout}\n{stderr}")
            raw = out_file.read_bytes() if out_file.exists() else b""
            if task.raw_path:
                Path(task.raw_path).parent.mkdir(parents=True, exist_ok=True)
                Path(task.raw_path).write_bytes(raw)
                Path(task.raw_path).with_suffix(".stdout.jsonl").write_text(stdout, encoding="utf-8")
            text = raw.decode("utf-8", errors="replace")
            payload = gateway.extract_json(text) if text else None
            if payload is not None and code == 0:
                return DescribeResult(True, parsed=payload, duration_ms=ms, usage=usage, raw_excerpt=text)
            combined = f"{stdout}\n{stderr}"
            kind = classify(failure_text(stdout, stderr)) if code not in (0, None) or not text else "INVALID_JSON"
            if gateway.exec_context_compacted(stdout):
                kind = "CONTEXT"
            return DescribeResult(False, error_kind=kind, error=ERROR_TEXT.get(kind, kind), duration_ms=ms,
                                  usage=usage, raw_excerpt=combined[-2000:])
        finally:
            shutil.rmtree(workdir, ignore_errors=True)


def validate_cli(binary: str) -> dict[str, Any]:
    """То же, что ``gateway.validate_runtime`` для codex, но для заданного файла CLI (без вызова модели)."""
    report: dict[str, Any] = {"ok": True, "problems": [], "binaries": {"CODEX_SESSION": binary}, "checks": {}}

    def fail(problem: str) -> None:
        report["ok"] = False
        report["problems"].append(problem)
    if not Path(binary).is_file():
        fail(f"заданный CLI исполнителя не найден: {binary}")
        return report
    version = gateway._cli_probe([binary, "--version"]).strip()
    report["checks"]["codex_version"] = version.splitlines()[0] if version else "UNKNOWN"
    help_text = gateway._cli_probe([binary, "exec", "--help"])
    if not help_text.strip():
        fail("codex CLI не отвечает на `exec --help`")
        return report
    for name, flag in {"structured_output": "--output-schema", "sandbox": "--sandbox",
                       "ignore_user_config": "--ignore-user-config", "feature_switch": "--disable",
                       "reasoning_level": "--config", "vision": "--image", "json_events": "--json"}.items():
        report["checks"][f"codex_{name}"] = flag in help_text
        if flag not in help_text:
            fail(f"codex CLI не поддерживает {flag} ({name})")
    states = gateway._codex_feature_states(binary)
    observed = {}
    for feature in gateway.CODEX_REQUIRED_OFF:
        observed[feature] = states.get(feature) or "UNKNOWN"
        if states.get(feature) != "false":
            fail(f"возможность {feature!r} не выключена (состояние {observed[feature]})")
    for feature in gateway.CODEX_MUST_BE_KNOWN:
        observed[feature] = states.get(feature) or "UNKNOWN"
        if feature not in states:
            fail(f"codex CLI больше не знает {feature!r}: изоляцию надо перепроверить")
    report["checks"]["codex_isolation_features"] = observed
    env_names = sorted(gateway._clean_env("validate"))
    leaked = [n for n in env_names if gateway._SECRET_NAME_RE.search(n)]
    report["checks"]["environment_names"] = env_names
    report["checks"]["environment_leaked_secrets"] = leaked
    if leaked:
        fail(f"в окружение сессии попали секреты: {', '.join(leaked)}")
    return report


class FakeDescriber:
    """Подменный исполнитель: ``responder(task) -> dict | DescribeResult``."""

    executor_id = "fake"

    def __init__(self, responder: Callable[[DescribeTask], Any] | None = None, model: str = "fake-model"):
        self.responder = responder or (lambda task: {
            "packet_id": task.packet_id, "chunk_id": task.chunk_id, "items": [], "dispositions": [],
            "unreadable_images": [], "context_requests": []})
        self.model = model
        self.calls: list[DescribeTask] = []
        self._lock = threading.Lock()

    def describe_runtime(self) -> dict[str, Any]:
        return {"executor": self.executor_id, "model": self.model, "effort": None, "account": "нет (тест)",
                "transport": "в процессе"}

    def validate_runtime(self) -> dict[str, Any]:
        return {"ok": True, "problems": [], "checks": {}}

    def run(self, task: DescribeTask, cancel: CancelToken) -> DescribeResult:
        with self._lock:
            self.calls.append(task)
        if cancel.cancelled:
            return DescribeResult(False, error_kind="CANCELLED", error=ERROR_TEXT["CANCELLED"])
        out = self.responder(task)
        if isinstance(out, DescribeResult):
            return out
        if out is None:
            return DescribeResult(False, error_kind="INVALID_JSON", error=ERROR_TEXT["INVALID_JSON"])
        return DescribeResult(True, parsed=out, duration_ms=1, usage={"input_tokens": 0, "output_tokens": 0})


_override: ChangeDescriber | None = None


def set_override(describer: ChangeDescriber | None) -> None:
    """Подменить исполнитель (тесты). ``None`` — вернуть настройку из окружения."""
    global _override
    _override = describer


def get(method: str = "records") -> ChangeDescriber:
    if _override is not None:
        return _override
    kind = settings.executor()
    if kind != "codex_exec":
        raise ValueError(f"неизвестный исполнитель ИИ-описаний: {kind!r}")
    if method == "sheet_match":
        return CodexExecDescriber(model=settings.sm_model(), effort=settings.sm_effort(),
                                  binary=settings.sm_codex_bin() or None)
    if method == "sheet_changes":
        # Модель и усилие способа заданы явно; подмены другой моделью при отказе нет.
        return CodexExecDescriber(model=settings.sc_model(), effort=settings.sc_effort(),
                                  binary=settings.sc_codex_bin() or None)
    return CodexExecDescriber()


def new_marker(prefix: str) -> str:
    return f"vcai-{prefix}-{uuid.uuid4().hex[:8]}"

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
                "invalid api key", "401")
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
    if any(m in low for m in AUTH_MARKERS):
        return "AUTH"
    kind = gateway.classify_failure(text)
    return {"CONTEXT_WINDOW_EXCEEDED": "CONTEXT", "CONTEXT_COMPACTED": "CONTEXT"}.get(kind, kind)


class CodexExecDescriber:
    executor_id = "codex_exec"

    def __init__(self, model: str | None = None, effort: str | None = None):
        self.model = model or settings.model()
        self.effort = effort or settings.effort()

    def describe_runtime(self) -> dict[str, Any]:
        return {"executor": self.executor_id, "model": self.model, "effort": self.effort,
                "account": "подписка Codex пользователя службы (~/.codex)", "transport": "codex exec"}

    def validate_runtime(self) -> dict[str, Any]:
        """Офлайн-проверка CLI: флаги, изоляция, окружение без секретов. Ни одного вызова модели."""
        return gateway.validate_runtime(require_vision=True, deep=False, require_json_events=True)

    def command(self, binary: str, workdir: Path, out_file: Path, schema_file: Path,
                image_paths: list[str]) -> list[str]:
        cmd = [binary, "exec", f"--model={self.model}", "--sandbox=read-only", "--skip-git-repo-check",
               "--ephemeral", "--ignore-user-config", "--ignore-rules", "--json"]
        for feature in gateway.CODEX_DISABLED_FEATURES:
            cmd += ["--disable", feature]
        if self.effort:
            cmd += ["-c", f"model_reasoning_effort={self.effort}"]
        cmd += [f"--output-schema={schema_file}"]
        cmd += [f"--image={p}" for p in image_paths]
        # Невариадические ключи между картинками и «-»: терминатор не может стать файлом.
        cmd += [f"--cd={workdir}", f"--output-last-message={out_file}", "-"]
        return cmd

    def run(self, task: DescribeTask, cancel: CancelToken) -> DescribeResult:
        import json
        binary = gateway._resolve_codex_binary()
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
            kind = classify(combined) if code not in (0, None) or not text else "INVALID_JSON"
            if gateway.exec_context_compacted(stdout):
                kind = "CONTEXT"
            return DescribeResult(False, error_kind=kind, error=ERROR_TEXT.get(kind, kind), duration_ms=ms,
                                  usage=usage, raw_excerpt=combined[-2000:])
        finally:
            shutil.rmtree(workdir, ignore_errors=True)


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


def get() -> ChangeDescriber:
    if _override is not None:
        return _override
    kind = settings.executor()
    if kind == "codex_exec":
        return CodexExecDescriber()
    raise ValueError(f"неизвестный исполнитель ИИ-описаний: {kind!r}")


def new_marker(prefix: str) -> str:
    return f"vcai-{prefix}-{uuid.uuid4().hex[:8]}"

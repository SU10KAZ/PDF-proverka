"""Codex exec transport for the classic audit pipeline.

This module is intentionally small: it mirrors the tuple contract used by
claude_runner._run_cli, but runs `codex exec` instead of `claude -p`.
Classic audit tasks are agentic: prompts instruct the model to read project
files and write JSON artifacts. Therefore the default Codex sandbox is
workspace-write, not read-only.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional, Sequence

from backend.app.core.config import ROOT_DIR, resolve_codex_model
from backend.app.models.usage import CLIResult, LLMResult
from backend.app.services.common.process_runner import run_command
from backend.app.services.llm.llm_runner import _try_parse_json_content

OnOutput = Optional[Callable[[str], Awaitable[None]]]

_CODEX_CLI_ENV = "AUDIT_CODEX_CLI_PATH"
_CODEX_CLI_ENV_LEGACY = "CODEX_CLI_PATH"
_CODEX_SANDBOX_ENV = "AUDIT_CODEX_SANDBOX"
_CODEX_JSON_SANDBOX_ENV = "AUDIT_CODEX_JSON_SANDBOX"
_DEFAULT_SANDBOX = "workspace-write"
_DEFAULT_JSON_SANDBOX = "read-only"
_ALLOWED_SANDBOXES = {"read-only", "workspace-write", "danger-full-access"}


def find_codex_cli() -> str | None:
    """Find an executable Codex CLI binary."""
    env_path = (os.environ.get(_CODEX_CLI_ENV) or os.environ.get(_CODEX_CLI_ENV_LEGACY) or "").strip()
    candidates: list[str | None] = []
    if env_path:
        candidates.append(env_path)
    candidates.extend([
        shutil.which("codex"),
        str(Path.home() / ".local" / "bin" / "codex"),
        str(Path.home() / ".npm-global" / "bin" / "codex"),
    ])

    for ext_root in (
        Path.home() / ".vscode-server" / "extensions",
        Path.home() / ".vscode" / "extensions",
    ):
        if ext_root.exists():
            ext_candidates = sorted(
                ext_root.glob("openai.chatgpt-*-linux-x64/bin/*/codex"),
                key=lambda p: p.stat().st_mtime if p.exists() else 0.0,
            )
            candidates.extend(str(p) for p in ext_candidates[::-1])

    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate).expanduser()
        try:
            resolved = path.resolve(strict=True)
        except (FileNotFoundError, OSError):
            continue
        if resolved.is_file() and os.access(str(resolved), os.X_OK):
            return str(resolved)
    return None


def _sandbox_mode() -> str:
    raw = (os.environ.get(_CODEX_SANDBOX_ENV) or _DEFAULT_SANDBOX).strip()
    return raw if raw in _ALLOWED_SANDBOXES else _DEFAULT_SANDBOX


def _json_sandbox_mode() -> str:
    raw = (os.environ.get(_CODEX_JSON_SANDBOX_ENV) or _DEFAULT_JSON_SANDBOX).strip()
    return raw if raw in _ALLOWED_SANDBOXES else _DEFAULT_JSON_SANDBOX


def _normalize_image_paths(image_paths: Sequence[str | Path] | None) -> list[Path]:
    if not image_paths:
        return []

    allowed_suffixes = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
    normalized: list[Path] = []
    seen: set[Path] = set()
    for raw_path in image_paths:
        try:
            path = Path(raw_path).expanduser().resolve(strict=True)
        except (OSError, RuntimeError):
            continue
        if not path.is_file() or path.suffix.lower() not in allowed_suffixes:
            continue
        if path in seen:
            continue
        seen.add(path)
        normalized.append(path)
    return normalized


def _build_prompt(
    task_text: str,
    *,
    stage: str,
    project_id: str,
    image_paths: Sequence[str | Path] | None = None,
) -> str:
    images = _normalize_image_paths(image_paths)
    image_section = ""
    if images:
        image_lines = "\n".join(f"- {path}" for path in images)
        image_section = (
            "\nAttached image files are available to this Codex exec run through "
            "`--image`. Use them only for this stage and cite their block/page "
            "labels from the task when they support an output item.\n"
            "<ATTACHED_IMAGES>\n"
            f"{image_lines}\n"
            "</ATTACHED_IMAGES>\n\n"
        )

    return (
        "You are running as OpenAI Codex exec inside the Audit Manager classic "
        "pipeline. You are replacing a Claude Code CLI agent for this single "
        "non-interactive stage.\n"
        f"Stage: {stage or 'unknown'}\n"
        f"Project: {project_id or 'unknown'}\n\n"
        "Follow the task exactly. When the task says Read or Write, interpret "
        "that as filesystem access. Read the requested files and create or "
        "overwrite only the output JSON artifacts explicitly requested by the "
        "task. Do not modify unrelated files. Do not print JSON instead of "
        "writing it when the task asks for an output file. Finish with one short "
        "status line.\n\n"
        f"{image_section}"
        "<PIPELINE_TASK>\n"
        f"{task_text or ''}\n"
        "</PIPELINE_TASK>\n"
    )


def _content_to_text(content: Any) -> str:
    """Convert OpenAI-style message content into text for Codex exec stdin."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        omitted_images = 0
        for item in content:
            if isinstance(item, str):
                parts.append(item)
                continue
            if not isinstance(item, dict):
                parts.append(str(item))
                continue
            if item.get("type") == "text":
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
            elif item.get("type") in {"image_url", "input_image"} or "image_url" in item:
                omitted_images += 1
        if omitted_images:
            parts.append(
                f"[{omitted_images} image attachment(s) omitted in Codex exec text mode; "
                "use the supplied MD/JSON context as source of truth.]"
            )
        return "\n\n".join(part for part in parts if part)
    if content is None:
        return ""
    return json.dumps(content, ensure_ascii=False) if isinstance(content, (dict, list)) else str(content)


def _build_json_prompt(
    messages: list[dict],
    *,
    stage: str,
    project_id: str,
    image_paths: Sequence[str | Path] | None = None,
) -> str:
    images = _normalize_image_paths(image_paths)
    parts = [
        "You are OpenAI Codex exec used as a JSON-only model inside the Audit Manager classic pipeline.",
        f"Stage: {stage or 'unknown'}",
        f"Project: {project_id or 'unknown'}",
        "",
        "All source data needed for this stage is included in the messages and attached images below. "
        "Inspect every attached image. Do not read files through tools, do not write files, "
        "do not call shell commands, do not use web search, "
        "and do not try to patch the workspace. Return exactly one valid JSON value for this stage. "
        "No Markdown fences, no prose, no status line.",
    ]
    if images:
        parts.extend(["", "<ATTACHED_IMAGES>", *(str(path) for path in images), "</ATTACHED_IMAGES>"])
    for idx, message in enumerate(messages, start=1):
        role = str(message.get("role") or "user").upper()
        parts.extend([
            "",
            f"<MESSAGE {idx} ROLE={role}>",
            _content_to_text(message.get("content")),
            f"</MESSAGE {idx}>",
        ])
    return "\n".join(parts).strip() + "\n"


def _extract_token_count(text: str) -> int:
    """Best-effort parse of `tokens used` emitted by Codex CLI."""
    import re

    match = re.search(r"tokens used\s*\n\s*([0-9][0-9\s\u00a0,._]*)", text or "", re.IGNORECASE)
    if not match:
        return 0
    digits = re.sub(r"\D", "", match.group(1))
    return int(digits or 0)


async def run_codex_exec(
    task_text: str,
    *,
    timeout: int,
    on_output: OnOutput = None,
    stage: str = "",
    project_id: str = "",
    model: str | None = None,
    image_paths: Sequence[str | Path] | None = None,
) -> tuple[int, str, CLIResult]:
    """Run Codex exec and return the classic `(exit_code, output, CLIResult)` tuple."""
    cli = find_codex_cli()
    resolved_model = resolve_codex_model(model)
    if not cli:
        msg = "codex_cli_not_found"
        return 127, msg, CLIResult(result_text=msg, is_error=True)

    fd, out_name = tempfile.mkstemp(prefix=f"codex_{stage or 'audit'}_", suffix=".md")
    os.close(fd)
    out_file = Path(out_name)
    images = _normalize_image_paths(image_paths)
    image_args: list[str] = []
    for image_path in images:
        image_args.extend(["--image", str(image_path)])
    prompt = _build_prompt(task_text, stage=stage, project_id=project_id, image_paths=images)
    cmd = [
        cli,
        "exec",
        "--ephemeral",
        "--ignore-user-config",
        "--ignore-rules",
        "--skip-git-repo-check",
        "--sandbox",
        _sandbox_mode(),
        "--model",
        resolved_model,
        *image_args,
        "-C",
        str(ROOT_DIR),
        "-o",
        str(out_file),
        "-",
    ]
    env_overrides = {k: None for k in os.environ if k.startswith("CLAUDE")}

    started = time.monotonic()
    try:
        exit_code, stdout, stderr = await run_command(
            cmd,
            input_text=prompt,
            timeout=timeout,
            on_output=on_output,
            env_overrides=env_overrides,
            cwd=str(ROOT_DIR),
            project_id=project_id,
        )
        duration_ms = int((time.monotonic() - started) * 1000)
        try:
            final_text = out_file.read_text(encoding="utf-8", errors="replace")
        except OSError:
            final_text = ""
        combined_parts = [part for part in (stdout, stderr, final_text) if part]
        combined = "\n".join(combined_parts)
        result = CLIResult(
            result_text=final_text or stdout or stderr or "",
            is_error=bool(exit_code != 0),
            cost_usd=0.0,
            duration_ms=duration_ms,
            duration_api_ms=duration_ms,
            num_turns=1,
        )
        return exit_code, combined, result
    finally:
        try:
            out_file.unlink()
        except OSError:
            pass


async def run_codex_json_messages(
    messages: list[dict],
    *,
    timeout: int,
    on_output: OnOutput = None,
    stage: str = "",
    project_id: str = "",
    model: str | None = None,
    image_paths: Sequence[str | Path] | None = None,
) -> LLMResult:
    """Run Codex exec as a JSON-only text model.

    Unlike ``run_codex_exec()``, this mode does not ask Codex to use filesystem
    tools. The backend supplies all context inline, parses the final answer, and
    writes the pipeline artifact itself.
    """
    cli = find_codex_cli()
    resolved_model = resolve_codex_model(model)
    if not cli:
        msg = "codex_cli_not_found"
        return LLMResult(text=msg, model=f"codex/{resolved_model}", is_error=True, error_message=msg)

    fd, out_name = tempfile.mkstemp(prefix=f"codex_{stage or 'json'}_", suffix=".json")
    os.close(fd)
    out_file = Path(out_name)
    images = _normalize_image_paths(image_paths)
    prompt = _build_json_prompt(
        messages,
        stage=stage,
        project_id=project_id,
        image_paths=images,
    )
    image_args: list[str] = []
    for image_path in images:
        image_args.extend(["--image", str(image_path)])
    cmd = [
        cli,
        "exec",
        "--ephemeral",
        "--ignore-user-config",
        "--ignore-rules",
        "--skip-git-repo-check",
        "--sandbox",
        _json_sandbox_mode(),
        "--model",
        resolved_model,
        *image_args,
        "-C",
        str(ROOT_DIR),
        "-o",
        str(out_file),
        "-",
    ]
    env_overrides = {k: None for k in os.environ if k.startswith("CLAUDE")}

    started = time.monotonic()
    try:
        exit_code, stdout, stderr = await run_command(
            cmd,
            input_text=prompt,
            timeout=timeout,
            on_output=on_output,
            env_overrides=env_overrides,
            cwd=str(ROOT_DIR),
            project_id=project_id,
        )
        duration_ms = int((time.monotonic() - started) * 1000)
        try:
            final_text = out_file.read_text(encoding="utf-8", errors="replace")
        except OSError:
            final_text = ""
        combined_parts = [part for part in (stdout, stderr, final_text) if part]
        combined = "\n".join(combined_parts)
        json_data = _try_parse_json_content(final_text)
        json_from_out_file = json_data is not None
        if json_data is None:
            json_data = _try_parse_json_content(stdout)
        # stderr НЕ парсим: при сбое codex печатает туда JSON-тело ошибки API
        # ({"error":{"message":"usage limit reached"}}), и жадный fallback-парсер
        # принимал его за ответ стадии → артефакт-ошибка уходил в пайплайн как успех.
        error = ""
        is_error = json_data is None
        if json_data is None:
            error = f"codex_exec_exit_{exit_code}; codex_json_not_found" if exit_code != 0 else "codex_json_not_found"
        elif exit_code != 0:
            if json_from_out_file:
                # -o файл записан самим codex как финальный ответ — ненулевой exit
                # после этого допускаем (пост-обработка), но фиксируем в error_message.
                error = f"codex_exec_exit_{exit_code}_ignored_after_valid_json"
            else:
                # JSON найден только в stdout при exit!=0 — не доверяем (может быть
                # телом/эхом ошибки), считаем провалом стадии.
                is_error = True
                json_data = None
                error = f"codex_exec_exit_{exit_code}; json_from_stdout_untrusted"
        return LLMResult(
            text=final_text or stdout or stderr or "",
            json_data=json_data,
            input_tokens=0,
            output_tokens=_extract_token_count(combined),
            cost_usd=0.0,
            duration_ms=duration_ms,
            model=f"codex/{resolved_model}",
            is_error=is_error,
            error_message=error,
            cost_source="subscription",
            finish_reason="stop" if not is_error else "error",
        )
    finally:
        try:
            out_file.unlink()
        except OSError:
            pass


__all__ = [
    "find_codex_cli",
    "run_codex_exec",
    "run_codex_json_messages",
]

"""Shared helpers for both runners.

A single `run_claude(...)` entry-point that wraps `claude -p` as subprocess.
Both the current-method runner and every multi-agent sub-agent go through
this helper, so behaviour, env scrubbing, and JSON parsing stay consistent.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from configs import config as cfg  # noqa: E402


@dataclass
class ClaudeResult:
    ok: bool
    raw_stdout: str
    raw_stderr: str
    parsed_json: dict[str, Any] | None
    findings_text: str | None
    duration_sec: float
    exit_code: int
    model: str


def _clean_env() -> dict[str, str]:
    """Strip CLAUDE_* / hook / settings overrides so nested `claude -p`
    does not inherit our session context."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("CLAUDE_")}
    env.pop("ANTHROPIC_API_KEY", None)
    env.pop("CLAUDE_PROJECT_DIR", None)
    env["CLAUDE_DISABLE_HOOKS"] = "1"
    return env


def _strip_codefences(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
    return text.strip()


def _try_parse_json(text: str) -> dict[str, Any] | None:
    if not text:
        return None
    cleaned = _strip_codefences(text)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass
    m = re.search(r"\{[\s\S]*\}", cleaned)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            return None
    return None


def run_claude(
    *,
    prompt: str,
    model: str,
    output_path: Path | None = None,
    timeout: int = cfg.DEFAULT_TIMEOUT_SEC,
    cwd: Path | None = None,
    label: str = "stage",
) -> ClaudeResult:
    """Run `claude -p` once, return parsed JSON + raw outputs.

    The prompt is sent on stdin. The model is selected explicitly. We use
    `--output-format json` so the wrapper gets structured output even if the
    model writes JSON inside ``result`` field.
    """
    cmd = [
        cfg.CLAUDE_CLI,
        "-p",
        "--model", model,
        "--allowedTools", cfg.ALLOWED_TOOLS,
        "--output-format", "json",
    ]
    env = _clean_env()
    work_cwd = cwd or cfg.TEMP_DIR
    work_cwd.mkdir(parents=True, exist_ok=True)

    started = time.time()
    try:
        proc = subprocess.run(
            cmd,
            input=prompt,
            env=env,
            cwd=str(work_cwd),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        exit_code = proc.returncode
        stdout = proc.stdout or ""
        stderr = proc.stderr or ""
    except subprocess.TimeoutExpired as e:
        return ClaudeResult(
            ok=False, raw_stdout=e.stdout or "", raw_stderr=f"TIMEOUT after {timeout}s",
            parsed_json=None, findings_text=None, duration_sec=timeout,
            exit_code=124, model=model,
        )

    duration = time.time() - started

    if cfg.LOG_RAW_OUTPUTS:
        log_dir = cfg.LOGS_DIR / label
        log_dir.mkdir(parents=True, exist_ok=True)
        ts = time.strftime("%Y%m%d_%H%M%S")
        (log_dir / f"{ts}.stdout.txt").write_text(stdout, encoding="utf-8")
        if stderr:
            (log_dir / f"{ts}.stderr.txt").write_text(stderr, encoding="utf-8")

    parsed: dict[str, Any] | None = None
    findings_text: str | None = None
    wrapper = _try_parse_json(stdout)
    if wrapper and isinstance(wrapper, dict):
        result_field = wrapper.get("result")
        if isinstance(result_field, str):
            findings_text = result_field
            parsed = _try_parse_json(result_field)
        elif isinstance(result_field, dict):
            parsed = result_field
            findings_text = json.dumps(result_field, ensure_ascii=False)
        else:
            parsed = wrapper
    else:
        findings_text = stdout
        parsed = _try_parse_json(stdout)

    if output_path is not None and parsed is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(parsed, ensure_ascii=False, indent=2), encoding="utf-8")

    ok = exit_code in (0, -1, None) and parsed is not None
    return ClaudeResult(
        ok=ok, raw_stdout=stdout, raw_stderr=stderr,
        parsed_json=parsed, findings_text=findings_text,
        duration_sec=duration, exit_code=exit_code if exit_code is not None else -1,
        model=model,
    )


def read_md(md_path: Path) -> str:
    return md_path.read_text(encoding="utf-8")


def case_dir(case_id: str) -> Path:
    return cfg.DATASETS_DIR / case_id

"""Text / MD verification via Claude CLI."""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Optional

from .context_loader import FindingContext
from .parse import EVDecision, missing_decision, parse_verification_response

_PROMPT_PATH = Path(__file__).parent / "prompts" / "verify_text.ru.md"
_DEFAULT_MODEL = os.environ.get("EV_TEXT_MODEL", "sonnet")
_CLAUDE_CWD = Path("/tmp/sonnet_clean")


def _format_finding(finding: dict, section: str = "") -> str:
    parts = [
        f"ID: {finding.get('id', '?')}",
        f"Раздел: {section or finding.get('section', '?')}",
        f"Критичность: {finding.get('severity', '?')}",
        f"Замечание: {finding.get('problem') or finding.get('description') or ''}",
        f"Норма: {finding.get('norm', '')}",
    ]
    return "\n".join(p for p in parts if p)


def _build_prompt(ctx: FindingContext) -> str:
    template = _PROMPT_PATH.read_text(encoding="utf-8")
    return (
        template.replace("{{FINDING}}", _format_finding(ctx.finding, ctx.section))
        .replace("{{MD_EXCERPT}}", ctx.md_excerpt[:6000] or "(фрагмент документа недоступен)")
    )


def _call_claude(prompt: str, model: str) -> str:
    try:
        from backend.app.core.config import get_claude_cli
        cli = get_claude_cli()
    except Exception:
        cli = "/home/coder/.local/bin/claude"
    _CLAUDE_CWD.mkdir(parents=True, exist_ok=True)
    env = {
        "HOME": os.environ.get("HOME", "/home/coder"),
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "LANG": os.environ.get("LANG", "C.UTF-8"),
        "LC_ALL": os.environ.get("LC_ALL", "C.UTF-8"),
        "ANTHROPIC_API_KEY": os.environ.get("ANTHROPIC_API_KEY", ""),
        "CLAUDE_CODE_OAUTH_TOKEN": os.environ.get("CLAUDE_CODE_OAUTH_TOKEN", ""),
    }
    proc = subprocess.run(
        [cli, "-p", "--model", model, "--allowedTools", "none",
         "--output-format", "json", "--max-turns", "1"],
        input=prompt,
        capture_output=True,
        text=True,
        cwd=str(_CLAUDE_CWD),
        env=env,
        timeout=180,
    )
    if proc.returncode != 0 and not (proc.stdout or "").strip():
        raise RuntimeError(proc.stderr or proc.stdout or "claude failed")
    if not (proc.stdout or "").strip():
        raise RuntimeError(proc.stderr or "claude returned empty output")
    wrapper = json.loads(proc.stdout)
    return wrapper.get("result", proc.stdout)


def verify_text(ctx: FindingContext, *, model: Optional[str] = None) -> EVDecision:
    model = model or _DEFAULT_MODEL
    if not ctx.md_excerpt.strip():
        return missing_decision(
            ctx.finding,
            verification_path="text",
            explanation="Текстовый контекст документа недоступен.",
        )
    try:
        raw = _call_claude(_build_prompt(ctx), model)
    except Exception as exc:
        return missing_decision(
            ctx.finding,
            verification_path="text",
            explanation=f"Claude CLI error: {exc}",
        )
    parsed = parse_verification_response(
        raw,
        expected_ids={str(ctx.finding.get("id", ""))},
        verification_path="text",
    )
    if not parsed:
        return missing_decision(ctx.finding, verification_path="text")
    d = parsed[0]
    d.model_used = model
    return d

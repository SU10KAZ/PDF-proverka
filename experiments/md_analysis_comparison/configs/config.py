"""Configuration for the MD-analysis comparison experiment.

Isolated from production. No imports from backend/app/* are allowed here.
Only the Claude CLI binary path is borrowed conceptually (same lookup logic).
"""
from __future__ import annotations

import os
import shutil
from pathlib import Path

EXP_ROOT = Path(__file__).resolve().parents[1]
DATASETS_DIR = EXP_ROOT / "datasets"
RESULTS_DIR = EXP_ROOT / "results"
LOGS_DIR = EXP_ROOT / "logs"
TEMP_DIR = EXP_ROOT / "temp"
REPORTS_DIR = EXP_ROOT / "reports"
PROMPTS_DIR = EXP_ROOT / "prompts"
COMPARISON_OUTPUTS_DIR = EXP_ROOT / "comparison_outputs"

for _d in (RESULTS_DIR, LOGS_DIR, TEMP_DIR, REPORTS_DIR, COMPARISON_OUTPUTS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

MODEL_OPUS = os.environ.get("EXP_MODEL_OPUS", "claude-opus-4-7")
MODEL_SONNET = os.environ.get("EXP_MODEL_SONNET", "claude-sonnet-4-6")

DEFAULT_TIMEOUT_SEC = int(os.environ.get("EXP_DEFAULT_TIMEOUT", "1800"))
AGENT_TIMEOUT_SEC = int(os.environ.get("EXP_AGENT_TIMEOUT", "900"))
CRITIC_TIMEOUT_SEC = int(os.environ.get("EXP_CRITIC_TIMEOUT", "1200"))

MULTI_AGENT_PARALLELISM = int(os.environ.get("EXP_MULTI_AGENT_PARALLELISM", "4"))
MAX_RETRIES = int(os.environ.get("EXP_MAX_RETRIES", "1"))


def _is_usable_cli(path: str | Path | None) -> bool:
    if not path:
        return False
    p = Path(path)
    return p.exists() and os.access(p, os.X_OK)


def _scan_vscode_claude() -> str | None:
    home = Path.home()
    ext_dirs = [home / ".vscode-server" / "extensions", home / ".vscode" / "extensions"]
    candidates: list[tuple[float, str]] = []
    for ext_dir in ext_dirs:
        if not ext_dir.exists():
            continue
        for d in ext_dir.glob("anthropic.claude-code-*"):
            binary = d / "resources" / "native-binary" / "claude"
            if _is_usable_cli(binary):
                try:
                    mtime = d.stat().st_mtime
                except OSError:
                    mtime = 0.0
                candidates.append((mtime, str(binary)))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][1]


def find_claude_cli() -> str:
    found = shutil.which("claude")
    if _is_usable_cli(found):
        return found
    extended = os.environ.get("PATH", "") + os.pathsep + str(Path.home() / ".local" / "bin")
    found = shutil.which("claude", path=extended)
    if _is_usable_cli(found):
        return found
    scanned = _scan_vscode_claude()
    if scanned:
        return scanned
    raise RuntimeError(
        "Claude CLI not found. Install Claude Code (VS Code extension or `claude` in PATH)."
    )


CLAUDE_CLI = find_claude_cli()

ALLOWED_TOOLS = "Read,Write,Grep,Glob"

LOG_RAW_OUTPUTS = os.environ.get("EXP_LOG_RAW", "1") == "1"

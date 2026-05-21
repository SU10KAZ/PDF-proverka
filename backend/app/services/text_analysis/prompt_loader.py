"""Phase 1 prompt loader (scaffolding — not wired into Stage 01).

Reads the production-ready Phase 1 completeness-lens prompts from
`prompts/pipeline/ru/phase1/`. No template substitution, no LLM, no
network. The future completeness-lens runner is responsible for
substituting placeholders into the loaded text.

Public API:
    PHASE1_PROMPTS_DIR: Path
    KNOWN_PROMPTS: frozenset[str]
    available_prompts() -> list[str]
    load_prompt(name: str) -> str
    extract_placeholders(text: str) -> list[str]
    PromptNotFoundError

This module is intentionally minimal — anything beyond file IO belongs in
the runner. See `prompts/pipeline/ru/phase1/README.md` for the manifest.
"""
from __future__ import annotations

import re
from pathlib import Path

from backend.app.core.config import PROMPTS_DIR

# Anchor under the existing PROMPTS_DIR convention so AUDIT_PROMPTS_DIR
# overrides keep working.
PHASE1_PROMPTS_DIR: Path = PROMPTS_DIR / "pipeline" / "ru" / "phase1"

# Locked manifest: every prompt name that must be present on disk for a
# valid Phase 1 prompt set. Adding a new file requires updating this set
# AND the README manifest.
KNOWN_PROMPTS: frozenset[str] = frozenset({
    "completeness_lens_production_prompt",
    "stage01_document_type_block",
    "stage01_few_shot_examples",
    "stage01_production_prompt",
    "stage01_severity_calibration",
})

# Single-brace placeholders, matching existing pipeline-prompt convention
# (`{NAME}`). Lookbehind/lookahead reject `{{NAME}}` (Jinja-style double
# braces) and lower-case forms — anything outside this pattern is plain
# markdown text.
_PLACEHOLDER_RE = re.compile(r"(?<!\{)\{([A-Z_][A-Z0-9_]*)\}(?!\})")


class PromptNotFoundError(FileNotFoundError):
    """Raised when a prompt file is missing or empty for a known name."""


def available_prompts() -> list[str]:
    """Return the sorted list of prompt names whose `.md` file is on disk.

    Subset of KNOWN_PROMPTS. Comparing the two reveals gaps.
    """
    out: list[str] = []
    for name in sorted(KNOWN_PROMPTS):
        if (PHASE1_PROMPTS_DIR / f"{name}.md").is_file():
            out.append(name)
    return out


def load_prompt(name: str) -> str:
    """Return raw Markdown text of the named prompt.

    Args:
        name: prompt name without the `.md` extension. Must be in
            KNOWN_PROMPTS. Case-sensitive (filenames carry meaning).

    Raises:
        ValueError: if `name` is not in KNOWN_PROMPTS or is not a string.
        PromptNotFoundError: if the file is missing or empty.
    """
    if not isinstance(name, str) or not name.strip():
        raise ValueError("prompt name must be a non-empty string")
    key = name.strip()
    if key not in KNOWN_PROMPTS:
        raise ValueError(
            f"unknown prompt {key!r}; expected one of {sorted(KNOWN_PROMPTS)}"
        )
    path = PHASE1_PROMPTS_DIR / f"{key}.md"
    if not path.is_file():
        raise PromptNotFoundError(f"prompt file missing: {path}")
    text = path.read_text(encoding="utf-8")
    if not text.strip():
        raise PromptNotFoundError(f"prompt file is empty: {path}")
    return text


def extract_placeholders(text: str) -> list[str]:
    """Return the sorted-unique list of `{PLACEHOLDER}` names found in text.

    Pure string scan — no substitution. Used by tests and the future
    runner to verify expected placeholders are present before LLM call.
    """
    if not isinstance(text, str):
        raise ValueError("text must be a string")
    return sorted({m.group(1) for m in _PLACEHOLDER_RE.finditer(text)})

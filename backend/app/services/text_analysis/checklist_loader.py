"""Discipline-checklist loader (Phase 1 scaffolding).

Reads `backend/app/data/discipline_checklists/<DISCIPLINE>.md` files as plain
text. No parsing, no filtering, no LLM. The future completeness-lens runner
will parse the tier sections and apply the `applies=<doc_type>` gates.

Public API:
    available_disciplines() -> list[str]
    load_checklist(discipline: str) -> str
    KNOWN_DISCIPLINES: frozenset[str]
    CHECKLIST_DIR: Path

This module is not wired into Stage 01 yet — see Step 0.2 of
`experiments/md_analysis_comparison/production_preparation/rollout/phase1_rollout.md`.
"""
from __future__ import annotations

from pathlib import Path

from backend.app.core.config import APP_DATA_DIR

CHECKLIST_DIR: Path = APP_DATA_DIR / "discipline_checklists"

# The 8 discipline codes that must each have a corresponding <CODE>.md file.
# Locked here so loader behaviour is independent of filesystem order.
KNOWN_DISCIPLINES: frozenset[str] = frozenset({
    "AR", "EOM", "KJ", "KM", "MULTI", "OV", "SS", "VK",
})


class ChecklistNotFoundError(FileNotFoundError):
    """Raised when a checklist file is missing for a known discipline."""


def available_disciplines() -> list[str]:
    """Return the sorted list of discipline codes that have a checklist file
    physically present on disk.

    The returned list is a subset of KNOWN_DISCIPLINES — a discipline whose
    file is missing is silently absent here (callers can compare against
    KNOWN_DISCIPLINES to find gaps).
    """
    out: list[str] = []
    for code in sorted(KNOWN_DISCIPLINES):
        if (CHECKLIST_DIR / f"{code}.md").is_file():
            out.append(code)
    return out


def load_checklist(discipline: str) -> str:
    """Return the raw Markdown text of the named discipline's checklist.

    Args:
        discipline: discipline code, must be in KNOWN_DISCIPLINES.

    Raises:
        ValueError: if discipline is not in KNOWN_DISCIPLINES.
        ChecklistNotFoundError: if the file is missing or empty.
    """
    if not isinstance(discipline, str) or not discipline.strip():
        raise ValueError("discipline must be a non-empty string")
    code = discipline.strip().upper()
    if code not in KNOWN_DISCIPLINES:
        raise ValueError(
            f"unknown discipline {code!r}; expected one of "
            f"{sorted(KNOWN_DISCIPLINES)}"
        )
    path = CHECKLIST_DIR / f"{code}.md"
    if not path.is_file():
        raise ChecklistNotFoundError(
            f"checklist file missing for discipline {code!r}: {path}"
        )
    text = path.read_text(encoding="utf-8")
    if not text.strip():
        raise ChecklistNotFoundError(
            f"checklist file is empty for discipline {code!r}: {path}"
        )
    return text

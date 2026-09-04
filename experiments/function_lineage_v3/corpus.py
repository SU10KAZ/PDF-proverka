"""Frozen corpus access for the v3.0 feasibility audit.

Reads only what already exists: the pair contract, the source PDF, the
recognized Markdown, ``blocks.json`` and the frozen v2 candidate artifacts.
Nothing is regenerated and nothing is written back into a pair directory.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Iterator, Mapping

REPO_ROOT = Path(__file__).resolve().parents[2]
SESSION_ID = "7cccec69bb0b4327"
PAIRS_ROOT = REPO_ROOT / "comparison" / "sessions" / SESSION_ID / "pairs"
CANDIDATE_ROOT = (
    REPO_ROOT
    / "comparison"
    / "ai_sheet_matcher"
    / "20260903_function_lineage_deterministic"
    / "candidate_artifacts"
)
COMPARISON_ROOT = REPO_ROOT / "comparison" / "ai_sheet_matcher"

#: The three frozen corpora, in the order every table in this track uses.
PROJECTS: dict[str, str] = {
    "p19cd7f695a": "IOS1.1",
    "pe336037597": "IOS2.1",
    "pb02de74a81": "IOS3.1",
}
CORPUS_ORDER = ("IOS1.1", "IOS2.1", "IOS3.1")
SIDES = ("LEFT", "RIGHT")

_PAGE_RE = re.compile(r"(?m)^##\s+Page\s+(\d+)\s*$")
_NORMALIZE_RE = re.compile(r"[^0-9a-zа-яё]+")


def read_json(path: Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def normalize(value: str) -> str:
    """Fold a string to the comparable form used for every text join here."""
    return _NORMALIZE_RE.sub(" ", str(value).lower().replace("ё", "е")).strip()


def pair_contract(pair_id: str) -> dict[str, Any]:
    return read_json(PAIRS_ROOT / pair_id / "pair.json")


def document_paths(pair_id: str, side: str) -> dict[str, Path]:
    contract = pair_contract(pair_id)[side.lower()]
    pdf = Path(str(contract["pdf_path"]))
    return {
        "pdf": pdf,
        "markdown": Path(str(contract["md_path"])),
        "html": Path(str(contract["html_path"])),
        "blocks": pdf.parent / "blocks.json",
        "code": str(contract["document_code"]),
    }


def markdown_pages(path: Path) -> dict[int, str]:
    text = Path(path).read_text(encoding="utf-8", errors="replace")
    matches = list(_PAGE_RE.finditer(text))
    pages: dict[int, str] = {}
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        pages[int(match.group(1))] = text[match.end():end]
    return pages


def blocks_document(path: Path) -> dict[str, Any] | None:
    path = Path(path)
    if not path.exists():
        return None
    return read_json(path)


def candidate_artifact(pair_id: str) -> dict[str, Any]:
    return read_json(CANDIDATE_ROOT / f"{pair_id}.json")


def documents() -> Iterator[tuple[str, str, str, dict[str, Path]]]:
    """Yield ``(pair_id, corpus, side, paths)`` in a stable order."""
    for pair_id in sorted(PROJECTS, key=lambda key: CORPUS_ORDER.index(PROJECTS[key])):
        for side in SIDES:
            yield pair_id, PROJECTS[pair_id], side, document_paths(pair_id, side)


def passports(pair_id: str) -> dict[str, dict[str, Mapping[str, Any]]]:
    artifact = candidate_artifact(pair_id)
    return {side: artifact["function_passports"][side] for side in SIDES}

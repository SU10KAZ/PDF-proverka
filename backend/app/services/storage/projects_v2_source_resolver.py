"""Read-only source resolver for projects_v2 version directories.

The audit pipeline receives ``versions/<vid>`` as its project directory in
v2-primary mode, while source files live under ``01_input`` and normalized work
copies under ``02_work``. This module only reads that layout and never falls
back to legacy ``projects/``.
"""
from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import NamedTuple, Optional

_DOC_SUFFIX = "_document.md"
_EXCLUDE_PREFIXES = ("audit_", "readme", "claude", "_combined")


class V2SourceFiles(NamedTuple):
    md_path: Optional[Path]
    pdf_path: Optional[Path]
    result_json_path: Optional[Path]


def _norm(value: str | None) -> str:
    value = unicodedata.normalize("NFKC", value or "").replace("ё", "е").replace("Ё", "Е")
    value = value.lower()
    return re.sub(r"[^0-9a-zа-я]+", "", value)


def _document_code_stem(document_code: str | None) -> str:
    name = Path(str(document_code or "").strip().rstrip("/")).name
    if name.lower().endswith(".pdf"):
        name = name[:-4]
    return name


def _is_doc_md(path: Path) -> bool:
    low = path.name.lower()
    return low.endswith(_DOC_SUFFIX) and not low.startswith(_EXCLUDE_PREFIXES)


def _stem_without_suffix(path: Path, suffix: str) -> str:
    name = path.name
    if name.lower().endswith(suffix):
        return name[: -len(suffix)]
    return path.stem


def _files(root: Path, pattern: str) -> list[Path]:
    if not root.is_dir():
        return []
    return sorted(p for p in root.rglob(pattern) if p.is_file())


def _select_candidate(paths: list[Path], document_code: str | None, *, suffix: str | None = None) -> Optional[Path]:
    if not paths:
        return None
    code_norm = _norm(_document_code_stem(document_code))
    if code_norm:
        exact: list[Path] = []
        contains: list[Path] = []
        for path in paths:
            stem = _stem_without_suffix(path, suffix) if suffix else path.stem
            stem_norm = _norm(stem)
            if stem_norm == code_norm:
                exact.append(path)
            elif stem_norm and (code_norm in stem_norm or stem_norm in code_norm):
                contains.append(path)
        if len(exact) == 1:
            return exact[0]
        if len(contains) == 1:
            return contains[0]
    if len(paths) == 1:
        return paths[0]
    return None


def _resolve_md(version_dir: Path, document_code: str | None) -> Optional[Path]:
    work = version_dir / "02_work"
    inp = version_dir / "01_input"
    normalized = work / "document.md"
    if normalized.is_file():
        return normalized
    candidates = [p for p in _files(work, f"*{_DOC_SUFFIX}") if _is_doc_md(p)]
    candidates.extend(p for p in _files(inp, f"*{_DOC_SUFFIX}") if _is_doc_md(p))
    return _select_candidate(candidates, document_code, suffix=_DOC_SUFFIX)


def _resolve_pdf(version_dir: Path, document_code: str | None) -> Optional[Path]:
    work = version_dir / "02_work"
    inp = version_dir / "01_input"
    normalized = work / "document.pdf"
    if normalized.is_file():
        return normalized
    candidates = _files(work, "*.pdf") + _files(inp, "*.pdf")
    return _select_candidate(candidates, document_code)


def _resolve_result_json(version_dir: Path, document_code: str | None) -> Optional[Path]:
    work = version_dir / "02_work"
    inp = version_dir / "01_input"
    normalized = work / "result.json"
    if normalized.is_file():
        return normalized
    candidates = _files(work, "*_result.json") + _files(inp, "*_result.json")
    return _select_candidate(candidates, document_code, suffix="_result.json")


def resolve_v2_source_files(version_dir: str | Path, document_code: str | None = None) -> V2SourceFiles:
    """Return MD/PDF/result JSON source files for a projects_v2 version.

    Preference order follows the storage standard: normalized ``02_work`` files
    first, immutable originals in ``01_input`` second. Missing members are
    returned as ``None`` so callers can fail-soft or raise their own diagnostic.
    """
    version_dir = Path(version_dir)
    return V2SourceFiles(
        md_path=_resolve_md(version_dir, document_code),
        pdf_path=_resolve_pdf(version_dir, document_code),
        result_json_path=_resolve_result_json(version_dir, document_code),
    )

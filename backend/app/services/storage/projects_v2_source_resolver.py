"""Read-only source resolver for projects_v2 version directories.

The audit pipeline receives ``versions/<vid>`` as its project directory in
v2-primary mode, while source files live under ``01_input`` and normalized work
copies under ``02_work``. This module only reads that layout and never falls
back to legacy ``projects/``.
"""
from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import NamedTuple, Optional

_DOC_SUFFIX = "_document.md"
# _results.md/_results.html — новый 3-файловый комплект портала (2026-07).
# Старые суффиксы в приоритете и остаются навсегда (чтение уже загруженных
# проектов); удалить после 2026-08-14 можно только ПРИЁМ старого формата
# (upload/миграция), не эти паттерны чтения.
_DOC_SUFFIXES = ("_document.md", "_results.md")
_OCR_HTML_SUFFIXES = ("_ocr.html", "_results.html", "_results.htm")
# _blocks.json — геометрия блоков нового комплекта портала (2026-07-16),
# опциональный файл; нормализованная рабочая копия — 02_work/blocks.json.
_BLOCKS_JSON_SUFFIX = "_blocks.json"
_EXCLUDE_PREFIXES = ("audit_", "readme", "claude", "_combined")


class V2SourceFiles(NamedTuple):
    md_path: Optional[Path]
    pdf_path: Optional[Path]
    result_json_path: Optional[Path]
    blocks_json_path: Optional[Path] = None


class VersionSourceFiles(NamedTuple):
    md_path: Optional[Path]
    pdf_path: Optional[Path]
    result_json_path: Optional[Path]
    project_info_path: Optional[Path]
    ocr_html_path: Optional[Path]
    md_paths: tuple[Path, ...]
    pdf_paths: tuple[Path, ...]
    result_json_paths: tuple[Path, ...]
    ocr_html_paths: tuple[Path, ...]
    layout: str
    blocks_json_path: Optional[Path] = None
    blocks_json_paths: tuple[Path, ...] = ()


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
    return low.endswith(_DOC_SUFFIXES) and not low.startswith(_EXCLUDE_PREFIXES)


def _stem_without_suffix(path: Path, suffix: "str | tuple[str, ...]") -> str:
    name = path.name
    suffixes = (suffix,) if isinstance(suffix, str) else suffix
    for s in suffixes:
        if name.lower().endswith(s):
            return name[: -len(s)]
    return path.stem


def _files(root: Path, pattern: str) -> list[Path]:
    if not root.is_dir():
        return []
    return sorted(p for p in root.rglob(pattern) if p.is_file())


def _doc_md_files(root: Path) -> list[Path]:
    """Кандидаты «главного MD» в приоритете суффиксов: _document.md → _results.md."""
    out: list[Path] = []
    for suffix in _DOC_SUFFIXES:
        out.extend(p for p in _files(root, f"*{suffix}") if _is_doc_md(p) and p not in out)
    return out


def _select_candidate(paths: list[Path], document_code: str | None, *, suffix: "str | tuple[str, ...] | None" = None) -> Optional[Path]:
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
    # По группам суффиксов в порядке приоритета: сперва только *_document.md,
    # затем только *_results.md — оба рядом не «неоднозначность», а старший метод.
    for suffix in _DOC_SUFFIXES:
        candidates = [p for p in _files(work, f"*{suffix}") if _is_doc_md(p)]
        candidates.extend(p for p in _files(inp, f"*{suffix}") if _is_doc_md(p))
        selected = _select_candidate(candidates, document_code, suffix=suffix)
        if selected is not None:
            return selected
    return None


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


def _resolve_blocks_json(version_dir: Path, document_code: str | None) -> Optional[Path]:
    work = version_dir / "02_work"
    inp = version_dir / "01_input"
    normalized = work / "blocks.json"
    if normalized.is_file():
        return normalized
    candidates = _files(work, f"*{_BLOCKS_JSON_SUFFIX}") + _files(inp, f"*{_BLOCKS_JSON_SUFFIX}")
    return _select_candidate(candidates, document_code, suffix=_BLOCKS_JSON_SUFFIX)


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
        blocks_json_path=_resolve_blocks_json(version_dir, document_code),
    )


# ─── Layout-aware helpers for audit readers ─────────────────────────────────

def is_projects_v2_version_dir(version_dir: str | Path) -> bool:
    """Return True for a projects_v2 ``versions/<vid>`` directory.

    The check is intentionally layout-based, not flag-based: smoke tests and
    repair tools may operate on v2 directories while production flags are still
    OFF. Legacy version directories keep their original root-file behavior.
    """
    version_dir = Path(version_dir)
    return (version_dir / "01_input").is_dir() or (version_dir / "02_work").is_dir()


def _unique(paths) -> tuple[Path, ...]:
    result: list[Path] = []
    seen: set[Path] = set()
    for path in paths:
        path = Path(path)
        if path in seen or not path.is_file():
            continue
        seen.add(path)
        result.append(path)
    return tuple(result)


def _legacy_doc_mds(root: Path) -> list[Path]:
    for suffix in _DOC_SUFFIXES:
        candidates = [p for p in root.glob(f"*{suffix}") if p.is_file() and _is_doc_md(p)]
        if candidates:
            return sorted(candidates)
    exclude_names = {"CLAUDE.md", "README.md"}
    return sorted(
        p for p in root.glob("*.md")
        if p.is_file()
        and p.name not in exclude_names
        and not p.name.lower().startswith(_EXCLUDE_PREFIXES)
    )


def _v2_project_info_path(version_dir: Path) -> Optional[Path]:
    for candidate in (
        version_dir / "01_input" / "project_info.json",
        version_dir / "project_info.json",
    ):
        if candidate.is_file():
            return candidate
    return None


def resolve_project_info_path(version_dir: str | Path) -> Optional[Path]:
    """Return project_info.json path for either projects_v2 or legacy layout."""
    version_dir = Path(version_dir)
    if is_projects_v2_version_dir(version_dir):
        return _v2_project_info_path(version_dir)
    candidate = version_dir / "project_info.json"
    if candidate.is_file():
        return candidate
    return _v2_project_info_path(version_dir)


def load_version_project_info(version_dir: str | Path) -> dict:
    """Load project_info for either layout, overlaying version.json.project_info.

    In projects_v2, immutable source metadata is in ``01_input/project_info.json``
    and audit-mutated metadata can be in ``version.json.project_info``. Legacy
    keeps reading only root ``project_info.json`` unless version.json explicitly
    provides an overlay.
    """
    version_dir = Path(version_dir)
    info: dict = {}
    info_path = resolve_project_info_path(version_dir)
    if info_path is not None:
        try:
            data = json.loads(info_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                info.update(data)
        except (json.JSONDecodeError, OSError, UnicodeDecodeError):
            pass
    version_json = version_dir / "version.json"
    if version_json.is_file():
        try:
            data = json.loads(version_json.read_text(encoding="utf-8"))
            version_info = data.get("project_info") if isinstance(data, dict) else None
            if isinstance(version_info, dict):
                info.update(version_info)
        except (json.JSONDecodeError, OSError, UnicodeDecodeError):
            pass
    return info


def _configured_path(root: Path, value: str | None) -> Optional[Path]:
    if not isinstance(value, str) or not value.strip():
        return None
    path = Path(value)
    if not path.is_absolute():
        path = root / path
    return path if path.is_file() else None


def _select_with_info(root: Path, paths: tuple[Path, ...], document_code: str | None, info: dict, key: str) -> Optional[Path]:
    configured = _configured_path(root, info.get(key))
    if configured is not None:
        return configured
    selected = _select_candidate(list(paths), document_code)
    if selected is not None:
        return selected
    return paths[0] if len(paths) == 1 else None


def resolve_version_source_files(
    version_dir: str | Path,
    document_code: str | None = None,
    *,
    project_info: dict | None = None,
) -> VersionSourceFiles:
    """Resolve audit source files for projects_v2 and legacy version layouts."""
    version_dir = Path(version_dir)
    info = project_info if isinstance(project_info, dict) else load_version_project_info(version_dir)
    document_code = document_code or info.get("document_code") or info.get("project_id") or version_dir.name

    if is_projects_v2_version_dir(version_dir):
        v2 = resolve_v2_source_files(version_dir, document_code)
        work = version_dir / "02_work"
        inp = version_dir / "01_input"
        pdf_paths = _unique([work / "document.pdf", *_files(work, "*.pdf"), *_files(inp, "*.pdf")])
        md_paths = _unique([work / "document.md", *_doc_md_files(work), *_doc_md_files(inp)])
        result_json_paths = _unique([work / "result.json", *_files(work, "*_result.json"), *_files(inp, "*_result.json")])
        ocr_html_paths = _unique([
            work / "ocr.html",
            *[p for s in _OCR_HTML_SUFFIXES for p in _files(work, f"*{s}")],
            *[p for s in _OCR_HTML_SUFFIXES for p in _files(inp, f"*{s}")],
        ])
        blocks_json_paths = _unique([
            work / "blocks.json",
            *_files(work, f"*{_BLOCKS_JSON_SUFFIX}"),
            *_files(inp, f"*{_BLOCKS_JSON_SUFFIX}"),
        ])
        return VersionSourceFiles(
            md_path=v2.md_path or (md_paths[0] if len(md_paths) == 1 else None),
            pdf_path=v2.pdf_path or (pdf_paths[0] if len(pdf_paths) == 1 else None),
            result_json_path=v2.result_json_path or (result_json_paths[0] if len(result_json_paths) == 1 else None),
            project_info_path=resolve_project_info_path(version_dir),
            ocr_html_path=ocr_html_paths[0] if ocr_html_paths else None,
            md_paths=md_paths,
            pdf_paths=pdf_paths,
            result_json_paths=result_json_paths,
            ocr_html_paths=ocr_html_paths,
            layout="projects_v2",
            blocks_json_path=v2.blocks_json_path or (blocks_json_paths[0] if blocks_json_paths else None),
            blocks_json_paths=blocks_json_paths,
        )

    pdf_paths = _unique(version_dir.glob("*.pdf"))
    md_paths = _unique(_legacy_doc_mds(version_dir))
    result_json_paths = _unique([*version_dir.glob("*_result.json"), version_dir / "result.json"])
    ocr_html_paths = _unique([
        *[p for s in _OCR_HTML_SUFFIXES for p in version_dir.glob(f"*{s}")],
        version_dir / "ocr.html",
    ])
    blocks_json_paths = _unique([
        *version_dir.glob(f"*{_BLOCKS_JSON_SUFFIX}"),
        version_dir / "blocks.json",
    ])
    return VersionSourceFiles(
        md_path=_select_with_info(version_dir, md_paths, document_code, info, "md_file"),
        pdf_path=_select_with_info(version_dir, pdf_paths, document_code, info, "pdf_file"),
        result_json_path=_select_candidate(list(result_json_paths), document_code, suffix="_result.json") or (result_json_paths[0] if len(result_json_paths) == 1 else None),
        project_info_path=resolve_project_info_path(version_dir),
        ocr_html_path=ocr_html_paths[0] if ocr_html_paths else None,
        md_paths=md_paths,
        pdf_paths=pdf_paths,
        result_json_paths=result_json_paths,
        ocr_html_paths=ocr_html_paths,
        layout="legacy",
        blocks_json_path=_select_candidate(list(blocks_json_paths), document_code, suffix=_BLOCKS_JSON_SUFFIX) or (blocks_json_paths[0] if len(blocks_json_paths) == 1 else None),
        blocks_json_paths=blocks_json_paths,
    )

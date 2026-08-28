"""Signed deterministic cache for DOCUMENT text preparation.

PAGE comparisons intentionally bypass this cache and extract only their
selected pages.  DOCUMENT comparisons may reuse the complete located fragment
set while the document version, PDF/Markdown contents, sheet index and
extractor contract are unchanged.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from backend.app.services.common.atomic_json import atomic_write_json

from . import text_comparison
from .production_artifacts import content_signature, file_content_identity, utc_now


CACHE_KIND = "stage_comparison_text_fragment_cache"
CACHE_SCHEMA_VERSION = "text-fragment-cache.v1"
CACHE_VERSION = "production-text-fragment-cache-v1"


def _sheet_index_identity(sheet_index: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        (
            {
                "pdf_page": int(item["pdf_page"]),
                "sheet_number": item.get("sheet_number"),
            }
            for item in sheet_index
            if item.get("pdf_page") is not None
        ),
        key=lambda item: (item["pdf_page"], str(item.get("sheet_number") or "")),
    )


def cache_input(
    *,
    stage: str,
    document: Mapping[str, Any],
    pdf_path: Path,
    markdown_path: Path,
    sheet_index: list[dict[str, Any]],
) -> dict[str, Any]:
    """Return the complete, content-addressed identity of one cache entry."""
    return {
        "producer": CACHE_VERSION,
        "stage": stage,
        "version_id": document.get("version_id"),
        "pdf": file_content_identity(pdf_path),
        "markdown": file_content_identity(markdown_path),
        "sheet_index": _sheet_index_identity(sheet_index),
    }


def cache_path(cache_dir: Path, *, stage: str, input_signature: str) -> Path:
    safe_stage = "".join(
        char for char in str(stage) if char.isalnum() or char in "-_"
    )
    if not safe_stage:
        raise ValueError("cache stage required")
    return Path(cache_dir) / f"{safe_stage}-{input_signature}.json"


def _validated_cached_fragments(
    value: Any,
    *,
    input_signature: str,
) -> list[dict[str, Any]] | None:
    if not isinstance(value, Mapping):
        return None
    if (
        value.get("kind") != CACHE_KIND
        or value.get("schema_version") != CACHE_SCHEMA_VERSION
        or value.get("input_signature") != input_signature
        or not isinstance(value.get("fragments"), list)
    ):
        return None
    fragments = list(value["fragments"])
    for fragment in fragments:
        if not isinstance(fragment, Mapping):
            return None
        if not fragment.get("id") or int(fragment.get("pdf_page") or 0) < 1:
            return None
    return [dict(fragment) for fragment in fragments]


def load_or_extract_document_fragments(
    *,
    stage: str,
    document: Mapping[str, Any],
    markdown_path: Path,
    pdf_path: Path,
    sheet_index: list[dict[str, Any]],
    fitz: Any,
    cache_dir: Path,
    generated_at: str | None = None,
) -> tuple[list[dict[str, Any]], str]:
    """Load or atomically create one full-document fragment cache entry."""
    identity = cache_input(
        stage=stage,
        document=document,
        pdf_path=pdf_path,
        markdown_path=markdown_path,
        sheet_index=sheet_index,
    )
    input_signature = content_signature(identity)
    target = cache_path(
        cache_dir,
        stage=stage,
        input_signature=input_signature,
    )
    try:
        cached_value = json.loads(target.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, UnicodeDecodeError, json.JSONDecodeError):
        cached_value = None
    cached = _validated_cached_fragments(
        cached_value,
        input_signature=input_signature,
    )
    if cached is not None:
        return cached, input_signature

    fragments = text_comparison.extract_document_fragments(
        stage=stage,
        markdown_path=markdown_path,
        pdf_path=pdf_path,
        sheet_index=sheet_index,
        fitz=fitz,
    )
    artifact = {
        "kind": CACHE_KIND,
        "schema_version": CACHE_SCHEMA_VERSION,
        "version": 1,
        "input_signature": input_signature,
        "generated_at": generated_at or utc_now(),
        "source": identity,
        "fragments": fragments,
        "counts": {
            "fragments": len(fragments),
            "located": sum(bool(item.get("bboxes")) for item in fragments),
        },
        "constraints": {"uses_model": False, "complete_document": True},
    }
    atomic_write_json(target, artifact)
    return [dict(fragment) for fragment in fragments], input_signature


__all__ = [
    "CACHE_KIND",
    "CACHE_SCHEMA_VERSION",
    "CACHE_VERSION",
    "cache_input",
    "cache_path",
    "load_or_extract_document_fragments",
]

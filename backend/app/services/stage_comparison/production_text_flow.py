"""Deterministic Stage 2/3 preparation for explicit production scopes.

Unlike the legacy document flow this module accepts comparison groups directly.
A PAGE selection therefore starts immediately and never depends on persisted
Sheet Matcher approval or a parent relation.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Mapping

from .production_artifacts import (
    content_signature,
    file_content_identity,
    stable_id,
    utc_now,
)
from . import sheet_matching, text_comparison, text_differences
from .text_fragment_cache import load_or_extract_document_fragments


PREPARATION_KIND = "stage_comparison_text_preparation"
PREPARATION_SCHEMA_VERSION = "text-preparation.v2"
PREPARATION_VERSION = "production-text-preparation-v2"


def normalize_comparison_groups(
    values: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    groups = []
    for value in values:
        left = sorted({int(page) for page in value.get("left_pages") or []})
        right = sorted({int(page) for page in value.get("right_pages") or []})
        if not left or not right or min([*left, *right]) < 1:
            raise ValueError("comparison group requires positive LEFT and RIGHT pages")
        relation_type = str(value.get("relation_type") or "MATCHED")
        group_id = str(value.get("id") or value.get("relation_id") or "").strip()
        if not group_id:
            group_id = stable_id("scope_", "LEFT_TO_RIGHT", left, right, relation_type)
        groups.append({
            "id": group_id,
            "left_pages": left,
            "right_pages": right,
            "relation_type": relation_type,
            # ``normalize_comparison_groups`` is intentionally idempotent:
            # Stage 3 receives the already-normalized Stage 2 preparation
            # groups.  Do not erase the confidence recorded on that first
            # pass merely because the source field is now named
            # ``relation_status``.
            "relation_status": value.get("relation_status", value.get("status")),
        })
    groups.sort(key=lambda group: (group["left_pages"], group["right_pages"], group["id"]))
    if len({group["id"] for group in groups}) != len(groups):
        raise ValueError("duplicate comparison group id")
    return groups


def prepare_text_scope(
    pair: Mapping[str, Any],
    comparison_groups: Iterable[Mapping[str, Any]],
    *,
    sheet_indexes: Mapping[str, list[dict[str, Any]]] | None = None,
    fitz: Any,
    generated_at: str | None = None,
    document_cache_dir: Path | None = None,
) -> dict[str, Any]:
    """Extract located text fragments only for pages in the explicit scope.

    When ``document_cache_dir`` is supplied, a content-addressed complete
    document extraction is reused and then projected onto the requested
    groups.  Production passes it only for DOCUMENT runs.  PAGE runs leave it
    unset and the PDF extractor is invoked for selected pages alone.
    """
    groups = normalize_comparison_groups(comparison_groups)
    selected = {
        "left": {page for group in groups for page in group["left_pages"]},
        "right": {page for group in groups for page in group["right_pages"]},
    }
    fragments: dict[str, list[dict[str, Any]]] = {}
    documents: dict[str, Any] = {}
    document_cache_signatures: dict[str, str] = {}
    for side, stage in (("left", "stage_1"), ("right", "stage_2")):
        document = pair.get(side)
        if not isinstance(document, Mapping):
            raise ValueError(f"pair.{side} document required")
        pdf_path = Path(str(document.get("pdf_path") or ""))
        markdown_path = Path(str(document.get("md_path") or ""))
        if not markdown_path.is_file():
            markdown_path = pdf_path.parent / "document.md"
        if not pdf_path.is_file():
            raise FileNotFoundError(pdf_path)
        if not markdown_path.is_file():
            raise FileNotFoundError(markdown_path)
        with fitz.open(str(pdf_path)) as pdf:
            page_count = int(pdf.page_count)
        if selected[side] and max(selected[side]) > page_count:
            raise ValueError(f"{side} page outside document")
        index = list((sheet_indexes or {}).get(side) or [])
        if not index:
            index = sheet_matching.placeholder_sheet_index(page_count)
        if document_cache_dir is not None:
            extracted, cache_signature = load_or_extract_document_fragments(
                stage=stage,
                document=document,
                markdown_path=markdown_path,
                pdf_path=pdf_path,
                sheet_index=index,
                fitz=fitz,
                cache_dir=Path(document_cache_dir),
                generated_at=generated_at,
            )
            document_cache_signatures[side.upper()] = cache_signature
        else:
            extracted = text_comparison.extract_document_fragments(
                stage=stage,
                markdown_path=markdown_path,
                pdf_path=pdf_path,
                sheet_index=index,
                fitz=fitz,
                selected_pages=selected[side],
            )
        fragments[side] = sorted(
            (
                fragment for fragment in extracted
                if int(fragment["pdf_page"]) in selected[side]
                and not text_differences.is_graphic_description(fragment)
            ),
            key=lambda fragment: (
                int(fragment["pdf_page"]),
                int(fragment.get("order") or 0),
                str(fragment["id"]),
            ),
        )
        documents[side.upper()] = {
            "pdf": file_content_identity(pdf_path),
            "markdown": file_content_identity(markdown_path),
            "version_id": document.get("version_id"),
        }
    input_signature = content_signature({
        "producer": PREPARATION_VERSION,
        "pair_id": pair.get("id"),
        "groups": groups,
        "documents": documents,
    })
    return {
        "kind": PREPARATION_KIND,
        "schema_version": PREPARATION_SCHEMA_VERSION,
        "version": 1,
        "pair_id": pair.get("id"),
        "direction": "LEFT_TO_RIGHT",
        "input_signature": input_signature,
        "generated_at": generated_at or utc_now(),
        "comparison_groups": groups,
        "fragments": fragments,
        "documents": documents,
        "extraction": {
            "mode": (
                "DOCUMENT_CACHE"
                if document_cache_dir is not None
                else "SCOPED_PAGES"
            ),
            "selected_pages": {
                side.upper(): sorted(pages) for side, pages in selected.items()
            },
            "document_cache_signatures": document_cache_signatures,
        },
        "constraints": {
            "uses_model": False,
            "parent_relation_required": False,
            "sheet_matcher_is_gate": False,
        },
    }


def build_text_differences_from_preparation(
    preparation: Mapping[str, Any],
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if (
        preparation.get("kind") != PREPARATION_KIND
        or preparation.get("schema_version") != PREPARATION_SCHEMA_VERSION
    ):
        raise ValueError("production text preparation artifact required")
    fragments = preparation.get("fragments")
    if not isinstance(fragments, Mapping):
        raise ValueError("text preparation fragments required")
    left_all = list(fragments.get("left") or [])
    right_all = list(fragments.get("right") or [])
    groups = []
    totals = {
        "sheet_groups_with_differences": 0,
        "changed": 0,
        "removed": 0,
        "added": 0,
        "model_ambiguity": 0,
        "model_failures": 0,
    }
    for group in normalize_comparison_groups(preparation.get("comparison_groups") or []):
        left_pages, right_pages = set(group["left_pages"]), set(group["right_pages"])
        result = text_differences.compare_group(
            [item for item in left_all if int(item["pdf_page"]) in left_pages],
            [item for item in right_all if int(item["pdf_page"]) in right_pages],
        )
        if not any(result[bucket] for bucket in ("changed", "removed", "added")):
            continue
        groups.append({
            "id": group["id"],
            "left_pages": group["left_pages"],
            "right_pages": group["right_pages"],
            "left_labels": [f"Страница {page}" for page in group["left_pages"]],
            "right_labels": [f"Страница {page}" for page in group["right_pages"]],
            "relation_type": group["relation_type"],
            "relation_status": group["relation_status"],
            "changed": result["changed"],
            "removed": result["removed"],
            "added": result["added"],
            "deterministic_same": result["same"],
            "deterministic_ambiguities": result["ambiguous"],
            "ambiguity_count": result["ambiguity_count"],
            "exact_equivalents": result["exact_equivalents"],
        })
        totals["sheet_groups_with_differences"] += 1
        for bucket in ("changed", "removed", "added"):
            totals[bucket] += len(result[bucket])
        totals["model_ambiguity"] += int(result["ambiguity_count"])
    return {
        "version": text_differences.VERSION,
        "kind": text_differences.KIND,
        "pair_id": preparation.get("pair_id"),
        "algorithm": "production_scope_" + text_differences.ALGORITHM,
        "production_path": "STAGE_2_3_DIRECT_SCOPE",
        "generated_at": generated_at or utc_now(),
        "source_signature": content_signature({
            "preparation": preparation.get("input_signature"),
            "algorithm": text_differences.ALGORITHM,
        }),
        "sheet_groups": groups,
        "summary": totals,
        "model": {"used": False, "failures": 0, "reason": "deterministic_production_flow"},
        "constraints": {
            "factual_differences_only": True,
            "graphics_analyzed": False,
            "engineering_findings_created": False,
            "one_row_per_sheet_group": False,
            "parent_relation_required": False,
            "sheet_matcher_is_gate": False,
        },
    }


__all__ = [
    "PREPARATION_KIND",
    "PREPARATION_SCHEMA_VERSION",
    "PREPARATION_VERSION",
    "build_text_differences_from_preparation",
    "normalize_comparison_groups",
    "prepare_text_scope",
]

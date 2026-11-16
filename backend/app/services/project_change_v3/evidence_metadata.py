"""Resolve the MD TEXT / structured TABLE metadata alias before V3 validation.

MD TEXT describes text authority; a Markdown table in that block still has
source modality TABLE. Only this redundant type label can be resolved here,
after an exact source identity match. The raw provider response is never edited.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any

from .validate import known_blocks_for_region

VERSION = "projectchange_v3_md_text_table_metadata/1"


def resolve_md_table_types(
    value: dict[str, Any],
    region: dict[str, Any],
    pages_by_key: dict[tuple[str, int], dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Return a separate canonical answer and a receipt, or the unchanged answer.

    No fuzzy matching, page/bbox/path correction, GRAPHIC conversion, or claim
    edits. Unknown sources and other type mismatches reach the strict validator
    unchanged. Unresolved hints are outside this acceptance path.
    """
    known = known_blocks_for_region(region, pages_by_key)
    resolved = value
    changes: list[dict[str, Any]] = []
    for ci, change in enumerate(value.get("projectchanges") or []):
        changed = False
        for ei, evidence in enumerate(change.get("evidence_items") or []):
            key = (evidence.get("side"), evidence.get("physical_page"), evidence.get("block_id"))
            block = known.get(key)
            if not block or not (
                evidence.get("block_type") == "TEXT"
                and block.get("modality") == "TABLE"
                and block.get("md_block_type") == "TEXT"
                and evidence.get("bbox") == block.get("bbox")
                and evidence.get("source_pdf") == pages_by_key[key[:2]].get("source_pdf")
                and evidence.get("crop_ref") == ""
                and not block.get("graphic_crop_ref")
            ):
                continue
            if resolved is value:
                resolved = deepcopy(value)
            resolved["projectchanges"][ci]["evidence_items"][ei]["block_type"] = "TABLE"
            changed = True
            changes.append({
                "projectchange_id": change["projectchange_id"],
                "evidence_index": ei,
                "side": key[0], "physical_page": key[1], "block_id": key[2],
                "field": "block_type", "from": "TEXT", "to": "TABLE",
                "reason": "exact_source_match_md_text_structured_table",
            })
        if changed:
            canonical_change = resolved["projectchanges"][ci]
            # Modalities describe the canonical evidence, not the MD authority label.
            canonical_change["modalities"] = [
                modality for modality in ("TEXT", "TABLE", "GRAPHIC")
                if any(e["block_type"] == modality for e in canonical_change["evidence_items"])
            ]
    return resolved, changes

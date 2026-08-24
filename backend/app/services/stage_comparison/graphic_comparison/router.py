"""Single production router and common ledger assembly for graphic comparison."""
from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Sequence

from .contract import SCHEMA_VERSION, stable_id, validate_ledger
from .extraction import (
    PreparedBlock,
    block_from_record,
    extract_ink,
    ink_length,
)
from .mode1 import run_mode1
from .policy import EXPERIMENTALLY_CALIBRATED_V1, GraphicMode1Policy
from .quality import extraction_pair_quality


NON_GRAPHIC_BLOCK_TYPES = {
    "text",
    "table",
    "stamp",
    "текст",
    "таблица",
    "штамп",
}


def _scope(blocks: Sequence[PreparedBlock]) -> list[dict[str, Any]]:
    output = []
    for block in blocks:
        item = block.public_scope()
        item["source"]["pdf_sha256"] = block.pdf_hash
        output.append(item)
    return output


def _base_ledger(
    left_blocks: Sequence[PreparedBlock],
    right_blocks: Sequence[PreparedBlock],
    policy: GraphicMode1Policy,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "comparison_scope": {
            "left_blocks": _scope(left_blocks),
            "right_blocks": _scope(right_blocks),
        },
        "route": "NO_GRAPHIC_COMPARISON",
        "mode": None,
        "policy": policy.public_dict(),
        "quality": {},
        "changes": [],
        "diagnostics": {
            "routing": {"reason_code": "NOT_EVALUATED", "message": "Graphic router has not evaluated the pair."},
            "filtered_regions": [],
            "targeted_vision": {"requests": [], "whole_block_comparator_used": False},
        },
    }


def _set_route(
    ledger: dict[str, Any], route: str, reason_code: str, message: str,
    *, mode: str | None = None,
) -> dict[str, Any]:
    ledger["route"] = route
    ledger["mode"] = mode
    ledger["diagnostics"]["routing"] = {
        "reason_code": reason_code,
        "message": message,
    }
    return validate_ledger(ledger)


def _records_to_blocks(
    pdf_path: str | Path,
    records: Sequence[dict[str, Any]],
    policy: GraphicMode1Policy,
    source_artifact: str,
) -> list[PreparedBlock]:
    if not isinstance(records, (list, tuple)):
        raise ValueError("prepared blocks must be arrays")
    return [
        block_from_record(pdf_path, record, policy, source_artifact=source_artifact)
        for record in records
    ]


def _clean_filtered_region(region: dict[str, Any], comparison_id: str) -> dict[str, Any]:
    return {
        "region_id": stable_id(
            "gr_",
            comparison_id,
            region.get("filtered_by"),
            region.get("left_bbox_visual_pt"),
            region.get("right_bbox_visual_pt"),
        ),
        "reason": region.get("filtered_by"),
        "left_bbox_visual_pt": region.get("left_bbox_visual_pt"),
        "right_bbox_visual_pt": region.get("right_bbox_visual_pt"),
        "left_only_ink_pt": region.get("left_only_ink_pt"),
        "right_only_ink_pt": region.get("right_only_ink_pt"),
        "border_status": region.get("border_status"),
        "border_probe": region.get("border_probe"),
    }


def _confidence(region: dict[str, Any], quality: dict[str, Any], registration: dict[str, Any]) -> str:
    if region["classification"] == "UNCERTAIN_GRAPHIC_CHANGE":
        return "LOW"
    extraction_values = [
        quality["left"].get("precision"), quality["left"].get("recall"),
        quality["right"].get("precision"), quality["right"].get("recall"),
    ]
    measured = [float(value) for value in extraction_values if value is not None]
    if (
        float(registration["coverage"]["sym_cov"]) >= 0.95
        and (not measured or min(measured) >= 0.95)
        and not region.get("border_status") == "REAL_BEYOND_BORDER"
    ):
        return "HIGH"
    return "MEDIUM"


def _region_ref(
    block: PreparedBlock, bbox: Sequence[float], ink_pt: float,
) -> dict[str, Any]:
    return {
        "block_id": block.block_id,
        "page_index": block.page_index,
        "bbox_visual_pt": [round(float(value), 2) for value in bbox],
        "ink_pt": round(float(ink_pt), 2),
    }


def _change_from_region(
    region: dict[str, Any],
    left: PreparedBlock,
    right: PreparedBlock,
    comparison_id: str,
    quality: dict[str, Any],
    registration: dict[str, Any],
) -> dict[str, Any]:
    region_id = stable_id(
        "gr_", comparison_id, region["classification"],
        region["left_bbox_visual_pt"], region["right_bbox_visual_pt"],
        region["left_only_ink_pt"], region["right_only_ink_pt"],
    )
    change_id = stable_id("gc_", comparison_id, region_id, region["classification"])
    left_region = None
    right_region = None
    if region["left_only_cells"] > 0:
        left_region = _region_ref(left, region["left_bbox_visual_pt"], region["left_only_ink_pt"])
    if region["right_only_cells"] > 0:
        right_region = _region_ref(right, region["right_bbox_visual_pt"], region["right_only_ink_pt"])
    return {
        "change_id": change_id,
        "type": region["classification"],
        "left_region": left_region,
        "right_region": right_region,
        "evidence": [{
            "kind": "VECTOR_LOCAL_DIFF",
            "region_id": region_id,
            # Both coordinates describe the same local comparison window.
            # Keeping them even when one lifecycle region is null makes
            # removals/additions traceable and gives targeted Vision two
            # honest crops without manufacturing a graphic object.
            "left_comparison_bbox_visual_pt": region["left_bbox_visual_pt"],
            "right_comparison_bbox_visual_pt": region["right_bbox_visual_pt"],
            "left_only_ink_pt": region["left_only_ink_pt"],
            "right_only_ink_pt": region["right_only_ink_pt"],
            "matched_context_cells": region["matched_context_cells"],
            "text_overlap": region["text_overlap"],
            "border_status": region["border_status"],
        }],
        "address_hints": list(region.get("address_hints") or []),
        "confidence": _confidence(region, quality, registration),
        "provenance": ["VECTOR"],
    }


def _targeted_vision_request(
    change: dict[str, Any],
    left: PreparedBlock,
    right: PreparedBlock,
) -> dict[str, Any]:
    evidence = change["evidence"][0]
    return {
        "request_id": stable_id("gvr_", change["change_id"]),
        "change_id": change["change_id"],
        "left_crop": {
            "block_id": left.block_id,
            "page_index": left.page_index,
            "bbox_visual_pt": evidence["left_comparison_bbox_visual_pt"],
        },
        "right_crop": {
            "block_id": right.block_id,
            "page_index": right.page_index,
            "bbox_visual_pt": evidence["right_comparison_bbox_visual_pt"],
        },
        "region_id": evidence["region_id"],
        "question": (
            "Подтверди, является ли локальное различие реальным графическим "
            "изменением или различием представления/кадрирования."
        ),
        "may_create_vector_coordinates": False,
        "status": "PENDING",
    }


def compare_prepared_blocks(
    *,
    left_pdf_path: str | Path,
    right_pdf_path: str | Path,
    left_records: Sequence[dict[str, Any]],
    right_records: Sequence[dict[str, Any]],
    policy: GraphicMode1Policy = EXPERIMENTALLY_CALIBRATED_V1,
    left_source_artifact: str = "blocks.json",
    right_source_artifact: str = "blocks.json",
) -> dict[str, Any]:
    """Route and compare already prepared blocks; never detects new blocks."""
    left_blocks = _records_to_blocks(left_pdf_path, left_records, policy, left_source_artifact)
    right_blocks = _records_to_blocks(right_pdf_path, right_records, policy, right_source_artifact)
    ledger = _base_ledger(left_blocks, right_blocks, policy)

    if not left_blocks or not right_blocks:
        return _set_route(
            ledger, "NO_GRAPHIC_COMPARISON", "EMPTY_PREPARED_SCOPE",
            "At least one prepared side is empty; no graphic comparison unit exists.",
        )
    if any(block.block_type in NON_GRAPHIC_BLOCK_TYPES for block in [*left_blocks, *right_blocks]):
        return _set_route(
            ledger, "NO_GRAPHIC_COMPARISON", "GRAPHIC_NOT_APPLICABLE",
            "Prepared text/table/stamp blocks are owned by their existing pipelines.",
        )
    if len(left_blocks) != 1 or len(right_blocks) != 1:
        return _set_route(
            ledger, "MODE_2_REQUIRED", "MULTI_BLOCK_CORRESPONDENCE_NOT_IN_G1",
            "The array contract is accepted, but G1 does not infer structural many-to-many correspondence.",
        )

    left, right = left_blocks[0], right_blocks[0]
    comparison_id = stable_id(
        "gcmp_", left.pdf_hash, left.block_id, left.page_index,
        right.pdf_hash, right.block_id, right.page_index, policy.version,
    )
    ledger["diagnostics"]["comparison_id"] = comparison_id
    left_ink = extract_ink(left, policy)
    right_ink = extract_ink(right, policy)
    quality = extraction_pair_quality(left, right, left_ink, right_ink, policy)
    ledger["quality"]["extraction"] = quality
    flags = quality["flags"]
    if flags["raster_backed_side"]:
        ledger["diagnostics"]["targeted_vision"].update({
            "reason": "No local vector uncertainty can be isolated honestly; a whole-block Vision comparator is outside G1.",
            "whole_block_forbidden": True,
        })
        return _set_route(
            ledger, "VISION_REQUIRED", "RASTER_BACKED_SOURCE",
            "At least one prepared side is predominantly raster-backed; Mode 1 vector evidence is insufficient.",
        )
    if flags["text_as_curves_asymmetry"]:
        ledger["diagnostics"]["targeted_vision"].update({
            "reason": "Asymmetric glyph outlines cannot be separated from graphics deterministically.",
            "whole_block_forbidden": True,
        })
        return _set_route(
            ledger, "VISION_REQUIRED", "TEXT_AS_CURVES_ASYMMETRY",
            "Text is represented as curves on one side only, so vector evidence is not comparable.",
        )
    if flags["precision_insufficient"] or flags["completeness_insufficient"]:
        ledger["diagnostics"]["targeted_vision"].update({
            "reason": "The rendered-source audit rejected extraction precision or completeness.",
            "whole_block_forbidden": True,
        })
        code = "EXTRACTION_COMPLETENESS_INSUFFICIENT" if flags["completeness_insufficient"] else "EXTRACTION_PRECISION_INSUFFICIENT"
        return _set_route(
            ledger, "VISION_REQUIRED", code,
            "Mode 1 is prohibited because extracted geometry does not explain the visible graphic source honestly.",
        )

    left_ink_pt = ink_length(left_ink["segments"])
    right_ink_pt = ink_length(right_ink["segments"])
    if max(left_ink_pt, right_ink_pt) < policy.min_ink_pt:
        ledger["quality"]["vector_ink_pt"] = {
            "left": round(left_ink_pt, 2), "right": round(right_ink_pt, 2),
        }
        return _set_route(
            ledger, "NO_GRAPHIC_COMPARISON", "TOO_LITTLE_VECTOR_GRAPHICS",
            f"Prepared scope contains only {left_ink_pt:.1f}/{right_ink_pt:.1f} pt of vector ink.",
        )

    mode1 = run_mode1(left, right, left_ink, right_ink, policy)
    registration = mode1["registration"]
    diff = mode1["diff"]
    ledger["quality"].update({
        "registration": registration,
        "diff": diff,
    })
    ledger["diagnostics"]["debug_view"] = mode1["debug_view"]
    ledger["diagnostics"]["filtered_regions"] = [
        _clean_filtered_region(region, comparison_id)
        for region in mode1["filtered_regions"]
    ]
    ledger["diagnostics"]["filtered_region_counts"] = dict(sorted(Counter(
        region.get("filtered_by") for region in mode1["filtered_regions"]
    ).items()))

    if not registration["success"]:
        return _set_route(
            ledger, "MODE_2_REQUIRED", "REGISTRATION_FAILED",
            f"Mode 1 registration failed: {registration['failure_reason']}.",
        )
    if registration["coverage"]["sym_cov"] < policy.min_symmetric_coverage:
        return _set_route(
            ledger, "MODE_2_REQUIRED", "LOW_MATCHED_GRAPHIC_COVERAGE",
            f"Matched graphic coverage {registration['coverage']['sym_cov']:.3f} is below {policy.min_symmetric_coverage:.2f}.",
        )
    if diff["changed_ink_fraction"] > policy.max_changed_ink_fraction:
        return _set_route(
            ledger, "MODE_2_REQUIRED", "CHANGED_FRACTION_TOO_LARGE",
            f"Changed ink fraction {diff['changed_ink_fraction']:.3f} exceeds the local-diff gate.",
        )
    if diff["n_regions_published"] > policy.max_published_regions:
        return _set_route(
            ledger, "MODE_2_REQUIRED", "TOO_MANY_LOCAL_REGIONS",
            f"{diff['n_regions_published']} local regions exceed the G1 maximum of {policy.max_published_regions}.",
        )

    changes = [
        _change_from_region(region, left, right, comparison_id, quality, registration)
        for region in mode1["regions"]
    ]
    ledger["changes"] = changes
    uncertain = [change for change in changes if change["type"] == "UNCERTAIN_GRAPHIC_CHANGE"]
    if uncertain:
        ledger["diagnostics"]["targeted_vision"]["requests"] = [
            _targeted_vision_request(change, left, right) for change in uncertain
        ]
        ledger["diagnostics"]["targeted_vision"]["whole_block_forbidden"] = True
        return _set_route(
            ledger, "VISION_REQUIRED", "LOCAL_BORDER_UNCERTAINTY",
            f"{len(uncertain)} local border region(s) require targeted Vision adjudication.",
            mode="MODE_1",
        )
    message = (
        f"Mode 1 found {len(changes)} local graphic region(s)."
        if changes
        else "Registration succeeded and no publishable graphic change region remains."
    )
    return _set_route(
        ledger, "MODE_1_APPLICABLE", "LOCAL_DIFF_WITHIN_GATES", message,
        mode="MODE_1",
    )


__all__ = ["NON_GRAPHIC_BLOCK_TYPES", "compare_prepared_blocks"]

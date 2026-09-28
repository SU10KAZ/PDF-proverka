"""Bounded independent source-verification batches for changed parameters."""
from __future__ import annotations

from typing import Any


def build_verification_batches(
    *,
    work_items: list[dict[str, Any]],
    projectchanges: list[dict[str, Any]],
    page_records: dict[tuple[str, int], dict[str, Any]],
    max_items_per_batch: int = 8,
    max_batches: int = 4,
) -> list[dict[str, Any]]:
    if max_items_per_batch < 1 or max_batches < 0:
        raise ValueError("invalid verification limits")
    changes = {str(change.get("projectchange_id")): change for change in projectchanges}
    batches = []
    for offset in range(0, min(len(work_items), max_items_per_batch * max_batches), max_items_per_batch):
        chunk = work_items[offset:offset + max_items_per_batch]
        payload_items = []
        images = []
        for work in chunk:
            change = changes.get(str(work.get("projectchange_id")))
            if change is None:
                raise ValueError(f"verification change missing: {work.get('projectchange_id')}")
            parameter = (change.get("changed_parameters") or [])[int(work["parameter_ordinal"]) - 1]
            sources = []
            for side, page, block_id in work.get("evidence") or []:
                page_record = page_records.get((str(side), int(page)))
                if page_record is None:
                    raise ValueError(f"verification page missing: {side}:{page}")
                block = next((row for row in page_record.get("blocks") or [] if row.get("block_id") == block_id), None)
                if block is None:
                    raise ValueError(f"verification block missing: {side}:{page}:{block_id}")
                sources.append({
                    "side": side,
                    "physical_page": page,
                    "block_id": block_id,
                    "modality": block.get("modality"),
                    "bbox": block.get("bbox"),
                    "structured_md": block.get("structured_md"),
                    "tables": block.get("tables") or [],
                })
                if block.get("graphic_crop_ref"):
                    images.append({
                        "path": block["graphic_crop_ref"],
                        "label": {"work_item_id": work["work_item_id"], "side": side,
                                  "physical_page": page, "block_id": block_id, "kind": "GRAPHIC_CROP",
                                  "bbox": block.get("bbox")},
                    })
                if block.get("modality") == "GRAPHIC" and page_record.get("full_page_ref"):
                    images.append({
                        "path": page_record["full_page_ref"],
                        "label": {"work_item_id": work["work_item_id"], "side": side,
                                  "physical_page": page, "block_id": "", "kind": "FULL_PAGE_CONTEXT",
                                  "bbox": [0, 0, 1, 1]},
                    })
            payload_items.append({
                "work_item_id": work["work_item_id"],
                "projectchange_id": work["projectchange_id"],
                "engineering_subject": change.get("engineering_subject"),
                "parameter": parameter,
                "sources": sources,
            })
        batches.append({
            "batch_id": f"VERIFY-B{len(batches) + 1:03d}",
            "work_item_ids": [row["work_item_id"] for row in chunk],
            "data": {"items": payload_items},
            "images": images,
        })
    return batches


def validate_verification_answer(
    *,
    pair_id: str,
    batch: dict[str, Any],
    answer: dict[str, Any],
) -> None:
    if str(answer.get("pair")) != str(pair_id):
        raise ValueError("verification pair mismatch")
    expected = set(batch["work_item_ids"])
    results = answer.get("results") or []
    actual = [str(row.get("work_item_id") or "") for row in results]
    if len(actual) != len(set(actual)) or set(actual) != expected:
        raise ValueError("verification result IDs are not an exact batch partition")
    allowed_refs = {
        item["work_item_id"]: {
            (str(source["side"]), int(source["physical_page"]), str(source["block_id"]))
            for source in item["sources"]
        }
        for item in batch["data"]["items"]
    }
    for result in results:
        cited = {
            (str(ref.get("side")), int(ref.get("physical_page") or 0), str(ref.get("block_id") or ""))
            for ref in result.get("evidence_refs") or []
        }
        if not cited or not cited <= allowed_refs[result["work_item_id"]]:
            raise ValueError(f"verification cites unavailable evidence: {result['work_item_id']}")


def apply_verification_results(
    quality: dict[str, Any],
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    by_id = {str(row["work_item_id"]): row for row in results}
    for work in quality.get("verification_work_items") or []:
        result = by_id.get(str(work["work_item_id"]))
        if result is not None:
            work["status"] = result["verdict"]
            work["result"] = result
    counts = {name: 0 for name in ("CONFIRMED", "CORRECTED", "CONFLICT", "UNREADABLE", "PENDING", "FAILED")}
    for work in quality.get("verification_work_items") or []:
        counts.setdefault(work["status"], 0)
        counts[work["status"]] += 1
    quality["verification_results"] = list(results)
    quality["summary"].update({
        "source_verification_completed": counts["CONFIRMED"] + counts["CORRECTED"] + counts["CONFLICT"] + counts["UNREADABLE"],
        "source_verification_confirmed": counts["CONFIRMED"],
        "source_verification_corrected": counts["CORRECTED"],
        "source_verification_conflicts": counts["CONFLICT"],
        "source_verification_unreadable": counts["UNREADABLE"],
        "source_verification_failed": counts["FAILED"],
        "source_verification_pending": counts["PENDING"] + counts["FAILED"],
        "independently_verified": counts["CONFIRMED"],
    })
    quality["summary"]["quality_complete"] = quality["summary"]["source_verification_pending"] == 0
    return quality

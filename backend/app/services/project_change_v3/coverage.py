"""Deterministic page coverage for a ProjectChange run.

The semantic map proves that every source page was classified as mapped or
unmatched.  It does not prove that the page content was analysed.  This module
keeps those two facts separate and produces a provider-independent work queue
for meaningful unmatched pages.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Iterable

COVERAGE_SCHEMA = "projectchange_v3_coverage/1"
ANALYSIS_STATUSES = frozenset({"PENDING", "PARTIAL", "ANALYZED", "EXCLUDED_WITH_REASON", "FAILED"})
MAPPING_STATUSES = frozenset({"MAPPED", "UNMATCHED"})


def build_page_coverage(
    *,
    pair_id: str,
    run_id: str,
    structure: list[dict[str, Any]],
    semantic_map: dict[str, Any],
    analysed_region_ids: Iterable[str] = (),
    reviewed_unmatched_pages: Iterable[tuple[str, int]] = (),
    failed_unmatched_pages: Iterable[tuple[str, int]] = (),
    page_records: dict[tuple[str, int], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Return exact page accounting without claiming that unmatched is analysed."""
    analysed = {str(value) for value in analysed_region_ids}
    reviewed_unmatched = {(str(side), int(page)) for side, page in reviewed_unmatched_pages}
    failed_unmatched = {(str(side), int(page)) for side, page in failed_unmatched_pages}
    page_records = page_records or {}
    memberships: dict[tuple[str, int], list[str]] = defaultdict(list)
    unmatched: dict[str, set[int]] = {
        "OLD": {int(page) for page in semantic_map.get("unmatched_old") or []},
        "NEW": {int(page) for page in semantic_map.get("unmatched_new") or []},
    }
    for region in semantic_map.get("regions") or []:
        region_id = str(region["region_id"])
        for side, field in (("OLD", "old_pages"), ("NEW", "new_pages")):
            for page in region.get(field) or []:
                memberships[(side, int(page))].append(region_id)

    pages: list[dict[str, Any]] = []
    unmatched_work_items: list[dict[str, Any]] = []
    for source_page in structure:
        side = str(source_page["side"])
        page = int(source_page["physical_page"])
        key = (side, page)
        region_ids = sorted(set(memberships.get(key, [])))
        is_unmatched = page in unmatched.get(side, set())
        if bool(region_ids) == is_unmatched:
            raise ValueError(f"coverage classification is not exclusive for {side}:{page}")

        record = page_records.get(key) or {}
        blocks = list(source_page.get("blocks") or [])
        native_text = str(record.get("native_page_text") or "").strip()
        modalities = sorted({str(block.get("modality")) for block in blocks if block.get("modality")})
        has_content = bool(blocks or native_text)
        mapping_status = "UNMATCHED" if is_unmatched else "MAPPED"
        exclusion_reason = ""
        if not has_content:
            analysis_status = "EXCLUDED_WITH_REASON"
            exclusion_reason = "NO_EXTRACTABLE_CONTENT"
        elif is_unmatched:
            analysis_status = (
                "FAILED" if key in failed_unmatched
                else "ANALYZED" if key in reviewed_unmatched
                else "PENDING"
            )
        else:
            completed = len(set(region_ids) & analysed)
            analysis_status = (
                "ANALYZED" if completed == len(region_ids)
                else "PARTIAL" if completed
                else "PENDING"
            )
        row = {
            "side": side,
            "physical_page": page,
            "mapping_status": mapping_status,
            "analysis_status": analysis_status,
            "region_ids": region_ids,
            "analysed_region_ids": sorted(set(region_ids) & analysed),
            "block_count": len(blocks),
            "modalities": modalities,
            "native_text_present": bool(native_text),
            "exclusion_reason": exclusion_reason,
            "requires_unmatched_review": bool(
                is_unmatched and has_content and analysis_status in {"PENDING", "FAILED"}
            ),
            "unmatched_review_status": (
                analysis_status if is_unmatched and has_content else "NOT_APPLICABLE"
            ),
        }
        pages.append(row)
        if is_unmatched and has_content:
            unmatched_work_items.append({
                "work_item_id": f"UNMATCHED-{side}-P{page:04d}",
                "side": side,
                "physical_page": page,
                "status": analysis_status,
                "reason": "CONTENT_PAGE_NOT_IN_ANY_BILATERAL_REGION",
                "block_count": len(blocks),
                "modalities": modalities,
            })

    expected = {(str(page["side"]), int(page["physical_page"])) for page in structure}
    actual = {(page["side"], page["physical_page"]) for page in pages}
    if actual != expected or len(actual) != len(pages):
        raise ValueError("coverage does not account for every source page exactly once")

    by_side: dict[str, dict[str, Any]] = {}
    for side in ("OLD", "NEW"):
        side_pages = [page for page in pages if page["side"] == side]
        status_counts = Counter(page["analysis_status"] for page in side_pages)
        mapping_counts = Counter(page["mapping_status"] for page in side_pages)
        by_side[side] = {
            "pages_total": len(side_pages),
            "mapping": dict(sorted(mapping_counts.items())),
            "analysis": dict(sorted(status_counts.items())),
            "pages_analysed": status_counts["ANALYZED"],
            "pages_pending": status_counts["PENDING"] + status_counts["PARTIAL"],
            "pages_excluded": status_counts["EXCLUDED_WITH_REASON"],
            "pages_failed": status_counts["FAILED"],
            "content_unmatched": sum(
                page["mapping_status"] == "UNMATCHED" and page["analysis_status"] != "EXCLUDED_WITH_REASON"
                for page in side_pages
            ),
            "content_unmatched_pending": sum(page["requires_unmatched_review"] for page in side_pages),
        }
    summary = {
        "pages_total": len(pages),
        "pages_analysed": sum(page["analysis_status"] == "ANALYZED" for page in pages),
        "pages_pending": sum(page["analysis_status"] in {"PENDING", "PARTIAL"} for page in pages),
        "pages_excluded": sum(page["analysis_status"] == "EXCLUDED_WITH_REASON" for page in pages),
        "pages_failed": sum(page["analysis_status"] == "FAILED" for page in pages),
        "content_unmatched": sum(
            page["mapping_status"] == "UNMATCHED" and page["analysis_status"] != "EXCLUDED_WITH_REASON"
            for page in pages
        ),
        "content_unmatched_pending": sum(page["requires_unmatched_review"] for page in pages),
        "complete": not any(page["requires_unmatched_review"] for page in pages) and all(
            page["analysis_status"] in {"ANALYZED", "EXCLUDED_WITH_REASON"} for page in pages
        ),
        "by_side": by_side,
    }
    return {
        "schema": COVERAGE_SCHEMA,
        "pair_id": pair_id,
        "run_id": run_id,
        "semantic_map_pair": str(semantic_map.get("pair") or ""),
        "analysed_region_ids": sorted(analysed),
        "pages": pages,
        "unmatched_work_items": unmatched_work_items,
        "summary": summary,
        "notes": [
            "Mapped/unmatched is page classification; analysed/pending is content processing.",
            "An unmatched content page stays pending until a separate comparison or bounded one-sided review handles it.",
            "Coverage does not assert that extracted engineering claims are correct.",
        ],
    }


def validate_page_coverage(value: dict[str, Any]) -> None:
    """Fail closed on malformed persisted coverage."""
    if value.get("schema") != COVERAGE_SCHEMA:
        raise ValueError("coverage schema mismatch")
    pages = value.get("pages")
    if not isinstance(pages, list):
        raise ValueError("coverage pages missing")
    keys: set[tuple[str, int]] = set()
    for page in pages:
        key = (str(page.get("side")), int(page.get("physical_page", 0)))
        if key in keys or key[0] not in {"OLD", "NEW"} or key[1] < 1:
            raise ValueError("invalid or duplicate coverage page")
        keys.add(key)
        if page.get("mapping_status") not in MAPPING_STATUSES:
            raise ValueError("invalid coverage mapping status")
        if page.get("analysis_status") not in ANALYSIS_STATUSES:
            raise ValueError("invalid coverage analysis status")
        if page.get("analysis_status") == "EXCLUDED_WITH_REASON" and not page.get("exclusion_reason"):
            raise ValueError("excluded coverage page has no reason")
    work = value.get("unmatched_work_items")
    if not isinstance(work, list):
        raise ValueError("coverage unmatched queue missing")
    work_keys = {(row.get("side"), row.get("physical_page")) for row in work}
    expected = {
        (page["side"], page["physical_page"])
        for page in pages
        if page.get("mapping_status") == "UNMATCHED"
        and page.get("analysis_status") != "EXCLUDED_WITH_REASON"
    }
    if work_keys != expected:
        raise ValueError("coverage unmatched queue differs from pending content pages")

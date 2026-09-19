"""Validate Mapper / Miner payloads for generic comparison identities."""
from __future__ import annotations

import re
from typing import Any


_REGION_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]+$")


class EvidenceTraceabilityError(RuntimeError):
    """A Miner evidence item does not trace to a known source block of its region.

    Raised ONLY by the evidence/provenance traceability check of
    ``validate_miner`` (unknown block / type / bbox, or a GRAPHIC crop that is
    not the block's crop).  It is the single rejection the engine may answer
    with one retry of the identical call; every other validation failure is
    final.  The message text is unchanged (``Untraceable evidence: <id>``).
    """

    def __init__(self, message: str, kind: str, projectchange_id: str):
        super().__init__(message)
        self.kind = kind
        self.projectchange_id = projectchange_id


def validate_map(
    pair_id: str,
    value: dict[str, Any],
    structure: list[dict[str, Any]],
) -> dict[str, Any]:
    if str(value.get("pair")) != str(pair_id):
        raise RuntimeError("Mapping pair mismatch")
    available = {
        side: {p["physical_page"] for p in structure if p["side"] == side}
        for side in ("OLD", "NEW")
    }
    ids = [r["region_id"] for r in value.get("regions") or []]
    if len(ids) != len(set(ids)):
        raise RuntimeError("Duplicate region IDs")
    if any(not _REGION_ID_RE.fullmatch(str(i)) for i in ids):
        raise RuntimeError("Invalid region IDs")
    stats: dict[str, Any] = {"semantic_regions": len(ids)}
    for side in ("OLD", "NEW"):
        field = side.lower() + "_pages"
        mapped = {p for r in value["regions"] for p in r[field]}
        unmatched = set(value["unmatched_" + side.lower()])
        if (
            not mapped <= available[side]
            or not unmatched <= available[side]
            or mapped & unmatched
            or mapped | unmatched != available[side]
        ):
            raise RuntimeError(f"{side} coverage failure")
        stats[side.lower() + "_pages_mapped"] = len(mapped)
        stats[side.lower() + "_pages_total"] = len(available[side])
    if any(not r["old_pages"] or not r["new_pages"] for r in value["regions"]):
        raise RuntimeError("One-sided region")
    return stats


def known_blocks_for_region(
    region: dict[str, Any],
    pages_by_key: dict[tuple[str, int], dict[str, Any]],
) -> dict[tuple[str, int, str], dict[str, Any]]:
    known: dict[tuple[str, int, str], dict[str, Any]] = {}
    for side_key, side in (("old_pages", "OLD"), ("new_pages", "NEW")):
        for page_no in region.get(side_key) or []:
            page = pages_by_key[(side, int(page_no))]
            for block in page["blocks"]:
                known[(side, int(page_no), block["block_id"])] = block
    return known


def validate_miner(
    pair_id: str,
    region: dict[str, Any],
    value: dict[str, Any],
    pages_by_key: dict[tuple[str, int], dict[str, Any]],
) -> None:
    if str(value.get("pair")) != str(pair_id) or value.get("region_id") != region.get(
        "region_id"
    ):
        raise RuntimeError("Miner identity mismatch")
    known = known_blocks_for_region(region, pages_by_key)
    ids = [c["projectchange_id"] for c in value.get("projectchanges") or []]
    hints = [h["hint_id"] for h in value.get("unresolved_hints") or []]
    if len(ids) != len(set(ids)) or len(hints) != len(set(hints)):
        raise RuntimeError("Bad miner IDs")
    for change in value.get("projectchanges") or []:
        if (
            not change["old_pages"]
            or not change["new_pages"]
            or not set(change["old_pages"]) <= set(region["old_pages"])
            or not set(change["new_pages"]) <= set(region["new_pages"])
        ):
            raise RuntimeError(f"Escaped/one-sided pages: {change['projectchange_id']}")
        if {e["side"] for e in change["evidence_items"]} != {"OLD", "NEW"}:
            raise RuntimeError(f"One-sided evidence: {change['projectchange_id']}")
        for evidence in change["evidence_items"]:
            block = known.get(
                (evidence["side"], evidence["physical_page"], evidence["block_id"])
            )
            if (
                block is None
                or evidence["block_type"] != block["modality"]
                or evidence["bbox"] != block["bbox"]
            ):
                raise EvidenceTraceabilityError(
                    f"Untraceable evidence: {change['projectchange_id']}",
                    "untraceable_evidence", change["projectchange_id"],
                )
            if (
                evidence["block_type"] == "GRAPHIC"
                and evidence["crop_ref"] != block["graphic_crop_ref"]
            ):
                raise EvidenceTraceabilityError(
                    f"Graphic crop mismatch: {change['projectchange_id']}",
                    "graphic_crop_mismatch", change["projectchange_id"],
                )

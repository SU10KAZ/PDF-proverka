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


MINER_PAGE_OUTSIDE_REGION = "MINER_PAGE_OUTSIDE_REGION"
MINER_ONE_SIDED_PROJECTCHANGE = "MINER_ONE_SIDED_PROJECTCHANGE"


class MinerStructuralError(RuntimeError):
    """A ProjectChange candidate is one-sided or names a page outside its region.

    Observability only: the acceptance rule is the one that used to raise
    ``Escaped/one-sided pages`` — a final ProjectChange needs OLD and NEW pages,
    all of them inside the semantic region of the call.  ``violations`` lists
    EVERY offending candidate with each of its reasons, so a candidate that is
    both one-sided and out of region is recorded under both codes.  The engine
    may answer this rejection with one retry of the identical call.
    """

    def __init__(self, violations: list[dict[str, Any]]):
        self.violations = violations
        self.codes = sorted({code for v in violations for code in v["codes"]})
        self.projectchange_ids = [v["projectchange_id"] for v in violations]
        self.kind = "+".join(self.codes)
        super().__init__(f"{self.kind}: {', '.join(self.projectchange_ids)}")


def miner_structural_violations(region: dict[str, Any], value: dict[str, Any]) -> list[dict[str, Any]]:
    region_old, region_new = set(region.get("old_pages") or []), set(region.get("new_pages") or [])
    violations: list[dict[str, Any]] = []
    for change in value.get("projectchanges") or []:
        old_pages, new_pages = list(change.get("old_pages") or []), list(change.get("new_pages") or [])
        outside_old = sorted(set(old_pages) - region_old)
        outside_new = sorted(set(new_pages) - region_new)
        codes = []
        if outside_old or outside_new:
            codes.append(MINER_PAGE_OUTSIDE_REGION)
        evidence_sides = {e.get("side") for e in change.get("evidence_items") or []}
        missing_evidence = [side for side in ("OLD", "NEW") if side not in evidence_sides]
        if not old_pages or not new_pages or missing_evidence:
            codes.append(MINER_ONE_SIDED_PROJECTCHANGE)
        if codes:
            violations.append({
                "projectchange_id": change.get("projectchange_id"), "codes": codes,
                "old_pages": old_pages, "new_pages": new_pages,
                "old_pages_outside_region": outside_old, "new_pages_outside_region": outside_new,
                "missing_sides": [side for side, pages in (("OLD", old_pages), ("NEW", new_pages)) if not pages],
                "missing_evidence_sides": missing_evidence,
                "region_old_pages": sorted(region_old), "region_new_pages": sorted(region_new),
            })
    return violations


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
    violations = miner_structural_violations(region, value)
    if violations:
        raise MinerStructuralError(violations)
    for change in value.get("projectchanges") or []:
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

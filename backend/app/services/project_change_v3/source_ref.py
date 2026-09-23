"""V3.1-B compact Miner output: deterministic source_ref expansion (research, OFF by default).

A V3.1 Miner answer cites each evidence block by ``side + physical_page +
block_id`` and no longer echoes ``source_pdf / block_type / bbox / crop_ref``
nor the card index ``modalities / old_pages / new_pages``.  This module turns
such an answer back into the V3 ``MINER_SCHEMA`` shape (same keys, same key
order), so the unchanged V3 validator, checkpoint, dedupe and result take it
as they take a V3 answer.

What it restores, and from where:

* ``source_pdf``  — ``page.json`` of the cited page (``source_pdf``);
* ``block_type``  — the block's ``modality``;
* ``bbox``        — the block's ``bbox`` (the exact floats);
* ``crop_ref``    — the block's ``graphic_crop_ref`` for GRAPHIC, else ``""``;
* ProjectChange ``modalities`` — the block types of the card's own refs
  (TEXT, TABLE, GRAPHIC order); ``old_pages`` / ``new_pages`` — the sorted
  pages of its OLD / NEW refs.  Set unions over what the model cited.

What it never creates or changes: engineering subject, summaries, OLD/NEW
states, parameters, confidence, ``why_one_event``, ``relevant_fragment``,
``evidence_role``, the grouping into cards, any hint field (hints keep the
pages the model declared) — every such value is copied as the model wrote it.

Fail-closed: every ref of every ProjectChange AND every hint must resolve to
exactly one block of the frozen region in the run's own source package; any
other outcome raises ``SourceRefError`` (``unresolvable_source_ref``) with the
reason, and nothing is guessed or repaired.  Resolving hint refs makes V3.1
stricter than V3, whose validator does not trace hint evidence at all.
"""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

from .contracts import MINER_FORMAT_V31_COMPACT, MINER_V31_EXPANSION_VERSION

UNRESOLVABLE_SOURCE_REF = "unresolvable_source_ref"
MODALITY_ORDER = ("TEXT", "TABLE", "GRAPHIC")
HINT_SOURCE_REF_POLICY = "FAIL_CLOSED_STRICTER_THAN_V3"

# Reasons (``SourceRefError.reason``).
MALFORMED_COMPACT_OBJECT = "malformed_compact_object"
MALFORMED_IDENTITY = "malformed_identity"
UNKNOWN_BLOCK_ID = "unknown_block_id"
AMBIGUOUS_BLOCK_ID = "ambiguous_block_id"
WRONG_SIDE = "wrong_side"
PAGE_OUTSIDE_REGION = "page_outside_region"
WRONG_PHYSICAL_PAGE = "wrong_physical_page"
UNKNOWN_BLOCK_TYPE = "unknown_block_type"
GRAPHIC_CROP_MISSING = "graphic_crop_missing"
GRAPHIC_CROP_MISMATCH = "graphic_crop_mismatch"
SOURCE_PACKAGE_MISMATCH = "source_package_mismatch"

_TOP_KEYS = ("pair", "region_id", "projectchanges", "unresolved_hints", "coverage_notes")
_CHANGE_KEYS = ("projectchange_id", "engineering_subject", "scope", "locations", "change_summary",
                "old_state", "new_state", "changed_parameters", "evidence_items", "confidence",
                "why_one_event")
_HINT_KEYS = ("hint_id", "kind", "engineering_subject", "suspected_change", "old_pages", "new_pages",
              "evidence_items", "missing_proof_or_conflict")
_REF_KEYS = ("side", "physical_page", "block_id", "relevant_fragment", "evidence_role")


class SourceRefError(RuntimeError):
    """A compact answer (or one of its source refs) does not resolve exactly: reject, never repair."""

    kind = UNRESOLVABLE_SOURCE_REF

    def __init__(self, reason: str, message: str, *, owner: str | None = None,
                 index: int | None = None, ref: Any = None):
        super().__init__(f"{UNRESOLVABLE_SOURCE_REF}: {reason}: {message}")
        self.reason = reason
        self.owner = owner
        self.index = index
        self.ref = ref

    def receipt(self) -> dict[str, Any]:
        return {"resolution": "FAIL", "kind": self.kind, "reason": self.reason, "owner": self.owner,
                "ref_index": self.index, "ref": self.ref, "message": str(self)}


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def region_page_keys(region: dict[str, Any]) -> list[tuple[str, int]]:
    """The frozen region's pages in the order the Miner bundle lists them (OLD, then NEW)."""
    return [(side, int(page)) for key, side in (("old_pages", "OLD"), ("new_pages", "NEW"))
            for page in region.get(key) or []]


def source_package_sha256(pages: list[dict[str, Any]]) -> str:
    """Identity of a region's source package: the page records, in bundle order."""
    return canonical_sha256(pages)


def _is_page_no(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def _exact_keys(value: Any, keys: tuple[str, ...], where: str) -> None:
    if not isinstance(value, dict) or set(value) != set(keys):
        found = sorted(value) if isinstance(value, dict) else type(value).__name__
        raise SourceRefError(MALFORMED_COMPACT_OBJECT, f"{where}: expected keys {list(keys)}, got {found}")


def check_compact_shape(raw: Any) -> None:
    """Shape of a V3.1 compact answer, checked here too — the expander does not trust the caller."""
    _exact_keys(raw, _TOP_KEYS, "answer")
    for field in ("projectchanges", "unresolved_hints", "coverage_notes"):
        if not isinstance(raw[field], list):
            raise SourceRefError(MALFORMED_COMPACT_OBJECT, f"answer.{field} is not a list")
    for i, change in enumerate(raw["projectchanges"]):
        _exact_keys(change, _CHANGE_KEYS, f"projectchanges[{i}]")
        if not isinstance(change["evidence_items"], list):
            raise SourceRefError(MALFORMED_COMPACT_OBJECT, f"projectchanges[{i}].evidence_items is not a list")
    for i, hint in enumerate(raw["unresolved_hints"]):
        _exact_keys(hint, _HINT_KEYS, f"unresolved_hints[{i}]")
        if not isinstance(hint["evidence_items"], list):
            raise SourceRefError(MALFORMED_COMPACT_OBJECT, f"unresolved_hints[{i}].evidence_items is not a list")


def block_locations(pages_by_key: dict[tuple[str, int], dict[str, Any]]) -> dict[str, list[tuple[str, int]]]:
    """Every (side, page) occurrence of every block_id in the run's source package."""
    index: dict[str, list[tuple[str, int]]] = {}
    for (side, page), record in pages_by_key.items():
        for block in record.get("blocks") or []:
            index.setdefault(block.get("block_id"), []).append((side, int(page)))
    return index


def _sha256_file(path: str) -> str | None:
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()
    except OSError:
        return None


def resolve_source_ref(
    ref: Any,
    *,
    region: dict[str, Any],
    pages_by_key: dict[tuple[str, int], dict[str, Any]],
    locations: dict[str, list[tuple[str, int]]],
    owner: str,
    index: int,
) -> dict[str, Any]:
    """One compact ref -> one V3 evidence item, or ``SourceRefError``."""
    def reject(reason: str, message: str) -> SourceRefError:
        return SourceRefError(reason, f"{owner} evidence[{index}]: {message}", owner=owner, index=index,
                              ref=copy.deepcopy(ref))

    if not isinstance(ref, dict) or set(ref) != set(_REF_KEYS):
        raise reject(MALFORMED_IDENTITY, f"expected keys {list(_REF_KEYS)}")
    side, page, block_id = ref["side"], ref["physical_page"], ref["block_id"]
    if side not in ("OLD", "NEW") or not _is_page_no(page) or not isinstance(block_id, str) or not block_id:
        raise reject(MALFORMED_IDENTITY, f"side={side!r} physical_page={page!r} block_id={block_id!r}")
    if not isinstance(ref["relevant_fragment"], str) or not isinstance(ref["evidence_role"], str):
        raise reject(MALFORMED_IDENTITY, "relevant_fragment / evidence_role must be strings")
    found = locations.get(block_id) or []
    if not found:
        raise reject(UNKNOWN_BLOCK_ID, f"block {block_id} is not in the source package")
    if len(found) != 1:
        raise reject(AMBIGUOUS_BLOCK_ID, f"block {block_id} occurs {len(found)} times: {sorted(found)}")
    ((block_side, block_page),) = found
    if side != block_side:
        raise reject(WRONG_SIDE, f"block {block_id} is on {block_side}, cited as {side}")
    if (side, page) not in set(region_page_keys(region)):
        raise reject(PAGE_OUTSIDE_REGION, f"{side} page {page} is not a page of region {region.get('region_id')}")
    if page != block_page:
        raise reject(WRONG_PHYSICAL_PAGE, f"block {block_id} is on {side} page {block_page}, cited as page {page}")
    record = pages_by_key[(side, page)]
    blocks = [b for b in record.get("blocks") or [] if b.get("block_id") == block_id]
    if len(blocks) != 1:  # unreachable while ``locations`` comes from the same records
        raise reject(AMBIGUOUS_BLOCK_ID, f"block {block_id} occurs {len(blocks)} times on {side} page {page}")
    block = blocks[0]
    modality = block.get("modality")
    if modality not in MODALITY_ORDER:
        raise reject(UNKNOWN_BLOCK_TYPE, f"block {block_id} has modality {modality!r}")
    crop_ref = ""
    if modality == "GRAPHIC":
        crop_ref = block.get("graphic_crop_ref") or ""
        if not crop_ref or not Path(crop_ref).is_file():
            raise reject(GRAPHIC_CROP_MISSING, f"GRAPHIC block {block_id} has no crop on disk ({crop_ref!r})")
        if _sha256_file(crop_ref) != block.get("graphic_crop_sha256"):
            raise reject(GRAPHIC_CROP_MISMATCH, f"GRAPHIC block {block_id}: crop bytes differ from the package")
    return {
        "side": side,
        "source_pdf": record.get("source_pdf", ""),
        "physical_page": page,
        "block_id": block_id,
        "block_type": modality,
        "bbox": copy.deepcopy(block.get("bbox")),
        "crop_ref": crop_ref,
        "relevant_fragment": ref["relevant_fragment"],
        "evidence_role": ref["evidence_role"],
    }


def _check_package(
    region: dict[str, Any],
    pages_by_key: dict[tuple[str, int], dict[str, Any]],
    model_visible_pages: list[dict[str, Any]] | None,
    expected_pdf_sha256: dict[str, str] | None,
) -> tuple[str, dict[str, str]]:
    keys = region_page_keys(region)
    missing = [k for k in keys if k not in pages_by_key]
    if missing:
        raise SourceRefError(SOURCE_PACKAGE_MISMATCH, f"region pages without a page record: {missing}")
    records = [pages_by_key[k] for k in keys]
    for (side, page), record in zip(keys, records):
        if record.get("side") != side or record.get("physical_page") != page:
            raise SourceRefError(SOURCE_PACKAGE_MISMATCH, f"page record {side}:{page} names "
                                 f"{record.get('side')}:{record.get('physical_page')}")
        if expected_pdf_sha256 and record.get("source_pdf_sha256") != expected_pdf_sha256.get(side):
            raise SourceRefError(SOURCE_PACKAGE_MISMATCH, f"page record {side}:{page} is from another PDF")
    package_sha = source_package_sha256(records)
    if model_visible_pages is not None and source_package_sha256(model_visible_pages) != package_sha:
        raise SourceRefError(SOURCE_PACKAGE_MISMATCH,
                             "the resolving source package differs from the pages the model was shown")
    return package_sha, {f"{side}:{page}": canonical_sha256(r) for (side, page), r in zip(keys, records)}


def expand_miner_output(
    raw: Any,
    *,
    region: dict[str, Any],
    pages_by_key: dict[tuple[str, int], dict[str, Any]],
    model_visible_pages: list[dict[str, Any]] | None = None,
    expected_pdf_sha256: dict[str, str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Compact V3.1 answer -> (V3-shaped answer, expansion receipt).  ``raw`` is never modified.

    ``pages_by_key`` is the run's whole source package (block_id must be unique
    in it); ``model_visible_pages`` are the page records the call showed the
    model (``data["pages"]``) — the package the refs are resolved against must
    be exactly that one.
    """
    check_compact_shape(raw)
    package_sha, page_shas = _check_package(region, pages_by_key, model_visible_pages, expected_pdf_sha256)
    locations = block_locations(pages_by_key)
    counts = {"projectchange_refs": 0, "hint_refs": 0}

    def expand_refs(items: list[Any], owner: str, counter: str) -> list[dict[str, Any]]:
        out = []
        for i, ref in enumerate(items):
            out.append(resolve_source_ref(ref, region=region, pages_by_key=pages_by_key,
                                          locations=locations, owner=owner, index=i))
            counts[counter] += 1
        return out

    changes, derived = [], {}
    for change in raw["projectchanges"]:
        owner = str(change["projectchange_id"])
        evidence = expand_refs(change["evidence_items"], owner, "projectchange_refs")
        index = {
            "old_pages": sorted({e["physical_page"] for e in evidence if e["side"] == "OLD"}),
            "new_pages": sorted({e["physical_page"] for e in evidence if e["side"] == "NEW"}),
            "modalities": [t for t in MODALITY_ORDER if any(e["block_type"] == t for e in evidence)],
        }
        derived[owner] = index
        semantic = copy.deepcopy(change)
        changes.append({
            "projectchange_id": semantic["projectchange_id"],
            "engineering_subject": semantic["engineering_subject"],
            "scope": semantic["scope"],
            "locations": semantic["locations"],
            "change_summary": semantic["change_summary"],
            "old_state": semantic["old_state"],
            "new_state": semantic["new_state"],
            "changed_parameters": semantic["changed_parameters"],
            "old_pages": index["old_pages"],
            "new_pages": index["new_pages"],
            "evidence_items": evidence,
            "modalities": index["modalities"],
            "confidence": semantic["confidence"],
            "why_one_event": semantic["why_one_event"],
        })
    hints = []
    for hint in raw["unresolved_hints"]:
        semantic = copy.deepcopy(hint)
        hints.append({
            "hint_id": semantic["hint_id"],
            "kind": semantic["kind"],
            "engineering_subject": semantic["engineering_subject"],
            "suspected_change": semantic["suspected_change"],
            "old_pages": semantic["old_pages"],
            "new_pages": semantic["new_pages"],
            "evidence_items": expand_refs(hint["evidence_items"], str(hint["hint_id"]), "hint_refs"),
            "missing_proof_or_conflict": semantic["missing_proof_or_conflict"],
        })
    expanded = {
        "pair": raw["pair"],
        "region_id": raw["region_id"],
        "projectchanges": changes,
        "unresolved_hints": hints,
        "coverage_notes": copy.deepcopy(raw["coverage_notes"]),
    }
    receipt = {
        "version": MINER_V31_EXPANSION_VERSION,
        "miner_format": MINER_FORMAT_V31_COMPACT,
        "resolution": "PASS",
        "parent_region_id": region.get("region_id"),
        "source_package_sha256": package_sha,
        "page_record_sha256": page_shas,
        "source_ref_count": counts["projectchange_refs"] + counts["hint_refs"],
        "source_refs_resolved": counts["projectchange_refs"] + counts["hint_refs"],
        **counts,
        "hint_source_ref_policy": HINT_SOURCE_REF_POLICY,
        "derived_card_index": derived,
        "raw_compact_canonical_sha256": canonical_sha256(raw),
        "expanded_v3_sha256": canonical_sha256(expanded),
    }
    return expanded, receipt


def compact_from_v3(answer: dict[str, Any]) -> dict[str, Any]:
    """Mechanical V3 -> V3.1 compact conversion (offline round-trip proof and fixtures only).

    Drops exactly the fields the expander restores; every other value is copied.
    """
    def ref(e: dict[str, Any]) -> dict[str, Any]:
        return {k: copy.deepcopy(e[k]) for k in _REF_KEYS}

    return {
        "pair": answer["pair"],
        "region_id": answer["region_id"],
        "projectchanges": [
            {**{k: copy.deepcopy(c[k]) for k in _CHANGE_KEYS if k != "evidence_items"},
             "evidence_items": [ref(e) for e in c["evidence_items"]]}
            for c in answer["projectchanges"]
        ],
        "unresolved_hints": [
            {**{k: copy.deepcopy(h[k]) for k in _HINT_KEYS if k != "evidence_items"},
             "evidence_items": [ref(e) for e in h["evidence_items"]]}
            for h in answer["unresolved_hints"]
        ],
        "coverage_notes": copy.deepcopy(answer["coverage_notes"]),
    }

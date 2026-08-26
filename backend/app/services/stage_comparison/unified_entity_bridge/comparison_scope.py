"""Deterministic Stage 5.3 sheet scope to graphic block scope join.

Stage 5.3 pages are PDF page numbers (1-based).  Graphic prepared-block pages
are indexes (0-based).  This module names both forms and converts them before
matching.  Graphic block-pair grouping is an explicit input: multiple block
pairs in one group are valid children of one sheet scope, while multiple
plausible groups fail closed.
"""
from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from backend.app.pipeline.stages.block_grounding.system_graph_comparator import (
    validate_comparison_result,
)
from backend.app.services.stage_comparison import high_level_project_changes
from backend.app.services.stage_comparison.graphic_comparison.contract import (
    LedgerValidationError,
    validate_ledger,
)

from .document_binding import (
    DocumentBindingValidationError,
    normalize_document_descriptor,
    normalize_pair_documents,
    validate_document_binding,
    verify_document_binding,
)
from .entity_bridge import BRIDGE_VERSION
from .page_identity import (
    PAGE_CONVENTION_VERSION,
    graphic_page_index_0based_to_canonical_index,
    text_pdf_page_1based_to_canonical_index,
)
from .side_entity_contract import (
    SIDE_BRIDGE_VERSION,
    validate_side_graph_entities,
)
from .text_entity_producer import (
    is_stale as text_entities_are_stale,
    validate_text_entities,
)


SCHEMA_VERSION = "text-graphic-scope-join.v2"
KIND = "stage_comparison_text_graphic_scope_join"
SCOPE_JOIN_VERSION = "explicit-page-base-scope-join-v2"
SCOPE_STATES = frozenset({"RESOLVED", "UNRESOLVED_SCOPE"})

_BUCKETS = (
    "high_level_changes",
    "detail_level_increased",
    "material_review",
    "non_material_review",
    "unresolved",
)


class ScopeJoinValidationError(ValueError):
    """Scope evidence cannot be joined without guessing."""


def _digest(value: Any) -> str:
    raw = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _stable_id(prefix: str, *parts: Any) -> str:
    return prefix + _digest(parts)[:20]


def _validate_stage53(stage53: Any) -> dict[str, Any]:
    if not isinstance(stage53, dict):
        raise ScopeJoinValidationError("Stage 5.3 artifact: object required")
    if (
        stage53.get("kind") != high_level_project_changes.KIND
        or stage53.get("schema_version") != high_level_project_changes.SCHEMA_VERSION
        or stage53.get("version") != high_level_project_changes.VERSION
        or not isinstance(stage53.get("pair_id"), str)
        or not stage53["pair_id"].strip()
    ):
        raise ScopeJoinValidationError("Stage 5.3 artifact: unsupported contract")
    return stage53


pdf_page_to_canonical_index = text_pdf_page_1based_to_canonical_index
graphic_page_to_canonical_index = graphic_page_index_0based_to_canonical_index


def _changes(stage53: dict[str, Any]) -> list[dict[str, Any]]:
    output = [
        change
        for bucket in _BUCKETS
        for change in stage53.get(bucket) or []
        if isinstance(change, dict)
    ]
    output.extend(
        change
        for change in (stage53.get("service_structure_summary") or {}).get("items") or []
        if isinstance(change, dict)
    )
    return output


def _stage_groups(
    stage53: dict[str, Any], text_entities: dict[str, Any]
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    seen_evidence: set[str] = set()
    for change in _changes(stage53):
        for detail in change.get("details") or []:
            if not isinstance(detail, dict):
                continue
            evidence_id = str(detail.get("evidence_id") or "").strip()
            group_id = str(detail.get("group_id") or "").strip()
            if not evidence_id or evidence_id in seen_evidence or not group_id:
                continue
            seen_evidence.add(evidence_id)
            group = grouped.setdefault(
                group_id,
                {
                    "sheet_group_id": group_id,
                    "left_pages": set(),
                    "right_pages": set(),
                    "evidence_ids": set(),
                    "source_change_ids": set(),
                    "pair_review_required": False,
                    "source_link_uncertain": False,
                },
            )
            for side in ("left", "right"):
                for page in detail.get(f"{side}_pages") or []:
                    pdf_page_to_canonical_index(page)
                    group[f"{side}_pages"].add(page)
            group["evidence_ids"].add(evidence_id)
            change_id = str(change.get("change_id") or "").strip()
            if change_id:
                group["source_change_ids"].add(change_id)
            group["pair_review_required"] = group["pair_review_required"] or (
                detail.get("pair_status") == "PAIR_REVIEW_REQUIRED"
            )
            group["source_link_uncertain"] = group["source_link_uncertain"] or (
                detail.get("source_status") == "UNCERTAIN"
                or change.get("status") == high_level_project_changes.SOURCE_LINK_UNCERTAIN
            )

    entity_ids: dict[str, set[str]] = defaultdict(set)
    for entity in text_entities["entities"]:
        for group_id in entity["sheet_groups"]:
            entity_ids[group_id].add(entity["entity_id"])

    output = []
    for group_id, group in sorted(grouped.items()):
        left_pages = sorted(group["left_pages"])
        right_pages = sorted(group["right_pages"])
        output.append(
            {
                "sheet_group_id": group_id,
                "left": {
                    "pdf_pages_1based": left_pages,
                    "canonical_page_indexes_0based": [
                        pdf_page_to_canonical_index(page) for page in left_pages
                    ],
                },
                "right": {
                    "pdf_pages_1based": right_pages,
                    "canonical_page_indexes_0based": [
                        pdf_page_to_canonical_index(page) for page in right_pages
                    ],
                },
                "evidence_ids": sorted(group["evidence_ids"]),
                "source_change_ids": sorted(group["source_change_ids"]),
                "text_entity_ids": sorted(entity_ids[group_id]),
                "pair_review_required": bool(group["pair_review_required"]),
                "source_link_uncertain": bool(group["source_link_uncertain"]),
            }
        )
    return output


def _select_block(blocks: Any, block_id: Any, where: str) -> dict[str, Any]:
    if not isinstance(blocks, list) or not blocks:
        raise ScopeJoinValidationError(f"{where}: non-empty block array required")
    if block_id is None:
        if len(blocks) != 1:
            raise ScopeJoinValidationError(
                f"{where}: explicit block id required for multi-block ledger scope"
            )
        block = blocks[0]
    else:
        matches = [block for block in blocks if block.get("block_id") == block_id]
        if len(matches) != 1:
            raise ScopeJoinValidationError(f"{where}: block id must resolve exactly once")
        block = matches[0]
    if not isinstance(block, dict) or not isinstance(block.get("block_id"), str):
        raise ScopeJoinValidationError(f"{where}: invalid block")
    graphic_page_to_canonical_index(block.get("page_index"))
    return block


def normalize_graphic_scope_groups(value: Any) -> list[dict[str, Any]]:
    """Validate explicit block-pair grouping and return deterministic references."""
    if not isinstance(value, list):
        raise ScopeJoinValidationError("graphic_scope_groups: array required")
    output: list[dict[str, Any]] = []
    seen_group_ids: set[str] = set()
    for group_index, raw_group in enumerate(value):
        if not isinstance(raw_group, dict) or set(raw_group) != {"block_pairs"}:
            raise ScopeJoinValidationError(
                f"graphic_scope_groups[{group_index}]: block_pairs object required"
            )
        raw_pairs = raw_group["block_pairs"]
        if not isinstance(raw_pairs, list) or not raw_pairs:
            raise ScopeJoinValidationError(
                f"graphic_scope_groups[{group_index}].block_pairs: non-empty array required"
            )
        pairs: list[dict[str, Any]] = []
        for pair_index, raw_pair in enumerate(raw_pairs):
            where = f"graphic_scope_groups[{group_index}].block_pairs[{pair_index}]"
            if not isinstance(raw_pair, dict) or not {
                "ledger",
                "comparison_result",
            } <= set(raw_pair) or set(raw_pair) - {
                "ledger",
                "comparison_result",
                "left_block_id",
                "right_block_id",
                "left_document",
                "right_document",
            }:
                raise ScopeJoinValidationError(f"{where}: invalid fields")
            # Document provenance is additive: a pair without descriptors keeps
            # byte-identical ids, so enriching one pair never renumbers others.
            try:
                documents = {
                    side: (
                        normalize_document_descriptor(
                            raw_pair[f"{side}_document"], f"{where}.{side}_document"
                        )
                        if f"{side}_document" in raw_pair
                        else None
                    )
                    for side in ("left", "right")
                }
            except DocumentBindingValidationError as error:
                raise ScopeJoinValidationError(f"{where}: {error}") from error
            ledger = raw_pair["ledger"]
            try:
                validate_ledger(ledger)
            except (LedgerValidationError, TypeError, ValueError) as error:
                raise ScopeJoinValidationError(f"{where}.ledger: invalid: {error}") from error
            scope = ledger.get("comparison_scope") or {}
            left = _select_block(
                scope.get("left_blocks"), raw_pair.get("left_block_id"), f"{where}.LEFT"
            )
            right = _select_block(
                scope.get("right_blocks"), raw_pair.get("right_block_id"), f"{where}.RIGHT"
            )
            comparison = raw_pair["comparison_result"]
            if comparison is not None:
                validation = validate_comparison_result(comparison)
                if not validation["valid"]:
                    raise ScopeJoinValidationError(
                        f"{where}.comparison_result: invalid: {validation['errors']}"
                    )
                for side, block in (("left", left), ("right", right)):
                    reference = comparison.get(f"{side}_graph") or {}
                    if (
                        reference.get("block_id") != block["block_id"]
                        or reference.get("page_index") != block["page_index"]
                    ):
                        raise ScopeJoinValidationError(
                            f"{where}.comparison_result: {side} block mismatch"
                        )
            if ledger.get("mode") == "MODE_2" and comparison is None:
                raise ScopeJoinValidationError(
                    f"{where}: MODE_2 ledger requires comparison_result"
                )
            pair = {
                "block_pair_ref": _stable_id(
                    "block_pair_",
                    left["block_id"],
                    left["page_index"],
                    right["block_id"],
                    right["page_index"],
                    _digest(ledger),
                    _digest(comparison) if comparison is not None else None,
                ),
                "left": {
                    "block_id": left["block_id"],
                    "page_index_0based": left["page_index"],
                    "canonical_page_index": graphic_page_to_canonical_index(
                        left["page_index"]
                    ),
                    **(
                        {"document": documents["left"]}
                        if documents["left"] is not None
                        else {}
                    ),
                },
                "right": {
                    "block_id": right["block_id"],
                    "page_index_0based": right["page_index"],
                    "canonical_page_index": graphic_page_to_canonical_index(
                        right["page_index"]
                    ),
                    **(
                        {"document": documents["right"]}
                        if documents["right"] is not None
                        else {}
                    ),
                },
                "mode": ledger.get("mode"),
                "route": ledger.get("route"),
                "ledger_schema_version": ledger.get("schema_version"),
                "ledger_digest": _digest(ledger),
                "comparison_schema_version": (
                    comparison.get("schema_version") if comparison is not None else None
                ),
                "comparison_digest": (
                    _digest(comparison) if comparison is not None else None
                ),
            }
            pairs.append(pair)
        pairs.sort(key=lambda item: item["block_pair_ref"])
        pair_refs = [item["block_pair_ref"] for item in pairs]
        if len(pair_refs) != len(set(pair_refs)):
            raise ScopeJoinValidationError(
                "graphic_scope_groups: duplicate block pair evidence"
            )
        group_id = _stable_id("graphic_scope_", pairs)
        if group_id in seen_group_ids:
            raise ScopeJoinValidationError("graphic_scope_groups: duplicate group")
        seen_group_ids.add(group_id)
        output.append(
            {
                "graphic_scope_group_id": group_id,
                "block_pairs": pairs,
            }
        )
    return sorted(output, key=lambda item: item["graphic_scope_group_id"])


def produce_graphic_scope_groups(block_pairs: Any) -> list[dict[str, Any]]:
    """Group production block pairs by their explicit canonical page pair.

    Callers must pass the complete flat set of available ledger/comparison
    pairs.  Several blocks on the same LEFT/RIGHT page pair become children of
    one graphic scope; no geometry, filename, array position, or fuzzy matcher
    is used.
    """
    if not isinstance(block_pairs, list):
        raise ScopeJoinValidationError("block_pairs: array required")
    grouped: dict[tuple[int, int], list[tuple[str, dict[str, Any]]]] = defaultdict(list)
    for pair in block_pairs:
        normalized = normalize_graphic_scope_groups([{"block_pairs": [pair]}])
        normalized_pair = normalized[0]["block_pairs"][0]
        key = (
            normalized_pair["left"]["canonical_page_index"],
            normalized_pair["right"]["canonical_page_index"],
        )
        grouped[key].append((normalized_pair["block_pair_ref"], pair))
    groups = [
        {
            "block_pairs": [
                pair for _, pair in sorted(pairs, key=lambda item: item[0])
            ]
        }
        for _, pairs in sorted(grouped.items())
    ]
    # Validate the complete grouping, including duplicate group/child IDs.
    normalize_graphic_scope_groups(groups)
    return groups


def _source_artifacts(
    stage53: dict[str, Any],
    text_entities: dict[str, Any],
    side_graph_entities: dict[str, Any],
    graphic_groups: list[dict[str, Any]],
    pair_documents: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "pair_documents": pair_documents,
        "stage53": {
            "pair_id": stage53["pair_id"],
            "schema_version": stage53["schema_version"],
            "version": stage53["version"],
            "source_signature": stage53.get("source_signature"),
            "artifact_digest": _digest(stage53),
        },
        "text_entities": {
            "schema_version": text_entities["schema_version"],
            "source_signature": text_entities["source_signature"],
        },
        "side_graph_entities": {
            "schema_version": side_graph_entities["schema_version"],
            "source_signature": side_graph_entities["source_signature"],
            "left_graph_entities_signature": side_graph_entities["sides"]["LEFT"][
                "source_signature"
            ],
            "right_graph_entities_signature": side_graph_entities["sides"]["RIGHT"][
                "source_signature"
            ],
        },
        "graphic_scope_groups": graphic_groups,
    }


def _scope_signature(sources: dict[str, Any]) -> str:
    return _digest(
        {
            "scope_join_version": SCOPE_JOIN_VERSION,
            "page_convention_version": PAGE_CONVENTION_VERSION,
            "entity_bridge_version": BRIDGE_VERSION,
            "side_bridge_version": SIDE_BRIDGE_VERSION,
            "source_artifacts": sources,
        }
    )


def _group_candidates(
    text_group: dict[str, Any], graphic_groups: list[dict[str, Any]]
) -> list[str]:
    left_pages = set(text_group["left"]["canonical_page_indexes_0based"])
    right_pages = set(text_group["right"]["canonical_page_indexes_0based"])
    return [
        group["graphic_scope_group_id"]
        for group in graphic_groups
        if all(
            pair["left"]["canonical_page_index"] in left_pages
            and pair["right"]["canonical_page_index"] in right_pages
            for pair in group["block_pairs"]
        )
    ]


def build_scope_join(
    stage53: Any,
    text_entities: Any,
    side_graph_entities: Any,
    graphic_scope_groups: Any,
    *,
    current_text_evidence_index: Any = None,
    pair_documents: Any = None,
) -> dict[str, Any]:
    stage = _validate_stage53(stage53)
    text = validate_text_entities(text_entities)
    graphics = validate_side_graph_entities(side_graph_entities)
    if text["source_artifact"]["artifact_digest"] != _digest(stage):
        raise ScopeJoinValidationError("TEXT_ENTITIES stale for current Stage 5.3")
    if current_text_evidence_index is not None and text_entities_are_stale(
        text, stage, current_text_evidence_index
    ):
        raise ScopeJoinValidationError("TEXT_ENTITIES stale for current Stage 5.3")
    groups = normalize_graphic_scope_groups(graphic_scope_groups)
    text_groups = _stage_groups(stage, text)
    candidates_by_text = {
        group["sheet_group_id"]: _group_candidates(group, groups)
        for group in text_groups
    }
    text_candidates_by_graphic: dict[str, list[str]] = defaultdict(list)
    for text_group_id, candidates in candidates_by_text.items():
        for candidate in candidates:
            text_candidates_by_graphic[candidate].append(text_group_id)
    graphic_by_id = {group["graphic_scope_group_id"]: group for group in groups}

    scopes: list[dict[str, Any]] = []
    resolved_graphics: set[str] = set()
    for text_group in text_groups:
        group_id = text_group["sheet_group_id"]
        candidates = candidates_by_text[group_id]
        resolved_id = (
            candidates[0]
            if len(candidates) == 1
            and len(text_candidates_by_graphic[candidates[0]]) == 1
            else None
        )
        if resolved_id is not None:
            state = "RESOLVED"
            reasons = ["exact_canonical_page_membership_unique"]
            graphic_scope = graphic_by_id[resolved_id]
            resolved_graphics.add(resolved_id)
        else:
            state = "UNRESOLVED_SCOPE"
            graphic_scope = None
            if not candidates:
                reasons = ["no_graphic_scope_group_on_canonical_pages"]
            elif len(candidates) > 1:
                reasons = ["multiple_graphic_scope_groups_on_sheet"]
            else:
                reasons = ["graphic_scope_group_matches_multiple_sheet_groups"]
        scope_ref = _stable_id(
            "scope_",
            stage["pair_id"],
            text_group["sheet_group_id"],
            text_group["left"],
            text_group["right"],
            graphic_scope,
        )
        children = []
        if graphic_scope is not None:
            children = [
                {
                    "scope_ref": _stable_id(
                        "block_scope_", scope_ref, pair["block_pair_ref"]
                    ),
                    "block_pair": pair,
                }
                for pair in graphic_scope["block_pairs"]
            ]
        scopes.append(
            {
                "scope_ref": scope_ref,
                "scope_level": "SHEET",
                "status": state,
                "reason_codes": reasons,
                "text_scope": text_group,
                "graphic_scope_group": graphic_scope,
                "child_block_scopes": children,
            }
        )

    for group in groups:
        group_id = group["graphic_scope_group_id"]
        if group_id in resolved_graphics:
            continue
        candidates = sorted(text_candidates_by_graphic[group_id])
        reason = (
            "no_text_scope_candidate"
            if not candidates
            else "graphic_scope_group_matches_multiple_sheet_groups"
        )
        scopes.append(
            {
                "scope_ref": _stable_id(
                    "scope_", stage["pair_id"], None, group, reason
                ),
                "scope_level": "SHEET",
                "status": "UNRESOLVED_SCOPE",
                "reason_codes": [reason],
                "text_scope": None,
                "graphic_scope_group": group,
                "child_block_scopes": [],
            }
        )

    scopes.sort(key=lambda item: item["scope_ref"])
    try:
        documents = normalize_pair_documents(pair_documents)
        binding = verify_document_binding(documents, groups)
    except DocumentBindingValidationError as error:
        raise ScopeJoinValidationError(f"scope join.pair_documents: {error}") from error
    sources = _source_artifacts(stage, text, graphics, groups, documents)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "kind": KIND,
        "scope_join_version": SCOPE_JOIN_VERSION,
        "document_binding": binding,
        "page_convention": {
            "version": PAGE_CONVENTION_VERSION,
            "text": "pdf_page_1based",
            "graphic": "page_index_0based",
            "canonical": "page_index_0based",
            "text_to_canonical": "pdf_page_1based - 1",
        },
        "versions": {
            "entity_bridge": BRIDGE_VERSION,
            "side_bridge": SIDE_BRIDGE_VERSION,
        },
        "source_signature": _scope_signature(sources),
        "source_artifacts": sources,
        "scopes": scopes,
        "diagnostics": {
            "text_sheet_groups": len(text_groups),
            "graphic_scope_groups": len(groups),
            "resolved_scopes": sum(item["status"] == "RESOLVED" for item in scopes),
            "unresolved_scopes": sum(
                item["status"] == "UNRESOLVED_SCOPE" for item in scopes
            ),
            "resolved_child_block_scopes": sum(
                len(item["child_block_scopes"])
                for item in scopes
                if item["status"] == "RESOLVED"
            ),
        },
    }
    return validate_scope_join(payload)


def validate_scope_join(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict) or set(payload) != {
        "schema_version",
        "kind",
        "scope_join_version",
        "document_binding",
        "page_convention",
        "versions",
        "source_signature",
        "source_artifacts",
        "scopes",
        "diagnostics",
    }:
        raise ScopeJoinValidationError("scope join: invalid envelope")
    if (
        payload["schema_version"] != SCHEMA_VERSION
        or payload["kind"] != KIND
        or payload["scope_join_version"] != SCOPE_JOIN_VERSION
    ):
        raise ScopeJoinValidationError("scope join: unsupported contract")
    try:
        binding = validate_document_binding(payload["document_binding"])
    except DocumentBindingValidationError as error:
        raise ScopeJoinValidationError(f"scope join.document_binding: {error}") from error
    if binding["pair_documents"] != payload["source_artifacts"].get("pair_documents"):
        raise ScopeJoinValidationError(
            "scope join.document_binding: pair documents disagree with source artifacts"
        )
    page = payload["page_convention"]
    if not isinstance(page, dict) or page.get("version") != PAGE_CONVENTION_VERSION:
        raise ScopeJoinValidationError("scope join.page_convention: invalid")
    if payload.get("versions") != {
        "entity_bridge": BRIDGE_VERSION,
        "side_bridge": SIDE_BRIDGE_VERSION,
    }:
        raise ScopeJoinValidationError("scope join.versions: invalid")
    if payload["source_signature"] != _scope_signature(payload["source_artifacts"]):
        raise ScopeJoinValidationError("scope join.source_signature: invalid")
    if not isinstance(payload["scopes"], list):
        raise ScopeJoinValidationError("scope join.scopes: array required")
    scope_ids: set[str] = set()
    child_ids: set[str] = set()
    for item in payload["scopes"]:
        if not isinstance(item, dict) or set(item) != {
            "scope_ref",
            "scope_level",
            "status",
            "reason_codes",
            "text_scope",
            "graphic_scope_group",
            "child_block_scopes",
        }:
            raise ScopeJoinValidationError("scope join.scopes: invalid fields")
        if (
            not isinstance(item["scope_ref"], str)
            or item["scope_ref"] in scope_ids
            or item["status"] not in SCOPE_STATES
            or not isinstance(item["reason_codes"], list)
            or not item["reason_codes"]
        ):
            raise ScopeJoinValidationError("scope join.scopes: invalid state")
        scope_ids.add(item["scope_ref"])
        if item["status"] == "RESOLVED" and (
            item["text_scope"] is None or item["graphic_scope_group"] is None
        ):
            raise ScopeJoinValidationError("scope join.scopes: unresolved evidence")
        for child in item["child_block_scopes"]:
            child_id = child.get("scope_ref") if isinstance(child, dict) else None
            if not isinstance(child_id, str) or child_id in child_ids:
                raise ScopeJoinValidationError("scope join child scope: invalid")
            child_ids.add(child_id)
    expected = {
        "text_sheet_groups": sum(item["text_scope"] is not None for item in payload["scopes"]),
        "graphic_scope_groups": len(
            {
                item["graphic_scope_group"]["graphic_scope_group_id"]
                for item in payload["scopes"]
                if item["graphic_scope_group"] is not None
            }
        ),
        "resolved_scopes": sum(item["status"] == "RESOLVED" for item in payload["scopes"]),
        "unresolved_scopes": sum(
            item["status"] == "UNRESOLVED_SCOPE" for item in payload["scopes"]
        ),
        "resolved_child_block_scopes": sum(
            len(item["child_block_scopes"])
            for item in payload["scopes"]
            if item["status"] == "RESOLVED"
        ),
    }
    if payload["diagnostics"] != expected:
        raise ScopeJoinValidationError("scope join.diagnostics: invalid")
    return payload


def scope_join_is_stale(
    artifact: Any,
    stage53: Any,
    text_entities: Any,
    side_graph_entities: Any,
    graphic_scope_groups: Any,
    *,
    current_text_evidence_index: Any = None,
) -> bool:
    try:
        current_stage = _validate_stage53(stage53)
        current_text = validate_text_entities(text_entities)
        current_graphics = validate_side_graph_entities(side_graph_entities)
        groups = normalize_graphic_scope_groups(graphic_scope_groups)
        validated = validate_scope_join(artifact)
    except (ScopeJoinValidationError, TypeError, ValueError):
        return True
    # Document binding is part of the saved evidence, not of the current call:
    # re-deriving it from the artifact keeps an enriched join from reading stale.
    sources = _source_artifacts(
        current_stage,
        current_text,
        current_graphics,
        groups,
        validated["document_binding"]["pair_documents"],
    )
    return (
        current_text["source_artifact"]["artifact_digest"] != _digest(current_stage)
        or (
            current_text_evidence_index is not None
            and text_entities_are_stale(
                current_text, current_stage, current_text_evidence_index
            )
        )
        or validated["source_signature"] != _scope_signature(sources)
    )


def query_text_scope(stage53: Any, sheet_group_id: str) -> dict[str, Any]:
    """Thin Stage 5.3 coverage adapter; it does not synthesize new conclusions."""
    stage = _validate_stage53(stage53)
    # An empty compatible TEXT entity envelope is sufficient because this query
    # uses only the Stage 5.3 group evidence.
    details: list[tuple[dict[str, Any], dict[str, Any]]] = []
    seen: set[str] = set()
    for change in _changes(stage):
        for detail in change.get("details") or []:
            if not isinstance(detail, dict) or detail.get("group_id") != sheet_group_id:
                continue
            evidence_id = str(detail.get("evidence_id") or "").strip()
            if evidence_id and evidence_id not in seen:
                seen.add(evidence_id)
                details.append((change, detail))
    if not details:
        return {
            "sheet_group_id": sheet_group_id,
            "status": "NOT_CHECKED",
            "reason_codes": ["sheet_group_not_in_stage53"],
            "evidence_ids": [],
            "pair_review_required": False,
            "source_link_uncertain": False,
        }
    pair_review = any(
        detail.get("pair_status") == "PAIR_REVIEW_REQUIRED" for _, detail in details
    )
    uncertain = any(
        detail.get("source_status") == "UNCERTAIN"
        or change.get("status") == high_level_project_changes.SOURCE_LINK_UNCERTAIN
        for change, detail in details
    )
    reasons = []
    if pair_review:
        reasons.append("pair_review_required")
    if uncertain:
        reasons.append("source_link_uncertain")
    if not reasons:
        reasons.append("stage53_sheet_group_checked")
    return {
        "sheet_group_id": sheet_group_id,
        "status": "CHECK_BLOCKED" if pair_review or uncertain else "CHECKED",
        "reason_codes": reasons,
        "evidence_ids": sorted(seen),
        "pair_review_required": pair_review,
        "source_link_uncertain": uncertain,
    }


def schema_path() -> Path:
    return Path(__file__).with_name("comparison_scope.schema.json")


__all__ = [
    "KIND",
    "PAGE_CONVENTION_VERSION",
    "SCHEMA_VERSION",
    "SCOPE_JOIN_VERSION",
    "SCOPE_STATES",
    "ScopeJoinValidationError",
    "build_scope_join",
    "graphic_page_to_canonical_index",
    "normalize_graphic_scope_groups",
    "pdf_page_to_canonical_index",
    "produce_graphic_scope_groups",
    "query_text_scope",
    "schema_path",
    "scope_join_is_stale",
    "validate_scope_join",
]

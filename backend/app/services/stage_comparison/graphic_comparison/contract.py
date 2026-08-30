"""Versioned GraphicChangeLedger contract for geometric and structural modes."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any, Iterable

from .confidence_policy import confidence_policy_by_id


# Keep the public G1 constant stable: existing producers continue to write v1.
SCHEMA_VERSION = "graphic-change-ledger.v1"
MODE1_SCHEMA_VERSION = SCHEMA_VERSION
MODE2_SCHEMA_VERSION = "graphic-change-ledger.v2"
SUPPORTED_SCHEMA_VERSIONS = frozenset({MODE1_SCHEMA_VERSION, MODE2_SCHEMA_VERSION})

ROUTES = {
    "MODE_1_APPLICABLE",
    "MODE_2_REQUIRED",
    "VISION_REQUIRED",
    "NO_GRAPHIC_COMPARISON",
}
CHANGE_TYPES = {
    "ADDED_GRAPHIC",
    "REMOVED_GRAPHIC",
    "GEOMETRY_CHANGED",
    "UNCERTAIN_GRAPHIC_CHANGE",
}
STRUCTURAL_CHANGE_TYPES = {
    "SYSTEM_BACKBONE_CHANGED",
    "FUNCTIONAL_GROUP_CHANGED",
    "NODE_ADDED",
    "NODE_REMOVED",
    "NODE_TYPE_CHANGED",
    "NODE_PARAMETER_CHANGED",
    "CONNECTION_CHANGED",
    "GROUP_COUNT_CHANGED",
    "DETAIL_LEVEL_INCREASED",
    "UNCERTAIN_STRUCTURAL_CHANGE",
}
STRUCTURAL_LEVELS = {"SYSTEM", "GROUP", "NODE", "EDGE"}
PROVENANCE = {"VECTOR", "VISION", "BOTH"}
CONFIDENCE = {"HIGH", "MEDIUM", "LOW"}

_LEDGER_KEYS = {
    "schema_version",
    "comparison_scope",
    "route",
    "mode",
    "policy",
    "quality",
    "changes",
    "diagnostics",
}
_MODE2_CHANGE_KEYS = {
    "change_id",
    "mode",
    "type",
    "summary",
    "raw_confidence",
    "mapped_confidence",
    "left_region",
    "right_region",
    "evidence",
    "address_hints",
    "confidence",
    "provenance",
    "structural",
}
_STRUCTURAL_KEYS = {
    "level",
    "source_level",
    "subject",
    "left_nodes",
    "right_nodes",
    "left_edges",
    "right_edges",
    "relation",
    "equivalence",
}


class LedgerValidationError(ValueError):
    pass


def stable_id(prefix: str, *parts: Any, length: int = 20) -> str:
    raw = json.dumps(parts, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return prefix + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


def _require_keys(value: dict, keys: Iterable[str], where: str) -> None:
    missing = [key for key in keys if key not in value]
    if missing:
        raise LedgerValidationError(f"{where}: missing {', '.join(missing)}")


def _reject_unknown_keys(value: dict, allowed: set[str], where: str) -> None:
    unknown = sorted(str(key) for key in set(value) - allowed)
    if unknown:
        raise LedgerValidationError(f"{where}: unknown {', '.join(unknown)}")


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _validate_bbox(value: Any, where: str) -> None:
    if (
        not isinstance(value, list)
        or len(value) != 4
        or not all(_is_number(item) and math.isfinite(float(item)) for item in value)
        or value[2] < value[0]
        or value[3] < value[1]
    ):
        raise LedgerValidationError(f"{where}: invalid bbox")


def _validate_region(region: Any, where: str, *, strict: bool = False) -> None:
    if region is None:
        return
    if not isinstance(region, dict):
        raise LedgerValidationError(f"{where}: must be object or null")
    _require_keys(region, ("block_id", "page_index", "bbox_visual_pt"), where)
    if strict:
        _reject_unknown_keys(
            region,
            {"block_id", "page_index", "bbox_visual_pt", "ink_pt"},
            where,
        )
        if not isinstance(region["block_id"], str) or not region["block_id"].strip():
            raise LedgerValidationError(f"{where}.block_id: non-empty string required")
        if (
            not isinstance(region["page_index"], int)
            or isinstance(region["page_index"], bool)
            or region["page_index"] < 0
        ):
            raise LedgerValidationError(f"{where}.page_index: non-negative integer required")
    if strict:
        _validate_bbox(region.get("bbox_visual_pt"), f"{where}.bbox_visual_pt")
        return
    # Legacy v1 intentionally keeps its original permissive numeric check.
    bbox = region.get("bbox_visual_pt")
    if (
        not isinstance(bbox, list)
        or len(bbox) != 4
        or not all(isinstance(item, (int, float)) for item in bbox)
        or bbox[2] < bbox[0]
        or bbox[3] < bbox[1]
    ):
        raise LedgerValidationError(f"{where}.bbox_visual_pt: invalid bbox")


def _validate_scope(payload: dict, *, strict: bool) -> None:
    scope = payload["comparison_scope"]
    if not isinstance(scope, dict):
        raise LedgerValidationError("comparison_scope must be an object")
    _require_keys(scope, ("left_blocks", "right_blocks"), "comparison_scope")
    if strict:
        _reject_unknown_keys(scope, {"left_blocks", "right_blocks"}, "comparison_scope")
    for side in ("left_blocks", "right_blocks"):
        if not isinstance(scope[side], list):
            raise LedgerValidationError(f"comparison_scope.{side} must be an array")
        if strict and len(scope[side]) != 1:
            raise LedgerValidationError(
                f"comparison_scope.{side}: exactly one graph block required"
            )
        for index, block in enumerate(scope[side]):
            where = f"comparison_scope.{side}[{index}]"
            if not isinstance(block, dict):
                raise LedgerValidationError(f"{where} must be an object")
            _require_keys(
                block,
                ("block_id", "page_index", "block_type", "bbox_visual_pt"),
                where,
            )
            if strict:
                _reject_unknown_keys(
                    block,
                    {
                        "block_id",
                        "page_index",
                        "block_type",
                        "bbox_visual_pt",
                        "source",
                    },
                    where,
                )
                _validate_region(
                    {
                        "block_id": block["block_id"],
                        "page_index": block["page_index"],
                        "bbox_visual_pt": block["bbox_visual_pt"],
                    },
                    where,
                    strict=True,
                )
                if block["block_type"] != "system_graph":
                    raise LedgerValidationError(f"{where}.block_type: system_graph required")
                if "source" in block and not isinstance(block["source"], dict):
                    raise LedgerValidationError(f"{where}.source: object required")


def _validate_common_envelope(payload: dict) -> None:
    _require_keys(payload, _LEDGER_KEYS, "ledger")
    if not isinstance(payload["route"], str) or payload["route"] not in ROUTES:
        raise LedgerValidationError("invalid route")
    if not isinstance(payload["changes"], list):
        raise LedgerValidationError("changes must be an array")


def _validate_v1(payload: dict) -> dict[str, Any]:
    """Preserve the G1 runtime rules byte-for-byte in meaning."""
    if payload["mode"] not in {None, "MODE_1"}:
        raise LedgerValidationError("invalid mode")
    if payload["route"] == "MODE_1_APPLICABLE" and payload["mode"] != "MODE_1":
        raise LedgerValidationError("Mode 1 route requires mode=MODE_1")
    _validate_scope(payload, strict=False)
    seen: set[str] = set()
    for index, change in enumerate(payload["changes"]):
        where = f"changes[{index}]"
        if not isinstance(change, dict):
            raise LedgerValidationError(f"{where}: must be an object")
        _require_keys(
            change,
            (
                "change_id",
                "type",
                "left_region",
                "right_region",
                "evidence",
                "address_hints",
                "confidence",
                "provenance",
            ),
            where,
        )
        change_id = str(change["change_id"])
        if not change_id or change_id in seen:
            raise LedgerValidationError(f"{where}.change_id: empty or duplicate")
        seen.add(change_id)
        if change["type"] not in CHANGE_TYPES:
            raise LedgerValidationError(f"{where}.type: unsupported")
        if change["confidence"] not in CONFIDENCE:
            raise LedgerValidationError(f"{where}.confidence: unsupported")
        if not isinstance(change["provenance"], list) or not change["provenance"]:
            raise LedgerValidationError(f"{where}.provenance: non-empty array required")
        if not set(change["provenance"]) <= PROVENANCE:
            raise LedgerValidationError(f"{where}.provenance: unsupported value")
        if not isinstance(change["evidence"], list) or not change["evidence"]:
            raise LedgerValidationError(f"{where}.evidence: non-empty array required")
        if not isinstance(change["address_hints"], list):
            raise LedgerValidationError(f"{where}.address_hints: array required")
        _validate_region(change["left_region"], f"{where}.left_region")
        _validate_region(change["right_region"], f"{where}.right_region")
    return payload


def _string_array(value: Any, where: str) -> list[str]:
    if (
        not isinstance(value, list)
        or any(not isinstance(item, str) or not item.strip() for item in value)
        or len(value) != len(set(value))
    ):
        raise LedgerValidationError(f"{where}: unique non-empty strings required")
    return value


def _validate_confidence_policy(payload: dict) -> Any:
    policy = payload["policy"]
    if not isinstance(policy, dict):
        raise LedgerValidationError("policy must be an object")
    _require_keys(policy, ("adapter", "confidence_mapping"), "policy")
    _reject_unknown_keys(policy, {"adapter", "confidence_mapping"}, "policy")
    adapter = policy["adapter"]
    if not isinstance(adapter, dict):
        raise LedgerValidationError("policy.adapter: object required")
    _require_keys(
        adapter,
        ("adapter_id", "adapter_version", "source_schema_version"),
        "policy.adapter",
    )
    _reject_unknown_keys(
        adapter,
        {"adapter_id", "adapter_version", "source_schema_version"},
        "policy.adapter",
    )
    if any(not isinstance(value, str) or not value for value in adapter.values()):
        raise LedgerValidationError("policy.adapter: non-empty strings required")
    mapping = policy["confidence_mapping"]
    if not isinstance(mapping, dict):
        raise LedgerValidationError("policy.confidence_mapping: object required")
    policy_id = mapping.get("policy_id")
    confidence_policy = confidence_policy_by_id(policy_id)
    if confidence_policy is None or mapping != confidence_policy.public_dict():
        raise LedgerValidationError("policy.confidence_mapping: unsupported policy")
    return confidence_policy


def _validate_grounding(item: Any, where: str) -> tuple[str, str]:
    if not isinstance(item, dict):
        raise LedgerValidationError(f"{where}: object required")
    required = {
        "kind",
        "id",
        "type",
        "bbox_visual_pt",
        "source_tokens",
        "confidence",
        "evidence",
    }
    _require_keys(item, required, where)
    _reject_unknown_keys(item, required, where)
    if not isinstance(item["kind"], str) or item["kind"] not in {"NODE", "EDGE"}:
        raise LedgerValidationError(f"{where}.kind: unsupported")
    for field in ("id", "type"):
        if not isinstance(item[field], str) or not item[field]:
            raise LedgerValidationError(f"{where}.{field}: non-empty string required")
    _validate_bbox(item["bbox_visual_pt"], f"{where}.bbox_visual_pt")
    tokens = _string_array(item["source_tokens"], f"{where}.source_tokens")
    if not tokens:
        raise LedgerValidationError(f"{where}.source_tokens: non-empty array required")
    if not _is_number(item["confidence"]) or not 0 <= item["confidence"] <= 1:
        raise LedgerValidationError(f"{where}.confidence: number in [0,1] required")
    if not isinstance(item["evidence"], list) or not item["evidence"]:
        raise LedgerValidationError(f"{where}.evidence: non-empty array required")
    return item["kind"], item["id"]


def _validate_evidence(item: Any, where: str) -> tuple[str, dict]:
    if not isinstance(item, dict):
        raise LedgerValidationError(f"{where}: object required")
    required = {
        "kind",
        "source_graph",
        "node_ids",
        "edge_ids",
        "source_tokens",
        "confidence",
        "graph_provenance",
        "grounding",
    }
    _require_keys(item, required, where)
    _reject_unknown_keys(item, required, where)
    if item["kind"] != "SYSTEM_GRAPH":
        raise LedgerValidationError(f"{where}.kind: SYSTEM_GRAPH required")
    source = item["source_graph"]
    if not isinstance(source, dict):
        raise LedgerValidationError(f"{where}.source_graph: object required")
    source_keys = {"side", "schema_version", "profile_id", "block_id", "page_index"}
    _require_keys(source, source_keys, f"{where}.source_graph")
    _reject_unknown_keys(source, source_keys, f"{where}.source_graph")
    if not isinstance(source["side"], str) or source["side"] not in {"LEFT", "RIGHT"}:
        raise LedgerValidationError(f"{where}.source_graph.side: unsupported")
    for field in ("schema_version", "profile_id", "block_id"):
        if not isinstance(source[field], str) or not source[field]:
            raise LedgerValidationError(f"{where}.source_graph.{field}: non-empty string required")
    if (
        not isinstance(source["page_index"], int)
        or isinstance(source["page_index"], bool)
        or source["page_index"] < 0
    ):
        raise LedgerValidationError(f"{where}.source_graph.page_index: invalid")
    node_ids = _string_array(item["node_ids"], f"{where}.node_ids")
    edge_ids = _string_array(item["edge_ids"], f"{where}.edge_ids")
    tokens = _string_array(item["source_tokens"], f"{where}.source_tokens")
    if not node_ids and not edge_ids:
        raise LedgerValidationError(f"{where}: node_ids or edge_ids required")
    if not tokens:
        raise LedgerValidationError(f"{where}.source_tokens: non-empty array required")
    if not _is_number(item["confidence"]) or not 0 <= item["confidence"] <= 1:
        raise LedgerValidationError(f"{where}.confidence: number in [0,1] required")
    if not isinstance(item["graph_provenance"], dict):
        raise LedgerValidationError(f"{where}.graph_provenance: object required")
    if not isinstance(item["grounding"], list) or not item["grounding"]:
        raise LedgerValidationError(f"{where}.grounding: non-empty array required")
    grounded_nodes: list[str] = []
    grounded_edges: list[str] = []
    for index, grounding in enumerate(item["grounding"]):
        kind, item_id = _validate_grounding(grounding, f"{where}.grounding[{index}]")
        (grounded_nodes if kind == "NODE" else grounded_edges).append(item_id)
    if grounded_nodes != node_ids or grounded_edges != edge_ids:
        raise LedgerValidationError(f"{where}.grounding: ids/order do not match evidence")
    return source["side"], source


def _validate_address_hint(item: Any, where: str) -> tuple[str, dict]:
    if not isinstance(item, dict):
        raise LedgerValidationError(f"{where}: object required")
    required = {
        "kind",
        "side",
        "block_id",
        "page_index",
        "bbox_visual_pt",
        "node_ids",
        "edge_ids",
    }
    _require_keys(item, required, where)
    _reject_unknown_keys(item, required, where)
    if (
        item["kind"] != "SYSTEM_GRAPH_LOCATION"
        or not isinstance(item["side"], str)
        or item["side"] not in {"LEFT", "RIGHT"}
    ):
        raise LedgerValidationError(f"{where}: invalid kind/side")
    _validate_region(
        {
            "block_id": item["block_id"],
            "page_index": item["page_index"],
            "bbox_visual_pt": item["bbox_visual_pt"],
        },
        where,
        strict=True,
    )
    _string_array(item["node_ids"], f"{where}.node_ids")
    _string_array(item["edge_ids"], f"{where}.edge_ids")
    return item["side"], item


def _validate_structural(change: dict, where: str) -> dict:
    structural = change["structural"]
    if not isinstance(structural, dict):
        raise LedgerValidationError(f"{where}.structural: object required")
    required = _STRUCTURAL_KEYS - {"equivalence"}
    _require_keys(structural, required, f"{where}.structural")
    _reject_unknown_keys(structural, _STRUCTURAL_KEYS, f"{where}.structural")
    if (
        not isinstance(structural["level"], str)
        or structural["level"] not in STRUCTURAL_LEVELS
    ):
        raise LedgerValidationError(f"{where}.structural.level: unsupported")
    if (
        not isinstance(structural["source_level"], str)
        or structural["source_level"] not in {"A", "B", "C"}
    ):
        raise LedgerValidationError(f"{where}.structural.source_level: unsupported")
    if structural["subject"] in (None, "", [], {}):
        raise LedgerValidationError(f"{where}.structural.subject: non-empty value required")
    for field in ("left_nodes", "right_nodes", "left_edges", "right_edges"):
        _string_array(structural[field], f"{where}.structural.{field}")
    if not isinstance(structural["relation"], dict):
        raise LedgerValidationError(f"{where}.structural.relation: object required")

    change_type = change["type"]
    left_nodes = structural["left_nodes"]
    right_nodes = structural["right_nodes"]
    left_edges = structural["left_edges"]
    right_edges = structural["right_edges"]
    expected_levels = {
        "SYSTEM_BACKBONE_CHANGED": "SYSTEM",
        "FUNCTIONAL_GROUP_CHANGED": "GROUP",
        "GROUP_COUNT_CHANGED": "GROUP",
        "NODE_ADDED": "NODE",
        "NODE_REMOVED": "NODE",
        "NODE_TYPE_CHANGED": "NODE",
        "NODE_PARAMETER_CHANGED": "NODE",
        "CONNECTION_CHANGED": "EDGE",
    }
    expected = expected_levels.get(change_type)
    if expected and structural["level"] != expected:
        raise LedgerValidationError(
            f"{where}.structural.level: {expected} required for {change_type}"
        )
    if change_type == "NODE_ADDED" and (left_nodes or not right_nodes):
        raise LedgerValidationError(f"{where}.structural: NODE_ADDED sides invalid")
    if change_type == "NODE_REMOVED" and (not left_nodes or right_nodes):
        raise LedgerValidationError(f"{where}.structural: NODE_REMOVED sides invalid")
    if change_type == "NODE_TYPE_CHANGED" and (not left_nodes or not right_nodes):
        raise LedgerValidationError(f"{where}.structural: NODE_TYPE_CHANGED needs both sides")
    # Изменение свойства говорит «у ЭТОГО аппарата стало другое значение»,
    # а значит обязано опираться на сопоставленную пару. Без одной из
    # сторон это утверждение не о свойстве, а о появлении или пропаже узла.
    if change_type == "NODE_PARAMETER_CHANGED" and (not left_nodes or not right_nodes):
        raise LedgerValidationError(
            f"{where}.structural: NODE_PARAMETER_CHANGED needs both sides"
        )
    if change_type == "CONNECTION_CHANGED" and not (left_edges or right_edges):
        raise LedgerValidationError(f"{where}.structural: CONNECTION_CHANGED needs an edge")
    if change_type == "GROUP_COUNT_CHANGED":
        relation = structural["relation"]
        if (
            not isinstance(relation.get("left_count"), int)
            or isinstance(relation.get("left_count"), bool)
            or not isinstance(relation.get("right_count"), int)
            or isinstance(relation.get("right_count"), bool)
            or relation["left_count"] == relation["right_count"]
        ):
            raise LedgerValidationError(f"{where}.structural.relation: unequal counts required")
    if change_type == "DETAIL_LEVEL_INCREASED":
        if structural.get("equivalence") != "representation_expansion":
            raise LedgerValidationError(
                f"{where}.structural.equivalence: representation_expansion required"
            )
        if not left_nodes or not right_nodes:
            raise LedgerValidationError(f"{where}.structural: detail expansion needs both sides")
    elif "equivalence" in structural:
        raise LedgerValidationError(f"{where}.structural.equivalence: detail type only")
    return structural


def _validate_mode2_change(
    change: Any,
    index: int,
    confidence_policy: Any,
    scope_by_side: dict[str, dict],
) -> tuple[str, str]:
    where = f"changes[{index}]"
    if not isinstance(change, dict):
        raise LedgerValidationError(f"{where}: must be an object")
    _require_keys(change, _MODE2_CHANGE_KEYS, where)
    _reject_unknown_keys(change, _MODE2_CHANGE_KEYS, where)
    if not isinstance(change["change_id"], str) or not change["change_id"].strip():
        raise LedgerValidationError(f"{where}.change_id: non-empty string required")
    if change["mode"] != "MODE_2":
        raise LedgerValidationError(f"{where}.mode: MODE_2 required")
    if (
        not isinstance(change["type"], str)
        or change["type"] not in STRUCTURAL_CHANGE_TYPES
    ):
        raise LedgerValidationError(f"{where}.type: unsupported")
    if not isinstance(change["summary"], str) or not change["summary"].strip():
        raise LedgerValidationError(f"{where}.summary: non-empty string required")
    raw = change["raw_confidence"]
    if not _is_number(raw) or not 0 <= raw <= 1:
        raise LedgerValidationError(f"{where}.raw_confidence: number in [0,1] required")
    if not isinstance(change["confidence"], str) or change["confidence"] not in CONFIDENCE:
        raise LedgerValidationError(f"{where}.confidence: unsupported")
    if change["mapped_confidence"] != change["confidence"]:
        raise LedgerValidationError(f"{where}.mapped_confidence: confidence mismatch")
    if confidence_policy.map(raw) != change["confidence"]:
        raise LedgerValidationError(f"{where}.confidence: policy mismatch")
    if not isinstance(change["provenance"], list) or not change["provenance"]:
        raise LedgerValidationError(f"{where}.provenance: non-empty array required")
    if (
        any(not isinstance(item, str) for item in change["provenance"])
        or not set(change["provenance"]) <= PROVENANCE
    ):
        raise LedgerValidationError(f"{where}.provenance: unsupported value")

    structural = _validate_structural(change, where)
    if not isinstance(change["evidence"], list) or not change["evidence"]:
        raise LedgerValidationError(f"{where}.evidence: non-empty array required")
    evidence_by_side: dict[str, tuple[dict, dict]] = {}
    for evidence_index, evidence in enumerate(change["evidence"]):
        side, source = _validate_evidence(
            evidence, f"{where}.evidence[{evidence_index}]"
        )
        if side in evidence_by_side:
            raise LedgerValidationError(f"{where}.evidence: duplicate {side} source")
        evidence_by_side[side] = (evidence, source)

    if not isinstance(change["address_hints"], list) or not change["address_hints"]:
        raise LedgerValidationError(f"{where}.address_hints: non-empty array required")
    hints_by_side: dict[str, dict] = {}
    for hint_index, hint in enumerate(change["address_hints"]):
        side, validated_hint = _validate_address_hint(
            hint, f"{where}.address_hints[{hint_index}]"
        )
        if side in hints_by_side:
            raise LedgerValidationError(f"{where}.address_hints: duplicate {side} hint")
        hints_by_side[side] = validated_hint

    for side, prefix in (("LEFT", "left"), ("RIGHT", "right")):
        structural_nodes = structural[f"{prefix}_nodes"]
        structural_edges = structural[f"{prefix}_edges"]
        has_references = bool(structural_nodes or structural_edges)
        if (
            has_references != (side in evidence_by_side)
            or has_references != (side in hints_by_side)
        ):
            raise LedgerValidationError(f"{where}: {side} evidence/address mismatch")
        region = change[f"{prefix}_region"]
        if has_references != (region is not None):
            raise LedgerValidationError(f"{where}.{prefix}_region: structural side mismatch")
        _validate_region(region, f"{where}.{prefix}_region", strict=True)
        if not has_references:
            continue
        evidence, source = evidence_by_side[side]
        hint = hints_by_side[side]
        if evidence["node_ids"] != structural_nodes or evidence["edge_ids"] != structural_edges:
            raise LedgerValidationError(f"{where}.evidence: {side} structural ids mismatch")
        scope = scope_by_side[side]
        for candidate in (source, hint, region):
            if (
                candidate["block_id"] != scope["block_id"]
                or candidate["page_index"] != scope["page_index"]
            ):
                raise LedgerValidationError(f"{where}: {side} block address mismatch")
        if hint["node_ids"] != structural_nodes or hint["edge_ids"] != structural_edges:
            raise LedgerValidationError(f"{where}.address_hints: {side} ids mismatch")

    try:
        signature = json.dumps(
            {"type": change["type"], "structural": structural},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as error:
        raise LedgerValidationError(f"{where}.structural: not JSON-compatible") from error
    return change["change_id"], signature


def _validate_mode2_conflicts(changes: list[dict]) -> None:
    added_right: set[str] = set()
    removed_left: set[str] = set()
    typed_left: set[str] = set()
    typed_right: set[str] = set()
    detail_left: set[str] = set()
    detail_right: set[str] = set()
    for change in changes:
        structural = change["structural"]
        if change["type"] == "NODE_ADDED":
            added_right.update(structural["right_nodes"])
        elif change["type"] == "NODE_REMOVED":
            removed_left.update(structural["left_nodes"])
        elif change["type"] == "NODE_TYPE_CHANGED":
            typed_left.update(structural["left_nodes"])
            typed_right.update(structural["right_nodes"])
        elif change["type"] == "DETAIL_LEVEL_INCREASED":
            detail_left.update(structural["left_nodes"])
            detail_right.update(structural["right_nodes"])
    if added_right & (typed_right | detail_right):
        raise LedgerValidationError("changes: right node has conflicting added/type/detail claims")
    if removed_left & (typed_left | detail_left):
        raise LedgerValidationError(
            "changes: left node has conflicting removed/type/detail claims"
        )


def _validate_v2(payload: dict) -> dict[str, Any]:
    _reject_unknown_keys(payload, _LEDGER_KEYS, "ledger")
    if not isinstance(payload["quality"], dict):
        raise LedgerValidationError("quality must be an object")
    if not isinstance(payload["diagnostics"], dict):
        raise LedgerValidationError("diagnostics must be an object")
    if payload["mode"] != "MODE_2":
        raise LedgerValidationError("v2 requires mode=MODE_2")
    if payload["route"] != "MODE_2_REQUIRED":
        raise LedgerValidationError("MODE_2 requires route=MODE_2_REQUIRED")
    _validate_scope(payload, strict=True)
    confidence_policy = _validate_confidence_policy(payload)
    scope_by_side = {
        "LEFT": payload["comparison_scope"]["left_blocks"][0],
        "RIGHT": payload["comparison_scope"]["right_blocks"][0],
    }
    seen_ids: set[str] = set()
    seen_signatures: set[str] = set()
    for index, change in enumerate(payload["changes"]):
        change_id, signature = _validate_mode2_change(
            change, index, confidence_policy, scope_by_side
        )
        if change_id in seen_ids:
            raise LedgerValidationError(f"changes[{index}].change_id: duplicate")
        if signature in seen_signatures:
            raise LedgerValidationError(f"changes[{index}]: duplicate structural claim")
        seen_ids.add(change_id)
        seen_signatures.add(signature)
    _validate_mode2_conflicts(payload["changes"])
    return payload


def validate_ledger(payload: Any) -> dict[str, Any]:
    """Validate v1 Mode 1 and v2 Mode 2 ledgers without mutating them."""
    if not isinstance(payload, dict):
        raise LedgerValidationError("ledger must be an object")
    _validate_common_envelope(payload)
    version = payload["schema_version"]
    if version == MODE1_SCHEMA_VERSION:
        return _validate_v1(payload)
    if version == MODE2_SCHEMA_VERSION:
        return _validate_v2(payload)
    raise LedgerValidationError("unsupported schema_version")


def schema_path(schema_version: str = SCHEMA_VERSION) -> Path:
    if schema_version == MODE1_SCHEMA_VERSION:
        return Path(__file__).with_name("graphic_change_ledger.schema.json")
    if schema_version == MODE2_SCHEMA_VERSION:
        return Path(__file__).with_name("graphic_change_ledger_v2.schema.json")
    raise LedgerValidationError("unsupported schema_version")


__all__ = [
    "CHANGE_TYPES",
    "CONFIDENCE",
    "LedgerValidationError",
    "MODE1_SCHEMA_VERSION",
    "MODE2_SCHEMA_VERSION",
    "PROVENANCE",
    "ROUTES",
    "SCHEMA_VERSION",
    "STRUCTURAL_CHANGE_TYPES",
    "STRUCTURAL_LEVELS",
    "SUPPORTED_SCHEMA_VERSIONS",
    "schema_path",
    "stable_id",
    "validate_ledger",
]

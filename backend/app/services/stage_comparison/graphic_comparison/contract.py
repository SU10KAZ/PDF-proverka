"""Compact common ledger contract shared by present Mode 1 and future Mode 2."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = "graphic-change-ledger.v1"
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
PROVENANCE = {"VECTOR", "VISION", "BOTH"}
CONFIDENCE = {"HIGH", "MEDIUM", "LOW"}


class LedgerValidationError(ValueError):
    pass


def stable_id(prefix: str, *parts: Any, length: int = 20) -> str:
    raw = json.dumps(parts, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return prefix + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


def _require_keys(value: dict, keys: Iterable[str], where: str) -> None:
    missing = [key for key in keys if key not in value]
    if missing:
        raise LedgerValidationError(f"{where}: missing {', '.join(missing)}")


def _validate_region(region: Any, where: str) -> None:
    if region is None:
        return
    if not isinstance(region, dict):
        raise LedgerValidationError(f"{where}: must be object or null")
    _require_keys(region, ("block_id", "page_index", "bbox_visual_pt"), where)
    bbox = region.get("bbox_visual_pt")
    if (
        not isinstance(bbox, list)
        or len(bbox) != 4
        or not all(isinstance(item, (int, float)) for item in bbox)
        or bbox[2] < bbox[0]
        or bbox[3] < bbox[1]
    ):
        raise LedgerValidationError(f"{where}.bbox_visual_pt: invalid bbox")


def validate_ledger(payload: Any) -> dict[str, Any]:
    """Validate the runtime contract without adding a JSON-schema dependency."""
    if not isinstance(payload, dict):
        raise LedgerValidationError("ledger must be an object")
    _require_keys(
        payload,
        ("schema_version", "comparison_scope", "route", "mode", "policy", "quality", "changes", "diagnostics"),
        "ledger",
    )
    if payload["schema_version"] != SCHEMA_VERSION:
        raise LedgerValidationError("unsupported schema_version")
    if payload["route"] not in ROUTES:
        raise LedgerValidationError("invalid route")
    if payload["mode"] not in {None, "MODE_1"}:
        raise LedgerValidationError("invalid mode")
    if payload["route"] == "MODE_1_APPLICABLE" and payload["mode"] != "MODE_1":
        raise LedgerValidationError("Mode 1 route requires mode=MODE_1")
    scope = payload["comparison_scope"]
    if not isinstance(scope, dict):
        raise LedgerValidationError("comparison_scope must be an object")
    _require_keys(scope, ("left_blocks", "right_blocks"), "comparison_scope")
    for side in ("left_blocks", "right_blocks"):
        if not isinstance(scope[side], list):
            raise LedgerValidationError(f"comparison_scope.{side} must be an array")
        for index, block in enumerate(scope[side]):
            if not isinstance(block, dict):
                raise LedgerValidationError(f"comparison_scope.{side}[{index}] must be an object")
            _require_keys(block, ("block_id", "page_index", "block_type", "bbox_visual_pt"), f"comparison_scope.{side}[{index}]")
    if not isinstance(payload["changes"], list):
        raise LedgerValidationError("changes must be an array")
    seen: set[str] = set()
    for index, change in enumerate(payload["changes"]):
        where = f"changes[{index}]"
        if not isinstance(change, dict):
            raise LedgerValidationError(f"{where}: must be an object")
        _require_keys(
            change,
            ("change_id", "type", "left_region", "right_region", "evidence", "address_hints", "confidence", "provenance"),
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


def schema_path() -> Path:
    return Path(__file__).with_name("graphic_change_ledger.schema.json")


__all__ = [
    "CHANGE_TYPES",
    "CONFIDENCE",
    "LedgerValidationError",
    "PROVENANCE",
    "ROUTES",
    "SCHEMA_VERSION",
    "schema_path",
    "stable_id",
    "validate_ledger",
]

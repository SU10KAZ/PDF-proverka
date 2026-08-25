"""Interop-only contract for TEXT/GRAPHIC evidence attached to one change.

This module deliberately does not correlate entities, merge claims, choose a
taxonomy, calculate a combined confidence, or produce a project summary.  It
only defines the lossless envelope that a future unified change layer can use
without modifying either source artifact.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = "unified-change-evidence.v1"
KIND = "unified_change_evidence"
EVIDENCE_SOURCES = frozenset({"TEXT", "GRAPHIC"})
AGGREGATE_SOURCES = frozenset({"TEXT", "GRAPHIC", "BOTH"})
CONFIDENCE_LEVELS = frozenset({"HIGH", "MEDIUM", "LOW", "UNKNOWN"})

TEXT_ARTIFACT_KIND = "stage_comparison_high_level_project_changes"
GRAPHIC_ARTIFACT_KIND = "graphic_change_ledger"
_SOURCE_ARTIFACTS = {
    "TEXT": {
        "kind": TEXT_ARTIFACT_KIND,
        "schema_versions": {"1.0"},
    },
    "GRAPHIC": {
        "kind": GRAPHIC_ARTIFACT_KIND,
        "schema_versions": {
            "graphic-change-ledger.v1",
            "graphic-change-ledger.v2",
        },
    },
}

_BUNDLE_KEYS = {
    "schema_version",
    "kind",
    "change_id",
    "source",
    "evidence_sources",
    "evidence",
}
_EVIDENCE_KEYS = {
    "evidence_id",
    "evidence_source",
    "source_artifact",
    "source_change_id",
    "provenance",
    "locations",
    "source_ids",
    "confidence",
}


class UnifiedEvidenceValidationError(ValueError):
    """The interop envelope is incomplete or contradicts its source labels."""


def _require_keys(value: dict, keys: Iterable[str], where: str) -> None:
    missing = sorted(key for key in keys if key not in value)
    if missing:
        raise UnifiedEvidenceValidationError(
            f"{where}: missing {', '.join(missing)}"
        )


def _reject_unknown_keys(value: dict, allowed: set[str], where: str) -> None:
    unknown = sorted(str(key) for key in set(value) - allowed)
    if unknown:
        raise UnifiedEvidenceValidationError(
            f"{where}: unknown {', '.join(unknown)}"
        )


def _nonempty_string(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise UnifiedEvidenceValidationError(f"{where}: non-empty string required")
    return value


def _string_array(value: Any, where: str, *, nonempty: bool = True) -> list[str]:
    if not isinstance(value, list):
        raise UnifiedEvidenceValidationError(f"{where}: array required")
    if any(not isinstance(item, str) or not item.strip() for item in value):
        raise UnifiedEvidenceValidationError(f"{where}: non-empty strings required")
    if len(value) != len(set(value)):
        raise UnifiedEvidenceValidationError(f"{where}: duplicate values")
    if nonempty and not value:
        raise UnifiedEvidenceValidationError(f"{where}: non-empty array required")
    return value


def _validate_source_artifact(value: Any, source: str, where: str) -> None:
    if not isinstance(value, dict):
        raise UnifiedEvidenceValidationError(f"{where}: object required")
    keys = {"kind", "schema_version"}
    _require_keys(value, keys, where)
    _reject_unknown_keys(value, keys, where)
    expected = _SOURCE_ARTIFACTS[source]
    if value["kind"] != expected["kind"]:
        raise UnifiedEvidenceValidationError(f"{where}.kind: source mismatch")
    if (
        not isinstance(value["schema_version"], str)
        or value["schema_version"] not in expected["schema_versions"]
    ):
        raise UnifiedEvidenceValidationError(
            f"{where}.schema_version: unsupported"
        )


def _validate_locations(value: Any, where: str) -> None:
    if not isinstance(value, list) or not value:
        raise UnifiedEvidenceValidationError(f"{where}: non-empty array required")
    for index, location in enumerate(value):
        item_where = f"{where}[{index}]"
        if not isinstance(location, dict):
            raise UnifiedEvidenceValidationError(f"{item_where}: object required")
        _require_keys(location, ("kind", "side"), item_where)
        _nonempty_string(location["kind"], f"{item_where}.kind")
        if (
            not isinstance(location["side"], str)
            or location["side"] not in {"LEFT", "RIGHT", "BOTH"}
        ):
            raise UnifiedEvidenceValidationError(f"{item_where}.side: unsupported")


def _validate_confidence(value: Any, where: str) -> None:
    if not isinstance(value, dict):
        raise UnifiedEvidenceValidationError(f"{where}: object required")
    keys = {"level", "raw", "source_scale"}
    _require_keys(value, keys, where)
    _reject_unknown_keys(value, keys, where)
    if (
        not isinstance(value["level"], str)
        or value["level"] not in CONFIDENCE_LEVELS
    ):
        raise UnifiedEvidenceValidationError(f"{where}.level: unsupported")
    raw = value["raw"]
    if raw is not None and (
        not isinstance(raw, (int, float))
        or isinstance(raw, bool)
        or not math.isfinite(float(raw))
        or not 0 <= raw <= 1
    ):
        raise UnifiedEvidenceValidationError(
            f"{where}.raw: null or finite number in [0,1] required"
        )
    _nonempty_string(value["source_scale"], f"{where}.source_scale")


def _validate_evidence(value: Any, index: int) -> tuple[str, str, str]:
    where = f"evidence[{index}]"
    if not isinstance(value, dict):
        raise UnifiedEvidenceValidationError(f"{where}: object required")
    _require_keys(value, _EVIDENCE_KEYS, where)
    _reject_unknown_keys(value, _EVIDENCE_KEYS, where)
    evidence_id = _nonempty_string(value["evidence_id"], f"{where}.evidence_id")
    source = value["evidence_source"]
    if not isinstance(source, str) or source not in EVIDENCE_SOURCES:
        raise UnifiedEvidenceValidationError(
            f"{where}.evidence_source: TEXT or GRAPHIC required"
        )
    _validate_source_artifact(
        value["source_artifact"], source, f"{where}.source_artifact"
    )
    source_change_id = _nonempty_string(
        value["source_change_id"], f"{where}.source_change_id"
    )
    if not isinstance(value["provenance"], dict) or not value["provenance"]:
        raise UnifiedEvidenceValidationError(
            f"{where}.provenance: non-empty object required"
        )
    _validate_locations(value["locations"], f"{where}.locations")
    _string_array(value["source_ids"], f"{where}.source_ids")
    _validate_confidence(value["confidence"], f"{where}.confidence")
    return evidence_id, source, source_change_id


def validate_unified_evidence_bundle(payload: Any) -> dict[str, Any]:
    """Validate a source-lossless evidence bundle without merging semantics."""
    if not isinstance(payload, dict):
        raise UnifiedEvidenceValidationError("bundle must be an object")
    _require_keys(payload, _BUNDLE_KEYS, "bundle")
    _reject_unknown_keys(payload, _BUNDLE_KEYS, "bundle")
    if payload["schema_version"] != SCHEMA_VERSION or payload["kind"] != KIND:
        raise UnifiedEvidenceValidationError("unsupported bundle contract")
    _nonempty_string(payload["change_id"], "bundle.change_id")
    if (
        not isinstance(payload["source"], str)
        or payload["source"] not in AGGREGATE_SOURCES
    ):
        raise UnifiedEvidenceValidationError("bundle.source: unsupported")
    declared_sources = _string_array(
        payload["evidence_sources"], "bundle.evidence_sources"
    )
    if any(source not in EVIDENCE_SOURCES for source in declared_sources):
        raise UnifiedEvidenceValidationError(
            "bundle.evidence_sources: TEXT/GRAPHIC only"
        )
    if not isinstance(payload["evidence"], list) or not payload["evidence"]:
        raise UnifiedEvidenceValidationError(
            "bundle.evidence: non-empty array required"
        )
    seen_ids: set[str] = set()
    seen_source_links: set[tuple[str, str]] = set()
    actual_sources: set[str] = set()
    for index, item in enumerate(payload["evidence"]):
        evidence_id, source, source_change_id = _validate_evidence(item, index)
        if evidence_id in seen_ids:
            raise UnifiedEvidenceValidationError(
                f"evidence[{index}].evidence_id: duplicate"
            )
        seen_ids.add(evidence_id)
        source_link = (source, source_change_id)
        if source_link in seen_source_links:
            raise UnifiedEvidenceValidationError(
                f"evidence[{index}].source_change_id: duplicate source link"
            )
        seen_source_links.add(source_link)
        actual_sources.add(source)
    canonical_sources = [
        source for source in ("TEXT", "GRAPHIC") if source in actual_sources
    ]
    if declared_sources != canonical_sources:
        raise UnifiedEvidenceValidationError(
            "bundle.evidence_sources: does not match evidence"
        )
    expected_source = (
        "BOTH" if actual_sources == EVIDENCE_SOURCES else canonical_sources[0]
    )
    if payload["source"] != expected_source:
        raise UnifiedEvidenceValidationError("bundle.source: does not match evidence")
    try:
        json.dumps(payload, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as error:
        raise UnifiedEvidenceValidationError("bundle: not JSON-compatible") from error
    return payload


def schema_path() -> Path:
    return Path(__file__).with_name("unified_change_evidence.schema.json")


__all__ = [
    "AGGREGATE_SOURCES",
    "CONFIDENCE_LEVELS",
    "EVIDENCE_SOURCES",
    "GRAPHIC_ARTIFACT_KIND",
    "KIND",
    "SCHEMA_VERSION",
    "TEXT_ARTIFACT_KIND",
    "UnifiedEvidenceValidationError",
    "schema_path",
    "validate_unified_evidence_bundle",
]

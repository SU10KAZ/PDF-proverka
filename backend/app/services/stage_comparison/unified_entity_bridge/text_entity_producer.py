"""Build stable TEXT_ENTITIES from ready Stage 5.3 evidence.

The producer is intentionally narrow.  It consumes only fields already present
in ``high_level_project_changes.json`` (plus an optional evidence-id keyed
index), recognises explicit project designations, and never opens a PDF or
invokes OCR, Vision, an LLM, or fuzzy matching.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

from backend.app.services.stage_comparison import high_level_project_changes

from .entity_normalizer import NORMALIZER_VERSION, canonical_entity_name


SCHEMA_VERSION = "text-entities.v1"
KIND = "stage_comparison_text_entities"
PRODUCER_VERSION = "stage5-3-text-entity-producer-v1"

ENTITY_TYPES = frozenset(
    {"SYSTEM", "EQUIPMENT", "FUNCTIONAL_NODE", "ROOM", "MATERIAL", "GROUP", "OTHER"}
)
CONFIDENCE_LEVELS = frozenset({"HIGH", "MEDIUM", "LOW", "UNKNOWN"})

_CHANGE_BUCKETS = (
    "high_level_changes",
    "detail_level_increased",
    "material_review",
    "non_material_review",
    "unresolved",
)
_TEXT_FIELDS = ("summary", "before", "after", "stage5_title")
_STRUCTURED_KEYS = ("entities", "designations", "subjects")
_NOISE_CANONICALS = frozenset(
    {
        "PROJECT",
        "ПРОЕКТ",
        "FLOOR",
        "ЭТАЖ",
        "SHEET",
        "ЛИСТ",
        "SYSTEM",
        "СИСТЕМА",
    }
)

# These patterns recognise explicit designations only.  They do not attempt to
# extract arbitrary noun phrases from prose.
_STRICT_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "ROOM_DESIGNATION",
        re.compile(
            r"(?<![A-ZА-ЯЁ0-9])помещени(?:е|я|ю)\s+"
            r"(?P<value>(?:\d{1,4}|[ЗСC])(?:[.]?[A-ZА-ЯЁ]+[.]?\d+)+|\d{1,4})"
            r"(?![A-ZА-ЯЁ0-9])",
            re.IGNORECASE,
        ),
    ),
    (
        "VRU_DESIGNATION",
        re.compile(
            r"(?<![A-ZА-ЯЁ0-9])(?P<value>(?:ВРУ|VRU)\s*[-‐‑‒–—]?\s*"
            r"(?:[A-ZА-ЯЁ]|\d{1,3}))(?![A-ZА-ЯЁ0-9])",
            re.IGNORECASE,
        ),
    ),
    (
        "PANEL_DESIGNATION",
        re.compile(
            r"(?<![A-ZА-ЯЁ0-9])(?P<value>(?:ЩР|SHR)[A-ZА-ЯЁ]?\s*[-‐‑‒–—]?\s*\d{1,4})"
            r"(?![A-ZА-ЯЁ0-9])",
            re.IGNORECASE,
        ),
    ),
    (
        "LOCAL_DEVICE_DESIGNATION",
        re.compile(
            r"(?<![A-ZА-ЯЁ0-9])(?P<value>\d{0,2}(?:QF|QS|FU|KM|KA|KT|SA|SB|HL|XT)"
            r"\s*[-‐‑‒–—]?\s*\d{1,4})(?![A-ZА-ЯЁ0-9])",
            re.IGNORECASE,
        ),
    ),
    (
        "MSB_DESIGNATION",
        re.compile(
            r"(?<![A-ZА-ЯЁ0-9])(?P<value>(?:ГРЩ|MSB)(?:\s+ТП)?)(?![A-ZА-ЯЁ0-9])",
            re.IGNORECASE,
        ),
    ),
)

_LOCAL_PREFIXES = frozenset({"QF", "QS", "FU", "KM", "KA", "KT", "SA", "SB", "HL", "XT"})


class TextEntityValidationError(ValueError):
    """The Stage 5.3 input or produced TEXT_ENTITIES artifact is invalid."""


def _digest(value: Any) -> str:
    raw = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _stable_id(prefix: str, *parts: Any) -> str:
    return prefix + _digest(parts)[:20]


def _unique_strings(values: Iterable[Any]) -> list[str]:
    output: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in output:
            output.append(text)
    return output


def _source_metadata(
    source: dict[str, Any], evidence_index: Any = None
) -> dict[str, Any]:
    evidence_index_version = None
    if isinstance(evidence_index, dict):
        raw_version = evidence_index.get("schema_version") or evidence_index.get(
            "version"
        )
        evidence_index_version = (
            str(raw_version).strip() if raw_version is not None else None
        )
    return {
        "kind": source.get("kind"),
        "schema_version": source.get("schema_version"),
        "version": source.get("version"),
        "pair_id": source.get("pair_id"),
        "source_signature": source.get("source_signature"),
        "artifact_digest": _digest(source),
        "evidence_index_digest": (
            _digest(evidence_index) if evidence_index is not None else None
        ),
        "evidence_index_version": evidence_index_version,
    }


def source_signature(source: dict[str, Any], evidence_index: Any = None) -> str:
    """Return the signature which makes an entity artifact stale-aware."""
    return _digest(
        {
            "producer_version": PRODUCER_VERSION,
            "normalizer_version": NORMALIZER_VERSION,
            "source_artifact": _source_metadata(source, evidence_index),
        }
    )


def is_stale(artifact: Any, source: Any, evidence_index: Any = None) -> bool:
    """Return true when Stage 5.3 or a producer/normalizer version changed."""
    if not isinstance(artifact, dict) or not isinstance(source, dict):
        return True
    try:
        return artifact.get("source_signature") != source_signature(
            source, evidence_index
        )
    except (TypeError, ValueError):
        return True


def _validate_source(source: Any) -> dict[str, Any]:
    if not isinstance(source, dict):
        raise TextEntityValidationError("Stage 5.3 artifact: object required")
    if (
        source.get("kind") != high_level_project_changes.KIND
        or source.get("schema_version") != high_level_project_changes.SCHEMA_VERSION
        or source.get("version") != high_level_project_changes.VERSION
    ):
        raise TextEntityValidationError("Stage 5.3 artifact: unsupported contract")
    if not isinstance(source.get("pair_id"), str) or not source["pair_id"].strip():
        raise TextEntityValidationError("Stage 5.3 artifact.pair_id: required")
    return source


def _details(source: dict[str, Any]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Return each atomic evidence once together with its first source change."""
    output: list[tuple[dict[str, Any], dict[str, Any]]] = []
    seen: set[str] = set()
    for bucket in _CHANGE_BUCKETS:
        for change in source.get(bucket) or []:
            if not isinstance(change, dict):
                continue
            for detail in change.get("details") or []:
                if not isinstance(detail, dict):
                    continue
                evidence_id = str(detail.get("evidence_id") or "").strip()
                if not evidence_id or evidence_id in seen:
                    continue
                seen.add(evidence_id)
                output.append((change, detail))
    service = source.get("service_structure_summary") or {}
    for change in service.get("items") or []:
        if not isinstance(change, dict):
            continue
        for detail in change.get("details") or []:
            if not isinstance(detail, dict):
                continue
            evidence_id = str(detail.get("evidence_id") or "").strip()
            if not evidence_id or evidence_id in seen:
                continue
            seen.add(evidence_id)
            output.append((change, detail))
    return output


def _index_by_evidence(value: Any) -> dict[str, dict[str, Any]]:
    if value is None:
        return {}
    if isinstance(value, dict) and isinstance(value.get("evidence"), list):
        value = value["evidence"]
    if isinstance(value, list):
        output = {}
        for item in value:
            if isinstance(item, dict) and str(item.get("evidence_id") or "").strip():
                output[str(item["evidence_id"])] = item
        return output
    if isinstance(value, dict):
        return {
            str(key): item
            for key, item in value.items()
            if isinstance(item, dict) and str(key).strip()
        }
    raise TextEntityValidationError("evidence index: mapping or array required")


def _entity_type(canonical: str, supplied: Any = None) -> tuple[str, str | None]:
    supplied_type = str(supplied or "").strip().upper()
    if supplied_type in ENTITY_TYPES:
        return supplied_type, None
    if supplied_type in {"PANEL", "DEVICE", "EQUIPMENT"}:
        return "EQUIPMENT", supplied_type
    if supplied_type in {"SPACE", "PREMISE", "ROOM"}:
        return "ROOM", supplied_type
    if supplied_type in {"MATERIAL", "PRODUCT"}:
        return "MATERIAL", supplied_type
    if supplied_type in {"SYSTEM", "NETWORK"}:
        return "SYSTEM", supplied_type
    if supplied_type in {"GROUP", "ASSEMBLY"}:
        return "GROUP", supplied_type
    if canonical.startswith("ROOM_"):
        return "ROOM", "ROOM"
    if canonical.startswith(("VRU_", "PANEL_")) or canonical in {"VRU", "PANEL"}:
        return "EQUIPMENT", "PANEL"
    if canonical.startswith("MSB"):
        return "SYSTEM", "MAIN_SWITCHBOARD"
    parts = [part for part in canonical.split("_") if part]
    family = (
        parts[1]
        if parts and parts[0].isdigit() and len(parts) > 1
        else (parts[0] if parts else "")
    )
    if family in _LOCAL_PREFIXES:
        return "EQUIPMENT", "SWITCHING_DEVICE"
    return "OTHER", supplied_type or None


def _context(*records: dict[str, Any]) -> tuple[str | None, dict[str, str]]:
    system_values: list[Any] = []
    parent: dict[str, str] = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        system_values.extend((record.get("system"), record.get("discipline")))
        nested = record.get("parent_context")
        if isinstance(nested, dict):
            for key in ("parent_group", "section", "room"):
                if nested.get(key) is not None:
                    parent.setdefault(key, str(nested[key]).strip())
        for key in ("parent_group", "section", "room"):
            if record.get(key) is not None:
                parent.setdefault(key, str(record[key]).strip())
    system = next((str(value).strip() for value in system_values if str(value or "").strip()), None)
    return system, {key: value for key, value in parent.items() if value}


def _structured_values(record: dict[str, Any]) -> Iterable[tuple[str, dict[str, Any]]]:
    for key in _STRUCTURED_KEYS:
        values = record.get(key)
        if values is None:
            continue
        if not isinstance(values, list):
            values = [values]
        for raw in values:
            if isinstance(raw, dict):
                name = raw.get("name") or raw.get("designation") or raw.get("label")
                if str(name or "").strip():
                    yield key, {**raw, "name": str(name).strip()}
            elif str(raw or "").strip():
                yield key, {"name": str(raw).strip()}


def _candidate(
    *,
    name: str,
    rule: str,
    change: dict[str, Any],
    detail: dict[str, Any],
    indexed: dict[str, Any],
    supplied: dict[str, Any] | None = None,
    field: str,
) -> dict[str, Any] | None:
    display = " ".join(str(name).split())
    canonical = canonical_entity_name(display)
    if not canonical or canonical in _NOISE_CANONICALS:
        return None
    supplied = supplied or {}
    entity_type, inferred_subtype = _entity_type(
        canonical, supplied.get("entity_type") or supplied.get("type")
    )
    if entity_type == "OTHER" and rule != "STRUCTURED_EVIDENCE":
        return None
    domain_subtype = str(supplied.get("domain_subtype") or inferred_subtype or "").strip() or None
    system, parent = _context(change, detail, indexed, supplied)
    evidence_id = str(detail["evidence_id"])
    fragment_ids = _unique_strings(
        [*(detail.get("left_fragment_ids") or []), *(detail.get("right_fragment_ids") or [])]
    )
    pages = sorted(
        {
            int(page)
            for page in [*(detail.get("left_pages") or []), *(detail.get("right_pages") or [])]
            if isinstance(page, int) and not isinstance(page, bool) and page >= 0
        }
    )
    sheet_groups = _unique_strings(
        [*(change.get("sheet_groups") or []), detail.get("group_id")]
    )
    return {
        "canonical_name": canonical,
        "display_name": display,
        "entity_type": entity_type,
        "domain_subtype": domain_subtype,
        "system": system,
        "parent_context": parent,
        "sheet_groups": sheet_groups,
        "pages": pages,
        "evidence_id": evidence_id,
        "fragment_ids": fragment_ids,
        "source_change_id": str(change.get("change_id") or "").strip(),
        "confidence": "HIGH" if entity_type != "OTHER" else "MEDIUM",
        "mention": {
            "evidence_id": evidence_id,
            "field": field,
            "rule": rule,
        },
    }


def _candidate_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        item["canonical_name"],
        item["entity_type"],
        item.get("domain_subtype"),
        canonical_entity_name(item.get("system")),
        tuple(
            sorted(
                (key, canonical_entity_name(value))
                for key, value in item["parent_context"].items()
            )
        ),
    )


def _is_context_ambiguous(item: dict[str, Any]) -> bool:
    parts = [part for part in item["canonical_name"].split("_") if part]
    family = (
        parts[1]
        if parts and parts[0].isdigit() and len(parts) > 1
        else (parts[0] if parts else "")
    )
    return family in _LOCAL_PREFIXES and not (
        item.get("system")
        or item["parent_context"].get("parent_group")
        or item["parent_context"].get("section")
    )


def build_text_entities(
    source: Any, evidence_index: Any = None
) -> dict[str, Any]:
    """Produce a deterministic TEXT_ENTITIES artifact from Stage 5.3."""
    source = _validate_source(source)
    external = _index_by_evidence(evidence_index)
    source_metadata = _source_metadata(source, evidence_index)
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    source_candidates = dropped_noise = 0

    for change, detail in _details(source):
        evidence_id = str(detail["evidence_id"])
        indexed = external.get(evidence_id, {})
        for record in (detail, indexed):
            for key, structured in _structured_values(record):
                source_candidates += 1
                item = _candidate(
                    name=structured["name"],
                    rule="STRUCTURED_EVIDENCE",
                    change=change,
                    detail=detail,
                    indexed=indexed,
                    supplied=structured,
                    field=key,
                )
                if item is None:
                    dropped_noise += 1
                else:
                    grouped[_candidate_key(item)].append(item)
        for field in _TEXT_FIELDS:
            text = detail.get(field)
            if not isinstance(text, str) or not text.strip():
                continue
            for rule, pattern in _STRICT_PATTERNS:
                for match in pattern.finditer(text):
                    source_candidates += 1
                    name = match.group("value")
                    if rule == "ROOM_DESIGNATION":
                        name = f"Помещение {name}"
                    item = _candidate(
                        name=name,
                        rule=rule,
                        change=change,
                        detail=detail,
                        indexed=indexed,
                        field=field,
                    )
                    if item is None:
                        dropped_noise += 1
                    else:
                        grouped[_candidate_key(item)].append(item)

    entities = []
    ambiguous = 0
    for key, mentions in sorted(grouped.items(), key=lambda item: repr(item[0])):
        template = mentions[0]
        evidence_ids = sorted({item["evidence_id"] for item in mentions})
        fragment_ids = sorted({value for item in mentions for value in item["fragment_ids"]})
        source_change_ids = sorted(
            {item["source_change_id"] for item in mentions if item["source_change_id"]}
        )
        entity_id = _stable_id(
            "txt_ent_",
            source["pair_id"],
            source.get("schema_version"),
            source.get("source_signature"),
            source_metadata["evidence_index_digest"],
            source_metadata["evidence_index_version"],
            key,
            evidence_ids,
        )
        entity = {
            "entity_id": entity_id,
            "canonical_name": template["canonical_name"],
            "display_names": sorted({item["display_name"] for item in mentions}),
            "entity_type": template["entity_type"],
            "domain_subtype": template["domain_subtype"],
            "system": template["system"],
            "parent_context": copy.deepcopy(template["parent_context"]),
            "sheet_groups": sorted({value for item in mentions for value in item["sheet_groups"]}),
            "pages": sorted({value for item in mentions for value in item["pages"]}),
            "evidence_ids": evidence_ids,
            "fragment_ids": fragment_ids,
            "source_change_ids": source_change_ids,
            "confidence": min(
                (item["confidence"] for item in mentions),
                key=("UNKNOWN", "LOW", "MEDIUM", "HIGH").index,
            ),
            "provenance": {
                "source_artifact_digest": _digest(source),
                "evidence_index_digest": source_metadata["evidence_index_digest"],
                "producer_version": PRODUCER_VERSION,
                "normalizer_version": NORMALIZER_VERSION,
                "mentions": sorted(
                    {tuple(item["mention"].values()) for item in mentions}
                ),
            },
        }
        entity["provenance"]["mentions"] = [
            {"evidence_id": item[0], "field": item[1], "rule": item[2]}
            for item in entity["provenance"]["mentions"]
        ]
        ambiguous += int(_is_context_ambiguous(entity))
        entities.append(entity)
    entities.sort(key=lambda item: item["entity_id"])
    produced = len(entities)
    artifact = {
        "schema_version": SCHEMA_VERSION,
        "kind": KIND,
        "producer_version": PRODUCER_VERSION,
        "normalizer_version": NORMALIZER_VERSION,
        "source_signature": source_signature(source, evidence_index),
        "source_artifact": source_metadata,
        "entities": entities,
        "quality_report": {
            "source_evidence": len(_details(source)),
            "total_source_candidates": source_candidates,
            "produced_entities": produced,
            "dropped_noise": dropped_noise,
            "ambiguous": ambiguous,
            "duplicated": max(0, source_candidates - dropped_noise - produced),
            "unresolved_context": ambiguous,
        },
    }
    return validate_text_entities(artifact)


def validate_text_entities(payload: Any) -> dict[str, Any]:
    """Validate the producer envelope, entity identity, and quality totals."""
    if not isinstance(payload, dict):
        raise TextEntityValidationError("TEXT_ENTITIES: object required")
    required = {
        "schema_version",
        "kind",
        "producer_version",
        "normalizer_version",
        "source_signature",
        "source_artifact",
        "entities",
        "quality_report",
    }
    if set(payload) != required:
        raise TextEntityValidationError("TEXT_ENTITIES: invalid envelope fields")
    if (
        payload["schema_version"] != SCHEMA_VERSION
        or payload["kind"] != KIND
        or payload["producer_version"] != PRODUCER_VERSION
        or payload["normalizer_version"] != NORMALIZER_VERSION
    ):
        raise TextEntityValidationError("TEXT_ENTITIES: unsupported contract")
    source = payload["source_artifact"]
    if not isinstance(source, dict) or set(source) != {
        "kind",
        "schema_version",
        "version",
        "pair_id",
        "source_signature",
        "artifact_digest",
        "evidence_index_digest",
        "evidence_index_version",
    }:
        raise TextEntityValidationError("TEXT_ENTITIES.source_artifact: invalid")
    expected_signature = _digest(
        {
            "producer_version": PRODUCER_VERSION,
            "normalizer_version": NORMALIZER_VERSION,
            "source_artifact": source,
        }
    )
    if payload["source_signature"] != expected_signature:
        raise TextEntityValidationError("TEXT_ENTITIES.source_signature: invalid")
    if not isinstance(payload["entities"], list):
        raise TextEntityValidationError("TEXT_ENTITIES.entities: array required")
    entity_ids: set[str] = set()
    for index, entity in enumerate(payload["entities"]):
        where = f"TEXT_ENTITIES.entities[{index}]"
        required_entity = {
            "entity_id",
            "canonical_name",
            "display_names",
            "entity_type",
            "domain_subtype",
            "system",
            "parent_context",
            "sheet_groups",
            "pages",
            "evidence_ids",
            "fragment_ids",
            "source_change_ids",
            "confidence",
            "provenance",
        }
        if not isinstance(entity, dict) or set(entity) != required_entity:
            raise TextEntityValidationError(f"{where}: invalid fields")
        entity_id = entity["entity_id"]
        if not isinstance(entity_id, str) or not entity_id.startswith("txt_ent_"):
            raise TextEntityValidationError(f"{where}.entity_id: invalid")
        if entity_id in entity_ids:
            raise TextEntityValidationError(f"{where}.entity_id: duplicate")
        entity_ids.add(entity_id)
        if (
            not isinstance(entity["canonical_name"], str)
            or not entity["canonical_name"]
            or canonical_entity_name(entity["canonical_name"]) != entity["canonical_name"]
        ):
            raise TextEntityValidationError(f"{where}.canonical_name: invalid")
        if entity["entity_type"] not in ENTITY_TYPES:
            raise TextEntityValidationError(f"{where}.entity_type: invalid")
        if entity["confidence"] not in CONFIDENCE_LEVELS:
            raise TextEntityValidationError(f"{where}.confidence: invalid")
        for key in (
            "display_names",
            "sheet_groups",
            "evidence_ids",
            "fragment_ids",
            "source_change_ids",
        ):
            values = entity[key]
            if (
                not isinstance(values, list)
                or (key in {"display_names", "evidence_ids"} and not values)
                or any(not isinstance(value, str) or not value for value in values)
                or len(values) != len(set(values))
            ):
                raise TextEntityValidationError(f"{where}.{key}: invalid")
        if not isinstance(entity["pages"], list) or any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in entity["pages"]
        ):
            raise TextEntityValidationError(f"{where}.pages: invalid")
        if not isinstance(entity["parent_context"], dict):
            raise TextEntityValidationError(f"{where}.parent_context: invalid")
        identity_key = (
            entity["canonical_name"],
            entity["entity_type"],
            entity.get("domain_subtype"),
            canonical_entity_name(entity.get("system")),
            tuple(
                sorted(
                    (key, canonical_entity_name(value))
                    for key, value in entity["parent_context"].items()
                )
            ),
        )
        expected_entity_id = _stable_id(
            "txt_ent_",
            source["pair_id"],
            source["schema_version"],
            source["source_signature"],
            source["evidence_index_digest"],
            source["evidence_index_version"],
            identity_key,
            entity["evidence_ids"],
        )
        if entity_id != expected_entity_id:
            raise TextEntityValidationError(f"{where}.entity_id: not stable")
        if not isinstance(entity["provenance"], dict) or not entity["provenance"]:
            raise TextEntityValidationError(f"{where}.provenance: invalid")
    quality = payload["quality_report"]
    quality_keys = {
        "source_evidence",
        "total_source_candidates",
        "produced_entities",
        "dropped_noise",
        "ambiguous",
        "duplicated",
        "unresolved_context",
    }
    if (
        not isinstance(quality, dict)
        or set(quality) != quality_keys
        or any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in quality.values()
        )
        or quality["produced_entities"] != len(payload["entities"])
    ):
        raise TextEntityValidationError("TEXT_ENTITIES.quality_report: invalid")
    try:
        json.dumps(payload, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as error:
        raise TextEntityValidationError("TEXT_ENTITIES: not JSON-compatible") from error
    return payload


def schema_path() -> Path:
    return Path(__file__).with_name("text_entities.schema.json")


__all__ = [
    "KIND",
    "PRODUCER_VERSION",
    "SCHEMA_VERSION",
    "TextEntityValidationError",
    "build_text_entities",
    "is_stale",
    "schema_path",
    "source_signature",
    "validate_text_entities",
]

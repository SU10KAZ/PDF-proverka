"""Evidence-first bridge between immutable TEXT and GRAPHIC entities.

The bridge answers only whether two supplied entity records can be linked.  It
does not extract entities from free text, use an LLM, merge changes, or mutate
either source artifact.  Ambiguity and explicit conflicts fail closed.
"""
from __future__ import annotations

import copy
import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

from backend.app.pipeline.stages.block_grounding.system_graph import (
    NODE_TYPES,
    validate_system_graph,
)

from .entity_normalizer import (
    NORMALIZER_VERSION,
    canonical_entity_name,
    normalize_entity_name,
    normalize_functional_role,
)


SCHEMA_VERSION = "entity-bridge.v1"
ARTIFACT_SCHEMA_VERSION = "entity-bridge.v2"
KIND = "text_graphic_entity_links"
BRIDGE_VERSION = "deterministic-entity-bridge-v1"

RELATIONS = frozenset({"SAME_ENTITY", "POSSIBLE_ENTITY", "UNKNOWN"})
CONFIDENCE_LEVELS = frozenset({"HIGH", "MEDIUM", "LOW", "UNKNOWN"})
EVIDENCE_OUTCOMES = frozenset({"MATCH", "CONFLICT", "AMBIGUITY"})

_LOCAL_DESIGNATION_FAMILIES = frozenset(
    {
        "QF",
        "QS",
        "FU",
        "KM",
        "KA",
        "KT",
        "SA",
        "SB",
        "HL",
        "XT",
    }
)
_DESIGNATION_FAMILIES = _LOCAL_DESIGNATION_FAMILIES | {
    "VRU",
    "PANEL",
    "ROOM",
    "MSB",
}
_KNOWN_FUNCTIONAL_ROLES = frozenset({"FIRE_PUMP"})
_CONTEXT_KEYS = ("discipline", "system", "parent_group", "sheet")
_CONFLICT_CONTEXT_KEYS = ("discipline", "system", "parent_group")
_STRONG_CONTEXT_KEYS = frozenset({"system", "parent_group"})

_BUNDLE_KEYS = {
    "schema_version",
    "kind",
    "bridge_version",
    "normalizer_version",
    "input_entity_ids",
    "links",
    "diagnostics",
}
_LINK_KEYS = {
    "entity_link_id",
    "text_entity_id",
    "graphic_entity_id",
    "relation",
    "confidence",
    "evidence",
}
_EVIDENCE_KEYS = {
    "rule",
    "level",
    "outcome",
    "tokens",
    "normalization",
    "context",
}
_NORMALIZATION_KEYS = {"source", "original", "canonical"}
_DIAGNOSTIC_KEYS = {
    "text_entity_count",
    "graphic_entity_count",
    "candidate_link_count",
    "relation_counts",
    "confidence_counts",
    "unresolved_text_entity_ids",
    "unresolved_graphic_entity_ids",
}


class BridgeValidationError(ValueError):
    """The bridge input/output is incomplete, ambiguous, or contradictory."""


def _stable_id(prefix: str, *parts: Any, length: int = 20) -> str:
    raw = json.dumps(parts, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return prefix + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


def _nonempty_string(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BridgeValidationError(f"{where}: non-empty string required")
    return value


def _string_array(value: Any, where: str, *, nonempty: bool = False) -> list[str]:
    if not isinstance(value, list):
        raise BridgeValidationError(f"{where}: array required")
    if nonempty and not value:
        raise BridgeValidationError(f"{where}: non-empty array required")
    if any(not isinstance(item, str) or not item.strip() for item in value):
        raise BridgeValidationError(f"{where}: non-empty strings required")
    if len(value) != len(set(value)):
        raise BridgeValidationError(f"{where}: duplicate values")
    return value


def _require_keys(value: dict, keys: Iterable[str], where: str) -> None:
    missing = sorted(key for key in keys if key not in value)
    if missing:
        raise BridgeValidationError(f"{where}: missing {', '.join(missing)}")


def _reject_unknown_keys(value: dict, keys: set[str], where: str) -> None:
    unknown = sorted(str(key) for key in set(value) - keys)
    if unknown:
        raise BridgeValidationError(f"{where}: unknown {', '.join(unknown)}")


def _values(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (set, frozenset)):
        return sorted(value, key=str)
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def _unique_normalizations(values: Iterable[Any], *, role: bool = False) -> list[dict]:
    output: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for value in values:
        item = normalize_functional_role(value) if role else normalize_entity_name(value)
        key = (item["original"], item["canonical"])
        if not item["canonical"] or key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


def _mapping_values(mapping: Any, *keys: str) -> list[Any]:
    if not isinstance(mapping, dict):
        return []
    output: list[Any] = []
    for key in keys:
        output.extend(_values(mapping.get(key)))
    return output


def _context_values(entity: dict, *, graphic: bool) -> dict[str, set[str]]:
    attributes = entity.get("attributes") if graphic else entity.get("values")
    graph_scope = entity.get("graph_scope") if graphic else None
    sources: dict[str, list[Any]] = {
        "discipline": [entity.get("discipline")],
        "system": [entity.get("system")],
        "parent_group": [entity.get("parent_group"), entity.get("parent_id")],
        "sheet": [entity.get("sheet")],
    }
    if graphic:
        sources["discipline"] += _mapping_values(attributes, "discipline")
        sources["discipline"] += _mapping_values(graph_scope, "discipline")
        sources["system"] += _mapping_values(attributes, "system")
        sources["system"] += _mapping_values(graph_scope, "system")
        sources["parent_group"] += _mapping_values(
            attributes, "parent_group", "parent_id", "section"
        )
        sources["parent_group"] += _mapping_values(
            graph_scope, "parent_group", "parent_id", "section"
        )
        sources["sheet"] += _mapping_values(graph_scope, "sheet", "sheet_id")
    else:
        sources["discipline"] += _mapping_values(attributes, "discipline")
        sources["system"] += _mapping_values(attributes, "system")
        sources["parent_group"] += _mapping_values(
            attributes, "parent_group", "parent_id", "section"
        )
        sources["sheet"] += _mapping_values(attributes, "sheet", "sheet_id")
    return {
        key: {
            canonical_entity_name(item)
            for value in values
            for item in _values(value)
            if canonical_entity_name(item)
        }
        for key, values in sources.items()
    }


def _explicit_roles(entity: dict, *, graphic: bool) -> list[dict]:
    attributes = entity.get("attributes") if graphic else entity.get("values")
    values = [entity.get("functional_role")]
    values += _mapping_values(attributes, "functional_role", "role")
    if graphic:
        canonical = canonical_entity_name(entity.get("canonical_identity"))
        if canonical in _KNOWN_FUNCTIONAL_ROLES:
            values.append(entity.get("canonical_identity"))
    return _unique_normalizations(values, role=True)


def _text_profile(entity: Any) -> dict:
    if not isinstance(entity, dict):
        raise BridgeValidationError("text entity: object required")
    entity_id = _nonempty_string(entity.get("id"), "text entity.id")
    name = _nonempty_string(entity.get("name"), f"text entity {entity_id}.name")
    identities = _unique_normalizations([name, entity.get("normalized_name")])
    if not identities:
        raise BridgeValidationError(f"text entity {entity_id}: canonical name required")
    entity_type = str(entity.get("type") or "").strip().upper()
    return {
        "id": entity_id,
        "identities": identities,
        "type": entity_type,
        "roles": _explicit_roles(entity, graphic=False),
        "context": _context_values(entity, graphic=False),
    }


def _graphic_labels(entity: dict) -> list[Any]:
    labels: list[Any] = []
    labels.extend(_values(entity.get("labels")))
    labels.extend(_values(entity.get("label")))
    labels.extend(_values(entity.get("display_label")))
    labels.extend(_mapping_values(entity.get("attributes"), "identity_set"))
    return labels


def _graphic_profile(entity: Any) -> dict:
    if not isinstance(entity, dict):
        raise BridgeValidationError("graphic entity: object required")
    entity_id = _nonempty_string(entity.get("id"), "graphic entity.id")
    node_type = str(entity.get("node_type") or entity.get("type") or "").strip().upper()
    if not node_type:
        raise BridgeValidationError(f"graphic entity {entity_id}.node_type: required")
    canonical = _unique_normalizations([entity.get("canonical_identity")])
    labels = _unique_normalizations(_graphic_labels(entity))
    conflicts = entity.get("conflicts") or []
    if not isinstance(conflicts, list):
        raise BridgeValidationError(f"graphic entity {entity_id}.conflicts: array required")
    return {
        "id": entity_id,
        "canonical": canonical,
        "labels": labels,
        "type": node_type,
        "roles": _explicit_roles(entity, graphic=True),
        "context": _context_values(entity, graphic=True),
        "has_identity_conflict": bool(conflicts),
    }


def _first_common(left: list[dict], right: list[dict]) -> tuple[dict, dict] | None:
    for left_item in left:
        for right_item in right:
            if left_item["canonical"] == right_item["canonical"]:
                return left_item, right_item
    return None


def _shared_context(text: dict, graphic: dict) -> dict[str, list[str]]:
    return {
        key: sorted(text["context"][key] & graphic["context"][key])
        for key in _CONTEXT_KEYS
        if text["context"][key] & graphic["context"][key]
    }


def _context_conflicts(text: dict, graphic: dict) -> dict[str, dict[str, list[str]]]:
    conflicts = {}
    for key in _CONFLICT_CONTEXT_KEYS:
        text_values = text["context"][key]
        graphic_values = graphic["context"][key]
        if text_values and graphic_values and not text_values & graphic_values:
            conflicts[key] = {
                "text": sorted(text_values),
                "graphic": sorted(graphic_values),
            }
    return conflicts


def _designation_parts(canonical: str) -> tuple[str, str] | None:
    parts = [part for part in canonical.split("_") if part]
    if len(parts) < 2:
        return None
    if parts[0].isdigit() and len(parts) >= 3:
        family = parts[1]
        identifier = "_".join([parts[0], *parts[2:]])
    else:
        family = parts[0]
        identifier = "_".join(parts[1:])
    if not identifier:
        return None
    return family, identifier


def _requires_context(canonical: str) -> bool:
    parts = _designation_parts(canonical)
    return bool(parts and parts[0] in _LOCAL_DESIGNATION_FAMILIES)


def _explicit_designation_conflict(text: dict, graphic: dict) -> tuple[dict, dict] | None:
    graph_values = [*graphic["canonical"], *graphic["labels"]]
    for text_item in text["identities"]:
        text_parts = _designation_parts(text_item["canonical"])
        if text_parts is None:
            continue
        for graph_item in graph_values:
            graph_parts = _designation_parts(graph_item["canonical"])
            if (
                graph_parts is not None
                and text_parts[0] in _DESIGNATION_FAMILIES
                and graph_parts[0] == text_parts[0]
                and graph_parts[1] != text_parts[1]
            ):
                return text_item, graph_item
    return None


def _normalization_evidence(
    text_item: dict | None, graphic_item: dict | None
) -> list[dict[str, str]]:
    output = []
    for source, item in (("TEXT", text_item), ("GRAPHIC", graphic_item)):
        if item is not None:
            output.append({"source": source, **item})
    return output


def _evidence(
    rule: str,
    *,
    level: int,
    outcome: str,
    text_item: dict | None = None,
    graphic_item: dict | None = None,
    context: dict | None = None,
    tokens: list[str] | None = None,
) -> dict:
    normalizations = _normalization_evidence(text_item, graphic_item)
    source_tokens = tokens or [item["original"] for item in normalizations]
    return {
        "rule": rule,
        "level": level,
        "outcome": outcome,
        "tokens": list(dict.fromkeys(str(item) for item in source_tokens if str(item))),
        "normalization": normalizations,
        "context": copy.deepcopy(context or {}),
    }


def _unknown_candidate(
    text: dict,
    graphic: dict,
    rule: str,
    *,
    text_item: dict | None,
    graphic_item: dict | None,
    context: dict | None = None,
) -> dict:
    priority = 1 if rule == "CANONICAL_IDENTITY_CONFLICT" else 4
    return {
        "text_id": text["id"],
        "graphic_id": graphic["id"],
        "rank": 0,
        "specificity": priority,
        "relation": "UNKNOWN",
        "confidence": "UNKNOWN",
        "evidence": [
            _evidence(
                rule,
                level=0,
                outcome="CONFLICT",
                text_item=text_item,
                graphic_item=graphic_item,
                context=context,
            )
        ],
    }


def _evaluate_pair(text: dict, graphic: dict) -> dict | None:
    canonical_match = _first_common(text["identities"], graphic["canonical"])
    label_match = _first_common(text["identities"], graphic["labels"])
    role_match = _first_common(text["roles"], graphic["roles"])
    identity_match = canonical_match or label_match
    context_match = _shared_context(text, graphic)
    context_conflict = _context_conflicts(text, graphic)
    designation_conflict = _explicit_designation_conflict(text, graphic)
    type_conflict = (
        text["type"] in NODE_TYPES
        and graphic["type"] in NODE_TYPES
        and text["type"] != graphic["type"]
    )

    if identity_match is not None:
        text_item, graphic_item = identity_match
        if type_conflict:
            return _unknown_candidate(
                text,
                graphic,
                "ENTITY_TYPE_CONFLICT",
                text_item=text_item,
                graphic_item=graphic_item,
                context={"text_type": text["type"], "graphic_type": graphic["type"]},
            )
        if graphic["has_identity_conflict"]:
            return _unknown_candidate(
                text,
                graphic,
                "GRAPHIC_IDENTITY_CONFLICT",
                text_item=text_item,
                graphic_item=graphic_item,
            )
        if context_conflict:
            return _unknown_candidate(
                text,
                graphic,
                "CONTEXT_CONFLICT",
                text_item=text_item,
                graphic_item=graphic_item,
                context=context_conflict,
            )
        strong_context = {
            key: value for key, value in context_match.items() if key in _STRONG_CONTEXT_KEYS
        }
        if _requires_context(text_item["canonical"]):
            if not strong_context:
                return _unknown_candidate(
                    text,
                    graphic,
                    "LOCAL_DESIGNATION_REQUIRES_CONTEXT",
                    text_item=text_item,
                    graphic_item=graphic_item,
                    context=context_match,
                )
            rule = "DESIGNATION_CONTEXT_MATCH"
            level = 3
            rank = 2
        elif canonical_match is not None:
            rule = "EXACT_CANONICAL_IDENTITY_MATCH"
            level = 1
            rank = 4
        else:
            rule = "NORMALIZED_DESIGNATION_MATCH"
            level = 2
            rank = 3
        evidence = [
            _evidence(
                rule,
                level=level,
                outcome="MATCH",
                text_item=text_item,
                graphic_item=graphic_item,
                context=context_match,
            )
        ]
        return {
            "text_id": text["id"],
            "graphic_id": graphic["id"],
            "rank": rank,
            "specificity": sum(len(value) for value in strong_context.values()),
            "relation": "SAME_ENTITY",
            "confidence": "HIGH",
            "evidence": evidence,
        }

    if role_match is not None:
        text_item, graphic_item = role_match
        if type_conflict:
            return _unknown_candidate(
                text,
                graphic,
                "ENTITY_TYPE_CONFLICT",
                text_item=text_item,
                graphic_item=graphic_item,
                context={"text_type": text["type"], "graphic_type": graphic["type"]},
            )
        if context_conflict:
            return _unknown_candidate(
                text,
                graphic,
                "CONTEXT_CONFLICT",
                text_item=text_item,
                graphic_item=graphic_item,
                context=context_conflict,
            )
        if graphic["has_identity_conflict"]:
            return _unknown_candidate(
                text,
                graphic,
                "GRAPHIC_IDENTITY_CONFLICT",
                text_item=text_item,
                graphic_item=graphic_item,
            )
        if designation_conflict is not None:
            return _unknown_candidate(
                text,
                graphic,
                "CANONICAL_IDENTITY_CONFLICT",
                text_item=designation_conflict[0],
                graphic_item=designation_conflict[1],
            )
        return {
            "text_id": text["id"],
            "graphic_id": graphic["id"],
            "rank": 1,
            "specificity": sum(
                len(value)
                for key, value in context_match.items()
                if key in _STRONG_CONTEXT_KEYS
            ),
            "relation": "POSSIBLE_ENTITY",
            "confidence": "MEDIUM",
            "evidence": [
                _evidence(
                    "FUNCTIONAL_ROLE_MATCH",
                    level=4,
                    outcome="MATCH",
                    text_item=text_item,
                    graphic_item=graphic_item,
                    context=context_match,
                )
            ],
        }

    if designation_conflict is not None:
        return _unknown_candidate(
            text,
            graphic,
            "CANONICAL_IDENTITY_CONFLICT",
            text_item=designation_conflict[0],
            graphic_item=designation_conflict[1],
        )
    return None


def _ambiguity_evidence(direction: str, text_ids: list[str], graphic_ids: list[str]) -> dict:
    return _evidence(
        "AMBIGUOUS_CARDINALITY",
        level=0,
        outcome="AMBIGUITY",
        tokens=[*text_ids, *graphic_ids],
        context={
            "direction": direction,
            "text_entity_ids": text_ids,
            "graphic_entity_ids": graphic_ids,
        },
    )


def _select_candidates(texts: list[dict], graphics: list[dict]) -> list[dict]:
    selected: list[dict] = []
    for text in texts:
        evaluated = [
            candidate
            for graphic in graphics
            if (candidate := _evaluate_pair(text, graphic)) is not None
        ]
        positives = [candidate for candidate in evaluated if candidate["rank"] > 0]
        if positives:
            best_score = max(
                (candidate["rank"], candidate["specificity"])
                for candidate in positives
            )
            best = [
                candidate
                for candidate in positives
                if (candidate["rank"], candidate["specificity"]) == best_score
            ]
            if len(best) > 1:
                graphic_ids = sorted(candidate["graphic_id"] for candidate in best)
                for candidate in best:
                    candidate["relation"] = "UNKNOWN"
                    candidate["confidence"] = "UNKNOWN"
                    candidate["evidence"].append(
                        _ambiguity_evidence(
                            "ONE_TEXT_TO_MULTIPLE_GRAPHIC",
                            [text["id"]],
                            graphic_ids,
                        )
                    )
            selected.extend(best)
        else:
            unknowns = [candidate for candidate in evaluated if candidate["rank"] == 0]
            if not unknowns:
                continue
            best_priority = max(candidate["specificity"] for candidate in unknowns)
            best = [
                candidate
                for candidate in unknowns
                if candidate["specificity"] == best_priority
            ]
            if len(best) > 1:
                graphic_ids = sorted(candidate["graphic_id"] for candidate in best)
                for candidate in best:
                    candidate["evidence"].append(
                        _ambiguity_evidence(
                            "ONE_TEXT_TO_MULTIPLE_GRAPHIC",
                            [text["id"]],
                            graphic_ids,
                        )
                    )
            selected.extend(best)

    by_graphic: dict[str, list[dict]] = defaultdict(list)
    for candidate in selected:
        if candidate["relation"] != "UNKNOWN":
            by_graphic[candidate["graphic_id"]].append(candidate)
    for graphic_id, candidates in by_graphic.items():
        if len(candidates) <= 1:
            continue
        text_ids = sorted(candidate["text_id"] for candidate in candidates)
        ambiguity = _ambiguity_evidence(
            "MULTIPLE_TEXT_TO_ONE_GRAPHIC", text_ids, [graphic_id]
        )
        for candidate in candidates:
            candidate["relation"] = "UNKNOWN"
            candidate["confidence"] = "UNKNOWN"
            candidate["evidence"].append(copy.deepcopy(ambiguity))
    return selected


def _public_link(candidate: dict) -> dict:
    return {
        "entity_link_id": _stable_id(
            "eln_", candidate["text_id"], candidate["graphic_id"]
        ),
        "text_entity_id": candidate["text_id"],
        "graphic_entity_id": candidate["graphic_id"],
        "relation": candidate["relation"],
        "confidence": candidate["confidence"],
        "evidence": candidate["evidence"],
    }


def _diagnostics(text_ids: list[str], graphic_ids: list[str], links: list[dict]) -> dict:
    resolved_text = {
        link["text_entity_id"] for link in links if link["relation"] == "SAME_ENTITY"
    }
    resolved_graphic = {
        link["graphic_entity_id"] for link in links if link["relation"] == "SAME_ENTITY"
    }
    return {
        "text_entity_count": len(text_ids),
        "graphic_entity_count": len(graphic_ids),
        "candidate_link_count": len(links),
        "relation_counts": {
            relation: sum(link["relation"] == relation for link in links)
            for relation in ("SAME_ENTITY", "POSSIBLE_ENTITY", "UNKNOWN")
        },
        "confidence_counts": {
            level: sum(link["confidence"] == level for link in links)
            for level in ("HIGH", "MEDIUM", "LOW", "UNKNOWN")
        },
        "unresolved_text_entity_ids": sorted(set(text_ids) - resolved_text),
        "unresolved_graphic_entity_ids": sorted(set(graphic_ids) - resolved_graphic),
    }


def build_entity_links(
    text_entities: Iterable[dict], graphic_entities: Iterable[dict]
) -> dict[str, Any]:
    """Build deterministic pair links while preserving source entity identities."""
    texts = [_text_profile(entity) for entity in list(text_entities)]
    graphics = [_graphic_profile(entity) for entity in list(graphic_entities)]
    text_ids = [entity["id"] for entity in texts]
    graphic_ids = [entity["id"] for entity in graphics]
    if len(text_ids) != len(set(text_ids)):
        raise BridgeValidationError("text entities: duplicate id")
    if len(graphic_ids) != len(set(graphic_ids)):
        raise BridgeValidationError("graphic entities: duplicate id")

    links = [
        _public_link(candidate)
        for candidate in _select_candidates(texts, graphics)
    ]
    links.sort(key=lambda link: (link["text_entity_id"], link["graphic_entity_id"]))
    payload = {
        "schema_version": SCHEMA_VERSION,
        "kind": KIND,
        "bridge_version": BRIDGE_VERSION,
        "normalizer_version": NORMALIZER_VERSION,
        "input_entity_ids": {"text": text_ids, "graphic": graphic_ids},
        "links": links,
        "diagnostics": _diagnostics(text_ids, graphic_ids, links),
    }
    return validate_entity_links(payload)


def _text_entities_from_artifact(artifact: dict[str, Any]) -> list[dict[str, Any]]:
    output = []
    for entity in artifact["entities"]:
        parent = entity.get("parent_context") or {}
        values = {
            "system": entity.get("system"),
            "parent_group": parent.get("parent_group"),
            "section": parent.get("section"),
            "room": parent.get("room"),
        }
        output.append(
            {
                "id": entity["entity_id"],
                "name": entity["display_names"][0],
                "normalized_name": entity["canonical_name"],
                "type": entity["entity_type"],
                "sheet": entity["sheet_groups"][0] if entity["sheet_groups"] else None,
                "page": entity["pages"][0] if entity["pages"] else None,
                "fragments": list(entity["fragment_ids"]),
                "values": {key: value for key, value in values.items() if value is not None},
            }
        )
    return output


def _graphic_entities_from_artifact(artifact: dict[str, Any]) -> list[dict[str, Any]]:
    output = []
    for entity in artifact["entities"]:
        parent = entity.get("parent_context") or {}
        attributes = {
            "functional_role": entity.get("functional_role"),
            "system": entity.get("system"),
            "parent_group": parent.get("parent_group"),
            "section": entity.get("section_context"),
        }
        output.append(
            {
                "id": entity["entity_id"],
                "node_type": entity["entity_type"],
                "canonical_identity": entity["canonical_name"],
                "labels": list(entity["display_labels"]),
                "attributes": {
                    key: value for key, value in attributes.items() if value is not None
                },
                "evidence": copy.deepcopy(entity["evidence_refs"]),
                "graph_scope": copy.deepcopy(entity["graph_scope"]),
                "conflicts": (
                    [{"kind": "GRAPH_ENTITY_IDENTITY_UNCERTAIN"}]
                    if entity["confidence"] == "UNKNOWN"
                    else []
                ),
            }
        )
    return output


def build_entity_links_from_artifacts(
    text_entities_artifact: Any,
    graph_entities_artifact: Any,
    *,
    current_stage53_artifact: Any = None,
    current_text_evidence_index: Any = None,
    current_system_graphs: Iterable[Any] | None = None,
) -> dict[str, Any]:
    """Run the production bridge from versioned entity artifacts.

    Optional current sources make the stale check explicit at the point of use.
    The low-level list API remains unchanged for G2.4.2 callers.
    """
    from .graph_entity_adapter import (
        is_stale as graph_entities_are_stale,
        validate_graph_entities,
    )
    from .text_entity_producer import (
        is_stale as text_entities_are_stale,
        validate_text_entities,
    )

    text_artifact = validate_text_entities(text_entities_artifact)
    graph_artifact = validate_graph_entities(graph_entities_artifact)
    if current_stage53_artifact is not None and text_entities_are_stale(
        text_artifact, current_stage53_artifact, current_text_evidence_index
    ):
        raise BridgeValidationError("TEXT_ENTITIES stale for current Stage 5.3")
    if current_system_graphs is not None and graph_entities_are_stale(
        graph_artifact, list(current_system_graphs)
    ):
        raise BridgeValidationError("GRAPH_ENTITIES stale for current SYSTEM_GRAPH")

    low_level = build_entity_links(
        _text_entities_from_artifact(text_artifact),
        _graphic_entities_from_artifact(graph_artifact),
    )
    payload = {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "kind": KIND,
        "bridge_version": BRIDGE_VERSION,
        "normalizer_version": NORMALIZER_VERSION,
        "source_signatures": {
            "text_entities": text_artifact["source_signature"],
            "graph_entities": graph_artifact["source_signature"],
        },
        "input_artifacts": {
            "text": {
                "kind": text_artifact["kind"],
                "schema_version": text_artifact["schema_version"],
            },
            "graphic": {
                "kind": graph_artifact["kind"],
                "schema_version": graph_artifact["schema_version"],
            },
        },
        "input_entity_ids": low_level["input_entity_ids"],
        "links": low_level["links"],
        "diagnostics": low_level["diagnostics"],
    }
    return validate_entity_links_artifact(payload)


def entity_links_are_stale(
    links_artifact: Any,
    text_entities_artifact: Any,
    graph_entities_artifact: Any,
) -> bool:
    """Return true unless links target the exact current entity artifacts."""
    if not all(
        isinstance(value, dict)
        for value in (links_artifact, text_entities_artifact, graph_entities_artifact)
    ):
        return True
    return (
        links_artifact.get("schema_version") != ARTIFACT_SCHEMA_VERSION
        or links_artifact.get("bridge_version") != BRIDGE_VERSION
        or links_artifact.get("normalizer_version") != NORMALIZER_VERSION
        or links_artifact.get("source_signatures")
        != {
            "text_entities": text_entities_artifact.get("source_signature"),
            "graph_entities": graph_entities_artifact.get("source_signature"),
        }
    )


def graphic_entities_from_system_graph(graph: Any) -> list[dict[str, Any]]:
    """Adapt valid SYSTEM_GRAPH nodes without changing or interpreting the graph."""
    validation = validate_system_graph(graph)
    if not validation["valid"]:
        raise BridgeValidationError(
            "SYSTEM_GRAPH invalid: " + ", ".join(validation["errors"])
        )
    block = graph.get("block")
    if not isinstance(block, dict):
        raise BridgeValidationError("SYSTEM_GRAPH block: object required")
    profile = graph.get("profile")
    scope = {
        "schema_version": graph["schema_version"],
        "profile_id": graph.get("profile_id")
        or (profile.get("id") if isinstance(profile, dict) else None),
        "block_id": block.get("block_id"),
        "page_index": block.get("page_index"),
        "discipline": graph.get("discipline"),
    }
    output = []
    for node in graph["nodes"]:
        attributes = copy.deepcopy(node.get("attrs") or {})
        if not isinstance(attributes, dict):
            raise BridgeValidationError(
                f"SYSTEM_GRAPH node {node['id']}.attrs: object required"
            )
        if node.get("section") is not None:
            attributes.setdefault("section", node["section"])
        labels = []
        for value in (
            node.get("label"),
            node.get("display_label"),
            *_values(attributes.get("identity_set")),
        ):
            value = str(value or "").strip()
            if value and value not in labels:
                labels.append(value)
        output.append(
            {
                "id": str(node["id"]),
                "node_type": node["type"],
                "canonical_identity": node.get("canonical_identity"),
                "labels": labels,
                "attributes": attributes,
                "evidence": copy.deepcopy(node["evidence"]),
                "graph_scope": copy.deepcopy(scope),
                "conflicts": copy.deepcopy(node.get("conflicts") or []),
            }
        )
    return output


def _validate_evidence(value: Any, where: str) -> None:
    if not isinstance(value, dict):
        raise BridgeValidationError(f"{where}: object required")
    _require_keys(value, _EVIDENCE_KEYS, where)
    _reject_unknown_keys(value, _EVIDENCE_KEYS, where)
    _nonempty_string(value["rule"], f"{where}.rule")
    if (
        not isinstance(value["level"], int)
        or isinstance(value["level"], bool)
        or value["level"] not in {0, 1, 2, 3, 4}
    ):
        raise BridgeValidationError(f"{where}.level: integer 0..4 required")
    if value["outcome"] not in EVIDENCE_OUTCOMES:
        raise BridgeValidationError(f"{where}.outcome: unsupported")
    _string_array(value["tokens"], f"{where}.tokens", nonempty=True)
    if not isinstance(value["normalization"], list):
        raise BridgeValidationError(f"{where}.normalization: array required")
    for index, normalized in enumerate(value["normalization"]):
        item_where = f"{where}.normalization[{index}]"
        if not isinstance(normalized, dict):
            raise BridgeValidationError(f"{item_where}: object required")
        _require_keys(normalized, _NORMALIZATION_KEYS, item_where)
        _reject_unknown_keys(normalized, _NORMALIZATION_KEYS, item_where)
        if normalized["source"] not in {"TEXT", "GRAPHIC"}:
            raise BridgeValidationError(f"{item_where}.source: unsupported")
        _nonempty_string(normalized["original"], f"{item_where}.original")
        _nonempty_string(normalized["canonical"], f"{item_where}.canonical")
    normalized_sources = [item["source"] for item in value["normalization"]]
    if len(normalized_sources) != len(set(normalized_sources)):
        raise BridgeValidationError(f"{where}.normalization: duplicate source")
    if value["outcome"] != "AMBIGUITY" and set(normalized_sources) != {
        "TEXT",
        "GRAPHIC",
    }:
        raise BridgeValidationError(
            f"{where}.normalization: both sources required"
        )
    if not isinstance(value["context"], dict):
        raise BridgeValidationError(f"{where}.context: object required")


def _expected_diagnostics(input_ids: dict, links: list[dict]) -> dict:
    return _diagnostics(input_ids["text"], input_ids["graphic"], links)


def validate_entity_links(payload: Any) -> dict[str, Any]:
    """Validate the versioned entity-link envelope and diagnostic invariants."""
    if not isinstance(payload, dict):
        raise BridgeValidationError("entity links: object required")
    _require_keys(payload, _BUNDLE_KEYS, "entity links")
    _reject_unknown_keys(payload, _BUNDLE_KEYS, "entity links")
    if (
        payload["schema_version"] != SCHEMA_VERSION
        or payload["kind"] != KIND
        or payload["bridge_version"] != BRIDGE_VERSION
        or payload["normalizer_version"] != NORMALIZER_VERSION
    ):
        raise BridgeValidationError("entity links: unsupported contract")
    input_ids = payload["input_entity_ids"]
    if not isinstance(input_ids, dict) or set(input_ids) != {"text", "graphic"}:
        raise BridgeValidationError("input_entity_ids: text/graphic object required")
    text_ids = set(_string_array(input_ids["text"], "input_entity_ids.text"))
    graphic_ids = set(_string_array(input_ids["graphic"], "input_entity_ids.graphic"))
    if not isinstance(payload["links"], list):
        raise BridgeValidationError("links: array required")
    seen_ids: set[str] = set()
    seen_pairs: set[tuple[str, str]] = set()
    positive_by_text: dict[str, set[str]] = defaultdict(set)
    positive_by_graphic: dict[str, set[str]] = defaultdict(set)
    for index, link in enumerate(payload["links"]):
        where = f"links[{index}]"
        if not isinstance(link, dict):
            raise BridgeValidationError(f"{where}: object required")
        _require_keys(link, _LINK_KEYS, where)
        _reject_unknown_keys(link, _LINK_KEYS, where)
        link_id = _nonempty_string(link["entity_link_id"], f"{where}.entity_link_id")
        if link_id in seen_ids:
            raise BridgeValidationError(f"{where}.entity_link_id: duplicate")
        seen_ids.add(link_id)
        text_id = _nonempty_string(link["text_entity_id"], f"{where}.text_entity_id")
        graphic_id = _nonempty_string(
            link["graphic_entity_id"], f"{where}.graphic_entity_id"
        )
        if text_id not in text_ids or graphic_id not in graphic_ids:
            raise BridgeValidationError(f"{where}: source entity reference missing")
        if link_id != _stable_id("eln_", text_id, graphic_id):
            raise BridgeValidationError(f"{where}.entity_link_id: not stable for pair")
        pair = (text_id, graphic_id)
        if pair in seen_pairs:
            raise BridgeValidationError(f"{where}: duplicate entity pair")
        seen_pairs.add(pair)
        if link["relation"] not in RELATIONS:
            raise BridgeValidationError(f"{where}.relation: unsupported")
        if link["confidence"] not in CONFIDENCE_LEVELS:
            raise BridgeValidationError(f"{where}.confidence: unsupported")
        if link["relation"] == "SAME_ENTITY" and link["confidence"] != "HIGH":
            raise BridgeValidationError(f"{where}: SAME_ENTITY must be HIGH")
        if link["relation"] == "POSSIBLE_ENTITY" and link["confidence"] not in {
            "MEDIUM",
            "LOW",
        }:
            raise BridgeValidationError(
                f"{where}: POSSIBLE_ENTITY must be MEDIUM/LOW"
            )
        if link["relation"] == "UNKNOWN" and link["confidence"] != "UNKNOWN":
            raise BridgeValidationError(f"{where}: UNKNOWN confidence required")
        if link["relation"] != "UNKNOWN":
            positive_by_text[text_id].add(graphic_id)
            positive_by_graphic[graphic_id].add(text_id)
        if not isinstance(link["evidence"], list) or not link["evidence"]:
            raise BridgeValidationError(f"{where}.evidence: non-empty array required")
        for evidence_index, evidence in enumerate(link["evidence"]):
            _validate_evidence(evidence, f"{where}.evidence[{evidence_index}]")
        outcomes = {item["outcome"] for item in link["evidence"]}
        if link["relation"] in {"SAME_ENTITY", "POSSIBLE_ENTITY"} and outcomes != {
            "MATCH"
        }:
            raise BridgeValidationError(f"{where}.evidence: positive link conflict")
        if link["relation"] == "UNKNOWN" and not outcomes & {
            "CONFLICT",
            "AMBIGUITY",
        }:
            raise BridgeValidationError(f"{where}.evidence: UNKNOWN reason required")
    if any(len(ids) > 1 for ids in positive_by_text.values()):
        raise BridgeValidationError("links: positive one-to-many cardinality")
    if any(len(ids) > 1 for ids in positive_by_graphic.values()):
        raise BridgeValidationError("links: positive many-to-one cardinality")
    if not isinstance(payload["diagnostics"], dict):
        raise BridgeValidationError("diagnostics: object required")
    _require_keys(payload["diagnostics"], _DIAGNOSTIC_KEYS, "diagnostics")
    _reject_unknown_keys(payload["diagnostics"], _DIAGNOSTIC_KEYS, "diagnostics")
    if payload["diagnostics"] != _expected_diagnostics(input_ids, payload["links"]):
        raise BridgeValidationError("diagnostics: does not match links")
    try:
        json.dumps(payload, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as error:
        raise BridgeValidationError("entity links: not JSON-compatible") from error
    return payload


def validate_entity_links_artifact(payload: Any) -> dict[str, Any]:
    """Validate the artifact-to-artifact v2 envelope and v1 matcher payload."""
    if not isinstance(payload, dict):
        raise BridgeValidationError("entity links artifact: object required")
    required = {
        "schema_version",
        "kind",
        "bridge_version",
        "normalizer_version",
        "source_signatures",
        "input_artifacts",
        "input_entity_ids",
        "links",
        "diagnostics",
    }
    if set(payload) != required:
        raise BridgeValidationError("entity links artifact: invalid envelope fields")
    if (
        payload["schema_version"] != ARTIFACT_SCHEMA_VERSION
        or payload["kind"] != KIND
        or payload["bridge_version"] != BRIDGE_VERSION
        or payload["normalizer_version"] != NORMALIZER_VERSION
    ):
        raise BridgeValidationError("entity links artifact: unsupported contract")
    signatures = payload["source_signatures"]
    if (
        not isinstance(signatures, dict)
        or set(signatures) != {"text_entities", "graph_entities"}
        or any(
            not isinstance(value, str) or len(value) != 64
            for value in signatures.values()
        )
    ):
        raise BridgeValidationError("entity links artifact.source_signatures: invalid")
    artifacts = payload["input_artifacts"]
    if not isinstance(artifacts, dict) or set(artifacts) != {"text", "graphic"}:
        raise BridgeValidationError("entity links artifact.input_artifacts: invalid")
    expected_artifacts = {
        "text": {
            "kind": "stage_comparison_text_entities",
            "schema_version": "text-entities.v1",
        },
        "graphic": {
            "kind": "system_graph_entities",
            "schema_version": "graph-entities.v1",
        },
    }
    if artifacts != expected_artifacts:
        raise BridgeValidationError("entity links artifact.input_artifacts: unsupported")
    legacy = {
        "schema_version": SCHEMA_VERSION,
        "kind": KIND,
        "bridge_version": BRIDGE_VERSION,
        "normalizer_version": NORMALIZER_VERSION,
        "input_entity_ids": payload["input_entity_ids"],
        "links": payload["links"],
        "diagnostics": payload["diagnostics"],
    }
    validate_entity_links(legacy)
    try:
        json.dumps(payload, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as error:
        raise BridgeValidationError("entity links artifact: not JSON-compatible") from error
    return payload


def schema_path() -> Path:
    return Path(__file__).with_name("entity_links.schema.json")


def artifact_schema_path() -> Path:
    return Path(__file__).with_name("entity_links_v2.schema.json")


__all__ = [
    "ARTIFACT_SCHEMA_VERSION",
    "BRIDGE_VERSION",
    "BridgeValidationError",
    "CONFIDENCE_LEVELS",
    "KIND",
    "RELATIONS",
    "SCHEMA_VERSION",
    "build_entity_links",
    "build_entity_links_from_artifacts",
    "entity_links_are_stale",
    "graphic_entities_from_system_graph",
    "artifact_schema_path",
    "schema_path",
    "validate_entity_links",
    "validate_entity_links_artifact",
]

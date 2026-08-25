"""Build and query an evidence-backed graphic coverage manifest.

Coverage describes what the existing graphic route actually checked.  It does
not infer a change, re-run comparison, or combine TEXT and GRAPHIC conclusions.
"""
from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .comparison_scope import (
    SCOPE_JOIN_VERSION,
    normalize_graphic_scope_groups,
    scope_join_is_stale,
    validate_scope_join,
)
from .graphic_coverage_policy import (
    DIMENSIONS,
    MODE2_OBSERVABLE_DIMENSIONS,
    POLICY_VERSION,
    public_policy,
)
from .side_entity_contract import (
    SIDES,
    SIDE_BRIDGE_VERSION,
    side_entity_links_are_stale,
    validate_side_entity_links,
    validate_side_graph_entities,
)
from .text_entity_producer import validate_text_entities


SCHEMA_VERSION = "graphic-coverage.v1"
KIND = "stage_comparison_graphic_coverage"
COVERAGE_BUILDER_VERSION = "graphic-coverage-builder-v1"
COVERAGE_STATES = frozenset(
    {"CHECKED", "NOT_CHECKED", "CHECK_BLOCKED", "NOT_APPLICABLE"}
)
SUBJECT_KINDS = frozenset({"SCOPE", "TEXT_ENTITY", "GRAPH_ENTITY"})


class GraphicCoverageValidationError(ValueError):
    """Coverage sources or records are malformed, stale, or contradictory."""


def _digest(value: Any) -> str:
    raw = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _stable_id(prefix: str, *parts: Any) -> str:
    return prefix + _digest(parts)[:20]


def _raw_pair_lookup(
    graphic_scope_groups: Any,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    normalized = normalize_graphic_scope_groups(graphic_scope_groups)
    raw_by_digests: dict[tuple[str, str | None], list[dict[str, Any]]] = defaultdict(list)
    for group in graphic_scope_groups:
        for pair in group["block_pairs"]:
            raw_by_digests[
                (
                    _digest(pair["ledger"]),
                    _digest(pair["comparison_result"])
                    if pair["comparison_result"] is not None
                    else None,
                )
            ].append(pair)
    output: dict[str, dict[str, Any]] = {}
    for group in normalized:
        for pair in group["block_pairs"]:
            key = (pair["ledger_digest"], pair["comparison_digest"])
            matches = raw_by_digests.get(key) or []
            if len(matches) != 1:
                raise GraphicCoverageValidationError(
                    "graphic scope block pair must map to one source artifact pair"
                )
            output[pair["block_pair_ref"]] = matches[0]
    return normalized, output


def _source_artifacts(
    scope_join: dict[str, Any],
    text_entities: dict[str, Any],
    side_graph_entities: dict[str, Any],
    side_entity_links: dict[str, Any],
    graphic_groups: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "stage53": scope_join["source_artifacts"]["stage53"],
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
        "side_entity_links": {
            "schema_version": side_entity_links["schema_version"],
            "source_signature": side_entity_links["source_signature"],
        },
        "scope_join": {
            "schema_version": scope_join["schema_version"],
            "source_signature": scope_join["source_signature"],
        },
        "graphic_scope_groups": graphic_groups,
    }


def _coverage_signature(sources: dict[str, Any]) -> str:
    return _digest(
        {
            "coverage_builder_version": COVERAGE_BUILDER_VERSION,
            "coverage_policy_version": POLICY_VERSION,
            "scope_join_version": SCOPE_JOIN_VERSION,
            "side_bridge_version": SIDE_BRIDGE_VERSION,
            "source_artifacts": sources,
        }
    )


def _record(
    *,
    scope_ref: str,
    subject_kind: str,
    subject_id: str | None,
    dimension: str,
    side: str,
    state: str,
    reason_codes: list[str],
    source_refs: dict[str, Any],
) -> dict[str, Any]:
    if not reason_codes:
        raise GraphicCoverageValidationError("coverage reason_codes: non-empty required")
    return {
        "coverage_id": _stable_id(
            "coverage_", scope_ref, subject_kind, subject_id, dimension, side
        ),
        "scope_ref": scope_ref,
        "subject": {"kind": subject_kind, "id": subject_id},
        "dimension": dimension,
        "side": side,
        "state": state,
        "reason_codes": sorted(set(reason_codes)),
        "source_refs": source_refs,
    }


def _quality(raw_pair: dict[str, Any]) -> tuple[bool, list[str], dict[str, Any]]:
    comparison = raw_pair.get("comparison_result")
    ledger = raw_pair["ledger"]
    quality = (
        comparison.get("comparison_quality")
        if isinstance(comparison, dict)
        else (ledger.get("quality") or {}).get("comparison")
    )
    if not isinstance(quality, dict):
        return False, ["comparison_quality_missing"], {}
    blocked = [str(item) for item in quality.get("blocked_changes_reason") or []]
    if quality.get("certain_changes_allowed") is not True and not blocked:
        blocked = ["certain_changes_not_allowed"]
    return not blocked and quality.get("certain_changes_allowed") is True, blocked, quality


def _scope_dimension_state(
    scope: dict[str, Any], dimension: str, raw_pairs: dict[str, dict[str, Any]]
) -> tuple[str, list[str]]:
    if dimension not in MODE2_OBSERVABLE_DIMENSIONS:
        return "NOT_APPLICABLE", ["dimension_not_observable_by_system_graph_mode2"]
    if scope["status"] != "RESOLVED":
        reason = (
            "no_system_graph_for_sheet"
            if "no_graphic_scope_group_on_canonical_pages" in scope["reason_codes"]
            else "scope_join_unresolved"
        )
        return "NOT_CHECKED", [reason, *scope["reason_codes"]]
    children = scope["child_block_scopes"]
    modes = [child["block_pair"]["mode"] for child in children]
    if not any(mode == "MODE_2" for mode in modes):
        return "NOT_APPLICABLE", ["mode1_local_graphic_delta_not_semantic_coverage"]
    if any(mode != "MODE_2" for mode in modes):
        return "NOT_CHECKED", ["sheet_scope_not_fully_covered_by_mode2"]
    blocked: list[str] = []
    for child in children:
        passed, reasons, _ = _quality(raw_pairs[child["block_pair"]["block_pair_ref"]])
        if not passed:
            blocked.extend(reasons)
    if blocked:
        return "CHECK_BLOCKED", ["comparison_quality_gate_blocked", *blocked]
    return "CHECKED", ["mode2_scope_compared_with_quality_gate_passed"]


def _match_sets(comparison: dict[str, Any], side: str) -> tuple[set[str], set[str]]:
    matching = comparison.get("matching") or {}
    side_key = "left_id" if side == "LEFT" else "right_id"
    high = {
        str(item[side_key])
        for item in matching.get("matches") or []
        if isinstance(item, dict)
        and item.get("decision") == "HIGH_MATCH"
        and str(item.get(side_key) or "").strip()
    }
    detail_key = "left_nodes" if side == "LEFT" else "right_nodes"
    for item in matching.get("detail_matches") or []:
        if isinstance(item, dict) and float(item.get("match_confidence") or 0.0) >= 0.85:
            high.update(str(node) for node in item.get(detail_key) or [] if str(node))
    ambiguous_key = "ambiguous_left_ids" if side == "LEFT" else "ambiguous_right_ids"
    ambiguous = {str(node) for node in matching.get(ambiguous_key) or [] if str(node)}
    return high, ambiguous


def _graph_entity_records(
    scope: dict[str, Any],
    dimension: str,
    side_graph_entities: dict[str, Any],
    raw_pairs: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    records = []
    if scope["graphic_scope_group"] is None:
        return records
    for side in SIDES:
        side_key = side.lower()
        pair_by_block = {
            child["block_pair"][side_key]["block_id"]: child
            for child in scope["child_block_scopes"]
        }
        for entity in side_graph_entities["sides"][side]["entities"]:
            block_id = entity["graph_scope"]["block_id"]
            child = pair_by_block.get(block_id)
            if child is None:
                continue
            pair_ref = child["block_pair"]["block_pair_ref"]
            raw = raw_pairs[pair_ref]
            refs = {
                "block_scope_refs": [child["scope_ref"]],
                "block_pair_refs": [pair_ref],
                "ledger_digests": [child["block_pair"]["ledger_digest"]],
                "comparison_digests": (
                    [child["block_pair"]["comparison_digest"]]
                    if child["block_pair"]["comparison_digest"] is not None
                    else []
                ),
                "graph_node_ids": entity["graph_node_ids"],
                "entity_link_ids": [],
            }
            if dimension not in MODE2_OBSERVABLE_DIMENSIONS:
                state, reasons = "NOT_APPLICABLE", [
                    "dimension_not_observable_by_system_graph_mode2"
                ]
            elif child["block_pair"]["mode"] != "MODE_2":
                state, reasons = "NOT_APPLICABLE", [
                    "mode1_local_graphic_delta_not_semantic_coverage"
                ]
            elif scope["status"] != "RESOLVED":
                state, reasons = "NOT_CHECKED", ["scope_join_unresolved"]
            else:
                passed, blocked, _ = _quality(raw)
                if not passed:
                    state, reasons = "CHECK_BLOCKED", [
                        "comparison_quality_gate_blocked",
                        *blocked,
                    ]
                else:
                    comparison = raw["comparison_result"]
                    high, ambiguous = _match_sets(comparison, side)
                    node_ids = set(entity["graph_node_ids"])
                    if node_ids & ambiguous:
                        state, reasons = "NOT_CHECKED", [
                            "comparator_identity_ambiguous_for_graph_entity"
                        ]
                    elif node_ids and node_ids <= high:
                        state, reasons = "CHECKED", [
                            "all_graph_entity_nodes_high_matched_by_comparator"
                        ]
                    else:
                        state, reasons = "NOT_CHECKED", [
                            "graph_entity_not_fully_high_matched_by_comparator"
                        ]
            records.append(
                _record(
                    scope_ref=scope["scope_ref"],
                    subject_kind="GRAPH_ENTITY",
                    subject_id=entity["entity_id"],
                    dimension=dimension,
                    side=side,
                    state=state,
                    reason_codes=reasons,
                    source_refs=refs,
                )
            )
    return records


def _text_side_record(
    *,
    scope: dict[str, Any],
    text_entity_id: str,
    dimension: str,
    side: str,
    scope_state: tuple[str, list[str]],
    side_links: dict[str, Any],
    graph_records: dict[tuple[str, str, str, str], dict[str, Any]],
) -> dict[str, Any]:
    candidates = [
        item
        for item in side_links["sides"][side]["links"]
        if item["text_entity_id"] == text_entity_id
    ]
    high = [
        item
        for item in candidates
        if item["relation"] == "SAME_ENTITY" and item["confidence"] == "HIGH"
    ]
    refs = {
        "block_scope_refs": [],
        "block_pair_refs": [],
        "ledger_digests": [],
        "comparison_digests": [],
        "graph_node_ids": [],
        "entity_link_ids": sorted(item["entity_link_id"] for item in candidates),
    }
    if dimension not in MODE2_OBSERVABLE_DIMENSIONS:
        state, reasons = "NOT_APPLICABLE", [
            "dimension_not_observable_by_system_graph_mode2"
        ]
    elif scope_state[0] in {"CHECK_BLOCKED", "NOT_APPLICABLE"}:
        state, reasons = scope_state
    elif scope["status"] != "RESOLVED":
        state, reasons = "NOT_CHECKED", ["scope_join_unresolved"]
    elif not high:
        state = "NOT_CHECKED"
        reasons = [
            "side_entity_link_ambiguous"
            if candidates
            else "no_high_side_entity_link"
        ]
    else:
        linked_graph_records = [
            graph_records.get(
                (
                    scope["scope_ref"],
                    link["graphic_entity_id"],
                    dimension,
                    side,
                )
            )
            for link in high
        ]
        for key in (
            "block_scope_refs",
            "block_pair_refs",
            "ledger_digests",
            "comparison_digests",
            "graph_node_ids",
        ):
            refs[key] = sorted(
                {
                    value
                    for item in linked_graph_records
                    if item is not None
                    for value in item["source_refs"][key]
                }
            )
        checked = [item for item in linked_graph_records if item and item["state"] == "CHECKED"]
        blocked = [
            item for item in linked_graph_records if item and item["state"] == "CHECK_BLOCKED"
        ]
        if checked:
            state, reasons = "CHECKED", [
                "high_side_entity_link_to_high_comparator_identity"
            ]
            refs["entity_link_ids"] = sorted(
                link["entity_link_id"]
                for link in high
                if any(
                    item
                    and item["subject"]["id"] == link["graphic_entity_id"]
                    and item["state"] == "CHECKED"
                    for item in linked_graph_records
                )
            )
        elif blocked:
            state, reasons = "CHECK_BLOCKED", ["linked_graph_entity_check_blocked"]
        else:
            state, reasons = "NOT_CHECKED", [
                "linked_graph_entity_not_reliably_covered"
            ]
    return _record(
        scope_ref=scope["scope_ref"],
        subject_kind="TEXT_ENTITY",
        subject_id=text_entity_id,
        dimension=dimension,
        side=side,
        state=state,
        reason_codes=reasons,
        source_refs=refs,
    )


def _combine_text_sides(
    left: dict[str, Any], right: dict[str, Any]
) -> tuple[str, list[str]]:
    states = {left["state"], right["state"]}
    if states == {"CHECKED"}:
        return "CHECKED", ["subject_checked_on_left_and_right"]
    if "CHECK_BLOCKED" in states:
        return "CHECK_BLOCKED", ["subject_check_blocked_on_at_least_one_side"]
    if states == {"NOT_APPLICABLE"}:
        return "NOT_APPLICABLE", ["dimension_not_observable_on_either_side"]
    return "NOT_CHECKED", ["subject_not_reliably_checked_on_both_sides"]


def build_graphic_coverage(
    stage53: Any,
    text_entities: Any,
    side_graph_entities: Any,
    side_entity_links: Any,
    scope_join: Any,
    graphic_scope_groups: Any,
) -> dict[str, Any]:
    text = validate_text_entities(text_entities)
    graphics = validate_side_graph_entities(side_graph_entities)
    links = validate_side_entity_links(side_entity_links)
    scopes_artifact = validate_scope_join(scope_join)
    normalized_groups, raw_pairs = _raw_pair_lookup(graphic_scope_groups)
    if scope_join_is_stale(
        scopes_artifact, stage53, text, graphics, graphic_scope_groups
    ):
        raise GraphicCoverageValidationError("scope join stale for coverage sources")
    if side_entity_links_are_stale(links, text, graphics):
        raise GraphicCoverageValidationError("side entity links stale for coverage sources")

    records: list[dict[str, Any]] = []
    for scope in scopes_artifact["scopes"]:
        scope_states: dict[str, tuple[str, list[str]]] = {}
        for dimension in DIMENSIONS:
            state = _scope_dimension_state(scope, dimension, raw_pairs)
            scope_states[dimension] = state
            records.append(
                _record(
                    scope_ref=scope["scope_ref"],
                    subject_kind="SCOPE",
                    subject_id=None,
                    dimension=dimension,
                    side="BOTH",
                    state=state[0],
                    reason_codes=state[1],
                    source_refs={
                        "block_scope_refs": [
                            child["scope_ref"]
                            for child in scope["child_block_scopes"]
                        ],
                        "block_pair_refs": [
                            child["block_pair"]["block_pair_ref"]
                            for child in scope["child_block_scopes"]
                        ],
                        "ledger_digests": [
                            child["block_pair"]["ledger_digest"]
                            for child in scope["child_block_scopes"]
                        ],
                        "comparison_digests": [
                            child["block_pair"]["comparison_digest"]
                            for child in scope["child_block_scopes"]
                            if child["block_pair"]["comparison_digest"] is not None
                        ],
                        "graph_node_ids": [],
                        "entity_link_ids": [],
                    },
                )
            )
        graph_for_scope: list[dict[str, Any]] = []
        for dimension in DIMENSIONS:
            graph_for_scope.extend(
                _graph_entity_records(scope, dimension, graphics, raw_pairs)
            )
        records.extend(graph_for_scope)
        graph_index = {
            (
                item["scope_ref"],
                item["subject"]["id"],
                item["dimension"],
                item["side"],
            ): item
            for item in graph_for_scope
        }
        text_scope = scope.get("text_scope") or {}
        for text_entity_id in text_scope.get("text_entity_ids") or []:
            for dimension in DIMENSIONS:
                per_side = {
                    side: _text_side_record(
                        scope=scope,
                        text_entity_id=text_entity_id,
                        dimension=dimension,
                        side=side,
                        scope_state=scope_states[dimension],
                        side_links=links,
                        graph_records=graph_index,
                    )
                    for side in SIDES
                }
                records.extend(per_side.values())
                combined_state, combined_reasons = _combine_text_sides(
                    per_side["LEFT"], per_side["RIGHT"]
                )
                records.append(
                    _record(
                        scope_ref=scope["scope_ref"],
                        subject_kind="TEXT_ENTITY",
                        subject_id=text_entity_id,
                        dimension=dimension,
                        side="BOTH",
                        state=combined_state,
                        reason_codes=combined_reasons,
                        source_refs={
                            key: sorted(
                                set(
                                    per_side["LEFT"]["source_refs"][key]
                                    + per_side["RIGHT"]["source_refs"][key]
                                )
                            )
                            for key in (
                                "block_scope_refs",
                                "block_pair_refs",
                                "ledger_digests",
                                "comparison_digests",
                                "graph_node_ids",
                                "entity_link_ids",
                            )
                        },
                    )
                )

    records.sort(key=lambda item: item["coverage_id"])
    sources = _source_artifacts(scopes_artifact, text, graphics, links, normalized_groups)
    state_counts = Counter(item["state"] for item in records)
    subject_counts = Counter(item["subject"]["kind"] for item in records)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "kind": KIND,
        "builder_version": COVERAGE_BUILDER_VERSION,
        "coverage_policy": public_policy(),
        "versions": {
            "scope_join": SCOPE_JOIN_VERSION,
            "side_bridge": SIDE_BRIDGE_VERSION,
        },
        "source_signature": _coverage_signature(sources),
        "source_artifacts": sources,
        "coverage": records,
        "summary": {
            "records": len(records),
            "by_state": {state: state_counts.get(state, 0) for state in sorted(COVERAGE_STATES)},
            "by_subject_kind": {
                kind: subject_counts.get(kind, 0) for kind in sorted(SUBJECT_KINDS)
            },
        },
    }
    return validate_graphic_coverage(payload)


def validate_graphic_coverage(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict) or set(payload) != {
        "schema_version",
        "kind",
        "builder_version",
        "coverage_policy",
        "versions",
        "source_signature",
        "source_artifacts",
        "coverage",
        "summary",
    }:
        raise GraphicCoverageValidationError("graphic coverage: invalid envelope")
    if (
        payload["schema_version"] != SCHEMA_VERSION
        or payload["kind"] != KIND
        or payload["builder_version"] != COVERAGE_BUILDER_VERSION
        or (payload.get("coverage_policy") or {}).get("version") != POLICY_VERSION
        or payload.get("versions")
        != {"scope_join": SCOPE_JOIN_VERSION, "side_bridge": SIDE_BRIDGE_VERSION}
    ):
        raise GraphicCoverageValidationError("graphic coverage: unsupported contract")
    if payload["source_signature"] != _coverage_signature(payload["source_artifacts"]):
        raise GraphicCoverageValidationError("graphic coverage.source_signature: invalid")
    if not isinstance(payload["coverage"], list):
        raise GraphicCoverageValidationError("graphic coverage.coverage: array required")
    ids: set[str] = set()
    keys: set[tuple[Any, ...]] = set()
    for item in payload["coverage"]:
        if not isinstance(item, dict) or set(item) != {
            "coverage_id",
            "scope_ref",
            "subject",
            "dimension",
            "side",
            "state",
            "reason_codes",
            "source_refs",
        }:
            raise GraphicCoverageValidationError("graphic coverage record: invalid fields")
        subject = item["subject"]
        key = (
            item["scope_ref"],
            subject.get("kind") if isinstance(subject, dict) else None,
            subject.get("id") if isinstance(subject, dict) else None,
            item["dimension"],
            item["side"],
        )
        if (
            not isinstance(item["coverage_id"], str)
            or item["coverage_id"] in ids
            or key in keys
            or not isinstance(subject, dict)
            or set(subject) != {"kind", "id"}
            or subject["kind"] not in SUBJECT_KINDS
            or item["dimension"] not in DIMENSIONS
            or item["side"] not in {*SIDES, "BOTH"}
            or item["state"] not in COVERAGE_STATES
            or not isinstance(item["reason_codes"], list)
            or not item["reason_codes"]
        ):
            raise GraphicCoverageValidationError("graphic coverage record: invalid value")
        refs = item["source_refs"]
        expected_ref_keys = {
            "block_scope_refs",
            "block_pair_refs",
            "ledger_digests",
            "comparison_digests",
            "graph_node_ids",
            "entity_link_ids",
        }
        if not isinstance(refs, dict) or set(refs) != expected_ref_keys:
            raise GraphicCoverageValidationError(
                "graphic coverage record.source_refs: invalid fields"
            )
        for values in refs.values():
            if (
                not isinstance(values, list)
                or any(not isinstance(value, str) or not value for value in values)
                or len(values) != len(set(values))
            ):
                raise GraphicCoverageValidationError(
                    "graphic coverage record.source_refs: invalid references"
                )
        ids.add(item["coverage_id"])
        keys.add(key)
    state_counts = Counter(item["state"] for item in payload["coverage"])
    subject_counts = Counter(item["subject"]["kind"] for item in payload["coverage"])
    expected = {
        "records": len(payload["coverage"]),
        "by_state": {state: state_counts.get(state, 0) for state in sorted(COVERAGE_STATES)},
        "by_subject_kind": {
            kind: subject_counts.get(kind, 0) for kind in sorted(SUBJECT_KINDS)
        },
    }
    if payload["summary"] != expected:
        raise GraphicCoverageValidationError("graphic coverage.summary: invalid")
    return payload


def graphic_coverage_is_stale(
    artifact: Any,
    stage53: Any,
    text_entities: Any,
    side_graph_entities: Any,
    side_entity_links: Any,
    scope_join: Any,
    graphic_scope_groups: Any,
) -> bool:
    try:
        validated = validate_graphic_coverage(artifact)
        text = validate_text_entities(text_entities)
        graphics = validate_side_graph_entities(side_graph_entities)
        links = validate_side_entity_links(side_entity_links)
        scopes = validate_scope_join(scope_join)
        normalized, _ = _raw_pair_lookup(graphic_scope_groups)
    except (GraphicCoverageValidationError, TypeError, ValueError):
        return True
    if scope_join_is_stale(scopes, stage53, text, graphics, graphic_scope_groups):
        return True
    if side_entity_links_are_stale(links, text, graphics):
        return True
    sources = _source_artifacts(scopes, text, graphics, links, normalized)
    return validated["source_signature"] != _coverage_signature(sources)


def coverage(
    manifest: Any,
    scope_ref: str,
    subject_id: str | None,
    dimension: str,
    side: str | None = None,
) -> dict[str, Any]:
    """Query ``coverage(scope, subject, dimension, side?)`` deterministically."""
    payload = validate_graphic_coverage(manifest)
    if dimension not in DIMENSIONS:
        raise GraphicCoverageValidationError("coverage dimension: unsupported")
    requested_side = side or "BOTH"
    matches = [
        item
        for item in payload["coverage"]
        if item["scope_ref"] == scope_ref
        and item["subject"]["id"] == subject_id
        and item["dimension"] == dimension
        and item["side"] == requested_side
    ]
    if not matches and side is None:
        matches = [
            item
            for item in payload["coverage"]
            if item["scope_ref"] == scope_ref
            and item["subject"]["id"] == subject_id
            and item["dimension"] == dimension
        ]
    if len(matches) != 1:
        raise GraphicCoverageValidationError(
            "coverage query must resolve exactly once; specify side when needed"
        )
    return matches[0]


def schema_path() -> Path:
    return Path(__file__).with_name("graphic_coverage.schema.json")


__all__ = [
    "COVERAGE_BUILDER_VERSION",
    "COVERAGE_STATES",
    "GraphicCoverageValidationError",
    "KIND",
    "SCHEMA_VERSION",
    "build_graphic_coverage",
    "coverage",
    "graphic_coverage_is_stale",
    "schema_path",
    "validate_graphic_coverage",
]

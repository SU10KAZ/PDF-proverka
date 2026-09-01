"""Deterministic LEFT -> RIGHT entity matching for the production flow.

The early pass is deliberately advisory.  The final pass uses explicit
engineering facts and can emit ``SAME_ENTITY`` only for a unique candidate
supported by several independent strong signals.  Names/designations are
supporting evidence only and no model response can promote a relation.

This module also builds the *explicit* TEXT <-> GRAPHIC candidate contract
consumed by G2.4.6.  It never evaluates or bypasses the existing M1--M8 gates.
Missing coverage or document-binding facts stay missing/fail-closed.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from copy import deepcopy
from difflib import SequenceMatcher
from typing import Any

from .production_artifacts import (
    canonical_json,
    canonical_strings,
    content_signature,
    stable_id,
    utc_now,
)
from .unified_entity_bridge.entity_normalizer import canonical_entity_name


KIND = "stage_comparison_entity_relations"
SCHEMA_VERSION = "entity-relations.v1"
ALGORITHM_VERSION = "production-entity-matcher-v2"
EARLY_KIND = "stage_comparison_early_entity_candidates"
EARLY_SCHEMA_VERSION = "early-entity-candidates.v1"
EARLY_ALGORITHM_VERSION = "production-early-entity-candidates-v2"
SYNTHESIS_CANDIDATE_KIND = "stage_comparison_text_graphic_candidates"
SYNTHESIS_CANDIDATE_SCHEMA_VERSION = "text-graphic-candidates.v1"
SYNTHESIS_CANDIDATE_VERSION = "g2.4.6-explicit-candidate-builder-v2"
BOUND_ATOMS_KIND = "stage_comparison_entity_bound_atoms"
BOUND_ATOMS_SCHEMA_VERSION = "entity-bound-atoms.v1"
BOUND_ATOMS_VERSION = "exact-entity-relation-atom-binding-v1"
DIRECTION = "LEFT_TO_RIGHT"
RELATIONS = frozenset(
    {"SAME_ENTITY", "POSSIBLE_ENTITY", "UNKNOWN", "DIFFERENT_ENTITY"}
)

_NESTED_FACT_CONTAINERS = (
    "attributes",
    "facts",
    "profile",
    "graph",
    "context",
    "metadata",
)
_SIGNAL_KEYS: dict[str, tuple[str, ...]] = {
    "functional_role": (
        "functional_role",
        "functional_roles",
        "role",
        "roles",
        "function",
        "functions",
    ),
    # ``fed_by`` and ``upstream`` describe one independent fact family and
    # therefore cannot be double-counted as two strong signals.
    "upstream_fed_by": (
        "upstream",
        "upstream_refs",
        "upstream_entities",
        "fed_by",
        "what_feeds_it",
        "feeders",
        "sources",
    ),
    "downstream_feeds": (
        "downstream",
        "downstream_refs",
        "downstream_entities",
        "feeds",
        "what_it_feeds",
        "loads",
        "consumers",
    ),
    "location": ("location", "locations", "room", "rooms", "zone", "zones"),
    "neighbours": (
        "neighbours",
        "neighbors",
        "neighbour_refs",
        "neighbor_refs",
        "adjacent",
        "adjacency",
    ),
    "topology_relationships": (
        "topology",
        "topology_tokens",
        "relationships",
        "graph_relationships",
        "connections",
        "edges",
    ),
    "parameters": (
        "parameters",
        "parameter_values",
        "technical_parameters",
        "properties",
    ),
    "entity_type": ("entity_type", "node_type", "type", "kind"),
}

_SIGNAL_WEIGHTS = {
    "explicit_project_entity_ref": 0.24,
    "functional_role": 0.20,
    "upstream_fed_by": 0.15,
    "downstream_feeds": 0.15,
    "location": 0.12,
    "neighbours": 0.09,
    "topology_relationships": 0.11,
    "parameters": 0.08,
    "entity_type": 0.03,
    "name_designation": 0.02,
}
_STRONG_SIGNAL_NAMES = frozenset(
    {
        "explicit_project_entity_ref",
        "functional_role",
        "upstream_fed_by",
        "downstream_feeds",
        "location",
        "neighbours",
        "topology_relationships",
        "parameters",
    }
)


def _canonical_fact_value(value: Any) -> Any:
    """Return a JSON-safe value with set-like containers canonically ordered.

    Entity producers use lists for engineering fact sets (including lists of
    nested mappings).  Their source order is not identity evidence, so neither
    fallback ids nor matcher signatures may depend on it.
    """
    if isinstance(value, Mapping):
        return {
            str(key): _canonical_fact_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        normalized = [_canonical_fact_value(item) for item in value]
        return sorted(normalized, key=canonical_json)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _fact_token(value: Any) -> Any:
    normalized = _canonical_fact_value(value)
    if isinstance(normalized, (Mapping, list)):
        return canonical_json(normalized)
    return normalized


def _values(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        output: list[Any] = []
        for key in sorted(value, key=str):
            nested = value[key]
            if nested is not None:
                output.append(f"{key}={_fact_token(nested)}")
        return output
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        return [_fact_token(item) for item in value]
    return [value]


def _source_values(record: Mapping[str, Any], keys: Iterable[str]) -> list[Any]:
    output: list[Any] = []
    for key in keys:
        output.extend(_values(record.get(key)))
    for container_key in _NESTED_FACT_CONTAINERS:
        container = record.get(container_key)
        if not isinstance(container, Mapping):
            continue
        for key in keys:
            output.extend(_values(container.get(key)))
    return output


def _reference(
    record: Mapping[str, Any],
    *,
    name: str | None,
    signals: Mapping[str, Iterable[str]],
    different_entity_refs: Iterable[str],
    project_entity_ref: str | None,
) -> str:
    for key in (
        "entity_ref",
        "entity_id",
        "id",
        "subject_ref",
        "canonical_identity",
    ):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    # Only normalized semantic facts participate in fallback identity.  Raw
    # producer mappings may contain insertion-ordered nested lists and other
    # transport details that must not churn entity ids.
    semantic_identity = {
        "canonical_name": canonical_entity_name(name) if name else None,
        "project_entity_ref": project_entity_ref,
        "signals": {
            signal: sorted(set(values))
            for signal, values in sorted(signals.items())
        },
        "different_entity_refs": sorted(set(different_entity_refs)),
        "identity_conflict": bool(record.get("identity_conflict")),
    }
    return stable_id("entity_", semantic_identity)


def _name(record: Mapping[str, Any]) -> str | None:
    for key in ("designation", "name", "label", "display_name", "title"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return " ".join(value.split())
    return None


def _project_ref(record: Mapping[str, Any]) -> str | None:
    value = record.get("project_entity_ref")
    return value.strip() if isinstance(value, str) and value.strip() else None


def normalize_entity(record: Mapping[str, Any], *, side: str) -> dict[str, Any]:
    """Normalize only explicit facts; never infer role from a name."""
    if not isinstance(record, Mapping):
        raise ValueError(f"{side} entity must be an object")
    name = _name(record)
    signals = {
        signal: canonical_strings(_source_values(record, keys))
        for signal, keys in _SIGNAL_KEYS.items()
    }
    different = canonical_strings(
        _source_values(
            record,
            (
                "different_entity_refs",
                "different_from",
                "incompatible_entity_refs",
                "identity_conflicts",
            ),
        )
    )
    project_ref = _project_ref(record)
    entity_ref = _reference(
        record,
        name=name,
        signals=signals,
        different_entity_refs=different,
        project_entity_ref=project_ref,
    )
    return {
        "side": side,
        "entity_ref": entity_ref,
        "project_entity_ref": project_ref,
        "name": name,
        "canonical_name": canonical_entity_name(name) if name else None,
        "signals": signals,
        "different_entity_refs": different,
        "identity_conflict": bool(record.get("identity_conflict")),
        "source_ref": record.get("source_ref"),
    }


def _overlap(left: Iterable[str], right: Iterable[str]) -> float | None:
    left_set, right_set = set(left), set(right)
    if not left_set or not right_set:
        return None
    return len(left_set & right_set) / max(1, min(len(left_set), len(right_set)))


def _name_similarity(left: Mapping[str, Any], right: Mapping[str, Any]) -> float | None:
    left_name, right_name = left.get("canonical_name"), right.get("canonical_name")
    if not left_name or not right_name:
        return None
    return SequenceMatcher(None, left_name, right_name, autojunk=False).ratio()


def _explicit_conflicts(
    left: Mapping[str, Any], right: Mapping[str, Any]
) -> list[dict[str, Any]]:
    conflicts: list[dict[str, Any]] = []
    left_ref = canonical_strings([left["entity_ref"]])[0]
    right_ref = canonical_strings([right["entity_ref"]])[0]
    if (
        right_ref in set(left["different_entity_refs"])
        or left_ref in set(right["different_entity_refs"])
    ):
        conflicts.append({"feature": "explicit_different_entity_ref"})
    if left.get("identity_conflict") or right.get("identity_conflict"):
        conflicts.append({"feature": "source_identity_conflict"})
    left_project, right_project = (
        left.get("project_entity_ref"),
        right.get("project_entity_ref"),
    )
    if left_project and right_project and left_project != right_project:
        conflicts.append(
            {
                "feature": "project_entity_ref",
                "left": left_project,
                "right": right_project,
            }
        )
    # Functional role is explicit engineering identity evidence.  Unlike a
    # changed parameter or connection, mutually exclusive roles are a real
    # identity conflict rather than the change being measured.
    left_roles = set(left["signals"]["functional_role"])
    right_roles = set(right["signals"]["functional_role"])
    if left_roles and right_roles and not left_roles & right_roles:
        conflicts.append(
            {
                "feature": "functional_role",
                "left": sorted(left_roles),
                "right": sorted(right_roles),
            }
        )
    return conflicts


def _pair_evaluation(
    left: Mapping[str, Any], right: Mapping[str, Any]
) -> dict[str, Any]:
    scores: dict[str, float | None] = {}
    for signal in _SIGNAL_KEYS:
        scores[signal] = _overlap(
            left["signals"][signal], right["signals"][signal]
        )
    if left.get("project_entity_ref") and right.get("project_entity_ref"):
        scores["explicit_project_entity_ref"] = float(
            left["project_entity_ref"] == right["project_entity_ref"]
        )
    else:
        scores["explicit_project_entity_ref"] = None
    scores["name_designation"] = _name_similarity(left, right)

    available = [
        (signal, value)
        for signal, value in scores.items()
        if value is not None
    ]
    weighted = (
        sum(float(value) * _SIGNAL_WEIGHTS[signal] for signal, value in available)
        / sum(_SIGNAL_WEIGHTS[signal] for signal, _value in available)
        if available
        else None
    )
    strong = sorted(
        signal
        for signal, value in scores.items()
        if signal in _STRONG_SIGNAL_NAMES
        and value is not None
        and float(value) >= 0.60
    )
    mismatches = sorted(
        signal
        for signal, value in scores.items()
        if signal != "name_designation" and value == 0
    )
    conflicts = _explicit_conflicts(left, right)
    structural_available = sum(
        value is not None
        for signal, value in scores.items()
        if signal != "name_designation"
    )
    evidence = [
        {
            "feature": signal,
            "score": round(float(value), 6),
            "strength": (
                "STRONG"
                if signal in strong
                else "SUPPORTING" if float(value) > 0 else "MISMATCH"
            ),
        }
        for signal, value in sorted(scores.items())
        if value is not None
    ]
    return {
        "score": round(weighted, 6) if weighted is not None else None,
        "strong_signals": strong,
        "mismatched_signals": mismatches,
        "conflicts": conflicts,
        "structural_available": structural_available,
        "evidence": evidence,
    }


def _normalized_entities(
    entities: Iterable[Mapping[str, Any]], *, side: str
) -> list[dict[str, Any]]:
    normalized = sorted(
        (normalize_entity(item, side=side) for item in entities),
        key=lambda item: item["entity_ref"],
    )
    refs = [item["entity_ref"] for item in normalized]
    if len(refs) != len(set(refs)):
        raise ValueError(f"duplicate {side} entity_ref")
    return normalized


def build_early_entity_candidates(
    left_entities: Iterable[Mapping[str, Any]],
    right_entities: Iterable[Mapping[str, Any]],
    *,
    top_k: int = 5,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build cheap advisory candidates; this function can never say SAME."""
    if not isinstance(top_k, int) or isinstance(top_k, bool) or not 1 <= top_k <= 20:
        raise ValueError("top_k must be between 1 and 20")
    left = _normalized_entities(left_entities, side="LEFT")
    right = _normalized_entities(right_entities, side="RIGHT")
    candidates: list[dict[str, Any]] = []
    for left_item in left:
        ranked = []
        for right_item in right:
            evaluation = _pair_evaluation(left_item, right_item)
            if evaluation["conflicts"]:
                relation = "DIFFERENT_ENTITY"
            elif evaluation["score"] is None:
                relation = "UNKNOWN"
            elif evaluation["score"] >= 0.18:
                relation = "POSSIBLE_ENTITY"
            else:
                relation = "UNKNOWN"
            ranked.append(
                {
                    "candidate_id": stable_id(
                        "ecan_", left_item["entity_ref"], right_item["entity_ref"]
                    ),
                    "left_entity_ref": left_item["entity_ref"],
                    "right_entity_ref": right_item["entity_ref"],
                    "relation": relation,
                    "score": evaluation["score"],
                    "strong_signals": evaluation["strong_signals"],
                    "evidence": evaluation["evidence"],
                    "conflicts": evaluation["conflicts"],
                    "same_entity_allowed": False,
                }
            )
        ranked.sort(
            key=lambda item: (
                -(item["score"] if item["score"] is not None else -1),
                item["right_entity_ref"],
            )
        )
        candidates.extend(ranked[:top_k])
    candidates.sort(key=lambda item: item["candidate_id"])
    input_signature = content_signature(
        {
            "algorithm": EARLY_ALGORITHM_VERSION,
            "left": left,
            "right": right,
            "top_k": top_k,
        }
    )
    return {
        "kind": EARLY_KIND,
        "schema_version": EARLY_SCHEMA_VERSION,
        "algorithm_version": EARLY_ALGORITHM_VERSION,
        "version": 1,
        "direction": DIRECTION,
        "input_signature": input_signature,
        "generated_at": generated_at or utc_now(),
        "candidates": candidates,
        "diagnostics": {
            "left_entities": len(left),
            "right_entities": len(right),
            "candidate_count": len(candidates),
            "top_k": top_k,
            "same_entity_emitted": False,
            "uses_model": False,
        },
    }


def _is_plausible(evaluation: Mapping[str, Any]) -> bool:
    if evaluation["conflicts"] or evaluation["score"] is None:
        return False
    strong_count = len(evaluation["strong_signals"])
    return strong_count >= 2 or (
        strong_count >= 1 and float(evaluation["score"]) >= 0.55
    )


def _same_eligible(evaluation: Mapping[str, Any]) -> bool:
    return (
        not evaluation["conflicts"]
        and evaluation["score"] is not None
        and float(evaluation["score"]) >= 0.68
        and len(evaluation["strong_signals"]) >= 3
    )


def match_entities(
    left_entities: Iterable[Mapping[str, Any]],
    right_entities: Iterable[Mapping[str, Any]],
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build final deterministic entity relations after TEXT + GRAPHIC facts."""
    left = _normalized_entities(left_entities, side="LEFT")
    right = _normalized_entities(right_entities, side="RIGHT")
    evaluations: dict[tuple[str, str], dict[str, Any]] = {}
    for left_item in left:
        for right_item in right:
            evaluations[(left_item["entity_ref"], right_item["entity_ref"])] = (
                _pair_evaluation(left_item, right_item)
            )

    plausible_by_left: dict[str, set[str]] = defaultdict(set)
    plausible_by_right: dict[str, set[str]] = defaultdict(set)
    for (left_ref, right_ref), evaluation in evaluations.items():
        if _is_plausible(evaluation):
            plausible_by_left[left_ref].add(right_ref)
            plausible_by_right[right_ref].add(left_ref)

    ranks: dict[tuple[str, str], int] = {}
    for left_item in left:
        left_ref = left_item["entity_ref"]
        ranked_refs = sorted(
            (right_item["entity_ref"] for right_item in right),
            key=lambda right_ref: (
                -(
                    evaluations[(left_ref, right_ref)]["score"]
                    if evaluations[(left_ref, right_ref)]["score"] is not None
                    else -1
                ),
                right_ref,
            ),
        )
        ranks.update({(left_ref, ref): rank for rank, ref in enumerate(ranked_refs, 1)})

    left_by_ref = {item["entity_ref"]: item for item in left}
    right_by_ref = {item["entity_ref"]: item for item in right}
    relations: list[dict[str, Any]] = []
    for (left_ref, right_ref), evaluation in sorted(evaluations.items()):
        unique = (
            plausible_by_left[left_ref] == {right_ref}
            and plausible_by_right[right_ref] == {left_ref}
        )
        if _same_eligible(evaluation) and unique:
            relation = "SAME_ENTITY"
        elif evaluation["conflicts"]:
            relation = "DIFFERENT_ENTITY"
        elif _is_plausible(evaluation) or evaluation["strong_signals"]:
            relation = "POSSIBLE_ENTITY"
        elif evaluation["structural_available"] == 0:
            relation = "UNKNOWN"
        elif (
            evaluation["score"] is not None
            and float(evaluation["score"]) < 0.15
            and evaluation["structural_available"] >= 2
        ):
            relation = "DIFFERENT_ENTITY"
        else:
            relation = "UNKNOWN"

        # UNKNOWN is actionable only for the best candidate of an otherwise
        # unmatched LEFT entity; this prevents a full cartesian review flood.
        review_required = relation == "POSSIBLE_ENTITY" or (
            relation == "UNKNOWN"
            and ranks[(left_ref, right_ref)] == 1
            and not plausible_by_left[left_ref]
        )
        left_item, right_item = left_by_ref[left_ref], right_by_ref[right_ref]
        shared_project_ref = (
            left_item["project_entity_ref"]
            if left_item["project_entity_ref"]
            and left_item["project_entity_ref"] == right_item["project_entity_ref"]
            else None
        )
        if relation == "SAME_ENTITY" and shared_project_ref is None:
            shared_project_ref = stable_id(
                "project_entity_", left_ref, right_ref, length=24
            )
        relation_id = stable_id("erel_", left_ref, right_ref)
        relations.append(
            {
                "relation_id": relation_id,
                "left_entity_ref": left_ref,
                "right_entity_ref": right_ref,
                "left_project_entity_ref": left_item["project_entity_ref"],
                "right_project_entity_ref": right_item["project_entity_ref"],
                "project_entity_ref": shared_project_ref,
                "relation": relation,
                "status": relation,
                "confidence": {
                    "SAME_ENTITY": "HIGH",
                    "POSSIBLE_ENTITY": "MEDIUM",
                    "DIFFERENT_ENTITY": "HIGH" if evaluation["conflicts"] else "MEDIUM",
                    "UNKNOWN": "UNKNOWN",
                }[relation],
                "score": evaluation["score"],
                "candidate_rank": ranks[(left_ref, right_ref)],
                "unique_candidate": unique,
                "strong_signals": evaluation["strong_signals"],
                "conflicting_signals": evaluation["conflicts"],
                "mismatched_signals": evaluation["mismatched_signals"],
                "evidence": evaluation["evidence"],
                "review_required": review_required,
                "provenance": {
                    "algorithm": ALGORITHM_VERSION,
                    "name_is_primary": False,
                    "independent_strong_signal_count": len(
                        evaluation["strong_signals"]
                    ),
                    "automatic_same_requires_unique_candidate": True,
                    "ai_final_decision": False,
                },
            }
        )

    relations.sort(key=lambda item: item["relation_id"])
    relation_counts = Counter(item["relation"] for item in relations)
    input_signature = content_signature(
        {"algorithm": ALGORITHM_VERSION, "left": left, "right": right}
    )
    return {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "algorithm_version": ALGORITHM_VERSION,
        "version": 1,
        "direction": DIRECTION,
        "input_signature": input_signature,
        "generated_at": generated_at or utc_now(),
        "relations": relations,
        "diagnostics": {
            "left_entities": len(left),
            "right_entities": len(right),
            "evaluated_pairs": len(evaluations),
            "relation_counts": {
                relation: relation_counts.get(relation, 0)
                for relation in sorted(RELATIONS)
            },
            "uses_model": False,
            "name_is_primary": False,
            "same_entity_minimum_independent_strong_signals": 3,
        },
    }


def entity_relations_are_stale(
    artifact: Mapping[str, Any] | None,
    left_entities: Iterable[Mapping[str, Any]],
    right_entities: Iterable[Mapping[str, Any]],
) -> bool:
    if not isinstance(artifact, Mapping):
        return True
    left = _normalized_entities(left_entities, side="LEFT")
    right = _normalized_entities(right_entities, side="RIGHT")
    expected = content_signature(
        {"algorithm": ALGORITHM_VERSION, "left": left, "right": right}
    )
    return (
        artifact.get("kind") != KIND
        or artifact.get("schema_version") != SCHEMA_VERSION
        or artifact.get("input_signature") != expected
    )


def entity_records_from_atoms(
    text_atoms: Iterable[Mapping[str, Any]],
    graphic_atoms: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Expose side-specific exact aliases to :func:`match_entities`.

    This is the deterministic Fact-production boundary used by the production
    orchestrator.  It is public so a bounded downstream replay can rebuild
    entity matching from persisted atoms without reopening a PDF or copying
    the logic into an AI-specific branch.
    """
    by_side: dict[str, dict[str, dict[str, Any]]] = {"LEFT": {}, "RIGHT": {}}

    def merge_record(
        side: str,
        subject: str,
        atom: Mapping[str, Any],
        **facts: Iterable[Any],
    ) -> None:
        record = by_side[side].setdefault(subject, {
            "entity_ref": subject,
            "project_entity_ref": atom.get("project_entity_ref"),
            "source_ref": atom.get("atom_id"),
            "source_refs": [],
        })
        source_ref = atom.get("atom_id")
        if isinstance(source_ref, str) and source_ref:
            record["source_refs"] = sorted({*record["source_refs"], source_ref})
        project_ref = atom.get("project_entity_ref")
        existing_ref = record.get("project_entity_ref")
        if existing_ref and project_ref and existing_ref != project_ref:
            record["project_entity_ref"] = None
            record["identity_conflict"] = True
        elif not existing_ref and project_ref and not record.get("identity_conflict"):
            record["project_entity_ref"] = project_ref
        for key, values in facts.items():
            normalized = [value for value in values if value not in (None, "")]
            if normalized:
                record[key] = [*(record.get(key) or []), *normalized]

    for atom in text_atoms:
        subject = atom.get("subject_ref") or atom.get("project_entity_ref")
        if not isinstance(subject, str) or not subject:
            continue
        for side, value_key in (("LEFT", "before_value"), ("RIGHT", "after_value")):
            value = atom.get(value_key)
            if value is None:
                continue
            merge_record(
                side,
                subject,
                atom,
                parameters=[f"{atom.get('facet_ref') or atom.get('dimension')}={value}"],
            )

    for atom in graphic_atoms:
        subject = atom.get("subject_ref")
        if not isinstance(subject, str) or not subject:
            continue
        provenance = atom.get("provenance")
        structural = (
            provenance.get("structured")
            if isinstance(provenance, Mapping)
            else None
        )
        if not isinstance(structural, Mapping):
            continue
        descriptor = structural.get("subject")
        if not isinstance(descriptor, Mapping) or not descriptor:
            continue
        relation = structural.get("relation")
        relation = relation if isinstance(relation, Mapping) else {}
        roles = [
            descriptor.get(key) for key in ("functional_role", "role", "function")
        ]
        entity_types = [descriptor.get("kind"), descriptor.get("node_type")]
        for side, prefix in (("LEFT", "left"), ("RIGHT", "right")):
            nodes = list(structural.get(f"{prefix}_nodes") or [])
            edges = list(structural.get(f"{prefix}_edges") or [])
            if not nodes and not edges:
                continue
            parameters = [
                f"{key[len(prefix) + 1:]}={value}"
                for key, value in relation.items()
                if str(key).startswith(prefix + "_") and value is not None
            ]
            merge_record(
                side,
                subject,
                atom,
                functional_roles=roles,
                entity_type=entity_types,
                neighbours=nodes,
                topology=edges,
                parameters=parameters,
            )
    return (
        sorted(by_side["LEFT"].values(), key=lambda item: item["entity_ref"]),
        sorted(by_side["RIGHT"].values(), key=lambda item: item["entity_ref"]),
    )


def _atom_ref(atom: Mapping[str, Any], where: str) -> str:
    value = atom.get("atom_id")
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{where}.atom_id must be a non-empty string")
    return value.strip()


def _atom_project_ref(atom: Mapping[str, Any]) -> str | None:
    value = atom.get("project_entity_ref")
    return value.strip() if isinstance(value, str) and value.strip() else None


def _nonempty_ref(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _relation_status(relation: Mapping[str, Any]) -> Any:
    return relation.get(
        "effective_relation",
        relation.get("relation", relation.get("status")),
    )


def _relation_rows(
    entity_relations: Mapping[str, Any] | Iterable[Mapping[str, Any]] | None,
) -> list[Mapping[str, Any]]:
    if entity_relations is None:
        return []
    if isinstance(entity_relations, Mapping):
        raw_relations = entity_relations.get("relations") or []
    else:
        raw_relations = entity_relations
    return [item for item in raw_relations if isinstance(item, Mapping)]


def _entity_relation_index(
    entity_relations: Mapping[str, Any] | Iterable[Mapping[str, Any]] | None,
) -> tuple[
    set[str] | None,
    dict[str, list[str]],
    dict[str, str],
    dict[str, Any] | None,
]:
    """Index exact aliases established by explicit SAME_ENTITY relations.

    The alias map is intentionally exact.  Conflicting aliases are discarded
    instead of being resolved by a score/name heuristic.  A compact semantic
    artifact binding is carried into every downstream candidate proof.
    """
    if entity_relations is None:
        return None, {}, {}, None
    raw_relations = _relation_rows(entity_relations)
    proven: set[str] = set()
    relation_ids: dict[str, list[str]] = defaultdict(list)
    aliases: dict[str, set[str]] = defaultdict(set)
    proofs: list[dict[str, Any]] = []
    for relation in raw_relations:
        if (
            _relation_status(relation) != "SAME_ENTITY"
            or relation.get("review_required") is True
        ):
            continue
        left_entity_ref = _nonempty_ref(relation.get("left_entity_ref"))
        right_entity_ref = _nonempty_ref(relation.get("right_entity_ref"))
        left_project_ref = _nonempty_ref(
            relation.get("left_project_entity_ref")
        )
        right_project_ref = _nonempty_ref(
            relation.get("right_project_entity_ref")
        )
        shared = _nonempty_ref(relation.get("project_entity_ref"))
        if shared is None and left_project_ref == right_project_ref:
            shared = left_project_ref
        if shared is None and left_entity_ref and right_entity_ref:
            shared = stable_id(
                "project_entity_", left_entity_ref, right_entity_ref, length=24
            )
        if shared is None:
            continue
        proven.add(shared)
        relation_id = _nonempty_ref(relation.get("relation_id")) or stable_id(
            "erel_", left_entity_ref, right_entity_ref, shared
        )
        relation_ids[shared].append(relation_id)
        exact_aliases = {
            alias
            for alias in (
                shared,
                left_entity_ref,
                right_entity_ref,
                left_project_ref,
                right_project_ref,
            )
            if alias is not None
        }
        for alias in exact_aliases:
            aliases[alias].add(shared)
        decision = relation.get("human_decision")
        proofs.append(
            {
                "relation_id": relation_id,
                "effective_relation_id": _nonempty_ref(
                    relation.get("effective_relation_id")
                ),
                "left_entity_ref": left_entity_ref,
                "right_entity_ref": right_entity_ref,
                "left_project_entity_ref": left_project_ref,
                "right_project_entity_ref": right_project_ref,
                "project_entity_ref": shared,
                "proof": "HUMAN" if isinstance(decision, Mapping) else "AUTOMATIC",
                "human_decision": (
                    {
                        "decision_id": decision.get("decision_id"),
                        "question_input_signature": decision.get(
                            "question_input_signature"
                        ),
                        "answer": decision.get("answer"),
                        "selected_refs": sorted(decision.get("selected_refs") or []),
                    }
                    if isinstance(decision, Mapping)
                    else None
                ),
            }
        )
    proofs.sort(key=lambda item: (item["relation_id"], canonical_json(item)))
    relation_ids_result = {
        key: sorted(set(value)) for key, value in relation_ids.items()
    }
    alias_map = {
        alias: next(iter(project_refs))
        for alias, project_refs in aliases.items()
        if len(project_refs) == 1
    }
    relation_payloads = sorted(
        (_canonical_fact_value(item) for item in raw_relations),
        key=canonical_json,
    )
    binding = {
        "kind": (
            entity_relations.get("kind")
            if isinstance(entity_relations, Mapping)
            else None
        ),
        "schema_version": (
            entity_relations.get("schema_version")
            if isinstance(entity_relations, Mapping)
            else None
        ),
        "input_signature": (
            entity_relations.get("input_signature")
            if isinstance(entity_relations, Mapping)
            else None
        ),
        # ``input_signature`` on an effective artifact may still describe the
        # immutable automatic matcher input.  Bind to effective relation
        # semantics as well, including an explicit human decision when used.
        "proof_signature": content_signature(proofs),
        "relations_signature": content_signature(relation_payloads),
    }
    binding["binding_signature"] = content_signature(binding)
    return proven, relation_ids_result, alias_map, binding


def _canonical_atom_project_ref(
    atom: Mapping[str, Any],
    *,
    proven_refs: set[str],
    alias_map: Mapping[str, str],
) -> str | None:
    explicit_project_ref = _atom_project_ref(atom)
    mapped: set[str] = set()
    if explicit_project_ref is not None:
        if explicit_project_ref in proven_refs:
            mapped.add(explicit_project_ref)
        elif explicit_project_ref in alias_map:
            mapped.add(alias_map[explicit_project_ref])
        else:
            return None
    for ref in (
        _nonempty_ref(atom.get("subject_ref")),
        _nonempty_ref(atom.get("entity_ref")),
    ):
        if ref is not None and ref in alias_map:
            mapped.add(alias_map[ref])
    return next(iter(mapped)) if len(mapped) == 1 else None


def _bind_atom(
    atom: Mapping[str, Any],
    *,
    proven_refs: set[str],
    alias_map: Mapping[str, str],
    relation_ids: Mapping[str, list[str]],
    relation_binding: Mapping[str, Any],
) -> tuple[dict[str, Any], bool]:
    result = deepcopy(dict(atom))
    canonical_ref = _canonical_atom_project_ref(
        result, proven_refs=proven_refs, alias_map=alias_map
    )
    if canonical_ref is None:
        return result, False
    existing_provenance = result.get("provenance")
    existing_binding = (
        existing_provenance.get("entity_relation_binding")
        if isinstance(existing_provenance, Mapping)
        else None
    )
    if (
        _atom_project_ref(result) == canonical_ref
        and _nonempty_ref(result.get("subject_ref")) == canonical_ref
        and isinstance(existing_binding, Mapping)
        and existing_binding.get("entity_relations_binding_signature")
        == relation_binding.get("binding_signature")
    ):
        return result, True
    original_project = _atom_project_ref(result)
    original_subject = _nonempty_ref(result.get("subject_ref"))
    result["project_entity_ref"] = canonical_ref
    if original_subject is not None and (
        original_subject in proven_refs or original_subject in alias_map
    ):
        result["subject_ref"] = canonical_ref
    provenance = dict(result.get("provenance") or {})
    provenance["entity_relation_binding"] = {
        "producer": BOUND_ATOMS_VERSION,
        "project_entity_ref": canonical_ref,
        "original_project_entity_ref": original_project,
        "original_subject_ref": original_subject,
        "entity_relation_ids": relation_ids.get(canonical_ref, []),
        "entity_relations_binding_signature": relation_binding.get(
            "binding_signature"
        ),
    }
    result["provenance"] = provenance
    return result, True


def bind_atoms_to_entity_relations(
    text_atoms: Iterable[Mapping[str, Any]],
    graphic_atoms: Iterable[Mapping[str, Any]],
    entity_relations: Mapping[str, Any] | Iterable[Mapping[str, Any]],
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Return atom copies canonically bound by exact SAME_ENTITY evidence.

    This is the explicit pre-synthesis hand-off: the G2.4.6 synthesizer must
    receive these copies, not the original side-specific atoms.  No fuzzy
    fallback is attempted and the upstream atom artifacts are never mutated.
    """
    relations_input: Mapping[str, Any] | list[Mapping[str, Any]]
    relations_input = (
        entity_relations
        if isinstance(entity_relations, Mapping)
        else list(entity_relations)
    )
    proven_refs, relation_ids, alias_map, relation_binding = (
        _entity_relation_index(relations_input)
    )
    assert proven_refs is not None and relation_binding is not None

    bound_text: list[dict[str, Any]] = []
    bound_graphic: list[dict[str, Any]] = []
    bound_ids: list[str] = []
    unresolved_ids: list[str] = []
    for where, raw_atoms, output in (
        ("TEXT", text_atoms, bound_text),
        ("GRAPHIC", graphic_atoms, bound_graphic),
    ):
        atoms = sorted(
            (dict(item) for item in raw_atoms),
            key=lambda item: _atom_ref(item, where),
        )
        atom_ids = [_atom_ref(atom, where) for atom in atoms]
        if len(atom_ids) != len(set(atom_ids)):
            raise ValueError(f"duplicate {where} atom_id")
        if any(atom.get("source") not in (None, where) for atom in atoms):
            raise ValueError(f"{where.lower()}_atoms contain another source")
        for atom in atoms:
            atom_id = _atom_ref(atom, where)
            bound, did_bind = _bind_atom(
                atom,
                proven_refs=proven_refs,
                alias_map=alias_map,
                relation_ids=relation_ids,
                relation_binding=relation_binding,
            )
            output.append(bound)
            (bound_ids if did_bind else unresolved_ids).append(atom_id)
    input_signature = content_signature(
        {
            "binder": BOUND_ATOMS_VERSION,
            "text_atoms": bound_text,
            "graphic_atoms": bound_graphic,
            "entity_relations_binding": relation_binding,
        }
    )
    return {
        "kind": BOUND_ATOMS_KIND,
        "schema_version": BOUND_ATOMS_SCHEMA_VERSION,
        "binder_version": BOUND_ATOMS_VERSION,
        "version": 1,
        "input_signature": input_signature,
        "generated_at": generated_at or utc_now(),
        "entity_relations_binding": relation_binding,
        "text_atoms": bound_text,
        "graphic_atoms": bound_graphic,
        "diagnostics": {
            "bound_atom_ids": sorted(bound_ids),
            "unresolved_atom_ids": sorted(unresolved_ids),
            "exact_alias_only": True,
            "automatic_artifacts_mutated": False,
            "uses_model": False,
        },
    }


def _pair_compatible(text_atom: Mapping[str, Any], graphic_atom: Mapping[str, Any]) -> bool:
    if text_atom.get("dimension") != graphic_atom.get("dimension"):
        return False
    text_facet, graphic_facet = text_atom.get("facet_ref"), graphic_atom.get("facet_ref")
    return not (text_facet and graphic_facet and text_facet != graphic_facet)


def _context_for(
    value: Any,
    *,
    project_ref: str,
    text_atom_id: str,
    graphic_atom_id: str,
    default: Any,
) -> Any:
    if not isinstance(value, Mapping):
        return deepcopy(default if value is None else value)
    if set(value) >= {"LEFT", "RIGHT"}:
        return deepcopy(value)
    pair_key = f"{text_atom_id}|{graphic_atom_id}"
    for key in (pair_key, project_ref, (text_atom_id, graphic_atom_id)):
        if key in value:
            return deepcopy(value[key])
    return deepcopy(default)


def build_text_graphic_synthesis_candidates(
    text_atoms: Iterable[Mapping[str, Any]],
    graphic_atoms: Iterable[Mapping[str, Any]],
    entity_relations: Mapping[str, Any] | Iterable[Mapping[str, Any]] | None = None,
    *,
    links_by_side: Any = None,
    coverage_by_side: Any = None,
    document_binding_state: Any = "DOCUMENT_BINDING_UNKNOWN",
    source_valid: Any = False,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build unique explicit G2.4.6 candidates without evaluating M-gates.

    When an entity-relation artifact is supplied, its ``SAME_ENTITY`` result
    is mandatory.  Without such an artifact, exact non-empty
    ``project_entity_ref`` equality on the two already-structured atoms is the
    explicit proof.  Ambiguous atom cardinality produces no candidate.
    """
    relations_input: Mapping[str, Any] | list[Mapping[str, Any]] | None
    if entity_relations is None or isinstance(entity_relations, Mapping):
        relations_input = entity_relations
    else:
        relations_input = list(entity_relations)
    text = sorted((dict(item) for item in text_atoms), key=lambda item: _atom_ref(item, "TEXT"))
    graphic = sorted(
        (dict(item) for item in graphic_atoms), key=lambda item: _atom_ref(item, "GRAPHIC")
    )
    text_ids = [_atom_ref(item, "TEXT") for item in text]
    graphic_ids = [_atom_ref(item, "GRAPHIC") for item in graphic]
    if len(text_ids) != len(set(text_ids)) or len(graphic_ids) != len(set(graphic_ids)):
        raise ValueError("duplicate atom_id")
    if any(item.get("source") not in (None, "TEXT") for item in text):
        raise ValueError("text_atoms must contain only TEXT atoms")
    if any(item.get("source") not in (None, "GRAPHIC") for item in graphic):
        raise ValueError("graphic_atoms must contain only GRAPHIC atoms")

    proven_refs, relation_ids, _alias_map, relation_binding = (
        _entity_relation_index(relations_input)
    )
    bound_atoms_signature = None
    if relations_input is not None:
        bound = bind_atoms_to_entity_relations(
            text,
            graphic,
            relations_input,
            generated_at=generated_at,
        )
        text = bound["text_atoms"]
        graphic = bound["graphic_atoms"]
        bound_atoms_signature = bound["input_signature"]
    potential: list[tuple[dict[str, Any], dict[str, Any], str]] = []
    omitted_unproven: list[list[str]] = []
    for text_atom in text:
        for graphic_atom in graphic:
            project_ref = _atom_project_ref(text_atom)
            graphic_project_ref = _atom_project_ref(graphic_atom)
            if proven_refs is not None and (
                not project_ref
                or project_ref != graphic_project_ref
                or project_ref not in proven_refs
            ):
                omitted_unproven.append(
                    [_atom_ref(text_atom, "TEXT"), _atom_ref(graphic_atom, "GRAPHIC")]
                )
                continue
            if not project_ref or project_ref != graphic_project_ref:
                continue
            if not _pair_compatible(text_atom, graphic_atom):
                continue
            potential.append((text_atom, graphic_atom, project_ref))

    text_counts = Counter(_atom_ref(item[0], "TEXT") for item in potential)
    graphic_counts = Counter(_atom_ref(item[1], "GRAPHIC") for item in potential)
    candidates: list[dict[str, Any]] = []
    ambiguous_pairs: list[list[str]] = []
    for text_atom, graphic_atom, project_ref in potential:
        text_id = _atom_ref(text_atom, "TEXT")
        graphic_id = _atom_ref(graphic_atom, "GRAPHIC")
        text_count = text_counts[text_id]
        graphic_count = graphic_counts[graphic_id]
        if text_count != 1 or graphic_count != 1:
            ambiguous_pairs.append([text_id, graphic_id])
            continue
        candidate_id = stable_id(
            "ugcand_", project_ref, text_id, graphic_id, length=24
        )
        resolved_links = _context_for(
            links_by_side,
            project_ref=project_ref,
            text_atom_id=text_id,
            graphic_atom_id=graphic_id,
            default={
                "LEFT": {"relation": "UNKNOWN", "confidence": "UNKNOWN"},
                "RIGHT": {"relation": "UNKNOWN", "confidence": "UNKNOWN"},
            },
        )
        resolved_coverage = _context_for(
            coverage_by_side,
            project_ref=project_ref,
            text_atom_id=text_id,
            graphic_atom_id=graphic_id,
            default={"LEFT": "NOT_CHECKED", "RIGHT": "NOT_CHECKED"},
        )
        resolved_binding = _context_for(
            document_binding_state,
            project_ref=project_ref,
            text_atom_id=text_id,
            graphic_atom_id=graphic_id,
            default="DOCUMENT_BINDING_UNKNOWN",
        )
        resolved_source_valid = _context_for(
            source_valid,
            project_ref=project_ref,
            text_atom_id=text_id,
            graphic_atom_id=graphic_id,
            default=False,
        )
        if not isinstance(resolved_source_valid, bool):
            raise ValueError("source_valid must resolve to boolean")
        proof = (
            "same_entity_relation"
            if relations_input is not None
            else "explicit_equal_project_entity_ref"
        )
        # Exactly the fields accepted by normalize_candidate(); any policy
        # decision remains inside the existing G2.4.5/G2.4.6 implementation.
        candidates.append(
            {
                "candidate_id": candidate_id,
                "text_atom_id": text_id,
                "graphic_atom_id": graphic_id,
                "subject_relation": "SAME_ENTITY",
                "links_by_side": resolved_links,
                "source_valid": resolved_source_valid,
                "coverage_by_side": resolved_coverage,
                "document_binding_state": resolved_binding,
                "text_count": text_count,
                "graphic_count": graphic_count,
                "subject_identity_provenance": {
                    "producer": SYNTHESIS_CANDIDATE_VERSION,
                    "proof": proof,
                    "project_entity_ref": project_ref,
                    "entity_relation_ids": relation_ids.get(project_ref, []),
                    "entity_relations_binding": relation_binding,
                    "bound_atoms_signature": bound_atoms_signature,
                },
            }
        )
    candidates.sort(key=lambda item: item["candidate_id"])
    input_signature = content_signature(
        {
            "builder": SYNTHESIS_CANDIDATE_VERSION,
            "text_atoms": text,
            "graphic_atoms": graphic,
            "entity_relations_binding": relation_binding,
            "bound_atoms_signature": bound_atoms_signature,
            "candidates": candidates,
        }
    )
    return {
        "kind": SYNTHESIS_CANDIDATE_KIND,
        "schema_version": SYNTHESIS_CANDIDATE_SCHEMA_VERSION,
        "builder_version": SYNTHESIS_CANDIDATE_VERSION,
        "version": 1,
        "input_signature": input_signature,
        "generated_at": generated_at or utc_now(),
        "candidates": candidates,
        "diagnostics": {
            "potential_pairs": len(potential),
            "emitted_candidates": len(candidates),
            "ambiguous_pairs": sorted(ambiguous_pairs),
            "unproven_pairs": sorted(omitted_unproven),
            "unique_project_entity_ref_required": True,
            "entity_relations_binding": relation_binding,
            "bound_atoms_signature": bound_atoms_signature,
            "exact_alias_only": True,
            "strict_m_gates_preserved": True,
            "uses_model": False,
        },
    }


# Public aliases keep orchestration code readable while the artifact name
# remains explicit about its G2.4.6 purpose.
build_g246_candidates = build_text_graphic_synthesis_candidates
build_synthesis_candidates = build_text_graphic_synthesis_candidates
build_early_candidates = build_early_entity_candidates


__all__ = [
    "ALGORITHM_VERSION",
    "BOUND_ATOMS_KIND",
    "BOUND_ATOMS_SCHEMA_VERSION",
    "BOUND_ATOMS_VERSION",
    "DIRECTION",
    "EARLY_ALGORITHM_VERSION",
    "EARLY_KIND",
    "EARLY_SCHEMA_VERSION",
    "KIND",
    "RELATIONS",
    "SCHEMA_VERSION",
    "SYNTHESIS_CANDIDATE_KIND",
    "SYNTHESIS_CANDIDATE_SCHEMA_VERSION",
    "SYNTHESIS_CANDIDATE_VERSION",
    "build_early_candidates",
    "build_early_entity_candidates",
    "bind_atoms_to_entity_relations",
    "build_g246_candidates",
    "build_synthesis_candidates",
    "build_text_graphic_synthesis_candidates",
    "entity_relations_are_stale",
    "entity_records_from_atoms",
    "match_entities",
    "normalize_entity",
]

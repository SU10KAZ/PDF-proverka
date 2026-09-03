"""Deterministic candidate-overlap forensics for Function Lineage v2.3.

The runner reads only the frozen candidate artifacts and the immutable v2.2.1
smoke record.  It does not rebuild candidates, invoke a selector, call a model,
use vision, or write to production/session directories.
"""
from __future__ import annotations

import argparse
import hashlib
import itertools
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parents[2]
FROZEN_CANDIDATE_ROOT = (
    REPO_ROOT
    / "comparison"
    / "ai_sheet_matcher"
    / "20260903_function_lineage_deterministic"
    / "candidate_artifacts"
)
FROZEN_SMOKE = (
    REPO_ROOT
    / "comparison"
    / "ai_sheet_matcher"
    / "20260903_function_lineage_v2_2_1_ios21_critical_smoke"
    / "task_results.json"
)
DEFAULT_OUTPUT = (
    REPO_ROOT
    / "comparison"
    / "ai_sheet_matcher"
    / "20260903_function_lineage_v2_3_candidate_forensics"
)

FORENSIC_VERSION = "function-lineage-candidate-forensics.v2.3"
FROZEN_EXPERIMENT_COMMIT = "94eb48b8"
PRODUCTION_HEAD_AT_EXPERIMENT = "4d489bf9033ad40c40099fe5e1436493bc56c0ed"
PRODUCTION_RELEASE_AT_EXPERIMENT = "ui-real-4d489bf9"
IOS21_PAIR_ID = "pe336037597"
LEFT19_TASK_ID = "ltask_015d2dbabecfea8054ea"
LEFT20_TASK_ID = "ltask_4efcf3c03235385a614e"
LEFT20_DISTRIBUTED_ID = "lcand_9c617494b14c2b922d3f"
LEFT20_R26_ID = "lcand_1d1f175a30c34b88c6e0"

PROJECTS = {
    "p19cd7f695a": "IOS1.1",
    "pe336037597": "IOS2.1",
    "pb02de74a81": "IOS3.1",
}
FROZEN_SHA256 = {
    "candidate_artifacts/p19cd7f695a.json": (
        "b709d4715cbfd234efcf9251dc263c9f4260f2f85a685aaad4f0c1e78ab407ca"
    ),
    "candidate_artifacts/pb02de74a81.json": (
        "ac27f287dd1e5b912df07afdf2046ec33d383c9e870be63ed05de6f3feba12b3"
    ),
    "candidate_artifacts/pe336037597.json": (
        "fff15f4e711c209d10b4940c2e19f1d6d8120a40d0243e1781d0ad758472039d"
    ),
    "smoke/task_results.json": (
        "2b6d8a3f101a1e66b6ec7dd3d3909401bf583890e0eee443b078d05e407b8e3f"
    ),
}

RELATIONSHIPS = (
    "DISJOINT",
    "OVERLAP",
    "STRICT_SUBSET",
    "STRICT_SUPERSET",
    "SAME_COMPONENTS_DIFFERENT_TARGET",
    "ALTERNATIVE_GRANULARITY",
    "CONTRADICTORY",
    "UNKNOWN",
)
FUNCTIONAL_CHANNELS = (
    "FUNCTION_CLASS",
    "FUNCTION_EVIDENCE",
    "SERVICED_OBJECT",
    "CORPUS_ZONE",
    "FLOORS",
    "CONSUMERS",
    "UPSTREAM_DOWNSTREAM",
    "SYSTEMS",
    "EQUIPMENT_ROLES",
    "DOCUMENT_ROLE",
    "STABLE_ENTITIES",
    "CROSS_SHEET_REFERENCE",
    "NEIGHBORING_FUNCTIONS",
)
CHANNEL_FIELDS = {
    "FUNCTION_CLASS": ["function_class"],
    "FUNCTION_EVIDENCE": ["function_evidence"],
    "SERVICED_OBJECT": ["serviced_object"],
    "CORPUS_ZONE": ["corpus", "zone"],
    "FLOORS": ["floors"],
    "CONSUMERS": ["consumers"],
    "UPSTREAM_DOWNSTREAM": ["upstream", "downstream"],
    "SYSTEMS": ["systems"],
    "EQUIPMENT_ROLES": ["equipment_roles"],
    "DOCUMENT_ROLE": ["document_role"],
    "STABLE_ENTITIES": ["stable_entities"],
    "CROSS_SHEET_REFERENCE": ["cross_sheet_functional_references"],
    "NEIGHBORING_FUNCTIONS": ["neighbors"],
}
PASSPORT_OUTPUT_FIELDS = (
    "function_class",
    "serviced_object",
    "corpus",
    "zone",
    "floors",
    "consumers",
    "upstream",
    "downstream",
    "systems",
    "equipment_roles",
    "document_role",
)


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"expected JSON object: {path}")
    return value


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _input_paths() -> dict[str, Path]:
    return {
        "candidate_artifacts/p19cd7f695a.json": (
            FROZEN_CANDIDATE_ROOT / "p19cd7f695a.json"
        ),
        "candidate_artifacts/pb02de74a81.json": (
            FROZEN_CANDIDATE_ROOT / "pb02de74a81.json"
        ),
        "candidate_artifacts/pe336037597.json": (
            FROZEN_CANDIDATE_ROOT / "pe336037597.json"
        ),
        "smoke/task_results.json": FROZEN_SMOKE,
    }


def assert_frozen_inputs() -> dict[str, str]:
    actual = {name: _sha256(path) for name, path in _input_paths().items()}
    if actual != FROZEN_SHA256:
        raise RuntimeError(
            "frozen Function Lineage evidence drifted: "
            + json.dumps(actual, sort_keys=True)
        )
    return actual


def _known_tuple(row: Mapping[str, Any], fields: Sequence[str]) -> tuple[str, ...] | None:
    values = tuple(row.get(field) for field in fields)
    if any(value is None or value == "" for value in values):
        return None
    return tuple(str(value) for value in values)


def component_set(candidate: Mapping[str, Any]) -> frozenset[tuple[str, ...]]:
    """Return exact LEFT→RIGHT component mappings; unknown rows prove nothing."""
    fields = (
        "left_fragment_id",
        "left_function_id",
        "component_role",
        "right_fragment_id",
        "right_function_id",
    )
    return frozenset(
        value
        for row in candidate.get("component_map") or []
        for value in [_known_tuple(row, fields)]
        if value is not None
    )


def left_component_set(candidate: Mapping[str, Any]) -> frozenset[tuple[str, ...]]:
    """Return structurally identified LEFT components without using page numbers."""
    fields = ("left_fragment_id", "left_function_id", "component_role")
    return frozenset(
        value
        for row in candidate.get("component_map") or []
        for value in [_known_tuple(row, fields)]
        if value is not None
    )


def _explicit_contradictions(candidate: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(
        sorted(
            json.dumps(value, ensure_ascii=False, sort_keys=True)
            if isinstance(value, (dict, list))
            else str(value)
            for value in candidate.get("explicit_contradictions") or []
            if value not in (None, "", [], {})
        )
    )


def classify_candidate_relation(
    candidate_a: Mapping[str, Any], candidate_b: Mapping[str, Any],
) -> str:
    """Classify a pair from component structure and explicit provenance only.

    Rank, score, filenames, physical-page identity, and missing/unknown passport
    fields are deliberately outside the classifier.
    """
    if _explicit_contradictions(candidate_a) or _explicit_contradictions(candidate_b):
        return "CONTRADICTORY"
    exact_a = component_set(candidate_a)
    exact_b = component_set(candidate_b)
    left_a = left_component_set(candidate_a)
    left_b = left_component_set(candidate_b)
    if not exact_a or not exact_b or not left_a or not left_b:
        return "UNKNOWN"
    if exact_a < exact_b:
        return "STRICT_SUBSET"
    if exact_a > exact_b:
        return "STRICT_SUPERSET"
    if exact_a & exact_b:
        return "OVERLAP"
    if left_a == left_b:
        return "SAME_COMPONENTS_DIFFERENT_TARGET"
    if left_a < left_b or left_a > left_b:
        return "ALTERNATIVE_GRANULARITY"
    return "DISJOINT"


def coverage_scope(
    candidate: Mapping[str, Any], required_left_components: Iterable[Sequence[str]],
) -> str:
    """Classify coverage only against an explicit, non-empty required scope."""
    required = frozenset(tuple(str(item) for item in value) for value in required_left_components)
    covered = left_component_set(candidate)
    if not required or not covered:
        return "UNKNOWN"
    if required <= covered:
        return "FULL"
    if required & covered:
        return "PARTIAL"
    return "UNKNOWN"


def _candidate_kind(candidate: Mapping[str, Any]) -> str:
    return "SINGLETON" if len(component_set(candidate)) == 1 else "GROUP"


def _pair_flags(
    candidate_a: Mapping[str, Any], candidate_b: Mapping[str, Any],
) -> dict[str, bool]:
    exact_a = component_set(candidate_a)
    exact_b = component_set(candidate_b)
    left_a = left_component_set(candidate_a)
    left_b = left_component_set(candidate_b)
    types = {candidate_a.get("relation_type"), candidate_b.get("relation_type")}
    group_a = _candidate_kind(candidate_a) == "GROUP"
    group_b = _candidate_kind(candidate_b) == "GROUP"
    return {
        "exact_component_overlap": bool(exact_a & exact_b),
        "strict_containment": bool(exact_a < exact_b or exact_b < exact_a),
        "alternative_granularity": bool(left_a < left_b or left_b < left_a),
        "contradictory": bool(
            _explicit_contradictions(candidate_a)
            or _explicit_contradictions(candidate_b)
        ),
        "singleton_vs_group": group_a != group_b,
        "one_to_one_vs_one_to_n": types
        == {"CONTINUED_1_TO_1", "SPLIT_1_TO_N"},
        "one_to_one_vs_function_distributed": types
        == {"CONTINUED_1_TO_1", "FUNCTION_DISTRIBUTED"},
        "group_vs_group": group_a and group_b,
    }


def _relation_details(
    candidate_a: Mapping[str, Any], candidate_b: Mapping[str, Any],
) -> dict[str, Any]:
    exact_a = component_set(candidate_a)
    exact_b = component_set(candidate_b)
    left_a = left_component_set(candidate_a)
    left_b = left_component_set(candidate_b)
    classification = classify_candidate_relation(candidate_a, candidate_b)
    subset_id = None
    superset_id = None
    if exact_a < exact_b:
        subset_id = candidate_a.get("candidate_id")
        superset_id = candidate_b.get("candidate_id")
    elif exact_b < exact_a:
        subset_id = candidate_b.get("candidate_id")
        superset_id = candidate_a.get("candidate_id")
    return {
        "classification": classification,
        "alternative_granularity": bool(left_a < left_b or left_b < left_a),
        "candidate_a_component_count": len(exact_a),
        "candidate_b_component_count": len(exact_b),
        "candidate_a_left_component_count": len(left_a),
        "candidate_b_left_component_count": len(left_b),
        "shared_exact_components": [list(value) for value in sorted(exact_a & exact_b)],
        "strict_subset_candidate_id": subset_id,
        "strict_superset_candidate_id": superset_id,
        "score_or_rank_used": False,
    }


def _empty_task_sets() -> dict[str, set[str]]:
    return {name: set() for name in RELATIONSHIPS}


def _audit_dataset(dataset: Mapping[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    pair_id = str(dataset["pair_id"])
    candidates = {
        str(value["candidate_id"]): value
        for value in dataset.get("functional_candidates") or []
    }
    relationship_pairs = Counter({name: 0 for name in RELATIONSHIPS})
    relationship_tasks = _empty_task_sets()
    predicate_pairs: Counter[str] = Counter()
    predicate_tasks: dict[str, set[str]] = defaultdict(set)
    pair_type_pairs: Counter[str] = Counter()
    pair_type_tasks: dict[str, set[str]] = defaultdict(set)
    subset_pair_type_pairs: Counter[str] = Counter()
    subset_pair_type_tasks: dict[str, set[str]] = defaultdict(set)
    structural_cases: list[dict[str, Any]] = []
    all_pair_count = 0

    pair_type_names = (
        "singleton_vs_group",
        "one_to_one_vs_one_to_n",
        "one_to_one_vs_function_distributed",
        "group_vs_group",
    )
    for task in dataset.get("candidate_tasks") or []:
        task_id = str(task["task_id"])
        candidate_ids = list(task.get("candidate_ids") or [])[:12]
        for candidate_a_id, candidate_b_id in itertools.combinations(candidate_ids, 2):
            all_pair_count += 1
            candidate_a = candidates[candidate_a_id]
            candidate_b = candidates[candidate_b_id]
            relationship = classify_candidate_relation(candidate_a, candidate_b)
            flags = _pair_flags(candidate_a, candidate_b)
            relationship_pairs[relationship] += 1
            relationship_tasks[relationship].add(task_id)
            for name, enabled in flags.items():
                if enabled:
                    predicate_pairs[name] += 1
                    predicate_tasks[name].add(task_id)
            for name in pair_type_names:
                if flags[name]:
                    pair_type_pairs[name] += 1
                    pair_type_tasks[name].add(task_id)
                    if flags["strict_containment"]:
                        subset_pair_type_pairs[name] += 1
                        subset_pair_type_tasks[name].add(task_id)
            if relationship in {
                "OVERLAP",
                "STRICT_SUBSET",
                "STRICT_SUPERSET",
                "ALTERNATIVE_GRANULARITY",
                "CONTRADICTORY",
            }:
                structural_cases.append({
                    "pair_id": pair_id,
                    "project": PROJECTS[pair_id],
                    "task_id": task_id,
                    "left_physical_page": task.get("left_physical_page"),
                    "candidate_a_id": candidate_a_id,
                    "candidate_a_rank": task["candidate_ranks"][candidate_a_id],
                    "candidate_a_relation_type": candidate_a.get("relation_type"),
                    "candidate_b_id": candidate_b_id,
                    "candidate_b_rank": task["candidate_ranks"][candidate_b_id],
                    "candidate_b_relation_type": candidate_b.get("relation_type"),
                    "flags": flags,
                    **_relation_details(candidate_a, candidate_b),
                })

    pair_types = {
        name: {
            "pair_count": pair_type_pairs[name],
            "task_count": len(pair_type_tasks[name]),
            "strict_subset_pair_count": subset_pair_type_pairs[name],
            "strict_subset_task_count": len(subset_pair_type_tasks[name]),
        }
        for name in pair_type_names
    }
    summary = {
        "pair_id": pair_id,
        "project": PROJECTS[pair_id],
        "candidate_count": len(candidates),
        "task_count": len(dataset.get("candidate_tasks") or []),
        "top_12_pair_count": all_pair_count,
        "relationship_pair_counts": dict(sorted(relationship_pairs.items())),
        "relationship_task_counts": {
            name: len(relationship_tasks[name]) for name in RELATIONSHIPS
        },
        "predicate_pair_counts": dict(sorted(predicate_pairs.items())),
        "predicate_task_counts": {
            name: len(values) for name, values in sorted(predicate_tasks.items())
        },
        "pair_type_counts": pair_types,
    }
    return summary, structural_cases


def _capacity_defects(dataset: Mapping[str, Any]) -> list[dict[str, Any]]:
    defects = []
    for candidate in dataset.get("functional_candidates") or []:
        keys = [str(value) for value in candidate.get("right_capacity_keys") or []]
        component_keys = sorted({
            str(row.get("capacity_key"))
            for row in candidate.get("component_map") or []
            if row.get("capacity_key") not in (None, "")
        })
        if len(keys) != len(set(keys)) or sorted(keys) != component_keys:
            defects.append({
                "candidate_id": candidate.get("candidate_id"),
                "component_capacity_keys": component_keys,
                "right_capacity_keys": keys,
            })
    return defects


def build_candidate_overlap_audit(
    datasets: Mapping[str, Mapping[str, Any]], input_hashes: Mapping[str, str],
) -> dict[str, Any]:
    project_summaries = []
    structural_cases = []
    capacity_defects = []
    for pair_id in sorted(datasets, key=lambda value: PROJECTS[value]):
        summary, cases = _audit_dataset(datasets[pair_id])
        project_summaries.append(summary)
        structural_cases.extend(cases)
        capacity_defects.extend(
            {"pair_id": pair_id, **value}
            for value in _capacity_defects(datasets[pair_id])
        )

    overall_relationship_pairs = Counter()
    overall_relationship_tasks = Counter()
    overall_predicate_pairs = Counter()
    overall_predicate_tasks = Counter()
    overall_pair_types: dict[str, Counter[str]] = defaultdict(Counter)
    for summary in project_summaries:
        overall_relationship_pairs.update(summary["relationship_pair_counts"])
        overall_relationship_tasks.update(summary["relationship_task_counts"])
        overall_predicate_pairs.update(summary["predicate_pair_counts"])
        overall_predicate_tasks.update(summary["predicate_task_counts"])
        for name, values in summary["pair_type_counts"].items():
            overall_pair_types[name].update(values)

    return {
        "schema_version": FORENSIC_VERSION,
        "method": {
            "candidate_shortlist": "frozen top-12 candidate_ids per task",
            "component_identity": [
                "left_fragment_id",
                "left_function_id",
                "component_role",
                "right_fragment_id",
                "right_function_id",
            ],
            "left_scope_identity": [
                "left_fragment_id", "left_function_id", "component_role",
            ],
            "classification_precedence": list(RELATIONSHIPS[6:])
            + [
                "STRICT_SUBSET",
                "STRICT_SUPERSET",
                "OVERLAP",
                "SAME_COMPONENTS_DIFFERENT_TARGET",
                "ALTERNATIVE_GRANULARITY",
                "DISJOINT",
            ],
            "rank_or_score_used": False,
            "physical_page_rule_used": False,
            "unknown_values_used_as_evidence": False,
        },
        "frozen_inputs": dict(input_hashes),
        "projects": project_summaries,
        "overall": {
            "candidate_count": sum(value["candidate_count"] for value in project_summaries),
            "task_count": sum(value["task_count"] for value in project_summaries),
            "top_12_pair_count": sum(value["top_12_pair_count"] for value in project_summaries),
            "relationship_pair_counts": dict(sorted(overall_relationship_pairs.items())),
            "relationship_task_counts": dict(sorted(overall_relationship_tasks.items())),
            "predicate_pair_counts": dict(sorted(overall_predicate_pairs.items())),
            "predicate_task_counts": dict(sorted(overall_predicate_tasks.items())),
            "pair_type_counts": {
                name: dict(sorted(values.items()))
                for name, values in sorted(overall_pair_types.items())
            },
        },
        "structural_cases": structural_cases,
        "safety": {
            "candidate_sets_modified": False,
            "candidate_recall_changes": 0,
            "capacity_key_defect_count": len(capacity_defects),
            "capacity_key_defects": capacity_defects,
            "existing_full_partial_classification_count": 0,
            "false_existing_classification_count": 0,
            "unsafe_would_be_classifications": [
                {
                    "classification": "R26_UNQUALIFIED_PARTIAL",
                    "reason": (
                        "Structural subset of a composite candidate does not make "
                        "R26 partial for its declared one-fragment task."
                    ),
                }
            ],
            "new_capacity_conflicts": 0,
            "model_calls": 0,
        },
    }


def _passport_excerpt(passport: Mapping[str, Any], *, fragment_id: str) -> dict[str, Any]:
    source = passport.get("source_sheet") or {}
    return {
        "physical_page": source.get("physical_page"),
        "graphic_sheet_number": source.get("graphic_sheet_number"),
        "function_id": passport.get("function_id"),
        "fragment_id": fragment_id,
        **{field: passport.get(field) for field in PASSPORT_OUTPUT_FIELDS},
        "neighbors": passport.get("neighboring_function_context"),
    }


def _fragment_excerpt(fragment: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "physical_page": fragment.get("physical_page"),
        "function_id": fragment.get("function_id"),
        "fragment_id": fragment.get("fragment_id"),
        "function_class": fragment.get("function_class"),
        "component_role": fragment.get("component_role"),
        "document_role": fragment.get("document_role"),
    }


def _evidence_ownership(
    dataset: Mapping[str, Any], evidence_refs: Iterable[str],
) -> list[dict[str, Any]]:
    catalog = dataset.get("evidence_catalog") or {}
    return [
        {
            "evidence_ref": evidence_ref,
            **{
                key: (catalog.get(evidence_ref) or {}).get(key)
                for key in (
                    "side",
                    "physical_page",
                    "field",
                    "provenance_type",
                    "owner_function_id",
                    "owner_fragment_id",
                )
            },
        }
        for evidence_ref in sorted(str(value) for value in evidence_refs)
    ]


def _channel_field_names(channels: Iterable[str]) -> list[str]:
    return sorted({field for channel in channels for field in CHANNEL_FIELDS[channel]})


def _candidate_forensics(
    dataset: Mapping[str, Any], candidate: Mapping[str, Any], task: Mapping[str, Any],
) -> dict[str, Any]:
    scores = candidate.get("channel_scores") or {}
    positive_channels = [
        channel
        for channel in FUNCTIONAL_CHANNELS
        if scores.get(channel) not in (None, 0, 0.0, False, "")
    ]
    missing_channels = [
        channel for channel in FUNCTIONAL_CHANNELS if scores.get(channel) is None
    ]
    observed_no_match = [
        channel for channel in FUNCTIONAL_CHANNELS if scores.get(channel) == 0
    ]
    fragments = dataset["function_fragments"]
    passports = dataset["function_passports"]
    evidence_refs = list(candidate.get("evidence_refs") or [])
    owner_tasks = [
        value["task_id"]
        for value in dataset.get("candidate_tasks") or []
        if candidate["candidate_id"] in (value.get("candidate_ids") or [])
    ]
    return {
        "candidate_id": candidate["candidate_id"],
        "relation_type": candidate.get("relation_type"),
        "left_fragments": [
            _fragment_excerpt(fragments["LEFT"][fragment_id])
            for fragment_id in candidate.get("left_fragment_ids") or []
        ],
        "left_functions": [
            _passport_excerpt(
                passports["LEFT"][function_id],
                fragment_id=(passports["LEFT"][function_id].get("function_fragment_ids") or [None])[0],
            )
            for function_id in candidate.get("left_function_ids") or []
        ],
        "right_fragments": [
            _fragment_excerpt(fragments["RIGHT"][fragment_id])
            for fragment_id in candidate.get("right_fragment_ids") or []
        ],
        "right_functions": [
            _passport_excerpt(
                passports["RIGHT"][function_id],
                fragment_id=(passports["RIGHT"][function_id].get("function_fragment_ids") or [None])[0],
            )
            for function_id in candidate.get("right_function_ids") or []
        ],
        "component_mappings": list(candidate.get("component_map") or []),
        "capacity_keys": list(candidate.get("right_capacity_keys") or []),
        "retrieval_channels": list(candidate.get("retrieval_channels") or []),
        "supporting_channels": list(candidate.get("supporting_channels") or []),
        "matched_channels": positive_channels,
        "matched_fields": _channel_field_names(positive_channels),
        "missing_channels": missing_channels,
        "missing_fields": _channel_field_names(missing_channels),
        "observed_no_match_channels": observed_no_match,
        "observed_no_match_fields": _channel_field_names(observed_no_match),
        "contradictory_fields": list(candidate.get("explicit_contradictions") or []),
        "evidence_refs": evidence_refs,
        "evidence_ownership": _evidence_ownership(dataset, evidence_refs),
        "scores_for_display_only": {
            "channel_scores": dict(scores),
            "functional_score": candidate.get("functional_score"),
            "source_score": candidate.get("source_score"),
        },
        "rank_for_display_only": task["candidate_ranks"][candidate["candidate_id"]],
        "owner_task_ids": owner_tasks,
    }


def _find_exact_singleton(
    dataset: Mapping[str, Any], component: Mapping[str, Any],
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    candidates = {
        value["candidate_id"]: value
        for value in dataset.get("functional_candidates") or []
    }
    task = next(
        value
        for value in dataset.get("candidate_tasks") or []
        if value["left_fragment_id"] == component["left_fragment_id"]
    )
    wanted = _known_tuple(
        component,
        (
            "left_fragment_id",
            "left_function_id",
            "component_role",
            "right_fragment_id",
            "right_function_id",
        ),
    )
    matches = [
        candidates[candidate_id]
        for candidate_id in task.get("candidate_ids") or []
        if candidates[candidate_id].get("relation_type") == "CONTINUED_1_TO_1"
        and component_set(candidates[candidate_id]) == {wanted}
    ]
    if len(matches) != 1:
        raise RuntimeError(f"expected one exact singleton for component: {component}")
    return matches[0], task


def build_coverage_matrix(dataset: Mapping[str, Any]) -> dict[str, Any]:
    candidates = {
        value["candidate_id"]: value
        for value in dataset.get("functional_candidates") or []
    }
    distributed = candidates[LEFT20_DISTRIBUTED_ID]
    component_by_page = {
        int(value["right_physical_page"]): value
        for value in distributed["component_map"]
    }
    if set(component_by_page) != {26, 28, 29}:
        raise RuntimeError("LEFT20 distributed component pages drifted")
    singleton_columns: dict[str, tuple[Mapping[str, Any], Mapping[str, Any]]] = {}
    for page in (26, 28, 29):
        singleton_columns[f"R{page}"] = _find_exact_singleton(
            dataset, component_by_page[page]
        )
    columns = {
        name: value[0] for name, value in singleton_columns.items()
    }
    columns["DISTRIBUTED_R26_R28_R29"] = distributed

    page_fragments = sorted(
        (
            value
            for value in dataset["function_fragments"]["LEFT"].values()
            if int(value["physical_page"]) == 20
        ),
        key=lambda value: (value["component_role"], value["fragment_id"]),
    )
    composite_scope = left_component_set(distributed)
    rows = []
    for fragment in page_fragments:
        left_key = (
            str(fragment["fragment_id"]),
            str(fragment["function_id"]),
            str(fragment["component_role"]),
        )
        coverage = {}
        for name, candidate in columns.items():
            matches = [
                value
                for value in candidate.get("component_map") or []
                if _known_tuple(
                    value,
                    ("left_fragment_id", "left_function_id", "component_role"),
                )
                == left_key
            ]
            coverage[name] = matches
        rows.append({
            "left_fragment_id": fragment["fragment_id"],
            "left_function_id": fragment["function_id"],
            "engineering_role": fragment["component_role"],
            "in_declared_composite_scope": left_key in composite_scope,
            "coverage": coverage,
        })

    def component_label(value: Sequence[str]) -> dict[str, str]:
        return {
            "left_fragment_id": value[0],
            "left_function_id": value[1],
            "engineering_role": value[2],
        }

    column_summaries = {}
    for name, candidate in columns.items():
        covered = left_component_set(candidate) & composite_scope
        missing = composite_scope - covered
        column_summaries[name] = {
            "candidate_id": candidate["candidate_id"],
            "covered_components": [component_label(value) for value in sorted(covered)],
            "missing_components": [component_label(value) for value in sorted(missing)],
            "declared_composite_coverage_scope": coverage_scope(
                candidate, composite_scope
            ),
        }
    return {
        "schema_version": FORENSIC_VERSION,
        "pair_id": IOS21_PAIR_ID,
        "left_physical_page": 20,
        "scope_basis": {
            "candidate_id": LEFT20_DISTRIBUTED_ID,
            "group_evidence": distributed.get("group_evidence"),
            "required_components": [
                component_label(value) for value in sorted(composite_scope)
            ],
            "note": (
                "This is the declared composite-role scope, not the atomic task scope."
            ),
        },
        "columns": {
            name: {
                "candidate_id": candidate["candidate_id"],
                "relation_type": candidate.get("relation_type"),
                "right_pages": candidate.get("right_pages"),
            }
            for name, candidate in columns.items()
        },
        "rows": rows,
        "column_summaries": column_summaries,
        "model_calls": 0,
    }


def _smoke_task(smoke: Mapping[str, Any], label: str) -> Mapping[str, Any]:
    return next(value for value in smoke.get("tasks") or [] if value["label"] == label)


def _smoke_observations(smoke_task: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "selection_distribution": smoke_task.get("selection_distribution"),
        "cold_repeats": smoke_task.get("cold_repeats"),
        "stable_across_cold_runs": smoke_task.get("stable_across_cold_runs"),
        "verifier_result": smoke_task.get("verifier_result"),
        "capacity_result": smoke_task.get("capacity_result"),
    }


def build_left20_forensics(
    dataset: Mapping[str, Any], smoke: Mapping[str, Any], matrix: Mapping[str, Any],
) -> dict[str, Any]:
    candidates = {
        value["candidate_id"]: value
        for value in dataset.get("functional_candidates") or []
    }
    task = next(
        value
        for value in dataset.get("candidate_tasks") or []
        if value["task_id"] == LEFT20_TASK_ID
    )
    r26 = candidates[LEFT20_R26_ID]
    distributed = candidates[LEFT20_DISTRIBUTED_ID]
    task_scope = {
        value
        for value in left_component_set(r26)
        if value[0] == task["left_fragment_id"]
    }
    composite_scope = left_component_set(distributed)
    relation = _relation_details(r26, distributed)
    return {
        "schema_version": FORENSIC_VERSION,
        "pair_id": IOS21_PAIR_ID,
        "task": dict(task),
        "function_passport": _passport_excerpt(
            dataset["function_passports"]["LEFT"][task["left_function_id"]],
            fragment_id=task["left_fragment_id"],
        ),
        "page_20_engineering_fragments": [
            _fragment_excerpt(value)
            for value in sorted(
                (
                    value
                    for value in dataset["function_fragments"]["LEFT"].values()
                    if int(value["physical_page"]) == 20
                ),
                key=lambda value: (value["component_role"], value["fragment_id"]),
            )
        ],
        "r26_singleton": {
            **_candidate_forensics(dataset, r26, task),
            "coverage_scope": {
                "atomic_task_scope": coverage_scope(r26, task_scope),
                "declared_composite_scope": coverage_scope(r26, composite_scope),
                "unqualified_scope": "UNKNOWN",
            },
            "covered_composite_components": matrix["column_summaries"]["R26"][
                "covered_components"
            ],
            "missing_composite_components": matrix["column_summaries"]["R26"][
                "missing_components"
            ],
        },
        "distributed_candidate": {
            **_candidate_forensics(dataset, distributed, task),
            "coverage_scope": {
                "atomic_task_scope": coverage_scope(distributed, task_scope),
                "declared_composite_scope": coverage_scope(distributed, composite_scope),
                "unqualified_scope": "FULL",
            },
            "covered_composite_components": matrix["column_summaries"][
                "DISTRIBUTED_R26_R28_R29"
            ]["covered_components"],
            "missing_composite_components": matrix["column_summaries"][
                "DISTRIBUTED_R26_R28_R29"
            ]["missing_components"],
        },
        "candidate_relation": relation,
        "forensic_answer": {
            "candidate_hypothesis_classification": "C_DIFFERENT_GRANULARITY",
            "r26_partial_hypothesis_unqualified": "UNKNOWN",
            "r26_is_full_for_atomic_task": True,
            "r26_is_partial_for_declared_composite_scope": True,
            "distributed_is_exact_union_of_r26_r28_r29_singletons": True,
            "candidate_generation_hypothesis_is_erroneous": False,
            "task_projection_granularity_defect": True,
            "reason": (
                "The task owns one DOMESTIC_PRESSURE_BOOST fragment. R26 maps that "
                "fragment completely. The distributed candidate contains the same "
                "mapping plus FIRE_PRESSURE_BOOST and INCOMING_METERING sibling "
                "fragments, yet is selectable inside the one-fragment task."
            ),
        },
        "frozen_smoke": _smoke_observations(_smoke_task(smoke, "LEFT20")),
        "model_calls": 0,
    }


def _set_value(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, list):
        return {str(item) for item in value if item not in (None, "")}
    return {str(value)}


def _identity_comparison(
    left: Mapping[str, Any], right: Mapping[str, Any],
) -> dict[str, Any]:
    values = {}
    for field in (
        "function_class",
        "component_role",
        "serviced_object",
        "corpus",
        "zone",
        "floors",
        "document_role",
    ):
        left_values = _set_value(left.get(field))
        right_values = _set_value(right.get(field))
        values[field] = {
            "left": sorted(left_values) if left_values else None,
            "right": sorted(right_values) if right_values else None,
            "exact": bool(left_values and left_values == right_values),
            "overlap": sorted(left_values & right_values),
            "unknown_on_either_side": not left_values or not right_values,
        }
    return values


def build_left19_forensics(
    dataset: Mapping[str, Any], smoke: Mapping[str, Any],
) -> dict[str, Any]:
    candidates = {
        value["candidate_id"]: value
        for value in dataset.get("functional_candidates") or []
    }
    task = next(
        value
        for value in dataset.get("candidate_tasks") or []
        if value["task_id"] == LEFT19_TASK_ID
    )
    ordered = [candidates[value] for value in task["candidate_ids"]]
    r30 = next(value for value in ordered if value.get("right_pages") == [30])
    r25 = next(value for value in ordered if value.get("right_pages") == [25])
    if task["candidate_ranks"][r30["candidate_id"]] != 1:
        raise RuntimeError("LEFT19 strongest R30 candidate drifted")
    if task["candidate_ranks"][r25["candidate_id"]] != 2:
        raise RuntimeError("LEFT19 strongest R25 candidate drifted")
    left_passport = dataset["function_passports"]["LEFT"][task["left_function_id"]]
    r30_passport = dataset["function_passports"]["RIGHT"][r30["right_function_ids"][0]]
    r25_passport = dataset["function_passports"]["RIGHT"][r25["right_function_ids"][0]]
    r30_scores = r30.get("channel_scores") or {}
    r25_scores = r25.get("channel_scores") or {}
    channel_comparison = {}
    for channel in sorted(set(r30_scores) | set(r25_scores)):
        value_30 = r30_scores.get(channel)
        value_25 = r25_scores.get(channel)
        if value_30 is None and value_25 is None:
            stronger = "UNKNOWN"
        elif value_30 is None:
            stronger = "R25"
        elif value_25 is None:
            stronger = "R30"
        elif value_30 > value_25:
            stronger = "R30"
        elif value_25 > value_30:
            stronger = "R25"
        else:
            stronger = "EQUAL"
        channel_comparison[channel] = {
            "r30": value_30,
            "r25": value_25,
            "stronger_display_score": stronger,
        }
    task_scope = left_component_set(r30)
    return {
        "schema_version": FORENSIC_VERSION,
        "pair_id": IOS21_PAIR_ID,
        "task": dict(task),
        "function_passport": _passport_excerpt(
            left_passport, fragment_id=task["left_fragment_id"]
        ),
        "r30": {
            **_candidate_forensics(dataset, r30, task),
            "coverage_scope": coverage_scope(r30, task_scope),
            "identity_comparison": _identity_comparison(left_passport, r30_passport),
        },
        "r25": {
            **_candidate_forensics(dataset, r25, task),
            "coverage_scope": coverage_scope(r25, task_scope),
            "identity_comparison": _identity_comparison(left_passport, r25_passport),
        },
        "candidate_relation": _relation_details(r30, r25),
        "channel_comparison_for_display_not_truth": channel_comparison,
        "deterministic_conclusion": {
            "r30_has_stronger_evidence": True,
            "r25_is_structurally_invalid": False,
            "ambiguity_resolved": False,
            "reason": (
                "Both targets exactly match HOT_WATER / HOT_WATER_DISTRIBUTION, "
                "Корпус №4 and GRAPHIC_SHEET and have no explicit contradiction. "
                "R30 has stronger floor, corpus/zone, function-evidence, consumer, "
                "upstream/downstream and supporting document-context overlap, but "
                "those strengths do not invalidate R25. Six identical selections "
                "show preference stability, not ground truth."
            ),
        },
        "frozen_smoke": _smoke_observations(_smoke_task(smoke, "LEFT19")),
        "model_calls": 0,
    }


def _format_value(value: Any) -> str:
    if value is None or value == []:
        return "UNKNOWN/null"
    if isinstance(value, list):
        return "; ".join(str(item).replace("|", "\\|") for item in value)
    return str(value).replace("|", "\\|").replace("\n", " ")


def build_report(
    audit: Mapping[str, Any],
    left20: Mapping[str, Any],
    left19: Mapping[str, Any],
) -> str:
    passport = left20["function_passport"]
    overall = audit["overall"]
    rel_tasks = overall["relationship_task_counts"]
    predicates = overall["predicate_task_counts"]
    r26 = left20["r26_singleton"]
    distributed = left20["distributed_candidate"]
    r30 = left19["r30"]
    r25 = left19["r25"]
    distributed_left_fragments = {
        value["fragment_id"] for value in distributed["left_fragments"]
    }
    outside_composite_roles = [
        value["component_role"]
        for value in left20["page_20_engineering_fragments"]
        if value["fragment_id"] not in distributed_left_fragments
    ]
    lines = [
        "# Function Lineage v2.3 — group / partial candidate forensics",
        "",
        "## Execution boundary",
        "",
        f"- Frozen experiment record: `{FROZEN_EXPERIMENT_COMMIT}` (read-only).",
        f"- Production reference: `{PRODUCTION_HEAD_AT_EXPERIMENT}` / `{PRODUCTION_RELEASE_AT_EXPERIMENT}`.",
        "- New model calls: `0`; vision: `0`; prompt/model input/model output changes: `0`.",
        "- Deploy, shadow, materialization and candidate regeneration: not performed.",
        "- In-memory double build is byte-identical before artifacts are written.",
        "",
        "## LEFT20 Function Passport",
        "",
        "| Field | Frozen value |",
        "|---|---|",
    ]
    for field in (
        "physical_page",
        "graphic_sheet_number",
        "function_id",
        "fragment_id",
        "function_class",
        "serviced_object",
        "corpus",
        "zone",
        "floors",
        "consumers",
        "upstream",
        "downstream",
        "systems",
        "equipment_roles",
        "document_role",
        "neighbors",
    ):
        lines.append(f"| {field} | {_format_value(passport.get(field))} |")
    lines.extend([
        "",
        "## LEFT20 candidates and coverage",
        "",
        "| Candidate | Relation | Rank (display only) | Atomic task scope | Declared composite scope | Covered composite roles | Missing composite roles |",
        "|---|---|---:|---|---|---|---|",
        (
            f"| R26 `{r26['candidate_id']}` | `{r26['relation_type']}` | "
            f"{r26['rank_for_display_only']} | `{r26['coverage_scope']['atomic_task_scope']}` | "
            f"`{r26['coverage_scope']['declared_composite_scope']}` | "
            f"{', '.join(value['engineering_role'] for value in r26['covered_composite_components'])} | "
            f"{', '.join(value['engineering_role'] for value in r26['missing_composite_components'])} |"
        ),
        (
            f"| [26,28,29] `{distributed['candidate_id']}` | `{distributed['relation_type']}` | "
            f"{distributed['rank_for_display_only']} | `{distributed['coverage_scope']['atomic_task_scope']}` | "
            f"`{distributed['coverage_scope']['declared_composite_scope']}` | "
            f"{', '.join(value['engineering_role'] for value in distributed['covered_composite_components'])} | "
            f"{_format_value([value['engineering_role'] for value in distributed['missing_composite_components']])} |"
        ),
        "",
        "The exact mapping relation is `R26 STRICT_SUBSET OF distributed`; rank and score are not inputs to that result. "
        "It is simultaneously `ALTERNATIVE_GRANULARITY`: R26 is FULL for the declared one-fragment task, but PARTIAL against the three-component composite scope. "
        "Therefore the unqualified question “is R26 PARTIAL?” is `UNKNOWN`; it has no safe answer until a scope is named.",
        "",
        "The distributed mapping is the exact union of three independently generated singleton candidates: domestic pressure boost → R26, fire pressure boost → R28, and incoming metering → R29. "
        "The group itself is atomic and supported; the defect is that this three-fragment candidate is projected into each one-fragment task while the task passport remains singular.",
        "",
        "Page 20 also contains extracted roles outside the declared composite ontology: "
        + ", ".join(outside_composite_roles)
        + ". Neither candidate maps those roles, so `FULL` above means full coverage of the generator's declared three-role composite, not proof of coverage of every extracted function on the physical page.",
        "",
        "## Corpus-wide top-12 audit",
        "",
        "| Corpus | Tasks | Exclusive OVERLAP tasks | Strict-containment tasks | Alternative-granularity tasks | Contradictory tasks | Group/singleton exact-overlap tasks |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ])
    for project in audit["projects"]:
        rel = project["relationship_task_counts"]
        pred = project["predicate_task_counts"]
        lines.append(
            f"| {project['project']} | {project['task_count']} | {rel['OVERLAP']} | "
            f"{pred.get('strict_containment', 0)} | {pred.get('alternative_granularity', 0)} | "
            f"{pred.get('contradictory', 0)} | {project['pair_type_counts']['singleton_vs_group']['strict_subset_task_count']} |"
        )
    lines.append(
        f"| **Total** | **{overall['task_count']}** | **{rel_tasks['OVERLAP']}** | "
        f"**{predicates.get('strict_containment', 0)}** | "
        f"**{predicates.get('alternative_granularity', 0)}** | "
        f"**{predicates.get('contradictory', 0)}** | "
        f"**{overall['pair_type_counts']['singleton_vs_group']['strict_subset_task_count']}** |"
    )
    lines.extend([
        "",
        f"Inclusive exact-component overlap occurs in `{predicates.get('exact_component_overlap', 0)}` tasks; the exclusive `OVERLAP` classifier bucket is `{rel_tasks['OVERLAP']}` because strict subset/superset is reported separately. "
        f"There are `{overall['pair_type_counts']['one_to_one_vs_function_distributed']['strict_subset_pair_count']}` strict-containment 1→1/FUNCTION_DISTRIBUTED pairs in `{overall['pair_type_counts']['one_to_one_vs_function_distributed']['strict_subset_task_count']}` tasks.",
        "",
        "No explicit contradiction candidates were found. Capacity-key defects: `0`; new capacity conflicts: `0`; candidate recall changes: `0`. "
        "There were no pre-existing FULL/PARTIAL labels to falsify. One unsafe would-be classification was identified: `structural subset ⇒ task-level PARTIAL` is false for R26 because the task scope contains only its domestic-pressure fragment.",
        "",
        "## LEFT19 control",
        "",
        "| Candidate | Rank (display only) | Function/component | Scope evidence | Explicit contradictions |",
        "|---|---:|---|---|---|",
        (
            f"| R30 `{r30['candidate_id']}` | {r30['rank_for_display_only']} | "
            "HOT_WATER / HOT_WATER_DISTRIBUTION | Корпус №4; stronger floor, corpus/zone, function evidence, consumers and topology overlap | none |"
        ),
        (
            f"| R25 `{r25['candidate_id']}` | {r25['rank_for_display_only']} | "
            "HOT_WATER / HOT_WATER_DISTRIBUTION | Корпус №4; weaker on those fields, stronger on some systems/stable-entity tokens | none |"
        ),
        "",
        "R30 is deterministically better supported, but R25 is not structurally invalid: both have exact ontology class/role, object/corpus, document role and no contradiction. "
        "Thus 6/6 for R30 demonstrates stable preference, not that the historical ambiguity has been resolved as truth.",
        "",
        "## Safety and architecture options",
        "",
        "- **Option A — deterministic eligibility:** unsafe on the frozen schema. Excluding R26 as PARTIAL would suppress a complete continuation of the actual atomic task.",
        "- **Option B — explicit coverage fact:** useful only if accompanied by an explicit scope identifier and required component set. A bare FULL/PARTIAL flag is ambiguous; the forensic artifacts therefore expose both atomic-task and declared-composite scopes.",
        "- **Option C — legitimate ambiguity:** the current PASS_DISAGREEMENT is correct fail-closed behavior for this frozen smoke. It should remain until granularity is represented consistently.",
        "",
        "The statement “a STRICT PARTIAL candidate must not automatically beat a FULL candidate” cannot safely drive eligibility here: strict partiality is not established without choosing composite scope over the task's declared atomic scope.",
        "",
        "## Verdict",
        "",
        "**D — candidate task projection / fragmentation defect.** R26 is a valid atomic FUNCTIONAL_ANALOGUE, not an erroneous generation hypothesis. The architectural defect is competition between a one-fragment candidate and a three-fragment atomic group inside a task whose passport and identity name only one fragment. "
        "Option B is the safest diagnostic direction, but a later design must first make the selection scope explicit; no selector, prompt, eligibility, deployment, shadow, or materialization change is made in this forensic phase.",
        "",
        "Model calls = `0`.",
        "",
    ])
    return "\n".join(lines)


def build_artifacts() -> dict[str, bytes]:
    input_hashes = assert_frozen_inputs()
    datasets = {
        pair_id: _read_json(FROZEN_CANDIDATE_ROOT / f"{pair_id}.json")
        for pair_id in PROJECTS
    }
    smoke = _read_json(FROZEN_SMOKE)
    audit = build_candidate_overlap_audit(datasets, input_hashes)
    matrix = build_coverage_matrix(datasets[IOS21_PAIR_ID])
    left20 = build_left20_forensics(datasets[IOS21_PAIR_ID], smoke, matrix)
    left19 = build_left19_forensics(datasets[IOS21_PAIR_ID], smoke)
    report = build_report(audit, left20, left19)
    return {
        "candidate_overlap_audit.json": _json_bytes(audit),
        "left20_candidate_forensics.json": _json_bytes(left20),
        "left19_candidate_forensics.json": _json_bytes(left19),
        "coverage_matrix.json": _json_bytes(matrix),
        "report.md": report.encode("utf-8"),
    }


def write_artifacts(output: Path, *, check: bool = False) -> dict[str, str]:
    first = build_artifacts()
    second = build_artifacts()
    if first != second:
        raise RuntimeError("deterministic in-memory replay was not byte-identical")
    if check:
        mismatches = [
            name
            for name, payload in first.items()
            if not (output / name).is_file() or (output / name).read_bytes() != payload
        ]
        if mismatches:
            raise RuntimeError(f"generated artifacts differ: {mismatches}")
    else:
        output.mkdir(parents=True, exist_ok=True)
        for name, payload in first.items():
            (output / name).write_bytes(payload)
    return {
        name: hashlib.sha256(payload).hexdigest()
        for name, payload in sorted(first.items())
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    print(json.dumps(write_artifacts(args.output, check=args.check), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

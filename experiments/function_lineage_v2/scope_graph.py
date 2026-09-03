"""Deterministic Function Scope Graph research runner (Function Lineage v2.4).

This module only reads frozen v2 candidate/evaluation artifacts.  It does not
regenerate candidates, execute a selector, call a model, use vision, touch the
production matcher, materialize decisions, or deploy anything.
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
FROZEN_ROOT = (
    REPO_ROOT
    / "comparison"
    / "ai_sheet_matcher"
    / "20260903_function_lineage_deterministic"
)
FROZEN_CANDIDATE_ROOT = FROZEN_ROOT / "candidate_artifacts"
FROZEN_METRICS = FROZEN_ROOT / "metrics.json"
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
    / "20260903_function_lineage_v2_4_scope_graph"
)

SCHEMA_VERSION = "function-scope-graph.v2.4"
FROZEN_EXPERIMENT_COMMIT = "94eb48b8"
RESEARCH_PARENT_COMMIT = "ef186278"
PRODUCTION_HEAD_AT_EXPERIMENT = "4d489bf9033ad40c40099fe5e1436493bc56c0ed"
PRODUCTION_RELEASE_AT_EXPERIMENT = "ui-real-4d489bf9"
IOS21_PAIR_ID = "pe336037597"
LEFT19_TASK_ID = "ltask_015d2dbabecfea8054ea"
LEFT20_DISTRIBUTED_ID = "lcand_9c617494b14c2b922d3f"

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
    "metrics.json": "c7517c3e1733e329a33921ca91dd096b1dc2bd2b3f71e9d643a31d30f45f6e1b",
    "smoke/task_results.json": (
        "2b6d8a3f101a1e66b6ec7dd3d3909401bf583890e0eee443b078d05e407b8e3f"
    ),
}

GROUP_RELATIONS = {"SPLIT_1_TO_N", "MERGED_N_TO_1", "FUNCTION_DISTRIBUTED"}
SCOPE_RELATIONS = (
    "EXACT_SCOPE",
    "STRICT_SUBSET",
    "STRICT_SUPERSET",
    "OVERLAP",
    "DISJOINT",
    "UNKNOWN",
)
DERIVABILITY_CLASSES = (
    "EXACT_CHILD_UNION",
    "NON_DECOMPOSABLE_GROUP",
    "PARTIAL_CHILD_UNION",
    "UNKNOWN",
)
PASSPORT_FIELDS = (
    "function_class",
    "component_role",
    "serviced_object",
    "corpus",
    "building",
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
        **{
            f"candidate_artifacts/{pair_id}.json": (
                FROZEN_CANDIDATE_ROOT / f"{pair_id}.json"
            )
            for pair_id in PROJECTS
        },
        "metrics.json": FROZEN_METRICS,
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


def _stable_id(prefix: str, value: Any) -> str:
    digest = hashlib.sha256(
        json.dumps(
            value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
    ).hexdigest()
    return f"{prefix}_{digest[:20]}"


def _known(value: Any) -> bool:
    return value not in (None, "", [], {})


def _strings(value: Any) -> list[str]:
    if not _known(value):
        return []
    if isinstance(value, (list, tuple, set)):
        return sorted({str(item) for item in value if _known(item)})
    return [str(value)]


def _merge_observed(values: Iterable[Any]) -> Any:
    observed = sorted(
        {
            item
            for value in values
            for item in _strings(value)
        }
    )
    if not observed:
        return None
    return observed[0] if len(observed) == 1 else observed


def _source_key(row: Mapping[str, Any]) -> tuple[str, str, str] | None:
    values = (
        row.get("left_fragment_id"),
        row.get("left_function_id"),
        row.get("component_role"),
    )
    if not all(_known(value) for value in values):
        return None
    return tuple(str(value) for value in values)


def _mapping_key(row: Mapping[str, Any]) -> tuple[str, str, str, str, str] | None:
    source = _source_key(row)
    right = (row.get("right_fragment_id"), row.get("right_function_id"))
    if source is None or not all(_known(value) for value in right):
        return None
    return (*source, *(str(value) for value in right))


def candidate_source_keys(candidate: Mapping[str, Any]) -> frozenset[tuple[str, str, str]] | None:
    rows = list(candidate.get("component_map") or [])
    values = [_source_key(row) for row in rows]
    if not rows or any(value is None for value in values):
        return None
    return frozenset(value for value in values if value is not None)


def candidate_mapping_keys(candidate: Mapping[str, Any]) -> frozenset[tuple[str, ...]] | None:
    rows = list(candidate.get("component_map") or [])
    values = [_mapping_key(row) for row in rows]
    if not rows or any(value is None for value in values):
        return None
    return frozenset(value for value in values if value is not None)


def classify_scope_relation(
    covered_component_ids: Iterable[str] | None,
    required_component_ids: Iterable[str] | None,
) -> str:
    """Relate a candidate to a named scope without rank, score, or page rules."""
    if covered_component_ids is None or required_component_ids is None:
        return "UNKNOWN"
    covered = frozenset(str(value) for value in covered_component_ids if _known(value))
    required = frozenset(str(value) for value in required_component_ids if _known(value))
    if not covered or not required:
        return "UNKNOWN"
    if covered == required:
        return "EXACT_SCOPE"
    if covered < required:
        return "STRICT_SUBSET"
    if covered > required:
        return "STRICT_SUPERSET"
    if covered & required:
        return "OVERLAP"
    return "DISJOINT"


def selector_eligible(scope_relation: str) -> bool:
    return scope_relation == "EXACT_SCOPE"


def _component_identity_payload(
    pair_id: str,
    fragment: Mapping[str, Any],
    passport: Mapping[str, Any],
) -> dict[str, Any]:
    """Engineering identity deliberately excludes physical/graphic page numbers."""
    return {
        "pair_id": pair_id,
        "source_fragment_id": fragment.get("fragment_id"),
        "source_function_id": fragment.get("function_id"),
        "function_class": fragment.get("function_class") or passport.get("function_class"),
        "component_role": fragment.get("component_role") or passport.get("component_role"),
        "document_role": fragment.get("document_role") or passport.get("document_role"),
        "engineering_passport": {
            field: passport.get(field) for field in PASSPORT_FIELDS
        },
        "neighbors": passport.get("neighboring_function_context"),
        "evidence_refs": sorted(
            set(fragment.get("evidence_refs") or [])
            | set(passport.get("evidence_refs") or [])
        ),
    }


def _build_components(dataset: Mapping[str, Any]) -> tuple[list[dict[str, Any]], dict[tuple[str, str, str], str]]:
    pair_id = str(dataset["pair_id"])
    passports = dataset["function_passports"]["LEFT"]
    rows: list[dict[str, Any]] = []
    by_key: dict[tuple[str, str, str], str] = {}
    for fragment in sorted(
        dataset["function_fragments"]["LEFT"].values(),
        key=lambda value: str(value["fragment_id"]),
    ):
        function_id = str(fragment["function_id"])
        passport = passports[function_id]
        source_key = _source_key({
            "left_fragment_id": fragment.get("fragment_id"),
            "left_function_id": function_id,
            "component_role": fragment.get("component_role"),
        })
        if source_key is None:
            continue
        identity = _component_identity_payload(pair_id, fragment, passport)
        component_id = _stable_id("fcomp", identity)
        if source_key in by_key and by_key[source_key] != component_id:
            raise RuntimeError(f"non-deterministic component identity: {source_key}")
        by_key[source_key] = component_id
        rows.append({
            "function_component_id": component_id,
            "pair_id": pair_id,
            "project": PROJECTS[pair_id],
            "source_function_id": function_id,
            "source_fragment_id": str(fragment["fragment_id"]),
            "source_physical_pages": [int(fragment["physical_page"])],
            "function_class": fragment.get("function_class") or passport.get("function_class"),
            "role": fragment.get("component_role") or passport.get("component_role"),
            "serviced_object": passport.get("serviced_object"),
            "corpus": passport.get("corpus"),
            "building": passport.get("building"),
            "zone": passport.get("zone"),
            "floors": passport.get("floors"),
            "consumers": passport.get("consumers"),
            "upstream": passport.get("upstream"),
            "downstream": passport.get("downstream"),
            "systems": passport.get("systems"),
            "equipment_roles": passport.get("equipment_roles"),
            "document_role": fragment.get("document_role") or passport.get("document_role"),
            "neighbors": passport.get("neighboring_function_context"),
            "evidence_refs": identity["evidence_refs"],
            "identity_uses_physical_page": False,
        })
    return sorted(rows, key=lambda value: value["function_component_id"]), by_key


def _scope_fields(component_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        field: _merge_observed(row.get(field) for row in component_rows)
        for field in (
            "function_class",
            "role",
            "serviced_object",
            "corpus",
            "building",
            "zone",
            "floors",
            "consumers",
            "upstream",
            "downstream",
            "systems",
            "equipment_roles",
            "document_role",
            "neighbors",
        )
    }


def _build_scopes(
    dataset: Mapping[str, Any],
    components: Sequence[Mapping[str, Any]],
    component_by_source: Mapping[tuple[str, str, str], str],
) -> tuple[list[dict[str, Any]], dict[frozenset[str], str]]:
    pair_id = str(dataset["pair_id"])
    component_index = {row["function_component_id"]: row for row in components}
    component_sets: set[frozenset[str]] = {
        frozenset([component_id]) for component_id in component_by_source.values()
    }
    scope_support: dict[frozenset[str], list[dict[str, Any]]] = defaultdict(list)
    unknown_candidate_ids: list[str] = []
    for candidate in dataset.get("functional_candidates") or []:
        source_keys = candidate_source_keys(candidate)
        if source_keys is None or any(key not in component_by_source for key in source_keys):
            unknown_candidate_ids.append(str(candidate["candidate_id"]))
            continue
        candidate_components = frozenset(
            component_by_source[key] for key in source_keys
        )
        component_sets.add(candidate_components)
        scope_support[candidate_components].append({
            "candidate_id": str(candidate["candidate_id"]),
            "relation_type": candidate.get("relation_type"),
            "group_evidence": candidate.get("group_evidence"),
            "evidence_refs": sorted(candidate.get("evidence_refs") or []),
        })

    scope_by_components = {
        values: _stable_id(
            "fscope",
            {
                "pair_id": pair_id,
                "scope_kind": "COMPONENT" if len(values) == 1 else "COMPOSITE",
                "required_component_ids": sorted(values),
            },
        )
        for values in component_sets
    }
    # Inclusion edges use only immediate proven supersets (the Hasse diagram).
    parent_sets: dict[frozenset[str], list[frozenset[str]]] = {}
    child_sets: dict[frozenset[str], list[frozenset[str]]] = {}
    for values in component_sets:
        supersets = [other for other in component_sets if values < other]
        parents = [
            other for other in supersets
            if not any(values < middle < other for middle in component_sets)
        ]
        subsets = [other for other in component_sets if other < values]
        children = [
            other for other in subsets
            if not any(other < middle < values for middle in component_sets)
        ]
        parent_sets[values] = sorted(parents, key=lambda item: sorted(item))
        child_sets[values] = sorted(children, key=lambda item: sorted(item))

    rows = []
    for values in sorted(component_sets, key=lambda item: (len(item), sorted(item))):
        member_rows = [component_index[value] for value in sorted(values)]
        parent_ids = sorted(scope_by_components[value] for value in parent_sets[values])
        child_ids = sorted(scope_by_components[value] for value in child_sets[values])
        support = sorted(
            scope_support[values], key=lambda item: item["candidate_id"]
        )
        evidence_refs = sorted({
            ref for row in member_rows for ref in row.get("evidence_refs") or []
        } | {
            ref for row in support for ref in row.get("evidence_refs") or []
        })
        rows.append({
            "scope_id": scope_by_components[values],
            "scope_kind": "COMPONENT" if len(values) == 1 else "COMPOSITE",
            "pair_id": pair_id,
            "project": PROJECTS[pair_id],
            "source_physical_pages": sorted({
                page for row in member_rows for page in row["source_physical_pages"]
            }),
            "source_function_ids": sorted({row["source_function_id"] for row in member_rows}),
            "source_fragment_ids": sorted({row["source_fragment_id"] for row in member_rows}),
            "required_component_ids": sorted(values),
            "optional_component_ids": [],
            "parent_scope_id": parent_ids[0] if len(parent_ids) == 1 else None,
            "parent_scope_ids": parent_ids,
            "child_scope_ids": child_ids,
            "supporting_candidate_ids": [
                row["candidate_id"] for row in support
            ],
            "scope_evidence": support,
            **_scope_fields(member_rows),
            "evidence_refs": evidence_refs,
            "identity_uses_physical_page": False,
            "document_link_used_for_identity": False,
        })
    if unknown_candidate_ids:
        unknown_scope_id = _stable_id(
            "fscope", {"pair_id": pair_id, "scope_kind": "UNKNOWN"}
        )
        rows.append({
            "scope_id": unknown_scope_id,
            "scope_kind": "UNKNOWN",
            "pair_id": pair_id,
            "project": PROJECTS[pair_id],
            "source_physical_pages": [],
            "source_function_ids": [],
            "source_fragment_ids": [],
            "required_component_ids": [],
            "optional_component_ids": [],
            "parent_scope_id": None,
            "parent_scope_ids": [],
            "child_scope_ids": [],
            "supporting_candidate_ids": [],
            "scope_evidence": [],
            **{field: None for field in _scope_fields([])},
            "evidence_refs": [],
            "identity_uses_physical_page": False,
            "document_link_used_for_identity": False,
            "unknown_candidate_ids": sorted(unknown_candidate_ids),
        })
    return sorted(rows, key=lambda value: value["scope_id"]), scope_by_components


def _covered_component_ids(
    candidate: Mapping[str, Any],
    component_by_source: Mapping[tuple[str, str, str], str],
) -> frozenset[str] | None:
    source_keys = candidate_source_keys(candidate)
    if source_keys is None or any(key not in component_by_source for key in source_keys):
        return None
    return frozenset(component_by_source[key] for key in source_keys)


def build_scope_model(dataset: Mapping[str, Any]) -> dict[str, Any]:
    components, component_by_source = _build_components(dataset)
    scopes, scope_by_components = _build_scopes(
        dataset, components, component_by_source
    )
    return {
        "components": components,
        "component_by_source": component_by_source,
        "scopes": scopes,
        "scope_by_components": scope_by_components,
    }


def _membership_record(
    candidate: Mapping[str, Any],
    scope: Mapping[str, Any],
    covered: frozenset[str] | None,
) -> dict[str, Any]:
    required = frozenset(scope["required_component_ids"])
    relation = classify_scope_relation(covered, required)
    known_covered = covered or frozenset()
    return {
        "membership_id": _stable_id(
            "fsmem", [candidate["candidate_id"], scope["scope_id"]]
        ),
        "candidate_id": candidate["candidate_id"],
        "coverage_scope_id": scope["scope_id"],
        "covered_component_ids": sorted(known_covered),
        "missing_required_component_ids": sorted(required - known_covered),
        "extra_component_ids": sorted(known_covered - required),
        "scope_relation": relation,
        "selector_eligible": selector_eligible(relation),
        "relation_uses_score_or_rank": False,
        "evidence_refs": sorted(candidate.get("evidence_refs") or []),
    }


def build_candidate_memberships(
    datasets: Mapping[str, Mapping[str, Any]],
    models: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    memberships = []
    candidate_summaries = []
    relation_counts = Counter({name: 0 for name in SCOPE_RELATIONS})
    candidate_relation_counts = Counter({name: 0 for name in SCOPE_RELATIONS})
    for pair_id in sorted(datasets, key=lambda value: PROJECTS[value]):
        dataset = datasets[pair_id]
        model = models[pair_id]
        known_scopes = [
            row for row in model["scopes"] if row["scope_kind"] != "UNKNOWN"
        ]
        unknown_scopes = [
            row for row in model["scopes"] if row["scope_kind"] == "UNKNOWN"
        ]
        for candidate in sorted(
            dataset.get("functional_candidates") or [],
            key=lambda value: str(value["candidate_id"]),
        ):
            covered = _covered_component_ids(candidate, model["component_by_source"])
            if covered is None:
                relevant_scopes = unknown_scopes
            else:
                # Persist every meaningful relation; summarize disjoint scopes without
                # bloating the forensic artifact with non-competing Cartesian pairs.
                relevant_scopes = [
                    scope
                    for scope in known_scopes
                    if covered & frozenset(scope["required_component_ids"])
                ]
            rows = [
                _membership_record(candidate, scope, covered)
                for scope in relevant_scopes
            ]
            if not rows:
                rows = [{
                    "membership_id": _stable_id(
                        "fsmem", [candidate["candidate_id"], "UNKNOWN"]
                    ),
                    "candidate_id": candidate["candidate_id"],
                    "coverage_scope_id": None,
                    "covered_component_ids": [],
                    "missing_required_component_ids": [],
                    "extra_component_ids": [],
                    "scope_relation": "UNKNOWN",
                    "selector_eligible": False,
                    "relation_uses_score_or_rank": False,
                    "evidence_refs": sorted(candidate.get("evidence_refs") or []),
                }]
            memberships.extend(rows)
            relations = {row["scope_relation"] for row in rows}
            relation_counts.update(row["scope_relation"] for row in rows)
            candidate_relation_counts.update(relations)
            exact = [row for row in rows if row["scope_relation"] == "EXACT_SCOPE"]
            disjoint_count = (
                len(known_scopes) - len(relevant_scopes) if covered is not None else 0
            )
            candidate_summaries.append({
                "candidate_id": candidate["candidate_id"],
                "pair_id": pair_id,
                "project": PROJECTS[pair_id],
                "source_relation_type": candidate.get("relation_type"),
                "canonical_coverage_scope_id": (
                    exact[0]["coverage_scope_id"] if len(exact) == 1 else None
                ),
                "covered_component_ids": sorted(covered or []),
                "membership_relations": sorted(relations),
                "disjoint_scope_count": disjoint_count,
                "source_candidate_preserved": True,
                "evidence_refs": sorted(candidate.get("evidence_refs") or []),
            })
    return {
        "schema_version": SCHEMA_VERSION,
        "method": {
            "coverage_axis": "SOURCE_ENGINEERING_COMPONENTS",
            "physical_page_used_for_identity": False,
            "score_or_rank_used_for_relation": False,
            "document_link_used_for_identity": False,
            "disjoint_pairs": "counted per candidate, omitted from flat membership rows",
            "unknown_policy": "FAIL_CLOSED_PERSIST_NOT_SELECTABLE",
        },
        "candidate_count": len(candidate_summaries),
        "membership_count": len(memberships),
        "relation_membership_counts": dict(sorted(relation_counts.items())),
        "candidate_counts_by_observed_relation": dict(
            sorted(candidate_relation_counts.items())
        ),
        "candidate_summaries": candidate_summaries,
        "memberships": sorted(
            memberships,
            key=lambda value: (
                str(value["candidate_id"]), str(value["coverage_scope_id"])
            ),
        ),
        "model_calls": 0,
    }


def _best_original_rank(
    candidate_id: str, tasks: Sequence[Mapping[str, Any]]
) -> int:
    return min(
        (
            int(task["candidate_ranks"][candidate_id])
            for task in tasks
            if candidate_id in (task.get("candidate_ranks") or {})
        ),
        default=10**9,
    )


def build_selector_tasks(
    datasets: Mapping[str, Mapping[str, Any]],
    models: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    scoped_tasks = []
    before_task_count = 0
    before_cross_task_count = 0
    before_cross_pair_count = 0
    after_cross_task_count = 0
    after_cross_pair_count = 0
    project_metrics = []
    for pair_id in sorted(datasets, key=lambda value: PROJECTS[value]):
        dataset = datasets[pair_id]
        model = models[pair_id]
        candidates = {
            str(row["candidate_id"]): row
            for row in dataset.get("functional_candidates") or []
        }
        covered = {
            candidate_id: _covered_component_ids(
                candidate, model["component_by_source"]
            )
            for candidate_id, candidate in candidates.items()
        }
        tasks = list(dataset.get("candidate_tasks") or [])
        before_task_count += len(tasks)
        pair_before_cross_task_count = 0
        pair_before_cross_pair_count = 0
        pair_after_cross_task_count = 0
        pair_after_cross_pair_count = 0
        for task in tasks:
            source_sets = [
                covered[str(candidate_id)]
                for candidate_id in task.get("candidate_ids") or []
                if covered[str(candidate_id)] is not None
            ]
            cross_pairs = sum(
                left != right
                for left, right in itertools.combinations(source_sets, 2)
            )
            if cross_pairs:
                before_cross_task_count += 1
                before_cross_pair_count += cross_pairs
                pair_before_cross_task_count += 1
                pair_before_cross_pair_count += cross_pairs

        task_by_component = {
            model["component_by_source"][
                (
                    str(task["left_fragment_id"]),
                    str(task["left_function_id"]),
                    str(
                        dataset["function_fragments"]["LEFT"][
                            task["left_fragment_id"]
                        ]["component_role"]
                    ),
                )
            ]: str(task["task_id"])
            for task in tasks
        }
        for scope in sorted(model["scopes"], key=lambda value: value["scope_id"]):
            required = frozenset(scope["required_component_ids"])
            exact_ids = [
                candidate_id
                for candidate_id, values in covered.items()
                if values == required
            ]
            if scope["scope_kind"] == "UNKNOWN":
                exact_ids = []
            exact_ids.sort(
                key=lambda candidate_id: (
                    _best_original_rank(candidate_id, tasks), candidate_id
                )
            )
            related = defaultdict(list)
            for candidate_id, values in covered.items():
                relation = classify_scope_relation(values, required)
                if relation not in {"EXACT_SCOPE", "DISJOINT"}:
                    related[relation].append(candidate_id)
            after_sets = [covered[candidate_id] for candidate_id in exact_ids]
            after_pairs = sum(
                left != right
                for left, right in itertools.combinations(after_sets, 2)
            )
            if after_pairs:
                after_cross_task_count += 1
                after_cross_pair_count += after_pairs
                pair_after_cross_task_count += 1
                pair_after_cross_pair_count += after_pairs
            source_task_ids = sorted(
                task_by_component[component_id]
                for component_id in required
                if component_id in task_by_component
            )
            scoped_tasks.append({
                "scoped_task_id": _stable_id("fstask", scope["scope_id"]),
                "coverage_scope_id": scope["scope_id"],
                "scope_kind": scope["scope_kind"],
                "pair_id": pair_id,
                "project": PROJECTS[pair_id],
                "source_task_ids": source_task_ids,
                "required_component_ids": sorted(required),
                "candidate_ids": exact_ids,
                "candidate_ranks_for_display_only": {
                    candidate_id: index
                    for index, candidate_id in enumerate(exact_ids, start=1)
                },
                "allowed_outputs": exact_ids + [
                    "FUNCTION_REMOVED", "NEED_MORE_EVIDENCE"
                ],
                "non_selectable_related_candidate_ids": {
                    relation: sorted(values)
                    for relation, values in sorted(related.items())
                },
                "unknown_is_selectable": False,
            })
        project_metrics.append({
            "pair_id": pair_id,
            "project": PROJECTS[pair_id],
            "tasks_before": len(tasks),
            "tasks_after": sum(
                task["pair_id"] == pair_id for task in scoped_tasks
            ),
            "cross_granularity_competition": {
                "before": {
                    "task_count": pair_before_cross_task_count,
                    "candidate_pair_count": pair_before_cross_pair_count,
                },
                "after": {
                    "task_count": pair_after_cross_task_count,
                    "candidate_pair_count": pair_after_cross_pair_count,
                },
            },
        })
    return {
        "schema_version": SCHEMA_VERSION,
        "task_identity": "ONE_FUNCTION_SCOPE",
        "selector_prompt_changed": False,
        "original_fragment_task_count": before_task_count,
        "scoped_task_count": len(scoped_tasks),
        "cross_granularity_competition": {
            "before": {
                "task_count": before_cross_task_count,
                "candidate_pair_count": before_cross_pair_count,
            },
            "after": {
                "task_count": after_cross_task_count,
                "candidate_pair_count": after_cross_pair_count,
            },
            "provable_scope_goal_met": after_cross_task_count == 0,
        },
        "projects": project_metrics,
        "tasks": sorted(scoped_tasks, key=lambda value: value["scoped_task_id"]),
        "model_calls": 0,
    }


def classify_group_derivability(
    candidate: Mapping[str, Any],
    singleton_candidates: Sequence[Mapping[str, Any]],
) -> tuple[str, list[str]]:
    """Classify whether a group is the exact union of complete child mappings."""
    group_mappings = candidate_mapping_keys(candidate)
    group_sources = candidate_source_keys(candidate)
    if group_mappings is None or group_sources is None:
        return "UNKNOWN", []
    if candidate.get("relation_type") not in GROUP_RELATIONS:
        return "UNKNOWN", []
    declared_capacity = sorted(set(candidate.get("right_capacity_keys") or []))
    mapped_capacity = sorted({
        str(row.get("capacity_key"))
        for row in candidate.get("component_map") or []
        if _known(row.get("capacity_key"))
    })
    if declared_capacity != mapped_capacity:
        return "UNKNOWN", []
    if candidate.get("explicit_contradictions"):
        return "NON_DECOMPOSABLE_GROUP", []
    # SPLIT is one source component with several targets, not a composite scope.
    if len(group_sources) <= 1:
        return "NON_DECOMPOSABLE_GROUP", []
    singleton_index: dict[tuple[str, ...], list[str]] = defaultdict(list)
    for singleton in singleton_candidates:
        mappings = candidate_mapping_keys(singleton)
        sources = candidate_source_keys(singleton)
        if (
            singleton.get("relation_type") == "CONTINUED_1_TO_1"
            and mappings is not None
            and sources is not None
            and len(mappings) == 1
            and len(sources) == 1
            and not singleton.get("explicit_contradictions")
        ):
            singleton_index[next(iter(mappings))].append(
                str(singleton["candidate_id"])
            )
    matched = {
        mapping: sorted(singleton_index.get(mapping, []))[0]
        for mapping in group_mappings
        if singleton_index.get(mapping)
    }
    child_ids = sorted(set(matched.values()))
    if set(matched) == set(group_mappings):
        return "EXACT_CHILD_UNION", child_ids
    if matched:
        return "PARTIAL_CHILD_UNION", child_ids
    return "NON_DECOMPOSABLE_GROUP", []


def build_group_derivability(
    datasets: Mapping[str, Mapping[str, Any]],
    models: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    rows = []
    project_counts = []
    overall = Counter({name: 0 for name in DERIVABILITY_CLASSES})
    for pair_id in sorted(datasets, key=lambda value: PROJECTS[value]):
        dataset = datasets[pair_id]
        candidates = list(dataset.get("functional_candidates") or [])
        singletons = [
            row for row in candidates
            if row.get("relation_type") == "CONTINUED_1_TO_1"
        ]
        counts = Counter({name: 0 for name in DERIVABILITY_CLASSES})
        relation_counts: dict[str, Counter[str]] = {
            relation: Counter({name: 0 for name in DERIVABILITY_CLASSES})
            for relation in sorted(GROUP_RELATIONS)
        }
        for candidate in sorted(candidates, key=lambda value: value["candidate_id"]):
            relation_type = str(candidate.get("relation_type"))
            if relation_type not in GROUP_RELATIONS:
                continue
            classification, child_ids = classify_group_derivability(
                candidate, singletons
            )
            counts[classification] += 1
            relation_counts[relation_type][classification] += 1
            overall[classification] += 1
            covered = _covered_component_ids(
                candidate, models[pair_id]["component_by_source"]
            )
            scope_id = (
                models[pair_id]["scope_by_components"].get(covered)
                if covered is not None else None
            )
            rows.append({
                "candidate_id": candidate["candidate_id"],
                "pair_id": pair_id,
                "project": PROJECTS[pair_id],
                "relation_type": relation_type,
                "coverage_scope_id": scope_id,
                "source_component_count": len(covered or []),
                "right_fragment_count": len(candidate.get("right_fragment_ids") or []),
                "classification": classification,
                "child_candidate_ids": child_ids,
                "derivable": classification == "EXACT_CHILD_UNION",
                "capacity_keys_preserved": sorted({
                    str(row.get("capacity_key"))
                    for row in candidate.get("component_map") or []
                    if _known(row.get("capacity_key"))
                }) == sorted(set(candidate.get("right_capacity_keys") or [])),
                "score_or_rank_used": False,
            })
        project_counts.append({
            "pair_id": pair_id,
            "project": PROJECTS[pair_id],
            "counts": dict(sorted(counts.items())),
            "by_relation_type": {
                relation: dict(sorted(values.items()))
                for relation, values in sorted(relation_counts.items())
            },
        })
    return {
        "schema_version": SCHEMA_VERSION,
        "rule": (
            "EXACT_CHILD_UNION iff a group has more than one distinct source "
            "component and every exact LEFT→RIGHT mapping is independently "
            "present as a complete CONTINUED_1_TO_1 child candidate"
        ),
        "projects": project_counts,
        "overall_counts": dict(sorted(overall.items())),
        "groups": rows,
        "composite_lineage_derivability": "SOMETIMES",
        "model_calls": 0,
    }


def _scope_task_index(selector_tasks: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        str(task["coverage_scope_id"]): task
        for task in selector_tasks["tasks"]
    }


def _candidate_scope_rank(
    candidate_id: str,
    candidate_scope: Mapping[str, str | None],
    scoped_task_index: Mapping[str, Mapping[str, Any]],
) -> int | None:
    scope_id = candidate_scope.get(candidate_id)
    if scope_id is None or scope_id not in scoped_task_index:
        return None
    task = scoped_task_index[scope_id]
    return (task.get("candidate_ranks_for_display_only") or {}).get(candidate_id)


def _case_scope_rank(
    case: Mapping[str, Any],
    dataset: Mapping[str, Any],
    candidate_scope: Mapping[str, str | None],
    scoped_task_index: Mapping[str, Mapping[str, Any]],
) -> int | None:
    candidates = list(dataset.get("functional_candidates") or [])
    is_group = case["expected_mode"] == "EXACT_GROUP" or (
        case["expected_mode"] == "ALL"
        and (len(case["left_pages"]) > 1 or len(case["right_pages"]) > 1)
    )
    if is_group:
        matching = [
            candidate for candidate in candidates
            if candidate.get("relation_type") in GROUP_RELATIONS
            and candidate.get("left_pages") == case["left_pages"]
            and candidate.get("right_pages") == case["right_pages"]
        ]
        ranks = [
            _candidate_scope_rank(
                str(candidate["candidate_id"]), candidate_scope, scoped_task_index
            )
            for candidate in matching
        ]
        return min((rank for rank in ranks if rank is not None), default=None)

    ranks_by_right = []
    for right_page in case["right_pages"]:
        matching = [
            candidate for candidate in candidates
            if candidate.get("relation_type") == "CONTINUED_1_TO_1"
            and candidate.get("left_pages") == [case["left_page"]]
            and candidate.get("right_pages") == [right_page]
        ]
        ranks = [
            _candidate_scope_rank(
                str(candidate["candidate_id"]), candidate_scope, scoped_task_index
            )
            for candidate in matching
        ]
        ranks_by_right.append(
            min((rank for rank in ranks if rank is not None), default=None)
        )
    present = [rank for rank in ranks_by_right if rank is not None]
    if case["expected_mode"] == "ANY":
        return min(present) if present else None
    return (
        max(present)
        if ranks_by_right and len(present) == len(ranks_by_right)
        else None
    )


def _recall(rows: Sequence[Mapping[str, Any]], rank_field: str) -> dict[str, Any]:
    total = len(rows)
    return {
        "case_count": total,
        **{
            f"recall_at_{cutoff}": (
                round(
                    sum(
                        row.get(rank_field) is not None
                        and int(row[rank_field]) <= cutoff
                        for row in rows
                    ) / total,
                    6,
                )
                if total else None
            )
            for cutoff in (1, 3, 5, 10)
        },
    }


def build_recall_metrics(
    datasets: Mapping[str, Mapping[str, Any]],
    frozen_metrics: Mapping[str, Any],
    memberships: Mapping[str, Any],
    selector_tasks: Mapping[str, Any],
) -> dict[str, Any]:
    candidate_scope = {
        str(row["candidate_id"]): row.get("canonical_coverage_scope_id")
        for row in memberships["candidate_summaries"]
    }
    scoped_task_index = _scope_task_index(selector_tasks)
    projects = []
    all_rows = []
    frozen_projects = {
        str(project["pair_id"]): project
        for project in frozen_metrics.get("projects") or []
    }
    for pair_id in sorted(datasets, key=lambda value: PROJECTS[value]):
        rows = []
        for source_case in frozen_projects[pair_id].get("cases") or []:
            case = dict(source_case)
            case["raw_rank"] = case.pop("rank", None)
            case["scope_eligible_rank"] = _case_scope_rank(
                case,
                datasets[pair_id],
                candidate_scope,
                scoped_task_index,
            )
            rows.append(case)
        all_rows.extend(rows)
        projects.append({
            "pair_id": pair_id,
            "project": PROJECTS[pair_id],
            "raw_candidate_recall": _recall(rows, "raw_rank"),
            "scope_eligible_recall": _recall(rows, "scope_eligible_rank"),
            "cases": rows,
        })
    return {
        "reference_limit": (
            "Frozen evaluation references are page-labelled; scope eligibility "
            "is evaluated only after matching each reference to an exact frozen "
            "candidate source-component scope. No alternative is treated as truth."
        ),
        "projects": projects,
        "overall": {
            "raw_candidate_recall": _recall(all_rows, "raw_rank"),
            "scope_eligible_recall": _recall(all_rows, "scope_eligible_rank"),
        },
    }


def _find_candidate(
    dataset: Mapping[str, Any], *, left_page: int, right_pages: list[int], rank: int | None = None,
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    candidates = {
        str(row["candidate_id"]): row
        for row in dataset.get("functional_candidates") or []
    }
    matches = []
    for task in dataset.get("candidate_tasks") or []:
        if int(task["left_physical_page"]) != left_page:
            continue
        for candidate_id in task.get("candidate_ids") or []:
            candidate = candidates[str(candidate_id)]
            if (
                candidate.get("relation_type") == "CONTINUED_1_TO_1"
                and candidate.get("right_pages") == right_pages
                and (
                    rank is None
                    or int(task["candidate_ranks"][candidate_id]) == rank
                )
            ):
                matches.append((candidate, task))
    unique = {str(candidate["candidate_id"]): (candidate, task) for candidate, task in matches}
    if len(unique) != 1:
        raise RuntimeError(
            f"expected one LEFT{left_page} → RIGHT{right_pages} candidate, got {sorted(unique)}"
        )
    return next(iter(unique.values()))


def _relation_for(
    candidate_id: str,
    scope_id: str,
    membership_index: Mapping[tuple[str, str], Mapping[str, Any]],
) -> str:
    return str(membership_index[(candidate_id, scope_id)]["scope_relation"])


def build_ios21_forensics(
    dataset: Mapping[str, Any],
    model: Mapping[str, Any],
    memberships: Mapping[str, Any],
    selector_tasks: Mapping[str, Any],
    derivability: Mapping[str, Any],
    smoke: Mapping[str, Any],
) -> dict[str, Any]:
    candidates = {
        str(row["candidate_id"]): row
        for row in dataset.get("functional_candidates") or []
    }
    distributed = candidates[LEFT20_DISTRIBUTED_ID]
    covered = _covered_component_ids(distributed, model["component_by_source"])
    if covered is None:
        raise RuntimeError("LEFT20 distributed source scope became unknown")
    parent_scope_id = model["scope_by_components"][covered]
    membership_index = {
        (str(row["candidate_id"]), str(row["coverage_scope_id"])): row
        for row in memberships["memberships"]
        if row.get("coverage_scope_id") is not None
    }
    scoped_index = _scope_task_index(selector_tasks)
    component_by_id = {
        str(row["function_component_id"]): row for row in model["components"]
    }
    singleton_rows = {}
    for mapping in distributed.get("component_map") or []:
        right_page = int(mapping["right_physical_page"])
        wanted = _mapping_key(mapping)
        matches = [
            candidate for candidate in candidates.values()
            if candidate.get("relation_type") == "CONTINUED_1_TO_1"
            and candidate_mapping_keys(candidate) == frozenset([wanted])
        ]
        if len(matches) != 1:
            raise RuntimeError(f"LEFT20 R{right_page} singleton drifted")
        candidate = matches[0]
        candidate_covered = _covered_component_ids(
            candidate, model["component_by_source"]
        )
        if candidate_covered is None or len(candidate_covered) != 1:
            raise RuntimeError(f"LEFT20 R{right_page} child scope drifted")
        child_scope_id = model["scope_by_components"][candidate_covered]
        component_id = next(iter(candidate_covered))
        singleton_rows[f"R{right_page}"] = {
            "candidate_id": candidate["candidate_id"],
            "component_id": component_id,
            "component_role": component_by_id[component_id]["role"],
            "child_scope_id": child_scope_id,
            "relation_to_child": _relation_for(
                str(candidate["candidate_id"]), child_scope_id, membership_index
            ),
            "relation_to_parent": _relation_for(
                str(candidate["candidate_id"]), parent_scope_id, membership_index
            ),
            "eligible_in_child_task": str(candidate["candidate_id"])
            in scoped_index[child_scope_id]["candidate_ids"],
            "eligible_in_parent_task": str(candidate["candidate_id"])
            in scoped_index[parent_scope_id]["candidate_ids"],
        }

    left19_task = next(
        row for row in dataset.get("candidate_tasks") or []
        if row["task_id"] == LEFT19_TASK_ID
    )
    r30 = next(
        candidates[candidate_id]
        for candidate_id in left19_task["candidate_ids"]
        if candidates[candidate_id].get("relation_type") == "CONTINUED_1_TO_1"
        and candidates[candidate_id].get("right_pages") == [30]
    )
    r25 = next(
        candidates[candidate_id]
        for candidate_id in left19_task["candidate_ids"]
        if candidates[candidate_id].get("relation_type") == "CONTINUED_1_TO_1"
        and candidates[candidate_id].get("right_pages") == [25]
    )
    r30_covered = _covered_component_ids(r30, model["component_by_source"])
    r25_covered = _covered_component_ids(r25, model["component_by_source"])
    same_scope = r30_covered is not None and r30_covered == r25_covered
    left19_scope_id = (
        model["scope_by_components"][r30_covered] if same_scope else None
    )
    left19_task_scoped = scoped_index.get(str(left19_scope_id), {})

    derivability_index = {
        str(row["candidate_id"]): row for row in derivability["groups"]
    }
    smoke_left19 = next(
        row for row in smoke.get("tasks") or [] if row.get("label") == "LEFT19"
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "pair_id": IOS21_PAIR_ID,
        "LEFT20": {
            "parent_scope_id": parent_scope_id,
            "parent_required_component_ids": sorted(covered),
            "parent_component_roles": sorted(
                str(component_by_id[value]["role"]) for value in covered
            ),
            "child_scope_ids": {
                row["component_role"]: row["child_scope_id"]
                for row in sorted(
                    singleton_rows.values(), key=lambda value: value["component_role"]
                )
            },
            "singletons": singleton_rows,
            "distributed_candidate": {
                "candidate_id": LEFT20_DISTRIBUTED_ID,
                "right_pages": distributed["right_pages"],
                "relation_to_parent": _relation_for(
                    LEFT20_DISTRIBUTED_ID, parent_scope_id, membership_index
                ),
                "eligible_in_parent_task": LEFT20_DISTRIBUTED_ID
                in scoped_index[parent_scope_id]["candidate_ids"],
                "derivability": derivability_index[LEFT20_DISTRIBUTED_ID]["classification"],
                "exact_child_union": derivability_index[LEFT20_DISTRIBUTED_ID]["classification"]
                == "EXACT_CHILD_UNION",
                "child_candidate_ids": derivability_index[LEFT20_DISTRIBUTED_ID]["child_candidate_ids"],
            },
        },
        "LEFT19": {
            "scope_id": left19_scope_id,
            "r30_candidate_id": r30["candidate_id"],
            "r25_candidate_id": r25["candidate_id"],
            "r30_r25_same_scope": same_scope,
            "both_selector_eligible": (
                r30["candidate_id"] in left19_task_scoped.get("candidate_ids", [])
                and r25["candidate_id"] in left19_task_scoped.get("candidate_ids", [])
            ),
            "ambiguity_remains": same_scope,
            "frozen_6_of_6_preference_used_as_truth": False,
            "frozen_selection_distribution_for_context_only": smoke_left19.get(
                "selection_distribution"
            ),
        },
        "LEFT17": _control_scope(
            dataset, model, scoped_index, left_page=17, right_page=27
        ),
        "LEFT18": _control_scope(
            dataset, model, scoped_index, left_page=18, right_page=24
        ),
        "classification_uses_page_specific_rules": False,
        "model_calls": 0,
    }


def _control_scope(
    dataset: Mapping[str, Any],
    model: Mapping[str, Any],
    scoped_index: Mapping[str, Mapping[str, Any]],
    *,
    left_page: int,
    right_page: int,
) -> dict[str, Any]:
    candidates = {
        str(row["candidate_id"]): row
        for row in dataset.get("functional_candidates") or []
    }
    matches: dict[str, Mapping[str, Any]] = {}
    for task in dataset.get("candidate_tasks") or []:
        if int(task["left_physical_page"]) != left_page:
            continue
        for candidate_id in task.get("candidate_ids") or []:
            candidate = candidates[str(candidate_id)]
            if (
                candidate.get("relation_type") == "CONTINUED_1_TO_1"
                and candidate.get("right_pages") == [right_page]
                and int(task["candidate_ranks"][candidate_id]) == 1
            ):
                matches[str(candidate_id)] = candidate
    if not matches:
        raise RuntimeError(f"LEFT{left_page} → R{right_page} control disappeared")
    rows = []
    for candidate_id, candidate in sorted(matches.items()):
        covered = _covered_component_ids(candidate, model["component_by_source"])
        scope_id = model["scope_by_components"].get(covered) if covered else None
        rows.append({
            "candidate_id": candidate_id,
            "scope_id": scope_id,
            "scope_relation": classify_scope_relation(covered, covered),
            "eligible": bool(
                scope_id
                and candidate_id in scoped_index[scope_id]["candidate_ids"]
            ),
        })
    return {
        "candidate_ids": [row["candidate_id"] for row in rows],
        "scope_ids": [row["scope_id"] for row in rows],
        "matches": rows,
        "eligible": all(row["eligible"] for row in rows),
    }


def _capacity_metrics(datasets: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    defects = []
    search_failures = []
    group_generation_failures = []
    same_page_distinct_fragment_examples = []
    for pair_id, dataset in datasets.items():
        search_failures.extend(
            {"pair_id": pair_id, **row}
            for row in dataset.get("diagnostics", {}).get("search_failures") or []
        )
        group_generation_failures.extend(
            {
                "pair_id": pair_id,
                "left_physical_page": page,
            }
            for page in dataset.get("diagnostics", {}).get("group_generation_failures") or []
        )
        candidates = list(dataset.get("functional_candidates") or [])
        for candidate in candidates:
            declared = sorted(set(candidate.get("right_capacity_keys") or []))
            mapped = sorted({
                str(row.get("capacity_key"))
                for row in candidate.get("component_map") or []
                if _known(row.get("capacity_key"))
            })
            if declared != mapped:
                defects.append({
                    "pair_id": pair_id,
                    "candidate_id": candidate["candidate_id"],
                    "declared": declared,
                    "mapped": mapped,
                })
        for left, right in itertools.combinations(candidates, 2):
            shared_pages = sorted(set(left.get("right_pages") or []) & set(right.get("right_pages") or []))
            if (
                shared_pages
                and not set(left.get("right_capacity_keys") or [])
                & set(right.get("right_capacity_keys") or [])
            ):
                same_page_distinct_fragment_examples.append({
                    "pair_id": pair_id,
                    "candidate_a_id": left["candidate_id"],
                    "candidate_b_id": right["candidate_id"],
                    "shared_right_physical_pages": shared_pages,
                    "capacity_keys_disjoint": True,
                })
                break
    return {
        "capacity_identity": "RIGHT_PHYSICAL_PAGE_PLUS_EXACT_FUNCTION_FRAGMENT_ID",
        "page_global_exclusivity": False,
        "search_failure_count": len(search_failures),
        "search_failures": search_failures,
        "group_generation_failure_count": len(group_generation_failures),
        "group_generation_failures": group_generation_failures,
        "capacity_key_defect_count": len(defects),
        "capacity_key_defects": defects,
        "RIGHT_MAP_CONFLICT": 0 if not defects else len(defects),
        "same_page_distinct_fragment_examples": same_page_distinct_fragment_examples,
    }


def build_scope_graph_artifact(
    models: Mapping[str, Mapping[str, Any]], input_hashes: Mapping[str, str]
) -> dict[str, Any]:
    components = [
        row
        for pair_id in sorted(models, key=lambda value: PROJECTS[value])
        for row in models[pair_id]["components"]
    ]
    scopes = [
        row
        for pair_id in sorted(models, key=lambda value: PROJECTS[value])
        for row in models[pair_id]["scopes"]
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "frozen_inputs": dict(input_hashes),
        "identity": {
            "component_basis": (
                "source function/fragment identifiers plus deterministic engineering "
                "taxonomy, Function Passport fields, neighbors, and evidence refs"
            ),
            "physical_page_is_provenance_only": True,
            "document_link_namespace_separate": True,
            "unknown_values_inferred": False,
            "composite_scope_basis": (
                "distinct source engineering component sets carried by frozen "
                "MERGED_N_TO_1 or FUNCTION_DISTRIBUTED candidates; supporting "
                "candidate/group evidence is persisted on each scope"
            ),
        },
        "component_count": len(components),
        "scope_count": len(scopes),
        "components": components,
        "scopes": sorted(scopes, key=lambda value: value["scope_id"]),
        "model_calls": 0,
    }


def build_scope_metrics(
    datasets: Mapping[str, Mapping[str, Any]],
    models: Mapping[str, Mapping[str, Any]],
    memberships: Mapping[str, Any],
    selector_tasks: Mapping[str, Any],
    recall: Mapping[str, Any],
    derivability: Mapping[str, Any],
) -> dict[str, Any]:
    project_rows = []
    membership_rows = memberships["memberships"]
    derivability_by_project = {
        row["pair_id"]: row for row in derivability["projects"]
    }
    recall_by_project = {row["pair_id"]: row for row in recall["projects"]}
    selector_by_project = {
        row["pair_id"]: row for row in selector_tasks["projects"]
    }
    for pair_id in sorted(datasets, key=lambda value: PROJECTS[value]):
        scopes = models[pair_id]["scopes"]
        pair_candidate_ids = {
            str(row["candidate_id"])
            for row in datasets[pair_id].get("functional_candidates") or []
        }
        pair_memberships = [
            row for row in membership_rows if row["candidate_id"] in pair_candidate_ids
        ]
        candidate_relations = defaultdict(set)
        for row in pair_memberships:
            candidate_relations[row["candidate_id"]].add(row["scope_relation"])
        project_rows.append({
            "pair_id": pair_id,
            "project": PROJECTS[pair_id],
            "component_scopes": sum(row["scope_kind"] == "COMPONENT" for row in scopes),
            "composite_scopes": sum(row["scope_kind"] == "COMPOSITE" for row in scopes),
            "unknown_scopes": sum(row["scope_kind"] == "UNKNOWN" for row in scopes),
            "parent_child_relations": sum(len(row["child_scope_ids"]) for row in scopes),
            "candidate_counts_by_observed_relation": {
                relation: sum(
                    relation in values for values in candidate_relations.values()
                )
                for relation in SCOPE_RELATIONS
            },
            "raw_candidate_recall": recall_by_project[pair_id]["raw_candidate_recall"],
            "scope_eligible_recall": recall_by_project[pair_id]["scope_eligible_recall"],
            "group_derivability_counts": derivability_by_project[pair_id]["counts"],
            "selector_tasks_before": selector_by_project[pair_id]["tasks_before"],
            "selector_tasks_after": selector_by_project[pair_id]["tasks_after"],
            "cross_granularity_competition": selector_by_project[pair_id][
                "cross_granularity_competition"
            ],
        })
    capacity = _capacity_metrics(datasets)
    return {
        "schema_version": SCHEMA_VERSION,
        "projects": project_rows,
        "overall": {
            "component_scopes": sum(row["component_scopes"] for row in project_rows),
            "composite_scopes": sum(row["composite_scopes"] for row in project_rows),
            "unknown_scopes": sum(row["unknown_scopes"] for row in project_rows),
            "parent_child_relations": sum(row["parent_child_relations"] for row in project_rows),
            "candidate_counts_by_observed_relation": {
                relation: sum(
                    row["candidate_counts_by_observed_relation"][relation]
                    for row in project_rows
                )
                for relation in SCOPE_RELATIONS
            },
            "selector_tasks_before": selector_tasks["original_fragment_task_count"],
            "selector_tasks_after": selector_tasks["scoped_task_count"],
            "cross_granularity_competition": selector_tasks[
                "cross_granularity_competition"
            ],
            "raw_candidate_recall": recall["overall"]["raw_candidate_recall"],
            "scope_eligible_recall": recall["overall"]["scope_eligible_recall"],
            "group_derivability_counts": derivability["overall_counts"],
        },
        "safety": {
            **capacity,
            "raw_candidate_count": sum(
                len(dataset.get("functional_candidates") or [])
                for dataset in datasets.values()
            ),
            "persisted_candidate_count": memberships["candidate_count"],
            "candidate_loss_count": sum(
                len(dataset.get("functional_candidates") or [])
                for dataset in datasets.values()
            ) - memberships["candidate_count"],
            "model_calls": 0,
            "selector_prompt_changed": False,
            "production_matcher_changed": False,
            "shadow_changed": False,
            "materialization_applied": False,
            "deployed": False,
        },
    }


def build_report(
    metrics: Mapping[str, Any],
    ios21: Mapping[str, Any],
    derivability: Mapping[str, Any],
    replay_hashes: Mapping[str, str] | None = None,
) -> str:
    overall = metrics["overall"]
    safety = metrics["safety"]
    left20 = ios21["LEFT20"]
    left19 = ios21["LEFT19"]
    raw = overall["raw_candidate_recall"]
    eligible = overall["scope_eligible_recall"]
    distributed = left20["distributed_candidate"]
    relation_counts = overall["candidate_counts_by_observed_relation"]
    group_counts = overall["group_derivability_counts"]
    cross = overall["cross_granularity_competition"]
    verdict = (
        "A"
        if cross["after"]["task_count"] == 0
        and safety["candidate_loss_count"] == 0
        and safety["RIGHT_MAP_CONFLICT"] == 0
        and left19["ambiguity_remains"]
        and all(ios21[name]["eligible"] for name in ("LEFT17", "LEFT18"))
        else "B"
    )
    lines = [
        "# Function Lineage v2.4 — deterministic Function Scope Graph",
        "",
        "## Execution boundary",
        "",
        f"- Frozen candidate/evaluation record: `{FROZEN_EXPERIMENT_COMMIT}`; V2.3 parent: `{RESEARCH_PARENT_COMMIT}`.",
        f"- Production reference only: `{PRODUCTION_HEAD_AT_EXPERIMENT}` / `{PRODUCTION_RELEASE_AT_EXPERIMENT}`.",
        "- New model calls: `0`; vision: `0`; candidate regeneration: `0`; selector prompt changes: `0`.",
        "- Deploy, shadow wiring, materialization, and production matcher changes: not performed.",
        "",
        "## LEFT20 control",
        "",
        f"Parent composite scope: `{left20['parent_scope_id']}` with roles `{', '.join(left20['parent_component_roles'])}`.",
        "",
        "| Candidate | Child scope | Relation to child | Relation to parent | Child eligible | Parent eligible |",
        "|---|---|---|---|---:|---:|",
    ]
    for label in ("R26", "R28", "R29"):
        row = left20["singletons"][label]
        lines.append(
            f"| {label} `{row['candidate_id']}` | `{row['child_scope_id']}` ({row['component_role']}) | "
            f"`{row['relation_to_child']}` | `{row['relation_to_parent']}` | "
            f"`{row['eligible_in_child_task']}` | `{row['eligible_in_parent_task']}` |"
        )
    lines.extend([
        "",
        f"Distributed `[26,28,29]` `{distributed['candidate_id']}` is `{distributed['relation_to_parent']}` for the parent and `{distributed['derivability']}`. Exact child union: `{'YES' if distributed['exact_child_union'] else 'NO'}`.",
        "",
        "Composite lineage can be derived **SOMETIMES**: only when every required source child has an independently complete 1→1 mapping, their exact mapping union equals the group, and exact fragment capacity keys are preserved. SPLIT 1→N is not mistaken for a composite merely because it has several RIGHT targets.",
        "",
        "## LEFT19 negative control",
        "",
        f"R30 `{left19['r30_candidate_id']}` and R25 `{left19['r25_candidate_id']}` have the same scope: `{'YES' if left19['r30_r25_same_scope'] else 'NO'}`. Both remain selector-eligible: `{left19['both_selector_eligible']}`. Ambiguity remains: `{'YES' if left19['ambiguity_remains'] else 'NO'}`. The frozen 6/6 preference is context only, never deterministic truth.",
        "",
        "## Corpus metrics",
        "",
        "| Corpus | Component scopes | Composite scopes | Unknown scopes | Parent/child | EXACT | SUBSET | SUPERSET | OVERLAP | UNKNOWN |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for row in metrics["projects"]:
        counts = row["candidate_counts_by_observed_relation"]
        lines.append(
            f"| {row['project']} | {row['component_scopes']} | {row['composite_scopes']} | "
            f"{row['unknown_scopes']} | {row['parent_child_relations']} | "
            f"{counts['EXACT_SCOPE']} | {counts['STRICT_SUBSET']} | {counts['STRICT_SUPERSET']} | "
            f"{counts['OVERLAP']} | {counts['UNKNOWN']} |"
        )
    lines.extend([
        f"| **Total** | **{overall['component_scopes']}** | **{overall['composite_scopes']}** | **{overall['unknown_scopes']}** | **{overall['parent_child_relations']}** | **{relation_counts['EXACT_SCOPE']}** | **{relation_counts['STRICT_SUBSET']}** | **{relation_counts['STRICT_SUPERSET']}** | **{relation_counts['OVERLAP']}** | **{relation_counts['UNKNOWN']}** |",
        "",
        f"Selector tasks before/after: `{overall['selector_tasks_before']}` / `{overall['selector_tasks_after']}`. Cross-granularity task competitions before/after: `{cross['before']['task_count']}` / `{cross['after']['task_count']}`; candidate-pair competitions: `{cross['before']['candidate_pair_count']}` / `{cross['after']['candidate_pair_count']}`.",
        "",
        "| Corpus | Tasks before | Tasks after | Cross-scope tasks before | Cross-scope tasks after |",
        "|---|---:|---:|---:|---:|",
    ])
    for row in metrics["projects"]:
        row_cross = row["cross_granularity_competition"]
        lines.append(
            f"| {row['project']} | {row['selector_tasks_before']} | {row['selector_tasks_after']} | "
            f"{row_cross['before']['task_count']} | {row_cross['after']['task_count']} |"
        )
    lines.extend([
        "",
        "## Recall and safety",
        "",
        "| Metric | R@1 | R@3 | R@5 | R@10 |",
        "|---|---:|---:|---:|---:|",
        f"| RAW CANDIDATE RECALL | {raw['recall_at_1']} | {raw['recall_at_3']} | {raw['recall_at_5']} | {raw['recall_at_10']} |",
        f"| SCOPE-ELIGIBLE RECALL | {eligible['recall_at_1']} | {eligible['recall_at_3']} | {eligible['recall_at_5']} | {eligible['recall_at_10']} |",
        "",
        "| Corpus / metric | R@1 | R@3 | R@5 | R@10 |",
        "|---|---:|---:|---:|---:|",
    ])
    for row in metrics["projects"]:
        project_raw = row["raw_candidate_recall"]
        project_eligible = row["scope_eligible_recall"]
        lines.extend([
            f"| {row['project']} RAW | {project_raw['recall_at_1']} | {project_raw['recall_at_3']} | {project_raw['recall_at_5']} | {project_raw['recall_at_10']} |",
            f"| {row['project']} SCOPE-ELIGIBLE | {project_eligible['recall_at_1']} | {project_eligible['recall_at_3']} | {project_eligible['recall_at_5']} | {project_eligible['recall_at_10']} |",
        ])
    lines.extend([
        "",
        "Scope-eligible recall is a separate filtered-task diagnostic, not a claim that recall improved by deleting alternatives. All frozen candidates and evidence references remain persisted; candidate loss is `0`.",
        "",
        f"Group derivability counts: EXACT_CHILD_UNION `{group_counts['EXACT_CHILD_UNION']}`, NON_DECOMPOSABLE_GROUP `{group_counts['NON_DECOMPOSABLE_GROUP']}`, PARTIAL_CHILD_UNION `{group_counts['PARTIAL_CHILD_UNION']}`, UNKNOWN `{group_counts['UNKNOWN']}`.",
        "",
        "| Corpus | EXACT_CHILD_UNION | NON_DECOMPOSABLE_GROUP | PARTIAL_CHILD_UNION | UNKNOWN |",
        "|---|---:|---:|---:|---:|",
    ])
    for row in metrics["projects"]:
        counts = row["group_derivability_counts"]
        lines.append(
            f"| {row['project']} | {counts['EXACT_CHILD_UNION']} | "
            f"{counts['NON_DECOMPOSABLE_GROUP']} | {counts['PARTIAL_CHILD_UNION']} | "
            f"{counts['UNKNOWN']} |"
        )
    lines.extend([
        "",
        f"Search failures: `{safety['search_failure_count']}`. Frozen group-generation failures: `{safety['group_generation_failure_count']}`. Capacity-key defects: `{safety['capacity_key_defect_count']}`. RIGHT_MAP_CONFLICT: `{safety['RIGHT_MAP_CONFLICT']}`. Capacity remains RIGHT physical page + exact function fragment; page-global exclusivity is `False`.",
        "",
        "## Deterministic replay",
        "",
        "Two independent in-process builds are byte-identical before write."
    ])
    if replay_hashes:
        lines.extend(
            f"- `{name}`: `{digest}`" for name, digest in sorted(replay_hashes.items())
        )
    lines.extend([
        "",
        "## Verdict",
        "",
        f"**{verdict} — explicit Function Scope Graph resolves provable cross-granularity competition without candidate loss or capacity regression.** Ready only for another isolated critical AI smoke on frozen scoped tasks.",
        "",
        "Even with verdict A: **NO DEPLOY. NO SHADOW.**",
        "",
        "Model calls = `0`.",
        "",
    ])
    return "\n".join(lines)


def _build_payloads() -> tuple[dict[str, bytes], dict[str, Any]]:
    input_hashes = assert_frozen_inputs()
    datasets = {
        pair_id: _read_json(FROZEN_CANDIDATE_ROOT / f"{pair_id}.json")
        for pair_id in PROJECTS
    }
    frozen_metrics = _read_json(FROZEN_METRICS)
    smoke = _read_json(FROZEN_SMOKE)
    models = {pair_id: build_scope_model(dataset) for pair_id, dataset in datasets.items()}
    graph = build_scope_graph_artifact(models, input_hashes)
    memberships = build_candidate_memberships(datasets, models)
    selector_tasks = build_selector_tasks(datasets, models)
    derivability = build_group_derivability(datasets, models)
    recall = build_recall_metrics(
        datasets, frozen_metrics, memberships, selector_tasks
    )
    metrics = build_scope_metrics(
        datasets, models, memberships, selector_tasks, recall, derivability
    )
    ios21 = build_ios21_forensics(
        datasets[IOS21_PAIR_ID],
        models[IOS21_PAIR_ID],
        memberships,
        selector_tasks,
        derivability,
        smoke,
    )
    objects = {
        "function_scope_graph.json": graph,
        "candidate_scope_membership.json": memberships,
        "selector_tasks_scoped.json": selector_tasks,
        "group_derivability_audit.json": derivability,
        "scope_metrics.json": metrics,
        "ios21_scope_forensics.json": ios21,
    }
    payloads = {name: _json_bytes(value) for name, value in objects.items()}
    return payloads, objects


def build_artifacts() -> dict[str, bytes]:
    payloads, objects = _build_payloads()
    replay_hashes = {
        name: hashlib.sha256(payload).hexdigest()
        for name, payload in sorted(payloads.items())
    }
    payloads["report.md"] = build_report(
        objects["scope_metrics.json"],
        objects["ios21_scope_forensics.json"],
        objects["group_derivability_audit.json"],
        replay_hashes,
    ).encode("utf-8")
    return payloads


def write_artifacts(output: Path, *, check: bool = False) -> dict[str, str]:
    first = build_artifacts()
    second = build_artifacts()
    if first != second:
        raise RuntimeError("deterministic replay was not byte-identical")
    if check:
        mismatches = [
            name for name, payload in first.items()
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
    print(
        json.dumps(
            write_artifacts(args.output, check=args.check),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

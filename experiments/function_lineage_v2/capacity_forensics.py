"""Function Lineage v2.6 — deterministic capacity conflict forensics.

Phase 1 of the v2.6 master task.  The module never calls a model.  It replays
the frozen v2.5 stratified evaluation, reconstructs every observed
``FUNCTION_FRAGMENT_CONFLICT`` and classifies its root cause against the
Function Scope Graph, the deterministic candidate artifacts and the group
derivability audit.

It additionally inventories the *latent* conflict surface over the whole
213-task scoped population, because an observed conflict count of nine is a
sample property, not an architecture property.
"""
from __future__ import annotations

import itertools
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from experiments.function_lineage_v2 import smoke as base_smoke
from experiments.function_lineage_v2 import stratified


REPO_ROOT = stratified.REPO_ROOT
EVALUATION_ROOT = stratified.DEFAULT_OUTPUT
SCOPE_ROOT = stratified.SCOPE_ROOT
CANDIDATE_ROOT = stratified.CANDIDATE_ROOT
DEFAULT_OUTPUT = (
    stratified.COMPARISON_ROOT / "20260903_function_lineage_v2_6_capacity_forensics"
)
SCHEMA_VERSION = "function-lineage-capacity-forensics.v2.6"

#: Frozen v2.5 evaluation artifacts consumed read-only by the forensics.
EVALUATION_SOURCES = (
    "stratified_population.json",
    "stratified_sample.json",
    "model_runs.jsonl",
    "metrics.json",
)

#: Root cause taxonomy of the master task.  ``B_LICENSED_EXACT_CHILD_UNION``
#: is the false-conflict form of ``B``: two atomic child lineages of one
#: certified exact union are co-owners, not competitors.
ROOT_CAUSE_CLASSES = (
    "A_TRUE_FUNCTION_FRAGMENT_CONFLICT",
    "B_HIERARCHICAL_DUPLICATE",
    "B_LICENSED_EXACT_CHILD_UNION",
    "C_TASK_DUPLICATION",
    "D_FRAGMENTATION_DEFECT",
    "E_CANDIDATE_DEFECT",
    "F_CAPACITY_ACCOUNTING_DEFECT",
    "G_UNKNOWN",
)

FALSE_CONFLICT_CLASSES = frozenset({
    "B_HIERARCHICAL_DUPLICATE",
    "B_LICENSED_EXACT_CHILD_UNION",
    "C_TASK_DUPLICATION",
    "D_FRAGMENTATION_DEFECT",
    "E_CANDIDATE_DEFECT",
    "F_CAPACITY_ACCOUNTING_DEFECT",
})

SCOPE_RELATIONS = (
    "SAME_SCOPE",
    "PARENT_CHILD",
    "OVERLAP",
    "UNRELATED",
    "UNKNOWN",
)


def _read_json(path: Path) -> dict[str, Any]:
    return base_smoke._read_json(path)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return base_smoke._read_jsonl(path)


def _sha_file(path: Path) -> str:
    return base_smoke._sha_file(path)


def _display_path(path: Path) -> str:
    return base_smoke._display_path(path)


def _json_bytes(value: Any) -> bytes:
    return stratified._json_bytes(value)


# ---------------------------------------------------------------------------
# frozen inputs
# ---------------------------------------------------------------------------


def load_sources() -> dict[str, Any]:
    """Load every frozen research input the forensics depends on."""
    sources = stratified._load_sources()
    raw_candidates = {
        pair_id: {
            str(value["candidate_id"]): value
            for value in sources["raw"][pair_id]["functional_candidates"]
        }
        for pair_id in stratified.PAIR_PROJECTS
    }
    fragments = {
        pair_id: sources["raw"][pair_id]["function_fragments"]
        for pair_id in stratified.PAIR_PROJECTS
    }
    evidence = {
        pair_id: sources["raw"][pair_id]["evidence_catalog"]
        for pair_id in stratified.PAIR_PROJECTS
    }
    scoped_tasks = {
        str(value["scoped_task_id"]): value for value in sources["tasks"]["tasks"]
    }
    fragment_tasks = {
        pair_id: {
            str(value["task_id"]): value
            for value in sources["raw"][pair_id]["candidate_tasks"]
        }
        for pair_id in stratified.PAIR_PROJECTS
    }
    groups = [dict(value) for value in sources["groups"]["groups"]]
    return {
        "raw_candidates": raw_candidates,
        "fragments": fragments,
        "evidence": evidence,
        "scoped_tasks": scoped_tasks,
        "fragment_tasks": fragment_tasks,
        "groups": groups,
        "scope_graph": _read_json(SCOPE_ROOT / "function_scope_graph.json"),
    }


def load_evaluation() -> dict[str, Any]:
    """Load the frozen v2.5 evaluation this forensics reasons about."""
    missing = [
        name for name in EVALUATION_SOURCES
        if not (EVALUATION_ROOT / name).exists()
    ]
    if missing:
        raise RuntimeError(f"frozen v2.5 evaluation artifacts are missing: {missing}")
    return {
        "population": _read_json(EVALUATION_ROOT / "stratified_population.json"),
        "sample": _read_json(EVALUATION_ROOT / "stratified_sample.json"),
        "records": _read_jsonl(EVALUATION_ROOT / "model_runs.jsonl"),
        "metrics": _read_json(EVALUATION_ROOT / "metrics.json"),
        "sha256": {
            name: _sha_file(EVALUATION_ROOT / name) for name in EVALUATION_SOURCES
        },
    }


# ---------------------------------------------------------------------------
# deterministic relations
# ---------------------------------------------------------------------------


def _left_fragments_for_key(candidate: Mapping[str, Any], key: str) -> list[str]:
    return sorted({
        str(row["left_fragment_id"])
        for row in candidate.get("component_map") or []
        if str(row.get("capacity_key")) == key
    })


def _mapping_rows(candidate: Mapping[str, Any]) -> set[tuple[str, str]]:
    return {
        (str(row["left_fragment_id"]), str(row["right_fragment_id"]))
        for row in candidate.get("component_map") or []
    }


def scope_relation(
    left_task: Mapping[str, Any] | None,
    right_task: Mapping[str, Any] | None,
) -> str:
    """Classify two scoped tasks by their required component sets."""
    if left_task is None or right_task is None:
        return "UNKNOWN"
    left = frozenset(left_task.get("required_component_ids") or [])
    right = frozenset(right_task.get("required_component_ids") or [])
    if not left or not right:
        return "UNKNOWN"
    if left == right:
        return "SAME_SCOPE"
    if left < right or right < left:
        return "PARENT_CHILD"
    if left & right:
        return "OVERLAP"
    return "UNRELATED"


def _exact_union_index(groups: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str], list[str]]:
    """Map an unordered child-candidate pair to certified exact-union parents."""
    index: dict[tuple[str, str], list[str]] = defaultdict(list)
    for row in groups:
        if row.get("classification") != "EXACT_CHILD_UNION":
            continue
        children = sorted({str(value) for value in row.get("child_candidate_ids") or []})
        for left, right in itertools.combinations(children, 2):
            index[(left, right)].append(str(row["candidate_id"]))
    return {key: sorted(value) for key, value in index.items()}


def _merged_cover_exists(
    raw_candidates: Mapping[str, Mapping[str, Any]],
    *,
    key: str,
    left_fragments: Iterable[str],
) -> list[str]:
    """Candidates whose mapping onto ``key`` covers all given LEFT fragments."""
    wanted = set(left_fragments)
    if not wanted:
        return []
    return sorted(
        candidate_id
        for candidate_id, candidate in raw_candidates.items()
        if wanted <= set(_left_fragments_for_key(candidate, key))
    )


def _evidence_support(
    candidate: Mapping[str, Any],
    *,
    fragment_id: str,
    evidence: Mapping[str, Mapping[str, Any]],
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for ref in candidate.get("evidence_refs") or []:
        row = evidence.get(str(ref))
        if row is None or str(row.get("owner_fragment_id")) != fragment_id:
            continue
        counts[str(row.get("provenance_type"))] += 1
    return {
        "FRAGMENT_OWNED_EVIDENCE": counts.get("FRAGMENT_OWNED_EVIDENCE", 0),
        "SHEET_SHARED_EVIDENCE": counts.get("SHEET_SHARED_EVIDENCE", 0),
    }


def classify_conflict(
    *,
    pair_id: str,
    capacity_key: str,
    claims: Sequence[Mapping[str, Any]],
    sources: Mapping[str, Any],
    union_index: Mapping[tuple[str, str], Sequence[str]],
) -> dict[str, Any]:
    """Classify one two-claim capacity conflict deterministically."""
    raw_candidates = sources["raw_candidates"][pair_id]
    fragments = sources["fragments"][pair_id]
    evidence = sources["evidence"][pair_id]
    scoped_tasks = sources["scoped_tasks"]

    left_claim, right_claim = claims
    left_id = str(left_claim["candidate_id"])
    right_id = str(right_claim["candidate_id"])
    left_candidate = raw_candidates.get(left_id)
    right_candidate = raw_candidates.get(right_id)
    fragment_id = capacity_key.split(":")[-1]
    right_fragment = fragments["RIGHT"].get(fragment_id)

    if left_candidate is None or right_candidate is None or right_fragment is None:
        return {
            "root_cause_class": "G_UNKNOWN",
            "reason": "MISSING_FROZEN_INPUT",
            "candidate_relationship": "unknown",
            "scope_relationship": "UNKNOWN",
        }

    left_task = scoped_tasks.get(str(left_claim.get("task_id") or ""))
    right_task = scoped_tasks.get(str(right_claim.get("task_id") or ""))
    relation = scope_relation(left_task, right_task)

    left_sources = frozenset(str(value) for value in left_candidate["left_fragment_ids"])
    right_sources = frozenset(str(value) for value in right_candidate["left_fragment_ids"])
    left_key_fragments = _left_fragments_for_key(left_candidate, capacity_key)
    right_key_fragments = _left_fragments_for_key(right_candidate, capacity_key)
    union_parents = list(union_index.get(tuple(sorted((left_id, right_id))), ()))

    detail: dict[str, Any] = {
        "left_scope_kind": (left_task or {}).get("scope_kind"),
        "right_scope_kind": (right_task or {}).get("scope_kind"),
        "left_required_component_ids": sorted((left_task or {}).get("required_component_ids") or []),
        "right_required_component_ids": sorted((right_task or {}).get("required_component_ids") or []),
        "left_source_fragment_ids": sorted(left_sources),
        "right_source_fragment_ids": sorted(right_sources),
        "left_fragment_ids_for_key": left_key_fragments,
        "right_fragment_ids_for_key": right_key_fragments,
        "left_relation_type": left_candidate.get("relation_type"),
        "right_relation_type": right_candidate.get("relation_type"),
        "right_fragment_function_class": right_fragment.get("function_class"),
        "right_fragment_physical_page": right_fragment.get("physical_page"),
        "certified_exact_union_parents": union_parents,
        "left_evidence_support": _evidence_support(
            left_candidate, fragment_id=fragment_id, evidence=evidence
        ),
        "right_evidence_support": _evidence_support(
            right_candidate, fragment_id=fragment_id, evidence=evidence
        ),
    }

    left_classes = sorted({
        str(fragments["LEFT"][value]["function_class"])
        for value in left_key_fragments if value in fragments["LEFT"]
    })
    right_classes = sorted({
        str(fragments["LEFT"][value]["function_class"])
        for value in right_key_fragments if value in fragments["LEFT"]
    })
    detail["left_claim_function_classes"] = left_classes
    detail["right_claim_function_classes"] = right_classes
    detail["class_mismatched_claims"] = sorted({
        candidate_id
        for candidate_id, classes in ((left_id, left_classes), (right_id, right_classes))
        if classes and classes != [str(right_fragment.get("function_class"))]
    })

    if left_id == right_id:
        return {
            "root_cause_class": "F_CAPACITY_ACCOUNTING_DEFECT",
            "reason": "SAME_CANDIDATE_REPORTED_AS_CONFLICT",
            "candidate_relationship": "same candidate",
            "scope_relationship": relation,
            **detail,
        }

    if relation == "SAME_SCOPE" and left_claim.get("task_id") != right_claim.get("task_id"):
        return {
            "root_cause_class": "C_TASK_DUPLICATION",
            "reason": "TWO_TASKS_SHARE_ONE_FUNCTIONAL_IDENTITY",
            "candidate_relationship": "different candidates same source scope",
            "scope_relationship": relation,
            **detail,
        }

    nested = left_sources < right_sources or right_sources < left_sources
    if nested:
        smaller, larger = (
            (left_candidate, right_candidate)
            if left_sources < right_sources
            else (right_candidate, left_candidate)
        )
        if _mapping_rows(smaller) <= _mapping_rows(larger):
            return {
                "root_cause_class": "B_HIERARCHICAL_DUPLICATE",
                "reason": "CHILD_MAPPING_IS_CONTAINED_IN_PARENT_MAPPING",
                "candidate_relationship": "child candidate vs parent",
                "scope_relationship": relation,
                **detail,
            }

    if union_parents:
        return {
            "root_cause_class": "B_LICENSED_EXACT_CHILD_UNION",
            "reason": "BOTH_CLAIMS_ARE_CHILDREN_OF_ONE_CERTIFIED_EXACT_UNION",
            "candidate_relationship": "child candidates of one EXACT_CHILD_UNION parent",
            "scope_relationship": relation,
            **detail,
        }

    left_owner = set(left_key_fragments)
    right_owner = set(right_key_fragments)
    if left_owner and right_owner and (
        left_owner == right_owner or left_owner < right_owner or right_owner < left_owner
    ):
        # Equal or nested ownership is one composed mapping expressed twice.
        # The capacity rule licenses it, so reaching here is an accounting bug.
        return {
            "root_cause_class": "D_FRAGMENTATION_DEFECT",
            "reason": "ONE_ATOMIC_MAPPING_IS_SPLIT_ACROSS_TWO_CANDIDATE_OBJECTS",
            "candidate_relationship": "different candidates same target fragment",
            "scope_relationship": relation,
            **detail,
        }

    unsupported = [
        candidate_id
        for candidate_id, support in (
            (left_id, detail["left_evidence_support"]),
            (right_id, detail["right_evidence_support"]),
        )
        if support["FRAGMENT_OWNED_EVIDENCE"] == 0
    ]
    if unsupported:
        return {
            "root_cause_class": "E_CANDIDATE_DEFECT",
            "reason": "CLAIM_HAS_NO_FRAGMENT_OWNED_EVIDENCE_FOR_THE_CONTESTED_FRAGMENT",
            "candidate_relationship": "other",
            "scope_relationship": relation,
            "unsupported_claim_candidate_ids": unsupported,
            **detail,
        }

    cover = _merged_cover_exists(
        raw_candidates,
        key=capacity_key,
        left_fragments=left_owner | right_owner,
    )
    # Partially overlapping ownership ({A,B} -> R against {B,C} -> R) is not a
    # fragmentation defect: the two claims assert different merge arities onto
    # one fragment and neither licenses the other.  It stays a true conflict,
    # but it is named so the representability gap is never hidden.
    partial_overlap = bool(left_owner & right_owner)
    return {
        "root_cause_class": "A_TRUE_FUNCTION_FRAGMENT_CONFLICT",
        "reason": (
            "INCOMPATIBLE_MERGE_ARITY_ONTO_ONE_EXACT_RIGHT_FRAGMENT"
            if partial_overlap
            else "TWO_EXCLUSIVE_LINEAGES_CLAIM_ONE_EXACT_RIGHT_FRAGMENT"
        ),
        "subclass": (
            "INCOMPATIBLE_MERGE_ARITY" if partial_overlap
            else "DISJOINT_LINEAGE_REUSE"
        ),
        "candidate_relationship": "true incompatible reuse",
        "scope_relationship": relation,
        "shared_owner_fragment_ids": sorted(left_owner & right_owner),
        "representable_convergence_candidate_ids": cover,
        "convergence_representable": bool(cover),
        **detail,
    }


# ---------------------------------------------------------------------------
# observed conflicts
# ---------------------------------------------------------------------------


def observed_conflicts(
    evaluation: Mapping[str, Any], sources: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Reconstruct every conflict observed in the frozen v2.5 model runs."""
    union_index = _exact_union_index(sources["groups"])
    decisions: dict[tuple[str, int, str], dict[str, str]] = defaultdict(dict)
    for record in evaluation["records"]:
        run_key = (str(record["pair_id"]), int(record["cold_run"]), str(record["pass_name"]))
        for result in (record.get("response") or {}).get("results") or []:
            decisions[run_key][str(result["task_id"])] = str(result["decision"])

    observations: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for record in evaluation["records"]:
        run_key = (str(record["pair_id"]), int(record["cold_run"]), str(record["pass_name"]))
        for error in (record.get("capacity_verification") or {}).get("errors") or []:
            parts = str(error).split(":")
            if len(parts) != 6 or parts[0] != "FUNCTION_FRAGMENT_CONFLICT":
                continue
            capacity_key = ":".join(parts[1:4])
            first, second = parts[4], parts[5]
            signature = (run_key[0], capacity_key, first, second)
            entry = observations.setdefault(signature, {
                "pair_id": run_key[0],
                "corpus": stratified.PAIR_PROJECTS[run_key[0]],
                "capacity_key": capacity_key,
                "candidate_ids": [first, second],
                "runs": set(),
                "error": str(error),
            })
            entry["runs"].add((run_key[1], run_key[2]))

    rows: list[dict[str, Any]] = []
    for signature in sorted(observations):
        entry = observations[signature]
        runs = sorted(entry.pop("runs"))
        first, second = entry["candidate_ids"]
        claims = []
        for candidate_id in (first, second):
            task_ids = sorted({
                task_id
                for cold_run, pass_name in runs
                for task_id, decision in decisions[
                    (entry["pair_id"], cold_run, pass_name)
                ].items()
                if decision == candidate_id
            })
            claims.append({
                "candidate_id": candidate_id,
                "task_id": task_ids[0] if len(task_ids) == 1 else None,
                "task_ids": task_ids,
            })
        verdict = classify_conflict(
            pair_id=entry["pair_id"],
            capacity_key=entry["capacity_key"],
            claims=claims,
            sources=sources,
            union_index=union_index,
        )
        rows.append({
            **entry,
            "claims": claims,
            "observation_runs": [
                {"cold_run": cold_run, "pass_name": pass_name}
                for cold_run, pass_name in runs
            ],
            "observation_count": len(runs),
            "verdict": verdict,
        })
    return rows


# ---------------------------------------------------------------------------
# latent conflict surface over the whole scoped population
# ---------------------------------------------------------------------------


def latent_inventory(
    evaluation: Mapping[str, Any], sources: Mapping[str, Any]
) -> dict[str, Any]:
    """Classify every capacity collision reachable in the scoped population.

    A collision is reachable when two scoped tasks can each select a candidate
    that consumes the same exact RIGHT fragment.  The V2.5 sample realised nine
    of them; the architecture must be judged on all of them.
    """
    union_index = _exact_union_index(sources["groups"])
    population = evaluation["population"]["tasks"]
    by_corpus: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in population:
        by_corpus[str(row["corpus"])].append(row)

    counts: Counter[str] = Counter({name: 0 for name in ROOT_CAUSE_CLASSES})
    relation_counts: dict[str, Counter[str]] = {
        relation: Counter({name: 0 for name in ROOT_CAUSE_CLASSES})
        for relation in SCOPE_RELATIONS
    }
    corpus_counts: dict[str, Counter[str]] = {
        corpus: Counter({name: 0 for name in ROOT_CAUSE_CLASSES})
        for corpus in stratified.CORPUS_ORDER
    }
    checked_pairs = 0
    colliding_pairs = 0

    for corpus in stratified.CORPUS_ORDER:
        rows = sorted(by_corpus.get(corpus, ()), key=lambda value: str(value["task_id"]))
        pair_id = stratified.PROJECT_PAIRS[corpus]
        raw_candidates = sources["raw_candidates"][pair_id]
        keys_by_candidate = {
            str(candidate_id): frozenset(
                str(value) for value in raw_candidates[str(candidate_id)]["right_capacity_keys"]
            )
            for row in rows for candidate_id in row["candidate_ids"]
            if str(candidate_id) in raw_candidates
        }
        for left_row, right_row in itertools.combinations(rows, 2):
            checked_pairs += 1
            collided = False
            for left_id in sorted(left_row["candidate_ids"]):
                left_keys = keys_by_candidate.get(str(left_id))
                if not left_keys:
                    continue
                for right_id in sorted(right_row["candidate_ids"]):
                    if str(left_id) == str(right_id):
                        continue
                    right_keys = keys_by_candidate.get(str(right_id))
                    if not right_keys:
                        continue
                    shared = sorted(left_keys & right_keys)
                    if not shared:
                        continue
                    collided = True
                    for capacity_key in shared:
                        verdict = classify_conflict(
                            pair_id=pair_id,
                            capacity_key=capacity_key,
                            claims=(
                                {"candidate_id": str(left_id), "task_id": str(left_row["task_id"])},
                                {"candidate_id": str(right_id), "task_id": str(right_row["task_id"])},
                            ),
                            sources=sources,
                            union_index=union_index,
                        )
                        name = str(verdict["root_cause_class"])
                        counts[name] += 1
                        relation_counts[str(verdict["scope_relationship"])][name] += 1
                        corpus_counts[corpus][name] += 1
            if collided:
                colliding_pairs += 1

    return {
        "scoped_task_pairs_examined": checked_pairs,
        "scoped_task_pairs_with_capacity_collision": colliding_pairs,
        "classified_collisions": sum(counts.values()),
        "counts": dict(sorted(counts.items())),
        "counts_by_scope_relation": {
            relation: dict(sorted(values.items()))
            for relation, values in sorted(relation_counts.items())
        },
        "counts_by_corpus": {
            corpus: dict(sorted(values.items()))
            for corpus, values in sorted(corpus_counts.items())
        },
        "false_conflict_collisions": sum(
            counts[name] for name in FALSE_CONFLICT_CLASSES
        ),
        "true_conflict_collisions": counts["A_TRUE_FUNCTION_FRAGMENT_CONFLICT"],
    }


def exact_child_union_exposure(
    evaluation: Mapping[str, Any], sources: Mapping[str, Any]
) -> dict[str, Any]:
    """Measure how many certified exact unions can produce a false conflict."""
    population = {str(row["task_id"]): row for row in evaluation["population"]["tasks"]}
    owner: dict[str, str] = {}
    for row in population.values():
        for candidate_id in row["candidate_ids"]:
            owner[str(candidate_id)] = str(row["task_id"])

    total = 0
    exposed = 0
    sibling_pairs = 0
    rows: list[dict[str, Any]] = []
    for group in sorted(sources["groups"], key=lambda value: str(value["candidate_id"])):
        if group.get("classification") != "EXACT_CHILD_UNION":
            continue
        total += 1
        pair_id = str(group["pair_id"])
        raw_candidates = sources["raw_candidates"][pair_id]
        children = sorted({str(value) for value in group.get("child_candidate_ids") or []})
        selectable = [value for value in children if value in owner]
        conflicting: list[list[str]] = []
        for left, right in itertools.combinations(selectable, 2):
            left_task = population.get(owner[left])
            right_task = population.get(owner[right])
            if left_task is None or right_task is None:
                continue
            if scope_relation(left_task, right_task) != "UNRELATED":
                continue
            shared = set(raw_candidates[left]["right_capacity_keys"]) & set(
                raw_candidates[right]["right_capacity_keys"]
            )
            if shared:
                conflicting.append([left, right])
        if conflicting:
            exposed += 1
            sibling_pairs += len(conflicting)
        rows.append({
            "candidate_id": str(group["candidate_id"]),
            "pair_id": pair_id,
            "project": str(group["project"]),
            "child_candidate_ids": children,
            "selectable_child_candidate_ids": selectable,
            "all_children_selectable": len(selectable) == len(children),
            "unrelated_sibling_pairs_sharing_capacity": conflicting,
        })
    return {
        "certified_exact_child_union_groups": total,
        "groups_exposed_to_false_sibling_conflict": exposed,
        "unrelated_sibling_pairs_sharing_capacity": sibling_pairs,
        "groups": rows,
    }


# ---------------------------------------------------------------------------
# artifact
# ---------------------------------------------------------------------------


def build() -> dict[str, Any]:
    sources = load_sources()
    evaluation = load_evaluation()
    conflicts = observed_conflicts(evaluation, sources)
    latent = latent_inventory(evaluation, sources)
    exposure = exact_child_union_exposure(evaluation, sources)

    observed_counts: Counter[str] = Counter({name: 0 for name in ROOT_CAUSE_CLASSES})
    observation_counts: Counter[str] = Counter({name: 0 for name in ROOT_CAUSE_CLASSES})
    for row in conflicts:
        name = str(row["verdict"]["root_cause_class"])
        observed_counts[name] += 1
        observation_counts[name] += int(row["observation_count"])

    reported = evaluation["metrics"]["safety"]
    ranked = _rank_root_causes(observed_counts, latent, exposure)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_lineage_capacity_conflict_forensics",
        "model_calls": 0,
        "production_head": stratified.PRODUCTION_HEAD,
        "production_release": stratified.PRODUCTION_RELEASE,
        "evaluation_sha256": evaluation["sha256"],
        "reported_v2_5_safety": {
            "FUNCTION_FRAGMENT_CONFLICT": reported["FUNCTION_FRAGMENT_CONFLICT"],
            "RIGHT_MAP_CONFLICT": reported["RIGHT_MAP_CONFLICT"],
            "capacity_defect_count": reported["capacity_defect_count"],
            "unsupported_accepted_match_count": reported["unsupported_accepted_match_count"],
            "verifier_rejects": reported["verifier_rejects"],
        },
        "observed": {
            "unique_conflicts": len(conflicts),
            "observation_repeats": sum(int(row["observation_count"]) for row in conflicts),
            "counts": dict(sorted(observed_counts.items())),
            "observation_counts": dict(sorted(observation_counts.items())),
            "false_conflicts": sum(
                observed_counts[name] for name in FALSE_CONFLICT_CLASSES
            ),
            "true_conflicts": observed_counts["A_TRUE_FUNCTION_FRAGMENT_CONFLICT"],
            "conflicts": conflicts,
        },
        "latent": latent,
        "exact_child_union_exposure": {
            key: value for key, value in exposure.items() if key != "groups"
        },
        "exact_child_union_groups": exposure["groups"],
        "ranked_root_causes": ranked,
    }


def _rank_root_causes(
    observed: Mapping[str, int],
    latent: Mapping[str, Any],
    exposure: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows = [
        {
            "rank": 1,
            "root_cause": "LATENT_FALSE_CONFLICT_BETWEEN_CERTIFIED_EXACT_UNION_SIBLINGS",
            "class": "B_LICENSED_EXACT_CHILD_UNION",
            "observed_in_v2_5": observed.get("B_LICENSED_EXACT_CHILD_UNION", 0),
            "latent_collisions": latent["counts"]["B_LICENSED_EXACT_CHILD_UNION"],
            "exposed_groups": exposure["groups_exposed_to_false_sibling_conflict"],
            "statement": (
                "Two atomic child lineages of one certified EXACT_CHILD_UNION are "
                "co-owners of one composed mapping, but capacity accounting keys on "
                "candidate_id, so their compatible claims are rejected as a conflict."
            ),
        },
        {
            "rank": 2,
            "root_cause": "PARENT_CHILD_DOUBLE_CONSUMPTION_HANDLED_ONLY_BY_A_HARNESS_HEURISTIC",
            "class": "B_HIERARCHICAL_DUPLICATE",
            "observed_in_v2_5": observed.get("B_HIERARCHICAL_DUPLICATE", 0),
            "latent_collisions": latent["counts"]["B_HIERARCHICAL_DUPLICATE"],
            "statement": (
                "The v2.5 harness skipped every task pair with intersecting source "
                "components, so parent/child double consumption never surfaced; the "
                "production verifier has no equivalent rule."
            ),
        },
        {
            "rank": 3,
            "root_cause": "TRUE_CONFLICTS_SCORED_AS_AN_ARCHITECTURE_SAFETY_DEFECT",
            "class": "A_TRUE_FUNCTION_FRAGMENT_CONFLICT",
            "observed_in_v2_5": observed.get("A_TRUE_FUNCTION_FRAGMENT_CONFLICT", 0),
            "latent_collisions": latent["counts"]["A_TRUE_FUNCTION_FRAGMENT_CONFLICT"],
            "statement": (
                "The verifier correctly rejected mutually exclusive claims, but the "
                "verdict rule counts any conflict as a defect, so correct fail-closed "
                "behaviour produced verdict D."
            ),
        },
    ]
    return rows


def write(output: Path | None = None) -> Path:
    target = Path(output or DEFAULT_OUTPUT)
    target.mkdir(parents=True, exist_ok=True)
    artifact = build()
    (target / "capacity_conflict_forensics.json").write_bytes(_json_bytes(artifact))
    (target / "report.md").write_text(render_report(artifact), encoding="utf-8")
    return target


def render_report(artifact: Mapping[str, Any]) -> str:
    observed = artifact["observed"]
    latent = artifact["latent"]
    exposure = artifact["exact_child_union_exposure"]
    lines = [
        "# Function Lineage v2.6 — capacity conflict forensics (Phase 1)",
        "",
        "Deterministic replay of the frozen v2.5 stratified evaluation. "
        "No model calls, no production state, no shadow.",
        "",
        "## Observed v2.5 conflicts",
        "",
        f"Unique conflicts `{observed['unique_conflicts']}`; observation repeats "
        f"`{observed['observation_repeats']}`; "
        f"true `{observed['true_conflicts']}`; false `{observed['false_conflicts']}`.",
        "",
        "| Root cause | Unique | Repeats |",
        "|---|---:|---:|",
    ]
    for name in ROOT_CAUSE_CLASSES:
        lines.append(
            f"| `{name}` | {observed['counts'][name]} | {observed['observation_counts'][name]} |"
        )
    lines.extend([
        "",
        "### Reconstructed conflicts",
        "",
        "| Corpus | Capacity key | Claim A | Claim B | Scope relation | Candidate relation | Class |",
        "|---|---|---|---|---|---|---|",
    ])
    for row in observed["conflicts"]:
        verdict = row["verdict"]
        first, second = row["claims"]
        lines.append(
            f"| {row['corpus']} | `{row['capacity_key']}` | "
            f"`{first['candidate_id']}` ({first['task_id']}) | "
            f"`{second['candidate_id']}` ({second['task_id']}) | "
            f"{verdict['scope_relationship']} | {verdict['candidate_relationship']} | "
            f"`{verdict['root_cause_class']}` |"
        )
    lines.extend([
        "",
        "## Latent conflict surface (whole 213-task scoped population)",
        "",
        f"Scoped task pairs examined `{latent['scoped_task_pairs_examined']}`; "
        f"pairs with a reachable capacity collision "
        f"`{latent['scoped_task_pairs_with_capacity_collision']}`; "
        f"classified collisions `{latent['classified_collisions']}`.",
        "",
        "A reachable collision is a pair of candidates two scoped tasks *may* both "
        "select. `A_TRUE_FUNCTION_FRAGMENT_CONFLICT` therefore measures the surface "
        "the verifier must guard, not a defect count. Every other class is a claim "
        "pair the current accounting would reject although the frozen deterministic "
        "evidence proves the two claims are one composed mapping.",
        "",
        "| Root cause | Reachable collisions |",
        "|---|---:|",
    ])
    for name in ROOT_CAUSE_CLASSES:
        lines.append(f"| `{name}` | {latent['counts'][name]} |")
    lines.extend([
        "",
        "| Scope relation | " + " | ".join(f"`{name}`" for name in ROOT_CAUSE_CLASSES) + " |",
        "|---" * (len(ROOT_CAUSE_CLASSES) + 1) + "|",
    ])
    for relation in SCOPE_RELATIONS:
        values = latent["counts_by_scope_relation"][relation]
        lines.append(
            f"| {relation} | " + " | ".join(str(values[name]) for name in ROOT_CAUSE_CLASSES) + " |"
        )
    lines.extend([
        "",
        "## EXACT_CHILD_UNION exposure",
        "",
        f"Certified groups `{exposure['certified_exact_child_union_groups']}`; "
        f"groups whose unrelated sibling children can collide on capacity "
        f"`{exposure['groups_exposed_to_false_sibling_conflict']}`; "
        f"sibling pairs `{exposure['unrelated_sibling_pairs_sharing_capacity']}`.",
        "",
        "## Ranked root causes",
        "",
    ])
    for row in artifact["ranked_root_causes"]:
        lines.extend([
            f"{row['rank']}. **{row['root_cause']}** (`{row['class']}`) — "
            f"observed `{row['observed_in_v2_5']}`, latent `{row['latent_collisions']}`.",
            f"   {row['statement']}",
        ])
    lines.append("")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args(list(argv) if argv is not None else None)
    target = write(args.output)
    print(json.dumps({"output": str(target)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI
    raise SystemExit(main())

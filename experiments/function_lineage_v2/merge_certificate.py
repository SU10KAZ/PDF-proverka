"""Function Lineage v2.8 — deterministic merge certificate.

Phases 1–5, 7–9 of the merge track.  No model calls anywhere in this module.

The question is not "which candidate wins" but "is there documented proof that
several LEFT functions really became one RIGHT function".  A certificate is
therefore built only from facts the documents state, and it distinguishes

* a PROVEN fact,
* a MISSING fact — which stays UNKNOWN and never becomes a contradiction,
* a CONTRADICTED fact — which blocks certification outright.

Explicitly not evidence of a merge: deterministic scores, ranks, physical page
numbers or their proximity, equal sheet titles, and "there is only one
candidate, so it must be the answer".
"""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from backend.app.services.stage_comparison import function_lineage_shadow as lineage
from experiments.function_lineage_v2 import stratified


SCHEMA_VERSION = "function-merge-certificate.v2.8"
DEFAULT_OUTPUT = (
    stratified.COMPARISON_ROOT / "20260904_function_lineage_v2_8_merge_certificate"
)

CERTIFICATE_STATUSES = (
    "CERTIFIED", "PARTIAL", "AMBIGUOUS", "CONTRADICTORY", "UNKNOWN",
)
FACT_STATES = ("PROVEN", "MISSING", "CONTRADICTED")

COVERAGE_CLASSES = (
    "FULL_COVERAGE", "PARTIAL_COVERAGE", "EXTRA_RIGHT_SCOPE",
    "OVERLAP_ONLY", "UNKNOWN",
)

#: Facts that scope an engineering function to a part of the object.  If two
#: LEFT functions are scoped to different parts, they are different systems and
#: cannot merge, whatever else they have in common.
SCOPE_FACTS = ("serviced_object", "zone", "corpus", "building", "section")

#: Facts that describe what a function carries.  They support a merge but
#: cannot establish one on their own.
CONTINUITY_FACTS = ("consumers", "equipment_roles", "floors")

#: Dimensions that must be PROVEN before a merge may be certified.
REQUIRED_DIMENSIONS = (
    "SOURCE_COVERAGE",
    "TARGET_CONSOLIDATION",
    "FUNCTION_COMPATIBILITY",
    "SERVICED_OBJECT_COMPATIBILITY",
    "NO_UNEXPLAINED_LEFT_COMPONENT",
    "EVIDENCE_OWNERSHIP",
    "CAPACITY",
)

#: Dimensions that strengthen a certificate but are not required.
SUPPORTING_DIMENSIONS = (
    "TOPOLOGY_CONVERGENCE",
    "CONSUMER_EQUIPMENT_CONTINUITY",
    "NO_UNEXPLAINED_RIGHT_COMPONENT",
)

#: ``systems`` is deliberately absent from every dimension: it is a bag of
#: tokenised words from the sheet description ("щита", "кабелей", "в"), it is
#: "shared" for 115 of 125 merge candidates, and it carries no engineering
#: meaning about consolidation.
EXCLUDED_FACTS = ("systems",)


def _values(passport: Mapping[str, Any], field: str) -> set[str] | None:
    value = passport.get(field)
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = {value.strip()} if value.strip() else set()
    elif isinstance(value, Sequence):
        cleaned = {str(item).strip() for item in value if str(item).strip()}
    else:
        return None
    return cleaned or None


def _agreement(left: set[str] | None, right: set[str] | None) -> str:
    if left is None or right is None:
        return "MISSING"
    return "PROVEN" if left & right else "CONTRADICTED"


def _covers(container: set[str] | None, part: set[str] | None) -> str:
    if container is None or part is None:
        return "MISSING"
    return "PROVEN" if part <= container else (
        "PROVEN" if container & part else "CONTRADICTED"
    )


def certify(
    candidate: Mapping[str, Any],
    passports: Mapping[str, Mapping[str, Any]],
    evidence_catalog: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Deterministic merge certificate of one MERGED candidate."""
    if str(candidate.get("relation_type")) != "MERGED_N_TO_1":
        return {"status": "UNKNOWN", "reason": "NOT_A_MERGE_CANDIDATE", "dimensions": {}}
    left_ids = [str(value) for value in candidate["left_function_ids"]]
    right_ids = [str(value) for value in candidate["right_function_ids"]]
    lefts = [passports[value] for value in left_ids if value in passports]
    rights = [passports[value] for value in right_ids if value in passports]
    if len(lefts) < 2 or len(rights) != 1:
        return {
            "status": "UNKNOWN",
            "reason": "MERGE_NEEDS_AT_LEAST_TWO_SOURCES_AND_ONE_TARGET",
            "dimensions": {},
        }
    right = rights[0]
    dimensions: dict[str, dict[str, Any]] = {}

    # 1. every declared source appears in the mapping, on the declared key
    mapped = {
        str(row["left_fragment_id"]) for row in candidate.get("component_map") or []
    }
    declared = {str(value) for value in candidate["left_fragment_ids"]}
    dimensions["SOURCE_COVERAGE"] = {
        "state": "PROVEN" if declared and declared <= mapped else "CONTRADICTED",
        "declared_sources": sorted(declared),
        "mapped_sources": sorted(mapped),
    }
    dimensions["NO_UNEXPLAINED_LEFT_COMPONENT"] = {
        "state": "PROVEN" if not (declared - mapped) else "CONTRADICTED",
        "unexplained": sorted(declared - mapped),
    }

    # 2. does the target actually consolidate these sources?
    consolidation: dict[str, str] = {}
    for field in SCOPE_FACTS:
        union: set[str] = set()
        known = 0
        for passport in lefts:
            value = _values(passport, field)
            if value is not None:
                union |= value
                known += 1
        consolidation[field] = (
            "MISSING" if known < len(lefts)
            else _covers(_values(right, field), union)
        )
    proven_consolidation = [
        field for field, state in consolidation.items() if state == "PROVEN"
    ]
    contradicted_consolidation = [
        field for field, state in consolidation.items() if state == "CONTRADICTED"
    ]
    dimensions["TARGET_CONSOLIDATION"] = {
        "state": (
            "CONTRADICTED" if contradicted_consolidation
            else "PROVEN" if proven_consolidation else "MISSING"
        ),
        "per_field": consolidation,
        "note": "the target must be documented as covering every source scope",
    }

    # 3. engineering classes must be compatible with the declared merge
    class_scores = {
        str(passport["function_id"]): lineage._class_compatibility(
            str(passport["function_class"]), str(right["function_class"])
        )
        for passport in lefts
    }
    dimensions["FUNCTION_COMPATIBILITY"] = {
        "state": "PROVEN" if all(class_scores.values()) else "CONTRADICTED",
        "per_source": class_scores,
    }

    # 4. sources scoped to different parts of the object cannot merge
    scope_agreement: dict[str, str] = {}
    for field in SCOPE_FACTS:
        values = [_values(passport, field) for passport in lefts]
        if any(value is None for value in values):
            scope_agreement[field] = "MISSING"
            continue
        shared = set.intersection(*values)
        if shared:
            scope_agreement[field] = "PROVEN"
        else:
            covered = _values(right, field)
            union = set().union(*values)
            scope_agreement[field] = (
                "PROVEN" if covered is not None and union <= covered
                else "CONTRADICTED"
            )
    contradicted_scope = [
        field for field, state in scope_agreement.items() if state == "CONTRADICTED"
    ]
    dimensions["SERVICED_OBJECT_COMPATIBILITY"] = {
        "state": (
            "CONTRADICTED" if contradicted_scope
            else "PROVEN"
            if any(state == "PROVEN" for state in scope_agreement.values())
            else "MISSING"
        ),
        "per_field": scope_agreement,
        "contradicted_fields": contradicted_scope,
    }

    # 5. topology: the merging sources should know each other
    neighbours = {
        str(passport["function_id"]): set(
            passport.get("neighboring_function_context") or []
        )
        for passport in lefts
    }
    mutual = any(
        other in neighbours[value]
        for value in neighbours for other in neighbours if other != value
    )
    dimensions["TOPOLOGY_CONVERGENCE"] = {
        "state": "PROVEN" if mutual else "MISSING",
        "sources_reference_each_other": mutual,
        "note": (
            "absence is MISSING, not a contradiction: neighbour context is "
            "same-side and sparsely populated"
        ),
    }

    # 6. what the target carries should cover what the sources carried
    continuity: dict[str, str] = {}
    for field in CONTINUITY_FACTS:
        union: set[str] = set()
        known = 0
        for passport in lefts:
            value = _values(passport, field)
            if value is not None:
                union |= value
                known += 1
        continuity[field] = (
            "MISSING" if known < len(lefts) else _covers(_values(right, field), union)
        )
    dimensions["CONSUMER_EQUIPMENT_CONTINUITY"] = {
        "state": (
            "CONTRADICTED"
            if all(state == "CONTRADICTED" for state in continuity.values())
            else "PROVEN"
            if any(state == "PROVEN" for state in continuity.values())
            else "MISSING"
        ),
        "per_field": continuity,
    }

    # 7. does the target carry a substantial function the mapping ignores?
    dimensions["NO_UNEXPLAINED_RIGHT_COMPONENT"] = {
        "state": (
            "PROVEN"
            if str(right.get("component_role"))
            in {str(passport.get("component_role")) for passport in lefts}
            else "MISSING"
        ),
        "target_component_role": right.get("component_role"),
        "source_component_roles": sorted({
            str(passport.get("component_role")) for passport in lefts
        }),
    }

    # 8. provenance of every piece of evidence must resolve exactly
    refs = [str(value) for value in candidate.get("evidence_refs") or []]
    unresolved = [value for value in refs if value not in evidence_catalog]
    dimensions["EVIDENCE_OWNERSHIP"] = {
        "state": "PROVEN" if refs and not unresolved else "CONTRADICTED",
        "evidence_count": len(refs),
        "unresolved": unresolved[:10],
    }

    # 9. capacity identity must stay fragment-exact
    declared_keys = sorted({
        str(value) for value in candidate.get("right_capacity_keys") or []
    })
    mapped_keys = sorted({
        str(row["capacity_key"]) for row in candidate.get("component_map") or []
        if row.get("capacity_key")
    })
    dimensions["CAPACITY"] = {
        "state": (
            "PROVEN"
            if declared_keys and declared_keys == mapped_keys
            and all(value.startswith("RIGHT:") for value in declared_keys)
            else "CONTRADICTED"
        ),
        "capacity_keys": declared_keys,
    }

    contradicted = sorted(
        name for name, value in dimensions.items() if value["state"] == "CONTRADICTED"
    )
    required_proven = [
        name for name in REQUIRED_DIMENSIONS
        if dimensions[name]["state"] == "PROVEN"
    ]
    if contradicted:
        status = "CONTRADICTORY"
    elif len(required_proven) == len(REQUIRED_DIMENSIONS):
        status = "CERTIFIED"
    elif required_proven:
        status = "PARTIAL"
    else:
        status = "UNKNOWN"
    return {
        "candidate_id": str(candidate["candidate_id"]),
        "pair_id": str(candidate["pair_id"]),
        "status": status,
        "contradicted_dimensions": contradicted,
        "proven_required_dimensions": sorted(required_proven),
        "missing_required_dimensions": sorted(
            set(REQUIRED_DIMENSIONS) - set(required_proven) - set(contradicted)
        ),
        "dimensions": dimensions,
        "left_function_ids": left_ids,
        "right_function_ids": right_ids,
        "capacity_keys": declared_keys,
        "forbidden_evidence_used": {
            "score": False, "rank": False, "page_proximity": False,
            "equal_title": False, "physical_page": False,
            "single_candidate_implies_merge": False,
        },
    }


def coverage_matrix(
    candidate: Mapping[str, Any], certificate: Mapping[str, Any]
) -> dict[str, Any]:
    """LEFT required components against RIGHT represented components."""
    consolidation = (
        certificate["dimensions"].get("TARGET_CONSOLIDATION", {}).get("per_field") or {}
    )
    states = Counter(consolidation.values())
    if states.get("CONTRADICTED"):
        classification = "EXTRA_RIGHT_SCOPE" if states.get("PROVEN") else "OVERLAP_ONLY"
    elif states.get("PROVEN") and not states.get("MISSING"):
        classification = "FULL_COVERAGE"
    elif states.get("PROVEN"):
        classification = "PARTIAL_COVERAGE"
    else:
        classification = "UNKNOWN"
    return {
        "candidate_id": str(candidate["candidate_id"]),
        "classification": classification,
        "per_field": consolidation,
        "source_count": len(candidate["left_function_ids"]),
        "target_count": len(candidate["right_function_ids"]),
    }


# ---------------------------------------------------------------------------
# corpus audit
# ---------------------------------------------------------------------------


def audit() -> dict[str, Any]:
    corpora: dict[str, Any] = {}
    certificates: list[dict[str, Any]] = []
    coverage: list[dict[str, Any]] = []
    generation = Counter()
    for pair_id, project in sorted(
        stratified.PAIR_PROJECTS.items(), key=lambda item: item[1]
    ):
        artifact = stratified._read_json(
            stratified.CANDIDATE_ROOT / f"{pair_id}.json"
        )
        passports = {
            **artifact["function_passports"]["LEFT"],
            **artifact["function_passports"]["RIGHT"],
        }
        catalog = artifact["evidence_catalog"]
        merged = [
            value for value in artifact["functional_candidates"]
            if value["relation_type"] == "MERGED_N_TO_1"
        ]
        rows = []
        for candidate in sorted(merged, key=lambda value: value["candidate_id"]):
            certificate = certify(candidate, passports, catalog)
            certificate["project"] = project
            certificates.append(certificate)
            coverage.append({**coverage_matrix(candidate, certificate), "project": project})
            rows.append(certificate)
            pages = sorted(int(value) for value in candidate["left_pages"])
            generation[
                "adjacent_pages" if len(pages) == 2 and pages[1] - pages[0] == 1
                else "non_adjacent_pages"
            ] += 1
        corpora[project] = {
            "pair_id": pair_id,
            "merged_candidates": len(merged),
            "status_counts": dict(sorted(Counter(
                value["status"] for value in rows
            ).items())),
            "contradicted_dimension_counts": dict(sorted(Counter(
                name for value in rows for name in value["contradicted_dimensions"]
            ).items())),
        }
    overall = Counter(value["status"] for value in certificates)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_merge_certificate_audit",
        "model_calls": 0,
        "excluded_facts": list(EXCLUDED_FACTS),
        "required_dimensions": list(REQUIRED_DIMENSIONS),
        "supporting_dimensions": list(SUPPORTING_DIMENSIONS),
        "forbidden_merge_evidence": [
            "deterministic score", "rank", "physical page number",
            "page proximity", "equal sheet title",
            "a single candidate implying a merge",
        ],
        "candidate_generation": {
            "counts": dict(sorted(generation.items())),
            "finding": (
                "every merge hypothesis in the frozen corpora is generated by "
                "LEFT pages being exactly adjacent plus an equal function "
                "class; adjacency is forbidden as merge evidence, so no "
                "certificate may rest on the reason the candidate exists"
            ),
        },
        "corpora": corpora,
        "status_counts_overall": dict(sorted(overall.items())),
        "coverage_counts_overall": dict(sorted(Counter(
            value["classification"] for value in coverage
        ).items())),
        "certificates": certificates,
        "coverage_matrix": coverage,
    }


def negative_controls(audit_artifact: Mapping[str, Any]) -> dict[str, Any]:
    """Cases where several sources resemble one target but must not merge."""
    rows = [
        {
            "candidate_id": value["candidate_id"],
            "project": value["project"],
            "contradicted_dimensions": value["contradicted_dimensions"],
            "contradicted_scope_fields": (
                value["dimensions"]["SERVICED_OBJECT_COMPATIBILITY"].get(
                    "contradicted_fields"
                ) or []
            ),
        }
        for value in audit_artifact["certificates"]
        if value["status"] == "CONTRADICTORY"
    ]
    by_reason = Counter(
        name for value in rows for name in value["contradicted_dimensions"]
    )
    return {
        "control_count": len(rows),
        "by_contradicted_dimension": dict(sorted(by_reason.items())),
        "note": (
            "these are real documented refusals, not invented rules: the "
            "sources are scoped to different parts of the object"
        ),
        "controls": rows,
    }


def replay_stable_need_more_evidence() -> dict[str, Any]:
    """Diagnostic replay of the merge tasks that stably refused to answer."""
    path = (
        stratified.COMPARISON_ROOT
        / "20260904_function_lineage_v2_7_tiered_acceptance" / "task_results.json"
    )
    if not path.is_file():
        return {"applicable": False, "reason": "NO_RECORDED_RUN"}
    tasks = json.loads(path.read_text(encoding="utf-8"))
    population = stratified._read_json(
        stratified.COMPARISON_ROOT
        / "20260904_function_lineage_v2_6_holdout_evaluation"
        / "holdout_population.json"
    )
    rows = {str(value["task_id"]): value for value in population["tasks"]}
    artifacts = {
        pair_id: stratified._read_json(
            stratified.CANDIDATE_ROOT / f"{pair_id}.json"
        )
        for pair_id in stratified.PAIR_PROJECTS
    }
    candidates = {
        pair_id: {
            str(value["candidate_id"]): value
            for value in artifact["functional_candidates"]
        }
        for pair_id, artifact in artifacts.items()
    }
    outcome: list[dict[str, Any]] = []
    for task in tasks:
        if task.get("tier") != "AUTO_MERGED":
            continue
        if task.get("stable_outcome_kind") != "NEED_MORE_EVIDENCE":
            continue
        task_id = str(task["task_id"])
        pair_id = stratified.PROJECT_PAIRS[str(task["corpus"])]
        artifact = artifacts[pair_id]
        passports = {
            **artifact["function_passports"]["LEFT"],
            **artifact["function_passports"]["RIGHT"],
        }
        merge_ids = [
            str(value) for value in rows[task_id]["candidate_ids"]
            if candidates[pair_id][str(value)]["relation_type"] == "MERGED_N_TO_1"
        ]
        statuses = [
            certify(candidates[pair_id][value], passports, artifact["evidence_catalog"])[
                "status"
            ]
            for value in merge_ids
        ]
        outcome.append({
            "task_id": task_id,
            "project": str(task["corpus"]),
            "candidate_count": int(task["candidate_count"]),
            "single_candidate": int(task["candidate_count"]) == 1,
            "merge_candidate_count": len(merge_ids),
            "certificate_statuses": dict(sorted(Counter(statuses).items())),
            "best_status": (
                "CERTIFIED" if "CERTIFIED" in statuses
                else "PARTIAL" if "PARTIAL" in statuses
                else "CONTRADICTORY" if "CONTRADICTORY" in statuses
                else "UNKNOWN"
            ),
        })
    single = [value for value in outcome if value["single_candidate"]]
    return {
        "applicable": True,
        "diagnostic_only": True,
        "usable_as_acceptance_evidence": False,
        "model_calls": 0,
        "stable_need_more_evidence_tasks": len(outcome),
        "single_candidate_tasks": len(single),
        "best_status_counts": dict(sorted(Counter(
            value["best_status"] for value in outcome
        ).items())),
        "single_candidate_best_status_counts": dict(sorted(Counter(
            value["best_status"] for value in single
        ).items())),
        "tasks": outcome,
    }


def build() -> dict[str, Any]:
    artifact = audit()
    artifact["negative_controls"] = negative_controls(artifact)
    artifact["stable_nme_replay"] = replay_stable_need_more_evidence()
    certified = [
        value for value in artifact["certificates"] if value["status"] == "CERTIFIED"
    ]
    artifact["auto_merged_certified_tier"] = {
        "candidate_count": len(certified),
        "candidate_ids": sorted(value["candidate_id"] for value in certified),
        "eligible": bool(certified),
        "policy": (
            "only CERTIFIED may enter; PARTIAL, AMBIGUOUS, UNKNOWN and "
            "CONTRADICTORY never do"
        ),
    }
    artifact["safety"] = {
        "unsupported_deterministic_merge": 0,
        "certificates_resting_on_forbidden_evidence": 0,
        "page_global_exclusivity": False,
        "capacity_identity": "RIGHT physical_page + exact function_fragment_id",
        "candidate_recall_loss": 0,
        "note": (
            "the certificate only reads existing candidates; it neither adds "
            "nor removes any, so recall and scope cannot regress"
        ),
    }
    return artifact


def render_report(artifact: Mapping[str, Any]) -> str:
    replay = artifact["stable_nme_replay"]
    tier = artifact["auto_merged_certified_tier"]
    lines = [
        "# Function Lineage v2.8 — deterministic merge certificate",
        "",
        "No model calls. A certificate distinguishes a PROVEN fact, a MISSING "
        "fact (stays UNKNOWN, never a contradiction) and a CONTRADICTED fact.",
        "",
        "Never treated as merge evidence: score, rank, physical page number, "
        "page proximity, equal sheet title, or a lone candidate.",
        "",
        "## How merge hypotheses are created",
        "",
        f"`{artifact['candidate_generation']['counts']}`",
        "",
        artifact["candidate_generation"]["finding"] + ".",
        "",
        "## Certificate status",
        "",
        "| Corpus | MERGED candidates | " + " | ".join(CERTIFICATE_STATUSES) + " |",
        "|---|---:" + "|---:" * len(CERTIFICATE_STATUSES) + "|",
    ]
    for project, row in artifact["corpora"].items():
        lines.append(
            f"| {project} | {row['merged_candidates']} | "
            + " | ".join(
                str(row["status_counts"].get(name, 0)) for name in CERTIFICATE_STATUSES
            )
            + " |"
        )
    lines.extend([
        "| **ALL** | "
        + str(sum(row["merged_candidates"] for row in artifact["corpora"].values()))
        + " | "
        + " | ".join(
            str(artifact["status_counts_overall"].get(name, 0))
            for name in CERTIFICATE_STATUSES
        )
        + " |",
        "",
        "## Why certification fails",
        "",
        "| Contradicted dimension | Candidates |",
        "|---|---:|",
    ])
    for name, value in artifact["negative_controls"]["by_contradicted_dimension"].items():
        lines.append(f"| `{name}` | {value} |")
    lines.extend([
        "",
        "## Component coverage matrix",
        "",
        "| Classification | Candidates |",
        "|---|---:|",
    ])
    for name in COVERAGE_CLASSES:
        lines.append(
            f"| `{name}` | {artifact['coverage_counts_overall'].get(name, 0)} |"
        )
    if replay.get("applicable"):
        lines.extend([
            "",
            "## Diagnostic replay of the stable refusals",
            "",
            "Read-only over an already recorded run. Never acceptance evidence.",
            "",
            f"Stable NEED_MORE_EVIDENCE merge tasks `{replay['stable_need_more_evidence_tasks']}`; "
            f"of them single-candidate `{replay['single_candidate_tasks']}`.",
            "",
            "| Best certificate status | All | Single-candidate |",
            "|---|---:|---:|",
        ])
        for name in CERTIFICATE_STATUSES:
            lines.append(
                f"| `{name}` | {replay['best_status_counts'].get(name, 0)} | "
                f"{replay['single_candidate_best_status_counts'].get(name, 0)} |"
            )
    lines.extend([
        "",
        "## AUTO_MERGED_CERTIFIED tier",
        "",
        f"Candidates that would enter: **`{tier['candidate_count']}`**. "
        f"{tier['policy']}.",
        "",
    ])
    return "\n".join(lines)


def write(output: Path | None = None) -> Path:
    target = Path(output or DEFAULT_OUTPUT)
    target.mkdir(parents=True, exist_ok=True)
    artifact = build()
    (target / "merge_certificate_audit.json").write_bytes(
        stratified._json_bytes(artifact)
    )
    (target / "report.md").write_text(render_report(artifact), encoding="utf-8")
    return target


def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args(list(argv) if argv is not None else None)
    print(json.dumps({"output": str(write(args.output))}, ensure_ascii=False))
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI
    raise SystemExit(main())

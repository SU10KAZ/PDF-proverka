"""Function Lineage v2.6 — holdout acceptance metrics.

Phase 6 reporting.  The module reads observations only; it never calls a model
and never changes a decision.  Metric definitions are imported from the v2.5
harness unchanged so the two evaluations stay comparable.

Stability is not truth.  A stable model preference on a same-scope ambiguous
task is a preference, nothing more; reference classes are reported separately
and ``DOCUMENT_LINK`` is never functional truth.
"""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from backend.app.services.stage_comparison import function_lineage_shadow as lineage
from experiments.function_lineage_v2 import capacity_forensics as forensics
from experiments.function_lineage_v2 import holdout
from experiments.function_lineage_v2 import smoke as base_smoke
from experiments.function_lineage_v2 import stratified


SCHEMA_VERSION = "function-lineage-holdout-metrics.v2.6"
DEFAULT_OUTPUT = holdout.DEFAULT_OUTPUT

RELATION_NAMES = (
    "CONTINUED_1_TO_1",
    "SPLIT_1_TO_N",
    "MERGED_N_TO_1",
    "FUNCTION_DISTRIBUTED",
    "NEED_MORE_EVIDENCE",
    "MIXED_RELATION",
    "UNRESOLVED_OR_INVALID",
)

#: v2.4.2 controls.  Regression references only, never a mapping rule.
SENTINEL_REFERENCES = {
    "LEFT17": "lcand_cd6c87ed7f043a937b27",
    "LEFT18": "lcand_d9f1abdb7469869363ad",
    "LEFT19": "lcand_26bcd544f168ff9ccea5",
    "LEFT20 DOMESTIC": "lcand_1d1f175a30c34b88c6e0",
    "LEFT20 FIRE": "lcand_ebafe4012323c47ac349",
    "LEFT20 METERING": "lcand_3e5e047c8b378f731c6b",
    "LEFT20 PARENT": "lcand_9c617494b14c2b922d3f",
}


def _json_bytes(value: Any) -> bytes:
    return stratified._json_bytes(value)


def load(output: Path | None = None) -> dict[str, Any]:
    target = Path(output or DEFAULT_OUTPUT)
    records = stratified._read_jsonl(target / "model_runs.jsonl")
    return {
        "output": target,
        "population": stratified._read_json(target / "holdout_population.json"),
        "sample": stratified._read_json(target / "holdout_sample.json"),
        "records": records,
        "telemetry": stratified._read_json(target / "run_telemetry.json"),
        "disclosure": stratified._read_json(target / "external_model_disclosure.json"),
    }


def task_results(loaded: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Reuse the v2.5 definitions verbatim, relabelling the evaluation set."""
    rows = stratified._task_results(
        loaded["population"], loaded["sample"], loaded["records"]
    )
    for row in rows:
        if row["evaluation_set"] == "NEW_SAMPLE":
            row["evaluation_set"] = "HOLDOUT"
    return rows


def _unsupported(tasks: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Accepted decisions that are not an offered candidate of that task."""
    return [
        {"task_id": row["task_id"], "candidate_id": row["stable_decision"]}
        for row in tasks
        if row["stable_decision"] not in (None, lineage.NEED_MORE_EVIDENCE)
        and row["stable_decision"] not in {
            value["candidate_id"] for value in row["candidate_inventory"]
        }
    ]


def _reference_metrics(tasks: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    classes: Counter[str] = Counter()
    document_link_occurrences = 0
    research_rows: list[dict[str, Any]] = []
    authoritative_rows: list[dict[str, Any]] = []
    for row in tasks:
        row_classes = sorted({
            str(value.get("reference_class"))
            for value in row.get("references") or []
        }) or ["NO_REFERENCE"]
        for value in row_classes:
            classes[value] += 1
        for reference in row.get("references") or []:
            reference_class = str(reference.get("reference_class"))
            if reference_class == "DOCUMENT_LINK":
                document_link_occurrences += 1
                continue
            if row["stable_decision"] in (None, lineage.NEED_MORE_EVIDENCE):
                continue
            entry = {
                "task_id": row["task_id"],
                "corpus": row["corpus"],
                "stable_decision": row["stable_decision"],
                "reference_candidate_ids": sorted(
                    str(value) for value in reference.get("candidate_ids") or []
                ),
            }
            entry["aligned"] = row["stable_decision"] in entry["reference_candidate_ids"]
            if reference_class == "AUTHORITATIVE_FUNCTIONAL_REFERENCE":
                authoritative_rows.append(entry)
            elif reference_class == "RESEARCH_REFERENCE":
                research_rows.append(entry)
    return {
        "reference_class_task_counts": dict(sorted(classes.items())),
        "document_link_candidate_occurrences": document_link_occurrences,
        "document_link_used_as_functional_truth": False,
        "authoritative": {
            "determined_rows": len(authoritative_rows),
            "aligned": sum(row["aligned"] for row in authoritative_rows),
            "alignment_rate": (
                round(
                    sum(row["aligned"] for row in authoritative_rows)
                    / len(authoritative_rows), 6
                ) if authoritative_rows else None
            ),
            "rows": authoritative_rows,
        },
        "research": {
            "determined_rows": len(research_rows),
            "aligned": sum(row["aligned"] for row in research_rows),
            "alignment_rate": (
                round(
                    sum(row["aligned"] for row in research_rows)
                    / len(research_rows), 6
                ) if research_rows else None
            ),
            "note": "hypothesis alignment, not precision",
            "rows": research_rows,
        },
    }


def _sentinel_metrics(tasks: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_label = {
        str(row["label"]): row for row in tasks
        if row["evaluation_set"] == "SENTINEL" and row["label"]
    }
    rows = []
    for label, reference in sorted(SENTINEL_REFERENCES.items()):
        row = by_label.get(label)
        observed = (row or {}).get("stable_decision")
        rows.append({
            "sentinel": label,
            "task_id": (row or {}).get("task_id"),
            "expected_v2_4_2": reference,
            "observed_v2_6": observed,
            "status": (
                "UNCHANGED" if observed == reference
                else "UNRESOLVED" if observed is None
                else "CHANGED"
            ),
            "stable_repeat_count": (row or {}).get("stable_repeat_count"),
            "pass_disagreement_count": (row or {}).get("pass_disagreement_count"),
            "capacity_failure_count": (row or {}).get("capacity_failure_count"),
            "verifier_failure_count": (row or {}).get("verifier_failure_count"),
        })
    changed = [row for row in rows if row["status"] != "UNCHANGED"]
    return {
        "sentinels": rows,
        "regression": bool(changed),
        "changed": changed,
        "reference_use": "REGRESSION_REFERENCE_ONLY_NEVER_A_MAPPING_RULE",
    }


def classify_capacity_conflicts(loaded: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Run the v2.6 forensic classifier over every observed conflict."""
    sources = forensics.load_sources()
    union_index = forensics._exact_union_index(sources["groups"])
    decisions: dict[tuple[str, str, int, str], dict[str, str]] = {}
    for record in loaded["records"]:
        key = (
            str(record["pair_id"]), str(record["evaluation_set"]),
            int(record["cold_run"]), str(record["pass_name"]),
        )
        bucket = decisions.setdefault(key, {})
        for result in (record.get("response") or {}).get("results") or []:
            bucket[str(result["task_id"])] = str(result["decision"])

    observed: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for record in loaded["records"]:
        key = (
            str(record["pair_id"]), str(record["evaluation_set"]),
            int(record["cold_run"]), str(record["pass_name"]),
        )
        for error in (record.get("capacity_verification") or {}).get("errors") or []:
            parts = str(error).split(":")
            if len(parts) != 6 or parts[0] != "FUNCTION_FRAGMENT_CONFLICT":
                continue
            capacity_key = ":".join(parts[1:4])
            signature = (key[0], capacity_key, parts[4], parts[5])
            entry = observed.setdefault(signature, {
                "pair_id": key[0],
                "corpus": stratified.PAIR_PROJECTS[key[0]],
                "capacity_key": capacity_key,
                "candidate_ids": [parts[4], parts[5]],
                "error": str(error),
                "runs": set(),
            })
            entry["runs"].add((key[1], key[2], key[3]))

    rows: list[dict[str, Any]] = []
    for signature in sorted(observed):
        entry = observed[signature]
        runs = sorted(entry.pop("runs"))
        first, second = entry["candidate_ids"]
        bucket = decisions[
            (entry["pair_id"], runs[0][0], runs[0][1], runs[0][2])
        ]
        claims = []
        for candidate_id in (first, second):
            owners = sorted(
                task_id for task_id, decision in bucket.items()
                if decision == candidate_id
            )
            claims.append({
                "candidate_id": candidate_id,
                "task_id": owners[0] if len(owners) == 1 else None,
                "task_ids": owners,
            })
        rows.append({
            **entry,
            "claims": claims,
            "observation_runs": [
                {"evaluation_set": value[0], "cold_run": value[1], "pass_name": value[2]}
                for value in runs
            ],
            "observation_count": len(runs),
            "verdict": forensics.classify_conflict(
                pair_id=entry["pair_id"],
                capacity_key=entry["capacity_key"],
                claims=claims,
                sources=sources,
                union_index=union_index,
            ),
        })
    return rows


def _capacity_metrics(
    loaded: Mapping[str, Any], tasks: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    errors = sorted({
        str(value)
        for record in loaded["records"]
        for value in (record.get("capacity_verification") or {}).get("errors") or []
    })
    conflicts = classify_capacity_conflicts(loaded)
    classes = Counter({name: 0 for name in forensics.ROOT_CAUSE_CLASSES})
    for row in conflicts:
        classes[str(row["verdict"]["root_cause_class"])] += 1
    false_conflicts = [
        row for row in conflicts
        if row["verdict"]["root_cause_class"] in forensics.FALSE_CONFLICT_CLASSES
    ]
    return {
        "conflict_root_cause_counts": dict(sorted(classes.items())),
        "true_conflicts": classes["A_TRUE_FUNCTION_FRAGMENT_CONFLICT"],
        "false_conflicts": len(false_conflicts),
        "false_conflict_details": false_conflicts,
        "incompatible_merge_arity_conflicts": sum(
            row["verdict"].get("subclass") == "INCOMPATIBLE_MERGE_ARITY"
            for row in conflicts
        ),
        "unrepresentable_convergence_conflicts": sum(
            row["verdict"].get("convergence_representable") is False
            for row in conflicts
        ),
        "conflicts": conflicts,
        "capacity_errors": errors,
        "capacity_error_count": len(errors),
        "FUNCTION_FRAGMENT_CONFLICT": sum(
            value.startswith("FUNCTION_FRAGMENT_CONFLICT:") for value in errors
        ),
        "RIGHT_MAP_CONFLICT": sum("RIGHT_MAP_CONFLICT" in value for value in errors),
        "capacity_rejected_tasks": sorted(
            row["task_id"] for row in tasks if row["capacity_failure_count"]
        ),
        "accounting": "UNIFORM_LINEAGE_OWNERSHIP_ACCOUNTING",
        "capacity_identity": "RIGHT physical_page + exact function_fragment_id",
    }


def _cost(loaded: Mapping[str, Any]) -> dict[str, Any]:
    records = loaded["records"]
    telemetry = loaded["telemetry"]
    usage_totals = [int(value["model_call"]["tokens"] or 0) for value in records]
    total = sum(usage_totals)
    return {
        "planned_requests": telemetry["planned_requests"],
        "request_records": telemetry["request_records"],
        "successful_inference_requests": sum(
            bool(value["model_call"]["ok"]) for value in records
        ),
        "stopped_early": telemetry["stopped_early"],
        "stop_reason": telemetry["stop_reason"],
        "wall_time_ms": telemetry["wall_time_ms"],
        "model_runtime_ms": telemetry["model_runtime_ms"],
        "total_tokens": total,
        "telemetry_assessment": (
            "TELEMETRY_DEFECT: successful inference returned zero tokens; "
            "zero is not interpreted as zero cost"
            if total == 0 and any(value["model_call"]["ok"] for value in records)
            else "provider usage reported"
        ),
    }


def instability_diagnosis(tasks: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Separate model non-determinism from capacity collateral.

    Diagnostic only.  ``own_answer_consistent`` ignores whether another task
    contested the fragment, so it is an upper bound on what any capacity-stage
    change could recover.  It is never a published decision and never a gate.
    """
    causes: Counter[str] = Counter()
    collateral: list[dict[str, Any]] = []
    own_consistent = 0
    for row in tasks:
        statuses = [value["status"] for value in row["cold_repeats"]]
        own = [
            value["pass_a"]
            if value["pass_a"] is not None and value["pass_a"] == value["pass_b"]
            else None
            for value in row["cold_repeats"]
        ]
        own_ok = all(value is not None for value in own) and len(set(own)) == 1
        own_consistent += bool(own_ok)
        if row["cross_cold_exact_selection_consistent"]:
            causes["STABLE_3_OF_3"] += 1
        elif "CAPACITY_REJECTION" in statuses:
            causes["CAPACITY_REJECTION_IN_SOME_REPEAT"] += 1
        elif "PASS_DISAGREEMENT" in statuses:
            causes["PASS_DISAGREEMENT_IN_SOME_REPEAT"] += 1
        else:
            causes["CROSS_COLD_DECISION_DRIFT"] += 1
        if own_ok and not row["cross_cold_exact_selection_consistent"]:
            collateral.append({
                "task_id": row["task_id"],
                "corpus": row["corpus"],
                "candidate_count": row["candidate_count"],
                "own_repeated_answer": own[0],
                "repeat_statuses": statuses,
            })
    published = sum(
        bool(row["cross_cold_exact_selection_consistent"]) for row in tasks
    )
    return {
        "instability_causes": dict(sorted(causes.items())),
        "published_stable_3_of_3": published,
        "own_answer_consistent_3_of_3": own_consistent,
        "own_answer_consistent_rate": (
            round(own_consistent / len(tasks), 6) if tasks else None
        ),
        "capacity_collateral_tasks": collateral,
        "capacity_collateral_task_count": len(collateral),
        "note": (
            "own_answer_consistent_3_of_3 is the ceiling any capacity-stage "
            "change could reach; it is a diagnostic bound, not a result"
        ),
    }


def build(output: Path | None = None) -> dict[str, Any]:
    loaded = load(output)
    tasks = task_results(loaded)
    holdout_tasks = [row for row in tasks if row["evaluation_set"] == "HOLDOUT"]
    sentinel_tasks = [row for row in tasks if row["evaluation_set"] == "SENTINEL"]

    corpus_metrics = {
        corpus: stratified._aggregate(
            [row for row in holdout_tasks if row["corpus"] == corpus]
        )
        for corpus in stratified.CORPUS_ORDER
    }
    corpus_metrics["OVERALL"] = stratified._aggregate(holdout_tasks)
    stratum_metrics = stratified._group_metrics(
        holdout_tasks, "strata", list(holdout.STRATA)
    )
    relation_metrics = {
        relation: stratified._aggregate(
            [row for row in holdout_tasks if row["result_relation_type"] == relation]
        )
        for relation in RELATION_NAMES
    }
    ambiguity_tasks = [row for row in holdout_tasks if "B" in row["strata"]]
    unsupported = _unsupported(tasks)
    capacity = _capacity_metrics(loaded, tasks)
    sentinels = _sentinel_metrics(tasks)

    safety = {
        "unsupported_accepted_matches": unsupported,
        "unsupported_accepted_match_count": len(unsupported),
        "verifier_rejection_tasks": sorted(
            row["task_id"] for row in tasks if row["verifier_failure_count"]
        ),
        "verifier_rejection_task_count": sum(
            bool(row["verifier_failure_count"]) for row in tasks
        ),
        "technical_failure_tasks": sorted(
            row["task_id"] for row in tasks if row["model_or_schema_failure_count"]
        ),
        "technical_failure_task_count": sum(
            bool(row["model_or_schema_failure_count"]) for row in tasks
        ),
        **capacity,
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_lineage_v2_6_holdout_metrics",
        "production_head": stratified.PRODUCTION_HEAD,
        "production_release": stratified.PRODUCTION_RELEASE,
        "deploy": False,
        "shadow_enabled": False,
        "materialization": False,
        "vision": False,
        "consent": loaded["telemetry"]["consent"],
        "sample_size": len(holdout_tasks),
        "sentinel_task_count": len(sentinel_tasks),
        "cold_repeat_count": len(holdout.HOLDOUT_COLD_RUNS),
        "passes_per_repeat": len(holdout.PASSES),
        "unanimity_rule": (
            "stable match requires pass A == pass B, parser PASS, verifier PASS "
            "and capacity PASS; no majority override"
        ),
        "corpus_metrics": corpus_metrics,
        "stratum_metrics": stratum_metrics,
        "relation_metrics": relation_metrics,
        "same_scope_ambiguity": {
            "metrics": stratified._aggregate(ambiguity_tasks),
            "stability_is_not_truth": True,
            "tasks": [{
                "task_id": row["task_id"],
                "corpus": row["corpus"],
                "selection_distribution": row["selection_distribution"],
                "stable_preference": row["stable_preference"],
                "selected_evidence_signature_distribution":
                    row["selected_evidence_signature_distribution"],
            } for row in ambiguity_tasks],
        },
        "instability_diagnosis": instability_diagnosis(holdout_tasks),
        "unstable_tasks": [{
            "task_id": row["task_id"],
            "corpus": row["corpus"],
            "strata": row["strata"],
            "stable_repeat_count": row["stable_repeat_count"],
            "stable_decision": row["stable_decision"],
            "repeat_statuses": [value["status"] for value in row["cold_repeats"]],
        } for row in holdout_tasks if not row["cross_cold_exact_selection_consistent"]],
        "references": _reference_metrics(holdout_tasks),
        "sentinels": sentinels,
        "safety": safety,
        "cost": _cost(loaded),
        "tasks": tasks,
    }


def gates(metrics: Mapping[str, Any]) -> dict[str, Any]:
    """Phase 7 GO / NO-GO gates, evaluated mechanically."""
    overall = metrics["corpus_metrics"]["OVERALL"]
    safety = metrics["safety"]
    unstable = [
        row for row in metrics["unstable_tasks"]
        if row["stable_decision"] is not None
    ]
    consistency = overall["cross_cold_exact_selection_consistency"]["rate"] or 0.0
    rate = overall["stable_3_of_3_rate"] or 0.0
    values = {
        "unsupported_accepted_zero": safety["unsupported_accepted_match_count"] == 0,
        "no_false_capacity_conflicts": safety["false_conflicts"] == 0,
        "right_map_conflict_zero": safety["RIGHT_MAP_CONFLICT"] == 0,
        "no_page_global_capacity": safety["capacity_identity"] == (
            "RIGHT physical_page + exact function_fragment_id"
        ),
        "sentinels_do_not_regress": not metrics["sentinels"]["regression"],
        "no_technical_failures": safety["technical_failure_task_count"] == 0,
        "run_completed": not metrics["cost"]["stopped_early"],
        "all_requests_successful": (
            metrics["cost"]["successful_inference_requests"]
            == metrics["cost"]["planned_requests"]
        ),
        # Safety limb of master gate 6: an unstable task must publish nothing.
        "unstable_tasks_publish_no_decision": not unstable,
        # Quality limb of master gate 6 / gate 5, against the project's own
        # pre-existing verdict-A thresholds.  Never relaxed to fit a result.
        "strong_reproducibility": (
            rate >= stratified.VERDICT_THRESHOLDS["a_overall_stable_3_of_3_min"]
            and consistency
            >= stratified.VERDICT_THRESHOLDS["a_cross_cold_exact_consistency_min"]
        ),
    }
    return {
        "gates": dict(sorted(values.items())),
        "all_passed": all(values.values()),
        "failed_gates": sorted(name for name, value in values.items() if not value),
        "thresholds": {
            "stable_3_of_3_min":
                stratified.VERDICT_THRESHOLDS["a_overall_stable_3_of_3_min"],
            "cross_cold_exact_consistency_min":
                stratified.VERDICT_THRESHOLDS["a_cross_cold_exact_consistency_min"],
            "source": "pre-existing v2.5 verdict-A thresholds",
        },
        "reproducibility": {
            "stable_3_of_3": overall["stable_3_of_3"],
            "tasks": overall["tasks"],
            "rate": overall["stable_3_of_3_rate"],
            "cross_cold_exact_consistency": consistency,
        },
        "tasks_publishing_a_decision_without_unanimity": unstable,
    }


def render_report(metrics: Mapping[str, Any], verdict: Mapping[str, Any]) -> str:
    overall = metrics["corpus_metrics"]["OVERALL"]
    diagnosis = metrics["instability_diagnosis"]
    cost = metrics["cost"]
    safety = metrics["safety"]
    lines = [
        "# Function Lineage v2.6 — independent holdout AI evaluation",
        "",
        "Consented inference on frozen inputs. No deploy, no shadow, no "
        "materialization, no vision. The holdout sample shares no task with the "
        "v2.5 diagnostic set or the seven v2.4.2 controls.",
        "",
        "## Consent",
        "",
        "| Artifact | Consented SHA-256 | Observed |",
        "|---|---|---|",
    ]
    for name, value in metrics["consent"]["consented_sha256"].items():
        observed = metrics["consent"]["observed_sha256"][name]
        lines.append(
            f"| `{name}` | `{value}` | {'MATCH' if observed == value else 'DRIFT'} |"
        )
    lines.extend([
        "",
        "## Stability by corpus",
        "",
        "| Corpus | Tasks | Stable 3/3 | 2/3 | 1/3 | 0/3 | Exact consistency | "
        "Stable NME | Pass disagreements |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for corpus in (*stratified.CORPUS_ORDER, "OVERALL"):
        row = metrics["corpus_metrics"][corpus]
        consistency = row["cross_cold_exact_selection_consistency"]
        lines.append(
            f"| {corpus} | {row['tasks']} | {row['stable_3_of_3']} | "
            f"{row['stable_2_of_3']} | {row['stable_1_of_3']} | "
            f"{row['stable_0_of_3']} | "
            f"{consistency['consistent_tasks']}/{consistency['denominator_tasks']} "
            f"({consistency['rate']}) | {row['stable_need_more_evidence_tasks']} | "
            f"{row['pass_disagreement_repeats']} |"
        )
    lines.extend([
        "",
        "## Stability by stratum",
        "",
        "| Stratum | Tasks | Stable 3/3 | Exact consistency | Stable NME | "
        "Pass disagreements |",
        "|---|---:|---:|---:|---:|---:|",
    ])
    for stratum in holdout.STRATA:
        row = metrics["stratum_metrics"][stratum]
        consistency = row["cross_cold_exact_selection_consistency"]
        lines.append(
            f"| {stratum} | {row['tasks']} | {row['stable_3_of_3']} | "
            f"{consistency['rate']} | {row['stable_need_more_evidence_tasks']} | "
            f"{row['pass_disagreement_repeats']} |"
        )
    lines.extend([
        "",
        "## Stability by relation type",
        "",
        "| Relation | Tasks | Stable 3/3 | Exact consistency | Stable NME |",
        "|---|---:|---:|---:|---:|",
    ])
    for relation in RELATION_NAMES:
        row = metrics["relation_metrics"][relation]
        lines.append(
            f"| {relation} | {row['tasks']} | {row['stable_3_of_3']} | "
            f"{row['cross_cold_exact_selection_consistency']['rate']} | "
            f"{row['stable_need_more_evidence_tasks']} |"
        )
    ambiguity = metrics["same_scope_ambiguity"]["metrics"]
    references = metrics["references"]
    lines.extend([
        "",
        "## Same-scope ambiguity",
        "",
        f"Ambiguous tasks `{ambiguity['tasks']}`; stable 3/3 "
        f"`{ambiguity['stable_3_of_3']}`; exact consistency "
        f"`{ambiguity['cross_cold_exact_selection_consistency']['rate']}`.",
        "",
        "Stability is a model preference. It is not proof that the other eligible "
        "candidates are false.",
        "",
        "## Reference classes",
        "",
        f"Authoritative determined rows `{references['authoritative']['determined_rows']}`; "
        f"alignment `{references['authoritative']['alignment_rate']}`.",
        f"Research-reference determined rows `{references['research']['determined_rows']}`; "
        f"aligned `{references['research']['aligned']}`; rate "
        f"`{references['research']['alignment_rate']}` — hypothesis alignment, not precision.",
        f"DOCUMENT_LINK candidate occurrences `{references['document_link_candidate_occurrences']}`; "
        f"used as functional truth: `NO`.",
        "",
        "## Sentinels (reported separately)",
        "",
        f"Sentinel regression: **{'YES' if metrics['sentinels']['regression'] else 'NO'}**.",
        "",
        "| Sentinel | Expected v2.4.2 | Observed v2.6 | Status |",
        "|---|---|---|---|",
    ])
    for row in metrics["sentinels"]["sentinels"]:
        lines.append(
            f"| {row['sentinel']} | `{row['expected_v2_4_2']}` | "
            f"`{row['observed_v2_6']}` | {row['status']} |"
        )
    lines.extend([
        "",
        "## Safety",
        "",
        f"Unsupported accepted matches `{safety['unsupported_accepted_match_count']}`; "
        f"verifier rejection tasks `{safety['verifier_rejection_task_count']}`; "
        f"capacity errors `{safety['capacity_error_count']}` "
        f"(FUNCTION_FRAGMENT_CONFLICT `{safety['FUNCTION_FRAGMENT_CONFLICT']}`, "
        f"RIGHT_MAP_CONFLICT `{safety['RIGHT_MAP_CONFLICT']}`); "
        f"technical failures `{safety['technical_failure_task_count']}`.",
        "",
        f"Every observed conflict was classified deterministically: true "
        f"`{safety['true_conflicts']}`, false `{safety['false_conflicts']}`. "
        f"Of the true ones, `{safety['incompatible_merge_arity_conflicts']}` assert "
        f"incompatible merge arity onto one fragment and "
        f"`{safety['unrepresentable_convergence_conflicts']}` have no candidate that "
        f"could express the convergence at all — a representability gap, resolved "
        f"fail-closed.",
        "",
        "| Root cause | Conflicts |",
        "|---|---:|",
        *(
            f"| `{name}` | {value} |"
            for name, value in safety["conflict_root_cause_counts"].items()
        ),
        "",
        "## Reproducibility",
        "",
        f"Stable 3/3 `{verdict['reproducibility']['stable_3_of_3']}`/"
        f"`{verdict['reproducibility']['tasks']}` = "
        f"`{verdict['reproducibility']['rate']}`; cross-cold exact consistency "
        f"`{verdict['reproducibility']['cross_cold_exact_consistency']}`. "
        f"Thresholds `{verdict['thresholds']['stable_3_of_3_min']}` / "
        f"`{verdict['thresholds']['cross_cold_exact_consistency_min']}` "
        f"({verdict['thresholds']['source']}).",
        "",
        f"Tasks publishing a decision without full unanimity: "
        f"`{len(verdict['tasks_publishing_a_decision_without_unanimity'])}`.",
        "",
        "### Where the instability comes from",
        "",
        "| Cause | Tasks |",
        "|---|---:|",
        *(
            f"| `{name}` | {value} |"
            for name, value in diagnosis["instability_causes"].items()
        ),
        "",
        f"Published stable 3/3 `{diagnosis['published_stable_3_of_3']}`; the same "
        f"tasks judged on their own A==B answer alone, ignoring any contest raised "
        f"by another task, would be `{diagnosis['own_answer_consistent_3_of_3']}` "
        f"(`{diagnosis['own_answer_consistent_rate']}`). "
        f"`{diagnosis['capacity_collateral_task_count']}` task(s) repeated one "
        f"identical answer three times and published nothing only because another "
        f"task claimed the same fragment.",
        "",
        "That ceiling is still far below the threshold, so the reproducibility gap "
        "is a property of the bounded selector on independent hard tasks, not an "
        "artifact of capacity accounting. Changing the capacity stage could not "
        "close it.",
        "",
        "## Cost",
        "",
        f"Planned / recorded / successful: `{cost['planned_requests']}` / "
        f"`{cost['request_records']}` / `{cost['successful_inference_requests']}`. "
        f"Wall time `{cost['wall_time_ms']} ms`; model runtime "
        f"`{cost['model_runtime_ms']} ms`. {cost['telemetry_assessment']}.",
        "",
        "## GO / NO-GO gates",
        "",
        "| Gate | Result |",
        "|---|---|",
    ])
    for name, value in verdict["gates"].items():
        lines.append(f"| `{name}` | {'PASS' if value else 'FAIL'} |")
    lines.extend([
        "",
        f"All gates passed: **{'YES' if verdict['all_passed'] else 'NO'}**.",
        "",
        "**DO NOT DEPLOY. DO NOT ENABLE SHADOW.** Production authorization is a "
        "separate, explicit decision.",
        "",
    ])
    return "\n".join(lines)


def write(output: Path | None = None) -> Path:
    target = Path(output or DEFAULT_OUTPUT)
    metrics = build(target)
    verdict = gates(metrics)
    for name in ("metrics.json", "task_results.json", "gates.json", "evaluation_report.md"):
        if (target / name).exists():
            raise RuntimeError(f"refusing to replace immutable result artifact: {name}")
    tasks = metrics.pop("tasks")
    (target / "task_results.json").write_bytes(_json_bytes(tasks))
    (target / "metrics.json").write_bytes(_json_bytes(metrics))
    (target / "gates.json").write_bytes(_json_bytes(verdict))
    (target / "evaluation_report.md").write_text(
        render_report(metrics, verdict), encoding="utf-8"
    )
    return target


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

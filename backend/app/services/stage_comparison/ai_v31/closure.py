"""Question-level closure contracts, routing, three-run gate and audit."""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping, Sequence

from ..ai_v3 import schemas as v3_schemas
from ..production_artifacts import content_signature
from . import schemas


def _objects(values: Any) -> list[Mapping[str, Any]]:
    return [value for value in values or () if isinstance(value, Mapping)]


def _question_rows(hro_plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for value in _objects(hro_plan.get("groups")):
        rows.append({
            "question_id": str(value.get("group_id") or ""),
            "question_type": str(value.get("decision_type") or "HUMAN_REVIEW_GROUP"),
            "title": value.get("title"),
            "question": value.get("question"),
            "affected_atomic_ids": sorted(map(str, value.get("affected_target_ids") or ())),
            "source_kind": "HumanReviewGroup",
            "source": value,
        })
    for value in _objects(hro_plan.get("standalone_questions")):
        rows.append({
            "question_id": str(value.get("question_id") or ""),
            "question_type": str(value.get("decision_type") or "HUMAN_QUESTION"),
            "title": value.get("title"),
            "question": value.get("question"),
            "affected_atomic_ids": sorted(map(str, value.get("affected_target_ids") or ())),
            "source_kind": "StandaloneHumanQuestion",
            "source": value,
        })
    return rows


def _task_index(factory: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        str(value.get("task_id") or ""): value
        for value in _objects(factory.get("tasks"))
    }


def _tasks_for_question(
    factory: Mapping[str, Any], question: Mapping[str, Any]
) -> list[Mapping[str, Any]]:
    question_id = str(question.get("question_id") or "")
    affected = set(map(str, question.get("affected_atomic_ids") or ()))
    output = []
    for task in _objects(factory.get("tasks")):
        task_affected = set(map(str, task.get("affected_target_ids") or ()))
        if task.get("human_question_id") == question_id or affected & task_affected:
            output.append(task)
    return output


def _candidate(task: Mapping[str, Any], candidate_id: str) -> Mapping[str, Any]:
    return next(
        (
            value for value in _objects(task.get("candidates"))
            if str(value.get("candidate_id") or "") == candidate_id
        ),
        {},
    )


def _audit_index(audit: Mapping[str, Any]) -> dict[tuple[str, str], str]:
    return {
        (
            str(value.get("task_id") or ""),
            str(value.get("selected_candidate_id") or ""),
        ): str(value.get("manual_verdict") or "")
        for value in _objects(audit.get("items"))
    }


def _stable_v3_core(
    runs: Sequence[Mapping[str, Any]], audits: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    if len(runs) != 3 or len(audits) != 3:
        raise ValueError("exactly three v3 runs and audits are required")
    per_run = []
    for run in runs:
        per_run.append({
            str(value.get("task_id") or ""): str(value.get("selected_candidate_id") or "")
            for value in _objects(run.get("stable_selections"))
            if value.get("status") == v3_schemas.VERIFIED_SELECTION
            and value.get("selected_candidate_id")
        })
    common_ids = set.intersection(*(set(value) for value in per_run))
    audit_indexes = [_audit_index(value) for value in audits]
    output = []
    for task_id in sorted(common_ids):
        candidate_ids = {value[task_id] for value in per_run}
        if len(candidate_ids) != 1:
            continue
        candidate_id = next(iter(candidate_ids))
        if not all(
            index.get((task_id, candidate_id)) == "SUPPORTED"
            for index in audit_indexes
        ):
            continue
        output.append({
            "task_id": task_id,
            "selected_candidate_id": candidate_id,
            "verified_all_runs": True,
            "manual_supported_all_runs": True,
        })
    return output


def _stable_human_candidate(
    task_id: str,
    runs: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    rows = []
    for run in runs:
        row = next(
            (
                value for value in _objects(run.get("stable_selections"))
                if str(value.get("task_id") or "") == task_id
            ),
            None,
        )
        if row is None or not row.get("selected_candidate_id"):
            return None
        pass_ids = list(map(str, row.get("pass_candidate_ids") or ()))
        if len(pass_ids) != 2 or len(set(pass_ids)) != 1:
            return None
        rows.append(row)
    candidate_ids = {str(value.get("selected_candidate_id") or "") for value in rows}
    if len(candidate_ids) != 1:
        return None
    return {
        "task_id": task_id,
        "selected_candidate_id": next(iter(candidate_ids)),
        "statuses": [value.get("status") for value in rows],
        "two_pass_unanimous_all_runs": True,
    }


def _node_id(ref: Any, side: str) -> str | None:
    prefix = f"{side}:NODE:"
    value = str(ref or "")
    return value[len(prefix):] if value.startswith(prefix) else None


def _unresolved_correspondence(
    direct_page: Mapping[str, Any],
) -> Mapping[str, Any]:
    result = direct_page.get("comparison_result") or {}
    for change in _objects(result.get("changes")):
        subject = change.get("subject") or {}
        if (
            isinstance(subject, Mapping)
            and subject.get("kind") == "unresolved_correspondence"
        ):
            return change
        if "соответств" in str(change.get("summary") or "").casefold():
            return change
    return {}


def _graph_decomposition(
    *,
    factory: Mapping[str, Any],
    stable_core: Sequence[Mapping[str, Any]],
    direct_page: Mapping[str, Any],
) -> dict[str, Any]:
    task_index = _task_index(factory)
    change = _unresolved_correspondence(direct_page)
    evidence = change.get("evidence") or {}
    left_source = set(map(str, ((evidence.get("left") or {}).get("node_ids") or ())))
    right_source = set(map(str, ((evidence.get("right") or {}).get("node_ids") or ())))
    relations = []
    covered_left: set[str] = set()
    covered_right: set[str] = set()
    diagnostics = []
    for stable in stable_core:
        task = task_index.get(str(stable.get("task_id") or "")) or {}
        if task.get("task_type") != v3_schemas.ENTITY_IDENTITY:
            continue
        candidate = _candidate(task, str(stable.get("selected_candidate_id") or ""))
        left_ids = sorted(filter(None, (_node_id(ref, "LEFT") for ref in candidate.get("left_refs") or ())))
        right_ids = sorted(filter(None, (_node_id(ref, "RIGHT") for ref in candidate.get("right_refs") or ())))
        direct_left = sorted(set(left_ids) & left_source)
        direct_right = sorted(set(right_ids) & right_source)
        record = {
            "task_id": stable.get("task_id"),
            "selected_candidate_id": stable.get("selected_candidate_id"),
            "subject": task.get("subject"),
            "left_node_ids": left_ids,
            "right_node_ids": right_ids,
            "covered_source_left_node_ids": direct_left,
            "covered_source_right_node_ids": direct_right,
            "engineering_significance": (
                "SOURCE_ENGINEERING_CORRESPONDENCE"
                if direct_left or direct_right
                else "DIAGNOSTIC_OR_AGGREGATE_RELATION"
            ),
        }
        relations.append(record)
        if direct_left or direct_right:
            covered_left.update(direct_left)
            covered_right.update(direct_right)
        else:
            diagnostics.append(record)
    missing_left = sorted(left_source - covered_left)
    missing_right = sorted(right_source - covered_right)
    blocked_tasks = [
        {
            "subproblem_id": f"graph_left:{node_id}",
            "side": "LEFT",
            "node_id": node_id,
            "engineering_significance": "ENGINEERING_SIGNIFICANT",
            "finite_candidates": [],
            "route_to_ai": False,
            "blocking_reason": "NO_BOUNDED_CORRESPONDENCE_CANDIDATES",
        }
        for node_id in missing_left
    ] + [
        {
            "subproblem_id": f"graph_right:{node_id}",
            "side": "RIGHT",
            "node_id": node_id,
            "engineering_significance": "ENGINEERING_SIGNIFICANT",
            "finite_candidates": [],
            "route_to_ai": False,
            "blocking_reason": "NO_BOUNDED_CORRESPONDENCE_CANDIDATES",
        }
        for node_id in missing_right
    ]
    return {
        "source_change_id": change.get("change_id"),
        "source_left_unresolved_node_ids": sorted(left_source),
        "source_right_unresolved_node_ids": sorted(right_source),
        "source_left_unresolved_count": len(left_source),
        "source_right_unresolved_count": len(right_source),
        "stable_core_relations": relations,
        "directly_covered_left_node_ids": sorted(covered_left),
        "directly_covered_right_node_ids": sorted(covered_right),
        "uncovered_left_node_ids": missing_left,
        "uncovered_right_node_ids": missing_right,
        "diagnostic_or_aggregate_stable_relations": diagnostics,
        "blocked_selector_subproblems": blocked_tasks,
        "fully_covered": not missing_left and not missing_right and bool(left_source or right_source),
    }


def _candidate_types(task: Mapping[str, Any]) -> set[str]:
    return {
        str(value.get("candidate_type") or "")
        for value in _objects(task.get("candidates"))
    }


def _candidate_id_for_type(task: Mapping[str, Any], candidate_type: str) -> str:
    return str(next(
        (
            value.get("candidate_id") for value in _objects(task.get("candidates"))
            if value.get("candidate_type") == candidate_type
        ),
        "",
    ))


def _base_contract(question: Mapping[str, Any]) -> dict[str, Any]:
    source = question.get("source") or {}
    evidence_refs = []
    for value in _objects(source.get("evidence_refs")):
        evidence_refs.append(
            str(value.get("evidence_ref") or value.get("target_id") or "")
        )
    return {
        "question_id": question["question_id"],
        "question_type": question["question_type"],
        "source_kind": question["source_kind"],
        "title": question.get("title"),
        "question": question.get("question"),
        "required_subproblems": [],
        "required_evidence": sorted(filter(None, evidence_refs)),
        "closure_conditions": [
            "EVERY_REQUIRED_SUBPROBLEM_RESOLVED",
            "EVERY_REQUIRED_EVIDENCE_ITEM_GROUNDED",
            "NO_BLOCKING_CONDITION_PRESENT",
            "HUMAN_DECISION_HAS_PRIORITY",
            "NO_AFFECTED_ATOMIC_ID_SILENTLY_REMOVED",
        ],
        "blocking_conditions": [],
        "affected_atomic_ids": question["affected_atomic_ids"],
        "current_status": schemas.OPEN,
    }


def analyze_question_closure(
    *,
    hro_plan: Mapping[str, Any],
    factory: Mapping[str, Any],
    v3_runs: Sequence[Mapping[str, Any]],
    v3_audits: Sequence[Mapping[str, Any]],
    direct_page: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Build all six contracts before routing any remaining semantic choice."""
    questions = _question_rows(hro_plan)
    expected = int((hro_plan.get("summary") or {}).get("mandatory_human_interactions") or 0)
    if len(questions) != expected:
        raise ValueError(
            f"HRO exact accounting failed: questions={len(questions)}, expected={expected}"
        )
    stable_core = _stable_v3_core(v3_runs, v3_audits)
    stable_ids = {str(value["task_id"]) for value in stable_core}
    graph = _graph_decomposition(
        factory=factory,
        stable_core=stable_core,
        direct_page=direct_page,
    )
    contracts = []
    analysis_rows = []
    routed_tasks = []
    blocked_subproblems = list(graph["blocked_selector_subproblems"])

    for question in questions:
        contract = _base_contract(question)
        related = _tasks_for_question(factory, question)
        qtype = question["question_type"]
        possible = False
        ai_needed = False
        reason = "No closure policy is defined for this question type."
        source = question.get("source") or {}
        human_answered = any(
            source.get(key) not in (None, "", {}, [])
            for key in ("human_decision", "resolved_answer", "selected_answer")
        )

        if human_answered:
            contract["blocking_conditions"] = ["HUMAN_DECISION_HAS_PRIORITY"]
            contract["current_status"] = schemas.BLOCKED_POLICY
            reason = "An existing human decision has priority and cannot be replaced by AI."
        elif qtype == "MODE_RELATION":
            contract["required_subproblems"] = ["AUTHORITATIVE_MODE_MAPPING"]
            contract["required_evidence"].append("HUMAN_AUTHORITY_FOR_MODE_MAPPING")
            contract["blocking_conditions"] = ["HUMAN_AUTHORITY_REQUIRED"]
            contract["current_status"] = schemas.BLOCKED_POLICY
            reason = "Mode mapping is reserved for human engineering authority."
        elif qtype == "RESERVE_LINE_INTERPRETATION":
            contract["required_subproblems"] = ["BOUNDED_LEFT_ABSENCE_OR_EXISTING_LINE_MATCH"]
            contract["required_evidence"].append("BOUNDED_LEFT_ABSENCE")
            contract["blocking_conditions"] = ["LEFT_ABSENCE_NOT_PROVEN"]
            contract["current_status"] = schemas.BLOCKED_MISSING_EVIDENCE
            reason = "The 0→2 claim lacks bounded proof of LEFT absence."
        elif qtype == "GRAPH_CORRESPONDENCE_CLARIFICATION":
            contract["required_subproblems"] = [
                f"graph_left:{value}"
                for value in graph["source_left_unresolved_node_ids"]
            ] + [
                f"graph_right:{value}"
                for value in graph["source_right_unresolved_node_ids"]
            ]
            contract["required_evidence"].extend(
                f"stable_v3:{value['task_id']}:{value['selected_candidate_id']}"
                for value in graph["stable_core_relations"]
            )
            if graph["fully_covered"]:
                contract["current_status"] = schemas.CLOSED_AI_STABLE
                possible = True
                reason = "Every source unresolved graph node is covered by stable v3 evidence."
            elif graph["directly_covered_left_node_ids"] or graph["directly_covered_right_node_ids"]:
                contract["current_status"] = schemas.PARTIALLY_RESOLVED
                contract["blocking_conditions"] = ["UNRESOLVED_ENGINEERING_NODES_REMAIN"]
                reason = (
                    "Stable v3 evidence covers only part of the exact source node inventory; "
                    "remaining nodes have no finite candidate set."
                )
            else:
                contract["blocking_conditions"] = ["UNRESOLVED_ENGINEERING_NODES_REMAIN"]
                reason = "No source unresolved graph node is completely resolved."
        elif qtype == "TABLE_ROW_IDENTITY":
            task = related[0] if related else {}
            stable = _stable_human_candidate(str(task.get("task_id") or ""), v3_runs)
            contract["required_subproblems"] = ["SELECT_EXACT_LEFT_ROW_FOR_RIGHT_VRU3"]
            contract["required_evidence"].append("UNIQUE_VRU3_ROW_IDENTITY")
            contract["blocking_conditions"] = ["TWO_LEFT_ROWS_REMAIN_PLAUSIBLE"]
            contract["current_status"] = schemas.BLOCKED_AMBIGUOUS_EVIDENCE
            reason = "Frozen evidence leaves two plausible LEFT rows; v3 correctly failed closed."
            if stable:
                contract["stable_fail_closed_candidate_id"] = stable["selected_candidate_id"]
        elif qtype == "TEXT_REQUIREMENT_EQUIVALENCE":
            task = related[0] if related else {}
            types = _candidate_types(task)
            texts = " ".join(
                str(value.get("question") or "")
                + " "
                + " ".join(map(str, (value.get("deterministic_features") or {}).get("left_texts") or ()))
                for value in _objects(task.get("candidates"))
            ).casefold()
            contract["required_subproblems"] = ["SEMANTIC_REQUIREMENT_RELATION"]
            if "мультиметр" in texts:
                stable = _stable_human_candidate(str(task.get("task_id") or ""), v3_runs)
                contract["required_evidence"].append("FULL_MEASUREMENT_FUNCTION_COVERAGE")
                contract["blocking_conditions"] = ["MEASUREMENT_FUNCTIONS_NOT_GROUNDED_ON_LEFT"]
                contract["current_status"] = schemas.BLOCKED_MISSING_EVIDENCE
                reason = (
                    "All existing v3 passes returned insufficient evidence; the LEFT label "
                    "does not prove the required measurement functions."
                )
                if stable:
                    contract["stable_fail_closed_candidate_id"] = stable["selected_candidate_id"]
            elif {"SAME_REQUIREMENT", "DIFFERENT_REQUIREMENT", "INSUFFICIENT_EVIDENCE"} <= types:
                possible = True
                ai_needed = True
                contract["required_evidence"].extend(sorted({
                    str(ref)
                    for candidate in _objects(task.get("candidates"))
                    for ref in candidate.get("text_refs") or ()
                }))
                contract["closure_conditions"].extend([
                    "PASS_1_EQUALS_PASS_2",
                    "VERIFIER_STATUS_IS_VERIFIED_SELECTION",
                    "SAME_CANDIDATE_IN_ALL_THREE_COLD_RUNS",
                    "MANUAL_CLOSURE_AUDIT_SAFE",
                ])
                reason = "A bounded N/PE semantic choice exists and needs the strict closure gate."
                routed_tasks.append({
                    "question_id": question["question_id"],
                    "task_id": task.get("task_id"),
                    "task_type": task.get("task_type"),
                    "route_to_ai": True,
                    "reason": "REMAINING_BOUNDED_HRO_CLOSURE_BLOCKER",
                    "allowed_candidate_types": [
                        "DIFFERENT_REQUIREMENT",
                        "INSUFFICIENT_EVIDENCE",
                        "SAME_REQUIREMENT",
                    ],
                    "candidate_ids": {
                        candidate_type: _candidate_id_for_type(task, candidate_type)
                        for candidate_type in (
                            "DIFFERENT_REQUIREMENT",
                            "INSUFFICIENT_EVIDENCE",
                            "SAME_REQUIREMENT",
                        )
                    },
                })
            else:
                contract["blocking_conditions"] = ["NO_FINITE_SEMANTIC_CANDIDATES"]
                contract["current_status"] = schemas.BLOCKED_MISSING_EVIDENCE
                reason = "No complete bounded candidate set exists."
        else:
            contract["blocking_conditions"] = ["UNSUPPORTED_CLOSURE_POLICY"]
            contract["current_status"] = schemas.BLOCKED_POLICY

        contracts.append(contract)
        analysis_rows.append({
            "question_id": question["question_id"],
            "question": question.get("title") or question.get("question"),
            "question_type": qtype,
            "closure_possible_with_current_evidence": possible,
            "reason": reason,
            "ai_needed": ai_needed,
            "current_status": contract["current_status"],
        })

    if stable_ids & {str(value.get("task_id") or "") for value in routed_tasks}:
        raise ValueError("stable v3 core must never be routed again")
    closed = [value for value in contracts if value["current_status"] in schemas.CLOSED_STATUSES]
    contracts_artifact = {
        "kind": "stage_comparison_question_closure_contracts",
        "schema_version": schemas.CONTRACT_SCHEMA_VERSION,
        "pair_id": hro_plan.get("pair_id"),
        "baseline_hro": expected,
        "contracts": contracts,
        "summary": {
            "contracts": len(contracts),
            "closed": len(closed),
            "remaining_hro": expected - len(closed),
            "exact_accounting": len(contracts) == expected,
        },
    }
    analysis_artifact = {
        "kind": "stage_comparison_question_closure_analysis",
        "schema_version": schemas.ANALYSIS_SCHEMA_VERSION,
        "pair_id": hro_plan.get("pair_id"),
        "questions": analysis_rows,
        "graph_correspondence": graph,
        "stable_v3_core": stable_core,
        "summary": {
            "closure_possible": sum(value["closure_possible_with_current_evidence"] for value in analysis_rows),
            "ai_needed": sum(value["ai_needed"] for value in analysis_rows),
            "stable_core_reused": len(stable_core),
            "stable_core_reselected": 0,
        },
    }
    tasks_artifact = {
        "kind": "stage_comparison_question_closure_ai_tasks",
        "schema_version": schemas.TASKS_SCHEMA_VERSION,
        "pair_id": hro_plan.get("pair_id"),
        "tasks": routed_tasks,
        "blocked_subproblems": blocked_subproblems,
        "reused_stable_core_task_ids": sorted(stable_ids),
        "constraints": {
            "stable_core_reselected": False,
            "missing_evidence_routed": False,
            "policy_questions_routed": False,
            "ambiguous_evidence_routed": False,
        },
    }
    return contracts_artifact, analysis_artifact, tasks_artifact


def materialize_closure_run(
    *,
    run_number: int,
    contracts_artifact: Mapping[str, Any],
    tasks_artifact: Mapping[str, Any],
    selector_run: Mapping[str, Any],
) -> dict[str, Any]:
    """Project two-pass verified selections to provisional closed questions."""
    contracts = deepcopy(list(contracts_artifact.get("contracts") or ()))
    question_by_task = {
        str(value.get("task_id") or ""): str(value.get("question_id") or "")
        for value in _objects(tasks_artifact.get("tasks"))
        if value.get("route_to_ai")
    }
    results = []
    for selection in _objects(selector_run.get("stable_selections")):
        task_id = str(selection.get("task_id") or "")
        question_id = question_by_task.get(task_id)
        if not question_id:
            continue
        pass_ids = list(map(str, selection.get("pass_candidate_ids") or ()))
        verified = selection.get("status") == v3_schemas.VERIFIED_SELECTION
        unanimous = len(pass_ids) == 2 and len(set(pass_ids)) == 1
        closed = verified and unanimous
        for contract in contracts:
            if contract.get("question_id") == question_id:
                contract["current_status"] = (
                    schemas.CLOSED_AI_STABLE if closed else schemas.OPEN
                )
                contract["run_selected_candidate_id"] = selection.get("selected_candidate_id")
        results.append({
            "question_id": question_id,
            "task_id": task_id,
            "pass_candidate_ids": pass_ids,
            "selected_candidate_id": selection.get("selected_candidate_id"),
            "verifier_status": selection.get("status"),
            "two_pass_unanimous": unanimous,
            "provisionally_closed": closed,
        })
    closed_ids = sorted(
        str(value.get("question_id") or "")
        for value in contracts if value.get("current_status") in schemas.CLOSED_STATUSES
    )
    baseline = int(contracts_artifact.get("baseline_hro") or len(contracts))
    diagnostics = selector_run.get("diagnostics") or {}
    return {
        "kind": "stage_comparison_question_closure_run",
        "schema_version": schemas.RUN_SCHEMA_VERSION,
        "run_number": run_number,
        "pair_id": contracts_artifact.get("pair_id"),
        "question_results": results,
        "provisional_closed_question_ids": closed_ids,
        "remaining_question_ids": sorted(
            str(value.get("question_id") or "")
            for value in contracts if str(value.get("question_id") or "") not in set(closed_ids)
        ),
        "contracts": contracts,
        "hro_before": baseline,
        "hro_after": baseline - len(closed_ids),
        "model_calls": int(diagnostics.get("model_calls") or 0),
        "runtime_ms": int(diagnostics.get("duration_ms") or 0),
        "selector_run": dict(selector_run),
        "constraints": {
            "exact_question_accounting": len(contracts) == baseline,
            "partial_resolution_does_not_close": all(
                value.get("question_id") not in closed_ids
                for value in contracts if value.get("current_status") == schemas.PARTIALLY_RESOLVED
            ),
            "no_question_silently_removed": len(closed_ids) + len(contracts) - len(closed_ids) == baseline,
        },
    }


def _technical_gate(run_artifacts: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if len(run_artifacts) != 3:
        raise ValueError("exactly three closure runs are required")
    closed_sets = [
        set(map(str, value.get("provisional_closed_question_ids") or ()))
        for value in run_artifacts
    ]
    identical_sets = len({tuple(sorted(value)) for value in closed_sets}) == 1
    common = set.intersection(*closed_sets) if closed_sets else set()
    problems = []
    if not identical_sets:
        problems.append("CLOSED_QUESTION_SET_DIFFERS")
    candidates: dict[str, set[str]] = {question_id: set() for question_id in common}
    all_verified = True
    all_two_pass = True
    for run in run_artifacts:
        for result in _objects(run.get("question_results")):
            question_id = str(result.get("question_id") or "")
            if question_id not in common:
                continue
            candidates[question_id].add(str(result.get("selected_candidate_id") or ""))
            all_verified = all_verified and result.get("verifier_status") == v3_schemas.VERIFIED_SELECTION
            all_two_pass = all_two_pass and result.get("two_pass_unanimous") is True
    candidate_stable = all(len(value) == 1 and "" not in value for value in candidates.values())
    if not candidate_stable:
        problems.append("CANDIDATE_DIFFERS_ACROSS_RUNS")
    if not all_verified:
        problems.append("SELECTION_NOT_VERIFIED_IN_EVERY_RUN")
    if not all_two_pass:
        problems.append("TWO_PASS_UNANIMITY_FAILED")
    stable = sorted(common) if not problems else []
    return {
        "closed_question_set_identical": identical_sets,
        "candidate_stable_across_runs": candidate_stable,
        "verified_all_runs": all_verified,
        "two_pass_unanimity_all_runs": all_two_pass,
        "candidate_ids": {
            key: sorted(value) for key, value in sorted(candidates.items())
        },
        "technical_stable_closed_question_ids": stable,
        "problems": problems,
    }


def build_pending_manual_audit(
    *,
    contracts_artifact: Mapping[str, Any],
    run_artifacts: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    technical = _technical_gate(run_artifacts)
    by_id = {
        str(value.get("question_id") or ""): value
        for value in _objects(contracts_artifact.get("contracts"))
    }
    items = []
    for question_id in technical["technical_stable_closed_question_ids"]:
        contract = by_id[question_id]
        items.append({
            "question_id": question_id,
            "selected_candidate_id": technical["candidate_ids"][question_id][0],
            "closure_conditions": [
                {"condition": condition, "checked": False, "note": ""}
                for condition in contract.get("closure_conditions") or ()
            ],
            "manual_verdict": schemas.PENDING,
            "note": "",
        })
    return {
        "kind": "stage_comparison_question_closure_manual_audit",
        "schema_version": schemas.AUDIT_SCHEMA_VERSION,
        "status": "PENDING" if items else "COMPLETE",
        "items": items,
    }


def evaluate_closure_gate(
    *,
    contracts_artifact: Mapping[str, Any],
    run_artifacts: Sequence[Mapping[str, Any]],
    manual_audit: Mapping[str, Any],
) -> dict[str, Any]:
    technical = _technical_gate(run_artifacts)
    expected = set(technical["technical_stable_closed_question_ids"])
    audit_items = {
        str(value.get("question_id") or ""): value
        for value in _objects(manual_audit.get("items"))
    }
    safe_ids = set()
    unsafe_ids = set()
    audit_problems = []
    for question_id in sorted(expected):
        item = audit_items.get(question_id)
        if item is None:
            audit_problems.append(f"MANUAL_AUDIT_MISSING:{question_id}")
            continue
        conditions = _objects(item.get("closure_conditions"))
        all_checked = bool(conditions) and all(value.get("checked") is True for value in conditions)
        if item.get("manual_verdict") == schemas.SAFE_TO_CLOSE and all_checked:
            safe_ids.add(question_id)
        else:
            unsafe_ids.add(question_id)
            audit_problems.append(f"UNSAFE_OR_INCOMPLETE_MANUAL_AUDIT:{question_id}")
    extra_audits = set(audit_items) - expected
    if extra_audits:
        audit_problems.append("MANUAL_AUDIT_HAS_EXTRA_QUESTIONS")
    problems = list(technical["problems"]) + audit_problems
    stable_closed = sorted(expected & safe_ids) if not problems else []
    total_calls = sum(int(value.get("model_calls") or 0) for value in run_artifacts)
    total_runtime = sum(int(value.get("runtime_ms") or 0) for value in run_artifacts)
    baseline = int(contracts_artifact.get("baseline_hro") or 0)
    if unsafe_ids:
        verdict = "C"
    elif stable_closed and not problems:
        verdict = "A"
    else:
        verdict = "B"
    return {
        "kind": "stage_comparison_question_closure_gate",
        "schema_version": schemas.GATE_SCHEMA_VERSION,
        "pair_id": contracts_artifact.get("pair_id"),
        "verdict": verdict,
        "recommend_rollout": verdict == "A",
        "problems": problems,
        "closed_question_set_identical": technical["closed_question_set_identical"],
        "candidate_stable_across_runs": technical["candidate_stable_across_runs"],
        "candidate_ids": technical["candidate_ids"],
        "stable_closed_question_ids": stable_closed,
        "manual_safe_question_ids": sorted(safe_ids),
        "manual_unsafe_question_ids": sorted(unsafe_ids),
        "unsupported_closures": len(unsafe_ids),
        "hro_before": baseline,
        "hro_runs": [int(value.get("hro_after") or 0) for value in run_artifacts],
        "stable_hro_after": baseline - len(stable_closed),
        "total_model_calls": total_calls,
        "total_runtime_ms": total_runtime,
        "seconds_per_closed_hro_question": (
            round(total_runtime / 1000 / len(stable_closed), 3)
            if stable_closed else None
        ),
        "product_value": bool(stable_closed),
        "constraints": {
            "all_six_contracts_accounted": len(contracts_artifact.get("contracts") or ()) == baseline,
            "manual_audit_required": True,
            "human_priority": True,
            "production_artifacts_mutated": False,
        },
    }


def apply_closure_gate(
    *, contracts_artifact: Mapping[str, Any], gate: Mapping[str, Any]
) -> dict[str, Any]:
    """Update contract current_status only after the complete cross-run gate."""
    value = deepcopy(dict(contracts_artifact))
    stable = set(map(str, gate.get("stable_closed_question_ids") or ()))
    for contract in value.get("contracts") or ():
        if str(contract.get("question_id") or "") in stable:
            contract["current_status"] = schemas.CLOSED_AI_STABLE
    baseline = int(value.get("baseline_hro") or 0)
    closed = sum(
        contract.get("current_status") in schemas.CLOSED_STATUSES
        for contract in value.get("contracts") or ()
    )
    value["summary"] = {
        "contracts": len(value.get("contracts") or ()),
        "closed": closed,
        "remaining_hro": baseline - closed,
        "exact_accounting": len(value.get("contracts") or ()) == baseline,
    }
    value["closure_gate_signature"] = content_signature(gate)
    return value


__all__ = [
    "analyze_question_closure",
    "apply_closure_gate",
    "build_pending_manual_audit",
    "evaluate_closure_gate",
    "materialize_closure_run",
]

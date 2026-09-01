from __future__ import annotations

from copy import deepcopy

from backend.app.services.stage_comparison.ai_v31 import (
    analyze_question_closure,
    apply_closure_gate,
    build_pending_manual_audit,
    evaluate_closure_gate,
    materialize_closure_run,
)
from backend.app.services.stage_comparison.ai_v31 import schemas
from backend.app.services.stage_comparison.ai_v31 import settings as production_settings
from backend.app.services.stage_comparison.ai_v31.production import (
    build_production_contracts,
)


MODE_ID = "mode-question"
RESERVE_ID = "reserve-question"
GRAPH_ID = "graph-question"
MEASUREMENT_ID = "measurement-question"
NPE_ID = "npe-question"
VRU_ID = "vru-question"
NPE_TASK_ID = "npe-task"


def _hro() -> dict:
    def question(question_id: str, decision_type: str, target: str, title: str) -> dict:
        return {
            "question_id": question_id,
            "decision_type": decision_type,
            "title": title,
            "question": title,
            "affected_target_ids": [target],
            "evidence_refs": [{"evidence_ref": f"evidence:{target}"}],
        }

    return {
        "pair_id": "pair",
        "summary": {"mandatory_human_interactions": 6},
        "groups": [{
            "group_id": MODE_ID,
            "decision_type": "MODE_RELATION",
            "title": "Modes",
            "question": "Modes?",
            "affected_target_ids": ["mode-target"],
            "evidence_refs": [{"target_id": "mode-evidence"}],
        }],
        "standalone_questions": [
            question(RESERVE_ID, "RESERVE_LINE_INTERPRETATION", "reserve-target", "Reserve"),
            question(GRAPH_ID, "GRAPH_CORRESPONDENCE_CLARIFICATION", "graph-target", "Graph"),
            question(MEASUREMENT_ID, "TEXT_REQUIREMENT_EQUIVALENCE", "measurement-task", "Measurement"),
            question(NPE_ID, "TEXT_REQUIREMENT_EQUIVALENCE", NPE_TASK_ID, "N/PE"),
            question(VRU_ID, "TABLE_ROW_IDENTITY", "vru-task", "VRU3"),
        ],
    }


def _candidate(
    candidate_id: str,
    task_id: str,
    candidate_type: str,
    *,
    left_refs: list[str] | None = None,
    right_refs: list[str] | None = None,
    left_texts: list[str] | None = None,
    right_texts: list[str] | None = None,
    effect: str = "RESOLVE_HUMAN_QUESTION",
    eligibility: str = "ELIGIBLE_FOR_AUTO_RESOLUTION",
) -> dict:
    refs = list(left_refs or ()) + list(right_refs or ())
    return {
        "candidate_id": candidate_id,
        "task_id": task_id,
        "candidate_type": candidate_type,
        "left_refs": left_refs or [],
        "right_refs": right_refs or [],
        "entity_refs": refs,
        "graph_refs": [],
        "table_refs": [],
        "text_refs": refs,
        "deterministic_features": {
            "left_texts": left_texts or [],
            "right_texts": right_texts or [],
        },
        "proof_requirements": [],
        "eligibility": eligibility,
        "resolution_effect": effect,
        "materialization": {},
        "candidate_signature": candidate_id + "-signature",
    }


def _task(
    task_id: str,
    task_type: str,
    question_id: str | None,
    candidates: list[dict],
    *,
    subject: str | None = None,
) -> dict:
    return {
        "task_id": task_id,
        "task_type": task_type,
        "human_question_id": question_id,
        "affected_target_ids": [task_id],
        "subject": subject,
        "candidates": candidates,
        "selectable_candidate_ids": [
            value["candidate_id"] for value in candidates
            if value["eligibility"] == "ELIGIBLE_FOR_AUTO_RESOLUTION"
        ],
    }


def _factory() -> dict:
    graph = _task(
        "graph-stable",
        "ENTITY_IDENTITY",
        None,
        [_candidate(
            "graph-candidate",
            "graph-stable",
            "ENTITY_PAIR",
            left_refs=["LEFT:NODE:L1"],
            right_refs=["RIGHT:NODE:R1"],
        )],
        subject="L1",
    )
    diagnostic = _task(
        "graph-diagnostic",
        "ENTITY_IDENTITY",
        None,
        [_candidate(
            "diagnostic-candidate",
            "graph-diagnostic",
            "ENTITY_PAIR",
            left_refs=["LEFT:NODE:COMPENSATION_GROUP:BUS1"],
            right_refs=["RIGHT:NODE:COMPENSATION_GROUP:BUS1"],
        )],
        subject="COMPENSATION_GROUP:BUS1",
    )
    measurement = _task(
        "measurement-task",
        "TEXT_EQUIVALENCE",
        MEASUREMENT_ID,
        [
            _candidate(
                "measurement-insufficient",
                "measurement-task",
                "INSUFFICIENT_EVIDENCE",
                left_texts=["Мультиметр"],
                effect="HUMAN_REQUIRED",
            ),
            _candidate(
                "measurement-same",
                "measurement-task",
                "SAME_REQUIREMENT",
                left_texts=["Мультиметр"],
            ),
        ],
    )
    npe = _task(
        NPE_TASK_ID,
        "TEXT_EQUIVALENCE",
        NPE_ID,
        [
            _candidate(
                "npe-different", NPE_TASK_ID, "DIFFERENT_REQUIREMENT",
                left_refs=["LEFT:TEXT:pe"], right_refs=["RIGHT:TEXT:npe"],
                left_texts=["К РЕ-шине ГРЩ2"],
                right_texts=["В панелях предусмотреть шины N и РЕ"],
            ),
            _candidate(
                "npe-insufficient", NPE_TASK_ID, "INSUFFICIENT_EVIDENCE",
                effect="HUMAN_REQUIRED",
            ),
            _candidate(
                "npe-same", NPE_TASK_ID, "SAME_REQUIREMENT",
                left_refs=["LEFT:TEXT:pe"], right_refs=["RIGHT:TEXT:npe"],
            ),
            _candidate(
                "npe-changed", NPE_TASK_ID, "REQUIREMENT_CHANGED",
                left_refs=["LEFT:TEXT:pe"], right_refs=["RIGHT:TEXT:npe"],
            ),
        ],
    )
    npe["source_kind"] = "TEXT_REQUIREMENT_EQUIVALENCE"
    reserve = _task(
        "reserve-task",
        "CHANGE_INTERPRETATION",
        RESERVE_ID,
        [_candidate(
            "reserve-added",
            "reserve-task",
            "SUPPORTED_CHANGE_0_TO_2",
            eligibility="INVALID",
        )],
    )
    vru = _task(
        "vru-task",
        "TABLE_ROW_IDENTITY",
        VRU_ID,
        [_candidate(
            "vru-insufficient",
            "vru-task",
            "INSUFFICIENT_EVIDENCE",
            effect="HUMAN_REQUIRED",
        )],
    )
    return {
        "candidate_set_signature": "factory-signature",
        "tasks": [graph, diagnostic, measurement, npe, reserve, vru],
    }


def _v3_runs() -> tuple[list[dict], list[dict]]:
    stable = [
        {
            "task_id": "graph-stable",
            "status": "VERIFIED_SELECTION",
            "selected_candidate_id": "graph-candidate",
            "pass_candidate_ids": ["graph-candidate", "graph-candidate"],
        },
        {
            "task_id": "graph-diagnostic",
            "status": "VERIFIED_SELECTION",
            "selected_candidate_id": "diagnostic-candidate",
            "pass_candidate_ids": ["diagnostic-candidate", "diagnostic-candidate"],
        },
        {
            "task_id": "measurement-task",
            "status": "HUMAN_REQUIRED",
            "selected_candidate_id": "measurement-insufficient",
            "pass_candidate_ids": ["measurement-insufficient", "measurement-insufficient"],
        },
        {
            "task_id": "vru-task",
            "status": "HUMAN_REQUIRED",
            "selected_candidate_id": "vru-insufficient",
            "pass_candidate_ids": ["vru-insufficient", "vru-insufficient"],
        },
    ]
    runs = [{"stable_selections": deepcopy(stable)} for _ in range(3)]
    audits = [{
        "status": "COMPLETE",
        "items": [
            {
                "task_id": "graph-stable",
                "selected_candidate_id": "graph-candidate",
                "manual_verdict": "SUPPORTED",
            },
            {
                "task_id": "graph-diagnostic",
                "selected_candidate_id": "diagnostic-candidate",
                "manual_verdict": "SUPPORTED",
            },
        ],
    } for _ in range(3)]
    return runs, audits


def _direct_page() -> dict:
    def node(node_id: str, node_type: str = "OUTGOING_DEVICE") -> dict:
        return {"node_id": node_id, "node_type": node_type}

    return {
        "comparison_result": {
            "changes": [{
                "change_id": "unresolved-change",
                "type": "UNCERTAIN_STRUCTURAL_CHANGE",
                "subject": {"kind": "unresolved_correspondence"},
                "summary": "Correspondence unresolved",
                "evidence": {
                    "left": {"node_ids": ["L1", "L2"], "nodes": [node("L1"), node("L2")]},
                    "right": {"node_ids": ["R1", "R2"], "nodes": [node("R1"), node("R2")]},
                },
            }],
        },
    }


def _analysis() -> tuple[dict, dict, dict]:
    runs, audits = _v3_runs()
    return analyze_question_closure(
        hro_plan=_hro(),
        factory=_factory(),
        v3_runs=runs,
        v3_audits=audits,
        direct_page=_direct_page(),
    )


def _selector_run(candidate_id: str = "npe-different", status: str = "VERIFIED_SELECTION") -> dict:
    return {
        "stable_selections": [{
            "task_id": NPE_TASK_ID,
            "status": status,
            "selected_candidate_id": candidate_id,
            "pass_candidate_ids": [candidate_id, candidate_id],
        }],
        "diagnostics": {"model_calls": 2, "duration_ms": 1000},
    }


def _closure_runs(candidate_ids: tuple[str, str, str] = ("npe-different",) * 3) -> tuple[dict, list[dict]]:
    contracts, _analysis_artifact, tasks = _analysis()
    runs = [
        materialize_closure_run(
            run_number=index,
            contracts_artifact=contracts,
            tasks_artifact=tasks,
            selector_run=_selector_run(candidate_id),
        )
        for index, candidate_id in enumerate(candidate_ids, 1)
    ]
    return contracts, runs


def test_question_closure_exact_accounting_and_statuses() -> None:
    contracts, analysis, tasks = _analysis()
    by_id = {value["question_id"]: value for value in contracts["contracts"]}
    assert contracts["summary"] == {
        "contracts": 6,
        "closed": 0,
        "remaining_hro": 6,
        "exact_accounting": True,
    }
    assert by_id[GRAPH_ID]["current_status"] == schemas.PARTIALLY_RESOLVED
    assert by_id[RESERVE_ID]["current_status"] == schemas.BLOCKED_MISSING_EVIDENCE
    assert by_id[MEASUREMENT_ID]["current_status"] == schemas.BLOCKED_MISSING_EVIDENCE
    assert by_id[VRU_ID]["current_status"] == schemas.BLOCKED_AMBIGUOUS_EVIDENCE
    assert by_id[MODE_ID]["current_status"] == schemas.BLOCKED_POLICY
    assert by_id[NPE_ID]["current_status"] == schemas.OPEN
    assert analysis["summary"]["closure_possible"] == 1
    assert analysis["summary"]["ai_needed"] == 1
    assert [value["task_id"] for value in tasks["tasks"]] == [NPE_TASK_ID]


def test_partial_graph_resolution_does_not_close_question() -> None:
    contracts, analysis, _tasks = _analysis()
    graph = analysis["graph_correspondence"]
    assert graph["directly_covered_left_node_ids"] == ["L1"]
    assert graph["directly_covered_right_node_ids"] == ["R1"]
    assert graph["uncovered_left_node_ids"] == ["L2"]
    assert graph["uncovered_right_node_ids"] == ["R2"]
    assert graph["fully_covered"] is False
    graph_contract = next(
        value for value in contracts["contracts"] if value["question_id"] == GRAPH_ID
    )
    assert graph_contract["current_status"] == schemas.PARTIALLY_RESOLVED
    assert GRAPH_ID not in {
        value["question_id"] for value in contracts["contracts"]
        if value["current_status"] in schemas.CLOSED_STATUSES
    }


def test_stable_core_is_reused_and_diagnostics_do_not_count_as_source_coverage() -> None:
    _contracts, analysis, tasks = _analysis()
    assert set(tasks["reused_stable_core_task_ids"]) == {
        "graph-stable", "graph-diagnostic"
    }
    assert not (
        set(tasks["reused_stable_core_task_ids"])
        & {value["task_id"] for value in tasks["tasks"]}
    )
    diagnostics = analysis["graph_correspondence"]["diagnostic_or_aggregate_stable_relations"]
    assert [value["task_id"] for value in diagnostics] == ["graph-diagnostic"]


def test_npe_uses_minimal_candidate_set_and_strict_two_pass_gate() -> None:
    contracts, _analysis_artifact, tasks = _analysis()
    route = tasks["tasks"][0]
    assert route["allowed_candidate_types"] == [
        "DIFFERENT_REQUIREMENT", "INSUFFICIENT_EVIDENCE", "SAME_REQUIREMENT"
    ]
    closed = materialize_closure_run(
        run_number=1,
        contracts_artifact=contracts,
        tasks_artifact=tasks,
        selector_run=_selector_run(),
    )
    assert closed["provisional_closed_question_ids"] == [NPE_ID]
    failed_selector = _selector_run()
    failed_selector["stable_selections"][0]["pass_candidate_ids"] = [
        "npe-different", "npe-same"
    ]
    open_run = materialize_closure_run(
        run_number=1,
        contracts_artifact=contracts,
        tasks_artifact=tasks,
        selector_run=failed_selector,
    )
    assert open_run["provisional_closed_question_ids"] == []
    assert open_run["hro_after"] == 6

    insufficient = materialize_closure_run(
        run_number=1,
        contracts_artifact=contracts,
        tasks_artifact=tasks,
        selector_run=_selector_run(
            candidate_id="npe-insufficient", status="HUMAN_REQUIRED"
        ),
    )
    assert insufficient["provisional_closed_question_ids"] == []
    assert insufficient["question_results"][0]["auto_closing_candidate"] is False


def test_production_policy_routes_evidence_shape_not_acceptance_ids() -> None:
    contracts, tasks = build_production_contracts(
        hro_plan=_hro(),
        factory=_factory(),
        human_decisions={"standalone_answers": [], "closure_overrides": []},
    )
    assert contracts["summary"] == {
        "contracts": 6,
        "eligible": 1,
        "closed": 0,
        "remaining_hro": 6,
        "exact_accounting": True,
    }
    assert [value["question_id"] for value in tasks["tasks"]] == [NPE_ID]
    assert tasks["constraints"]["general_v3_tasks_routed"] is False


def test_production_policy_human_override_blocks_reclosure() -> None:
    contracts, tasks = build_production_contracts(
        hro_plan=_hro(),
        factory=_factory(),
        human_decisions={
            "closure_overrides": [{
                "question_id": NPE_ID,
                "action": "REOPEN_FOR_HUMAN",
            }],
        },
    )
    contract = next(
        value for value in contracts["contracts"]
        if value["question_id"] == NPE_ID
    )
    assert contract["current_status"] == schemas.BLOCKED_POLICY
    assert contract["blocking_conditions"] == ["HUMAN_DECISION_HAS_PRIORITY"]
    assert tasks["tasks"] == []


def test_question_closure_feature_flag_is_independent(monkeypatch) -> None:
    monkeypatch.setenv("STAGE_COMPARISON_AI_ANALYST_V2", "false")
    monkeypatch.setenv("STAGE_COMPARISON_AI_ANALYST_V3", "false")
    monkeypatch.setenv(production_settings.FEATURE_FLAG, "true")
    assert production_settings.enabled() is True


def test_missing_policy_and_ambiguous_evidence_never_route_to_ai() -> None:
    contracts, analysis, tasks = _analysis()
    routed = {value["question_id"] for value in tasks["tasks"]}
    assert routed == {NPE_ID}
    by_id = {value["question_id"]: value for value in analysis["questions"]}
    for question_id in (RESERVE_ID, MEASUREMENT_ID, VRU_ID, MODE_ID, GRAPH_ID):
        assert by_id[question_id]["ai_needed"] is False
    assert tasks["constraints"] == {
        "stable_core_reselected": False,
        "missing_evidence_routed": False,
        "policy_questions_routed": False,
        "ambiguous_evidence_routed": False,
    }
    assert len(contracts["contracts"]) == 6


def test_identical_closed_set_and_same_candidate_are_both_required() -> None:
    contracts, runs = _closure_runs(("npe-different", "npe-same", "npe-different"))
    audit = build_pending_manual_audit(contracts_artifact=contracts, run_artifacts=runs)
    gate = evaluate_closure_gate(
        contracts_artifact=contracts,
        run_artifacts=runs,
        manual_audit=audit,
    )
    assert gate["closed_question_set_identical"] is True
    assert gate["candidate_stable_across_runs"] is False
    assert gate["stable_closed_question_ids"] == []
    assert gate["verdict"] == "B"


def test_manual_closure_audit_is_mandatory_and_updates_contract_only_when_safe() -> None:
    contracts, runs = _closure_runs()
    audit = build_pending_manual_audit(contracts_artifact=contracts, run_artifacts=runs)
    pending = evaluate_closure_gate(
        contracts_artifact=contracts,
        run_artifacts=runs,
        manual_audit=audit,
    )
    assert pending["stable_closed_question_ids"] == []
    assert pending["verdict"] == "C"
    audit["status"] = "COMPLETE"
    audit["items"][0]["manual_verdict"] = schemas.SAFE_TO_CLOSE
    for condition in audit["items"][0]["closure_conditions"]:
        condition["checked"] = True
    gate = evaluate_closure_gate(
        contracts_artifact=contracts,
        run_artifacts=runs,
        manual_audit=audit,
    )
    assert gate["stable_closed_question_ids"] == [NPE_ID]
    assert gate["unsupported_closures"] == 0
    assert gate["verdict"] == "A"
    final_contracts = apply_closure_gate(contracts_artifact=contracts, gate=gate)
    assert final_contracts["summary"]["remaining_hro"] == 5
    assert len(final_contracts["contracts"]) == 6


def test_human_priority_prevents_ai_routing() -> None:
    hro = _hro()
    question = next(
        value for value in hro["standalone_questions"] if value["question_id"] == NPE_ID
    )
    question["human_decision"] = {"answer": "SAME_REQUIREMENT"}
    runs, audits = _v3_runs()
    contracts, analysis, tasks = analyze_question_closure(
        hro_plan=hro,
        factory=_factory(),
        v3_runs=runs,
        v3_audits=audits,
        direct_page=_direct_page(),
    )
    contract = next(
        value for value in contracts["contracts"] if value["question_id"] == NPE_ID
    )
    row = next(value for value in analysis["questions"] if value["question_id"] == NPE_ID)
    assert contract["current_status"] == schemas.BLOCKED_POLICY
    assert contract["blocking_conditions"] == ["HUMAN_DECISION_HAS_PRIORITY"]
    assert row["ai_needed"] is False
    assert tasks["tasks"] == []


def test_no_question_is_silently_removed_from_each_run() -> None:
    contracts, runs = _closure_runs()
    for run in runs:
        assert run["constraints"]["exact_question_accounting"] is True
        assert run["constraints"]["no_question_silently_removed"] is True
        assert len(run["provisional_closed_question_ids"]) + len(run["remaining_question_ids"]) == 6
        assert set(run["provisional_closed_question_ids"]).isdisjoint(run["remaining_question_ids"])


def test_graph_question_closes_only_when_exact_source_inventory_is_covered() -> None:
    direct = _direct_page()
    change = direct["comparison_result"]["changes"][0]
    change["evidence"]["left"]["node_ids"] = ["L1"]
    change["evidence"]["right"]["node_ids"] = ["R1"]
    runs, audits = _v3_runs()
    contracts, analysis, tasks = analyze_question_closure(
        hro_plan=_hro(),
        factory=_factory(),
        v3_runs=runs,
        v3_audits=audits,
        direct_page=direct,
    )
    graph = next(
        value for value in contracts["contracts"] if value["question_id"] == GRAPH_ID
    )
    assert graph["current_status"] == schemas.CLOSED_AI_STABLE
    assert analysis["graph_correspondence"]["fully_covered"] is True
    assert all(
        not value["subproblem_id"].startswith("graph_")
        for value in tasks["blocked_subproblems"]
    )


def test_measurement_stable_insufficient_result_is_reused_without_model_call() -> None:
    contracts, analysis, tasks = _analysis()
    measurement = next(
        value for value in contracts["contracts"]
        if value["question_id"] == MEASUREMENT_ID
    )
    row = next(
        value for value in analysis["questions"]
        if value["question_id"] == MEASUREMENT_ID
    )
    assert measurement["stable_fail_closed_candidate_id"] == "measurement-insufficient"
    assert measurement["current_status"] == schemas.BLOCKED_MISSING_EVIDENCE
    assert row["ai_needed"] is False
    assert MEASUREMENT_ID not in {value["question_id"] for value in tasks["tasks"]}


def test_any_unsafe_manual_closure_audit_fails_gate() -> None:
    contracts, runs = _closure_runs()
    audit = build_pending_manual_audit(contracts_artifact=contracts, run_artifacts=runs)
    audit["status"] = "COMPLETE"
    audit["items"][0]["manual_verdict"] = schemas.UNSAFE_TO_CLOSE
    for condition in audit["items"][0]["closure_conditions"]:
        condition["checked"] = True
    gate = evaluate_closure_gate(
        contracts_artifact=contracts,
        run_artifacts=runs,
        manual_audit=audit,
    )
    assert gate["verdict"] == "C"
    assert gate["stable_closed_question_ids"] == []
    assert gate["unsupported_closures"] == 1

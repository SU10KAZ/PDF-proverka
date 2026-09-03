from __future__ import annotations

import copy
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.app.services.stage_comparison import function_lineage_shadow as lineage
from experiments.function_lineage_v2 import smoke, transport


def _small_payload():
    return {
        "payload_signature": "payload-signature",
        "task_ids": ["task-a", "task-b"],
        "task_contexts": [
            {
                "task_id": "task-a",
                "allowed_decisions": ["candidate-a", lineage.NEED_MORE_EVIDENCE],
            },
            {
                "task_id": "task-b",
                "allowed_decisions": ["candidate-b", lineage.NEED_MORE_EVIDENCE],
            },
        ],
    }


def _valid_response(*, first="candidate-a", second="candidate-b"):
    return {
        "results": [
            {"task_id": "task-a", "decision": first},
            {"task_id": "task-b", "decision": second},
        ],
    }


def test_frozen_smoke_selects_exactly_four_critical_contexts():
    metadata, shards = smoke.build_frozen_smoke_inputs()
    assert list(metadata["selected_tasks"]) == ["LEFT17", "LEFT18", "LEFT19", "LEFT20"]
    assert [task_id for shard in shards for task_id in shard["task_ids"]] == list(
        smoke.TASKS.values()
    )
    assert metadata["smoke_shard_characters"] == [191683, 114177, 147542]
    assert all(value <= transport.TARGET_CHARACTERS for value in metadata["smoke_shard_characters"])


def test_critical_candidate_controls_are_frozen():
    metadata, _ = smoke.build_frozen_smoke_inputs()
    values = metadata["selected_tasks"]
    left17 = values["LEFT17"]["candidate_inventory"]
    left18 = values["LEFT18"]["candidate_inventory"]
    left19 = values["LEFT19"]["candidate_inventory"]
    left20 = values["LEFT20"]["candidate_inventory"]
    assert any(row["rank"] == 1 and row["right_physical_pages"] == [27] for row in left17)
    assert any(row["rank"] == 1 and row["right_physical_pages"] == [24] for row in left18)
    assert any(row["rank"] == 1 and row["right_physical_pages"] == [30] for row in left19)
    assert any(row["rank"] == 2 and row["right_physical_pages"] == [25] for row in left19)
    group = next(row for row in left20 if row["candidate_id"] == "lcand_9c617494b14c2b922d3f")
    assert group["relation_type"] == "FUNCTION_DISTRIBUTED"
    assert group["right_physical_pages"] == [26, 28, 29]
    assert len(group["component_mapping"]) == 3


def test_all_allowed_smoke_decisions_pass_existing_verifier():
    _, shards = smoke.build_frozen_smoke_inputs()
    dataset = smoke._dataset(smoke._read_json(smoke.CANDIDATE_INPUT))
    result = smoke._preflight_verifier(dataset, shards)
    assert result["all_allowed_decisions_verifier_pass"] is True
    assert result["allowed_decision_failures"] == []
    assert result["capacity_key_defects"] == []


def test_unknown_candidate_fails_closed_before_existing_verifier():
    payload = _small_payload()
    response = _valid_response(first="candidate-unknown")
    result = transport.verify_transport_response(payload, response)
    assert result["ok"] is False
    assert result["task_results"]["task-a"]["errors"] == [
        "UNKNOWN_CANDIDATE_ID"
    ]


def test_generated_schema_contains_no_oneof():
    schema = transport.output_schema(_small_payload())
    assert "oneOf" not in str(schema)


def test_generated_schema_contains_no_unsupported_union_constructs():
    schema = transport.output_schema(_small_payload())
    assert transport.provider_safe_schema_problems(schema) == []
    rendered = str(schema)
    assert all(name not in rendered for name in ("anyOf", "allOf", "not"))


def test_known_task_and_its_candidate_are_accepted_by_parser():
    assert transport.verify_transport_response(
        _small_payload(), _valid_response(),
    )["ok"] is True


def test_need_more_evidence_is_accepted_by_parser():
    assert transport.verify_transport_response(
        _small_payload(),
        _valid_response(first=lineage.NEED_MORE_EVIDENCE),
    )["ok"] is True


def test_candidate_from_another_task_is_rejected():
    result = transport.verify_transport_response(
        _small_payload(), _valid_response(first="candidate-b"),
    )
    assert result["ok"] is False
    assert result["task_results"]["task-a"]["errors"] == [
        "CANDIDATE_ID_NOT_ALLOWED_FOR_TASK"
    ]


def test_duplicate_task_is_rejected():
    response = _valid_response()
    response["results"][1]["task_id"] = "task-a"
    result = transport.verify_transport_response(_small_payload(), response)
    assert result["ok"] is False
    assert "DUPLICATE_TASK" in result["global_errors"]


def test_missing_task_is_rejected():
    response = _valid_response()
    response["results"].pop()
    result = transport.verify_transport_response(_small_payload(), response)
    assert result["ok"] is False
    assert "MISSING_TASK" in result["global_errors"]


def test_unexpected_task_is_rejected():
    response = _valid_response()
    response["results"].append({"task_id": "task-c", "decision": "candidate-a"})
    result = transport.verify_transport_response(_small_payload(), response)
    assert result["ok"] is False
    assert "UNEXPECTED_TASK" in result["global_errors"]


def test_extra_fields_are_rejected():
    response = _valid_response()
    response["free_mapping"] = {}
    response["results"][0]["pages"] = [1]
    result = transport.verify_transport_response(_small_payload(), response)
    assert result["ok"] is False
    assert "RESPONSE_FIELDS_INVALID" in result["global_errors"]
    assert "RESULT_FIELDS_INVALID" in result["global_errors"]


def _observations(choice_by_repeat):
    values = []
    for cold_run, pair in enumerate(choice_by_repeat, 1):
        for pass_name, decision in zip(smoke.PASSES, pair):
            values.append({
                "cold_run": cold_run,
                "pass_name": pass_name,
                "decision": decision,
                "model_ok": True,
                "request_failure_kind": None,
                "response_contract_ok": True,
                "verifier_ok": True,
                "verifier_errors": [],
                "capacity_ok": True,
                "capacity_errors": [],
                "shard_id": "shard",
            })
    return values


def _fake_manifest():
    metadata, _ = smoke.build_frozen_smoke_inputs()
    return {"frozen_inputs": metadata}


def test_two_pass_rule_has_no_majority_override(monkeypatch):
    same = _observations([("a", "a"), ("a", "a"), ("a", "a")])
    disagree = _observations([("a", "b"), ("a", "a"), ("a", "a")])
    data = {task_id: copy.deepcopy(same) for task_id in smoke.TASKS.values()}
    data[smoke.TASKS["LEFT19"]] = disagree
    monkeypatch.setattr(smoke, "_observations", lambda records: data)
    rows = smoke._task_results(_fake_manifest(), [])
    left19 = next(value for value in rows if value["label"] == "LEFT19")
    assert left19["stable_repeat_count"] == 2
    assert left19["stable_across_cold_runs"] is False
    assert left19["cold_repeats"][0]["status"] == "PASS_DISAGREEMENT"


def test_stability_requires_same_result_across_cold_runs(monkeypatch):
    changing = _observations([("a", "a"), ("b", "b"), ("a", "a")])
    data = {task_id: copy.deepcopy(changing) for task_id in smoke.TASKS.values()}
    monkeypatch.setattr(smoke, "_observations", lambda records: data)
    rows = smoke._task_results(_fake_manifest(), [])
    assert all(value["stable_repeat_count"] == 3 for value in rows)
    assert all(value["stable_across_cold_runs"] is False for value in rows)


def test_fragment_capacity_allows_page_reuse_for_distinct_fragments():
    candidates = {
        "a": {"right_capacity_keys": ["RIGHT:27:frag_a"]},
        "b": {"right_capacity_keys": ["RIGHT:27:frag_b"]},
    }
    assert lineage.verify_capacity([
        {"candidate_id": "a"}, {"candidate_id": "b"},
    ], candidates) == []


def test_no_inference_response_reports_verifier_and_capacity_not_applicable(monkeypatch):
    failed = [{
        "cold_run": 1,
        "pass_name": "A",
        "decision": None,
        "model_ok": False,
        "request_failure_kind": "SCHEMA_FAILURE",
        "response_contract_ok": None,
        "verifier_ok": None,
        "verifier_errors": [],
        "capacity_ok": None,
        "capacity_errors": [],
        "shard_id": "shard",
    }]
    data = {task_id: copy.deepcopy(failed) for task_id in smoke.TASKS.values()}
    monkeypatch.setattr(smoke, "_observations", lambda records: data)
    rows = smoke._task_results(_fake_manifest(), [])
    assert all(value["verifier_result"] == "N/A" for value in rows)
    assert all(value["capacity_result"] == "N/A" for value in rows)


def test_one_failed_request_is_not_double_counted():
    records = [{
        "task_ids": ["task-a", "task-b"],
        "model_call": {
            "ok": False,
            "attempts": 1,
            "failure_kind": "SCHEMA_FAILURE",
        },
        "transport_verification": {"ok": False},
    }]
    assert smoke._request_counters(records) == {
        "request_attempts": 1,
        "request_start_failures": 1,
        "successful_inference_requests": 0,
        "affected_task_observations": 2,
        "schema_failures": 1,
        "model_runtime_failures": 0,
        "semantic_response_failures": 0,
    }


def test_verdict_precedence_is_fail_closed():
    telemetry = {"stopped_early": True}
    record = {
        "model_call": {"ok": False},
        "transport_verification": {"ok": False},
    }
    tasks = [{
        "candidate_inventory": [1],
        "verifier_result": "PASS",
        "stable_across_cold_runs": True,
        "stable_unresolved": False,
    }]
    capacity = {"capacity_errors": [], "left20_capacity_keys_exact": True}
    result = smoke._verdict([record], telemetry, tasks, capacity)
    assert result["verdict"] == "E"
    assert result["experiment_valid"] is False

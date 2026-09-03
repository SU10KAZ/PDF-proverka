from __future__ import annotations

import copy
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.app.services.stage_comparison import function_lineage_shadow as lineage
from experiments.function_lineage_v2 import smoke, transport


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
    _, shards = smoke.build_frozen_smoke_inputs()
    payload = shards[0]["model_payload"]
    response = {
        "payload_signature": payload["payload_signature"],
        "selections": [
            {
                "task_id": context["task_id"],
                "decision": "lcand_unknown" if index == 0 else lineage.NEED_MORE_EVIDENCE,
            }
            for index, context in enumerate(payload["task_contexts"])
        ],
    }
    result = transport.verify_transport_response(payload, response)
    assert result["ok"] is False
    assert result["task_results"][payload["task_ids"][0]]["errors"] == [
        "CANDIDATE_ID_NOT_BOUNDED"
    ]


def _observations(choice_by_repeat):
    values = []
    for cold_run, pair in enumerate(choice_by_repeat, 1):
        for pass_name, decision in zip(smoke.PASSES, pair):
            values.append({
                "cold_run": cold_run,
                "pass_name": pass_name,
                "decision": decision,
                "model_ok": True,
                "schema_ok": True,
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


def test_verdict_precedence_is_fail_closed():
    telemetry = {"stopped_early": True}
    record = {
        "model_call": {"ok": False},
        "transport_verification": {"ok": False},
    }
    tasks = [{"candidate_inventory": [1], "verifier_result": "PASS", "stable_across_cold_runs": True}]
    capacity = {"capacity_errors": [], "left20_capacity_keys_exact": True}
    result = smoke._verdict([record], telemetry, tasks, capacity)
    assert result["verdict"] == "E"
    assert result["experiment_valid"] is False

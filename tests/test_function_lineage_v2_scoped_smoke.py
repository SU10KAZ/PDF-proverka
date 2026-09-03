from __future__ import annotations

from experiments.function_lineage_v2 import scoped_smoke


def test_frozen_scoped_smoke_has_only_seven_exact_scope_tasks():
    frozen, shards, _ = scoped_smoke.build_frozen_smoke_inputs()
    contexts = [
        context
        for shard in shards
        for context in shard["model_payload"]["task_contexts"]
    ]
    assert len(contexts) == 7
    assert len(shards) == 4
    assert [value["task_id"] for value in contexts] == [
        value[0] for value in scoped_smoke.TASKS.values()
    ]
    assert frozen["seven_selected_critical_contexts_sha256"] == (
        scoped_smoke.EXPECTED_CONTEXTS_SHA256
    )
    for context in contexts:
        assert context["function_scope_core"]["scope_id"] == context["scope_id"]
        assert all(
            value["scope_relation"] == "EXACT_SCOPE"
            for value in context["functional_candidates"]
        )
        assert "FUNCTION_REMOVED" not in context["allowed_decisions"]


def test_scoped_smoke_provider_contract_and_verifier_preflight():
    _, shards, dataset = scoped_smoke.build_frozen_smoke_inputs()
    result = scoped_smoke._preflight(dataset, shards)
    assert result["ok"] is True
    assert result["parser_fail_closed"] is True
    assert result["cross_granularity_selectable_competition"] == 0
    assert all(not shard["provider_safe_schema_problems"] for shard in shards)


def test_left20_parent_excludes_child_singletons():
    frozen, _, _ = scoped_smoke.build_frozen_smoke_inputs()
    references = frozen["references_for_post_inference_comparison_only"]
    parent = frozen["selected_tasks"]["LEFT20 PARENT"]["candidate_inventory"]
    parent_ids = {value["candidate_id"] for value in parent}
    assert references["LEFT20_DISTRIBUTED"] in parent_ids
    assert not parent_ids.intersection(
        references[key]
        for key in ("LEFT20_R26", "LEFT20_R28", "LEFT20_R29")
    )

"""AI Analyst v3 bounded candidates, selector stability and safe projection."""
from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from backend.app.services.stage_comparison.ai import gateway
from backend.app.services.stage_comparison.ai_v3 import candidate_factory as factory_module
from backend.app.services.stage_comparison.ai_v3 import materialization, schemas, settings
from backend.app.services.stage_comparison.ai_v3.engine import (
    BoundedSelectorAnalyst,
    MAX_PROMPT_BYTES,
    SINGLE,
    UNANIMITY,
)
from backend.app.services.stage_comparison.ai_v3.reproducibility import evaluate
from backend.app.services.stage_comparison.ai_v3.verifier import verify_selection
from backend.app.services.stage_comparison.production_artifacts import content_signature


def _candidate(
    task_id: str, kind: str, *, eligibility=schemas.AUTO,
    effect="RESOLVE_HUMAN_QUESTION", semantic=True,
):
    return factory_module._make_candidate(
        task_id=task_id,
        candidate_type=kind,
        summary=kind,
        proof_requirements=[
            factory_module._proof("BOUND", "PROVEN"),
            *([factory_module._proof("SEMANTIC_RANKING", "REQUIRED")] if semantic else []),
        ],
        eligibility=eligibility,
        resolution_effect=effect,
        materialization={"kind": "TEXT_EQUIVALENCE", "answer": kind},
    )


def _task(task_id="t1"):
    candidates = [_candidate(task_id, "SAME_REQUIREMENT"), factory_module._human_fallback(task_id)]
    return factory_module._decorate_task(
        {"task_id": task_id, "source_kind": "TEXT", "summary": "same?"},
        schemas.TEXT_EQUIVALENCE,
        candidates,
        human_question_id="q1",
        affected_target_ids=[task_id],
    )


def _factory(task=None):
    task = task or _task()
    value = {
        "kind": "stage_comparison_ai_v3_candidate_factory",
        "schema_version": schemas.CANDIDATE_SCHEMA_VERSION,
        "factory_version": schemas.FACTORY_VERSION,
        "pair_id": "p",
        "fast_input_signature": "fast",
        "tasks": [task],
        "constraints": {},
    }
    value["candidate_set_signature"] = content_signature(value)
    return value


def _patch_factory(monkeypatch, task=None):
    value = _factory(task)
    monkeypatch.setattr(
        "backend.app.services.stage_comparison.ai_v3.engine.build_candidate_factory",
        lambda **_kwargs: (
            value,
            {"bundles": [candidate for row in value["tasks"] for candidate in row["candidates"]]},
            {},
        ),
    )
    return value


def _response_for(prompt: str, choose: str = "SAME_REQUIREMENT") -> dict:
    tasks = json.loads(prompt.split("BOUNDED TASKS\n", 1)[1].rsplit("\n\nОтветь", 1)[0])
    return {
        "selections": [
            {
                "task_id": task["task_id"],
                "selected_candidate_id": next(
                    option["candidate_id"] for option in task["options"]
                    if option["candidate_type"] == choose
                ),
                "confidence_bucket": "HIGH",
                "optional_short_reason": "bounded",
            }
            for task in tasks
        ]
    }


def _call(choices):
    iterator = iter(choices)

    def fake(_provider, prompt, **_kwargs):
        return gateway.CallResult(
            "CODEX_SESSION", settings.MODEL, "low", True,
            parsed=_response_for(prompt, next(iterator)),
        )
    return fake


@pytest.fixture
def minimal_artifacts():
    return {
        "direct_page_mode2": {
            "sources": {}, "left_graph": {}, "right_graph": {},
            "comparison_result": {}, "diagnostics": {},
        },
        "unified_synthesis": {"changes": [], "review_items": []},
        "engineer_decisions": {"decisions": []},
        "human_review_plan": {
            "standalone_questions": [{"question_id": "q1"}],
            "summary": {"review_groups": 1, "standalone_human_questions": 1,
                        "mandatory_human_interactions": 2},
        },
        "document_inconsistencies": {"items": []},
    }


def test_feature_flag_is_distinct_and_default_false(monkeypatch):
    monkeypatch.delenv(settings.FEATURE_FLAG, raising=False)
    monkeypatch.delenv("STAGE_COMPARISON_AI_ANALYST_V2", raising=False)
    assert settings.FEATURE_FLAG == "STAGE_COMPARISON_AI_ANALYST_V3"
    assert settings.enabled() is False
    with pytest.raises(RuntimeError):
        settings.require_enabled()


def test_candidate_generation_order_and_signatures_are_deterministic():
    first = _task()
    second = _task()
    assert first == second
    assert first["task_signature"] == second["task_signature"]
    assert [value["candidate_id"] for value in first["candidates"]] == sorted(
        [value["candidate_id"] for value in first["candidates"]],
        key=lambda candidate_id: next(
            (value["candidate_type"], value["candidate_id"])
            for value in first["candidates"] if value["candidate_id"] == candidate_id
        ),
    )
    for candidate in first["candidates"]:
        unsigned = {key: value for key, value in candidate.items() if key != "candidate_signature"}
        assert candidate["candidate_signature"] == content_signature(unsigned)


def test_selector_schema_has_only_candidate_id_not_evidence_fields():
    schema = schemas.selector_schema(["a", "b"])
    properties = schema["properties"]["selections"]["items"]["properties"]
    assert set(properties) == {
        "task_id", "selected_candidate_id", "confidence_bucket", "optional_short_reason"
    }
    assert properties["selected_candidate_id"]["enum"] == ["a", "b"]
    assert not ({"evidence_refs", "values", "units", "claims"} & set(properties))


def test_table_row_factory_builds_real_pairs_and_prefilters_wrong_section():
    catalog = {
        "LEFT:ROW:L1": {"ref": "LEFT:ROW:L1", "side": "LEFT", "row_id": "L1",
                         "row_kind": "FEEDER", "section": "S1", "label": "A",
                         "designations": ["A"], "values": [], "cables": []},
        "RIGHT:ROW:R1": {"ref": "RIGHT:ROW:R1", "side": "RIGHT", "row_id": "R1",
                          "row_kind": "FEEDER", "section": "S1", "label": "A",
                          "designations": ["A"], "values": [], "cables": []},
        "RIGHT:ROW:R2": {"ref": "RIGHT:ROW:R2", "side": "RIGHT", "row_id": "R2",
                          "row_kind": "FEEDER", "section": "S2", "label": "A",
                          "designations": ["A"], "values": [], "cables": []},
    }
    task = {"task_id": "row", "source_kind": "TABLE_ROW_UNPROVEN", "side": "LEFT",
            "routing_payload": {"row_id": "L1", "side": "LEFT",
                                "candidate_row_ids": ["R1", "R2"]}}
    values = factory_module._table_candidates(task, catalog)
    pairs = [value for value in values if value["candidate_type"] == "ROW_PAIR"]
    assert len(pairs) == 2
    assert sum(value["eligibility"] == schemas.INVALID for value in pairs) == 1
    assert {value["candidate_type"] for value in values} >= {"ROW_PAIR", "NONE", "INSUFFICIENT_EVIDENCE"}


def test_change_factory_prebinds_formatting_and_real_values():
    catalog = {
        "FAST:CHANGE:c": {"ref": "FAST:CHANGE:c", "facet": "cable_parallel_count", "relation": {}},
        "LEFT:ROW:L2": {"ref": "LEFT:ROW:L2", "side": "LEFT", "cables": ["2х(5х95)"]},
        "LEFT:ROW:L3": {"ref": "LEFT:ROW:L3", "side": "LEFT", "cables": ["3х(5х120)"]},
        "RIGHT:ROW:R2": {"ref": "RIGHT:ROW:R2", "side": "RIGHT", "cables": ["2х(5х150)"]},
    }
    focus = {"candidate_refs": ["FAST:CHANGE:c"],
             "context_refs": ["LEFT:ROW:L2", "LEFT:ROW:L3", "RIGHT:ROW:R2"]}
    values = factory_module._change_candidates(
        {"task_id": "c", "source_kind": "CHANGE_INCOMPLETE_EVIDENCE"}, focus, catalog
    )
    assert {value["candidate_type"] for value in values} >= {
        "FORMATTING_ONLY_2_TO_2", "REAL_CHANGE_3_TO_2", "DIFFERENT_ENTITY",
        "INSUFFICIENT_EVIDENCE",
    }
    formatting = next(value for value in values if value["candidate_type"] == "FORMATTING_ONLY_2_TO_2")
    assert formatting["values"] == {"before": 2, "after": 2}
    assert formatting["left_refs"] == ["LEFT:ROW:L2"]


def test_bounded_absence_cannot_become_auto_resolution():
    catalog = {"FAST:CHANGE:r": {
        "ref": "FAST:CHANGE:r", "facet": "reserve_branch_count",
        "relation": {"left_value": 0, "right_value": 2},
    }}
    values = factory_module._change_candidates(
        {"task_id": "r", "summary": "резервные линии", "source_kind": "CHANGE"},
        {"candidate_refs": ["FAST:CHANGE:r"], "context_refs": []}, catalog,
    )
    supported = next(value for value in values if value["candidate_type"] == "SUPPORTED_CHANGE_0_TO_2")
    assert supported["eligibility"] == schemas.INVALID
    assert supported["prefilter_reasons"] == ["LEFT_ABSENCE_NOT_PROVEN"]


def test_text_candidates_bind_exact_spans_and_no_absence_claim():
    catalog = {
        "LEFT:TEXT:L": {"ref": "LEFT:TEXT:L", "side": "LEFT", "text": "Мультиметр"},
        "RIGHT:TEXT:R": {"ref": "RIGHT:TEXT:R", "side": "RIGHT",
                          "text": "Применить многофункциональные измерительные приборы"},
    }
    question = {
        "question_id": "q", "question": "same", "target_text": catalog["RIGHT:TEXT:R"]["text"],
        "affected_target_ids": ["t"],
        "candidate_evidence": {"strong_semantic_candidate": "Мультиметр",
                               "full_searchable_text": True, "recognition_coverage": "HIGH"},
    }
    values = factory_module._text_candidates(task_id="t", question=question, catalog=catalog)
    same = next(value for value in values if value["candidate_type"] == "SAME_REQUIREMENT")
    added = next(value for value in values if value["candidate_type"] == "REQUIREMENT_ADDED")
    assert same["text_refs"] == ["LEFT:TEXT:L", "RIGHT:TEXT:R"]
    assert added["eligibility"] == schemas.INVALID


def test_graph_entity_prefilter_and_mode_advisory_only():
    catalog = {
        "LEFT:NODE:L": {"side": "LEFT", "entity_type": "LOAD", "section": "S1"},
        "RIGHT:NODE:R": {"side": "RIGHT", "entity_type": "BUS_SECTION", "section": "S1"},
    }
    values = factory_module._graph_candidates(
        {"task_id": "g", "routing_payload": {"left_node_id": "L", "right_node_ids": ["R"]}},
        catalog,
    )
    pair = next(value for value in values if value["candidate_type"] == "ENTITY_PAIR")
    assert pair["eligibility"] == schemas.INVALID
    mode = factory_module._mode_candidates({"task_id": "m", "summary": "Рабочий ↔ Аварийный"})
    assert all(value["resolution_effect"] == "HUMAN_REQUIRED" for value in mode)
    assert mode[0]["eligibility"] == schemas.ADVISORY


def test_deterministic_single_winner_skips_ai(monkeypatch, minimal_artifacts):
    only = _candidate("t1", "PROVEN", semantic=False)
    task = factory_module._decorate_task(
        {"task_id": "t1", "source_kind": "TEXT", "summary": "proven"},
        schemas.TEXT_EQUIVALENCE, [only],
    )
    assert task["deterministic_winner_candidate_id"] == only["candidate_id"]
    _patch_factory(monkeypatch, task)
    monkeypatch.setenv(settings.FEATURE_FLAG, "true")
    analyst = BoundedSelectorAnalyst(
        artifacts=minimal_artifacts, pair_id="p", call=lambda *_a, **_k: pytest.fail("AI called")
    )
    run = analyst.run()
    assert run["diagnostics"]["model_calls"] == 0
    assert run["stable_selections"][0]["source"] == "DETERMINISTIC"


def test_verifier_rejects_invented_or_cross_task_candidate(minimal_artifacts):
    task = _task()
    check = verify_selection(
        task=task,
        selection={"task_id": "t1", "selected_candidate_id": "invented"},
        catalog={}, artifacts=minimal_artifacts,
        frozen_fast_signature="x", current_fast_signature="x",
    )
    assert check["status"] == schemas.INVALID_RESPONSE
    assert "CANDIDATE_NOT_IN_TASK" in check["errors"]


def test_human_priority_rejects_materialization(minimal_artifacts):
    task = _task()
    chosen = next(value for value in task["candidates"] if value["candidate_type"] == "SAME_REQUIREMENT")
    protected = deepcopy(minimal_artifacts)
    protected["engineer_decisions"] = {"decisions": [{
        "target_id": "t1", "decision": "APPROVED", "author": "engineer",
    }]}
    check = verify_selection(
        task=task,
        selection={"task_id": "t1", "selected_candidate_id": chosen["candidate_id"]},
        catalog={}, artifacts=protected,
        frozen_fast_signature="x", current_fast_signature="x",
    )
    assert check["status"] == schemas.REJECTED_SELECTION
    assert "HUMAN_DECISION_HAS_PRIORITY" in check["errors"]


def test_single_pass_selector_and_no_model_evidence(monkeypatch, minimal_artifacts):
    _patch_factory(monkeypatch)
    monkeypatch.setenv(settings.FEATURE_FLAG, "true")
    run = BoundedSelectorAnalyst(
        artifacts=minimal_artifacts, pair_id="p", mode=SINGLE,
        cache_enabled=False, call=_call(["SAME_REQUIREMENT"]),
    ).run()
    assert run["diagnostics"]["model_calls"] == 1
    assert run["stable_selections"][0]["status"] == schemas.VERIFIED_SELECTION
    assert "evidence_refs" not in run["stable_selections"][0]


def test_oversized_prompt_fails_closed_before_model_call(monkeypatch, minimal_artifacts):
    task = _task()
    task["candidates"][0]["summary"] = "x" * MAX_PROMPT_BYTES
    task["task_signature"] = content_signature({
        key: value for key, value in task.items() if key != "task_signature"
    })
    _patch_factory(monkeypatch, task)
    monkeypatch.setenv(settings.FEATURE_FLAG, "true")
    run = BoundedSelectorAnalyst(
        artifacts=minimal_artifacts,
        pair_id="p",
        mode=SINGLE,
        cache_enabled=False,
        call=lambda *_a, **_k: pytest.fail("oversized prompt reached model"),
    ).run()
    assert run["diagnostics"]["model_calls"] == 0
    assert run["diagnostics"]["call_metrics"][0]["error_kind"] == "PROMPT_TOO_LARGE"
    assert run["stable_selections"][0]["status"] == schemas.HUMAN_REQUIRED


def test_two_pass_unanimity_and_cache_identity_isolation(monkeypatch, minimal_artifacts):
    _patch_factory(monkeypatch)
    monkeypatch.setenv(settings.FEATURE_FLAG, "true")
    run = BoundedSelectorAnalyst(
        artifacts=minimal_artifacts, pair_id="p", mode=UNANIMITY,
        cache_enabled=False, call=_call(["SAME_REQUIREMENT", "SAME_REQUIREMENT"]),
    ).run()
    assert run["stable_selections"][0]["status"] == schemas.VERIFIED_SELECTION
    first, second = run["prompt_manifest"]
    assert first["prompt_signature"] == second["prompt_signature"]
    assert first["cache_key"] != second["cache_key"]
    assert first["pass_identity"] != second["pass_identity"]


def test_unanimity_disagreement_is_human_required(monkeypatch, minimal_artifacts):
    _patch_factory(monkeypatch)
    monkeypatch.setenv(settings.FEATURE_FLAG, "true")
    run = BoundedSelectorAnalyst(
        artifacts=minimal_artifacts, pair_id="p", mode=UNANIMITY,
        cache_enabled=False,
        call=_call(["SAME_REQUIREMENT", "INSUFFICIENT_EVIDENCE"]),
    ).run()
    assert run["stable_selections"][0]["status"] == schemas.HUMAN_REQUIRED
    assert run["stable_selections"][0]["unanimous"] is False


def _delegate_artifact():
    return {
        "kind": "stage_comparison_ai_v2_materialization",
        "schema_version": "v2",
        "source": "AI_ANALYST_V2",
        "outcomes": [],
        "human_review_plan": {
            "standalone_questions": [{"question_id": "q1"}],
            "summary": {"review_groups": 1, "standalone_human_questions": 1,
                        "mandatory_human_interactions": 2},
        },
        "diagnostics": {}, "constraints": {},
    }


def test_materialization_uses_existing_pipeline_and_manual_supported_only(
    monkeypatch, minimal_artifacts,
):
    factory = _factory()
    chosen = next(value for value in factory["tasks"][0]["candidates"]
                  if value["candidate_type"] == "SAME_REQUIREMENT")
    run = {"model": settings.MODEL, "reasoning_effort": "low", "input_signature": "run",
           "candidate_set_signature": factory["candidate_set_signature"],
           "diagnostics": {"model_calls": 2}, "stable_selections": [{
               "task_id": "t1", "task_type": schemas.TEXT_EQUIVALENCE,
               "status": schemas.VERIFIED_SELECTION,
               "selected_candidate_id": chosen["candidate_id"],
               "confidence_bucket": "HIGH",
           }]}
    captured = {}

    def delegate(**kwargs):
        captured.update(kwargs)
        return _delegate_artifact()

    monkeypatch.setattr(materialization, "materialize_verified_resolutions", delegate)
    complete = {"status": "COMPLETE", "items": [{
        "task_id": "t1", "manual_verdict": "SUPPORTED",
    }]}
    result = materialization.materialize_stable_selections(
        artifacts=minimal_artifacts, factory=factory, run=run,
        pair_id="p", manual_audit=complete,
    )
    assert "run" in captured
    assert result["source"] == "AI_ANALYST_V3"
    assert result["outcomes"][0]["outcome"] == "RESOLVED_HUMAN_QUESTION"
    assert result["diagnostics"]["human_interactions_saved"] == 1

    partial = deepcopy(complete)
    partial["items"][0]["manual_verdict"] = "PARTIALLY_SUPPORTED"
    result = materialization.materialize_stable_selections(
        artifacts=minimal_artifacts, factory=factory, run=run,
        pair_id="p", manual_audit=partial,
    )
    assert result["outcomes"][0]["outcome"] == "HUMAN_REQUIRED"
    assert result["diagnostics"]["human_interactions_saved"] == 0


def _write_run(
    directory: Path, *, candidate="c", cache=False, unsupported=0, hro=5,
):
    directory.mkdir(parents=True)
    factory = {"candidate_set_signature": "factory"}
    run = {
        "fast_input_signature": "fast",
        "prompt_manifest": [{"batch_id": "b", "pass_identity": "pass_1",
                             "prompt_signature": "prompt", "schema_signature": "schema",
                             "task_ids": ["t"]}],
        "diagnostics": {"cache": {"enabled": cache, "hits": 0},
                        "model_calls": 1, "duration_ms": 10},
    }
    fingerprint = content_signature(candidate)
    materialized = {
        "outcomes": [{"task_id": "t", "selected_candidate_id": candidate,
                      "product_fingerprint": fingerprint, "outcome": "NO_CHANGE"}],
        "human_review_plan": {"summary": {"mandatory_human_interactions": hro}},
        "diagnostics": {"unsupported_materialized": unsupported},
    }
    audit = {"status": "COMPLETE", "items": [{"task_id": "t", "manual_verdict": "SUPPORTED"}]}
    for name, value in (("candidate_factory", factory), ("run", run),
                        ("materialization", materialized), ("manual_audit", audit)):
        (directory / f"{name}.json").write_text(json.dumps(value), encoding="utf-8")


def test_three_run_reproducibility_gate_and_stable_core(tmp_path):
    dirs = [tmp_path / f"run{i}" for i in range(3)]
    for directory in dirs:
        _write_run(directory)
    result = evaluate(dirs)
    assert result["verdict"] == "A"
    assert result["minimum_pairwise_product_overlap"] == 1.0
    assert len(result["all_run_stable_core"]) == 1


def test_reproducibility_drift_is_b_and_cache_is_c(tmp_path):
    dirs = [tmp_path / f"run{i}" for i in range(3)]
    _write_run(dirs[0])
    _write_run(dirs[1], candidate="other")
    _write_run(dirs[2])
    assert evaluate(dirs)["verdict"] == "B"
    dirty = [tmp_path / f"cold{i}" for i in range(3)]
    _write_run(dirty[0])
    _write_run(dirty[1], cache=True)
    _write_run(dirty[2])
    assert evaluate(dirty)["verdict"] == "C"


def test_reproducibility_hro_drift_is_safe_b_not_systemic_c(tmp_path):
    dirs = [tmp_path / f"run{i}" for i in range(3)]
    _write_run(dirs[0])
    _write_run(dirs[1], hro=4)
    _write_run(dirs[2])
    result = evaluate(dirs)
    assert result["verdict"] == "B"
    assert result["systemic_problems"] == []
    assert result["stability_problems"] == ["HRO count differs"]


def test_production_orchestrator_does_not_reference_v3():
    source = Path(
        "backend/app/services/stage_comparison/production_orchestrator.py"
    ).read_text(encoding="utf-8")
    assert "AI_ANALYST_V3" not in source
    assert "ai_v3" not in source

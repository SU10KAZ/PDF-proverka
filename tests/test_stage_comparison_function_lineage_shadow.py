from __future__ import annotations

import copy
import json
from types import SimpleNamespace

import pytest

from backend.app.services.stage_comparison import function_lineage_shadow as lineage
from backend.app.services.stage_comparison import production_orchestrator as orchestrator
from backend.app.services.stage_comparison.sheet_matcher import match_sheets


def _indexes(*, multiple_functions: bool = False):
    if multiple_functions:
        return {
            "left": [{
                "pdf_page": 1,
                "title": "Схема водоснабжения и пожаротушения",
                "functional_content": ["водоснабжение", "пожаротушение"],
                "main_entities": ["насос ХВС", "насос ВПВ"],
            }],
            "right": [
                {
                    "pdf_page": 2,
                    "title": "Схема водоснабжения",
                    "functional_content": ["водоснабжение"],
                    "main_entities": ["насос ХВС"],
                },
                {
                    "pdf_page": 3,
                    "title": "Схема пожаротушения",
                    "functional_content": ["пожаротушение"],
                    "main_entities": ["насос ВПВ"],
                },
            ],
        }
    return {
        "left": [{
            "pdf_page": 1,
            "title": "Однолинейная схема ВРУ-1",
            "functional_content": ["электрическое распределение ВРУ-1"],
            "main_entities": ["ВРУ-1"],
        }],
        "right": [{
            "pdf_page": 4,
            "title": "Однолинейная схема ВРУ-1",
            "functional_content": ["электрическое распределение ВРУ-1"],
            "main_entities": ["ВРУ-1"],
        }],
    }


def _dataset(*, multiple_functions: bool = False):
    indexes = _indexes(multiple_functions=multiple_functions)
    relations = match_sheets(indexes["left"], indexes["right"])
    return lineage.build_dataset(
        pair_id="pair-1",
        sheet_indexes=indexes,
        sheet_relations=relations,
    )


def _model_result(dataset, *, choose=None, ok=True):
    selections = []
    for task in dataset.tasks:
        candidate_id = (
            choose(task) if choose is not None
            else (task["candidate_ids"][0] if task["candidate_ids"] else lineage.NEED_MORE_EVIDENCE)
        )
        selections.append({"task_id": task["task_id"], "candidate_id": candidate_id})
    payload = lineage.build_selector_payload(dataset)
    return SimpleNamespace(
        ok=ok,
        parsed={
            "payload_signature": payload["payload_signature"],
            "selections": selections,
        } if ok else None,
        duration_ms=7,
        usage={"input_tokens": 10, "output_tokens": 3},
        error_kind="" if ok else "MODEL_FAILURE",
        attempts=1,
    )


def test_document_link_and_functional_analogue_namespaces_are_independent():
    dataset = _dataset()

    assert dataset.document_link_map["relation_namespace"] == "DOCUMENT_LINK"
    assert dataset.document_link_map["links"]
    assert all(
        item["functional_score_contribution"] == 0
        for item in dataset.document_link_map["links"]
    )
    assert dataset.candidates
    assert all(
        item["relation_namespace"] == "FUNCTIONAL_ANALOGUE"
        for item in dataset.candidates.values()
    )


def test_function_passport_v2_is_compact_and_provenance_complete():
    dataset = _dataset(multiple_functions=True)

    for side in ("LEFT", "RIGHT"):
        for passport in dataset.function_passports[side].values():
            assert set(lineage.PASSPORT_FIELDS) <= set(passport)
            assert set(lineage.PASSPORT_FIELDS) == set(passport["provenance"])
            assert passport["evidence_refs"]
            assert "raw_excerpt" not in passport


def test_multi_function_sheet_candidate_does_not_inherit_sibling_fragment_evidence():
    dataset = _dataset(multiple_functions=True)
    task = next(task for task in dataset.tasks if task["candidate_ids"])
    candidate = dataset.candidates[task["candidate_ids"][0]]
    sibling = next(
        fragment
        for fragment_id, fragment in dataset.function_fragments["LEFT"].items()
        if fragment_id != task["left_fragment_id"]
        and fragment["physical_page"] == task["left_physical_page"]
    )
    sibling_owned_refs = {
        ref
        for ref in sibling["evidence_refs"]
        if dataset.evidence_catalog[ref]["provenance_type"]
        == lineage.FRAGMENT_OWNED_EVIDENCE
    }

    assert sibling_owned_refs
    assert sibling_owned_refs.isdisjoint(candidate["evidence_refs"])


def test_multi_function_sheet_candidate_accepts_owned_and_explicit_shared_evidence():
    dataset = _dataset(multiple_functions=True)
    task = next(task for task in dataset.tasks if task["candidate_ids"])
    candidate_id = task["candidate_ids"][0]
    candidate = dataset.candidates[candidate_id]
    evidence = [dataset.evidence_catalog[ref] for ref in candidate["evidence_refs"]]
    response = _model_result(
        dataset,
        choose=lambda value: (
            candidate_id
            if value["task_id"] == task["task_id"]
            else lineage.NEED_MORE_EVIDENCE
        ),
    ).parsed
    payload = lineage.build_selector_payload(dataset)

    verified = lineage.verify_selector_response(
        dataset, response["payload_signature"], response
    )

    assert {item["provenance_type"] for item in evidence} == {
        lineage.FRAGMENT_OWNED_EVIDENCE,
        lineage.SHEET_SHARED_EVIDENCE,
    }
    assert all(
        item["owner_function_id"] is None and item["owner_fragment_id"] is None
        for item in evidence
        if item["provenance_type"] == lineage.SHEET_SHARED_EVIDENCE
    )
    assert payload["policy"]["same_page_fragment_evidence_is_not_transferable"] is True
    assert {
        payload["evidence_provenance"][ref]["provenance_type"]
        for ref in candidate["evidence_refs"]
    } == {
        lineage.FRAGMENT_OWNED_EVIDENCE,
        lineage.SHEET_SHARED_EVIDENCE,
    }
    assert verified["ok"] is True
    assert verified["task_results"][task["task_id"]]["errors"] == []


def test_verifier_rejects_sibling_fragment_evidence_added_to_candidate():
    dataset = _dataset(multiple_functions=True)
    task = next(task for task in dataset.tasks if task["candidate_ids"])
    candidate_id = task["candidate_ids"][0]
    candidate = dataset.candidates[candidate_id]
    sibling = next(
        fragment
        for fragment_id, fragment in dataset.function_fragments["LEFT"].items()
        if fragment_id != task["left_fragment_id"]
        and fragment["physical_page"] == task["left_physical_page"]
    )
    sibling_ref = next(
        ref
        for ref in sibling["evidence_refs"]
        if dataset.evidence_catalog[ref]["provenance_type"]
        == lineage.FRAGMENT_OWNED_EVIDENCE
    )
    candidate["evidence_refs"].append(sibling_ref)
    response = _model_result(
        dataset,
        choose=lambda value: (
            candidate_id
            if value["task_id"] == task["task_id"]
            else lineage.NEED_MORE_EVIDENCE
        ),
    ).parsed

    verified = lineage.verify_selector_response(
        dataset, response["payload_signature"], response
    )

    assert verified["ok"] is False
    assert "EVIDENCE_FUNCTION_OWNER_MISMATCH" in (
        verified["task_results"][task["task_id"]]["errors"]
    )
    assert "EVIDENCE_FRAGMENT_OWNER_MISMATCH" in (
        verified["task_results"][task["task_id"]]["errors"]
    )


def test_same_page_does_not_make_unlisted_shared_evidence_valid():
    dataset = _dataset(multiple_functions=True)
    task = next(task for task in dataset.tasks if task["candidate_ids"])
    candidate_id = task["candidate_ids"][0]
    candidate = dataset.candidates[candidate_id]
    evidence_ref = "unlisted-same-page-shared-evidence"
    dataset.evidence_catalog[evidence_ref] = {
        "evidence_id": evidence_ref,
        "side": "LEFT",
        "physical_page": task["left_physical_page"],
        "field": "systems",
        "provenance_type": lineage.SHEET_SHARED_EVIDENCE,
        "owner_function_id": None,
        "owner_fragment_id": None,
    }
    candidate["evidence_refs"].append(evidence_ref)
    response = _model_result(
        dataset,
        choose=lambda value: (
            candidate_id
            if value["task_id"] == task["task_id"]
            else lineage.NEED_MORE_EVIDENCE
        ),
    ).parsed

    verified = lineage.verify_selector_response(
        dataset, response["payload_signature"], response
    )

    errors = verified["task_results"][task["task_id"]]["errors"]
    assert "EVIDENCE_NOT_OWNED_BY_CANDIDATE" in errors
    assert "EVIDENCE_SET_INCOMPLETE" in errors


def test_verifier_rejects_unbounded_candidate_id():
    dataset = _dataset()
    payload = lineage.build_selector_payload(dataset)
    response = {
        "payload_signature": payload["payload_signature"],
        "selections": [
            {"task_id": task["task_id"], "candidate_id": "invented-candidate"}
            for task in dataset.tasks
        ],
    }

    verified = lineage.verify_selector_response(
        dataset, payload["payload_signature"], response
    )

    assert verified["ok"] is False
    assert all(
        "CANDIDATE_ID_NOT_BOUNDED" in result["errors"]
        for result in verified["task_results"].values()
    )


def test_same_right_sheet_is_reusable_through_different_fragments():
    candidates = {
        "a": {"right_capacity_keys": ["RIGHT:29:frag_meter"]},
        "b": {"right_capacity_keys": ["RIGHT:29:frag_incoming"]},
    }

    assert lineage.verify_capacity(
        [{"candidate_id": "a"}, {"candidate_id": "b"}], candidates
    ) == []


def test_same_right_function_fragment_reuse_is_blocked():
    candidates = {
        "a": {"right_capacity_keys": ["RIGHT:29:frag_meter"]},
        "b": {"right_capacity_keys": ["RIGHT:29:frag_meter"]},
    }

    errors = lineage.verify_capacity(
        [{"candidate_id": "a"}, {"candidate_id": "b"}], candidates
    )

    assert len(errors) == 1
    assert errors[0].startswith("FUNCTION_FRAGMENT_CONFLICT:RIGHT:29:frag_meter")


def test_verifier_rejects_invented_fragment_and_evidence():
    dataset = _dataset()
    payload = lineage.build_selector_payload(dataset)
    response = {
        "payload_signature": payload["payload_signature"],
        "selections": [{
            "task_id": task["task_id"],
            "candidate_id": lineage.NEED_MORE_EVIDENCE,
            "fragment_ids": ["invented"],
            "evidence_refs": ["invented"],
        } for task in dataset.tasks],
    }

    verified = lineage.verify_selector_response(
        dataset, payload["payload_signature"], response
    )

    assert verified["ok"] is False
    for result in verified["task_results"].values():
        assert "AI_INVENTED_FRAGMENT" in result["errors"]
        assert "AI_INVENTED_EVIDENCE" in result["errors"]


def test_two_pass_disagreement_is_unresolved(monkeypatch):
    dataset = _dataset()
    calls = 0

    def call(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        return _model_result(
            dataset,
            choose=(
                (lambda _task: lineage.NEED_MORE_EVIDENCE)
                if calls == 2 else None
            ),
        )

    monkeypatch.setattr(lineage.ai_gateway, "call", call)
    indexes = _indexes()
    result = lineage.run_shadow(
        pair_id="pair-1",
        run_id="run-1",
        sheet_indexes=indexes,
        sheet_relations=match_sheets(indexes["left"], indexes["right"]),
    )["function_lineage_map"]

    assert result["shadow_status"] == "COMPLETED"
    assert result["stable_lineages"] == []
    assert result["unresolved_lineages"][0]["reason_code"] == "PASS_DISAGREEMENT"
    assert result["model_calls"] == 2


def test_model_failure_fails_closed(monkeypatch):
    dataset = _dataset()
    calls = 0

    def call(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        return _model_result(dataset, ok=calls == 1)

    monkeypatch.setattr(lineage.ai_gateway, "call", call)
    indexes = _indexes()
    result = lineage.run_shadow(
        pair_id="pair-1",
        run_id="run-1",
        sheet_indexes=indexes,
        sheet_relations=match_sheets(indexes["left"], indexes["right"]),
    )["function_lineage_map"]

    assert result["shadow_status"] == "FAILED"
    assert result["stable_lineages"] == []
    assert result["verifier_result"]["status"] == "FAILED"
    assert result["materialization"]["applied"] is False


def test_manual_functional_mapping_is_never_mutated_and_has_priority(monkeypatch):
    dataset = _dataset()
    monkeypatch.setenv("AI_FUNCTION_LINEAGE_MATERIALIZATION_ENABLED", "true")
    monkeypatch.setattr(
        lineage.ai_gateway, "call", lambda *_args, **_kwargs: _model_result(dataset)
    )
    manual = [{
        "mapping_id": "human-1",
        "relation_namespace": "FUNCTIONAL_ANALOGUE",
        "left_pages": [1],
        "right_pages": [99],
    }, {
        "mapping_id": "document-only",
        "relation_namespace": "DOCUMENT_LINK",
        "left_pages": [1],
        "right_pages": [88],
    }]
    before = copy.deepcopy(manual)
    indexes = _indexes()

    artifacts = lineage.run_shadow(
        pair_id="pair-1",
        run_id="run-1",
        sheet_indexes=indexes,
        sheet_relations=match_sheets(indexes["left"], indexes["right"]),
        manual_mappings=manual,
    )

    function_map = artifacts["function_lineage_map"]
    assert manual == before
    assert len(function_map["engineer_disagreements"]) == 1
    assert function_map["engineer_disagreements"][0]["resolution"] == (
        "HUMAN_MAPPING_PRESERVED"
    )
    assert function_map["materialization"] == {
        "feature_flag": "AI_FUNCTION_LINEAGE_MATERIALIZATION_ENABLED",
        "requested": True,
        "implemented": False,
        "applied": False,
        "production_result_changed": False,
    }
    assert artifacts["derived_sheet_map"]["production_sheet_scope_unchanged"] is True


def test_both_lineage_flags_are_off_by_default(monkeypatch):
    monkeypatch.delenv("AI_FUNCTION_LINEAGE_SHADOW_ENABLED", raising=False)
    monkeypatch.delenv("AI_FUNCTION_LINEAGE_SHADOW_PAIR_ALLOWLIST", raising=False)
    monkeypatch.delenv("AI_FUNCTION_LINEAGE_SHADOW_RUN_ALLOWLIST", raising=False)
    monkeypatch.delenv("AI_FUNCTION_LINEAGE_MATERIALIZATION_ENABLED", raising=False)

    assert lineage.ai_settings.function_lineage_shadow_enabled() is False
    assert lineage.ai_settings.function_lineage_shadow_pair_allowlist() == frozenset()
    assert lineage.ai_settings.function_lineage_shadow_run_allowlist() == frozenset()
    assert lineage.ai_settings.function_lineage_shadow_target_allowed(
        pair_id="pair-1", run_id="run-1"
    ) is False
    assert lineage.ai_settings.function_lineage_materialization_enabled() is False


def test_flag_off_makes_no_calls_or_writes(monkeypatch):
    monkeypatch.delenv("AI_FUNCTION_LINEAGE_SHADOW_ENABLED", raising=False)
    monkeypatch.setenv("AI_FUNCTION_LINEAGE_SHADOW_PAIR_ALLOWLIST", "pair-1")
    monkeypatch.setattr(
        orchestrator.function_lineage_shadow,
        "run_shadow",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not run")),
    )
    writes = []
    monkeypatch.setattr(
        orchestrator.production_store,
        "save_artifact",
        lambda *_args: writes.append(_args),
    )

    off = orchestrator._maybe_run_function_lineage_shadow(
        "session-1", "pair-1", run_id="run-1", ai_mode="STANDARD",
        indexes={"left": [], "right": []}, sheet_relations={},
    )

    assert off is None
    assert writes == []
    assert orchestrator._function_lineage_shadow_gate(
        pair_id="pair-1", run_id="run-1", ai_mode="STANDARD"
    )["diagnostic_reason"] == "SHADOW_DISABLED"


def test_flag_on_with_empty_allowlists_makes_no_calls_or_writes(monkeypatch):
    monkeypatch.setenv("AI_FUNCTION_LINEAGE_SHADOW_ENABLED", "true")
    monkeypatch.delenv("AI_FUNCTION_LINEAGE_SHADOW_PAIR_ALLOWLIST", raising=False)
    monkeypatch.delenv("AI_FUNCTION_LINEAGE_SHADOW_RUN_ALLOWLIST", raising=False)
    monkeypatch.setattr(
        orchestrator.function_lineage_shadow,
        "run_shadow",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not run")),
    )
    writes = []
    monkeypatch.setattr(
        orchestrator.production_store,
        "save_artifact",
        lambda *_args: writes.append(_args),
    )

    result = orchestrator._maybe_run_function_lineage_shadow(
        "session-1", "pair-1", run_id="run-1", ai_mode="STANDARD",
        indexes={"left": [], "right": []}, sheet_relations={},
    )

    assert result is None
    assert writes == []
    assert orchestrator._function_lineage_shadow_gate(
        pair_id="pair-1", run_id="run-1", ai_mode="STANDARD"
    )["diagnostic_reason"] == "SHADOW_DISABLED"


def test_disallowed_pair_makes_no_calls(monkeypatch):
    monkeypatch.setenv("AI_FUNCTION_LINEAGE_SHADOW_ENABLED", "true")
    monkeypatch.setenv(
        "AI_FUNCTION_LINEAGE_SHADOW_PAIR_ALLOWLIST", "pair-other, pair-second"
    )
    monkeypatch.delenv("AI_FUNCTION_LINEAGE_SHADOW_RUN_ALLOWLIST", raising=False)
    monkeypatch.setattr(
        orchestrator.function_lineage_shadow,
        "run_shadow",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not run")),
    )

    result = orchestrator._maybe_run_function_lineage_shadow(
        "session-1", "pair-1", run_id="run-1", ai_mode="STANDARD",
        indexes={"left": [], "right": []}, sheet_relations={},
    )

    assert result is None
    assert orchestrator._function_lineage_shadow_gate(
        pair_id="pair-1", run_id="run-1", ai_mode="STANDARD"
    )["diagnostic_reason"] == "PAIR_NOT_ALLOWED"


def test_disallowed_run_makes_no_calls_and_records_reason(monkeypatch):
    monkeypatch.setenv("AI_FUNCTION_LINEAGE_SHADOW_ENABLED", "true")
    monkeypatch.delenv("AI_FUNCTION_LINEAGE_SHADOW_PAIR_ALLOWLIST", raising=False)
    monkeypatch.setenv("AI_FUNCTION_LINEAGE_SHADOW_RUN_ALLOWLIST", "run-other")
    monkeypatch.setattr(
        orchestrator.function_lineage_shadow,
        "run_shadow",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not run")),
    )

    result = orchestrator._maybe_run_function_lineage_shadow(
        "session-1", "pair-1", run_id="run-1", ai_mode="STANDARD",
        indexes={"left": [], "right": []}, sheet_relations={},
    )

    assert result is None
    assert orchestrator._function_lineage_shadow_gate(
        pair_id="pair-1", run_id="run-1", ai_mode="STANDARD"
    )["diagnostic_reason"] == "RUN_NOT_ALLOWED"


@pytest.mark.parametrize("ai_mode", ["FAST", "DEEP"])
def test_fast_and_deep_never_run_shadow(monkeypatch, ai_mode):
    monkeypatch.setenv("AI_FUNCTION_LINEAGE_SHADOW_ENABLED", "true")
    monkeypatch.setenv("AI_FUNCTION_LINEAGE_SHADOW_PAIR_ALLOWLIST", "pair-1")
    monkeypatch.setenv("AI_FUNCTION_LINEAGE_SHADOW_RUN_ALLOWLIST", "run-1")
    monkeypatch.setattr(
        orchestrator.function_lineage_shadow,
        "run_shadow",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not run")),
    )

    result = orchestrator._maybe_run_function_lineage_shadow(
        "session-1", "pair-1", run_id="run-1", ai_mode=ai_mode,
        indexes={"left": [], "right": []}, sheet_relations={},
    )

    assert result is None
    assert orchestrator._function_lineage_shadow_gate(
        pair_id="pair-1", run_id="run-1", ai_mode=ai_mode
    )["diagnostic_reason"] == "SHADOW_DISABLED"


def test_allowed_pair_runs_shadow_and_persists_three_artifacts(monkeypatch):
    monkeypatch.setenv("AI_FUNCTION_LINEAGE_SHADOW_ENABLED", "true")
    monkeypatch.setenv("AI_FUNCTION_LINEAGE_SHADOW_PAIR_ALLOWLIST", "pair-1")
    monkeypatch.delenv("AI_FUNCTION_LINEAGE_SHADOW_RUN_ALLOWLIST", raising=False)
    artifacts = lineage.failure_artifacts(
        pair_id="pair-1", run_id="run-1", reason_code="TEST"
    )
    calls = []
    writes = []
    monkeypatch.setattr(
        orchestrator.function_lineage_shadow,
        "run_shadow",
        lambda **kwargs: calls.append(kwargs) or artifacts,
    )
    monkeypatch.setattr(
        orchestrator.production_store,
        "save_artifact",
        lambda _session, _pair, name, payload: writes.append((name, payload)),
    )

    result = orchestrator._maybe_run_function_lineage_shadow(
        "session-1", "pair-1", run_id="run-1", ai_mode="STANDARD",
        indexes={"left": [], "right": []}, sheet_relations={},
    )

    assert len(calls) == 1
    assert {name for name, _payload in writes} == {
        "document_link_map", "function_lineage_map", "derived_sheet_map",
    }
    assert result["shadow_status"] == "FAILED"
    assert result["diagnostic_reason"] == "SHADOW_FAILED"


def test_allowed_run_runs_shadow(monkeypatch):
    monkeypatch.setenv("AI_FUNCTION_LINEAGE_SHADOW_ENABLED", "true")
    monkeypatch.setenv("AI_FUNCTION_LINEAGE_SHADOW_PAIR_ALLOWLIST", "pair-other")
    monkeypatch.setenv("AI_FUNCTION_LINEAGE_SHADOW_RUN_ALLOWLIST", "run-1")
    artifacts = lineage.failure_artifacts(
        pair_id="pair-1", run_id="run-1", reason_code="TEST"
    )
    artifacts["function_lineage_map"]["shadow_status"] = "COMPLETED"
    calls = []
    monkeypatch.setattr(
        orchestrator.function_lineage_shadow,
        "run_shadow",
        lambda **kwargs: calls.append(kwargs) or artifacts,
    )
    monkeypatch.setattr(
        orchestrator.production_store,
        "save_artifact",
        lambda *_args: None,
    )

    result = orchestrator._maybe_run_function_lineage_shadow(
        "session-1", "pair-1", run_id="run-1", ai_mode="STANDARD",
        indexes={"left": [], "right": []}, sheet_relations={},
    )

    assert len(calls) == 1
    assert result["diagnostic_reason"] == "SHADOW_EXECUTED"
    gate = orchestrator._function_lineage_shadow_gate(
        pair_id="pair-1", run_id="run-1", ai_mode="STANDARD"
    )
    assert gate["allowed"] is True
    assert gate["pair_allowed"] is False
    assert gate["run_allowed"] is True


def test_shadow_exception_does_not_escape_main_pipeline_helper(monkeypatch):
    monkeypatch.setenv("AI_FUNCTION_LINEAGE_SHADOW_ENABLED", "true")
    monkeypatch.setenv("AI_FUNCTION_LINEAGE_SHADOW_PAIR_ALLOWLIST", "pair-1")
    writes = {}
    monkeypatch.setattr(
        orchestrator.function_lineage_shadow,
        "run_shadow",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("/home/secret")),
    )
    monkeypatch.setattr(
        orchestrator.production_store,
        "save_artifact",
        lambda _session, _pair, name, payload: writes.setdefault(name, payload),
    )

    result = orchestrator._maybe_run_function_lineage_shadow(
        "session-1", "pair-1", run_id="run-1", ai_mode="STANDARD",
        indexes={"left": [], "right": []}, sheet_relations={},
    )

    assert result["shadow_status"] == "FAILED"
    assert result["diagnostic_reason"] == "SHADOW_FAILED"
    assert result["reason_code"] == "RuntimeError"
    assert "/home/secret" not in json.dumps(writes)


def test_standard_pipeline_runs_shadow_before_content_without_materializing(
    tmp_path, monkeypatch,
):
    from tests.test_stage_comparison_production_orchestrator import (
        _atom,
        _install_run_fakes,
    )

    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[_atom("text-voltage", "TEXT")],
        graphic_atoms=[],
    )
    monkeypatch.setenv("AI_FUNCTION_LINEAGE_SHADOW_ENABLED", "true")
    monkeypatch.setenv("AI_FUNCTION_LINEAGE_SHADOW_PAIR_ALLOWLIST", "pair-1")
    order = []
    text_branch = orchestrator._run_text_branch

    def run_shadow(**kwargs):
        order.append("shadow")
        artifacts = lineage.failure_artifacts(
            pair_id=kwargs["pair_id"],
            run_id=kwargs["run_id"],
            reason_code="TEST_SHADOW",
        )
        artifacts["function_lineage_map"]["shadow_status"] = "COMPLETED"
        return artifacts

    def run_text(*args, **kwargs):
        order.append("content")
        return text_branch(*args, **kwargs)

    monkeypatch.setattr(orchestrator.function_lineage_shadow, "run_shadow", run_shadow)
    monkeypatch.setattr(orchestrator, "_run_text_branch", run_text)

    state = orchestrator.run_production_comparison(
        "session-1",
        "pair-1",
        input_mode="PAGE",
        left_pages=[1],
        right_pages=[1],
        ai_mode="STANDARD",
    )

    assert order[:2] == ["shadow", "content"]
    assert "function_lineage_shadow" not in state["stages"]
    assert orchestrator.production_store.load_artifact(
        "session-1", "pair-1", "function_lineage_map"
    )["run_id"] == state["run_id"]
    synthesis = orchestrator.production_store.load_artifact(
        "session-1", "pair-1", "unified_synthesis"
    )
    assert "function_lineage" not in json.dumps(synthesis).lower()
    assert state["function_lineage_shadow"]["diagnostic_reason"] == (
        "SHADOW_EXECUTED"
    )
    assert state["function_lineage_shadow"]["executed"] is True


def test_disallowed_pair_keeps_main_pipeline_unchanged_and_saves_reason(
    tmp_path, monkeypatch,
):
    from tests.test_stage_comparison_production_orchestrator import (
        _atom,
        _install_run_fakes,
    )

    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[_atom("text-voltage", "TEXT")],
        graphic_atoms=[],
    )
    monkeypatch.setenv("AI_FUNCTION_LINEAGE_SHADOW_ENABLED", "true")
    monkeypatch.setenv(
        "AI_FUNCTION_LINEAGE_SHADOW_PAIR_ALLOWLIST", "pair-other"
    )
    monkeypatch.delenv("AI_FUNCTION_LINEAGE_SHADOW_RUN_ALLOWLIST", raising=False)
    monkeypatch.setattr(
        orchestrator.function_lineage_shadow,
        "run_shadow",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not run")),
    )

    state = orchestrator.run_production_comparison(
        "session-1",
        "pair-1",
        input_mode="PAGE",
        left_pages=[1],
        right_pages=[1],
        ai_mode="STANDARD",
    )

    diagnostic = state["function_lineage_shadow"]
    assert diagnostic["diagnostic_reason"] == "PAIR_NOT_ALLOWED"
    assert diagnostic["executed"] is False
    assert state["status"] in {"COMPLETED", "PARTIAL"}
    assert orchestrator.production_store.load_artifact(
        "session-1", "pair-1", "function_lineage_map"
    ) is None
    synthesis = orchestrator.production_store.load_artifact(
        "session-1", "pair-1", "unified_synthesis"
    )
    assert "function_lineage" not in json.dumps(synthesis).lower()


def test_sheet_diagnostic_export_contains_run_bound_shadow_and_redacts_paths(monkeypatch):
    state = {
        "run_id": "run-1",
        "status": "COMPLETED",
        "selection": {"input_mode": "DOCUMENT", "ai_mode": "STANDARD"},
        "analysis_config": {"ai_mode": "STANDARD"},
        "stages": {
            "sheet_matching": {"status": "COMPLETED"},
            "sheet_scope": {"status": "COMPLETED"},
        },
    }
    artifacts = {
        "state": state,
        "sheet_relations": {"kind": "sheet", "schema_version": "v1"},
        "document_link_map": {
            "run_id": "run-1", "kind": "document_link_map", "links": [],
        },
        "function_lineage_map": {
            "run_id": "run-1", "kind": "function_lineage_map",
            "shadow_status": "COMPLETED", "stable_lineages": [],
            "unresolved_lineages": [{"reason_code": "NEED_MORE_EVIDENCE"}],
            "verifier_result": {"status": "PASSED"},
            "diagnostic_path": "/home/coder/private/model.json",
        },
        "derived_sheet_map": {
            "run_id": "run-1", "kind": "derived_sheet_map", "relations": [],
        },
    }
    monkeypatch.setattr(
        orchestrator.store,
        "get_pair_for_production",
        lambda *_args: {
            "id": "pair-1",
            "left": {"filename": "left.pdf"},
            "right": {"filename": "right.pdf"},
        },
    )
    monkeypatch.setattr(
        orchestrator.production_store,
        "load_artifact",
        lambda _session, _pair, name: artifacts.get(name),
    )

    result = orchestrator.get_production_stage_result(
        "session-1", "pair-1", "run-1", "sheets"
    )

    shadow = result["outputs"]["function_lineage_shadow"]
    assert set(shadow) == {
        "document_link_map", "function_lineage_map", "derived_sheet_map",
    }
    assert shadow["function_lineage_map"]["shadow_status"] == "COMPLETED"
    serialized = json.dumps(result, ensure_ascii=False)
    assert "/home/coder" not in serialized
    assert "NEED_MORE_EVIDENCE" in serialized


def test_flag_off_export_does_not_surface_stale_shadow(monkeypatch):
    state = {
        "run_id": "run-new",
        "status": "COMPLETED",
        "selection": {"input_mode": "DOCUMENT", "ai_mode": "FAST"},
        "stages": {"sheet_matching": {"status": "COMPLETED"}},
    }
    artifacts = {
        "state": state,
        "sheet_relations": {},
        "function_lineage_map": {"run_id": "run-old", "shadow_status": "COMPLETED"},
    }
    monkeypatch.setattr(
        orchestrator.store,
        "get_pair_for_production",
        lambda *_args: {"id": "pair-1", "left": {}, "right": {}},
    )
    monkeypatch.setattr(
        orchestrator.production_store,
        "load_artifact",
        lambda _session, _pair, name: artifacts.get(name),
    )

    result = orchestrator.get_production_stage_result(
        "session-1", "pair-1", "run-new", "sheets"
    )

    assert "function_lineage_shadow" not in result["outputs"]

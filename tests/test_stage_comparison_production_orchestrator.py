from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import (
    entity_matcher,
    paths,
    production_orchestrator as orchestrator,
    production_store,
)
from backend.app.services.stage_comparison.sheet_matcher import match_sheets


def _pair(tmp_path: Path, pair_id: str = "pair-1") -> dict:
    return {
        "id": pair_id,
        "left": {
            "pdf_path": str(tmp_path / "left.pdf"),
            "md_path": str(tmp_path / "left.md"),
            "document_code": "LEFT-DOC",
            "version_id": "left-v1",
        },
        "right": {
            "pdf_path": str(tmp_path / "right.pdf"),
            "md_path": str(tmp_path / "right.md"),
            "document_code": "RIGHT-DOC",
            "version_id": "right-v1",
        },
    }


def _atom(atom_id: str, source: str, *, project_ref: str = "project:panel") -> dict:
    return {
        "atom_id": atom_id,
        "source": source,
        "scope_ref": "scope:selected",
        "subject_ref": "equipment:panel",
        "project_entity_ref": project_ref,
        "facet_ref": "voltage",
        "dimension": "PARAMETER",
        "direction": "ALTERED",
        "outcome": "MATERIAL_CHANGE",
        "confidence": "HIGH",
        "before_value": "220 V",
        "after_value": "380 V",
        "evidence_ref": f"evidence:{atom_id}",
        "source_artifact": {
            "kind": "stage_comparison_text_differences"
            if source == "TEXT"
            else "graphic_change_ledger",
            "schema_version": "1",
            "artifact_ref": f"artifact:{source.lower()}",
        },
        "provenance": {
            "producer": "test",
            "locations": {
                "LEFT": [{"page": 1, "fragment_id": "left-f", "bboxes": [[.1, .2, .3, .4]]}],
                "RIGHT": [{"page": 1, "fragment_id": "right-f", "bboxes": [[.2, .3, .4, .5]]}],
            },
        },
    }


def _unlocated_atom(atom_id: str, source: str, *, project_ref: str | None = None) -> dict:
    """A review item with nothing to open: values but no place on a sheet.

    This is the only kind of TEXT review item that still owes the engineer a
    Stage 5 question.  An item that does have a location is shown in Stage 7
    as a finding and confirmed there, once.
    """
    atom = _atom(atom_id, source, project_ref=project_ref)
    atom["provenance"] = {
        **atom["provenance"],
        "locations": {"LEFT": [], "RIGHT": []},
    }
    return atom


def _type_atom(atom_id: str, source: str) -> dict:
    atom = _atom(atom_id, source)
    atom.update({
        "facet_ref": "device_type",
        "dimension": "TYPE",
        "direction": "REPLACED",
        "before_value": "QS1",
        "after_value": "QF3",
    })
    return atom


def _text_artifacts(atoms: list[dict]) -> tuple[dict, dict, dict, dict, dict]:
    preparation = {
        "kind": "stage_comparison_text_preparation",
        "schema_version": "text-preparation.v1",
        "input_signature": "preparation-input",
    }
    differences = {
        "kind": "stage_comparison_text_differences",
        "version": 1,
        "source_signature": "stage3-input",
        "sheet_groups": [],
    }
    fact_production = {
        "kind": "stage_comparison_text_fact_production",
        "schema_version": "text-fact-production.v1",
        "input_signature": "fact-production-input",
        "facts": [
            {"fact_id": f"fact:{atom['atom_id']}", "outcome": atom["outcome"]}
            for atom in atoms
        ],
        "not_applicable_source_evidence": [],
        "unresolved_source_evidence": [],
    }
    semantic = {
        "kind": "stage_comparison_text_semantic_validation",
        "schema_version": "text-semantic-validation.v1",
        "input_signature": "semantic-input",
        "stage3_signature": "stage3-input",
        "text_fact_production_signature": "fact-production-input",
        "facts": [],
    }
    atom_artifact = {
        "kind": "stage_comparison_text_atoms",
        "schema_version": "text-atoms.v1",
        "input_signature": "atom-input",
        "atoms": atoms,
    }
    return preparation, differences, fact_production, semantic, atom_artifact


def _candidate(text_id: str, graphic_id: str) -> dict:
    return {
        "candidate_id": "candidate-1",
        "text_atom_id": text_id,
        "graphic_atom_id": graphic_id,
        "subject_relation": "SAME_ENTITY",
        "links_by_side": {
            "LEFT": {"relation": "SAME_ENTITY", "confidence": "HIGH"},
            "RIGHT": {"relation": "SAME_ENTITY", "confidence": "HIGH"},
        },
        "source_valid": True,
        "coverage_by_side": {"LEFT": "CHECKED", "RIGHT": "CHECKED"},
        "document_binding_state": "DOCUMENT_BINDING_PROVEN",
        "text_count": 1,
        "graphic_count": 1,
        "subject_identity_provenance": {"producer": "test-entity-proof"},
    }


def _graphic_ledger(change_id: str, left_page: int, right_page: int) -> dict:
    return {
        "schema_version": "graphic-change-ledger.v1",
        "comparison_scope": {
            "left_blocks": [{
                "block_id": f"left-{left_page}",
                "page_index": left_page - 1,
                "block_type": "image",
                "bbox_visual_pt": [0, 0, 100, 100],
            }],
            "right_blocks": [{
                "block_id": f"right-{right_page}",
                "page_index": right_page - 1,
                "block_type": "image",
                "bbox_visual_pt": [0, 0, 100, 100],
            }],
        },
        "route": "MODE_1_APPLICABLE",
        "mode": "MODE_1",
        "policy": {"version": "test"},
        "quality": {},
        "changes": [{
            "change_id": change_id,
            "type": "GEOMETRY_CHANGED",
            "left_region": {
                "block_id": f"left-{left_page}",
                "page_index": left_page - 1,
                "bbox_visual_pt": [10, 10, 20, 20],
            },
            "right_region": {
                "block_id": f"right-{right_page}",
                "page_index": right_page - 1,
                "bbox_visual_pt": [11, 11, 21, 21],
            },
            "evidence": [{"kind": "VECTOR_LOCAL_DIFF"}],
            "address_hints": [],
            "confidence": "HIGH",
            "provenance": ["VECTOR"],
        }],
        "diagnostics": {},
    }


def _empty_routed_ledger(route: str, mode: str | None = None) -> dict:
    ledger = _graphic_ledger("unused", 10, 24)
    ledger.update({
        "route": route,
        "mode": mode,
        "changes": [],
        "diagnostics": {"routing": {"reason_code": f"reason-{route}"}},
    })
    return ledger


def _install_run_fakes(
    monkeypatch,
    tmp_path: Path,
    *,
    text_atoms: list[dict],
    graphic_atoms: list[dict],
    graphic_ledger: dict | None = None,
    sheet_relations: dict | None = None,
    capture: dict | None = None,
) -> dict:
    pair = _pair(tmp_path)
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    monkeypatch.setattr(orchestrator.store, "get_pair_for_production", lambda *_: pair)
    monkeypatch.setattr(orchestrator, "_validate_page_bounds", lambda *_: None)
    relations = sheet_relations or match_sheets([], [], generated_at="fixed")
    monkeypatch.setattr(
        orchestrator,
        "_run_sheet_matcher",
        lambda _pair: (relations, {"left": [], "right": []}),
    )

    def text_branch(
        _pair,
        _pair_id,
        groups,
        _indexes,
        _existing,
        *,
        document_cache_dir=None,
    ):
        if capture is not None:
            capture.setdefault("document_cache_dirs", []).append(
                document_cache_dir
            )
        if capture is not None:
            captured = [
                {
                    **dict(group),
                    "left_pages": list(group.get("left_pages") or []),
                    "right_pages": list(group.get("right_pages") or []),
                }
                for group in groups
            ]
            capture["groups"] = captured
            capture.setdefault("groups_history", []).append(captured)
        return _text_artifacts(text_atoms)

    monkeypatch.setattr(orchestrator, "_run_text_branch", text_branch)
    ledger = (
        graphic_ledger
        if graphic_atoms and graphic_ledger is not None
        else {"changes": [{"change_id": "graphic-change"}]}
        if graphic_atoms
        else None
    )
    def graphic_branch(_session, _pair_id, _pair, _request, groups):
        if capture is not None:
            captured = [
                {
                    **dict(group),
                    "left_pages": list(group.get("left_pages") or []),
                    "right_pages": list(group.get("right_pages") or []),
                }
                for group in groups
            ]
            capture.setdefault("graphic_groups_history", []).append(captured)
        return (
            ledger,
            {
                "status": "COMPLETED" if graphic_atoms else "NOT_APPLICABLE",
                "source_state": "VALID" if graphic_atoms else "NOT_APPLICABLE",
                "changes": len(graphic_atoms),
                "mode": "MODE_2" if graphic_atoms else None,
            },
        )

    monkeypatch.setattr(orchestrator, "_run_graphic_branch", graphic_branch)
    monkeypatch.setattr(
        orchestrator,
        "ledger_to_graphic_atoms",
        lambda _ledger: {"atoms": graphic_atoms},
    )
    empty_entities = entity_matcher.match_entities([], [], generated_at="fixed")
    monkeypatch.setattr(orchestrator, "_run_entity_matcher", lambda *_: empty_entities)
    monkeypatch.setattr(
        orchestrator,
        "_build_synthesis_candidates",
        lambda text, graphic, _entities, **_gate_facts: (
            [_candidate(text[0]["atom_id"], graphic[0]["atom_id"])]
            if text and graphic
            else []
        ),
    )
    return pair


def _run(pair_id: str = "pair-1", *, input_mode: str = "PAGE") -> dict:
    kwargs = {
        "input_mode": input_mode,
        "left_block_ids": [],
        "right_block_ids": [],
    }
    if input_mode == "PAGE":
        kwargs.update(left_pages=[1], right_pages=[1])
    return orchestrator.run_production_comparison("session-1", pair_id, **kwargs)


def test_text_branch_runs_fact_producer_before_closed_semantic_validation(
    tmp_path, monkeypatch
):
    calls = []
    preparation = {"kind": "preparation", "input_signature": "prep-signature"}
    differences = {"kind": "differences"}
    fact_production = {
        "input_signature": "facts-signature",
        "facts": [{"fact_id": "fact-1"}],
        "not_applicable_source_evidence": [{"source_evidence_ref": "n-a"}],
    }

    def prepare(*_args, **options):
        calls.append(("prepare", options.get("document_cache_dir")))
        return preparation

    def build_differences(actual):
        assert actual is preparation
        calls.append(("diff", None))
        return differences

    def produce(actual_differences, actual_preparation):
        assert actual_differences is differences
        assert actual_preparation is preparation
        calls.append(("facts", None))
        return fact_production

    def validate(actual_differences, facts, **options):
        assert actual_differences is differences
        assert facts == fact_production["facts"]
        assert options["not_applicable_source_evidence"] == (
            fact_production["not_applicable_source_evidence"]
        )
        calls.append(("semantic", None))
        return {
            "kind": orchestrator.SEMANTIC_KIND,
            "schema_version": orchestrator.SEMANTIC_SCHEMA_VERSION,
            "stage3_signature": "stage3-signature",
            "input_signature": "semantic-signature",
            "facts": [],
            "provenance": {"producer": "closed-validator"},
        }

    def atoms(actual_differences, semantic, **_options):
        assert actual_differences is differences
        assert semantic["text_fact_production_signature"] == "facts-signature"
        calls.append(("atoms", None))
        return {"atoms": [], "input_signature": "atoms-signature"}

    monkeypatch.setattr(orchestrator, "prepare_text_scope", prepare)
    monkeypatch.setattr(
        orchestrator, "build_text_differences_from_preparation", build_differences
    )
    monkeypatch.setattr(orchestrator, "produce_text_facts", produce)
    monkeypatch.setattr(
        orchestrator, "stage3_content_signature", lambda _value: "stage3-signature"
    )
    monkeypatch.setattr(orchestrator, "build_semantic_validation", validate)
    monkeypatch.setattr(orchestrator, "build_text_atoms", atoms)

    cache_dir = tmp_path / "document-cache"
    first = orchestrator._run_text_branch(
        {},
        "pair-1",
        [],
        {},
        {
            "kind": orchestrator.SEMANTIC_KIND,
            "schema_version": orchestrator.SEMANTIC_SCHEMA_VERSION,
            "stage3_signature": "stage3-signature",
            "text_fact_production_signature": "old-facts",
        },
        document_cache_dir=cache_dir,
    )
    second = orchestrator._run_text_branch(
        {}, "pair-1", [], {}, first[3], document_cache_dir=cache_dir
    )

    assert first[2] is fact_production
    assert second[3] == first[3]
    assert [name for name, _value in calls] == [
        "prepare", "diff", "facts", "semantic", "atoms",
        "prepare", "diff", "facts", "atoms",
    ]
    assert calls[0][1] == cache_dir


def test_text_stage_does_not_mark_unresolved_placeholder_as_valid():
    stage = orchestrator._text_stage_summary(
        {
            "comparison_groups": [{"id": "scope"}],
            "fragments": {"left": [], "right": [{}]},
            "input_signature": "preparation",
        },
        {
            "summary": {"added": 1},
            "source_signature": "differences",
        },
        {
            "facts": [],
            "not_applicable_source_evidence": [],
            "unresolved_source_evidence": ["source-1"],
            "input_signature": "fact-production",
        },
        {
            "diagnostics": {"facts": 0, "unresolved_source_evidence": 1},
            "input_signature": "semantic",
        },
        {
            "atoms": [{"atom_id": "placeholder", "review_status": "REVIEW_REQUIRED"}],
            "input_signature": "atoms",
        },
    )

    assert stage["source_state"] == "REVIEW_REQUIRED"
    assert stage["deltas"] == 1
    assert stage["automatic_atoms"] == 0
    assert stage["review_required"] == 1
    assert stage["reason_counts"] == {"unresolved_text_structure": 1}


def _split_sheet_relations() -> dict:
    artifact = match_sheets(
        [{
            "pdf_page": 10,
            "comparison_group_ref": "panel",
            "functional_content": ["distribution"],
            "main_entities": ["panel"],
            "topology": ["input -> panel"],
        }],
        [
            {
                "pdf_page": 24,
                "comparison_group_ref": "panel",
                "functional_content": ["distribution"],
                "main_entities": ["panel"],
                "topology": ["input -> panel"],
            },
            {
                "pdf_page": 25,
                "comparison_group_ref": "panel",
                "functional_content": ["distribution"],
                "main_entities": ["panel"],
                "topology": ["input -> panel"],
            },
        ],
        generated_at="fixed",
    )
    # Scope-answer tests exercise a genuinely ambiguous grouped relation.
    # HIGH grouped relations are already proven and must not be asked again.
    relation = artifact["relations"][0]
    relation["status"] = "POSSIBLE"
    relation["confidence"] = 0.5
    relation["supported_edges"] = []
    for edge in relation.get("candidate_edges") or []:
        edge["status"] = "POSSIBLE"
        edge["cardinality_edge_supported"] = False
    return artifact


def test_page_runs_without_sheet_gate_and_persists_stage3(tmp_path, monkeypatch):
    capture: dict = {}
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[_atom("text-voltage", "TEXT")],
        graphic_atoms=[],
        capture=capture,
    )

    state = _run()

    assert capture["groups"] == [{
        "left_pages": [1],
        "right_pages": [1],
        "relation_type": "USER_SELECTED",
    }]
    assert state["constraints"]["sheet_matcher_is_page_gate"] is False
    assert state["constraints"]["parent_relation_required"] is False
    assert paths.production_text_differences_path("session-1", "pair-1").is_file()
    assert paths.production_text_fact_production_path(
        "session-1", "pair-1"
    ).is_file()
    assert capture["document_cache_dirs"] == [None]
    assert state["stages"]["text"]["automatic_atoms"] == 1
    assert state["stages"]["text"]["review_required"] == 0
    assert state["stages"]["text"]["preparation"]["status"] == "COMPLETED"
    assert state["stages"]["text"]["deterministic_diff"]["status"] == (
        "COMPLETED"
    )
    assert state["stages"]["text"]["semantic_validation"]["status"] == (
        "COMPLETED"
    )
    assert state["stages"]["text"]["text_atoms"]["atoms"] == 1
    assert not paths.high_level_project_changes_path("session-1", "pair-1").exists()
    assert not paths.project_change_summary_path("session-1", "pair-1").exists()


def test_page_suggestion_has_persistent_question_action(tmp_path, monkeypatch):
    relations = match_sheets(
        [{
            "pdf_page": 1,
            "functional_content": ["distribution"],
            "main_entities": ["panel"],
            "topology": ["input -> panel"],
        }],
        [{
            "pdf_page": 2,
            "functional_content": ["distribution"],
            "main_entities": ["panel"],
            "topology": ["input -> panel"],
        }],
        generated_at="fixed",
    )
    capture: dict = {}
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[],
        graphic_atoms=[],
        sheet_relations=relations,
        capture=capture,
    )

    state = _run()
    suggestion = state["sheet_suggestions"]["suggestions"][0]
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    question = next(
        item for item in queue["questions"]
        if item["question_id"] == suggestion["question_id"]
    )
    assert {item["code"] for item in question["answer_options"]} == {
        "COMPARE_ADDITIONALLY", "REPLACE", "ADD_TO_GROUP", "IGNORE",
    }

    updated = orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{
            "question_id": suggestion["question_id"],
            "answer": "IGNORE",
        }],
        author="server-engineer",
        expected_input_signature=queue["input_signature"],
        expected_revision=0,
    )
    reopened = orchestrator.get_review_questions("session-1", "pair-1")
    reopened_state = orchestrator.get_production_state("session-1", "pair-1")
    assert suggestion["question_id"] not in {
        item["question_id"] for item in reopened["questions"]
    }
    assert reopened["application"]["applied_decision_ids"]
    assert updated["suggestion_actions"] == {
        suggestion["suggestion_id"]: "IGNORE"
    }
    assert reopened["suggestion_actions"] == updated["suggestion_actions"]
    assert reopened_state["suggestion_actions"] == updated["suggestion_actions"]
    semantics = reopened["suggestion_action_semantics"]
    assert semantics["state"] == "IGNORED"
    assert semantics["scope_applied"] is False
    assert semantics["pipeline_rerun"] is False
    assert semantics["generation_run_id"] == state["run_id"]
    assert semantics["effective_page_groups"] == capture["groups"]
    assert semantics["outcomes"][0]["suggestion_id"] == suggestion["suggestion_id"]
    assert semantics["outcomes"][0]["action"] == "IGNORE"
    assert semantics["outcomes"][0]["state"] == "IGNORED"
    assert semantics["outcomes"][0]["scope_applied"] is False
    assert semantics["outcomes"][0]["pipeline_rerun"] is False
    assert len(capture["groups_history"]) == 1


@pytest.mark.parametrize(
    ("action", "expected_pages"),
    [
        ("REPLACE", [([1], [2])]),
        ("COMPARE_ADDITIONALLY", [([1], [1]), ([1], [2])]),
        ("ADD_TO_GROUP", [([1], [1, 2])]),
    ],
)
def test_page_materializing_action_reruns_exact_groups_and_publishes_generation(
    tmp_path, monkeypatch, action, expected_pages
):
    relations = match_sheets(
        [{
            "pdf_page": 1,
            "functional_content": ["distribution"],
            "main_entities": ["panel"],
            "topology": ["input -> panel"],
        }],
        [{
            "pdf_page": 2,
            "functional_content": ["distribution"],
            "main_entities": ["panel"],
            "topology": ["input -> panel"],
        }],
        generated_at="fixed",
    )
    capture: dict = {}
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[],
        graphic_atoms=[],
        sheet_relations=relations,
        capture=capture,
    )
    initial = _run()
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    question = next(
        item for item in queue["questions"]
        if item["question_type"] == "PAGE_SUGGESTION_ACTION"
    )

    updated = orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{"question_id": question["question_id"], "answer": action}],
        author="server-engineer",
        expected_input_signature=queue["input_signature"],
        expected_revision=queue["revision"],
    )

    assert updated["state"]["run_id"] != initial["run_id"]
    assert updated["state"]["input_signature"] != initial["input_signature"]
    assert updated["state"]["stale"] is False
    assert len(capture["groups_history"]) == 2
    actual_pages = [
        (group["left_pages"], group["right_pages"])
        for group in capture["groups_history"][-1]
    ]
    assert actual_pages == expected_pages
    stage = updated["state"]["stages"]["sheet_scope"]
    assert stage["scope_applied"] is True
    assert stage["pipeline_rerun"] is True
    assert stage["effective_page_groups"] == capture["groups_history"][-1]
    assert updated["state"]["generation_scope"]["page_groups"] == (
        capture["groups_history"][-1]
    )
    semantics = updated["suggestion_action_semantics"]
    assert semantics["state"] == "MATERIALIZED"
    assert semantics["scope_applied"] is True
    assert semantics["pipeline_rerun"] is True
    assert semantics["generation_run_id"] == updated["state"]["run_id"]
    assert semantics["effective_page_groups"] == capture["groups_history"][-1]
    assert semantics["outcomes"][0]["action"] == action
    assert semantics["outcomes"][0]["state"] == "MATERIALIZED"
    application = production_store.load_artifact(
        "session-1", "pair-1", "review_application"
    )
    assert application["diagnostics"]["scope_applied"] is True
    assert application["diagnostics"]["pipeline_rerun"] is True
    assert application["diagnostics"]["effective_page_groups"] == (
        capture["groups_history"][-1]
    )
    snapshot = production_store.load_artifact(
        "session-1", "pair-1", "source_snapshot"
    )
    assert snapshot["run_id"] == updated["state"]["run_id"]
    assert snapshot["generation_input_signature"] == updated["state"][
        "input_signature"
    ]


def test_page_action_rerun_failure_is_failed_and_does_not_publish_answer(
    tmp_path, monkeypatch
):
    relations = match_sheets(
        [{"pdf_page": 1, "functional_content": ["panel"]}],
        [{"pdf_page": 2, "functional_content": ["panel"]}],
        generated_at="fixed",
    )
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[],
        graphic_atoms=[],
        sheet_relations=relations,
    )
    _run()
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    question = next(
        item for item in queue["questions"]
        if item["question_type"] == "PAGE_SUGGESTION_ACTION"
    )
    monkeypatch.setattr(
        orchestrator,
        "_run_entity_matcher",
        lambda *_: (_ for _ in ()).throw(RuntimeError("forced PAGE rerun failure")),
    )

    with pytest.raises(RuntimeError, match="forced PAGE rerun failure"):
        orchestrator.update_review_answers(
            "session-1",
            "pair-1",
            answers=[{
                "question_id": question["question_id"],
                "answer": "REPLACE",
            }],
            author="server-engineer",
            expected_input_signature=queue["input_signature"],
            expected_revision=queue["revision"],
        )

    state = production_store.load_artifact("session-1", "pair-1", "state")
    assert state["status"] == "FAILED"
    answers = production_store.load_artifact(
        "session-1", "pair-1", "review_answers"
    )
    assert answers is None or answers.get("revision", 0) == 0


def test_page_full_rerun_reapplies_still_current_saved_action(
    tmp_path, monkeypatch
):
    relations = match_sheets(
        [{"pdf_page": 1, "functional_content": ["panel"]}],
        [{"pdf_page": 2, "functional_content": ["panel"]}],
        generated_at="fixed",
    )
    capture: dict = {}
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[],
        graphic_atoms=[],
        sheet_relations=relations,
        capture=capture,
    )
    _run()
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    question = next(
        item for item in queue["questions"]
        if item["question_type"] == "PAGE_SUGGESTION_ACTION"
    )
    applied = orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{
            "question_id": question["question_id"],
            "answer": "REPLACE",
        }],
        author="server-engineer",
        expected_input_signature=queue["input_signature"],
        expected_revision=queue["revision"],
    )

    manual = _run()

    assert manual["run_id"] != applied["state"]["run_id"]
    assert len(capture["groups_history"]) == 3
    assert capture["groups_history"][-1][0]["left_pages"] == [1]
    assert capture["groups_history"][-1][0]["right_pages"] == [2]
    assert manual["stages"]["sheet_scope"]["scope_applied"] is True
    assert manual["stages"]["sheet_scope"]["pipeline_rerun"] is True
    reopened = orchestrator.get_review_questions("session-1", "pair-1")
    assert question["question_id"] not in {
        item["question_id"] for item in reopened["questions"]
    }
    assert reopened["suggestion_action_semantics"]["state"] == "MATERIALIZED"


def test_page_same_action_and_replace_to_ignore_keep_generation_truth(
    tmp_path, monkeypatch
):
    relations = match_sheets(
        [{"pdf_page": 1, "functional_content": ["panel"]}],
        [{"pdf_page": 2, "functional_content": ["panel"]}],
        generated_at="fixed",
    )
    capture: dict = {}
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[],
        graphic_atoms=[],
        sheet_relations=relations,
        capture=capture,
    )
    _run()
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    question = next(
        item for item in queue["questions"]
        if item["question_type"] == "PAGE_SUGGESTION_ACTION"
    )
    applied = orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{
            "question_id": question["question_id"],
            "answer": "REPLACE",
        }],
        author="server-engineer",
        expected_input_signature=queue["input_signature"],
        expected_revision=queue["revision"],
    )
    applied_run_id = applied["state"]["run_id"]
    reopened = orchestrator.get_review_questions("session-1", "pair-1")

    repeated = orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{
            "question_id": question["question_id"],
            "answer": "REPLACE",
        }],
        author="server-engineer",
        expected_input_signature=reopened["input_signature"],
        expected_revision=reopened["revision"],
    )

    assert repeated["state"]["run_id"] == applied_run_id
    assert len(capture["groups_history"]) == 2
    semantics = repeated["suggestion_action_semantics"]
    assert semantics["pipeline_rerun"] is True
    assert semantics["generation_was_materialized"] is True
    assert semantics["this_update_reran"] is False
    assert semantics["outcomes"][0]["scope_applied"] is True
    assert semantics["outcomes"][0]["pipeline_rerun"] is False

    reopened = orchestrator.get_review_questions("session-1", "pair-1")
    reverted = orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{
            "question_id": question["question_id"],
            "answer": "IGNORE",
        }],
        author="server-engineer",
        expected_input_signature=reopened["input_signature"],
        expected_revision=reopened["revision"],
    )

    assert reverted["state"]["run_id"] != applied_run_id
    semantics = reverted["suggestion_action_semantics"]
    assert semantics["pipeline_rerun"] is True
    assert semantics["generation_was_materialized"] is False
    assert semantics["this_update_reran"] is True
    assert semantics["outcomes"][0]["state"] == "IGNORED"
    assert semantics["outcomes"][0]["scope_applied"] is False
    assert semantics["outcomes"][0]["pipeline_rerun"] is True


def test_page_action_validates_all_materialized_page_bounds_before_updating(
    tmp_path, monkeypatch
):
    relations = match_sheets(
        [{"pdf_page": 1, "functional_content": ["panel"]}],
        [{"pdf_page": 2, "functional_content": ["panel"]}],
        generated_at="fixed",
    )
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[],
        graphic_atoms=[],
        sheet_relations=relations,
    )
    initial = _run()
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    question = next(
        item for item in queue["questions"]
        if item["question_type"] == "PAGE_SUGGESTION_ACTION"
    )

    def reject_right_page_two(_pair, _request, groups=None):
        if any(2 in group.get("right_pages", []) for group in groups or []):
            raise ValueError("right_page_out_of_range")

    monkeypatch.setattr(orchestrator, "_validate_page_bounds", reject_right_page_two)
    with pytest.raises(ValueError, match="right_page_out_of_range"):
        orchestrator.update_review_answers(
            "session-1",
            "pair-1",
            answers=[{
                "question_id": question["question_id"],
                "answer": "REPLACE",
            }],
            author="server-engineer",
            expected_input_signature=queue["input_signature"],
            expected_revision=queue["revision"],
        )

    state = orchestrator.get_production_state("session-1", "pair-1")
    assert state["run_id"] == initial["run_id"]
    assert state["status"] == initial["status"]
    assert production_store.load_artifact(
        "session-1", "pair-1", "review_answers"
    ) is None

def test_page_rejects_multiple_materializing_actions_in_one_mutation(
    tmp_path, monkeypatch
):
    relations = match_sheets([], [], generated_at="fixed")
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[],
        graphic_atoms=[],
        sheet_relations=relations,
    )
    monkeypatch.setattr(
        orchestrator,
        "page_selection_suggestions",
        lambda *_: {
            "kind": "stage_comparison_sheet_suggestions",
            "schema_version": "sheet-suggestions.v1",
            "direction": "LEFT_TO_RIGHT",
            "selected_scope": {"left_pages": [1], "right_pages": [1]},
            "selection_preserved": True,
            "sheet_matcher_is_gate": False,
            "input_signature": "two-suggestions",
            "suggestions": [
                {
                    "suggestion_id": "suggestion-a",
                    "relation_id": "relation-a",
                    "selected_left_pages": [1],
                    "selected_right_pages": [1],
                    "suggested_left_pages": [1],
                    "suggested_right_pages": [2],
                    "relation_type": "MATCHED",
                    "actions": [
                        "COMPARE_ADDITIONALLY", "REPLACE", "ADD_TO_GROUP", "IGNORE"
                    ],
                },
                {
                    "suggestion_id": "suggestion-b",
                    "relation_id": "relation-b",
                    "selected_left_pages": [1],
                    "selected_right_pages": [1],
                    "suggested_left_pages": [1],
                    "suggested_right_pages": [3],
                    "relation_type": "MATCHED",
                    "actions": [
                        "COMPARE_ADDITIONALLY", "REPLACE", "ADD_TO_GROUP", "IGNORE"
                    ],
                },
            ],
        },
    )
    initial = _run()
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    questions = [
        item for item in queue["questions"]
        if item["question_type"] == "PAGE_SUGGESTION_ACTION"
    ]
    assert len(questions) == 2

    with pytest.raises(ValueError, match="multiple materializing PAGE"):
        orchestrator.update_review_answers(
            "session-1",
            "pair-1",
            answers=[
                {"question_id": questions[0]["question_id"], "answer": "REPLACE"},
                {
                    "question_id": questions[1]["question_id"],
                    "answer": "COMPARE_ADDITIONALLY",
                },
            ],
            author="server-engineer",
            expected_input_signature=queue["input_signature"],
            expected_revision=queue["revision"],
        )

    assert orchestrator.get_production_state(
        "session-1", "pair-1"
    )["run_id"] == initial["run_id"]
    assert production_store.load_artifact(
        "session-1", "pair-1", "review_answers"
    ) is None

    accepted = orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[
            {"question_id": questions[0]["question_id"], "answer": "REPLACE"},
            {"question_id": questions[1]["question_id"], "answer": "IGNORE"},
        ],
        author="server-engineer",
        expected_input_signature=queue["input_signature"],
        expected_revision=queue["revision"],
    )
    outcomes = {
        item["action"]: item
        for item in accepted["suggestion_action_semantics"]["outcomes"]
    }
    assert outcomes["REPLACE"]["state"] == "MATERIALIZED"
    assert outcomes["REPLACE"]["scope_applied"] is True
    assert outcomes["REPLACE"]["pipeline_rerun"] is True
    assert outcomes["IGNORE"]["state"] == "IGNORED"
    assert outcomes["IGNORE"]["scope_applied"] is False
    assert outcomes["IGNORE"]["pipeline_rerun"] is False

    accepted_run_id = accepted["state"]["run_id"]
    reopened = orchestrator.get_review_questions("session-1", "pair-1")
    repeated_ignore = orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{
            "question_id": outcomes["IGNORE"]["question_id"],
            "answer": "IGNORE",
        }],
        author="server-engineer",
        expected_input_signature=reopened["input_signature"],
        expected_revision=reopened["revision"],
    )
    assert repeated_ignore["state"]["run_id"] == accepted_run_id
    semantics = repeated_ignore["suggestion_action_semantics"]
    assert semantics["pipeline_rerun"] is True
    assert semantics["this_update_reran"] is False
    repeated_outcomes = {
        item["action"]: item for item in semantics["outcomes"]
    }
    assert repeated_outcomes["REPLACE"]["pipeline_rerun"] is False
    assert repeated_outcomes["IGNORE"]["pipeline_rerun"] is False

    reopened = orchestrator.get_review_questions("session-1", "pair-1")
    restored = orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[
            {
                "question_id": outcomes["REPLACE"]["question_id"],
                "answer": "IGNORE",
            },
            {
                "question_id": outcomes["IGNORE"]["question_id"],
                "answer": "IGNORE",
            },
        ],
        author="server-engineer",
        expected_input_signature=reopened["input_signature"],
        expected_revision=reopened["revision"],
    )
    assert restored["state"]["run_id"] != accepted_run_id
    restored_outcomes = {
        item["question_id"]: item
        for item in restored["suggestion_action_semantics"]["outcomes"]
    }
    assert restored_outcomes[
        outcomes["REPLACE"]["question_id"]
    ]["pipeline_rerun"] is True
    assert restored_outcomes[
        outcomes["IGNORE"]["question_id"]
    ]["pipeline_rerun"] is False


def test_page_incomplete_suggestion_is_not_actionable():
    filtered = orchestrator._filter_page_suggestions({
        "suggestions": [
            {
                "suggestion_id": "partial",
                "suggested_left_pages": [1],
                "suggested_right_pages": [],
                "actions": ["REPLACE"],
            },
            {
                "suggestion_id": "valid",
                "suggested_left_pages": [1],
                "suggested_right_pages": [2],
                "actions": ["REPLACE"],
            },
        ],
    })

    assert [
        item["suggestion_id"] for item in filtered["suggestions"]
    ] == ["valid"]
    assert filtered["diagnostics"][
        "excluded_non_materializable_suggestions"
    ] == 1
    questions = orchestrator._sheet_suggestion_questions(filtered)
    assert len(questions) == 1
    assert questions[0]["context"]["suggestion_id"] == "valid"


def test_page_suggestion_question_is_bound_to_matcher_generation():
    suggestion = {
        "suggestion_id": "suggestion-versioned",
        "relation_id": "relation-versioned",
        "selected_left_pages": [1],
        "selected_right_pages": [1],
        "suggested_left_pages": [1],
        "suggested_right_pages": [2],
        "relation_type": "MATCHED",
        "actions": ["REPLACE", "IGNORE"],
    }
    first = orchestrator._sheet_suggestion_questions({
        "input_signature": "matcher-generation-a",
        "suggestions": [suggestion],
    })[0]
    second = orchestrator._sheet_suggestion_questions({
        "input_signature": "matcher-generation-b",
        "suggestions": [suggestion],
    })[0]

    assert first["question_id"] == second["question_id"]
    assert first["input_signature"] != second["input_signature"]


def test_document_uses_production_split_relations_as_scope(tmp_path, monkeypatch):
    capture: dict = {}
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[],
        graphic_atoms=[],
        sheet_relations=_split_sheet_relations(),
        capture=capture,
    )

    _run(input_mode="DOCUMENT")

    assert capture["groups"][0]["left_pages"] == [10]
    assert capture["groups"][0]["right_pages"] == [24, 25]
    assert capture["groups"][0]["relation_type"] == "SPLIT"
    assert len(capture["document_cache_dirs"]) == 1
    assert capture["document_cache_dirs"][0].name == "text_fragment_cache"
    stored = production_store.load_artifact("session-1", "pair-1", "sheet_relations")
    assert stored["kind"] == "stage_comparison_sheet_relations"


def test_document_sheet_select_publishes_new_generation_for_effective_subset(
    tmp_path, monkeypatch
):
    capture: dict = {}
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[_atom("text-automatic-scope", "TEXT")],
        graphic_atoms=[],
        sheet_relations=_split_sheet_relations(),
        capture=capture,
    )
    captured_text_branch = orchestrator._run_text_branch

    def scope_sensitive_text(
        pair, pair_id, groups, indexes, existing, *, document_cache_dir=None
    ):
        captured_text_branch(
            pair,
            pair_id,
            groups,
            indexes,
            existing,
            document_cache_dir=document_cache_dir,
        )
        right_pages = sorted({
            page for group in groups for page in group.get("right_pages") or []
        })
        atom_id = (
            "text-selected-right-24"
            if right_pages == [24]
            else "text-automatic-scope"
        )
        return _text_artifacts([_atom(atom_id, "TEXT")])

    monkeypatch.setattr(orchestrator, "_run_text_branch", scope_sensitive_text)
    initial_state = _run(input_mode="DOCUMENT")
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    question = next(
        item for item in queue["questions"] if item["category"] == "SHEET"
    )

    updated = orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{
            "question_id": question["question_id"],
            "answer": "SELECT_RIGHT:24",
        }],
        author="server-engineer",
        expected_input_signature=queue["input_signature"],
        expected_revision=queue["revision"],
    )

    assert updated["state"]["run_id"] != initial_state["run_id"]
    assert len(capture["groups_history"]) == 2
    assert len(capture["graphic_groups_history"]) == 2
    assert capture["groups_history"][0][0]["right_pages"] == [24, 25]
    assert capture["groups_history"][1][0]["right_pages"] == [24]
    snapshot = production_store.load_artifact(
        "session-1", "pair-1", "source_snapshot"
    )
    assert snapshot["run_id"] == updated["state"]["run_id"]
    assert snapshot["text"]["artifact"]["atoms"][0]["atom_id"] == (
        "text-selected-right-24"
    )
    automatic = production_store.load_artifact(
        "session-1", "pair-1", "sheet_relations"
    )
    application = production_store.load_artifact(
        "session-1", "pair-1", "review_application"
    )
    assert automatic["relations"][0]["right_pages"] == [24, 25]
    assert application["effective_sheet_relations"]["relations"][0][
        "right_pages"
    ] == [24]
    assert application["diagnostics"]["scope_applied"] is True
    assert application["diagnostics"]["pipeline_rerun"] is True


def test_document_sheet_valid_other_replaces_regenerated_scope(
    tmp_path, monkeypatch
):
    capture: dict = {}
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[],
        graphic_atoms=[],
        sheet_relations=_split_sheet_relations(),
        capture=capture,
    )
    _run(input_mode="DOCUMENT")
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    question = next(
        item for item in queue["questions"] if item["category"] == "SHEET"
    )

    updated = orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{
            "question_id": question["question_id"],
            "answer": "OTHER",
            "explicit_candidate": {
                "left_pages": [11],
                "right_pages": [30],
                "relation_type": "MATCHED",
            },
        }],
        author="server-engineer",
        expected_input_signature=queue["input_signature"],
        expected_revision=queue["revision"],
    )

    assert capture["groups_history"][-1][0]["left_pages"] == [11]
    assert capture["groups_history"][-1][0]["right_pages"] == [30]
    assert capture["groups_history"][-1][0]["relation_type"] == "MATCHED"
    assert updated["application"]["diagnostics"]["scope_applied"] is True
    assert updated["application"]["diagnostics"]["pipeline_rerun"] is True


def test_incomplete_sheet_reanswer_restores_automatic_scope(
    tmp_path, monkeypatch
):
    capture: dict = {}
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[],
        graphic_atoms=[],
        sheet_relations=_split_sheet_relations(),
        capture=capture,
    )
    _run(input_mode="DOCUMENT")
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    question = next(
        item for item in queue["questions"] if item["category"] == "SHEET"
    )
    orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{
            "question_id": question["question_id"],
            "answer": "SELECT_RIGHT:24",
        }],
        author="server-engineer",
        expected_input_signature=queue["input_signature"],
        expected_revision=queue["revision"],
    )
    reopened = orchestrator.get_review_questions("session-1", "pair-1")

    updated = orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{
            "question_id": question["question_id"],
            "answer": "OTHER",
        }],
        author="server-engineer",
        expected_input_signature=reopened["input_signature"],
        expected_revision=reopened["revision"],
    )

    assert capture["groups_history"][-1][0]["right_pages"] == [24, 25]
    assert question["question_id"] in {
        item["question_id"] for item in updated["questions"]
    }
    assert updated["application"]["diagnostics"]["scope_applied"] is False
    assert updated["application"]["diagnostics"]["pipeline_rerun"] is True


def test_document_sheet_no_removes_relation_from_regenerated_scope(
    tmp_path, monkeypatch
):
    capture: dict = {}
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[_atom("text-in-scope", "TEXT")],
        graphic_atoms=[],
        sheet_relations=_split_sheet_relations(),
        capture=capture,
    )
    captured_text_branch = orchestrator._run_text_branch

    def scope_sensitive_text(
        pair, pair_id, groups, indexes, existing, *, document_cache_dir=None
    ):
        captured_text_branch(
            pair,
            pair_id,
            groups,
            indexes,
            existing,
            document_cache_dir=document_cache_dir,
        )
        return _text_artifacts(
            [_atom("text-in-scope", "TEXT")] if groups else []
        )

    monkeypatch.setattr(orchestrator, "_run_text_branch", scope_sensitive_text)
    _run(input_mode="DOCUMENT")
    assert len(
        orchestrator.get_production_changes("session-1", "pair-1")["rows"]
    ) == 1
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    question = next(
        item for item in queue["questions"] if item["category"] == "SHEET"
    )

    updated = orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{"question_id": question["question_id"], "answer": "NO"}],
        author="server-engineer",
        expected_input_signature=queue["input_signature"],
        expected_revision=queue["revision"],
    )

    assert capture["groups_history"][-1] == []
    assert capture["graphic_groups_history"][-1] == []
    assert orchestrator.get_production_changes(
        "session-1", "pair-1"
    )["rows"] == []
    application = updated["application"]
    assert application["effective_sheet_relations"]["relations"][0][
        "status"
    ] == "NO_MATCH"
    assert application["diagnostics"]["scope_applied"] is True
    assert application["diagnostics"]["pipeline_rerun"] is True


def test_document_sheet_yes_same_scope_does_not_rerun_producers(
    tmp_path, monkeypatch
):
    capture: dict = {}
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[],
        graphic_atoms=[],
        sheet_relations=_split_sheet_relations(),
        capture=capture,
    )
    initial_state = _run(input_mode="DOCUMENT")
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    question = next(
        item for item in queue["questions"] if item["category"] == "SHEET"
    )

    updated = orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{"question_id": question["question_id"], "answer": "YES"}],
        author="server-engineer",
        expected_input_signature=queue["input_signature"],
        expected_revision=queue["revision"],
    )

    assert updated["state"]["run_id"] == initial_state["run_id"]
    assert len(capture["groups_history"]) == 1
    assert len(capture["graphic_groups_history"]) == 1
    assert updated["application"]["diagnostics"]["scope_applied"] is True
    assert updated["application"]["diagnostics"]["pipeline_rerun"] is False


def test_document_full_rerun_reuses_saved_sheet_scope_answer(
    tmp_path, monkeypatch
):
    capture: dict = {}
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[],
        graphic_atoms=[],
        sheet_relations=_split_sheet_relations(),
        capture=capture,
    )
    _run(input_mode="DOCUMENT")
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    question = next(
        item for item in queue["questions"] if item["category"] == "SHEET"
    )
    applied = orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{
            "question_id": question["question_id"],
            "answer": "SELECT_RIGHT:24",
        }],
        author="server-engineer",
        expected_input_signature=queue["input_signature"],
        expected_revision=queue["revision"],
    )

    manual = _run(input_mode="DOCUMENT")

    assert manual["run_id"] != applied["state"]["run_id"]
    assert len(capture["groups_history"]) == 3
    assert capture["groups_history"][1][0]["right_pages"] == [24]
    assert capture["groups_history"][2][0]["right_pages"] == [24]
    reopened = orchestrator.get_review_questions("session-1", "pair-1")
    assert question["question_id"] not in {
        item["question_id"] for item in reopened["questions"]
    }
    assert reopened["application"]["diagnostics"]["scope_applied"] is True


def test_sheet_scope_rerun_failure_is_failed_and_does_not_publish_answer(
    tmp_path, monkeypatch
):
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[],
        graphic_atoms=[],
        sheet_relations=_split_sheet_relations(),
    )
    _run(input_mode="DOCUMENT")
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    question = next(
        item for item in queue["questions"] if item["category"] == "SHEET"
    )
    monkeypatch.setattr(
        orchestrator,
        "_run_entity_matcher",
        lambda *_: (_ for _ in ()).throw(RuntimeError("forced rerun failure")),
    )

    with pytest.raises(RuntimeError, match="forced rerun failure"):
        orchestrator.update_review_answers(
            "session-1",
            "pair-1",
            answers=[{
                "question_id": question["question_id"],
                "answer": "SELECT_RIGHT:24",
            }],
            author="server-engineer",
            expected_input_signature=queue["input_signature"],
            expected_revision=queue["revision"],
        )

    assert orchestrator.get_production_state(
        "session-1", "pair-1"
    )["status"] == "FAILED"
    assert production_store.load_artifact(
        "session-1", "pair-1", "review_answers"
    ) is None


def test_fast_runs_have_unique_generation_ids_with_fixed_clock(
    tmp_path, monkeypatch
):
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[],
        graphic_atoms=[],
    )
    monkeypatch.setattr(
        orchestrator, "utc_now", lambda: "2026-08-28T12:00:00+00:00"
    )

    first = _run()
    second = _run()

    assert first["run_id"] != second["run_id"]


@pytest.mark.parametrize(
    ("text_atoms", "graphic_atoms", "expected_mode", "expected_rows"),
    [
        ([_atom("text-only", "TEXT")], [], "TEXT", 1),
        ([], [_atom("graphic-only", "GRAPHIC")], "GRAPHIC", 1),
        (
            [_type_atom("text-both", "TEXT")],
            [_type_atom("graphic-both", "GRAPHIC")],
            "BOTH",
            1,
        ),
    ],
)
def test_text_graphic_independence_and_strict_union(
    tmp_path,
    monkeypatch,
    text_atoms,
    graphic_atoms,
    expected_mode,
    expected_rows,
):
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=text_atoms,
        graphic_atoms=graphic_atoms,
    )

    _run()
    synthesis = production_store.load_artifact(
        "session-1", "pair-1", "unified_synthesis"
    )
    changes = orchestrator.get_production_changes("session-1", "pair-1")

    assert len(changes["rows"]) == expected_rows
    assert synthesis["changes"][0]["source_mode"] == expected_mode
    # The strict file is the G2.4.6 payload itself, not an orchestration wrapper.
    assert set(synthesis) == {
        "synthesis_version",
        "kind",
        "direction",
        "policy_version",
        "identity_version",
        "changes",
        "review_items",
        "contested_groups",
        "presentation_groups",
        "diagnostics",
        "source_artifacts",
        "provenance",
        "validation",
    }


def test_decisions_are_locked_optimistic_server_authored_and_build_final(
    tmp_path, monkeypatch
):
    pair = _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[_atom("text-voltage", "TEXT")],
        graphic_atoms=[],
    )
    _run()
    initial = orchestrator.get_production_changes("session-1", "pair-1")
    target_id = initial["rows"][0]["target_id"]

    updated = orchestrator.update_engineer_decisions(
        "session-1",
        "pair-1",
        updates=[{
            "target_id": target_id,
            "decision": "APPROVED",
            "author": "client-forgery",
            "comment": "checked",
        }],
        author="server-engineer",
        expected_input_signature=initial["input_signature"],
        expected_revision=initial["revision"],
    )
    report = orchestrator.get_final_report("session-1", "pair-1")

    assert updated["rows"][0]["engineer_decision"]["author"] == "server-engineer"
    assert [item["change_id"] for item in report["approved_atomic_changes"]] == [target_id]
    with pytest.raises(orchestrator.ProductionStateConflictError, match="required"):
        orchestrator.update_engineer_decisions(
            "session-1", "pair-1", updates=[], author="server-engineer"
        )
    with pytest.raises(production_store.ProductionConflictError):
        orchestrator.update_engineer_decisions(
            "session-1",
            "pair-1",
            updates=[],
            author="server-engineer",
            expected_revision=initial["revision"],
        )
    with pytest.raises(production_store.ProductionConflictError):
        orchestrator.update_engineer_decisions(
            "session-1",
            "pair-1",
            updates=[],
            author="server-engineer",
            expected_input_signature="stale-signature",
        )

    # Source signatures are checked read-only on GET and block later writes.
    Path(pair["left"]["pdf_path"]).write_bytes(b"changed")
    assert orchestrator.get_production_state("session-1", "pair-1")["stale"] is True
    with pytest.raises(orchestrator.ProductionStateConflictError):
        orchestrator.update_engineer_decisions(
            "session-1", "pair-1", updates=[], author="server-engineer"
        )


def test_question_revision_and_effective_application_survive_get_and_reanswer(
    tmp_path, monkeypatch
):
    relations = match_sheets(
        [{
            "pdf_page": 10,
            "comparison_group_ref": "panel",
            "functional_content": ["distribution"],
            "main_entities": ["panel"],
            "topology": ["input -> panel"],
        }],
        [
            {
                "pdf_page": 24,
                "comparison_group_ref": "panel",
                "functional_content": ["distribution"],
                "main_entities": ["panel"],
                "topology": ["input -> panel"],
            },
            {
                "pdf_page": 25,
                "comparison_group_ref": "panel",
                "functional_content": ["distribution"],
                "main_entities": ["panel"],
                "topology": ["input -> panel"],
            },
        ],
        generated_at="fixed",
    )
    relation = relations["relations"][0]
    relation["status"] = "POSSIBLE"
    relation["confidence"] = 0.5
    relation["supported_edges"] = []
    for edge in relation.get("candidate_edges") or []:
        edge["status"] = "POSSIBLE"
        edge["cardinality_edge_supported"] = False
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[],
        graphic_atoms=[],
        sheet_relations=relations,
    )
    _run(input_mode="DOCUMENT")
    initial = orchestrator.get_review_questions("session-1", "pair-1")
    question_id = initial["questions"][0]["question_id"]

    first = orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{"question_id": question_id, "answer": "YES"}],
        author="server-engineer",
        expected_input_signature=initial["input_signature"],
        expected_revision=0,
    )
    reopened = orchestrator.get_review_questions("session-1", "pair-1")
    second = orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{"question_id": question_id, "answer": "NO"}],
        author="server-engineer",
        expected_input_signature=reopened["input_signature"],
        expected_revision=reopened["revision"],
    )

    assert first["revision"] == 1
    assert reopened["revision"] == 1
    assert reopened["application"]["diagnostics"]["pipeline_rerun"] is False
    assert reopened["application"]["applied_decision_ids"]
    assert second["revision"] == 2
    stored = production_store.load_artifact(
        "session-1", "pair-1", "review_answers"
    )
    assert stored["decisions"][0]["author"] == "server-engineer"
    assert stored["decisions"][0]["answer"] == "NO"


def test_typed_change_answer_rebuilds_only_dependent_synthesis(tmp_path, monkeypatch):
    unresolved = _unlocated_atom("text-unresolved", "TEXT")
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[unresolved],
        graphic_atoms=[],
    )
    _run()
    before = orchestrator.get_production_changes("session-1", "pair-1")
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    question = next(
        item for item in queue["questions"] if item["category"] == "CHANGE"
    )
    assert before["rows"][0]["target_kind"] == "REVIEW_EVIDENCE"

    updated = orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{
            "question_id": question["question_id"],
            "answer": "OTHER",
            "typed_resolution": {
                "project_entity_ref": "project:panel",
            },
        }],
        author="server-engineer",
        expected_input_signature=queue["input_signature"],
        expected_revision=queue["revision"],
    )
    after = orchestrator.get_production_changes("session-1", "pair-1")
    application = production_store.load_artifact(
        "session-1", "pair-1", "review_application"
    )

    assert updated["state"]["status"] in {"COMPLETED", "PARTIAL"}
    assert len(after["rows"]) == 1
    assert after["rows"][0]["target_kind"] == "CHANGE"
    assert after["rows"][0]["change"]["project_entity_ref"] == "project:panel"
    assert application["change_resolutions"][0]["resolution"] == "TYPED_RESOLUTION"
    assert application["diagnostics"]["pipeline_rerun"] is False

    reopened = orchestrator.get_review_questions("session-1", "pair-1")
    orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{"question_id": question["question_id"], "answer": "NO"}],
        author="server-engineer",
        expected_input_signature=reopened["input_signature"],
        expected_revision=reopened["revision"],
    )
    assert orchestrator.get_production_changes("session-1", "pair-1")["rows"] == []


def test_incomplete_reanswer_rolls_effective_synthesis_back_to_automatic(
    tmp_path, monkeypatch
):
    unresolved = _unlocated_atom("text-reanswer", "TEXT")
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[unresolved],
        graphic_atoms=[],
    )
    _run()
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    question = next(
        item for item in queue["questions"] if item["category"] == "CHANGE"
    )
    orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{
            "question_id": question["question_id"],
            "answer": "OTHER",
            "typed_resolution": {
                "project_entity_ref": "project:panel",
                "outcome": "MATERIAL_CHANGE",
            },
        }],
        author="server-engineer",
        expected_input_signature=queue["input_signature"],
        expected_revision=queue["revision"],
    )
    assert orchestrator.get_production_changes(
        "session-1", "pair-1"
    )["rows"][0]["target_kind"] == "CHANGE"

    reopened = orchestrator.get_review_questions("session-1", "pair-1")
    updated = orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{
            "question_id": question["question_id"],
            "answer": "OTHER",
            "typed_resolution": {"facet_ref": "voltage"},
        }],
        author="server-engineer",
        expected_input_signature=reopened["input_signature"],
        expected_revision=reopened["revision"],
    )
    effective = production_store.load_artifact(
        "session-1", "pair-1", "unified_synthesis"
    )
    automatic = production_store.load_artifact(
        "session-1", "pair-1", "automatic_unified_synthesis"
    )

    assert orchestrator.canonical_synthesis_digest(
        effective
    ) == orchestrator.canonical_synthesis_digest(automatic)
    assert orchestrator.get_production_changes(
        "session-1", "pair-1"
    )["rows"][0]["target_kind"] == "REVIEW_EVIDENCE"
    assert question["question_id"] in {
        item["question_id"] for item in updated["questions"]
    }
    assert updated["application"]["change_resolutions"][0]["resolution"] == (
        "REVIEW_REQUIRED"
    )


def test_same_generation_answer_is_reapplied_on_full_rerun(tmp_path, monkeypatch):
    unresolved = _unlocated_atom("text-rerun", "TEXT")
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[unresolved],
        graphic_atoms=[],
    )
    _run()
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    question = next(
        item for item in queue["questions"] if item["category"] == "CHANGE"
    )
    orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{
            "question_id": question["question_id"],
            "answer": "OTHER",
            "typed_resolution": {
                "project_entity_ref": "project:panel",
                "outcome": "MATERIAL_CHANGE",
            },
        }],
        author="server-engineer",
        expected_input_signature=queue["input_signature"],
        expected_revision=queue["revision"],
    )

    _run()

    row = orchestrator.get_production_changes(
        "session-1", "pair-1"
    )["rows"][0]
    application = production_store.load_artifact(
        "session-1", "pair-1", "review_application"
    )
    reopened = orchestrator.get_review_questions("session-1", "pair-1")
    assert row["target_kind"] == "CHANGE"
    assert row["change"]["project_entity_ref"] == "project:panel"
    assert application["applied_decision_ids"]
    assert question["question_id"] not in {
        item["question_id"] for item in reopened["questions"]
    }


def test_contested_yes_materializes_only_selected_change(tmp_path, monkeypatch):
    text = _type_atom("text-contested", "TEXT")
    text["direction"] = "ADDED"
    graphic = _type_atom("graphic-contested", "GRAPHIC")
    graphic["direction"] = "REMOVED"
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[text],
        graphic_atoms=[graphic],
    )
    _run()
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    question = next(
        item
        for item in queue["questions"]
        if item["question_type"] == "CHANGE_CONTESTED"
    )
    selected = question["context"]["change_ids"][0]

    updated = orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{
            "question_id": question["question_id"],
            "answer": "YES",
            "typed_resolution": {"selected_change_ids": [selected]},
        }],
        author="server-engineer",
        expected_input_signature=queue["input_signature"],
        expected_revision=queue["revision"],
    )
    effective = production_store.load_artifact(
        "session-1", "pair-1", "unified_synthesis"
    )

    assert [item["change_id"] for item in effective["changes"]] == [selected]
    assert effective["changes"][0]["review_status"] == "CONFIRMED"
    assert effective["contested_groups"] == []
    assert updated["application"]["change_resolutions"][0]["resolution"] == (
        "CONFIRMED"
    )


def test_materialization_failure_does_not_publish_or_persist_answer(
    tmp_path, monkeypatch
):
    unresolved = _unlocated_atom("text-failed-answer", "TEXT")
    unresolved["outcome"] = "REVIEW_REQUIRED"
    unresolved["review_status"] = "REVIEW_REQUIRED"
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[unresolved],
        graphic_atoms=[],
    )
    initial_state = _run()
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    question = next(
        item for item in queue["questions"] if item["category"] == "CHANGE"
    )
    before = production_store.load_artifact(
        "session-1", "pair-1", "unified_synthesis"
    )
    def fail_materialization(*_args, **_kwargs):
        raise orchestrator.ProductionStateConflictError(
            "forced materialization failure"
        )

    monkeypatch.setattr(
        orchestrator, "_rebuild_dependent_synthesis", fail_materialization
    )
    with pytest.raises(
        orchestrator.ProductionStateConflictError,
        match="forced materialization failure",
    ):
        orchestrator.update_review_answers(
            "session-1",
            "pair-1",
            answers=[{
                "question_id": question["question_id"],
                "answer": "OTHER",
                "typed_resolution": {
                    "project_entity_ref": "project:panel",
                    "outcome": "MATERIAL_CHANGE",
                },
            }],
            author="server-engineer",
            expected_input_signature=queue["input_signature"],
            expected_revision=queue["revision"],
        )

    assert production_store.load_artifact(
        "session-1", "pair-1", "review_answers"
    ) is None
    assert production_store.load_artifact(
        "session-1", "pair-1", "unified_synthesis"
    ) == before
    assert orchestrator.get_production_state(
        "session-1", "pair-1"
    )["status"] == initial_state["status"]
    assert question["question_id"] in {
        item["question_id"]
        for item in orchestrator.get_review_questions(
            "session-1", "pair-1"
        )["questions"]
    }


def test_entity_answer_rebinds_atoms_and_recomputes_g246_only(tmp_path, monkeypatch):
    text = _type_atom("text-type", "TEXT")
    text["project_entity_ref"] = None
    text["subject_ref"] = "left:panel"
    graphic = _type_atom("graphic-type", "GRAPHIC")
    graphic["project_entity_ref"] = None
    graphic["subject_ref"] = "right:panel"
    actual_candidate_builder = orchestrator._build_synthesis_candidates
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[text],
        graphic_atoms=[graphic],
    )
    possible = entity_matcher.match_entities(
        [{
            "entity_ref": "left:panel",
            "functional_role": ["DISTRIBUTION"],
            "upstream": ["source:1"],
        }],
        [{
            "entity_ref": "right:panel",
            "functional_role": ["DISTRIBUTION"],
            "upstream": ["source:1"],
        }],
        generated_at="fixed",
    )
    assert possible["relations"][0]["relation"] == "POSSIBLE_ENTITY"
    monkeypatch.setattr(orchestrator, "_run_entity_matcher", lambda *_: possible)
    monkeypatch.setattr(
        orchestrator, "_build_synthesis_candidates", actual_candidate_builder
    )
    _run()
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    question = next(
        item for item in queue["questions"] if item["category"] == "ENTITY"
    )

    orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{"question_id": question["question_id"], "answer": "YES"}],
        author="server-engineer",
        expected_input_signature=queue["input_signature"],
        expected_revision=queue["revision"],
    )
    synthesis = production_store.load_artifact(
        "session-1", "pair-1", "unified_synthesis"
    )
    application = production_store.load_artifact(
        "session-1", "pair-1", "review_application"
    )

    assert len(synthesis["changes"]) == 1
    assert synthesis["changes"][0]["source_mode"] == "BOTH"
    assert application["diagnostics"]["pipeline_rerun"] is False
    assert (
        application["effective_entity_relations"]["relations"][0]["relation"]
        == "SAME_ENTITY"
    )


def test_evidence_uses_page_info_and_never_exposes_server_paths(tmp_path, monkeypatch):
    pair = _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[_atom("text-voltage", "TEXT")],
        graphic_atoms=[],
    )
    _run()
    target_id = orchestrator.get_production_changes(
        "session-1", "pair-1"
    )["rows"][0]["target_id"]
    calls = []

    def page_info(_session, _pair, side, page):
        calls.append((side, page))
        return {"page": page, "width": 100.0, "height": 200.0, "signature": "pdf"}

    monkeypatch.setattr(orchestrator.store, "page_info_payload", page_info)
    payload = orchestrator.get_change_evidence("session-1", "pair-1", target_id)

    assert calls == [("left", 1), ("right", 1)]
    assert payload["sides"]["LEFT"][0]["page_size"] == {
        "width": 100.0,
        "height": 200.0,
    }
    assert str(tmp_path) not in str(payload)
    assert payload["sides"]["LEFT"][0]["document_ref"] == "LEFT"
    assert pair["left"]["pdf_path"] not in str(payload)


def test_rebuild_and_evidence_ignore_mutated_legacy_graphic_ledger(
    tmp_path, monkeypatch
):
    text = _type_atom("text-snapshot", "TEXT")
    text["project_entity_ref"] = None
    text["provenance"] = {**text["provenance"], "locations": {"LEFT": [], "RIGHT": []}}
    graphic = _type_atom("graphic-snapshot", "GRAPHIC")
    snapshot_ledger = {
        "marker": "snapshot",
        "changes": [{
            "change_id": graphic["evidence_ref"],
            "left_region": {
                "block_id": "left-snapshot",
                "page_index": 0,
                "bbox_visual_pt": [10, 20, 30, 40],
            },
            "right_region": {
                "block_id": "right-snapshot",
                "page_index": 0,
                "bbox_visual_pt": [15, 25, 35, 45],
            },
        }],
    }
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[text],
        graphic_atoms=[graphic],
        graphic_ledger=snapshot_ledger,
    )
    markers = []
    malicious = _atom("graphic-legacy", "GRAPHIC", project_ref="project:other")

    def adapt(ledger):
        marker = ledger.get("marker")
        markers.append(marker)
        return {"atoms": [graphic] if marker == "snapshot" else [malicious]}

    monkeypatch.setattr(orchestrator, "ledger_to_graphic_atoms", adapt)
    _run()
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    question = next(
        item
        for item in queue["questions"]
        if item.get("context", {}).get("atom_id") == text["atom_id"]
    )
    legacy_path = paths.graphic_change_ledger_path("session-1", "pair-1")
    legacy_path.write_text(
        json.dumps({
            "marker": "legacy",
            "changes": [{
                "change_id": graphic["evidence_ref"],
                "left_region": {
                    "block_id": "left-legacy",
                    "page_index": 98,
                    "bbox_visual_pt": [1, 1, 2, 2],
                },
                "right_region": {
                    "block_id": "right-legacy",
                    "page_index": 98,
                    "bbox_visual_pt": [1, 1, 2, 2],
                },
            }],
        }),
        encoding="utf-8",
    )

    orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{
            "question_id": question["question_id"],
            "answer": "OTHER",
            "typed_resolution": {
                "project_entity_ref": "project:panel",
                "outcome": "MATERIAL_CHANGE",
            },
        }],
        author="server-engineer",
        expected_input_signature=queue["input_signature"],
        expected_revision=queue["revision"],
    )
    row = orchestrator.get_production_changes(
        "session-1", "pair-1"
    )["rows"][0]
    assert row["change"]["source_mode"] == "BOTH"
    assert "legacy" not in markers
    assert markers[-1] == "snapshot"

    calls = []

    def page_info(_session, _pair, side, page):
        calls.append((side, page))
        return {"page": page, "width": 100.0, "height": 200.0}

    monkeypatch.setattr(orchestrator.store, "page_info_payload", page_info)
    evidence = orchestrator.get_change_evidence(
        "session-1", "pair-1", row["target_id"]
    )
    graphic_locations = [
        location
        for locations in evidence["sides"].values()
        for location in locations
        if location["source"] == "GRAPHIC"
    ]
    assert {location["page"] for location in graphic_locations} == {1}
    assert all("legacy" not in str(location) for location in graphic_locations)
    assert ("left", 99) not in calls and ("right", 99) not in calls


def test_direct_page_sources_are_resolved_only_from_pair(tmp_path):
    pair = _pair(tmp_path)
    request = orchestrator.normalize_run_request(
        input_mode="PAGE",
        left_pages=[2],
        right_pages=[3],
        left_block_ids=["left-block"],
        right_block_ids=["right-block"],
    )

    left, right = orchestrator._direct_page_sources(pair, request)

    assert left["pdf_path"] == pair["left"]["pdf_path"]
    assert left["blocks_path"] == str(Path(pair["left"]["pdf_path"]).parent / "blocks.json")
    assert left["page_index_0based"] == 1
    assert right["page_index_0based"] == 2
    assert left["block_id"] == "left-block"


def test_document_one_to_one_resolves_blocks_server_side_and_calls_router(
    tmp_path, monkeypatch
):
    pair = _pair(tmp_path)
    request = orchestrator.normalize_run_request(input_mode="DOCUMENT")
    calls = []
    monkeypatch.setattr(
        orchestrator,
        "_prepared_graphic_block_ids",
        lambda _document, _pages, side: [f"{side}-graphic"],
    )
    monkeypatch.setattr(
        orchestrator.store,
        "run_graphic_comparison",
        lambda session, pair_id, left, right, **options: calls.append(
            (session, pair_id, left, right, options)
        ) or _empty_routed_ledger("MODE_1_APPLICABLE", "MODE_1"),
    )

    _ledger, stage = orchestrator._run_graphic_branch(
        "session-1",
        "pair-1",
        pair,
        request,
        [{
            "left_pages": [10],
            "right_pages": [24],
            "relation_type": "MATCHED",
            "status": "HIGH",
        }],
    )

    assert calls == [(
        "session-1",
        "pair-1",
        ["left-graphic"],
        ["right-graphic"],
        {"persist": False},
    )]
    assert stage["selection_source"] == "SERVER_MATCHED_PAGES"
    assert stage["router_runs"] == 1
    assert stage["mode1_groups"] == 1


def test_document_explicit_graphic_scope_rejects_non_graphic_block_ids(
    tmp_path, monkeypatch
):
    pair = _pair(tmp_path)
    (tmp_path / "blocks.json").write_text(
        json.dumps({
            "pages": [],
            "blocks": [
                {"block_id": "left-image", "page_index": 9, "block_type": "image"},
                {"block_id": "left-text", "page_index": 9, "block_type": "text"},
                {"block_id": "right-graphic", "page_index": 23, "block_type": "GrApHiC"},
                {"block_id": "right-table", "page_index": 23, "block_type": "table"},
            ],
        }),
        encoding="utf-8",
    )
    calls = []
    monkeypatch.setattr(
        orchestrator.store,
        "run_graphic_comparison",
        lambda _session, _pair_id, left, right, **_options: calls.append(
            (left, right)
        ) or _empty_routed_ledger("MODE_1_APPLICABLE", "MODE_1"),
    )
    groups = [{
        "left_pages": [10],
        "right_pages": [24],
        "relation_type": "MATCHED",
        "status": "HIGH",
    }]

    _ledger, mixed_stage = orchestrator._run_graphic_branch(
        "session-1",
        "pair-1",
        pair,
        orchestrator.normalize_run_request(
            input_mode="DOCUMENT",
            left_block_ids=["left-image", "left-text"],
            right_block_ids=["right-graphic", "right-table"],
        ),
        groups,
    )
    empty_ledger, non_graphic_stage = orchestrator._run_graphic_branch(
        "session-1",
        "pair-1",
        pair,
        orchestrator.normalize_run_request(
            input_mode="DOCUMENT",
            left_block_ids=["left-text"],
            right_block_ids=["right-table"],
        ),
        groups,
    )

    assert calls == [(["left-image"], ["right-graphic"])]
    assert mixed_stage["selection_source"] == "CLIENT_BLOCK_IDS"
    assert empty_ledger["kind"] == orchestrator.DOCUMENT_GRAPHIC_BUNDLE_KIND
    assert non_graphic_stage["status"] == "NOT_APPLICABLE"
    assert non_graphic_stage["reason_code"] == (
        "NO_CLIENT_GRAPHIC_BLOCK_IN_EFFECTIVE_SHEET_SCOPE"
    )


def test_document_split_graphics_fail_safe_without_new_comparator(tmp_path):
    ledger, stage = orchestrator._run_graphic_branch(
        "session-1",
        "pair-1",
        _pair(tmp_path),
        orchestrator.normalize_run_request(input_mode="DOCUMENT"),
        [{
            "left_pages": [10],
            "right_pages": [24, 25],
            "relation_type": "SPLIT",
            "status": "HIGH",
        }],
    )

    assert ledger["kind"] == orchestrator.DOCUMENT_GRAPHIC_BUNDLE_KIND
    assert stage["status"] == "NOT_APPLICABLE"
    assert stage["reason_code"] == "grouped_graphic_comparison_not_supported"
    assert stage["group_results"][0]["required_action"] == (
        "CONFIRM_GROUPED_SHEET_WITHOUT_GRAPHIC_COMPARISON"
    )


@pytest.mark.parametrize(
    ("route", "mode", "expected_status", "expected_source_state"),
    [
        ("MODE_1_APPLICABLE", "MODE_1", "COMPLETED", "ABSENT"),
        ("MODE_2_REQUIRED", None, "NOT_APPLICABLE", "NOT_APPLICABLE"),
        ("VISION_REQUIRED", None, "CHECK_BLOCKED", "CHECK_BLOCKED"),
        ("NO_GRAPHIC_COMPARISON", None, "NOT_APPLICABLE", "NOT_APPLICABLE"),
    ],
)
def test_document_graphic_stage_follows_validated_router_route(
    tmp_path,
    monkeypatch,
    route,
    mode,
    expected_status,
    expected_source_state,
):
    pair = _pair(tmp_path)
    request = orchestrator.normalize_run_request(input_mode="DOCUMENT")
    monkeypatch.setattr(
        orchestrator,
        "_prepared_graphic_block_ids",
        lambda _document, _pages, side: [f"{side}-graphic"],
    )
    ledger = _empty_routed_ledger(route, mode)
    monkeypatch.setattr(
        orchestrator.store,
        "run_graphic_comparison",
        lambda *_args, **_kwargs: ledger,
    )

    actual_ledger, stage = orchestrator._run_graphic_branch(
        "session-1",
        "pair-1",
        pair,
        request,
        [{
            "left_pages": [10],
            "right_pages": [24],
            "relation_type": "MATCHED",
            "status": "HIGH",
        }],
    )

    assert actual_ledger["kind"] == orchestrator.DOCUMENT_GRAPHIC_BUNDLE_KIND
    assert actual_ledger["groups"][0]["ledger"] == ledger
    assert stage["route"] == route
    assert stage["status"] == expected_status
    assert stage["source_state"] == expected_source_state
    assert stage["reason_code"] == f"reason-{route}"
    snapshot = orchestrator._build_source_snapshot(
        run_id="run-route",
        generation_input_signature="generation-route",
        text_artifact={"atoms": []},
        text_source_state="ABSENT",
        graphic_ledger=actual_ledger,
        graphic_source_state=stage["source_state"],
    )
    assert snapshot["graphic"]["ledger"] == actual_ledger
    assert snapshot["graphic"]["source_state"] == expected_source_state


def test_document_graphic_bundle_routes_every_high_one_to_one_and_namespaces_evidence(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    calls = []

    def prepared(_document, pages, side):
        return [f"{side}-{pages[0]}"]

    def compare(_session, _pair_id, left, right, **options):
        calls.append((left, right, options))
        left_page = int(left[0].rsplit("-", 1)[1])
        right_page = int(right[0].rsplit("-", 1)[1])
        # Deliberately reuse the source change id.  The document bundle must
        # produce collision-free atom and evidence identities per sheet.
        return _graphic_ledger("same-source-change", left_page, right_page)

    monkeypatch.setattr(orchestrator, "_prepared_graphic_block_ids", prepared)
    monkeypatch.setattr(orchestrator.store, "run_graphic_comparison", compare)
    groups = [
        {
            "id": "sheet-a",
            "left_pages": [10],
            "right_pages": [24],
            "relation_type": "MATCHED",
            "status": "HIGH",
        },
        {
            "id": "sheet-b",
            "left_pages": [11],
            "right_pages": [25],
            "relation_type": "MATCHED",
            "status": "HIGH",
        },
    ]

    bundle, stage = orchestrator._run_graphic_branch(
        "session-1",
        "pair-1",
        _pair(tmp_path),
        orchestrator.normalize_run_request(input_mode="DOCUMENT"),
        groups,
    )

    assert calls == [
        (["left-10"], ["right-24"], {"persist": False}),
        (["left-11"], ["right-25"], {"persist": False}),
    ]
    assert bundle["kind"] == orchestrator.DOCUMENT_GRAPHIC_BUNDLE_KIND
    assert stage["status"] == "COMPLETED"
    assert stage["groups_completed"] == 2
    assert stage["router_runs"] == 2
    assert stage["mode1_groups"] == 2
    assert stage["changes"] == 2
    assert production_store.load_artifact(
        "session-1", "pair-1", "document_graphic_bundle"
    )["input_signature"] == bundle["input_signature"]

    atoms = orchestrator._graphic_atoms_from_source(bundle)
    assert len(atoms) == 2
    assert len({atom["atom_id"] for atom in atoms}) == 2
    assert len({atom["evidence_ref"] for atom in atoms}) == 2
    assert {
        atom["source_artifact"]["kind"] for atom in atoms
    } == {orchestrator.DOCUMENT_GRAPHIC_BUNDLE_KIND}
    synthesis = {
        "changes": [{
            "change_id": "document-graphic-change",
            "source_mode": "GRAPHIC",
            "evidence_refs": [
                {
                    "source": "GRAPHIC",
                    "atom_id": atom["atom_id"],
                    "evidence_ref": atom["evidence_ref"],
                    "source_artifact": atom["source_artifact"],
                }
                for atom in atoms
            ],
        }],
        "review_items": [],
    }
    evidence = orchestrator.build_evidence_navigation(
        "document-graphic-change",
        synthesis=synthesis,
        graphic_ledger=bundle,
    )
    assert {item["page"] for item in evidence["sides"]["LEFT"]} == {10, 11}
    assert {item["page"] for item in evidence["sides"]["RIGHT"]} == {24, 25}

    snapshot = orchestrator._build_source_snapshot(
        run_id="document-bundle-run",
        generation_input_signature="document-bundle-generation",
        text_artifact={"atoms": []},
        text_source_state="ABSENT",
        graphic_ledger=bundle,
        graphic_source_state="VALID",
    )
    state = {
        "run_id": "document-bundle-run",
        "input_signature": "document-bundle-generation",
        "stages": {
            "source_snapshot": {"input_signature": snapshot["input_signature"]}
        },
    }
    assert orchestrator._validate_source_snapshot(snapshot, state)["graphic"][
        "ledger"
    ]["input_signature"] == bundle["input_signature"]


def test_document_graphic_groups_fail_closed_independently(tmp_path, monkeypatch):
    prepared_calls = []
    router_calls = []

    def prepared(_document, pages, side):
        page = pages[0]
        prepared_calls.append((side, page))
        if page in {10, 24}:
            return [f"{side}-{page}-a", f"{side}-{page}-b"]
        return [f"{side}-{page}"]

    def compare(_session, _pair_id, left, right, **_options):
        router_calls.append((left, right))
        left_page = int(left[0].rsplit("-", 1)[1])
        right_page = int(right[0].rsplit("-", 1)[1])
        if left_page == 12:
            raise RuntimeError("one isolated router failure")
        return _graphic_ledger("surviving-change", left_page, right_page)

    monkeypatch.setattr(orchestrator, "_prepared_graphic_block_ids", prepared)
    monkeypatch.setattr(orchestrator.store, "run_graphic_comparison", compare)
    groups = [
        {
            "id": "ambiguous-blocks",
            "left_pages": [10],
            "right_pages": [24],
            "relation_type": "MATCHED",
            "status": "HIGH",
        },
        {
            "id": "possible-sheet",
            "left_pages": [11],
            "right_pages": [25],
            "relation_type": "MATCHED",
            "status": "POSSIBLE",
        },
        {
            "id": "failed-router",
            "left_pages": [12],
            "right_pages": [26],
            "relation_type": "MATCHED",
            "status": "HIGH",
        },
        {
            "id": "surviving-router",
            "left_pages": [13],
            "right_pages": [27],
            "relation_type": "MATCHED",
            "status": "HIGH",
        },
        {
            "id": "grouped-sheet",
            "left_pages": [14],
            "right_pages": [28, 29],
            "relation_type": "SPLIT",
            "status": "HIGH",
        },
    ]

    bundle, stage = orchestrator._run_graphic_branch(
        "session-1",
        "pair-1",
        _pair(tmp_path),
        orchestrator.normalize_run_request(input_mode="DOCUMENT"),
        groups,
    )

    # POSSIBLE and grouped relations never inspect or route graphic blocks.
    assert not any(page in {11, 25, 14, 28, 29} for _side, page in prepared_calls)
    assert router_calls == [
        (["left-12"], ["right-26"]),
        (["left-13"], ["right-27"]),
    ]
    results = {
        item["group"]["id"]: item for item in bundle["groups"]
    }
    assert results["ambiguous-blocks"]["status"] == "REVIEW_REQUIRED"
    assert results["ambiguous-blocks"]["reason_code"] == (
        "ambiguous_prepared_graphic_blocks"
    )
    assert results["possible-sheet"]["status"] == "REVIEW_REQUIRED"
    assert results["possible-sheet"]["reason_code"] == (
        "sheet_relation_requires_review"
    )
    assert results["failed-router"]["status"] == "CHECK_BLOCKED"
    assert results["surviving-router"]["status"] == "COMPLETED"
    assert results["grouped-sheet"]["status"] == "NOT_APPLICABLE"
    assert results["grouped-sheet"]["reason_code"] == (
        "grouped_graphic_comparison_not_supported"
    )
    assert stage["status"] == "CHECK_BLOCKED"
    assert stage["coverage"] == "PARTIAL"
    assert stage["groups_completed"] == 1
    assert stage["groups_review_required"] == 2
    assert stage["groups_blocked"] == 1
    assert stage["groups_not_applicable"] == 1
    assert stage["router_runs"] == 2
    assert stage["router_failed_groups"] == 1
    assert stage["mode1_groups"] == 1
    assert stage["changes"] == 1
    block_question = next(
        item for item in stage["engineer_questions"]
        if item["question_type"] == "GRAPHIC_BLOCK_SELECTION"
    )
    assert block_question["left_block_ids"] == ["left-10-a", "left-10-b"]
    assert block_question["right_block_ids"] == ["right-24-a", "right-24-b"]
    assert len(orchestrator._graphic_atoms_from_source(bundle)) == 1


def test_document_graphic_bundle_signature_and_diagnostics_are_fail_closed(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        orchestrator,
        "_prepared_graphic_block_ids",
        lambda _document, pages, side: [f"{side}-{pages[0]}"],
    )
    monkeypatch.setattr(
        orchestrator.store,
        "run_graphic_comparison",
        lambda *_args, **_kwargs: _graphic_ledger("change", 10, 24),
    )
    bundle, _stage = orchestrator._run_graphic_branch(
        "session-1",
        "pair-1",
        _pair(tmp_path),
        orchestrator.normalize_run_request(input_mode="DOCUMENT"),
        [{
            "id": "sheet-a",
            "left_pages": [10],
            "right_pages": [24],
            "relation_type": "MATCHED",
            "status": "HIGH",
        }],
    )

    damaged = json.loads(json.dumps(bundle))
    damaged["groups"][0]["change_refs"][0]["evidence_ref"] = "rewritten"
    with pytest.raises(orchestrator.ProductionStateConflictError):
        orchestrator._validate_document_graphic_bundle(damaged)

    lying = json.loads(json.dumps(bundle))
    lying["diagnostics"]["groups_completed"] = 0
    core = {key: value for key, value in lying.items() if key != "input_signature"}
    lying["input_signature"] = orchestrator.content_signature(core)
    with pytest.raises(
        orchestrator.ProductionStateConflictError,
        match="diagnostics changed",
    ):
        orchestrator._validate_document_graphic_bundle(lying)


def test_grouped_page_graphic_scope_never_compares_only_first_pages(
    tmp_path, monkeypatch
):
    calls = []
    monkeypatch.setattr(
        orchestrator,
        "compare_selected_pages",
        lambda *_: calls.append(True),
    )

    ledger, stage = orchestrator._run_graphic_branch(
        "session-1",
        "pair-1",
        _pair(tmp_path),
        orchestrator.normalize_run_request(
            input_mode="PAGE", left_pages=[1], right_pages=[1]
        ),
        [{"left_pages": [1], "right_pages": [1, 2], "relation_type": "USER_GROUPED"}],
    )

    assert ledger is None
    assert calls == []
    assert stage["status"] == "NOT_APPLICABLE"
    assert stage["mode"] == "MODE_2_REQUIRED"
    assert stage["reason_code"] == (
        "GROUPED_PAGE_CARDINALITY_REQUIRES_NEW_COMPARATOR"
    )


def test_page_additional_group_invalidates_explicit_block_selection(
    tmp_path, monkeypatch
):
    calls = []
    monkeypatch.setattr(
        orchestrator, "compare_selected_pages", lambda *_: calls.append(True)
    )

    ledger, stage = orchestrator._run_graphic_branch(
        "session-1",
        "pair-1",
        _pair(tmp_path),
        orchestrator.normalize_run_request(
            input_mode="PAGE",
            left_pages=[1],
            right_pages=[1],
            left_block_ids=["left-explicit"],
            right_block_ids=["right-explicit"],
        ),
        [
            {"left_pages": [1], "right_pages": [1], "relation_type": "USER_SELECTED"},
            {"left_pages": [1], "right_pages": [2], "relation_type": "MATCHED"},
        ],
    )

    assert ledger is None
    assert calls == []
    assert stage["status"] == "NOT_APPLICABLE"
    assert stage["reason_code"] == "PAGE_ACTION_INVALIDATES_EXPLICIT_BLOCK_SCOPE"


def test_multiple_page_groups_run_direct_mode2_independently_and_bundle_evidence(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    calls = []

    def compare(left, right):
        left_page = int(left["page_index_0based"]) + 1
        right_page = int(right["page_index_0based"]) + 1
        calls.append((left_page, right_page))
        return {
            "graphic_change_ledger": _graphic_ledger(
                f"change-{left_page}-{right_page}", left_page, right_page
            )
        }

    monkeypatch.setattr(orchestrator, "compare_selected_pages", compare)
    monkeypatch.setattr(
        orchestrator,
        "validate_direct_page_comparison_result",
        lambda payload: payload,
    )
    groups = [
        {"left_pages": [1], "right_pages": [1], "relation_type": "USER_SELECTED"},
        {"left_pages": [1], "right_pages": [2], "relation_type": "MATCHED"},
    ]

    bundle, stage = orchestrator._run_graphic_branch(
        "session-1",
        "pair-1",
        _pair(tmp_path),
        orchestrator.normalize_run_request(
            input_mode="PAGE", left_pages=[1], right_pages=[1]
        ),
        groups,
    )

    assert calls == [(1, 1), (1, 2)]
    assert bundle["kind"] == orchestrator.PAGE_GRAPHIC_BUNDLE_KIND
    assert stage["status"] == "COMPLETED"
    assert stage["groups_completed"] == 2
    assert stage["changes"] == 2
    stored = production_store.load_artifact(
        "session-1", "pair-1", "page_graphic_bundle"
    )
    assert stored["input_signature"] == bundle["input_signature"]
    atoms = orchestrator._graphic_atoms_from_source(bundle)
    assert len(atoms) == 2
    assert len({atom["atom_id"] for atom in atoms}) == 2
    assert len({atom["evidence_ref"] for atom in atoms}) == 2

    synthesis = {
        "changes": [{
            "change_id": "combined-graphic",
            "source_mode": "GRAPHIC",
            "evidence_refs": [
                {
                    "source": "GRAPHIC",
                    "atom_id": atom["atom_id"],
                    "evidence_ref": atom["evidence_ref"],
                    "source_artifact": atom["source_artifact"],
                }
                for atom in atoms
            ],
        }],
        "review_items": [],
    }
    evidence = orchestrator.build_evidence_navigation(
        "combined-graphic",
        synthesis=synthesis,
        graphic_ledger=bundle,
    )
    assert {item["page"] for item in evidence["sides"]["LEFT"]} == {1}
    assert {item["page"] for item in evidence["sides"]["RIGHT"]} == {1, 2}
    snapshot = orchestrator._build_source_snapshot(
        run_id="bundle-run",
        generation_input_signature="bundle-generation",
        text_artifact={"atoms": []},
        text_source_state="ABSENT",
        graphic_ledger=bundle,
        graphic_source_state="VALID",
    )
    state = {
        "run_id": "bundle-run",
        "input_signature": "bundle-generation",
        "stages": {
            "source_snapshot": {"input_signature": snapshot["input_signature"]}
        },
    }
    assert orchestrator._validate_source_snapshot(snapshot, state)["graphic"][
        "ledger"
    ]["input_signature"] == bundle["input_signature"]
    damaged = json.loads(json.dumps(snapshot))
    damaged["graphic"]["ledger"]["groups"][0]["ledger"]["changes"][0][
        "confidence"
    ] = "LOW"
    with pytest.raises(orchestrator.ProductionStateConflictError):
        orchestrator._validate_source_snapshot(damaged, state)


def test_multiple_page_groups_preserve_success_when_one_direct_group_fails(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    calls = []

    def compare(left, right):
        right_page = int(right["page_index_0based"]) + 1
        calls.append(right_page)
        if right_page == 2:
            raise orchestrator.DirectPageComparisonError("blocked additional group")
        return {"graphic_change_ledger": _graphic_ledger("original", 1, 1)}

    monkeypatch.setattr(orchestrator, "compare_selected_pages", compare)
    monkeypatch.setattr(
        orchestrator,
        "validate_direct_page_comparison_result",
        lambda payload: payload,
    )

    bundle, stage = orchestrator._run_graphic_branch(
        "session-1",
        "pair-1",
        _pair(tmp_path),
        orchestrator.normalize_run_request(
            input_mode="PAGE", left_pages=[1], right_pages=[1]
        ),
        [
            {"left_pages": [1], "right_pages": [1], "relation_type": "USER_SELECTED"},
            {"left_pages": [1], "right_pages": [2], "relation_type": "MATCHED"},
        ],
    )

    assert calls == [1, 2]
    assert stage["status"] == "CHECK_BLOCKED"
    assert stage["source_state"] == "CHECK_BLOCKED"
    assert stage["groups_completed"] == 1
    assert stage["groups_blocked"] == 1
    assert stage["changes"] == 1
    assert len(orchestrator._graphic_atoms_from_source(bundle)) == 1
    assert [item["status"] for item in stage["group_results"]] == [
        "COMPLETED", "CHECK_BLOCKED"
    ]


def test_compare_additionally_retains_both_graphics_through_snapshot_and_reanswer(
    tmp_path, monkeypatch
):
    actual_graphic_branch = orchestrator._run_graphic_branch
    actual_adapter = orchestrator.ledger_to_graphic_atoms
    relations = match_sheets(
        [{"pdf_page": 1, "functional_content": ["panel"]}],
        [{"pdf_page": 2, "functional_content": ["panel"]}],
        generated_at="fixed",
    )
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[],
        graphic_atoms=[],
        sheet_relations=relations,
    )
    monkeypatch.setattr(orchestrator, "_run_graphic_branch", actual_graphic_branch)
    monkeypatch.setattr(orchestrator, "ledger_to_graphic_atoms", actual_adapter)
    monkeypatch.setattr(
        orchestrator,
        "validate_direct_page_comparison_result",
        lambda payload: payload,
    )

    def compare(left, right):
        left_page = int(left["page_index_0based"]) + 1
        right_page = int(right["page_index_0based"]) + 1
        return {
            "mode": "MODE_2",
            "graphic_change_ledger": _graphic_ledger(
                f"change-{left_page}-{right_page}", left_page, right_page
            ),
        }

    monkeypatch.setattr(orchestrator, "compare_selected_pages", compare)
    _run()
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    question = next(
        item for item in queue["questions"]
        if item["question_type"] == "PAGE_SUGGESTION_ACTION"
    )

    applied = orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{
            "question_id": question["question_id"],
            "answer": "COMPARE_ADDITIONALLY",
        }],
        author="server-engineer",
        expected_input_signature=queue["input_signature"],
        expected_revision=queue["revision"],
    )

    snapshot = production_store.load_artifact(
        "session-1", "pair-1", "source_snapshot"
    )
    bundle = snapshot["graphic"]["ledger"]
    assert bundle["kind"] == orchestrator.PAGE_GRAPHIC_BUNDLE_KIND
    assert len(orchestrator._graphic_atoms_from_source(bundle)) == 2
    rows = orchestrator.get_production_changes("session-1", "pair-1")["rows"]
    assert len(rows) == 2
    assert applied["state"]["stages"]["graphic"]["changes"] == 2

    monkeypatch.setattr(
        orchestrator.store,
        "page_info_payload",
        lambda _session, _pair, _side, page: {
            "page": page,
            "width": 100.0,
            "height": 100.0,
        },
    )
    evidence_pages = []
    for row in rows:
        evidence = orchestrator.get_change_evidence(
            "session-1", "pair-1", row["target_id"]
        )
        evidence_pages.append({
            side: {item["page"] for item in evidence["sides"][side]}
            for side in ("LEFT", "RIGHT")
        })
    assert {next(iter(item["RIGHT"])) for item in evidence_pages} == {1, 2}

    reopened = orchestrator.get_review_questions("session-1", "pair-1")
    repeated = orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{
            "question_id": question["question_id"],
            "answer": "COMPARE_ADDITIONALLY",
        }],
        author="server-engineer",
        expected_input_signature=reopened["input_signature"],
        expected_revision=reopened["revision"],
    )
    assert repeated["state"]["run_id"] == applied["state"]["run_id"]
    assert len(orchestrator.get_production_changes(
        "session-1", "pair-1"
    )["rows"]) == 2
    assert repeated["suggestion_action_semantics"]["this_update_reran"] is False


def test_compare_additionally_partial_group_keeps_successful_graphic_facts(
    tmp_path, monkeypatch
):
    actual_graphic_branch = orchestrator._run_graphic_branch
    actual_adapter = orchestrator.ledger_to_graphic_atoms
    relations = match_sheets(
        [{"pdf_page": 1, "functional_content": ["panel"]}],
        [{"pdf_page": 2, "functional_content": ["panel"]}],
        generated_at="fixed",
    )
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[],
        graphic_atoms=[],
        sheet_relations=relations,
    )
    monkeypatch.setattr(orchestrator, "_run_graphic_branch", actual_graphic_branch)
    monkeypatch.setattr(orchestrator, "ledger_to_graphic_atoms", actual_adapter)
    monkeypatch.setattr(
        orchestrator,
        "validate_direct_page_comparison_result",
        lambda payload: payload,
    )

    def compare(left, right):
        right_page = int(right["page_index_0based"]) + 1
        if right_page == 2:
            raise orchestrator.DirectPageComparisonError("blocked additional")
        return {
            "mode": "MODE_2",
            "graphic_change_ledger": _graphic_ledger("original", 1, 1),
        }

    monkeypatch.setattr(orchestrator, "compare_selected_pages", compare)
    _run()
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    question = next(
        item for item in queue["questions"]
        if item["question_type"] == "PAGE_SUGGESTION_ACTION"
    )

    updated = orchestrator.update_review_answers(
        "session-1",
        "pair-1",
        answers=[{
            "question_id": question["question_id"],
            "answer": "COMPARE_ADDITIONALLY",
        }],
        author="server-engineer",
        expected_input_signature=queue["input_signature"],
        expected_revision=queue["revision"],
    )

    assert updated["state"]["status"] == "PARTIAL"
    graphic_stage = updated["state"]["stages"]["graphic"]
    assert graphic_stage["status"] == "CHECK_BLOCKED"
    assert graphic_stage["groups_completed"] == 1
    assert graphic_stage["groups_blocked"] == 1
    assert len(orchestrator.get_production_changes(
        "session-1", "pair-1"
    )["rows"]) == 1


def test_page_bounds_are_checked_before_any_producer(tmp_path, monkeypatch):
    pair = _pair(tmp_path)
    monkeypatch.setattr(orchestrator.store, "get_pair_for_production", lambda *_: pair)
    monkeypatch.setattr(orchestrator, "_page_count", lambda _path: 1)
    called = []
    monkeypatch.setattr(orchestrator, "_run_sheet_matcher", lambda *_: called.append(True))

    with pytest.raises(ValueError, match="left_page_out_of_range"):
        orchestrator.run_production_comparison(
            "session-1",
            "pair-1",
            input_mode="PAGE",
            left_pages=[2],
            right_pages=[1],
        )

    assert called == []


def test_fatal_error_after_start_is_persisted_as_failed(tmp_path, monkeypatch):
    pair = _pair(tmp_path)
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    monkeypatch.setattr(orchestrator.store, "get_pair_for_production", lambda *_: pair)
    monkeypatch.setattr(
        orchestrator,
        "_run_sheet_matcher",
        lambda *_: (_ for _ in ()).throw(RuntimeError("private /srv/path")),
    )

    with pytest.raises(RuntimeError):
        orchestrator.run_production_comparison(
            "session-1", "pair-1", input_mode="DOCUMENT"
        )

    state = production_store.load_artifact("session-1", "pair-1", "state")
    assert state["status"] == "FAILED"
    assert state["progress"] == 100
    assert state["reason_code"] == "RuntimeError"
    assert "/srv/path" not in str(state)


def test_failed_rerun_never_serves_or_mutates_previous_generation(tmp_path, monkeypatch):
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[_atom("text-voltage", "TEXT")],
        graphic_atoms=[],
    )
    _run()
    previous = orchestrator.get_production_changes("session-1", "pair-1")
    assert len(previous["rows"]) == 1

    monkeypatch.setattr(
        orchestrator,
        "_run_entity_matcher",
        lambda *_: (_ for _ in ()).throw(RuntimeError("fatal")),
    )
    with pytest.raises(RuntimeError):
        _run()

    state = orchestrator.get_production_state("session-1", "pair-1")
    changes = orchestrator.get_production_changes("session-1", "pair-1")
    report = orchestrator.get_final_report("session-1", "pair-1")
    assert state["status"] == "FAILED"
    assert changes["available"] is False and changes["rows"] == []
    assert report["available"] is False
    with pytest.raises(orchestrator.ProductionStateConflictError, match="not published"):
        orchestrator.update_engineer_decisions(
            "session-1",
            "pair-1",
            updates=[],
            author="server-engineer",
            expected_input_signature=previous["input_signature"],
            expected_revision=previous["revision"],
        )


def test_source_change_during_run_fails_before_publication(tmp_path, monkeypatch):
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[_atom("text-voltage", "TEXT")],
        graphic_atoms=[],
    )
    signatures = iter(("source-at-start", "source-at-publication"))
    monkeypatch.setattr(
        orchestrator,
        "_input_signature",
        lambda *_args, **_kwargs: next(signatures),
    )

    with pytest.raises(
        orchestrator.ProductionStateConflictError,
        match="sources changed during comparison",
    ):
        _run()

    state = production_store.load_artifact("session-1", "pair-1", "state")
    assert state["status"] == "FAILED"
    assert state["reason_code"] == "ProductionStateConflictError"


def test_decision_write_failure_leaves_pair_unpublished(tmp_path, monkeypatch):
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[_atom("text-voltage", "TEXT")],
        graphic_atoms=[],
    )
    _run()
    changes = orchestrator.get_production_changes("session-1", "pair-1")
    original_decisions = production_store.load_artifact(
        "session-1", "pair-1", "engineer_decisions"
    )
    original_save = production_store.save_artifact

    def fail_decision_write(session_id, pair_id, name, value):
        if name == "engineer_decisions":
            raise OSError("simulated decision write failure")
        return original_save(session_id, pair_id, name, value)

    monkeypatch.setattr(production_store, "save_artifact", fail_decision_write)
    with pytest.raises(OSError, match="decision write failure"):
        orchestrator.update_engineer_decisions(
            "session-1",
            "pair-1",
            updates=[],
            author="server-engineer",
            expected_input_signature=changes["input_signature"],
            expected_revision=changes["revision"],
        )

    state = production_store.load_artifact("session-1", "pair-1", "state")
    assert state["status"] == "UPDATING"
    assert production_store.load_artifact(
        "session-1", "pair-1", "engineer_decisions"
    ) == original_decisions
    assert orchestrator.get_production_changes(
        "session-1", "pair-1"
    )["available"] is False
    assert orchestrator.get_final_report(
        "session-1", "pair-1"
    )["available"] is False


def test_tampered_decisions_fail_closed_against_published_state(tmp_path, monkeypatch):
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[_atom("text-voltage", "TEXT")],
        graphic_atoms=[],
    )
    _run()
    decisions = production_store.load_artifact(
        "session-1", "pair-1", "engineer_decisions"
    )
    decisions["tampered"] = True
    production_store.save_artifact(
        "session-1", "pair-1", "engineer_decisions", decisions
    )

    with pytest.raises(
        orchestrator.ProductionStateConflictError,
        match="decisions digest does not match state",
    ):
        orchestrator.get_production_changes("session-1", "pair-1")
    with pytest.raises(orchestrator.ProductionStateConflictError):
        orchestrator.get_final_report("session-1", "pair-1")


def test_tampered_source_snapshot_blocks_evidence_and_reanswer(tmp_path, monkeypatch):
    unresolved = _unlocated_atom("text-unresolved", "TEXT")
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[unresolved],
        graphic_atoms=[],
    )
    _run()
    changes = orchestrator.get_production_changes("session-1", "pair-1")
    queue = orchestrator.get_review_questions("session-1", "pair-1")
    question = next(
        item for item in queue["questions"] if item["category"] == "CHANGE"
    )
    snapshot = production_store.load_artifact(
        "session-1", "pair-1", "source_snapshot"
    )
    snapshot["text"]["artifact"]["atoms"][0]["after_value"] = "tampered"
    production_store.save_artifact(
        "session-1", "pair-1", "source_snapshot", snapshot
    )

    with pytest.raises(
        orchestrator.ProductionStateConflictError,
        match="TEXT snapshot digest changed",
    ):
        orchestrator.get_change_evidence(
            "session-1", "pair-1", changes["rows"][0]["target_id"]
        )
    with pytest.raises(orchestrator.ProductionStateConflictError):
        orchestrator.update_review_answers(
            "session-1",
            "pair-1",
            answers=[{
                "question_id": question["question_id"],
                "answer": "OTHER",
                "typed_resolution": {"project_entity_ref": "project:panel"},
            }],
            author="server-engineer",
            expected_input_signature=queue["input_signature"],
            expected_revision=queue["revision"],
        )
    assert production_store.load_artifact(
        "session-1", "pair-1", "review_answers"
    ) is None
    assert production_store.load_artifact(
        "session-1", "pair-1", "state"
    )["status"] in {"COMPLETED", "PARTIAL"}


def test_final_get_is_current_projection_even_if_cached_file_lags(tmp_path, monkeypatch):
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[_atom("text-voltage", "TEXT")],
        graphic_atoms=[],
    )
    _run()
    changes = orchestrator.get_production_changes("session-1", "pair-1")
    target_id = changes["rows"][0]["target_id"]
    orchestrator.update_engineer_decisions(
        "session-1",
        "pair-1",
        updates=[{"target_id": target_id, "decision": "APPROVED"}],
        author="server-engineer",
        expected_input_signature=changes["input_signature"],
        expected_revision=changes["revision"],
    )
    production_store.save_artifact(
        "session-1",
        "pair-1",
        "final_report",
        {"approved_atomic_changes": [], "summary": {"approved": 0}},
    )

    report = orchestrator.get_final_report("session-1", "pair-1")

    assert [item["change_id"] for item in report["approved_atomic_changes"]] == [
        target_id
    ]


def test_fallback_markdown_is_part_of_published_source_signature(tmp_path, monkeypatch):
    pair = _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[_atom("text-voltage", "TEXT")],
        graphic_atoms=[],
    )
    fallback = Path(pair["left"]["pdf_path"]).parent / "document.md"
    fallback.write_text("version one", encoding="utf-8")
    _run()
    assert orchestrator.get_production_state("session-1", "pair-1")["stale"] is False

    fallback.write_text("version two", encoding="utf-8")

    assert orchestrator.get_production_state("session-1", "pair-1")["stale"] is True


def test_pair_lock_rejects_overlapping_run_without_changing_state(tmp_path, monkeypatch):
    pair = _pair(tmp_path)
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    monkeypatch.setattr(orchestrator.store, "get_pair_for_production", lambda *_: pair)

    with production_store.production_pair_lock("session-1", "pair-1"):
        with pytest.raises(production_store.ProductionConflictError, match="being updated"):
            _run()

    assert production_store.load_artifact("session-1", "pair-1", "state") is None


def test_running_state_reports_active_runner_only_while_pair_lock_is_held(
    tmp_path, monkeypatch
):
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[_atom("text-voltage", "TEXT")],
        graphic_atoms=[],
    )
    terminal = _run()
    running = {
        **terminal,
        "status": "RUNNING",
        "current_stage": "content_analysis",
        "current_substage": "graphic_structural_comparison",
    }
    production_store.save_artifact(
        "session-1", "pair-1", "state", running
    )

    with production_store.production_pair_lock("session-1", "pair-1"):
        public = orchestrator.get_production_state(
            "session-1", "pair-1"
        )

    assert public["status"] == "RUNNING"
    assert public["runner_active"] is True
    assert public["orphaned_run"] is False
    assert public["run_recoverable"] is False


def test_running_state_without_pair_lock_is_orphaned_and_recoverable(
    tmp_path, monkeypatch
):
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[_atom("text-voltage", "TEXT")],
        graphic_atoms=[],
    )
    terminal = _run()
    production_store.save_artifact(
        "session-1",
        "pair-1",
        "state",
        {
            **terminal,
            "status": "RUNNING",
            "current_stage": "content_analysis",
            "current_substage": "graphic_structural_comparison",
        },
    )

    public = orchestrator.get_production_state("session-1", "pair-1")

    assert public["status"] == "RUNNING"
    assert public["runner_active"] is False
    assert public["orphaned_run"] is True
    assert public["run_recoverable"] is True


def test_new_run_recovers_orphan_without_accepting_old_progress_or_artifacts(
    tmp_path, monkeypatch
):
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[_atom("text-voltage", "TEXT")],
        graphic_atoms=[],
    )
    first = _run()
    old_run_id = first["run_id"]
    production_store.save_artifact(
        "session-1",
        "pair-1",
        "state",
        {
            **first,
            "status": "RUNNING",
            "current_stage": "content_analysis",
            "current_substage": "graphic_structural_comparison",
        },
    )

    recovered = _run()

    assert recovered["run_id"] != old_run_id
    assert recovered["status"] in {"COMPLETED", "PARTIAL"}
    assert recovered["recovered_from_interrupted_run"] == {
        "run_id": old_run_id,
        "status": "INTERRUPTED",
        "previous_status": "RUNNING",
        "started_at": first["started_at"],
        "last_activity_at": first["last_activity_at"],
        "input_signature": first["input_signature"],
        "interrupted_at": recovered["started_at"],
    }
    snapshot = production_store.load_artifact(
        "session-1", "pair-1", "source_snapshot"
    )
    assert snapshot["run_id"] == recovered["run_id"]
    assert snapshot["generation_input_signature"] == recovered[
        "input_signature"
    ]

    before_late_event = production_store.load_artifact(
        "session-1", "pair-1", "state"
    )
    rejected = orchestrator._publish_progress_event(
        "session-1",
        "pair-1",
        old_run_id,
        current_stage="content_analysis",
        current_substage="late_old_callback",
        message="late",
        stage_key="graphic",
        stage_status="RUNNING",
    )
    assert rejected == before_late_event
    assert production_store.load_artifact(
        "session-1", "pair-1", "state"
    ) == before_late_event


def test_exact_entity_binding_reaches_g246_and_merges_both_sources():
    text = _type_atom("text-device-type", "TEXT")
    text["subject_ref"] = "panel:exact"
    graphic = _type_atom("graphic-device-type", "GRAPHIC")
    graphic["project_entity_ref"] = None
    graphic["subject_ref"] = "panel:exact"
    graphic["provenance"]["structured"] = {
        "subject": {
            "kind": "panel",
            "functional_role": "MAIN_DISTRIBUTION",
        },
        "left_nodes": ["INPUT", "BUS"],
        "right_nodes": ["INPUT", "BUS"],
        "left_edges": ["INPUT->BUS"],
        "right_edges": ["INPUT->BUS"],
        "relation": {"left_count": 27, "right_count": 30},
    }
    left, right = orchestrator._entity_records([text], [graphic])
    relations = entity_matcher.match_entities(left, right, generated_at="fixed")
    same = [item for item in relations["relations"] if item["relation"] == "SAME_ENTITY"]
    assert len(same) == 1

    bound = orchestrator._bind_synthesis_atoms([text], [graphic], relations)
    assert bound["text_atoms"][0]["project_entity_ref"] == "project:panel"
    assert bound["graphic_atoms"][0]["project_entity_ref"] == "project:panel"
    candidates = orchestrator._build_synthesis_candidates(
        bound["text_atoms"],
        bound["graphic_atoms"],
        relations,
        source_valid=True,
        coverage_by_side={"LEFT": "CHECKED", "RIGHT": "CHECKED"},
        document_binding_state="DOCUMENT_BINDING_PROVEN",
    )
    synthesis = orchestrator.synthesize_unified_changes(
        text_atoms=bound["text_atoms"],
        graphic_atoms=bound["graphic_atoms"],
        candidates=candidates,
        source_states={"TEXT": "VALID", "GRAPHIC": "VALID"},
    )
    assert len(synthesis["changes"]) == 1
    assert synthesis["changes"][0]["source_mode"] == "BOTH"


def test_entity_records_do_not_invent_counterparts_for_added_or_removed_atoms():
    added = _atom("added", "TEXT")
    added.update({
        "subject_ref": "panel:new",
        "before_value": None,
        "after_value": "380 V",
    })
    removed = _atom("removed", "TEXT")
    removed.update({
        "subject_ref": "panel:old",
        "before_value": "220 V",
        "after_value": None,
    })

    left, right = orchestrator._entity_records([added, removed], [])

    assert [item["entity_ref"] for item in left] == ["panel:old"]
    assert [item["entity_ref"] for item in right] == ["panel:new"]


def test_pipeline_stage_metadata_is_truthful_and_skips_superseded_candidates():
    relations = {
        "relations": [
            {
                "relation_id": "confirmed",
                "status": "HIGH",
                "relation_type": "MATCHED",
                "left_pages": [1],
                "right_pages": [2],
            },
            {
                "relation_id": "superseded",
                "status": "CANDIDATE_SUPERSEDED",
                "relation_type": "SPLIT",
                "left_pages": [1],
                "right_pages": [2, 3],
            },
            {
                "relation_id": "review",
                "status": "POSSIBLE",
                "relation_type": "MERGED",
                "left_pages": [4, 5],
                "right_pages": [6],
            },
            {
                "relation_id": "none",
                "status": "NO_MATCH",
                "relation_type": "NO_MATCH",
                "left_pages": [7],
                "right_pages": [],
            },
        ]
    }

    assert [
        group["id"] for group in orchestrator._sheet_comparison_groups(relations)
    ] == ["confirmed", "review"]
    assert orchestrator._sheet_relation_counts(relations) == {
        "CANDIDATE_SUPERSEDED": 1,
        "HIGH": 1,
        "MERGED": 1,
        "NO_MATCH": 1,
        "POSSIBLE": 1,
        "SPLIT": 1,
    }
    assert orchestrator._initial_pipeline_stages("DOCUMENT") == {
        "sheet_matching": {
            "status": "RUNNING",
            "relations": 0,
            "relation_counts": {},
        },
        "sheet_scope": {"status": "NOT_STARTED", "groups": 0},
        "text": {"status": "NOT_STARTED", "atoms": 0, "deltas": 0},
        "graphic": {"status": "NOT_STARTED", "changes": 0},
    }
    assert orchestrator._initial_pipeline_stages("PAGE")["sheet_matching"][
        "status"
    ] == "PENDING_ADVISORY"


def test_progress_event_is_atomic_and_rejects_a_stale_run(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    initial = {
        "run_id": "run-current",
        "status": "RUNNING",
        "revision": 4,
        "last_activity_at": "2026-01-01T00:00:00+00:00",
        "stages": {"text": {"status": "RUNNING"}},
    }
    production_store.save_artifact(
        "session-1", "pair-1", "state", initial
    )

    stale = orchestrator._publish_progress_event(
        "session-1",
        "pair-1",
        "run-stale",
        current_stage="content_analysis",
        current_substage="text_preparation",
        message="stale",
        stage_key="text",
        stage_status="RUNNING",
    )

    assert stale == initial
    current = orchestrator._publish_progress_event(
        "session-1",
        "pair-1",
        "run-current",
        current_stage="content_analysis",
        current_substage="text_difference_search",
        message="Поиск различий в тексте…",
        processed=2,
        total=5,
        unit="differences",
        duration_ms=17,
        run_duration_ms=99,
        stage_key="text",
        stage_status="RUNNING",
        stage_started_at="2026-01-01T00:00:00+00:00",
    )

    assert current["revision"] == 5
    assert current["current_stage"] == "content_analysis"
    assert current["processed"] == 2
    assert current["total"] == 5
    assert current["duration_ms"] == 99
    assert current["last_activity_at"] != initial["last_activity_at"]
    assert current["stages"]["text"]["progress"] == {
        "status": "RUNNING",
        "started_at": "2026-01-01T00:00:00+00:00",
        "last_activity_at": current["last_activity_at"],
        "current_stage": "content_analysis",
        "current_substage": "text_difference_search",
        "message": "Поиск различий в тексте…",
        "processed": 2,
        "total": 5,
        "unit": "differences",
        "current_item": None,
        "recent_unit_durations_ms": [],
        "duration_ms": 17,
    }


def test_sheet_stage_is_not_completed_before_its_artifact_is_saved(
    tmp_path, monkeypatch
):
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[_atom("text-voltage", "TEXT")],
        graphic_atoms=[],
    )
    original_save = production_store.save_artifact

    def fail_sheet_save(session_id, pair_id, artifact, value):
        if artifact == "sheet_relations":
            raise OSError("simulated artifact failure")
        return original_save(session_id, pair_id, artifact, value)

    monkeypatch.setattr(production_store, "save_artifact", fail_sheet_save)

    with pytest.raises(OSError, match="simulated artifact failure"):
        _run(input_mode="DOCUMENT")

    failed = production_store.load_artifact("session-1", "pair-1", "state")
    assert failed["status"] == "FAILED"
    assert failed["stages"]["sheet_matching"]["status"] == "FAILED"


def test_progress_callback_failure_fails_generation_not_text_evidence(
    tmp_path, monkeypatch
):
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[_atom("text-voltage", "TEXT")],
        graphic_atoms=[],
    )
    base_text_branch = orchestrator._run_text_branch

    def reporting_text_branch(*args, **kwargs):
        callback = orchestrator._TEXT_PROGRESS_CALLBACK.get()
        assert callback is not None
        callback(
            substage="text_difference_search",
            message="Поиск различий в тексте…",
        )
        return base_text_branch(*args, **kwargs)

    original_publish = orchestrator._publish_progress_event

    def fail_text_progress(*args, **kwargs):
        if kwargs.get("current_substage") == "text_difference_search":
            raise OSError("simulated progress failure")
        return original_publish(*args, **kwargs)

    monkeypatch.setattr(orchestrator, "_run_text_branch", reporting_text_branch)
    monkeypatch.setattr(orchestrator, "_publish_progress_event", fail_text_progress)

    with pytest.raises(orchestrator.ProductionProgressPublicationError):
        _run()

    failed = production_store.load_artifact("session-1", "pair-1", "state")
    assert failed["status"] == "FAILED"
    assert failed["reason_code"] == "ProductionProgressPublicationError"


def test_sequential_question_and_synthesis_progress_never_both_run(
    tmp_path, monkeypatch
):
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[_atom("text-voltage", "TEXT")],
        graphic_atoms=[],
    )
    original_publish = orchestrator._publish_progress_event
    snapshots = []

    def record_progress(*args, **kwargs):
        state = original_publish(*args, **kwargs)
        snapshots.append(copy.deepcopy(state.get("stages") or {}))
        return state

    monkeypatch.setattr(orchestrator, "_publish_progress_event", record_progress)

    _run()

    assert snapshots
    assert not any(
        (stages.get("review_questions") or {}).get("status") == "RUNNING"
        and (stages.get("unified_synthesis") or {}).get("status") == "RUNNING"
        for stages in snapshots
    )


def test_text_progress_reports_real_algorithm_boundaries(monkeypatch):
    preparation = {"input_signature": "preparation"}
    differences = {"source_signature": "differences"}
    fact_production = {
        "input_signature": "facts",
        "facts": [],
        "not_applicable_source_evidence": [],
    }
    semantic = {
        "kind": orchestrator.SEMANTIC_KIND,
        "schema_version": orchestrator.SEMANTIC_SCHEMA_VERSION,
        "stage3_signature": "differences",
        "text_fact_production_signature": "facts",
        "input_signature": "semantic",
        "facts": [],
    }
    monkeypatch.setattr(
        orchestrator, "prepare_text_scope", lambda *_args, **_kwargs: preparation
    )
    monkeypatch.setattr(
        orchestrator,
        "build_text_differences_from_preparation",
        lambda _preparation: differences,
    )
    monkeypatch.setattr(
        orchestrator,
        "produce_text_facts",
        lambda _differences, _preparation: fact_production,
    )
    monkeypatch.setattr(
        orchestrator,
        "stage3_content_signature",
        lambda _differences: "differences",
    )
    monkeypatch.setattr(
        orchestrator,
        "build_semantic_validation",
        lambda *_args, **_kwargs: semantic,
    )
    monkeypatch.setattr(
        orchestrator,
        "build_text_atoms",
        lambda *_args, **_kwargs: {"atoms": []},
    )
    events = []
    token = orchestrator._TEXT_PROGRESS_CALLBACK.set(
        lambda **event: events.append(event)
    )
    try:
        orchestrator._run_text_branch({}, "pair-1", [], {}, None)
    finally:
        orchestrator._TEXT_PROGRESS_CALLBACK.reset(token)

    assert [event["substage"] for event in events] == [
        "text_preparation",
        "text_difference_search",
        "text_difference_validation",
        "text_change_formation",
    ]
    assert all("processed" not in event and "total" not in event for event in events)


@pytest.mark.parametrize(
    ("group_count", "determinate"),
    [(1, False), (3, True)],
)
def test_graphic_group_progress_is_determinate_only_for_multiple_units(
    monkeypatch, group_count, determinate
):
    groups = [
        {
            "id": f"group-{index}",
            "left_pages": [index + 1],
            "right_pages": [index + 11],
        }
        for index in range(group_count)
    ]
    monkeypatch.setattr(
        orchestrator,
        "_document_graphic_entry",
        lambda *_args, **_kwargs: {"status": "NOT_APPLICABLE"},
    )
    monkeypatch.setattr(
        orchestrator,
        "_build_document_graphic_bundle",
        lambda entries: {"entries": list(entries)},
    )
    monkeypatch.setattr(
        orchestrator,
        "_validate_document_graphic_bundle",
        lambda bundle: bundle,
    )
    monkeypatch.setattr(
        orchestrator.production_store,
        "save_artifact",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        orchestrator,
        "_document_graphic_stage",
        lambda _bundle: {
            "status": "NOT_APPLICABLE",
            "source_state": "NOT_APPLICABLE",
        },
    )
    events = []
    token = orchestrator._GRAPHIC_PROGRESS_CALLBACK.set(
        lambda **event: events.append(event)
    )
    try:
        orchestrator._run_graphic_branch(
            "session-1",
            "pair-1",
            {},
            {
                "input_mode": "DOCUMENT",
                "left_block_ids": [],
                "right_block_ids": [],
            },
            groups,
        )
    finally:
        orchestrator._GRAPHIC_PROGRESS_CALLBACK.reset(token)

    assert len(events) == group_count * 2
    assert all(event["current_item"]["group_id"] for event in events)
    if determinate:
        assert all(event["total"] == group_count for event in events)
        assert events[-1]["processed"] == group_count
        assert len(events[-1]["recent_unit_durations_ms"]) == group_count
    else:
        assert all(
            event["processed"] is None and event["total"] is None
            for event in events
        )


def test_terminal_state_clears_current_operation_and_preserves_stage_progress(
    tmp_path, monkeypatch
):
    monkeypatch.setenv(
        orchestrator.PROGRESS_ACTIVITY_WARNING_ENV,
        "37",
    )
    _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[_atom("text-voltage", "TEXT")],
        graphic_atoms=[],
    )

    state = _run()

    assert state["progress"] == 100
    assert isinstance(state["progress"], int)
    assert state["last_activity_at"] == state["completed_at"]
    assert state["duration_ms"] >= 0
    for field in (
        "current_stage",
        "current_substage",
        "message",
        "processed",
        "total",
        "unit",
        "current_item",
    ):
        assert state[field] is None
    assert state["recent_unit_durations_ms"] == []
    assert state["constraints"]["activity_warning_threshold_sec"] == 37
    for stage_key in (
        "sheet_matching",
        "text",
        "graphic",
        "entity_matching",
        "review_questions",
        "unified_synthesis",
        "final_report",
    ):
        progress = state["stages"][stage_key]["progress"]
        assert progress["started_at"]
        assert progress["last_activity_at"]
        assert progress["duration_ms"] >= 0
    questions = state["stages"]["review_questions"]
    assert questions["total"] == questions["pending"] + questions["answered"]


def test_failed_run_preserves_failed_location_and_clears_current_operation(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    production_store.save_artifact(
        "session-1",
        "pair-1",
        "state",
        {
            "run_id": "run-1",
            "status": "RUNNING",
            "revision": 2,
            "started_at": "2026-01-01T00:00:00+00:00",
            "current_stage": "content_analysis",
            "current_substage": "text_difference_search",
            "message": "Поиск различий в тексте…",
            "processed": None,
            "total": None,
            "unit": None,
            "current_item": None,
            "recent_unit_durations_ms": [],
            "stages": {
                "text": {
                    "status": "RUNNING",
                    "progress": {
                        "status": "RUNNING",
                        "started_at": "2026-01-01T00:00:00+00:00",
                    },
                }
            },
        },
    )
    monkeypatch.setattr(
        orchestrator,
        "_run_production_comparison_impl",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("private")),
    )

    with pytest.raises(RuntimeError, match="private"):
        orchestrator._run_production_comparison_locked(
            "session-1",
            "pair-1",
            input_mode="PAGE",
            left_pages=[1],
            right_pages=[1],
        )

    failed = production_store.load_artifact(
        "session-1", "pair-1", "state"
    )
    assert failed["status"] == "FAILED"
    assert failed["progress"] == 100
    assert failed["failed_stage"] == "content_analysis"
    assert failed["failed_substage"] == "text_difference_search"
    assert failed["current_stage"] is None
    assert failed["current_substage"] is None
    assert failed["message"] is None
    assert failed["reason_code"] == "RuntimeError"
    assert failed["stages"]["text"]["status"] == "FAILED"
    assert failed["stages"]["text"]["progress"]["status"] == "FAILED"

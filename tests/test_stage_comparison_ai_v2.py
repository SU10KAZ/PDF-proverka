"""AI Analyst v2: whole-sheet context, fail-closed verification and safety."""
from __future__ import annotations

from copy import deepcopy

import pytest

from backend.app.services.stage_comparison.ai import cache as legacy_cache
from backend.app.services.stage_comparison.ai import gateway, response_contract
from backend.app.services.stage_comparison.ai_v2 import context, expansion, inventory
from backend.app.services.stage_comparison.ai_v2 import schemas, settings, verifier
from backend.app.services.stage_comparison.ai_v2.engine import WholeDocumentAnalyst
from scripts.stage_comparison_ai_v2_experiment import (
    _benchmark_row,
    _ensure_manual_audit,
)


def _node(node_id: str, label: str, section: str) -> dict:
    return {
        "id": node_id,
        "type": "LOAD",
        "label": label,
        "canonical_identity": label,
        "section": section,
        "confidence": 0.8,
        "source_tokens": [label, "20A"],
        "attrs": {"rating_a": 20, "status": "ACTIVE"},
    }


def _row(row_id: str, side: str, label: str, value: float) -> dict:
    return {
        "row_id": row_id,
        "side": side,
        "consumer_label": label,
        "own_designations": [label],
        "row_designations": [],
        "feeder_designations": [],
        "section_ref": "S1",
        "row_kind": "FEEDER",
        "mode_label": "Рабочий",
        "cables": ["5x16"],
        "values": [{
            "facet_ref": "power_kw",
            "values": [value],
            "unit": "кВт",
            "raw": f"P={value:g} кВт",
            "mode_label": "Рабочий",
            "mode_status": "PROVEN",
        }],
    }


@pytest.fixture
def artifacts() -> dict:
    left = _node("L1", "Насос", "S1")
    right = _node("R1", "Насос", "S1")
    edge_left = {
        "id": "E1", "type": "FEEDS", "from": "BUS1", "to": "L1",
        "confidence": 0.9, "source_tokens": [],
    }
    edge_right = {
        "id": "E2", "type": "FEEDS", "from": "BUS1", "to": "R1",
        "confidence": 0.9, "source_tokens": [],
    }
    direct = {
        "sources": {
            "LEFT": {"page_index_0based": 0, "block_id": "BL", "document": {
                "document_code": "A", "version_id": "v1",
            }},
            "RIGHT": {"page_index_0based": 0, "block_id": "BR", "document": {
                "document_code": "B", "version_id": "v2",
            }},
        },
        "left_graph": {
            "discipline": "EOM", "profile_id": "generic",
            "nodes": [left], "edges": [edge_left], "quality": {},
        },
        "right_graph": {
            "discipline": "EOM", "profile_id": "generic",
            "nodes": [right], "edges": [edge_right], "quality": {},
        },
        "diagnostics": {"electrical_load_tables": {
            "LEFT": {"rows": [_row("RL", "LEFT", "Насос", 10)], "counts": {"rows": 1}},
            "RIGHT": {"rows": [_row("RR", "RIGHT", "Насос", 12)], "counts": {"rows": 1}},
        }},
        "comparison_result": {
            "comparison_quality": {
                "left_graph_valid": True, "right_graph_valid": True,
                "left_identity_coverage": 1.0, "right_identity_coverage": 1.0,
            },
            "functional_groups": {},
            "matching": {
                "metrics": {"left_nodes": 1, "right_nodes": 1},
                "ambiguous": [{
                    "left_id": "L1",
                    "right_candidates": [{"right_id": "R1", "score": 0.8}],
                }],
                "unmatched_left": ["L1"], "unmatched_right": ["R1"],
            },
        },
    }
    legacy_inventory = {
        "items": [{
            "item_id": "change-1",
            "kind": "CHANGE_INCOMPLETE_EVIDENCE",
            "decision": "AI_ELIGIBLE",
            "unresolved": True,
            "reason_code": "BOTH_SIDES_READABLE",
            "reason": "read",
            "available_evidence": ["both"],
            "missing_evidence": ["identity"],
            "subject": "Насос",
            "summary": "Насос: значение изменено.",
            "routing_payload": {"change_id": "C1"},
        }, {
            "item_id": "no-evidence",
            "kind": "TEXT_REVIEW",
            "decision": "AI_INELIGIBLE_INSUFFICIENT_EVIDENCE",
            "unresolved": True,
            "reason_code": "OPPOSITE_SIDE_NOT_RECOGNISED",
            "missing_evidence": ["LEFT"],
            "summary": "текст",
        }, {
            "item_id": "human-only",
            "kind": "TABLE_ROW_BLOCKED",
            "decision": "AI_INELIGIBLE_POLICY",
            "unresolved": True,
            "reason_code": "COMPARING_DIFFERENT_MODES_FORBIDDEN",
            "missing_evidence": ["mode"],
            "summary": "режимы",
        }],
    }
    structured = {
        "left_nodes": ["L1"], "right_nodes": ["R1"],
        "relation": {"left_value": 10, "right_value": 12, "unit": "кВт"},
    }
    change = {
        "change_id": "C1", "subject_ref": "pump", "facet_ref": "power",
        "dimension": "PARAMETER", "direction": "INCREASED",
        "outcome": "REVIEW_REQUIRED", "confidence": {"level": "LOW"},
        "provenance": {"source_atoms": [{"provenance": {"structured": structured}}]},
    }
    return {
        "direct_page_mode2": direct,
        "ai_routing_inventory": legacy_inventory,
        "unified_synthesis": {"changes": [change], "review_items": []},
        "document_inconsistencies": {"items": [{
            "inconsistency_id": "I1", "kind": "LABEL_CONFLICT",
            "side": "RIGHT", "subject": "R1",
            "summary": "Подпись отличается от секции.",
            "evidence": {"label_section": "2", "geometric_section": "1"},
        }]},
        "text_preparation": {"comparison_groups": [], "fragments": {}},
        "sheet_relations": {"relations": [], "sheet_labels": {}},
        "preliminary_report": {"summary": {"counts": {
            "automatic": 2, "review": 3, "inconsistency": 1, "unproven": 1,
        }}},
        "engineer_decisions": {"decisions": []},
        "state": {"status": "COMPLETED", "duration_ms": 5},
    }


def _bundle(artifacts: dict):
    inv = inventory.build_inventory(
        legacy_inventory=artifacts["ai_routing_inventory"],
        direct_page=artifacts["direct_page_mode2"], pair_id="p",
    )
    return inv, context.build_context_bundle(
        artifacts=artifacts, inventory=inv, pair_id="p"
    )


def _claim(kind="IDENTITY_FEATURE", **overrides):
    value = {
        "kind": kind,
        "subject_ref": "LEFT:NODE:L1",
        "object_ref": "RIGHT:NODE:R1",
        "attribute": "canonical_identity",
        "value": "Насос",
        "unit": None,
        "operation": "NONE",
        "operands": [],
        "expected": None,
        "evidence_refs": ["LEFT:NODE:L1", "RIGHT:NODE:R1"],
    }
    value.update(overrides)
    return value


def _resolution(task_id: str, task_type=schemas.FUNCTIONAL_IDENTITY, **overrides):
    value = {
        "task_id": task_id,
        "task_type": task_type,
        "status": "RESOLVED",
        "verdict": "SAME_ENTITY",
        "selected_candidate_refs": ["LEFT:NODE:L1", "RIGHT:NODE:R1"],
        "evidence_refs": ["LEFT:NODE:L1", "RIGHT:NODE:R1"],
        "claims": [_claim()],
        "confidence": "HIGH",
        "requested_evidence": [],
        "engineering_summary": "Узлы соответствуют друг другу.",
        "human_question": None,
    }
    value.update(overrides)
    return value


def test_sheet_context_contains_both_levels_and_stable_refs(artifacts):
    inv, bundle = _bundle(artifacts)
    assert bundle.sheet_context["sides"]["LEFT"]["document_code"] == "A"
    assert len(bundle.sheet_context["entities"]) == 2
    assert len(bundle.sheet_context["table_rows"]) == 2
    assert "LEFT:NODE:L1" in bundle.evidence_catalog
    assert set(bundle.focused_by_task) == {
        item["task_id"] for item in inv["items"] if item["unresolved"]
    }
    focus = bundle.focused_by_task["change-1"]
    assert {"LEFT:ROW:RL", "RIGHT:ROW:RR"} <= set(focus["context_refs"])
    assert not ({"LEFT:ROW:RL", "RIGHT:ROW:RR"} & set(focus["candidate_refs"]))


def test_benchmark_row_records_three_sessions_and_ignores_volatile_timestamps():
    run = {"diagnostics": {
        "model": settings.MODEL,
        "reasoning_effort": "low",
        "duration_ms": 900,
        "model_calls": 3,
        "sessions": 3,
        "cache": {"hits": 0},
        "session_metrics": [
            {"sequence": index, "duration_ms": 300, "prompt_bytes": 1000}
            for index in range(1, 4)
        ],
    }}
    materialization = {
        "unified_synthesis": {"generated_at": "cold", "changes": []},
        "human_review_plan": {
            "generated_at": "cold",
            "summary": {"review_groups": 1, "standalone_human_questions": 5,
                        "mandatory_human_interactions": 6, "mode_atoms": 14},
        },
        "preliminary_report": {"generated_at": "cold", "summary": {"counts": {}}},
        "document_inconsistencies": {"generated_at": "cold", "items": []},
    }
    cold = _benchmark_row(run, materialization)
    warm = _benchmark_row(
        {"diagnostics": {**run["diagnostics"], "model_calls": 0, "sessions": 0}},
        {
            key: ({**value, "generated_at": "warm"} if isinstance(value, dict) else value)
            for key, value in materialization.items()
        },
    )

    assert cold["model_calls"] == 3
    assert cold["sessions"] == 3
    assert cold["prompt_bytes_total"] == 3000
    assert cold["human_review"]["mandatory_interactions"] == 6
    assert cold["read_model_signature"] == warm["read_model_signature"]


def test_compact_context_keeps_every_evidence_ref_and_is_smaller(artifacts):
    _inv, bundle = _bundle(artifacts)
    compact = context.model_sheet_view(bundle.sheet_context)
    indexed = {item["ref"] for item in compact["evidence_index"]}
    assert indexed == set(bundle.evidence_catalog)
    assert context.serialized_bytes(compact) < context.serialized_bytes(
        context.legacy_model_sheet_view(bundle.sheet_context)
    )


def test_focused_model_evidence_drops_only_repeated_transport_fields(artifacts):
    _inv, bundle = _bundle(artifacts)
    original = bundle.evidence_catalog["LEFT:ROW:RL"]
    compact = context.model_evidence_view(original)
    assert compact["values"][0]["values"] == original["values"][0]["values"]
    assert compact["values"][0]["unit"] == original["values"][0]["unit"]
    assert compact["cables"] == original["cables"]
    assert "text" not in compact and "raw" not in compact["values"][0]


def test_quality_selected_batching_covers_each_table_task_once(monkeypatch, artifacts):
    old = deepcopy(artifacts["ai_routing_inventory"])
    old["items"][0]["kind"] = "TABLE_ROW_UNPROVEN"
    old["items"][0]["routing_payload"] = {
        "row_id": "RL", "side": "LEFT", "candidate_row_ids": ["RR"],
    }
    artifacts = deepcopy(artifacts)
    artifacts["ai_routing_inventory"] = old
    monkeypatch.setenv(settings.FEATURE_FLAG, "true")
    analyst = WholeDocumentAnalyst(
        artifacts=artifacts,
        pair_id="p",
        effort="low",
        call=lambda *args, **kwargs: gateway.CallResult(
            "CODEX_SESSION", settings.MODEL, "low", False, error="unused"
        ),
    )
    packages = analyst._table_packages()
    assert len(packages) == 1
    table_ids = [
        question.source_item_id
        for package in packages for question in package.questions
    ]
    eligible_table_ids = [
        item["task_id"] for item in inventory.eligible_items(analyst.inventory)
        if item["task_type"] == schemas.TABLE_ROW_IDENTITY
    ]
    assert table_ids == eligible_table_ids


def test_unresolved_inventory_is_complete_and_routes_every_eligible(artifacts):
    inv, _ = _bundle(artifacts)
    counts = inv["counts"]
    assert counts["total_engineering_unresolved"] == 4
    assert counts["routed"] + counts["not_routed"] == 4
    assert counts[inventory.ELIGIBLE] == counts["routed"] == 2
    assert counts[inventory.NO_EVIDENCE] == 1
    assert counts[inventory.POLICY] == 1
    assert counts[inventory.HUMAN_AUTHORITY] == 0


def test_duplicate_source_ids_do_not_silently_drop_facets(artifacts):
    old = deepcopy(artifacts["ai_routing_inventory"])
    duplicate = deepcopy(old["items"][2])
    duplicate["summary"] = "другой параметр того же match"
    old["items"].append(duplicate)
    inv = inventory.build_inventory(
        legacy_inventory=old, direct_page=artifacts["direct_page_mode2"],
    )
    ids = [item["task_id"] for item in inv["items"] if item["unresolved"]]
    assert len(ids) == len(set(ids))


def test_table_questions_are_remapped_to_unique_v2_routes(
    monkeypatch, artifacts,
):
    old = deepcopy(artifacts["ai_routing_inventory"])
    eligible = old["items"][0]
    eligible["kind"] = "TABLE_ROW_UNPROVEN"
    eligible["routing_payload"] = {
        "row_id": "RL", "side": "LEFT", "candidate_row_ids": ["RR"],
    }
    collision = deepcopy(old["items"][1])
    collision["item_id"] = eligible["item_id"]
    old["items"].insert(0, collision)
    artifacts = deepcopy(artifacts)
    artifacts["ai_routing_inventory"] = old
    monkeypatch.setenv(settings.FEATURE_FLAG, "true")
    analyst = WholeDocumentAnalyst(
        artifacts=artifacts, pair_id="p", effort="low",
        call=lambda *args, **kwargs: gateway.CallResult(
            "CODEX_SESSION", settings.MODEL, "low", False, error="unused"
        ),
    )
    packages = analyst._table_packages()
    questions = [question for package in packages for question in package.questions]
    assert len(questions) == 1
    assert questions[0].source_item_id.startswith("aiv2_task")
    assert questions[0].source_item_id in {
        item["task_id"] for item in analyst.inventory["items"]
    }


@pytest.mark.parametrize("task_type", schemas.TASK_TYPES)
def test_each_task_type_has_a_strict_schema(task_type):
    schema = schemas.TASK_SCHEMAS[task_type]
    assert schema["additionalProperties"] is False
    resolution = schema["properties"]["resolution"]
    assert resolution["additionalProperties"] is False
    assert resolution["properties"]["task_type"]["const"] == task_type


def test_table_identity_schema_remains_strict():
    from backend.app.services.stage_comparison.ai import identity
    assert identity.IDENTITY_SCHEMA["additionalProperties"] is False
    assert identity.IDENTITY_SCHEMA["properties"]["resolutions"]["items"][
        "additionalProperties"
    ] is False


def test_functional_identity_and_graph_claim_are_verified(artifacts):
    inv, bundle = _bundle(artifacts)
    task = next(item for item in inv["items"] if item["source_kind"] == "GRAPH_ENTITY_AMBIGUITY")
    edge_claim = _claim(
        kind="GRAPH_RELATION", attribute="relation", value="FEEDS",
        subject_ref="LEFT:NODE:L1", object_ref=None,
        evidence_refs=["LEFT:EDGE:E1"],
    )
    # A relation to BUS1 needs an addressable BUS node; the exact invalid
    # claim is rejected, while identity remains independently valid.
    bad = _resolution(task["task_id"], claims=[_claim(), edge_claim])
    assert not verifier.verify_resolution(task, bad, bundle).ok
    good = _resolution(task["task_id"])
    assert verifier.verify_resolution(task, good, bundle).ok


def test_arithmetic_is_recomputed_not_trusted(artifacts):
    inv, bundle = _bundle(artifacts)
    task = next(item for item in inv["items"] if item["source_kind"] == "GRAPH_ENTITY_AMBIGUITY")
    arithmetic = _claim(
        kind="ARITHMETIC", subject_ref=None, object_ref=None, attribute=None,
        value=None, operation="SUM", expected=40,
        operands=[
            {"evidence_ref": "LEFT:NODE:L1", "value": 20},
            {"evidence_ref": "RIGHT:NODE:R1", "value": 20},
        ],
    )
    assert verifier.verify_resolution(
        task, _resolution(task["task_id"], claims=[_claim(), arithmetic]), bundle
    ).ok
    arithmetic["expected"] = 41
    assert not verifier.verify_resolution(
        task, _resolution(task["task_id"], claims=[_claim(), arithmetic]), bundle
    ).ok


def test_change_cannot_use_fast_question_as_its_own_value_proof(artifacts):
    inv, bundle = _bundle(artifacts)
    task = next(item for item in inv["items"] if item["task_id"] == "change-1")
    resolution = _resolution(
        task["task_id"], task_type=schemas.CHANGE_INTERPRETATION,
        verdict="SUPPORTED_CHANGE",
        selected_candidate_refs=[
            "FAST:CHANGE:C1", "LEFT:NODE:L1", "RIGHT:NODE:R1",
        ],
        evidence_refs=["FAST:CHANGE:C1", "LEFT:NODE:L1", "RIGHT:NODE:R1"],
        claims=[
            _claim(),
            _claim(
                kind="VALUE", subject_ref="LEFT:NODE:L1", object_ref=None,
                attribute="count", value=1,
                evidence_refs=["FAST:CHANGE:C1"],
            ),
            _claim(
                kind="VALUE", subject_ref="RIGHT:NODE:R1", object_ref=None,
                attribute="count", value=3,
                evidence_refs=["FAST:CHANGE:C1"],
            ),
        ],
    )
    check = verifier.verify_resolution(task, resolution, bundle)
    assert not check.ok
    assert any("независимыми evidence" in error for error in check.errors)


def test_formatting_only_requires_equal_independent_row_values(artifacts):
    inv, bundle = _bundle(artifacts)
    task = next(item for item in inv["items"] if item["task_id"] == "change-1")
    for ref in ("LEFT:ROW:RL", "RIGHT:ROW:RR"):
        bundle.evidence_catalog[ref]["cable_count"] = 3
    resolution = _resolution(
        task["task_id"], task_type=schemas.CHANGE_INTERPRETATION,
        verdict="FORMATTING_ONLY",
        selected_candidate_refs=[
            "FAST:CHANGE:C1", "LEFT:NODE:L1", "RIGHT:NODE:R1",
        ],
        evidence_refs=[
            "FAST:CHANGE:C1", "LEFT:NODE:L1", "RIGHT:NODE:R1",
            "LEFT:ROW:RL", "RIGHT:ROW:RR",
        ],
        claims=[
            _claim(),
            _claim(
                kind="VALUE", subject_ref="LEFT:ROW:RL", object_ref=None,
                attribute="count", value=3, evidence_refs=["LEFT:ROW:RL"],
            ),
            _claim(
                kind="VALUE", subject_ref="RIGHT:ROW:RR", object_ref=None,
                attribute="count", value=3, evidence_refs=["RIGHT:ROW:RR"],
            ),
        ],
    )
    assert verifier.verify_resolution(task, resolution, bundle).ok


@pytest.mark.parametrize("mutation", ["value", "entity", "relation"])
def test_invented_evidence_is_rejected(artifacts, mutation):
    inv, bundle = _bundle(artifacts)
    task = next(item for item in inv["items"] if item["source_kind"] == "GRAPH_ENTITY_AMBIGUITY")
    resolution = _resolution(task["task_id"])
    if mutation == "value":
        resolution["claims"] = [_claim(value="Выдуманный объект")]
    elif mutation == "entity":
        resolution["claims"] = [_claim(subject_ref="LEFT:NODE:invented")]
    else:
        resolution["claims"] = [_claim(
            kind="GRAPH_RELATION", attribute="relation", value="INVENTED",
            evidence_refs=["LEFT:EDGE:E1"],
        )]
    assert not verifier.verify_resolution(task, resolution, bundle).ok


def test_expansion_has_allowlist_and_two_step_budget(artifacts):
    inv, bundle = _bundle(artifacts)
    task = next(item for item in inv["items"] if item["source_kind"] == "GRAPH_ENTITY_AMBIGUITY")
    assert expansion.expand_focus(bundle, task["task_id"], ["filesystem_search"]) == []
    assert set(schemas.EXPANSION_ALLOWLIST) == {
        "neighboring_rows", "neighboring_entities", "summary_row",
        "opposite_section_peer", "graph_neighbors", "larger_text_window",
        "bounded_image_crop",
    }
    budget = expansion.ExpansionBudget(2)
    assert budget.take() and budget.take() and not budget.take()


def test_feature_flag_is_real_and_fail_closed(monkeypatch, artifacts):
    monkeypatch.delenv(settings.FEATURE_FLAG, raising=False)
    with pytest.raises(RuntimeError, match=settings.FEATURE_FLAG):
        WholeDocumentAnalyst(artifacts=artifacts, pair_id="p", effort="low")
    monkeypatch.setenv(settings.FEATURE_FLAG, "true")
    analyst = WholeDocumentAnalyst(
        artifacts=artifacts, pair_id="p", effort="low",
        call=lambda *args, **kwargs: gateway.CallResult(
            "CODEX_SESSION", settings.MODEL, "low", False, error="stop"
        ),
    )
    assert analyst.effort == "low"


def test_cache_identity_includes_effort_prompt_and_schema():
    base = dict(
        evidence_digest="e", model=settings.MODEL, prompt_version="p",
        schema_version="s", role="r", schema_digest="schema",
    )
    low = legacy_cache.cache_key(
        **base, reasoning_level="low", prompt_digest="one"
    )
    medium = legacy_cache.cache_key(
        **base, reasoning_level="medium", prompt_digest="one"
    )
    changed = legacy_cache.cache_key(
        **base, reasoning_level="low", prompt_digest="two"
    )
    assert len({low, medium, changed}) == 3


def test_timeout_and_cancel_leave_work_for_human(monkeypatch, artifacts):
    monkeypatch.setenv(settings.FEATURE_FLAG, "true")

    def timed_out(*args, **kwargs):
        return gateway.CallResult(
            "CODEX_SESSION", settings.MODEL, "low", False,
            error="timeout", error_kind="TIMEOUT",
        )

    timeout_run = WholeDocumentAnalyst(
        artifacts=artifacts, pair_id="p", effort="low", call=timed_out,
    ).run()
    assert timeout_run["diagnostics"]["model_timeouts"] == 1
    assert timeout_run["diagnostics"]["unsupported_published"] == 0

    token = gateway.CancelToken()
    token.cancel()

    def cancelled(*args, **kwargs):
        assert kwargs["cancel"].cancelled
        return gateway.CallResult(
            "CODEX_SESSION", settings.MODEL, "low", False,
            error="cancelled", error_kind="CANCELLED",
        )

    cancelled_run = WholeDocumentAnalyst(
        artifacts=artifacts, pair_id="p", effort="low", call=cancelled,
        cancel=token,
    ).run()
    assert cancelled_run["diagnostics"]["human_required"] > 0
    assert cancelled_run["constraints"]["unsupported_results_published"] is False


def test_summary_contains_only_verified_finding_ids(monkeypatch, artifacts):
    monkeypatch.setenv(settings.FEATURE_FLAG, "true")
    analyst = WholeDocumentAnalyst(
        artifacts=artifacts, pair_id="p", effort="low",
        call=lambda *args, **kwargs: gateway.CallResult(
            "CODEX_SESSION", settings.MODEL, "low", False, error="unused"
        ),
    )
    report = analyst._preliminary_report([
        {"task_id": "change-1", "status": "AI_RESOLVED_VERIFIED",
         "saves_human_decision": True,
         "resolution": {"engineering_summary": "Проверенный вывод."}},
        {"task_id": "no-evidence", "status": "HUMAN_REQUIRED",
         "saves_human_decision": False},
    ], {"changes": []})
    assert report["verified_resolution_ids"] == ["change-1"]
    assert report["engineering_narrative"] == [{
        "text": "Проверенный вывод.", "finding_ids": ["change-1"],
    }]
    assert report["constraints"]["summary_from_verified_only"] is True


def test_batch_schema_rejects_unknown_fields():
    payload = {"resolutions": [_resolution("x") | {"shell": "ls"}]}
    errors = response_contract.validate(payload, schemas.ANALYST_SCHEMA)
    assert any("shell" in error for error in errors)


def test_completed_manual_audit_survives_identical_cache_replay(tmp_path):
    path = tmp_path / "manual_audit.json"
    path.write_text(
        '{"status":"COMPLETE","items":[{"task_id":"x",'
        '"manual_verdict":"SUPPORTED","note":"checked"}]}',
        encoding="utf-8",
    )
    run = {"resolutions": [{"task_id": "x", "status": "AI_RESOLVED_VERIFIED"}]}
    _ensure_manual_audit(path, run)
    assert '"checked"' in path.read_text(encoding="utf-8")

    run["resolutions"][0]["task_id"] = "changed"
    _ensure_manual_audit(path, run)
    assert "PENDING_MANUAL_AUDIT" in path.read_text(encoding="utf-8")

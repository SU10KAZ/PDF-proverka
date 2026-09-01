"""AI Analyst v2 verified relations become ordinary deterministic findings."""
from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

from backend.app.pipeline.stages.block_grounding.graph_identity_matcher import (
    apply_verified_entity_relations,
    match_graph_nodes,
)
from backend.app.pipeline.stages.block_grounding.system_graph import (
    SCHEMA_VERSION as GRAPH_SCHEMA_VERSION,
    make_node,
)
from backend.app.pipeline.stages.block_grounding.system_graph_comparator import (
    compare_system_graphs,
)
from backend.app.services.stage_comparison.ai_v2.materialization import (
    HUMAN_REQUIRED,
    MATERIALIZED_FINDING,
    NO_CHANGE,
    REJECTED_VERIFIER,
    build_verified_entity_relations,
    materialize_verified_resolutions,
)
from backend.app.services.stage_comparison.engineer_review import (
    build_engineer_decisions,
)
from backend.app.services.stage_comparison.unified_change_synthesizer import (
    synthesize_unified_changes,
)


ROOT = Path(__file__).resolve().parents[1]


def _evidence(value: str) -> list[dict]:
    return [{
        "kind": "token",
        "role": "test",
        "value": value,
        "bbox": [1.0, 1.0, 2.0, 2.0],
        "source_tokens": [value],
    }]


def _node(node_id: str, canonical: str, rating: int) -> dict:
    return make_node(
        node_id,
        "OUTGOING_DEVICE",
        confidence=0.8,
        evidence=_evidence(node_id),
        bbox=[1.0, 1.0, 2.0, 2.0],
        source_tokens=[node_id, str(rating)],
        label=node_id,
        canonical_identity=canonical,
        attrs={"rating_a": rating, "status": "ACTIVE"},
    )


def _graph(node: dict) -> dict:
    return {
        "schema_version": GRAPH_SCHEMA_VERSION,
        "block": {"block_id": "b", "page_index": 0},
        "profile": {"profile_id": "test", "profile_version": "test"},
        "nodes": [node],
        "edges": [],
        "quality": {"identity_coverage": 1.0},
        "provenance": {"producer": "test"},
    }


def _relation() -> dict:
    return {
        "resolution_id": "r1",
        "task_id": "identity-1",
        "left_entity_ref": "LEFT:NODE:L",
        "right_entity_ref": "RIGHT:NODE:R",
        "relation_type": "SAME_ENTITY",
        "evidence_refs": ["LEFT:NODE:L", "RIGHT:NODE:R"],
        "verifier_result": {"ok": True, "errors": []},
        "confidence": "HIGH",
        "source": "AI_ANALYST_V2",
    }


def _resolution(
    task_id: str,
    *,
    verdict: str,
    task_type: str = "CHANGE_INTERPRETATION",
    source_kind: str = "TEXT_REVIEW",
    evidence_refs: list[str] | None = None,
    status: str = "AI_RESOLVED_VERIFIED",
    reason_code: str | None = None,
) -> dict:
    return {
        "task_id": task_id,
        "task_type": task_type,
        "source_kind": source_kind,
        "status": status,
        "reason_code": reason_code,
        "resolution": ({
            "task_id": task_id,
            "task_type": task_type,
            "status": "RESOLVED",
            "verdict": verdict,
            "selected_candidate_refs": list(evidence_refs or []),
            "evidence_refs": list(evidence_refs or []),
            "confidence": "HIGH",
        } if status == "AI_RESOLVED_VERIFIED" else None),
        "verifier": (
            {"ok": True, "errors": [], "verifier_version": "v"}
            if status == "AI_RESOLVED_VERIFIED"
            else None
        ),
    }


def _run(*records: dict) -> dict:
    return {
        "model": "gpt-5.6-sol",
        "reasoning_effort": "low",
        "context_signature": "context",
        "input_signature": "run",
        "resolutions": list(records),
        "derived_table": {},
    }


def test_grsh_27_to_30_keeps_fast_primary_producer_and_automatic_status():
    acceptance = (
        ROOT / "comparison" / "ai_analyst_v2"
        / "20260901_grsh_human_review_orchestrator"
    )
    provenance = json.loads(
        (acceptance / "provenance_27_to_30.json").read_text(encoding="utf-8")
    )
    materialization = json.loads(
        (acceptance / "materialization.json").read_text(encoding="utf-8")
    )
    target_id = provenance["after"]["change_id"]
    change = next(
        value for value in materialization["unified_synthesis"]["changes"]
        if value["change_id"] == target_id
    )
    report_item = next(
        item
        for section in materialization["preliminary_report"]["sections"]
        for item in section.get("items") or ()
        if (item.get("navigation") or {}).get("target_id") == target_id
    )

    assert (change["before_value"], change["after_value"]) == (27, 30)
    primary = [
        (atom.get("source"), (atom.get("provenance") or {}).get("producer"))
        for atom in (change.get("provenance") or {}).get("source_atoms") or ()
    ]
    assert primary == [("GRAPHIC", "graphic-change-ledger-adapter-v1")]
    assert report_item["status"] == "Найдено автоматически"
    supporting = [
        resolution
        for atom in (change.get("provenance") or {}).get("source_atoms") or ()
        for resolution in (atom.get("provenance") or {}).get(
            "supporting_resolution"
        ) or ()
    ]
    assert supporting
    assert {item["source"] for item in supporting} == {"AI_ANALYST_V2"}


def _text_atom(atom_id: str = "text-review") -> dict:
    return {
        "atom_id": atom_id,
        "source": "TEXT",
        "scope_ref": "scope",
        "subject_ref": None,
        "project_entity_ref": None,
        "facet_ref": None,
        "dimension": "UNKNOWN_DIMENSION",
        "direction": "ADDED",
        "outcome": "REVIEW_REQUIRED",
        "confidence": "UNKNOWN",
        "before_value": None,
        "after_value": "Стадия Лист",
        "evidence_ref": "e:" + atom_id,
        "source_artifact": {
            "kind": "stage_comparison_text_atoms",
            "schema_version": "text-atoms.v1",
            "artifact_ref": "artifact:text",
        },
        "provenance": {"producer": "test", "locations": {"RIGHT": []}},
        "review_status": "REVIEW_REQUIRED",
    }


def _graphic_review_atom(atom_id: str = "graphic-review") -> dict:
    return {
        "atom_id": atom_id,
        "source": "GRAPHIC",
        "scope_ref": "scope",
        "subject_ref": "ВРУ4",
        "project_entity_ref": None,
        "facet_ref": "cable_parallel_count",
        "dimension": "STRUCTURE",
        "direction": "ALTERED",
        "outcome": "REVIEW_REQUIRED",
        "confidence": "LOW",
        "before_value": None,
        "after_value": None,
        "evidence_ref": "e:" + atom_id,
        "source_artifact": {
            "kind": "graphic_change_ledger",
            "schema_version": "graphic-change-ledger.v2",
            "artifact_ref": "artifact:graphic",
        },
        "provenance": {"producer": "test", "structured": {
            "level": "NODE",
            "source_level": "C",
            "subject": {"kind": "individual_node", "identity": ["ВРУ4"]},
            "left_nodes": ["L"],
            "right_nodes": ["R"],
            "left_edges": [],
            "right_edges": [],
            "relation": {},
        }},
        "review_status": "REVIEW_REQUIRED",
    }


def _artifacts(atom: dict, *, inconsistency: bool = False) -> dict:
    synthesis = synthesize_unified_changes(
        text_atoms=[atom] if atom["source"] == "TEXT" else [],
        graphic_atoms=[atom] if atom["source"] == "GRAPHIC" else [],
    )
    decisions = build_engineer_decisions(synthesis, generated_at="fixed")
    item = {
        "inconsistency_id": "doc-error",
        "kind": "LABEL_CONFLICT",
        "side": "RIGHT",
        "subject": "TS1",
        "summary": "TS1 среди TS2.",
        "verdict": "REVIEW",
        "evidence": {"bbox": [1, 1, 2, 2]},
    }
    return {
        "unified_synthesis": synthesis,
        "engineer_decisions": decisions,
        "text_atoms": {"atoms": [atom] if atom["source"] == "TEXT" else []},
        "document_inconsistencies": {"items": [item] if inconsistency else []},
        "electrical_table_changes": {},
        "preliminary_report": {"summary": {"counts": {
            "review": len(decisions["decisions"]),
        }}},
    }


def test_verified_identity_materialization_creates_typed_relation():
    record = _resolution(
        "identity-1",
        verdict="SAME_ENTITY",
        task_type="FUNCTIONAL_IDENTITY",
        source_kind="GRAPH_ENTITY_AMBIGUITY",
        evidence_refs=["LEFT:NODE:L", "RIGHT:NODE:R"],
    )
    artifact = build_verified_entity_relations(_run(record), generated_at="fixed")
    relation = artifact["relations"][0]
    assert relation["relation_type"] == "SAME_ENTITY"
    assert relation["source"] == "AI_ANALYST_V2"
    assert relation["verifier_result"]["ok"] is True
    assert relation["model_metadata"]["model"] == "gpt-5.6-sol"
    assert relation["input_context_signature"] == "context"


def test_identity_reenters_deterministic_diff_and_never_supplies_parameters():
    left, right = _graph(_node("L", "LEFT-NAME", 20)), _graph(_node("R", "RIGHT-NAME", 32))
    result = compare_system_graphs(
        left, right, verified_entity_relations={"relations": [_relation()]}
    )
    change = next(item for item in result["changes"] if item["type"] == "NODE_PARAMETER_CHANGED")
    assert change["evidence"]["reason"]["left_value"] == 20
    assert change["evidence"]["reason"]["right_value"] == 32
    assert change["evidence"]["reason"]["identity_match_method"] == "verified_entity_relation"


def test_identity_to_no_change():
    result = compare_system_graphs(
        _graph(_node("L", "LEFT-NAME", 20)),
        _graph(_node("R", "RIGHT-NAME", 20)),
        verified_entity_relations={"relations": [_relation()]},
    )
    assert not [item for item in result["changes"] if item["type"] == "NODE_PARAMETER_CHANGED"]


def test_human_confirmed_relation_has_priority_over_ai():
    left = _graph(_node("L", "LEFT", 20))
    right = {
        **_graph(_node("R", "RIGHT", 20)),
        "nodes": [_node("R", "RIGHT", 20), _node("R2", "OTHER", 20)],
    }
    base = match_graph_nodes(left, right)
    human = {
        "relation_id": "h1",
        "left_entity_ref": "L",
        "right_entity_ref": "R2",
        "relation": "SAME_ENTITY",
        "human_decision": {"decision_id": "d1"},
    }
    projected = apply_verified_entity_relations(
        left, right, base, {"relations": [_relation()]}, human_relations=[human]
    )
    assert any(
        item["left_id"] == "L" and item["right_id"] == "R2"
        for item in projected["matches"]
    )
    assert projected["verified_relation_projection"]["rejected"][0]["reason"] == (
        "LOWER_PRIORITY_RELATION_CONFLICT"
    )


def test_formatting_only_removes_exact_stage7_target_and_keeps_audit():
    atom = _text_atom()
    artifacts = _artifacts(atom)
    task_id = artifacts["unified_synthesis"]["review_items"][0]["review_evidence_id"]
    run = _run(_resolution(task_id, verdict="FORMATTING_ONLY"))
    result = materialize_verified_resolutions(
        artifacts=artifacts, run=run, pair_id="p", generated_at="fixed"
    )
    assert result["diagnostics"]["stage7_before"] == 1
    assert result["diagnostics"]["stage7_after"] == 0
    assert result["outcomes"][0]["outcome"] == NO_CHANGE
    assert result["outcomes"][0]["removed_review_target_ids"] == [task_id]


def test_document_inconsistency_materializes_without_ab_change():
    artifacts = _artifacts(_text_atom(), inconsistency=True)
    run = _run(_resolution(
        "doc-error",
        verdict="DOCUMENT_ERROR",
        task_type="LABEL_CONFLICT",
        source_kind="CONSISTENCY_REVIEW",
        evidence_refs=["FAST:INCONSISTENCY:doc-error"],
    ))
    result = materialize_verified_resolutions(
        artifacts=artifacts, run=run, pair_id="p", generated_at="fixed"
    )
    item = result["document_inconsistencies"]["items"][0]
    assert item["verdict"] == "CONFIRMED"
    assert result["outcomes"][0]["outcome"] == MATERIALIZED_FINDING
    assert result["diagnostics"]["stage7_before"] == result["diagnostics"]["stage7_after"]


def test_partial_is_not_materialized():
    atom = _text_atom()
    artifacts = _artifacts(atom)
    task_id = artifacts["unified_synthesis"]["review_items"][0]["review_evidence_id"]
    audit = {"status": "COMPLETE", "items": [{
        "task_id": task_id, "manual_verdict": "PARTIALLY_SUPPORTED",
    }]}
    result = materialize_verified_resolutions(
        artifacts=artifacts,
        run=_run(_resolution(task_id, verdict="FORMATTING_ONLY")),
        pair_id="p",
        manual_audit=audit,
        generated_at="fixed",
    )
    assert result["outcomes"][0]["outcome"] == HUMAN_REQUIRED
    assert result["diagnostics"]["stage7_after"] == 1


def test_pending_manual_audit_is_fail_closed():
    atom = _text_atom()
    artifacts = _artifacts(atom)
    task_id = artifacts["unified_synthesis"]["review_items"][0]["review_evidence_id"]
    result = materialize_verified_resolutions(
        artifacts=artifacts,
        run=_run(_resolution(task_id, verdict="FORMATTING_ONLY")),
        pair_id="p",
        manual_audit={"status": "PENDING_MANUAL_AUDIT", "items": []},
        generated_at="fixed",
    )
    assert result["outcomes"][0]["outcome"] == HUMAN_REQUIRED
    assert result["outcomes"][0]["reason_code"] == "MANUAL_AUDIT_NOT_SUPPORTED"
    assert result["diagnostics"]["stage7_after"] == 1


def test_existing_human_finding_decision_prevents_ai_overwrite():
    atom = _text_atom()
    artifacts = _artifacts(atom)
    task_id = artifacts["unified_synthesis"]["review_items"][0]["review_evidence_id"]
    artifacts["engineer_decisions"]["decisions"][0]["decision"] = "REJECTED"
    result = materialize_verified_resolutions(
        artifacts=artifacts,
        run=_run(_resolution(task_id, verdict="FORMATTING_ONLY")),
        pair_id="p",
        generated_at="fixed",
    )
    assert result["outcomes"][0]["reason_code"] == "HUMAN_DECISION_HAS_PRIORITY"
    assert result["diagnostics"]["stage7_after"] == 1


def test_supported_change_uses_extracted_cable_values_and_report_provenance():
    atom = _graphic_review_atom()
    artifacts = _artifacts(atom)
    task_id = artifacts["unified_synthesis"]["changes"][0]["change_id"]
    left = _node("L", "ВРУ4", 20)
    right = _node("R", "ВРУ4", 20)
    left["attrs"]["cables"] = ["2хППГнг(А)-HF"]
    right["attrs"]["cables"] = ["3хППГнг(А)-HF"]
    artifacts["direct_page_mode2"] = {
        "left_graph": _graph(left),
        "right_graph": _graph(right),
        "diagnostics": {"electrical_load_tables": {}},
    }
    run = _run(_resolution(
        task_id,
        verdict="SUPPORTED_CHANGE",
        source_kind="CHANGE_INCOMPLETE_EVIDENCE",
        evidence_refs=["LEFT:NODE:L", "RIGHT:NODE:R"],
    ))
    result = materialize_verified_resolutions(
        artifacts=artifacts, run=run, pair_id="p", generated_at="fixed"
    )
    assert result["outcomes"][0]["outcome"] == MATERIALIZED_FINDING
    change = next(
        item for item in result["unified_synthesis"]["changes"]
        if item.get("facet_ref") == "cable_parallel_count"
    )
    assert (change["before_value"], change["after_value"]) == (2, 3)
    assert change["review_status"] == "CONFIRMED"
    assert result["preliminary_report"]["summary"]["counts"]["ai_verified"] == 1
    report_item = result["preliminary_report"]["sections"][1]["groups"][0]["items"][0]
    assert report_item["navigation"]["kind"] == "CHANGE"


def test_verified_table_identity_reenters_ordinary_atom_and_synthesis_path():
    artifacts = _artifacts(_text_atom())
    task_id = "table-identity"
    run = _run(_resolution(
        task_id,
        verdict="SAME_ENTITY",
        task_type="TABLE_ROW_IDENTITY",
        source_kind="TABLE_UNPROVEN_ROW",
        evidence_refs=["LEFT:ROW:l", "RIGHT:ROW:r"],
    ))
    run["derived_table"] = {
        "changes": [{
            "change_id": "etchg_ai",
            "source_item_id": task_id,
            "left_row_id": "l",
            "right_row_id": "r",
            "subject": "ШУ-ХЦ",
            "facet_ref": "demand_active_power_kw",
            "base_facet_ref": "demand_active_power_kw",
            "facet_title": "Расчётная мощность",
            "before_value": 13.7,
            "after_value": 37.5,
            "direction": "INCREASED",
            "confidence": "MEDIUM",
            "match_method": "AI_IDENTITY",
            "evidence": {"LEFT": {"row_id": "l"}, "RIGHT": {"row_id": "r"}},
        }],
        "unchanged": [],
        "blocked": [],
    }
    result = materialize_verified_resolutions(
        artifacts=artifacts, run=run, pair_id="p", generated_at="fixed"
    )
    assert result["outcomes"][0]["outcome"] == MATERIALIZED_FINDING
    change = next(
        value for value in result["unified_synthesis"]["changes"]
        if value.get("facet_ref") == "demand_active_power_kw"
    )
    assert (change["before_value"], change["after_value"]) == (13.7, 37.5)
    source_atom = change["provenance"]["source_atoms"][0]
    assert source_atom["provenance"]["ai_change_resolution"]["source"] == (
        "AI_ANALYST_V2"
    )
    assert result["preliminary_report"]["summary"]["counts"]["ai_verified"] == 1


def test_exact_inventory_accounting_and_cache_replay_materialization():
    atom = _text_atom()
    artifacts = _artifacts(atom)
    task_id = artifacts["unified_synthesis"]["review_items"][0]["review_evidence_id"]
    run = _run(
        _resolution(task_id, verdict="FORMATTING_ONLY"),
        _resolution(
            "reject", verdict="INSUFFICIENT_EVIDENCE", status="HUMAN_REQUIRED",
            reason_code="VERIFIER_REJECTED",
        ),
        _resolution(
            "human", verdict="INSUFFICIENT_EVIDENCE", status="HUMAN_REQUIRED",
            reason_code="INSUFFICIENT_EVIDENCE",
        ),
    )
    first = materialize_verified_resolutions(
        artifacts=artifacts, run=run, pair_id="p", generated_at="fixed"
    )
    second = materialize_verified_resolutions(
        artifacts=artifacts, run=deepcopy(run), pair_id="p", generated_at="fixed"
    )
    assert len(first["outcomes"]) == 3
    assert {item["outcome"] for item in first["outcomes"]} == {
        NO_CHANGE, REJECTED_VERIFIER, HUMAN_REQUIRED,
    }
    assert first["input_signature"] == second["input_signature"]
    assert first["diagnostics"] == second["diagnostics"]

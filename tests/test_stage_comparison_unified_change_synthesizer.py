"""G2.4.6 deterministic UNION synthesis acceptance and production replay."""
from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from typing import Any

import pytest

from backend.app.services.stage_comparison.unified_change_synthesizer import (
    SynthesisValidationError,
    ledger_to_graphic_atoms,
    stage53_to_text_atoms,
    synthesize_unified_changes,
    validate_synthesis,
)


ROOT = Path(__file__).resolve().parents[1]
PILOT_LEDGER = (
    ROOT / "experiments/g2_4_4_3_correct_sides/ios/graphic_change_ledger.json"
)
PILOT_LEFT_GRAPH = (
    ROOT / "experiments/g2_4_4_3_correct_sides/ios/left_system_graph.json"
)
PILOT_RIGHT_GRAPH = (
    ROOT / "experiments/g2_4_4_3_correct_sides/ios/right_system_graph.json"
)
POLICY_V2 = ROOT / "tests/fixtures/g2_4_5_policy_cases_v2.json"


def _atom(
    atom_id: str,
    source: str,
    *,
    scope_ref: str = "scope-1",
    subject_ref: str = "equipment:QF1",
    project_entity_ref: str | None = "equipment:QF1",
    dimension: str = "PARAMETER",
    direction: str = "ALTERED",
    outcome: str = "MATERIAL_CHANGE",
    confidence: str = "MEDIUM",
    facet_ref: str | None = None,
    before_value: Any = None,
    after_value: Any = None,
) -> dict[str, Any]:
    return {
        "atom_id": atom_id,
        "source": source,
        "scope_ref": scope_ref,
        "subject_ref": subject_ref,
        "project_entity_ref": project_entity_ref,
        "dimension": dimension,
        "direction": direction,
        "outcome": outcome,
        "confidence": confidence,
        "evidence_ref": f"evidence:{atom_id}",
        "source_artifact": {
            "kind": "stage_comparison_high_level_project_changes"
            if source == "TEXT"
            else "graphic_change_ledger",
            "schema_version": "1.0"
            if source == "TEXT"
            else "graphic-change-ledger.v2",
            "artifact_ref": f"artifact:{source.lower()}",
        },
        "provenance": {"producer": "synthetic-acceptance-fixture-v1"},
        "facet_ref": facet_ref,
        "before_value": before_value,
        "after_value": after_value,
    }


def _candidate(
    text_atom_id: str,
    graphic_atom_id: str,
    *,
    candidate_id: str = "candidate-1",
    coverage: dict[str, str] | None = None,
    subject_relation: str = "SAME_ENTITY",
    links_by_side: dict[str, Any] | None = None,
    text_count: int = 1,
    graphic_count: int = 1,
) -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "text_atom_id": text_atom_id,
        "graphic_atom_id": graphic_atom_id,
        "subject_relation": subject_relation,
        "links_by_side": links_by_side
        if links_by_side is not None
        else {
            "LEFT": {"relation": "SAME_ENTITY", "confidence": "HIGH"},
            "RIGHT": {"relation": "SAME_ENTITY", "confidence": "HIGH"},
        },
        "source_valid": True,
        "coverage_by_side": coverage
        or {"LEFT": "CHECKED", "RIGHT": "CHECKED"},
        "document_binding_state": "DOCUMENT_BINDING_PROVEN",
        "text_count": text_count,
        "graphic_count": graphic_count,
        "subject_identity_provenance": {
            "producer": "entity-bridge-v1",
            "link_ids": [f"link:{candidate_id}"],
        },
    }


def _only_change(result: dict[str, Any]) -> dict[str, Any]:
    assert len(result["changes"]) == 1
    return result["changes"][0]


def test_text_only_voltage_change_is_preserved():
    voltage = _atom(
        "text-voltage",
        "TEXT",
        facet_ref="voltage",
        before_value="220 В",
        after_value="380 В",
        confidence="HIGH",
    )

    change = _only_change(synthesize_unified_changes(text_atoms=[voltage]))

    assert change["source_mode"] == "TEXT"
    assert (change["before_value"], change["after_value"]) == ("220 В", "380 В")
    assert change["confidence"] == {"level": "HIGH", "basis": "SINGLE_SOURCE"}


def test_text_only_temperature_range_change_is_preserved():
    temperature = _atom(
        "text-temperature",
        "TEXT",
        facet_ref="temperature_range",
        before_value="-10…+40 °C",
        after_value="-25…+50 °C",
    )

    change = _only_change(synthesize_unified_changes(text_atoms=[temperature]))

    assert change["facet_ref"] == "temperature_range"
    assert change["before_value"] == "-10…+40 °C"
    assert change["after_value"] == "-25…+50 °C"


@pytest.mark.parametrize("graphic_state", ["NOT_CHECKED", "NOT_APPLICABLE"])
def test_graphic_silence_does_not_remove_or_lower_text_change(graphic_state):
    voltage = _atom(
        "text-voltage",
        "TEXT",
        facet_ref="voltage",
        confidence="HIGH",
    )

    result = synthesize_unified_changes(
        text_atoms=[voltage],
        source_states={"TEXT": "VALID", "GRAPHIC": graphic_state},
    )

    change = _only_change(result)
    assert change["confidence"]["level"] == "HIGH"
    assert change["relation_status"] == "SINGLE_SOURCE"
    assert result["diagnostics"]["source_states"]["GRAPHIC"] == graphic_state


def test_graphic_only_qs1_to_qf3_is_preserved():
    graphic = _atom(
        "graphic-section-device",
        "GRAPHIC",
        project_entity_ref=None,
        dimension="TYPE",
        direction="REPLACED",
        before_value="QS1",
        after_value="QF3",
        confidence="HIGH",
    )

    change = _only_change(synthesize_unified_changes(graphic_atoms=[graphic]))

    assert change["source_mode"] == "GRAPHIC"
    assert (change["before_value"], change["after_value"]) == ("QS1", "QF3")


def test_strict_text_graphic_pair_becomes_one_both_change():
    text = _atom(
        "text-section-device",
        "TEXT",
        dimension="TYPE",
        direction="REPLACED",
        before_value="QS1",
        after_value="QF3",
    )
    graphic = _atom(
        "graphic-section-device",
        "GRAPHIC",
        project_entity_ref=None,
        dimension="TYPE",
        direction="REPLACED",
        before_value="SWITCH_DISCONNECTOR",
        after_value="CIRCUIT_BREAKER",
    )

    result = synthesize_unified_changes(
        text_atoms=[text],
        graphic_atoms=[graphic],
        candidates=[_candidate(text["atom_id"], graphic["atom_id"])],
    )
    change = _only_change(result)

    assert change["source_mode"] == "BOTH"
    assert change["relation_status"] == "CORROBORATING"
    assert [item["source"] for item in change["evidence_refs"]] == ["TEXT", "GRAPHIC"]
    assert result["diagnostics"]["strict_merges"] == 1


def test_m3_remains_observation_only_and_m2_provenance_is_retained():
    text = _atom("text-type", "TEXT", dimension="TYPE", direction="REPLACED")
    graphic = _atom(
        "graphic-type",
        "GRAPHIC",
        project_entity_ref=None,
        dimension="TYPE",
        direction="REPLACED",
    )
    candidate = _candidate(
        text["atom_id"], graphic["atom_id"], links_by_side={}
    )

    change = _only_change(
        synthesize_unified_changes(
            text_atoms=[text], graphic_atoms=[graphic], candidates=[candidate]
        )
    )

    assert change["source_mode"] == "BOTH"
    assert change["provenance"]["gates"]["M3"]["state"] == "REVIEW_REQUIRED"
    assert change["provenance"]["subject_identity_provenance"] == candidate[
        "subject_identity_provenance"
    ]


def test_same_entity_parameter_and_connection_remain_two_changes():
    text = _atom(
        "text-rating",
        "TEXT",
        dimension="PARAMETER",
        facet_ref="nominal_rating",
    )
    graphic = _atom(
        "graphic-connection",
        "GRAPHIC",
        project_entity_ref=None,
        dimension="CONNECTION",
    )

    result = synthesize_unified_changes(
        text_atoms=[text],
        graphic_atoms=[graphic],
        candidates=[_candidate(text["atom_id"], graphic["atom_id"])],
    )

    assert len(result["changes"]) == 2
    assert {change["dimension"] for change in result["changes"]} == {
        "PARAMETER",
        "CONNECTION",
    }
    evaluation = result["diagnostics"]["candidate_evaluations"][0]
    assert evaluation["gates"]["M4"]["state"] == "FAIL"


def test_certain_graphic_event_is_normal_change():
    graphic = _atom(
        "graphic-connection",
        "GRAPHIC",
        project_entity_ref=None,
        dimension="CONNECTION",
        confidence="HIGH",
    )

    change = _only_change(synthesize_unified_changes(graphic_atoms=[graphic]))

    assert change["review_status"] == "CONFIRMED"
    assert change["outcome"] == "MATERIAL_CHANGE"


def test_uncertain_graphic_event_remains_review_required_change():
    graphic = _atom(
        "graphic-uncertain",
        "GRAPHIC",
        project_entity_ref=None,
        dimension="STRUCTURE",
        outcome="REVIEW_REQUIRED",
        confidence="LOW",
    )

    result = synthesize_unified_changes(graphic_atoms=[graphic])
    change = _only_change(result)

    assert change["review_status"] == "REVIEW_REQUIRED"
    assert change["source_mode"] == "GRAPHIC"
    assert result["review_items"] == []


def test_contradictory_sources_preserve_both_and_create_contested_group():
    text = _atom(
        "text-added",
        "TEXT",
        dimension="STRUCTURE",
        direction="ADDED",
    )
    graphic = _atom(
        "graphic-removed",
        "GRAPHIC",
        project_entity_ref=None,
        dimension="STRUCTURE",
        direction="REMOVED",
    )

    result = synthesize_unified_changes(
        text_atoms=[text],
        graphic_atoms=[graphic],
        candidates=[_candidate(text["atom_id"], graphic["atom_id"])],
    )

    assert len(result["changes"]) == 2
    assert len(result["contested_groups"]) == 1
    assert all(
        change["relation_status"] == "CONTRADICTORY"
        and change["review_status"] == "REVIEW_REQUIRED"
        for change in result["changes"]
    )
    assert {item["source"] for item in result["contested_groups"][0]["evidence_refs"]} == {
        "TEXT",
        "GRAPHIC",
    }


def test_unknown_dimension_is_traceable_review_item_without_change_id():
    text = _atom(
        "text-unknown",
        "TEXT",
        dimension="UNKNOWN_DIMENSION",
        outcome="MATERIAL_CHANGE",
    )

    result = synthesize_unified_changes(text_atoms=[text])

    assert result["changes"] == []
    assert len(result["review_items"]) == 1
    review = result["review_items"][0]
    assert review["review_evidence_id"].startswith("ureview_")
    assert "change_id" not in review
    assert review["outcome"] == "REVIEW_REQUIRED"


def test_two_parameter_facets_have_atomic_ids_and_one_presentation_group():
    voltage = _atom("text-voltage", "TEXT", facet_ref="voltage")
    temperature = _atom(
        "text-temperature", "TEXT", facet_ref="temperature_range"
    )

    result = synthesize_unified_changes(text_atoms=[temperature, voltage])

    assert len(result["changes"]) == 2
    assert len({change["change_id"] for change in result["changes"]}) == 2
    assert len(result["presentation_groups"]) == 1
    assert result["presentation_groups"][0]["change_ids"] == sorted(
        change["change_id"] for change in result["changes"]
    )


def test_missing_parameter_facets_fail_safe_to_separate_identities():
    first = _atom("text-param-1", "TEXT")
    second = _atom("text-param-2", "TEXT")

    result = synthesize_unified_changes(text_atoms=[first, second])

    assert len(result["changes"]) == 2
    assert len({change["change_id"] for change in result["changes"]}) == 2
    assert all(
        change["provenance"]["identity"]["evidence_scope"] is not None
        for change in result["changes"]
    )


def test_graphic_corroboration_keeps_text_change_id_and_changes_content_only():
    text = _atom("text-type", "TEXT", dimension="TYPE", direction="REPLACED")
    graphic = _atom(
        "graphic-type",
        "GRAPHIC",
        project_entity_ref=None,
        dimension="TYPE",
        direction="REPLACED",
    )
    before = _only_change(synthesize_unified_changes(text_atoms=[text]))
    after = _only_change(
        synthesize_unified_changes(
            text_atoms=[text],
            graphic_atoms=[graphic],
            candidates=[_candidate(text["atom_id"], graphic["atom_id"])],
        )
    )

    assert after["change_id"] == before["change_id"]
    assert after["content_signature"] != before["content_signature"]
    assert (before["source_mode"], after["source_mode"]) == ("TEXT", "BOTH")


def test_input_candidate_and_evidence_order_do_not_change_output():
    text_a = _atom(
        "text-a", "TEXT", subject_ref="entity:A", project_entity_ref="entity:A",
        dimension="TYPE", direction="REPLACED",
    )
    graph_a = _atom(
        "graph-a", "GRAPHIC", subject_ref="entity:A", project_entity_ref=None,
        dimension="TYPE", direction="REPLACED",
    )
    text_b = _atom(
        "text-b", "TEXT", subject_ref="entity:B", project_entity_ref="entity:B",
        dimension="TYPE", direction="REPLACED",
    )
    graph_b = _atom(
        "graph-b", "GRAPHIC", subject_ref="entity:B", project_entity_ref=None,
        dimension="TYPE", direction="REPLACED",
    )
    candidates = [
        _candidate("text-a", "graph-a", candidate_id="candidate-a"),
        _candidate("text-b", "graph-b", candidate_id="candidate-b"),
    ]

    first = synthesize_unified_changes(
        text_atoms=[text_a, text_b],
        graphic_atoms=[graph_a, graph_b],
        candidates=candidates,
    )
    second = synthesize_unified_changes(
        text_atoms=[text_b, text_a],
        graphic_atoms=[graph_b, graph_a],
        candidates=list(reversed(candidates)),
    )

    assert first == second
    assert all(
        [item["source"] for item in change["evidence_refs"]] == ["TEXT", "GRAPHIC"]
        for change in first["changes"]
    )


@pytest.mark.parametrize(
    ("candidate_overrides", "failed_gate"),
    [
        ({"coverage": {"LEFT": "CHECKED", "RIGHT": "NOT_CHECKED"}}, "M7"),
        ({"subject_relation": "UNKNOWN"}, "M2"),
        ({"text_count": 1, "graphic_count": 2}, "M8"),
    ],
)
def test_unproven_merge_gates_leave_both_changes(candidate_overrides, failed_gate):
    text = _atom("text-type", "TEXT", dimension="TYPE", direction="REPLACED")
    graphic = _atom(
        "graphic-type", "GRAPHIC", project_entity_ref=None,
        dimension="TYPE", direction="REPLACED",
    )
    candidate = _candidate(text["atom_id"], graphic["atom_id"], **candidate_overrides)

    result = synthesize_unified_changes(
        text_atoms=[text], graphic_atoms=[graphic], candidates=[candidate]
    )

    assert len(result["changes"]) == 2
    evaluation = result["diagnostics"]["candidate_evaluations"][0]
    assert evaluation["merge_allowed"] is False
    assert evaluation["gates"][failed_gate]["state"] != "PASS"


def test_document_only_text_atom_is_preserved_for_review_without_change_id():
    document = _atom(
        "text-city",
        "TEXT",
        project_entity_ref=None,
        subject_ref="document:stamp",
        dimension="PARAMETER",
        facet_ref="city",
    )

    result = synthesize_unified_changes(text_atoms=[document])

    assert result["changes"] == []
    assert len(result["review_items"]) == 1
    review = result["review_items"][0]
    assert "change_id" not in review
    assert review["reason_codes"] == ["engineering_scope_unresolved"]
    assert review["evidence_refs"][0]["evidence_ref"] == "evidence:text-city"
    assert result["diagnostics"]["engineering_scope_review_atoms"] == [
        "text-city"
    ]


def test_two_engineering_scope_unresolved_text_atoms_remain_distinct():
    first = _atom(
        "text-city-a",
        "TEXT",
        project_entity_ref=None,
        subject_ref="document:stamp",
        facet_ref="city",
    )
    second = _atom(
        "text-city-b",
        "TEXT",
        project_entity_ref=None,
        subject_ref="document:stamp",
        facet_ref="city",
    )

    result = synthesize_unified_changes(text_atoms=[second, first])

    assert result["changes"] == []
    assert len(result["review_items"]) == 2
    assert len(
        {item["review_evidence_id"] for item in result["review_items"]}
    ) == 2
    assert {item["atom_id"] for item in result["review_items"]} == {
        "text-city-a",
        "text-city-b",
    }


def test_stage53_adapter_keeps_text_fact_without_any_subject_identity():
    stage53 = {
        "kind": "stage_comparison_high_level_project_changes",
        "schema_version": "1.0",
        "high_level_changes": [
            {
                "change_id": "hlc-document",
                "details": [
                    {
                        "evidence_id": "ev-document",
                        "before": "old note",
                        "after": "new note",
                    }
                ],
            }
        ],
        "detail_level_increased": [],
        "material_review": [],
        "non_material_review": [],
        "unresolved": [],
    }
    adapted = stage53_to_text_atoms(
        stage53,
        structured_facts={
            "ev-document": {
                "scope_ref": "scope-1",
                "dimension": "PARAMETER",
                "direction": "ALTERED",
                "outcome": "MATERIAL_CHANGE",
                "confidence": "MEDIUM",
                "facet_ref": "document_note",
            }
        },
        artifact_ref="artifact:stage53",
    )

    assert len(adapted["atoms"]) == 1
    assert adapted["atoms"][0]["subject_ref"] is None
    result = synthesize_unified_changes(text_atoms=adapted["atoms"])
    review = result["review_items"][0]
    assert review["subject_ref"] is None
    assert review["evidence_refs"][0]["evidence_ref"] == "ev-document"
    assert review["reason_codes"] == ["engineering_scope_unresolved"]


def test_stage53_adapter_requires_structured_facts_and_does_not_parse_summary():
    stage53 = {
        "kind": "stage_comparison_high_level_project_changes",
        "schema_version": "1.0",
        "high_level_changes": [
            {
                "change_id": "hlc-1",
                "details": [
                    {
                        "evidence_id": "ev-voltage",
                        "summary": "Напряжение изменено с 220 В на 380 В",
                        "before": "220 В",
                        "after": "380 В",
                    }
                ],
            }
        ],
        "detail_level_increased": [],
        "material_review": [],
        "non_material_review": [],
        "unresolved": [],
    }

    missing = stage53_to_text_atoms(
        stage53, structured_facts={}, artifact_ref="artifact:stage53"
    )
    adapted = stage53_to_text_atoms(
        stage53,
        structured_facts={
            "ev-voltage": {
                "scope_ref": "scope-1",
                "subject_ref": "equipment:QF1",
                "project_entity_ref": "equipment:QF1",
                "dimension": "PARAMETER",
                "direction": "INCREASED",
                "outcome": "MATERIAL_CHANGE",
                "confidence": "HIGH",
                "facet_ref": "voltage",
            }
        },
        artifact_ref="artifact:stage53",
    )

    assert missing["atoms"] == []
    assert missing["diagnostics"]["missing_structured_facts"] == ["ev-voltage"]
    assert adapted["atoms"][0]["before_value"] == "220 В"
    assert adapted["atoms"][0]["dimension"] == "PARAMETER"


def test_real_pilot_preserves_all_four_graphic_changes_without_parent_relation():
    ledger = json.loads(PILOT_LEDGER.read_text(encoding="utf-8"))
    left_graph = json.loads(PILOT_LEFT_GRAPH.read_text(encoding="utf-8"))
    right_graph = json.loads(PILOT_RIGHT_GRAPH.read_text(encoding="utf-8"))
    adapted = ledger_to_graphic_atoms(
        ledger, artifact_ref=str(PILOT_LEDGER.relative_to(ROOT))
    )
    result = synthesize_unified_changes(graphic_atoms=adapted["atoms"])
    by_evidence = {
        change["evidence_refs"][0]["evidence_ref"]: change
        for change in result["changes"]
    }

    assert len(result["changes"]) == 4
    assert result["diagnostics"]["strict_merges"] == 0
    assert adapted["diagnostics"]["scope_source"] == "DIRECT_PAGE"
    assert adapted["diagnostics"]["parent_relation_required"] is False
    assert (len(left_graph["nodes"]), len(left_graph["edges"])) == (73, 99)
    assert (len(right_graph["nodes"]), len(right_graph["edges"])) == (82, 111)
    assert left_graph["quality"]["outgoing_devices"] == 27
    assert right_graph["quality"]["outgoing_devices"] == 30
    assert (len(ledger["comparison_scope"]["left_blocks"]), len(ledger["comparison_scope"]["right_blocks"])) == (1, 1)
    assert (by_evidence["chg_6edbdea8fb72"]["before_value"], by_evidence["chg_6edbdea8fb72"]["after_value"]) == (27, 30)
    assert (by_evidence["chg_1b601fa171f2"]["before_value"], by_evidence["chg_1b601fa171f2"]["after_value"]) == (
        "QS1",
        "QF3",
    )
    assert not {"NODE_ADDED", "NODE_REMOVED"}.intersection(
        change["type"] for change in ledger["changes"]
    )


def test_corpus_v2_authorizes_no_real_cross_source_merge():
    fixture = json.loads(POLICY_V2.read_text(encoding="utf-8"))
    relation_verdicts = [
        call["expected"].get("relation_status")
        for case in fixture["cases"]
        for call in case["policy_calls"]
        if call["function"] == "evaluate_source_relation"
    ]

    assert fixture["schema_version"] == "g2.4.5-policy-cases-v2"
    assert fixture["expected_verdict_distribution"] == {
        "SINGLE_SOURCE": 8,
        "REVIEW_REQUIRED": 5,
        "UNRELATED": 1,
    }
    assert "CORROBORATING" not in relation_verdicts
    assert "CONTRADICTORY" not in relation_verdicts


def test_final_validator_rejects_identity_and_evidence_tampering():
    result = synthesize_unified_changes(
        text_atoms=[_atom("text-voltage", "TEXT", facet_ref="voltage")]
    )
    bad_id = deepcopy(result)
    bad_id["changes"][0]["change_id"] = "uchg_00000000000000000000"
    bad_order = deepcopy(result)
    bad_order["changes"][0]["content_signature"] = "0" * 64

    with pytest.raises(SynthesisValidationError, match="identity mismatch"):
        validate_synthesis(bad_id)
    with pytest.raises(SynthesisValidationError, match="content_signature"):
        validate_synthesis(bad_order)


def test_output_contains_versioned_contract_provenance_and_no_timestamp():
    result = synthesize_unified_changes(
        text_atoms=[_atom("text-voltage", "TEXT", facet_ref="voltage")]
    )

    assert result["synthesis_version"] == "unified-change-synthesis.v1"
    assert result["direction"] == "LEFT_TO_RIGHT"
    assert result["policy_version"] == "unified-change-policy-v1"
    assert result["identity_version"] == "unified-change-identity.v1"
    assert result["provenance"]["uses_llm"] is False
    assert result["validation"]["valid"] is True
    assert "timestamp" not in json.dumps(result).lower()

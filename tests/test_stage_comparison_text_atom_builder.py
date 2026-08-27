from __future__ import annotations

from copy import deepcopy

from backend.app.services.stage_comparison.text_atom_builder import build_text_atoms
from backend.app.services.stage_comparison.text_semantic_validation import (
    build_semantic_validation,
    iter_stage3_evidence,
)


def _differences(*items):
    return {
        "version": 1,
        "kind": "stage_comparison_text_differences",
        "pair_id": "pair-1",
        "source_signature": "stage3-input",
        "sheet_groups": [{
            "id": "sheet-group-1",
            "left_pages": [10],
            "right_pages": [24],
            "changed": list(items),
            "removed": [],
            "added": [],
        }],
    }


def _item(before, after, suffix):
    return {
        "before": before,
        "after": after,
        "left_fragment_ids": [f"left-{suffix}"],
        "right_fragment_ids": [f"right-{suffix}"],
        "left_pages": [10],
        "right_pages": [24],
        "left_anchors": [{
            "fragment_id": f"left-{suffix}", "page": 10,
            "bboxes": [[10, 20, 80, 36]],
        }],
        "right_anchors": [{
            "fragment_id": f"right-{suffix}", "page": 24,
            "bboxes": [[12, 22, 90, 38]],
        }],
    }


def _fact(source_ref, facet, before, after):
    return {
        "source_evidence_ref": source_ref,
        "subject_ref": "equipment:panel-1",
        "project_entity_ref": "project-equipment:panel-1",
        "dimension": "PARAMETER",
        "direction": "ALTERED",
        "outcome": "MATERIAL_CHANGE",
        "confidence": "HIGH",
        "facet_ref": facet,
        "before_value": before,
        "after_value": after,
    }


def test_voltage_and_temperature_are_two_atomic_text_changes():
    stage3 = _differences(_item(
        "Напряжение 220 В; температура -10…+40 °C",
        "Напряжение 380 В; температура -25…+50 °C",
        "params",
    ))
    source_ref = next(iter_stage3_evidence(stage3))[0]
    stage4 = build_semantic_validation(stage3, [
        _fact(source_ref, "voltage", "220 В", "380 В"),
        _fact(source_ref, "temperature_range", "-10…+40 °C", "-25…+50 °C"),
    ], generated_at="fixed")

    result = build_text_atoms(stage3, stage4, generated_at="fixed")

    assert len(result["atoms"]) == 2
    assert {atom["facet_ref"] for atom in result["atoms"]} == {
        "voltage", "temperature_range",
    }
    assert all(atom["dimension"] == "PARAMETER" for atom in result["atoms"])
    assert result["diagnostics"]["one_property_per_atom"] is True


def test_same_entity_different_dimensions_remain_two_atoms():
    stage3 = _differences(
        _item("Тип QS1", "Тип QF3", "type"),
        _item("Подключение A-B", "Подключение A-C", "connection"),
    )
    refs = [entry[0] for entry in iter_stage3_evidence(stage3)]
    facts = [
        {
            **_fact(refs[0], "device_type", "QS1", "QF3"),
            "dimension": "TYPE", "direction": "REPLACED",
        },
        {
            **_fact(refs[1], "connection", "A-B", "A-C"),
            "dimension": "CONNECTION",
        },
    ]

    atoms = build_text_atoms(stage3, build_semantic_validation(stage3, facts))["atoms"]

    assert {atom["dimension"] for atom in atoms} == {"TYPE", "CONNECTION"}
    assert len({atom["atom_id"] for atom in atoms}) == 2


def test_unresolved_text_evidence_survives_as_review_required():
    stage3 = _differences(_item("Было", "Стало", "unknown"))

    result = build_text_atoms(stage3)

    assert len(result["atoms"]) == 1
    atom = result["atoms"][0]
    assert atom["dimension"] == "UNKNOWN_DIMENSION"
    assert atom["project_entity_ref"] is None
    assert atom["review_status"] == "REVIEW_REQUIRED"
    assert atom["provenance"]["legacy_stage5_used"] is False
    assert atom["provenance"]["locations"]["LEFT"][0]["page"] == 10


def test_stale_stage4_is_rejected():
    stage3 = _differences(_item("220", "380", "voltage"))
    source_ref = next(iter_stage3_evidence(stage3))[0]
    stage4 = build_semantic_validation(stage3, [_fact(source_ref, "voltage", "220", "380")])
    stage4["stage3_signature"] = "other-input"

    try:
        build_text_atoms(stage3, stage4)
    except ValueError as error:
        assert "stale" in str(error)
    else:
        raise AssertionError("stale Stage 4 must fail closed")


def test_stage4_is_rejected_when_stage3_content_changes_under_same_source_signature():
    stage3 = _differences(_item("220", "380", "voltage"))
    source_ref = next(iter_stage3_evidence(stage3))[0]
    stage4 = build_semantic_validation(
        stage3, [_fact(source_ref, "voltage", "220", "380")]
    )
    changed = deepcopy(stage3)
    changed["sheet_groups"][0]["changed"][0]["after"] = "400"

    try:
        build_text_atoms(changed, stage4)
    except ValueError as error:
        assert "stale" in str(error)
    else:
        raise AssertionError("Stage 4 must bind the actual Stage 3 evidence content")


def test_input_order_does_not_change_atom_ids():
    stage3 = _differences(
        _item("220", "380", "voltage"),
        _item("27", "30", "count"),
    )
    refs = [entry[0] for entry in iter_stage3_evidence(stage3)]
    facts = [
        _fact(refs[0], "voltage", "220", "380"),
        {**_fact(refs[1], "count", 27, 30), "dimension": "QUANTITY"},
    ]
    first = build_text_atoms(stage3, build_semantic_validation(stage3, facts))
    reordered = deepcopy(stage3)
    reordered["sheet_groups"][0]["changed"].reverse()
    second = build_text_atoms(
        reordered,
        build_semantic_validation(reordered, reversed(facts)),
    )

    assert [atom["atom_id"] for atom in first["atoms"]] == [
        atom["atom_id"] for atom in second["atoms"]
    ]

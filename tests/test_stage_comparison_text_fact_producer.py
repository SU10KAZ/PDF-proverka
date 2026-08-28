from __future__ import annotations

from copy import deepcopy

from backend.app.services.stage_comparison.production_text_flow import (
    PREPARATION_KIND,
    PREPARATION_SCHEMA_VERSION,
    build_text_differences_from_preparation,
)
from backend.app.services.stage_comparison.text_atom_builder import build_text_atoms
from backend.app.services.stage_comparison.text_fact_producer import produce_text_facts
from backend.app.services.stage_comparison.text_semantic_validation import (
    build_semantic_validation,
)


def _fragment(
    fragment_id: str,
    side: str,
    text: str,
    *,
    order: int,
    kind: str = "paragraph",
    parts: list[str] | None = None,
) -> dict:
    return {
        "id": fragment_id,
        "stage": "stage_1" if side == "left" else "stage_2",
        "pdf_page": 10 if side == "left" else 24,
        "text": text,
        "canonical_text": text.casefold(),
        "source_block_id": "left-block" if side == "left" else "right-block",
        "source_kind": kind,
        "source_group": (
            "left-block:table" if side == "left" else "right-block:table"
        ) if kind == "table_row" else (
            "left-block" if side == "left" else "right-block"
        ),
        "location_parts": list(parts or []),
        "order": order,
        "bboxes": [{"x": .1, "y": .2, "width": .3, "height": .04}],
    }


def _preparation(left: list[dict], right: list[dict], *, status: str = "HIGH") -> dict:
    return {
        "kind": PREPARATION_KIND,
        "schema_version": PREPARATION_SCHEMA_VERSION,
        "version": 1,
        "pair_id": "pair-1",
        "input_signature": "prepared-input",
        "comparison_groups": [{
            "id": "group-1",
            "left_pages": [10],
            "right_pages": [24],
            "relation_type": "MATCHED",
            "relation_status": status,
        }],
        "fragments": {"left": left, "right": right},
    }


def _differences(preparation: dict) -> dict:
    return build_text_differences_from_preparation(
        preparation, generated_at="fixed"
    )


def _header(side: str, order: int) -> dict:
    return _fragment(
        f"{side}-header", side,
        "Наименование потребителей | Кол-во | Установленная мощность | Потребная мощность",
        order=order,
        kind="table_row",
        parts=[
            "Наименование потребителей", "Кол-во",
            "Установленная мощность", "Потребная мощность",
        ],
    )


def _units(side: str, order: int) -> dict:
    return _fragment(
        f"{side}-units", side,
        "шт | кВт | кВт | Кп.к | Кс | Кн.м | Cosφ | tgf | Pp=РуКс | Qp=Pptgf | Sp | A",
        order=order,
        kind="table_row",
        parts=["шт", "кВт", "кВт", "Кп.к", "Кс", "Кн.м", "Cosφ", "tgf", "Pp=РуКс", "Qp=Pptgf", "Sp", "A"],
    )


def _load_row(side: str, order: int, *, quantity: str = "1", current: str = "0.67") -> dict:
    parts = [
        "Насос циркуляционный НЦГ-1", quantity, "0.37", "0.37",
        "1.0", "0.9", "1.0", "0.75", "0.88", "0.33", "0.29",
        "0.44", current,
    ]
    return _fragment(
        f"{side}-load", side, " | ".join(parts),
        order=order, kind="table_row", parts=parts,
    )


def test_changed_labeled_values_become_one_fact_per_property_with_alias_entity():
    before = _fragment(
        "left-values", "left",
        "ШР-1: Напряжение 220 В; температура -10…+40 °C",
        order=1,
    )
    after = _fragment(
        "right-values", "right",
        "ШР-1: Напряжение 380 В; температура -25…+50 °C",
        order=1,
    )
    preparation = _preparation([before], [after])

    result = produce_text_facts(
        _differences(preparation), preparation, generated_at="fixed"
    )

    assert result["diagnostics"]["facts"] == 2
    assert result["diagnostics"]["automatic_facts"] == 2
    assert result["diagnostics"]["one_property_per_fact"] is True
    assert {fact["facet_ref"] for fact in result["facts"]} == {
        "voltage", "temperature_range",
    }
    assert {fact["subject_ref"] for fact in result["facts"]} == {
        "text_entity:PANEL_1"
    }
    assert all(fact["project_entity_ref"] for fact in result["facts"])
    assert all(fact["provenance"]["source_anchors"] for fact in result["facts"])
    assert {fact["direction"] for fact in result["facts"]} == {
        "INCREASED", "ALTERED",
    }


def test_recognized_table_emits_only_changed_cells_not_one_row_fact():
    left = [_header("left", 1), _units("left", 2), _load_row("left", 3)]
    right = [
        _header("right", 1), _units("right", 2),
        _load_row("right", 3, quantity="2", current="1.34"),
    ]
    preparation = _preparation(left, right)

    result = produce_text_facts(_differences(preparation), preparation)

    assert {fact["facet_ref"] for fact in result["facts"]} == {
        "quantity", "maximum_calculated_current_a",
    }
    by_facet = {fact["facet_ref"]: fact for fact in result["facts"]}
    assert by_facet["quantity"]["before_value"] == "1"
    assert by_facet["quantity"]["after_value"] == "2"
    assert by_facet["maximum_calculated_current_a"]["after_value"] == "1.34 A"
    assert by_facet["quantity"]["dimension"] == "QUANTITY"


def test_added_possible_table_row_is_structured_but_requires_review():
    right = [
        _header("right", 1),
        _units("right", 2),
        _fragment(
            "right-title", "right", "ВРУ-ИТП",
            order=3, kind="table_row", parts=["ВРУ-ИТП"],
        ),
        _load_row("right", 4),
        _fragment(
            "right-notes", "right", "Примечания:", order=5,
        ),
        _fragment(
            "right-narrative", "right",
            "Щиты изготовить напольного исполнения, степень защиты не ниже IP31.",
            order=6,
        ),
    ]
    preparation = _preparation([], right, status="POSSIBLE")
    differences = _differences(preparation)

    production = produce_text_facts(differences, preparation)
    semantic = build_semantic_validation(
        differences,
        production["facts"],
        not_applicable_source_evidence=production["not_applicable_source_evidence"],
    )
    atoms = build_text_atoms(differences, semantic)

    assert production["diagnostics"]["facts"] == 12
    assert production["diagnostics"]["review_required_facts"] == 12
    assert production["diagnostics"]["sheet_relation_blocked_facts"] == 12
    assert production["diagnostics"]["opposite_coverage_blocked_facts"] == 12
    assert {
        tuple(fact["provenance"]["review_requirement"]["reason_codes"])
        for fact in production["facts"]
    } == {(
        "sheet_relation_unconfirmed",
        "opposite_side_structured_coverage_incomplete",
    )}
    assert not any(
        fact["provenance"]["review_requirement"]
        ["only_upstream_relation_blocker"]
        for fact in production["facts"]
    )
    assert not any(
        fact["provenance"]["review_requirement"]
        ["per_atom_question_actionable"]
        for fact in production["facts"]
    )
    assert production["diagnostics"]["not_applicable_source_evidence"] == 4
    assert production["diagnostics"]["unresolved_source_evidence"] == 1
    assert len(atoms["atoms"]) == 13  # 12 properties + one unresolved narrative
    assert atoms["diagnostics"]["not_applicable_count"] == 4
    assert atoms["diagnostics"]["review_required_atoms"] == 13
    assert all(
        atom["outcome"] == "REVIEW_REQUIRED"
        for atom in atoms["atoms"]
    )


def test_high_relation_with_zero_opposite_coverage_never_claims_addition():
    right = [_header("right", 1), _units("right", 2), _load_row("right", 3)]
    preparation = _preparation([], right, status="HIGH")

    production = produce_text_facts(_differences(preparation), preparation)

    assert production["diagnostics"]["facts"] == 12
    assert production["diagnostics"]["automatic_facts"] == 0
    assert production["diagnostics"]["opposite_coverage_blocked_facts"] == 12
    assert {
        tuple(fact["provenance"]["review_requirement"]["reason_codes"])
        for fact in production["facts"]
    } == {("opposite_side_structured_coverage_incomplete",)}
    assert all(fact["outcome"] == "REVIEW_REQUIRED" for fact in production["facts"])
    assert all(fact["confidence"] == "UNKNOWN" for fact in production["facts"])
    assert not any(
        fact["provenance"]["review_requirement"]["per_atom_question_actionable"]
        for fact in production["facts"]
    )


def test_heading_and_table_headers_are_explicit_not_applicable_not_review_atoms():
    right = [
        _header("right", 1),
        _units("right", 2),
        _fragment("right-heading", "right", "Параметры", order=3, kind="heading"),
    ]
    preparation = _preparation([], right)
    differences = _differences(preparation)

    production = produce_text_facts(differences, preparation)
    semantic = build_semantic_validation(
        differences,
        not_applicable_source_evidence=production["not_applicable_source_evidence"],
    )
    atoms = build_text_atoms(differences, semantic)

    assert production["facts"] == []
    assert production["diagnostics"]["not_applicable_source_evidence"] == 3
    assert production["unresolved_source_evidence"] == []
    assert atoms["atoms"] == []
    assert atoms["diagnostics"]["not_applicable_count"] == 3


def test_narrative_and_ambiguous_table_fail_closed():
    right = [
        _fragment(
            "right-narrative", "right",
            "При необходимости изменить оборудование и согласовать решение.",
            order=1,
        ),
        _fragment(
            "right-unknown-row", "right", "Насос | 1 | 5.5",
            order=2, kind="table_row", parts=["Насос", "1", "5.5"],
        ),
    ]
    preparation = _preparation([], right)

    result = produce_text_facts(_differences(preparation), preparation)

    assert result["facts"] == []
    assert result["not_applicable_source_evidence"] == []
    assert result["diagnostics"]["unresolved_source_evidence"] == 2


def test_fact_signature_and_counts_are_independent_of_fragment_array_order():
    right = [_header("right", 1), _units("right", 2), _load_row("right", 3)]
    preparation = _preparation([], right)
    differences = _differences(preparation)
    first = produce_text_facts(differences, preparation, generated_at="first")
    reordered = deepcopy(preparation)
    reordered["fragments"]["right"].reverse()
    second = produce_text_facts(differences, reordered, generated_at="second")

    assert first["input_signature"] == second["input_signature"]
    assert first["facts"] == second["facts"]
    assert first["diagnostics"] == second["diagnostics"]

"""Состязательные пробы привязки: что верификатор ОБЯЗАН отклонить.

Проверка «значение встречается где-то на этой стороне» пропускает почти всё,
что стоит поймать. Площадь соседнего помещения лежит в том же окне контекста;
правильное число рядом с выдуманным объектом выглядит безупречно; «добавлено»
вместо «удалено» отличается от истины ровно одним словом.

Каждый тест здесь — попытка провести через верификатор заведомо неверный
разбор. Все они обязаны провалиться, и провалиться по названной причине.
"""
from __future__ import annotations

import pytest

from backend.app.services.stage_comparison.ai import (
    evidence as evidence_module,
    verifier,
)
from backend.app.services.stage_comparison.review_queue import (
    UngroundedObjectLabelError,
    mint_project_entity_ref,
    object_label_is_grounded,
)


def _fragment(fragment_id: str, page: int, text: str, order: int) -> dict:
    return {
        "id": fragment_id,
        "pdf_page": page,
        "text": text,
        "canonical_text": text.casefold(),
        "source_kind": "table_row",
        "order": order,
    }


def _preparation() -> dict:
    return {
        "fragments": {
            "left": [
                _fragment("l0", 29, "Экспликация помещений", 1),
                _fragment("l1", 29, "24.5 | Кладовая | 6,02", 2),
                _fragment("l2", 29, "24.6 | Холл | 15,71", 3),
            ],
            "right": [
                _fragment("r0", 8, "Экспликация помещений", 1),
                _fragment("r1", 8, "24.5 | Кладовая | 6,40", 2),
                _fragment("r2", 8, "24.6 | Холл | 15,71", 3),
            ],
        },
    }


def _relations() -> dict:
    return {
        "kind": "stage_comparison_sheet_relations",
        "sheet_labels": {"LEFT": {"29": "План 3 этажа"}, "RIGHT": {"8": "План 3 этажа"}},
        "relations": [{
            "relation_id": "srel_a", "left_pages": [29], "right_pages": [8],
            "relation_type": "MATCHED", "status": "HIGH",
        }],
    }


def _groups() -> list[dict]:
    return [{"id": "srel_a", "left_pages": [29], "right_pages": [8]}]


def _review_item(
    *,
    bucket: str = "changed",
    before: str | None = "24.5 | Кладовая | 6,02",
    after: str | None = "24.5 | Кладовая | 6,40",
    coverage: str = "SUFFICIENT",
    dimension: str = "UNKNOWN_DIMENSION",
) -> dict:
    locations = {
        "LEFT": [{"page": 29, "fragment_id": "l1"}] if before is not None else [],
        "RIGHT": [{"page": 8, "fragment_id": "r1"}] if after is not None else [],
    }
    return {
        "review_evidence_id": "ureview_1",
        "atom_id": "tatom_1",
        "source": "TEXT",
        "scope_ref": evidence_module.scope_ref_for_group(_groups()[0]),
        "dimension": dimension,
        "direction": "ALTERED",
        "outcome": "REVIEW_REQUIRED",
        "before_value": before,
        "after_value": after,
        "reason_codes": ["dimension_unknown"],
        "provenance": {
            "source_atom": {
                "stage3_bucket": bucket,
                "structured_fact": False,
                "locations": locations,
                "recognition_coverage": {"status": coverage, "reason_codes": []},
            },
            "source_atom_outcome": "REVIEW_REQUIRED",
        },
    }


def _view(**kwargs) -> dict:
    packages = evidence_module.build_packages(
        review_items=[_review_item(**kwargs)],
        preparation=_preparation(),
        sheet_relations=_relations(),
        comparison_groups=_groups(),
        batch_size=10,
    )
    return packages[0].items[0].model_view()


def _resolution(**overrides) -> dict:
    base = {
        "item_id": "ureview_1",
        "resolution_status": "AI_RESOLVED",
        "dimension": "PARAMETER",
        "direction": "INCREASED",
        "outcome": "MATERIAL_CHANGE",
        "object_label": "помещение 24.5",
        "object_evidence_ref": "L2",
        "facet_label": "площадь",
        "before_value": "24.5 | Кладовая | 6,02",
        "before_evidence_ref": "L2",
        "after_value": "24.5 | Кладовая | 6,40",
        "after_evidence_ref": "R2",
        "confidence": "HIGH",
        "evidence_quotes": [
            {"side": "LEFT", "evidence_ref": "L2", "quote": "24.5 | Кладовая | 6,02"},
            {"side": "RIGHT", "evidence_ref": "R2", "quote": "24.5 | Кладовая | 6,40"},
        ],
        "needs_human_review": False,
        "human_reason": "NOT_APPLICABLE",
        "human_question": None,
        "engineering_summary": "Площадь кладовой 24.5 увеличена с 6,02 до 6,40 м².",
    }
    base.update(overrides)
    return base


def _errors(view: dict, resolution: dict) -> list[str]:
    result = verifier.verify_resolution(view, resolution)
    assert not result.ok, "заведомо неверный разбор прошёл проверку"
    return result.errors


def test_the_reference_resolution_still_passes():
    # Без этого теста любой из следующих проходил бы по случайной причине.
    assert verifier.verify_resolution(_view(), _resolution()).ok


# ── Значение взято у другого объекта ──────────────────────────────────────

def test_a_correct_number_taken_from_another_room_is_refused():
    # «15,71» — настоящая площадь, но помещения 24.6, а не 24.5. Строка лежит
    # в том же окне контекста, поэтому проверка «есть на этой стороне» её
    # пропускала.
    errors = _errors(_view(), _resolution(
        after_value="24.6 | Холл | 15,71", after_evidence_ref="R3",
    ))

    assert any("объект" in error for error in errors)


def test_a_value_bound_to_a_line_that_does_not_contain_it_is_refused():
    errors = _errors(_view(), _resolution(before_evidence_ref="L1"))

    assert any("привязка" in error for error in errors)


def test_a_resolution_leaning_only_on_neighbouring_lines_is_refused():
    # Ни одна названная строка не относится к самому разбираемому месту.
    errors = _errors(_view(), _resolution(
        object_label="помещение 24.6",
        object_evidence_ref="L3",
        before_value="24.6 | Холл | 15,71", before_evidence_ref="L3",
        after_value="24.6 | Холл | 15,71", after_evidence_ref="R3",
        direction="ALTERED",
        evidence_quotes=[
            {"side": "LEFT", "evidence_ref": "L3", "quote": "24.6 | Холл | 15,71"},
        ],
    ))

    assert any("разбираемому" in error for error in errors)


# ── Выдуманный объект ─────────────────────────────────────────────────────

def test_a_fabricated_object_label_is_refused_by_the_verifier():
    errors = _errors(_view(), _resolution(object_label="помещение 99.9"))

    assert any("объект" in error for error in errors)


def test_a_fabricated_object_label_never_receives_a_project_reference():
    evidence = ["24.5 | Кладовая | 6,02", "24.5 | Кладовая | 6,40"]

    assert object_label_is_grounded("помещение 24.5", evidence) is True
    assert object_label_is_grounded("помещение 99.9", evidence) is False
    with pytest.raises(UngroundedObjectLabelError):
        mint_project_entity_ref("помещение 99.9", evidence=evidence)


def test_a_grounded_label_still_receives_the_same_reference_as_before():
    evidence = ["24.5 | Кладовая | 6,02"]

    assert (
        mint_project_entity_ref("помещение 24.5", evidence=evidence)
        == mint_project_entity_ref("помещение 24.5")
    )


def test_an_object_reference_without_a_line_is_refused():
    errors = _errors(_view(), _resolution(object_evidence_ref=None))

    assert any("привязка" in error for error in errors)


def test_an_object_reference_to_a_line_outside_the_package_is_refused():
    errors = _errors(_view(), _resolution(object_evidence_ref="L99"))

    assert any("такой строки в пакете нет" in error for error in errors)


# ── Стороны ───────────────────────────────────────────────────────────────

def test_swapped_left_and_right_are_refused():
    errors = _errors(_view(), _resolution(
        before_value="24.5 | Кладовая | 6,40", before_evidence_ref="R2",
        after_value="24.5 | Кладовая | 6,02", after_evidence_ref="L2",
    ))

    assert any("стороны" in error or "привязка" in error for error in errors)


def test_a_quote_declared_on_the_wrong_side_is_refused():
    errors = _errors(_view(), _resolution(evidence_quotes=[
        {"side": "RIGHT", "evidence_ref": "L2", "quote": "24.5 | Кладовая | 6,02"},
    ]))

    assert any("стороны" in error or "цитата" in error for error in errors)


# ── Направление ───────────────────────────────────────────────────────────

def test_added_instead_of_removed_is_a_failure_not_a_warning():
    view = _view(bucket="removed", after=None)
    errors = _errors(view, _resolution(
        direction="ADDED",
        after_value=None, after_evidence_ref=None,
        evidence_quotes=[
            {"side": "LEFT", "evidence_ref": "L2", "quote": "24.5 | Кладовая | 6,02"},
        ],
    ))

    assert any("направление" in error for error in errors)


def test_removed_instead_of_added_is_a_failure_not_a_warning():
    view = _view(bucket="added", before=None)
    errors = _errors(view, _resolution(
        direction="REMOVED",
        before_value=None, before_evidence_ref=None,
        after_value="24.5 | Кладовая | 6,40", after_evidence_ref="R2",
        object_evidence_ref="R2",
        evidence_quotes=[
            {"side": "RIGHT", "evidence_ref": "R2", "quote": "24.5 | Кладовая | 6,40"},
        ],
    ))

    assert any("направление" in error for error in errors)


def test_a_changed_row_cannot_be_declared_added():
    errors = _errors(_view(), _resolution(direction="ADDED"))

    assert any("направление" in error for error in errors)


# ── Тип изменения ─────────────────────────────────────────────────────────

def test_a_dimension_the_deterministic_layer_already_established_cannot_be_changed():
    view = _view(dimension="PARAMETER")

    errors = _errors(view, _resolution(dimension="TYPE"))

    assert any("тип изменения" in error for error in errors)


def test_an_unknown_dimension_can_never_be_resolved():
    errors = _errors(_view(), _resolution(dimension="UNKNOWN_DIMENSION"))

    assert any("политика" in error for error in errors)


# ── Полнота распознавания ─────────────────────────────────────────────────

@pytest.mark.parametrize("status", ["INSUFFICIENT", "PARTIAL", "UNKNOWN"])
def test_an_item_with_unproven_recognition_can_never_be_resolved(status):
    view = _view(coverage=status)

    errors = _errors(view, _resolution())

    assert any("полнота" in error for error in errors)


def test_an_item_with_unproven_recognition_may_still_be_handed_to_a_human():
    view = _view(coverage="INSUFFICIENT")
    refusal = _resolution(
        resolution_status="HUMAN_REQUIRED",
        needs_human_review=True,
        human_reason="EVIDENCE_TRUNCATED",
        human_question="Строка 24.5 распозналась ненадёжно — сверьте по чертежу.",
    )

    assert verifier.verify_resolution(view, refusal).ok


# ── Место доказательства ──────────────────────────────────────────────────

def test_a_resolution_without_any_evidence_reference_is_refused():
    errors = _errors(_view(), _resolution(
        before_evidence_ref=None, after_evidence_ref=None,
        object_evidence_ref=None, evidence_quotes=[],
    ))

    assert any("привязка" in error for error in errors)


def test_a_quote_pointing_at_a_line_that_does_not_contain_it_is_refused():
    errors = _errors(_view(), _resolution(evidence_quotes=[
        {"side": "LEFT", "evidence_ref": "L1", "quote": "24.5 | Кладовая | 6,02"},
    ]))

    assert any("привязка" in error for error in errors)

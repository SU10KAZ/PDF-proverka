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
    facet_ref: str | None = None,
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
        "facet_ref": facet_ref,
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
    # Заголовок таблицы входит в доказательства так же, как её строки: вид
    # объекта живёт именно в нём, а номер — в строке.
    evidence = [
        "Экспликация помещений",
        "24.5 | Кладовая | 6,02",
        "24.5 | Кладовая | 6,40",
    ]

    assert object_label_is_grounded("помещение 24.5", evidence) is True
    assert object_label_is_grounded("помещение 99.9", evidence) is False
    with pytest.raises(UngroundedObjectLabelError):
        mint_project_entity_ref("помещение 99.9", evidence=evidence)


def test_a_fabricated_object_kind_never_receives_a_project_reference():
    """Совпал номер — это ещё не тот же объект.

    «Вымышленный агрегат 24.5» опирается ровно на то же число, что и
    «помещение 24.5». Если вид объекта не проверять, рядом с настоящим
    помещением молча заводится объект, которого в проекте нет.
    """
    evidence = ["Экспликация помещений", "24.5 | Кладовая | 6,02"]

    assert object_label_is_grounded("вымышленный агрегат 24.5", evidence) is False
    with pytest.raises(UngroundedObjectLabelError):
        mint_project_entity_ref("вымышленный агрегат 24.5", evidence=evidence)


def test_a_grounded_label_still_receives_the_same_reference_as_before():
    evidence = ["Экспликация помещений", "24.5 | Кладовая | 6,02"]

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


# ── Свойство объекта ──────────────────────────────────────────────────────

def test_a_facet_that_contradicts_the_recognised_property_is_refused():
    """Доказана площадь — «высота потолка» это другое свойство, не формулировка.

    Свойство распознал детерминированный слой: колонка экспликации, из которой
    взято число. Разрешить модели переименовать его значит опубликовать
    правильное число под неправильным смыслом — и никаким доказательством
    этого потом не поймать, потому что число-то настоящее.
    """
    view = _view(facet_ref="room_area_m2")

    errors = _errors(view, _resolution(facet_label="высота потолка"))

    assert any("свойство" in error for error in errors)


def test_a_facet_the_catalogue_does_not_know_is_refused_on_a_proven_property():
    view = _view(facet_ref="room_area_m2")

    errors = _errors(view, _resolution(facet_label="класс энергоэффективности"))

    assert any("свойство" in error for error in errors)


def test_the_recognised_property_still_accepts_its_own_name():
    view = _view(facet_ref="room_area_m2")

    assert verifier.verify_resolution(view, _resolution(facet_label="площадь")).ok
    assert verifier.verify_resolution(
        view, _resolution(facet_label="площадь помещения")
    ).ok


def test_a_disputed_property_may_still_be_handed_to_a_human():
    """Модель имеет право не согласиться — но вопросом, а не публикацией."""
    view = _view(facet_ref="room_area_m2")
    refusal = _resolution(
        resolution_status="HUMAN_REQUIRED",
        needs_human_review=True,
        human_reason="CONTRADICTORY_EVIDENCE",
        human_question="Это площадь или высота? Колонка распозналась неоднозначно.",
        facet_label="площадь",
    )

    assert verifier.verify_resolution(view, refusal).ok


# ── Полная негативная матрица ─────────────────────────────────────────────
#
# Девять способов ошибиться, каждый из которых выглядит правдоподобно. Ни один
# не имеет права быть опубликован. Матрица держится списком целиком: частичное
# прохождение — это дыра, а не прогресс.

_NEGATIVE_MATRIX = [
    (
        "A. число другого помещения",
        {},
        {"after_value": "24.6 | Холл | 15,71", "after_evidence_ref": "R3"},
        "объект",
    ),
    (
        "B. правильное значение при подменённом свойстве",
        {"facet_ref": "room_area_m2"},
        {"facet_label": "высота потолка"},
        "свойство",
    ),
    (
        "C. неправильный тип изменения",
        {"dimension": "PARAMETER"},
        {"dimension": "QUANTITY"},
        "тип изменения",
    ),
    (
        "D. «добавлено» там, где Stage 3 нашёл удаление",
        {"bucket": "removed", "after": None},
        {
            "direction": "ADDED",
            "after_value": None,
            "after_evidence_ref": None,
            "evidence_quotes": [
                {"side": "LEFT", "evidence_ref": "L2",
                 "quote": "24.5 | Кладовая | 6,02"},
            ],
        },
        "направление",
    ),
    (
        "E. «удалено» там, где Stage 3 нашёл добавление",
        {"bucket": "added", "before": None},
        {
            "direction": "REMOVED",
            "before_value": None,
            "before_evidence_ref": None,
            "evidence_quotes": [
                {"side": "RIGHT", "evidence_ref": "R2",
                 "quote": "24.5 | Кладовая | 6,40"},
            ],
        },
        "направление",
    ),
    (
        "F. стороны переставлены местами",
        {},
        {
            "before_value": "24.5 | Кладовая | 6,40",
            "before_evidence_ref": "R2",
            "after_value": "24.5 | Кладовая | 6,02",
            "after_evidence_ref": "L2",
        },
        "",
    ),
    (
        "G. выдуманный вид объекта при совпавшем номере",
        {},
        {"object_label": "вымышленный агрегат 24.5"},
        "объект",
    ),
    (
        "H. ссылка на строку другого объекта",
        {},
        {"object_evidence_ref": "L3"},
        "объект",
    ),
    (
        "I. распознавание не подтверждено",
        {"coverage": "INSUFFICIENT"},
        {},
        "полнота",
    ),
]


@pytest.mark.parametrize(
    "name, item, resolution, expected",
    _NEGATIVE_MATRIX,
    ids=[case[0].split(".")[0] for case in _NEGATIVE_MATRIX],
)
def test_the_negative_matrix_is_rejected_in_full(name, item, resolution, expected):
    result = verifier.verify_resolution(_view(**item), _resolution(**resolution))

    assert not result.ok, f"{name}: заведомо неверный разбор прошёл проверку"
    if expected:
        assert any(expected in error for error in result.errors), (
            f"{name}: отклонено, но не по названной причине: {result.errors}"
        )


def test_the_negative_matrix_covers_every_probe_the_audit_named():
    """Матрица держится целиком: девять из девяти, не восемь."""
    assert len(_NEGATIVE_MATRIX) == 9
    assert [case[0][0] for case in _NEGATIVE_MATRIX] == list("ABCDEFGHI")

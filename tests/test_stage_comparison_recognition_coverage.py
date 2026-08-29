"""Полнота распознавания: что она обязана не пропустить и что не сломать.

Инвариант, который здесь закрепляется:

    отсутствие распознанного доказательства
    не является доказательством отсутствия.

Главный кейс — реальный, с пары АР. Markdown правой редакции прочитал номера
помещений «З15.1» и «З15.2» с кириллической «З» вместо цифры «3». В нативном
текстовом слое того же PDF на том же листе стояли «315.1» и «315.2» с теми же
названиями и площадями. Сравнение только Markdown породило четыре
«удалено» и четыре «добавлено» как существенные изменения проекта. Ни одного
изменения проекта там не было.
"""
from __future__ import annotations

import pytest

from backend.app.services.stage_comparison import recognition_coverage as rc
from backend.app.services.stage_comparison.production_text_flow import (
    PREPARATION_KIND,
    PREPARATION_SCHEMA_VERSION,
    build_text_differences_from_preparation,
)
from backend.app.services.stage_comparison.text_fact_producer import produce_text_facts

LEFT_PAGE = 7
RIGHT_PAGE = 9

#: Фон листа: без него нативный слой считается непригодным по объёму.
_SHEET_NOISE = " ".join("экспликация помещений этажа" for _ in range(12))


def _row(fragment_id: str, side: str, parts: list[str], *, order: int,
         native: list[str] | None = None) -> dict:
    """Строка таблицы. ``native`` — что стоит в текстовом слое PDF под ней."""
    text = " ".join(parts)
    fragment = {
        "id": fragment_id,
        "stage": "stage_1" if side == "left" else "stage_2",
        "pdf_page": LEFT_PAGE if side == "left" else RIGHT_PAGE,
        "text": text,
        "canonical_text": text.casefold(),
        "source_block_id": f"{side}-block",
        "source_kind": "table_row",
        "source_group": f"{side}-block:table",
        "location_parts": list(parts),
        "order": order,
        "bboxes": [{"x": .1, "y": .2 + order / 100, "width": .3, "height": .03}],
    }
    fragment["pdf_canonical_text"] = " ".join(native or parts).casefold()
    return fragment


def _header(side: str) -> dict:
    return _row(
        f"{side}-header", side,
        ["Номер помещения", "Наименование", "Площадь, м2"],
        order=1,
    )


def _index(pages: dict[str, dict[int, str]]) -> dict[str, dict[str, dict]]:
    return {
        side: {
            str(page): rc.build_page_index(f"{text} {_SHEET_NOISE}")
            for page, text in values.items()
        }
        for side, values in pages.items()
    }


def _preparation(left: list[dict], right: list[dict], index: dict | None) -> dict:
    payload = {
        "kind": PREPARATION_KIND,
        "schema_version": PREPARATION_SCHEMA_VERSION,
        "version": 1,
        "pair_id": "pair-ar",
        "input_signature": "prepared-input",
        "comparison_groups": [{
            "id": "group-1",
            "left_pages": [LEFT_PAGE],
            "right_pages": [RIGHT_PAGE],
            "relation_type": "MATCHED",
            "relation_status": "HIGH",
        }],
        "fragments": {"left": left, "right": right},
    }
    if index is not None:
        payload["recognition_index"] = index
    return payload


def _facts(left: list[dict], right: list[dict], index: dict | None) -> tuple[dict, dict]:
    preparation = _preparation(left, right, index)
    differences = build_text_differences_from_preparation(
        preparation, generated_at="fixed"
    )
    return differences, produce_text_facts(
        differences, preparation, generated_at="fixed"
    )


# ── Обязательный регрессионный сценарий АР ────────────────────────────────

_AR_ROWS = (
    (["315.1", "Кладовая уборочного инвентаря", "19,92"], ["З15.1", "Кладовая уборочного инвентаря", "19,92"]),
    (["315.2", "Кладовая уборочного инвентаря", "19,72"], ["З15.2", "Кладовая уборочного инвентаря", "19,72"]),
)
#: Нативный текстовый слой ОБЕИХ сторон одинаков и содержит правильные номера.
_AR_NATIVE = (
    "315.1 Кладовая уборочного инвентаря 19.92 "
    "315.2 Кладовая уборочного инвентаря 19.72"
)


def _ar_pair() -> tuple[list[dict], list[dict], dict]:
    left = [_header("left")]
    right = [_header("right")]
    for order, (left_parts, right_parts) in enumerate(_AR_ROWS, start=2):
        left.append(_row(f"l{order}", "left", left_parts, order=order))
        # Markdown правой стороны прочитал «З», а слой PDF содержит «3».
        right.append(_row(
            f"r{order}", "right", right_parts, order=order, native=left_parts,
        ))
    index = _index({
        "LEFT": {LEFT_PAGE: _AR_NATIVE},
        "RIGHT": {RIGHT_PAGE: _AR_NATIVE},
    })
    return left, right, index


def test_ar_recognition_slip_never_becomes_a_material_removal_or_addition():
    left, right, index = _ar_pair()

    differences, production = _facts(left, right, index)

    assert differences["summary"]["removed"] >= 2
    assert differences["summary"]["added"] >= 2
    # Расхождение осталось на месте — его никто не прячет. Но ни один факт не
    # имеет права называться существенным изменением проекта.
    assert production["facts"], "факты должны быть построены, а не выброшены"
    assert all(fact["outcome"] == "REVIEW_REQUIRED" for fact in production["facts"])
    assert all(fact["confidence"] == "UNKNOWN" for fact in production["facts"])
    assert production["diagnostics"]["recognition_coverage_blocked_facts"] == len(
        production["facts"]
    )
    reasons = {
        code
        for fact in production["facts"]
        for code in fact["provenance"]["review_requirement"]["reason_codes"]
    }
    assert rc.REASON_COVERAGE_NOT_PROVEN in reasons
    assert {
        rc.REASON_OPPOSITE_CONTAINS_VALUE, rc.REASON_OWN_SIDE_MISMATCH,
    } & reasons


def test_ar_slip_is_named_in_the_published_stage3_coverage():
    left, right, index = _ar_pair()

    differences, _production = _facts(left, right, index)

    coverage = differences["recognition_coverage"]
    assert coverage["contract_version"] == rc.CONTRACT_VERSION
    assert coverage["index_available"] is True
    statuses = {value["status"] for value in coverage["by_evidence"].values()}
    assert statuses == {rc.INSUFFICIENT}
    # Уровни контракта опубликованы все, а не только тот, что решил вопрос.
    assert set(coverage["documents"]) == {"LEFT", "RIGHT"}
    assert set(coverage["pages"]) == {"LEFT", "RIGHT"}
    assert coverage["groups"]["group-1"]["sides"]["RIGHT"]["status"] in rc.STATUSES


# ── Настоящее изменение проверку проходит ─────────────────────────────────

def test_a_room_that_really_disappeared_still_becomes_a_material_change():
    left = [
        _header("left"),
        _row("l2", "left", ["315.1", "Кладовая", "19,92"], order=2),
        _row("l3", "left", ["411.7", "Электрощитовая", "8,40"], order=3),
    ]
    right = [
        _header("right"),
        _row("r2", "right", ["315.1", "Кладовая", "19,92"], order=2),
    ]
    index = _index({
        "LEFT": {LEFT_PAGE: "315.1 Кладовая 19.92 411.7 Электрощитовая 8.40"},
        "RIGHT": {RIGHT_PAGE: "315.1 Кладовая 19.92"},
    })

    _differences, production = _facts(left, right, index)

    removed = [
        fact for fact in production["facts"]
        if fact["direction"] == "REMOVED"
    ]
    assert removed, "настоящее удаление обязано остаться находкой"
    assert all(fact["outcome"] == "MATERIAL_CHANGE" for fact in removed)
    assert production["diagnostics"]["recognition_coverage_blocked_facts"] == 0


def test_a_real_area_change_still_becomes_a_material_change():
    left = [_header("left"), _row("l2", "left", ["315.1", "Кладовая", "19,92"], order=2)]
    right = [_header("right"), _row("r2", "right", ["315.1", "Кладовая", "21,50"], order=2)]
    index = _index({
        "LEFT": {LEFT_PAGE: "315.1 Кладовая 19.92"},
        "RIGHT": {RIGHT_PAGE: "315.1 Кладовая 21.50"},
    })

    _differences, production = _facts(left, right, index)

    areas = [fact for fact in production["facts"] if fact["facet_ref"] == "room_area_m2"]
    assert areas and all(fact["outcome"] == "MATERIAL_CHANGE" for fact in areas)


# ── Гейт CHANGED (раньше проверки полноты не проходил вовсе) ──────────────

def test_changed_is_gated_on_coverage_too_not_only_removed_and_added():
    # Обе стороны прочитаны, но правая — неверно: в слое PDF стоит «21.50»,
    # а Markdown прочитал «2150». Сравнивать площадь по такому чтению нельзя.
    left = [_header("left"), _row("l2", "left", ["315.1", "Кладовая", "19,92"], order=2)]
    right = [_header("right"), _row(
        "r2", "right", ["315.1", "Кладовая", "2150"], order=2,
        native=["315.1", "Кладовая", "21,50"],
    )]
    index = _index({
        "LEFT": {LEFT_PAGE: "315.1 Кладовая 19.92"},
        "RIGHT": {RIGHT_PAGE: "315.1 Кладовая 21.50"},
    })

    _differences, production = _facts(left, right, index)

    assert production["facts"]
    assert all(fact["outcome"] == "REVIEW_REQUIRED" for fact in production["facts"])
    reasons = {
        code
        for fact in production["facts"]
        for code in fact["provenance"]["review_requirement"]["reason_codes"]
    }
    assert rc.REASON_OWN_SIDE_MISMATCH in reasons


# ── Уровни контракта ──────────────────────────────────────────────────────

def test_fragment_count_alone_is_not_a_coverage_verdict():
    # Страница, на которой прочитан ровно один фрагмент из большой таблицы,
    # раньше считалась «полностью покрытой»: фрагментов больше нуля.
    native = rc.build_page_index(
        "315.1 Кладовая 19.92 315.2 Кладовая 19.72 411.7 Щитовая 8.40 " + _SHEET_NOISE
    )
    read_wrong = _row("x", "left", ["З15.1", "Кладовая", "19,92"], order=2,
                      native=["315.1", "Кладовая", "19,92"])

    verdict = rc.page_coverage([read_wrong], native)

    assert verdict["fragments"] == 1
    assert verdict["status"] == rc.INSUFFICIENT
    assert rc.REASON_PAGE_INSUFFICIENT in verdict["reason_codes"]


def test_a_page_the_side_read_nothing_from_can_never_prove_an_absence():
    native = rc.build_page_index("315.1 Кладовая 19.92 " + _SHEET_NOISE)

    verdict = rc.page_coverage([], native)

    assert verdict["status"] == rc.INSUFFICIENT
    assert rc.REASON_NO_FRAGMENTS in verdict["reason_codes"]


def test_a_sheet_without_a_native_text_layer_is_unknown_not_sufficient():
    # Чертёж со шрифтами в кривых: независимого сигнала нет вовсе.
    verdict = rc.page_coverage(
        [_row("x", "left", ["315.1", "Кладовая", "19,92"], order=2)],
        rc.build_page_index(""),
    )

    assert verdict["status"] == rc.UNKNOWN
    assert rc.REASON_NO_TEXT_LAYER in verdict["reason_codes"]


def test_missing_coverage_verdict_is_unknown_and_never_a_green_light():
    verdict = rc.coverage_of({"recognition_coverage": {"by_evidence": {}}}, "tde_missing")

    assert verdict["status"] == rc.UNKNOWN
    assert rc.REASON_COVERAGE_NOT_PROVEN in verdict["reason_codes"]
    assert rc.is_sufficient(verdict) is False


def test_a_stage3_artifact_built_without_the_check_does_not_pass_silently():
    left, right, _index = _ar_pair()

    _differences, production = _facts(left, right, None)

    assert all(fact["outcome"] == "REVIEW_REQUIRED" for fact in production["facts"])


def test_worst_verdict_wins_over_the_most_frequent_one():
    assert rc.worst([rc.SUFFICIENT, rc.SUFFICIENT, rc.PARTIAL]) == rc.PARTIAL
    assert rc.worst([rc.SUFFICIENT, rc.INSUFFICIENT, rc.UNKNOWN]) == rc.INSUFFICIENT
    assert rc.worst([]) == rc.UNKNOWN


# ── Нормализация и токены ─────────────────────────────────────────────────

def test_lookalike_letters_are_never_folded_into_digits():
    # Именно на этом различии держится вся проверка.
    assert rc.salient_tokens("З15.1") != rc.salient_tokens("315.1")
    assert "315.1" in rc.salient_tokens("315.1 Кладовая")


@pytest.mark.parametrize("written", ["19,92", "19.92"])
def test_decimal_separator_is_not_a_recognition_difference(written):
    assert "19.92" in rc.salient_tokens(f"Площадь {written} м2")


def test_units_repeated_in_every_row_are_not_evidence_of_presence():
    # «м2» стоит в каждой строке экспликации: совпадение по нему доказывало бы
    # присутствие любой строки на любом листе.
    assert "м2" in rc.salient_tokens("19,92 м2")
    assert "м2" not in rc.checkable_tokens("19,92 м2")
    assert rc.checkable_tokens("315.1 Кладовая 19,92 м2") == {"315.1", "19.92"}


def test_a_number_split_by_the_text_layer_still_counts_as_present():
    index = {"LEFT": {"7": rc.build_page_index("3151 Кладовая " + _SHEET_NOISE)}}

    lenient, usable = rc.native_tokens_for(index, "LEFT", [7])
    strict, _usable = rc.native_tokens_for(index, "LEFT", [7], compact=False)

    assert usable is True
    assert "3151" in lenient
    # Ослабление работает только в сторону осторожности: при проверке своей
    # стороны потерянная запятая обязана остаться расхождением.
    assert "21.50" not in rc.native_tokens_for(
        {"LEFT": {"7": rc.build_page_index("2150 Кладовая " + _SHEET_NOISE)}},
        "LEFT", [7], compact=False,
    )[0]
    assert strict == {"3151"}

from __future__ import annotations

import hashlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.ai_sheet_matcher.core import production_sources_unchanged
from experiments.candidate_v4.core import (
    CHANNELS,
    FINAL_TOP_K,
    build_candidate_v4_dataset,
    build_function_passports,
    build_sheet_passport,
    compose_many_to_one_groups,
    compose_one_to_many_groups,
    explicit_contradictions,
    retrieve_candidates,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _body(
    *, sheet: str, title: str, summary: str, zone: str = "", entities: str = "",
) -> str:
    return f"""> **Stamp:** Code: TEST.ИОС.С | Stage: П | Sheet: {sheet} | Object: Test object | Name: {title} | Organization: Test
**[IMAGE]** | Type: Схема | Zone: {zone}
**Summary:** {summary}
**Description:** {summary}
**Entities:** {entities}
"""


def _fixture(left_bodies: list[str], right_bodies: list[str]):
    sheets = {"LEFT": {}, "RIGHT": {}}
    functions = {"LEFT": {}, "RIGHT": {}}
    for side, bodies in (("LEFT", left_bodies), ("RIGHT", right_bodies)):
        for page, body in enumerate(bodies, 1):
            sheets[side][page] = build_sheet_passport(
                pair_id="fixture", version_id="v001", side=side, page=page,
                body=body, page_count=len(bodies),
            )
            functions[side][page] = build_function_passports(sheets[side][page])
    return sheets, functions


def _candidate(page: int, rank: int, score: float = 0.8) -> dict:
    return {
        "candidate_id": f"candidate-{page}",
        "right_physical_page": page,
        "ranking_score": score,
        "rank": rank,
        "full_union_rank": rank,
    }


def test_cross_document_functional_retrieval() -> None:
    left = [_body(
        sheet="1", title="Насосная станция", zone="Корпус №7",
        summary="Насос хозяйственно-питьевого водоснабжения корпуса 7, повышение давления.",
    )]
    right = [
        _body(sheet=str(page), title=f"Спецификация {page}", summary="Общая ведомость материалов.")
        for page in range(1, 12)
    ] + [_body(
        sheet="44", title="Установка повышения давления", zone="Корпус №7",
        summary="Насос хозяйственно-питьевого водоснабжения корпуса 7, повышение давления.",
    )]
    sheets, functions = _fixture(left, right)

    result = retrieve_candidates(
        pair_id="fixture", left_page=1, passports=sheets, function_passports=functions,
    )

    target = next(item for item in result["candidates"] if item["right_physical_page"] == 12)
    assert target["channel_ranks"]["FUNCTION"] == 1
    assert "FUNCTION" in target["which_channels_found"]


def test_correct_candidate_outside_page_window() -> None:
    left = [_body(sheet="1", title="Схема водомерного узла", summary="Ввод В1 и общедомовой водомерный узел.")]
    right = [
        _body(sheet=str(page), title="Пустой лист", summary=f"Нерелевантный текст {page}.")
        for page in range(1, 15)
    ] + [_body(sheet="30", title="Схема водомерного узла", summary="Ввод В1 и общедомовой водомерный узел.")]
    sheets, functions = _fixture(left, right)

    result = retrieve_candidates(
        pair_id="fixture", left_page=1, passports=sheets, function_passports=functions,
    )

    rank = next(item["rank"] for item in result["candidates"] if item["right_physical_page"] == 15)
    assert rank <= FINAL_TOP_K
    assert abs(1 - 15) > FINAL_TOP_K


def test_null_signal_does_not_mean_mismatch() -> None:
    left, right = _fixture(
        [_body(sheet="1", title="Лист", summary="Общая функция.")],
        [_body(sheet="2", title="Лист", summary="Общая функция.", zone="Корпус №2")],
    )[0].values()
    left[1]["object_corpus"] = []
    left[1]["zone"] = []

    assert explicit_contradictions(left[1], right[1]) == []


def test_explicit_contradiction_is_separate_negative_signal() -> None:
    sheets, _functions = _fixture(
        [_body(sheet="1", title="Стояки", summary="Водоснабжение.", zone="Корпус №1")],
        [_body(sheet="1", title="Стояки", summary="Водоснабжение.", zone="Корпус №2")],
    )

    contradictions = explicit_contradictions(sheets["LEFT"][1], sheets["RIGHT"][1])

    assert contradictions[0]["kind"] == "INCOMPATIBLE_CORPUS"
    assert contradictions[0]["penalty"] > 0


def test_union_retrieval_keeps_single_channel_candidate() -> None:
    sheets, functions = _fixture(
        [_body(sheet="9", title="Аварийный щит", summary="Питание освещения.", entities="ЩР-77а")],
        [
            _body(sheet="1", title="Насос", summary="Насос водоснабжения."),
            _body(sheet="2", title="Новый аварийный щит", summary="Другая редакция.", entities="ЩАО-77"),
        ],
    )

    result = retrieve_candidates(
        pair_id="fixture", left_page=1, passports=sheets, function_passports=functions,
    )
    target = next(item for item in result["candidates"] if item["right_physical_page"] == 2)

    assert "ENTITY" in target["which_channels_found"]
    assert set(target["channel_scores"]) == set(CHANNELS)


def test_duplicate_candidate_deduplication() -> None:
    sheets, functions = _fixture(
        [_body(sheet="1", title="Водоснабжение корпуса", summary="Водоснабжение корпуса 1.", zone="Корпус №1")],
        [_body(sheet="1", title="Водоснабжение корпуса", summary="Водоснабжение корпуса 1.", zone="Корпус №1")],
    )
    result = retrieve_candidates(
        pair_id="fixture", left_page=1, passports=sheets, function_passports=functions,
    )

    assert len(result["candidates"]) == 1
    assert len({item["candidate_id"] for item in result["candidates"]}) == 1
    assert len(result["candidates"][0]["which_channels_found"]) > 1


def test_bounded_top_k() -> None:
    left = [_body(sheet="1", title="Водоснабжение", summary="Стояки водоснабжения.")]
    right = [
        _body(sheet=str(page), title="Водоснабжение", summary=f"Стояки водоснабжения вариант {page}.")
        for page in range(1, 21)
    ]
    sheets, functions = _fixture(left, right)

    result = retrieve_candidates(
        pair_id="fixture", left_page=1, passports=sheets, function_passports=functions, top_k=4,
    )

    assert result["candidate_count"] == 4
    assert result["union_candidate_count"] <= result["bounds"]["max_union"]


def test_one_to_many_group_generation() -> None:
    sheets, functions = _fixture(
        [_body(sheet="1", title="Однолинейная схема ВРУ", summary="Распределение нагрузок ВРУ.")],
        [
            _body(sheet="2.1", title="Однолинейная схема ВРУ (начало)", summary="Распределение нагрузок ВРУ."),
            _body(sheet="2.2", title="Однолинейная схема ВРУ (конец)", summary="Расчет нагрузок ВРУ."),
        ],
    )
    candidates = [_candidate(1, 1), _candidate(2, 2)]

    groups = compose_one_to_many_groups(
        pair_id="fixture", left_page=1,
        candidate_set={"candidates": candidates, "_full_union": candidates},
        passports=sheets, function_passports=functions,
    )

    assert any(item["relation_type"] == "SPLIT_1_TO_N" and item["right_pages"] == [1, 2] for item in groups)


def test_many_to_one_group_generation() -> None:
    sheets, functions = _fixture(
        [
            _body(sheet="1", title="Схема ВРУ-3", summary="Распределение ВРУ-3, часть один."),
            _body(sheet="2", title="Схема ВРУ-3", summary="Распределение ВРУ-3, часть два."),
        ],
        [_body(sheet="7", title="Схема ВРУ-3", summary="Объединенная схема распределения ВРУ-3.")],
    )
    row = _candidate(1, 1)
    candidate_sets = {
        1: {"candidates": [row], "_full_union": [row]},
        2: {"candidates": [row], "_full_union": [row]},
    }

    groups = compose_many_to_one_groups(
        pair_id="fixture", candidate_sets=candidate_sets,
        passports=sheets, function_passports=functions,
    )

    assert any(item["relation_type"] == "MERGED_N_TO_1" and item["left_pages"] == [1, 2] for item in groups)


def test_function_distributed_group_generation() -> None:
    sheets, functions = _fixture(
        [_body(
            sheet="5", title="Насосная станция и водомерный узел",
            summary="Насосная ХВС и ВПВ: насос хозяйственно-питьевого водоснабжения, установка пожаротушения, общедомовой водомерный узел и ввод В1.",
        )],
        [
            _body(sheet="1", title="Насосная ХВС", summary="Насос хозяйственно-питьевого водоснабжения и повышение давления."),
            _body(sheet="2", title="Примечания", summary="Общие примечания."),
            _body(sheet="7", title="Насосная ВПВ", summary="Насосная ВПВ и установка пожаротушения."),
            _body(sheet="8", title="Примечания", summary="Общие примечания."),
            _body(sheet="6", title="Водомерный узел", summary="Водомерный узел с двумя вводами, ввод В1."),
        ],
    )
    candidates = [_candidate(1, 1, 0.9), _candidate(3, 2, 0.85), _candidate(5, 3, 0.8)]

    groups = compose_one_to_many_groups(
        pair_id="fixture", left_page=1,
        candidate_set={"candidates": candidates, "_full_union": candidates},
        passports=sheets, function_passports=functions,
    )

    distributed = next(item for item in groups if item["relation_type"] == "FUNCTION_DISTRIBUTED" and item["right_pages"] == [1, 3, 5])
    assert set(distributed["component_coverage"]) == {
        "DOMESTIC_PRESSURE_BOOST", "FIRE_PRESSURE_BOOST", "INCOMING_METERING",
    }


def test_no_premature_global_displacement() -> None:
    sheets, functions = _fixture(
        [
            _body(sheet="1", title="Общая функция", summary="Объединенная схема водоснабжения."),
            _body(sheet="2", title="Общая функция", summary="Объединенная схема водоснабжения."),
        ],
        [_body(sheet="1", title="Общая функция", summary="Объединенная схема водоснабжения.")],
    )
    sets = {
        page: retrieve_candidates(
            pair_id="fixture", left_page=page, passports=sheets, function_passports=functions,
        )
        for page in (1, 2)
    }

    assert all(sets[page]["candidates"][0]["right_physical_page"] == 1 for page in (1, 2))
    groups = compose_many_to_one_groups(
        pair_id="fixture", candidate_sets=sets, passports=sheets, function_passports=functions,
    )
    assert any(item["left_pages"] == [1, 2] and item["right_pages"] == [1] for item in groups)


def test_deterministic_repeatability() -> None:
    sheets, functions = _fixture(
        [_body(sheet="1", title="Стояки", summary="Водоснабжение корпуса 1.", zone="Корпус №1")],
        [
            _body(sheet="1", title="Стояки", summary="Водоснабжение корпуса 1.", zone="Корпус №1"),
            _body(sheet="2", title="Стояки", summary="Водоснабжение корпуса 2.", zone="Корпус №2"),
        ],
    )
    first = retrieve_candidates(pair_id="fixture", left_page=1, passports=sheets, function_passports=functions)
    second = retrieve_candidates(pair_id="fixture", left_page=1, passports=sheets, function_passports=functions)

    assert first == second


def test_production_v3_sources_unchanged_and_ios21_regression() -> None:
    production_files = [
        REPO_ROOT / "backend/app/services/stage_comparison/sheet_matcher.py",
        REPO_ROOT / "backend/app/services/stage_comparison/production_orchestrator.py",
    ]
    before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in production_files}

    dataset = build_candidate_v4_dataset(REPO_ROOT, "pe336037597")

    after = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in production_files}
    assert before == after
    assert production_sources_unchanged(dataset.base)
    for left, right in ((17, 7), (18, 8), (19, 9)):
        assert right in {
            item["right_physical_page"] for item in dataset.candidate_sets[left]["candidates"]
        }
    distributed = next(
        item for item in dataset.group_candidates
        if item["candidate_group_id"] == "fcand_6294159aac7851a636dd"
    )
    assert distributed["right_pages"] == [26, 28, 29]

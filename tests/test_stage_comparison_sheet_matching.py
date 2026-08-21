from __future__ import annotations

from pathlib import Path

import fitz
import pytest

from backend.app.services.stage_comparison import sheet_matching, store


def _record(page: int, sheet: str | None, title: str | None) -> dict:
    display = f"Sheet {sheet}" if sheet else f"Page {page}"
    if title:
        display += f" — {title}"
    return {"pdf_page": page, "sheet_number": sheet, "title": title, "display": display}


def _html(*labels: str) -> str:
    links = "\n".join(
        f'<li><a href="#page-{index}">{label}</a></li>' for index, label in enumerate(labels)
    )
    return f"<html><body><ol>{links}</ol></body></html>"


def _write_pdf(path: Path, pages: int) -> None:
    document = fitz.open()
    for number in range(1, pages + 1):
        page = document.new_page(width=200, height=120)
        page.insert_text((20, 40), f"page {number}")
    path.write_bytes(document.tobytes())
    document.close()


def test_parse_html_sheet_index_and_page_anchor_number():
    index = sheet_matching.extract_sheet_index_from_results_html(_html(
        "Page 1",
        "Page 2",
        "Sheet 10 — Корпуса 1, 2. План кровли.",
    ))

    assert index == [
        _record(1, None, None),
        _record(2, None, None),
        _record(3, "10", "Корпуса 1, 2. План кровли"),
    ]
    assert sheet_matching.extract_sheet_index_from_results_html(
        '<a href="#page-11">Sheet 8</a>'
    )[0]["pdf_page"] == 12


def test_parser_ignores_page_body_metadata_and_unrelated_links():
    html = """
    <a href="#other">Sheet 99 — Wrong</a>
    <a href="#page-0"><span>Sheet 1</span> — Кладочные планы</a>
    <section id="page-0">Summary: wrong. Description: wrong. Verification: wrong.</section>
    """

    assert sheet_matching.extract_sheet_index_from_results_html(html) == [
        _record(1, "1", "Кладочные планы")
    ]


def test_canonicalize_title_is_cosmetic_only():
    canonical = sheet_matching.canonicalize_sheet_title

    assert canonical(" Корпуса 1, 2.  План кровли. ") == "корпуса 1, 2. план кровли"
    assert canonical("Кладочный план - 1 этажа") == canonical("Кладочный план –1 этажа")
    assert canonical("Оси Ё.1 — Ё.4") == "оси е.1-е.4"
    assert canonical("М 1_200") == canonical("м 1:200")


def test_same_sheet_number_and_exact_title_is_high():
    result = sheet_matching.match_sheet_indexes(
        [_record(1, "2", "Архитектурные решения. Кладочные планы.")],
        [_record(8, "2", "Архитектурные решения. Кладочные планы")],
    )

    suggestion = result["suggestions"][0]
    assert suggestion["confidence"] == "high"
    assert suggestion["reason"] == ["same_sheet_number_and_title"]
    assert suggestion["primary_right_page"] == 8


def test_same_unique_title_is_high_even_without_sheet():
    result = sheet_matching.match_sheet_indexes(
        [_record(1, None, "Узел устройства стойки фахверка")],
        [_record(9, None, "Узел устройства стойки фахверка")],
    )

    assert result["suggestions"][0]["confidence"] == "high"
    assert result["suggestions"][0]["reason"] == ["same_unique_title"]


def test_minus_one_spacing_matches_as_exact_title():
    result = sheet_matching.match_sheet_indexes(
        [_record(1, "4", "Кладочный план -1 этажа")],
        [_record(2, "4", "Кладочный план - 1 этажа")],
    )

    assert result["suggestions"][0]["reason"] == ["same_sheet_number_and_title"]


def test_similar_title_and_same_sheet_is_high():
    result = sheet_matching.match_sheet_indexes(
        [_record(1, "7", "План кровли корпуса 1")],
        [_record(2, "7", "План кровли корпуса 1А")],
    )

    suggestion = result["suggestions"][0]
    assert suggestion["title_similarity"] >= 0.92
    assert suggestion["confidence"] == "high"
    assert suggestion["reason"] == ["similar_title"]


def test_similar_title_with_different_sheet_is_medium():
    result = sheet_matching.match_sheet_indexes(
        [_record(1, "7", "План кровли корпуса 1")],
        [_record(2, "8", "План кровли корпуса 1А")],
    )

    assert result["suggestions"][0]["confidence"] == "medium"


def test_equipment_identifier_survives_generic_words_and_punctuation():
    result = sheet_matching.match_sheet_indexes(
        [
            _record(24, "3", "Однолинейная расчетная схема ВРУ2"),
            _record(51, "28", "Однолинейная расчетная схема ВРУ.ИТП"),
        ],
        [
            _record(26, "4", "Однолинейная схема ВРУ-2"),
            _record(32, "9", "Однолинейная схема ВРУ-ИТП"),
        ],
    )

    assert [item["primary_right_page"] for item in result["suggestions"]] == [26, 32]
    assert all(
        item["reason"] == ["same_equipment_identifier"]
        for item in result["suggestions"]
    )


def test_equipment_groups_cover_split_and_merged_sheets_in_order():
    result = sheet_matching.match_sheet_indexes(
        [
            _record(24, "1", "Однолинейная расчетная схема ВРУ-1"),
            _record(25, "2", "Однолинейная расчетная схема ВРУ-1"),
            _record(27, "4", "Однолинейная расчетная схема ВРУ-3"),
            _record(28, "5", "Однолинейная расчетная схема ВРУ-3"),
        ],
        [
            _record(40, "2", "Однолинейная схема ВРУ-1 (начало)"),
            _record(41, "3", "Однолинейная схема ВРУ-1 (конец)"),
            _record(42, "5", "Однолинейная схема ВРУ-3"),
        ],
    )

    suggestions = result["suggestions"]
    assert [item["primary_right_pages"] for item in suggestions] == [
        [40], [41], [42], [42],
    ]
    assert suggestions[2]["confidence"] == suggestions[3]["confidence"] == "medium"
    assert result["unmatched_right_pages"] == []


def test_fractional_continuation_pages_form_one_semantic_group():
    result = sheet_matching.match_sheet_indexes(
        [_record(52, "29", "Однолинейная расчетная схема ГРЩ")],
        [
            _record(21, "1.1", "Однолинейная схема ГРЩ"),
            _record(22, "1.2", None),
            _record(23, "1.3", None),
        ],
    )

    suggestion = result["suggestions"][0]
    assert suggestion["primary_right_pages"] == [21, 22, 23]
    assert suggestion["confidence"] == "medium"
    assert result["unmatched_right_pages"] == []
    summary = store._matching_summary(result, {"links": [], "unlinked_left_pages": []})
    assert summary["unmatched_right"] == 0


def test_repeated_panel_series_infers_rename_and_two_missing_titles():
    left_titles = [
        "Однолинейная расчетная схема ЩР-1",
        "Однолинейная расчетная схема ЩР-1а",
        None,
        "Однолинейная расчетная схема ЩР-2а",
        "Однолинейная расчетная схема ЩР-3",
        "Однолинейная расчетная схема ЩР-3а",
        "Однолинейная расчетная схема ЩР-4",
        "Однолинейная расчетная схема ЩР-4а",
        "Однолинейная расчетная схема ЩР-5",
        "Однолинейная расчетная схема ЩР-5а",
        "Однолинейная расчетная схема ЩР-6",
        None,
    ]
    left = [
        _record(page, str(sheet), title)
        for page, sheet, title in zip(range(39, 51), range(16, 28), left_titles)
    ]
    right = [
        *[_record(32 + number, str(10 + number), f"Однолинейная схема ЩО-{number}")
          for number in range(1, 7)],
        *[_record(41 + number, str(13 + number), f"Однолинейная схема ЩАО-{number}")
          for number in range(1, 7)],
    ]

    result = sheet_matching.match_sheet_indexes(left, right)
    by_left = {
        item["left_page"]: item["primary_right_page"]
        for item in result["suggestions"]
    }

    assert [by_left[page] for page in range(39, 51)] == [
        33, 42, 34, 43, 35, 44, 36, 45, 37, 46, 38, 47,
    ]
    assert all(
        item["reason"] == ["inferred_panel_series_rename"]
        for item in result["suggestions"]
    )


def test_markdown_summary_disambiguates_repeated_volume_titles():
    markdown = """
## Page 37
> **Stamp:** | Name: Общее название |
**Summary:** Схема подключения БВУ к блокам БПИ в электрощитовой.
## Page 38
> **Stamp:** | Name: Общее название |
**Summary:** Система молниезащиты и заземления, уравнивание потенциалов.
"""
    semantics = sheet_matching.extract_page_semantics_from_markdown(markdown)
    assert "Общее название" not in semantics[37]
    left = [
        {**_record(37, "14", "Общее название"), "_semantic_text": semantics[37]},
        {**_record(38, "15", "Общее название"), "_semantic_text": semantics[38]},
    ]
    right = [
        {**_record(45, "23", "Другое общее название"), "_semantic_text": semantics[37]},
        {**_record(46, "24", "Другое общее название"), "_semantic_text": semantics[38]},
    ]

    result = sheet_matching.match_sheet_indexes(left, right)

    assert [item["primary_right_page"] for item in result["suggestions"]] == [45, 46]
    assert all(item["reason"] == ["same_semantic_topic"] for item in result["suggestions"])
    assert all("_semantic_text" not in item for item in result["left_sheet_index"])


@pytest.mark.parametrize(("left_title", "right_title"), [
    ("Архитектурный план этажа", "План с маркировкой отверстий"),
    ("План кровли", "Принципиальная схема электроснабжения"),
])
def test_title_conflict_is_unmatched(left_title: str, right_title: str):
    result = sheet_matching.match_sheet_indexes(
        [_record(1, "5", left_title)],
        [_record(2, "5", right_title)],
    )

    suggestion = result["suggestions"][0]
    assert suggestion["primary_right_page"] is None
    assert suggestion["confidence"] == "unmatched"


def test_repeated_title_is_disambiguated_by_sheet_number():
    title = "Архитектурные решения. Кладочные планы"
    result = sheet_matching.match_sheet_indexes(
        [_record(1, "1", title), _record(2, "2", title)],
        [_record(11, "2", title), _record(12, "1", title)],
    )

    assert [item["primary_right_page"] for item in result["suggestions"]] == [12, 11]
    assert all(item["confidence"] == "high" for item in result["suggestions"])


def test_global_sequence_repairs_duplicate_sheet_number_and_conflicting_titles():
    common = "Часть 1. Архитектурные решения. Планы"
    result = sheet_matching.match_sheet_indexes(
        [
            _record(24, "1", common),
            _record(25, "2", common),
            _record(26, "3", common),
            _record(27, "4", common),
            _record(28, "5", common),
        ],
        [
            _record(3, "1", common),
            _record(4, "2", common),
            _record(5, "2", common),  # OCR error: the PDF title block says Sheet 3.
            _record(6, "4", "Корпуса 1, 2. План 1 этажа"),
            _record(7, "5", "Корпуса 1, 2. План 2 этажа"),
        ],
    )

    suggestions = result["suggestions"]
    assert [item["primary_right_page"] for item in suggestions] == [3, 4, 5, 6, 7]
    assert suggestions[2]["reason"] == ["sequence_repaired_sheet_number"]
    assert suggestions[3]["reason"] == ["same_sheet_number_and_sequence"]
    assert suggestions[4]["reason"] == ["same_sheet_number_and_sequence"]
    assert result["unmatched_left_pages"] == []
    assert result["unmatched_right_pages"] == []


def test_global_sequence_skips_duplicate_service_page_before_drawings():
    common = "Архитектурные решения. Кладочные планы"
    result = sheet_matching.match_sheet_indexes(
        [
            _record(5, "1", None),
            _record(6, "1", common),
            _record(7, "2", "Кладочный план -1 этажа"),
            _record(8, "3", "Кладочный план технического пространства"),
            _record(9, "4", common),
        ],
        [
            _record(5, "1", common),
            _record(6, "2", "Кладочный план - 1 этажа"),
            _record(7, "3", "Кладочный план технического пространства"),
            _record(8, "4", common),
        ],
    )

    assert [item["primary_right_page"] for item in result["suggestions"]] == [
        None, 5, 6, 7, 8,
    ]
    assert result["unmatched_left_pages"] == [5]
    assert result["unmatched_right_pages"] == []


def test_global_sequence_prefers_drawing_run_after_sheet_number_reset():
    notes = "Пояснительная записка"
    drawings = "Мероприятия по обеспечению доступности инвалидов"
    left = [
        *[_record(page, str(page), notes) for page in range(1, 7)],
        *[_record(page + 6, str(page), drawings) for page in range(1, 6)],
    ]
    right = [_record(page + 20, str(page), drawings) for page in range(1, 6)]

    result = sheet_matching.match_sheet_indexes(left, right)

    by_left = {
        item["left_page"]: item["primary_right_page"]
        for item in result["suggestions"]
    }
    assert [by_left[page] for page in range(7, 12)] == [21, 22, 23, 24, 25]
    assert all(by_left[page] is None for page in range(1, 7))


def test_global_sequence_aligns_more_than_one_independent_numbered_run():
    left = [
        *[_record(page, str(page), None) for page in range(1, 4)],
        *[_record(page + 3, str(page), None) for page in range(1, 4)],
    ]
    right = [
        *[_record(page + 10, str(page), None) for page in range(1, 4)],
        *[_record(page + 13, str(page), None) for page in range(1, 4)],
    ]

    result = sheet_matching.match_sheet_indexes(left, right)

    assert [item["primary_right_page"] for item in result["suggestions"]] == [
        11, 12, 13, 14, 15, 16,
    ]


def test_page_without_sheet_or_title_does_not_become_high():
    result = sheet_matching.match_sheet_indexes(
        [_record(1, None, None)],
        [_record(1, None, None)],
    )

    assert result["suggestions"][0]["primary_right_page"] is None
    assert result["unmatched_left_pages"] == [1]
    assert result["unmatched_right_pages"] == [1]


def test_top_three_uses_title_similarity_and_same_sheet_only():
    right = [_record(page, str(page), f"План кровли корпуса {page}") for page in range(1, 6)]
    result = sheet_matching.match_sheet_indexes(
        [_record(1, "X", "Совершенно другое название")], right
    )

    assert result["suggestions"][0]["primary_right_page"] is None
    assert len(result["suggestions"][0]["alternatives"]) == 3


def test_missing_html_returns_clear_status_and_manual_page_rows(tmp_path, monkeypatch):
    stage_1 = tmp_path / "stage_1"
    stage_2 = tmp_path / "stage_2"
    stage_1.mkdir()
    stage_2.mkdir()
    left_pdf = stage_1 / "design.pdf"
    right_pdf = stage_2 / "working.pdf"
    _write_pdf(left_pdf, 2)
    _write_pdf(right_pdf, 3)
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "runtime"))
    session, _ = store.create_session(str(stage_1), str(stage_2))
    pair = store.create_pair(session["id"], str(left_pdf), str(right_pdf))["pair"]

    state = store.run_sheet_matching(session["id"], pair["id"])

    assert state["suggestions"]["status"] == "sheet_index_unavailable"
    assert state["suggestions"]["unavailable_sides"] == ["left", "right"]
    assert len(state["suggestions"]["left_sheet_index"]) == 2
    assert len(state["suggestions"]["right_sheet_index"]) == 3
    assert all(item["primary_right_page"] is None for item in state["suggestions"]["suggestions"])


def test_manual_many_to_many_links_and_deleted_decision_survive_rerun(tmp_path, monkeypatch):
    runtime = tmp_path / "runtime"
    stage_1 = tmp_path / "stage_1"
    stage_2 = tmp_path / "stage_2"
    stage_1.mkdir()
    stage_2.mkdir()
    left_pdf = stage_1 / "design.pdf"
    right_pdf = stage_2 / "working.pdf"
    _write_pdf(left_pdf, 3)
    _write_pdf(right_pdf, 3)
    (stage_1 / "design_results.html").write_text(_html(
        "Sheet 1 — Корпус 4. План 1 этажа",
        "Sheet 2 — Корпус 4. План 2 этажа",
        "Page 3",
    ), encoding="utf-8")
    (stage_2 / "working_results.html").write_text(_html(
        "Sheet 8 — Корпус 4. План 1 этажа",
        "Sheet 9 — Корпус 4. План 2 этажа",
        "Sheet 10 — Корпус 4. План кровли",
    ), encoding="utf-8")
    monkeypatch.setenv("COMPARISON_ROOT", str(runtime))
    session, _ = store.create_session(str(stage_1), str(stage_2))
    pair = store.create_pair(session["id"], str(left_pdf), str(right_pdf))["pair"]

    first = store.run_sheet_matching(session["id"], pair["id"])
    saved = store.save_sheet_links(
        session["id"],
        pair["id"],
        [
            {
                "left_pages": [1], "right_pages": [1, 3], "source": "manual",
                "confidence": "manual", "reason": ["user_corrected"],
            },
            {
                "left_pages": [1, 2], "right_pages": [2], "source": "manual",
                "confidence": "manual", "reason": ["user_corrected"],
            },
        ],
        unlinked_left_pages=[3],
    )
    rerun = store.run_sheet_matching(session["id"], pair["id"])

    assert first["suggestions"]["version"] == 2
    assert first["suggestions"]["suggestions"][0]["primary_right_page"] == 1
    assert saved["links"]["links"][0]["right_pages"] == [1, 3]
    assert saved["links"]["links"][1]["left_pages"] == [1, 2]
    assert saved["links"]["unlinked_left_pages"] == [3]
    assert rerun["links"] == saved["links"]

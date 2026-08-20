from __future__ import annotations

from pathlib import Path

import fitz
import pytest

from backend.app.services.stage_comparison import sheet_matching, store


def _page(page: int, *, sheet: str, name: str, image: str = "") -> str:
    return (
        f"## Page {page}\n"
        f"> **Stamp:** Code: TEST | Sheet: {sheet} | Name: {name} | Organization: TEST\n"
        f"{image}\n{name}\n"
    )


def _passport(page: int, title: str, sheet: str = "1") -> sheet_matching.SheetPassport:
    return sheet_matching.extract_sheet_passport(
        page,
        f"> **Stamp:** Code: X | Sheet: {sheet} | Name: {title} | Org: X\n{title}",
    )


def _write_pdf(path: Path, pages: int) -> None:
    document = fitz.open()
    for number in range(1, pages + 1):
        page = document.new_page(width=200, height=120)
        page.insert_text((20, 40), f"page {number}")
    path.write_bytes(document.tobytes())
    document.close()


def test_split_markdown_pages_and_extract_small_passport():
    markdown = "header\n" + _page(
        7,
        sheet="5",
        name="Корпуса 3, 3.1. План 2 этажа",
        image="**[IMAGE]** | Type: План | Level: +15,000",
    ) + _page(8, sheet="6", name="Корпус 4. План кровли")

    sections = sheet_matching.split_markdown_pages(markdown)
    passports = sheet_matching.build_sheet_passports(markdown)

    assert [section.pdf_page for section in sections] == [7, 8]
    assert passports[0].sheet_number == "5"
    assert passports[0].buildings == ("3", "3.1")
    assert passports[0].floor == "2"
    assert passports[0].level == "+15.000"
    assert passports[0].kind == "plan"
    assert passports[0].sheet_title == "Корпуса 3, 3.1. План 2 этажа"
    assert passports[0].sheet_title_source == "stamp"
    assert passports[0].sheet_title_reliable is True
    assert passports[1].roof is True
    assert passports[1].kind == "roof"


def test_underground_and_contents_title_are_plain_md_signals():
    markdown = (
        "## Page 1\nСодержание тома\n"
        "| Лист | Наименование | Примечание |\n|---|---|---|\n"
        "| 10 | Корпуса 1, 2. План второго подземного этажа | |\n"
        + _page(9, sheet="10", name="", image="**[IMAGE]** | Type: План | Level: -9,600")
    )

    passport = sheet_matching.build_sheet_passports(markdown)[1]

    assert passport.buildings == ("1", "2")
    assert passport.underground is True
    assert passport.underground_level == 2
    assert passport.level == "-9.600"
    assert passport.source["contents_title"].startswith("Корпуса 1, 2")
    assert passport.sheet_title_source == "contents"


def test_canonicalize_sheet_title_is_cosmetic_only():
    canonical = sheet_matching.canonicalize_sheet_title

    assert canonical(" Архитектурные решения.  Кладочные планы. ") == (
        "архитектурные решения. кладочные планы"
    )
    assert canonical("Кладочный план - 1 этажа") == canonical("Кладочный план –1 этажа")
    assert canonical("Оси Ё.1 — Ё.4") == "оси е.1-е.4"


def test_exact_title_and_same_sheet_is_high():
    result = sheet_matching.suggest_sheet_matches(
        [_passport(1, "Архитектурные решения. Кладочные планы.", "2")],
        [_passport(8, "Архитектурные решения. Кладочные планы", "2")],
    )

    suggestion = result["suggestions"][0]
    assert suggestion["confidence"] == "high"
    assert suggestion["reason"][:2] == ["same_sheet_title", "same_sheet_number"]


def test_exact_unique_title_without_sheet_is_high():
    result = sheet_matching.suggest_sheet_matches(
        [_passport(1, "Узел устройства стойки фахверка", "")],
        [_passport(9, "Узел устройства стойки фахверка", "")],
    )

    suggestion = result["suggestions"][0]
    assert suggestion["confidence"] == "high"
    assert suggestion["reason"][0] == "same_unique_sheet_title"


def test_minus_one_spacing_matches_as_the_same_title():
    result = sheet_matching.suggest_sheet_matches(
        [_passport(1, "Кладочный план -1 этажа", "4")],
        [_passport(2, "Кладочный план - 1 этажа", "4")],
    )

    assert result["suggestions"][0]["confidence"] == "high"
    assert result["suggestions"][0]["reason"][0] == "same_sheet_title"


def test_repeated_title_is_disambiguated_by_matching_sheet():
    title = "Архитектурные решения. Кладочные планы"
    left = sheet_matching.build_sheet_passports(
        _page(1, sheet="1", name=title) + _page(2, sheet="2", name=title)
    )
    right = sheet_matching.build_sheet_passports(
        _page(11, sheet="2", name=title) + _page(12, sheet="1", name=title)
    )

    result = sheet_matching.suggest_sheet_matches(left, right)

    assert [item["primary_right_page"] for item in result["suggestions"]] == [12, 11]
    assert all(item["confidence"] == "high" for item in result["suggestions"])


@pytest.mark.parametrize(("left_title", "right_title"), [
    ("Архитектурный план этажа", "План с маркировкой отверстий"),
    (
        "Кладочный план -1 этажа подземной автостоянки в осях П.1-П.12",
        "План с маркировкой отверстий -1 этажа подземной автостоянки в осях П.1-П.12",
    ),
])
def test_title_conflict_caps_passport_match_at_medium(left_title, right_title):
    def passport(page: int, title: str) -> sheet_matching.SheetPassport:
        return sheet_matching.extract_sheet_passport(
            page,
            f"> **Stamp:** Code: X | Sheet: 5 | Name: {title} | Org: X\n"
            "Корпус 4. План 1 этажа\n**[IMAGE]** | Type: План | Level: +3,000",
        )

    result = sheet_matching.suggest_sheet_matches(
        [passport(1, left_title)],
        [passport(2, right_title)],
    )

    suggestion = result["suggestions"][0]
    assert suggestion["confidence"] == "medium"
    assert "title_conflict" in suggestion["reason"]


@pytest.mark.parametrize("bad_title", [
    "Verification: Проверить соответствие осей",
    "Summary: Фрагмент архитектурного плана этажа",
    "Description: На изображении показан план",
])
def test_generated_metadata_is_not_a_sheet_title(bad_title):
    passport = _passport(1, bad_title)

    assert passport.sheet_title is None
    assert passport.canonical_sheet_title is None
    assert passport.sheet_title_source == "none"
    assert passport.sheet_title_reliable is False


def test_high_match_uses_content_and_sheet_number_cannot_override_conflict():
    left = [_passport(10, "Корпуса 1, 2. План 5 этажа", "99")]
    correct = _passport(31, "Корпуса 1, 2. План 5 этажа", "8")
    wrong_same_sheet = _passport(32, "Корпус 4. План 2 этажа", "99")

    result = sheet_matching.suggest_sheet_matches(left, [wrong_same_sheet, correct])
    suggestion = result["suggestions"][0]

    assert suggestion["primary_right_page"] == 31
    assert suggestion["confidence"] == "high"
    assert suggestion["reason"][0] == "same_unique_sheet_title"


def test_top_three_alternatives_and_unmatched_pages_are_kept():
    left = [_passport(1, "Пояснительная записка", "A")]
    right = [_passport(page, f"Корпус {page}. План {page} этажа", str(page)) for page in range(1, 6)]

    result = sheet_matching.suggest_sheet_matches(left, right)
    suggestion = result["suggestions"][0]

    assert suggestion["primary_right_page"] is None
    assert len(suggestion["alternatives"]) == 3
    assert result["unmatched_left_pages"] == [1]
    assert result["unmatched_right_pages"] == [1, 2, 3, 4, 5]


def test_manual_many_to_many_links_survive_auto_rerun(tmp_path, monkeypatch):
    runtime = tmp_path / "runtime"
    stage_1 = tmp_path / "stage_1"
    stage_2 = tmp_path / "stage_2"
    stage_1.mkdir()
    stage_2.mkdir()
    left_pdf = stage_1 / "design.pdf"
    right_pdf = stage_2 / "working.pdf"
    _write_pdf(left_pdf, 2)
    _write_pdf(right_pdf, 3)
    (stage_1 / "design_results.md").write_text(
        _page(1, sheet="1", name="Корпус 4. План 1 этажа")
        + _page(2, sheet="2", name="Корпус 4. План 2 этажа"),
        encoding="utf-8",
    )
    (stage_2 / "working_results.md").write_text(
        _page(1, sheet="8", name="Корпус 4. План 1 этажа")
        + _page(2, sheet="9", name="Корпус 4. План 2 этажа")
        + _page(3, sheet="10", name="Корпус 4. План кровли"),
        encoding="utf-8",
    )
    monkeypatch.setenv("COMPARISON_ROOT", str(runtime))
    session, _ = store.create_session(str(stage_1), str(stage_2))
    pair = store.create_pair(session["id"], str(left_pdf), str(right_pdf))["pair"]

    first = store.run_sheet_matching(session["id"], pair["id"])
    saved = store.save_sheet_links(
        session["id"],
        pair["id"],
        [
            {
                "left_pages": [1],
                "right_pages": [1, 3],
                "source": "manual",
                "confidence": "manual",
                "reason": ["user_corrected"],
            },
            {
                "left_pages": [1, 2],
                "right_pages": [2],
                "source": "manual",
                "confidence": "manual",
                "reason": ["user_corrected"],
            },
        ],
    )
    rerun = store.run_sheet_matching(session["id"], pair["id"])

    assert first["suggestions"]["suggestions"][0]["primary_right_page"] == 1
    assert saved["links"]["links"][0]["right_pages"] == [1, 3]  # one P -> many RD
    assert saved["links"]["links"][1]["left_pages"] == [1, 2]  # many P -> one RD
    assert rerun["links"] == saved["links"]

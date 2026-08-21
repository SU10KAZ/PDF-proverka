from __future__ import annotations

import copy
import hashlib
from pathlib import Path

import fitz

from backend.app.services.stage_comparison import text_comparison as tc


def _fragment(
    side: str, page: int, text: str, index: int = 0,
    *, x: float = 0.1, y: float = 0.1, kind: str = "paragraph",
) -> dict:
    canonical = tc.canonicalize_text(text)
    return {
        "id": f"{side}_{page}_{index}_{hashlib.sha1(canonical.encode()).hexdigest()[:8]}",
        "stage": "stage_1" if side == "left" else "stage_2",
        "pdf_page": page,
        "sheet_number": str(page),
        "text": text,
        "canonical_text": canonical,
        "source_block_id": f"block_{page}",
        "source_kind": kind,
        "source_group": f"block_{page}",
        "order": index,
        "char_count": len(canonical),
        "bboxes": [{"x": x, "y": y, "width": 0.2, "height": 0.03}],
        "source_location": {"pdf_page": page},
    }


def _link(left_pages=(1,), right_pages=(1,)) -> dict:
    return {
        "id": "manual_link",
        "left_pages": list(left_pages),
        "right_pages": list(right_pages),
        "source": "manual",
        "confidence": "manual",
        "reason": ["user_corrected"],
    }


def test_canonical_exact_text_is_formatting_only() -> None:
    assert tc.canonicalize_text(" **Площадь 15,000 м²** — План ") == tc.canonicalize_text(
        "площадь 15.000 м² - план"
    )
    assert tc.canonicalize_text("Площадь 15,2 м²") != tc.canonicalize_text(
        "Площадь 15,8 м²"
    )


def test_same_text_inside_linked_pair_is_excluded() -> None:
    left = [_fragment("left", 1, "Кладочный план первого этажа")]
    right = [_fragment("right", 1, "Кладочный план первого этажа")]
    result = tc.compare_fragments(left, right, [_link()])
    assert [item["status"] for item in result["matches"]] == ["same_on_linked_sheet"]
    assert result["remaining"] == {"left": [], "right": []}


def test_different_numeric_value_remains() -> None:
    left = [_fragment("left", 1, "Электрощитовая — 12.4 м²")]
    right = [_fragment("right", 1, "Электрощитовая — 13.1 м²")]
    result = tc.compare_fragments(left, right, [_link()])
    assert result["matches"] == []
    assert result["remaining"] == {"left": [left[0]["id"]], "right": [right[0]["id"]]}


def test_markdown_table_is_fragmented_and_compared_by_rows() -> None:
    left_md = """## Page 1
### BLOCK #1 [TEXT]: block_left

| Номер | Наименование | Площадь |
|---|---|---|
| 01.23 | Электрощитовая | 12.4 м² |
| 01.24 | Коридор | 18.0 м² |
"""
    right_md = left_md.replace("block_left", "block_right").replace("18.0", "19.0")
    left = tc.parse_markdown_fragments(left_md, "stage_1")
    right = tc.parse_markdown_fragments(right_md, "stage_2")
    rows_left = [item for item in left if item["source_kind"] == "table_row"]
    rows_right = [item for item in right if item["source_kind"] == "table_row"]
    assert len(rows_left) == len(rows_right) == 3
    result = tc.compare_fragments(left, right, [_link()])
    matched = {item["canonical_text"] for item in result["matches"]}
    assert tc.canonicalize_text("01.23 Электрощитовая 12.4 м²") in matched
    assert all("01.24" not in item for item in matched)


def test_remaining_text_found_elsewhere_has_actual_page_and_is_excluded() -> None:
    phrase = "Маркировка помещений общего пользования"
    left = [_fragment("left", 1, phrase)]
    right = [
        _fragment("right", 1, "Совсем другой текст"),
        _fragment("right", 2, phrase),
    ]
    result = tc.compare_fragments(left, right, [_link()], right_page_count=2)
    found = result["matches"][0]
    assert found["status"] == "found_on_other_sheet"
    assert found["right_page"] == 2
    assert found["expected_right_pages"] == [1]
    assert left[0]["id"] not in result["remaining"]["left"]


def test_found_elsewhere_target_is_not_left_as_linked_side_remaining() -> None:
    phrase = "Длинная уникальная строка вынесена на соседний лист"
    left = [_fragment("left", 1, phrase)]
    right = [_fragment("right", 1, "Иной текст"), _fragment("right", 2, phrase)]
    result = tc.compare_fragments(left, right, [_link()], right_page_count=2)
    matched_target = result["matches"][0]["right_fragment_id"]
    assert matched_target in result["used_right"]
    assert result["remaining"]["left"] == []


def test_short_generic_text_does_not_create_cross_sheet_hint() -> None:
    left = [_fragment("left", 1, "План")]
    right = [_fragment("right", 1, "Иное"), _fragment("right", 2, "План")]
    result = tc.compare_fragments(left, right, [_link()], right_page_count=2)
    assert result["matches"] == []


def test_frequent_phrase_does_not_create_false_cross_sheet_link() -> None:
    phrase = "Условные обозначения для проекта"
    left = [_fragment("left", 1, phrase)]
    right = [_fragment("right", 1, "Иное")]
    right.extend(_fragment("right", page, phrase, page) for page in range(2, 10))
    result = tc.compare_fragments(left, right, [_link()], right_page_count=10)
    assert not [item for item in result["matches"] if item["status"] == "found_on_other_sheet"]


def test_sheet_link_hint_is_advisory_and_does_not_mutate_manual_links() -> None:
    link = _link()
    original = copy.deepcopy(link)
    phrase_a = "Полное наименование инженерного помещения номер 101"
    phrase_b = "Полное наименование инженерного помещения номер 102"
    left = [_fragment("left", 1, phrase_a), _fragment("left", 1, phrase_b, 1)]
    right = [
        _fragment("right", 1, "Иной лист"),
        _fragment("right", 2, phrase_a),
        _fragment("right", 2, phrase_b, 1),
    ]
    result = tc.compare_fragments(left, right, [link], right_page_count=2)
    _, hints, _ = tc.build_metrics_and_hints(
        result, left, right, [link], {}, {2: "Лист 2 — План"}
    )
    assert hints and hints[0]["actual_page"] == 2
    assert link == original


def test_one_p_to_multiple_rd_pages_remains_supported() -> None:
    common = "Общая площадь квартиры без учета лоджий"
    left = [_fragment("left", 1, common)]
    right = [_fragment("right", 2, "Другой текст"), _fragment("right", 3, common)]
    result = tc.compare_fragments(left, right, [_link((1,), (2, 3))])
    assert result["matches"][0]["status"] == "same_on_linked_sheet"
    assert result["matches"][0]["expected_right_pages"] == [2, 3]


def test_rerun_is_deterministic() -> None:
    left = [_fragment("left", 1, "Детерминированная строка проекта")]
    right = [_fragment("right", 1, "Детерминированная строка проекта")]
    first = tc.compare_fragments(copy.deepcopy(left), copy.deepcopy(right), [_link()])
    second = tc.compare_fragments(copy.deepcopy(left), copy.deepcopy(right), [_link()])
    assert first["matches"] == second["matches"]
    assert first["remaining"] == second["remaining"]


def test_overlay_contains_both_pages_and_found_elsewhere_marker() -> None:
    phrase = "Текст перенесен на другой лист проекта"
    left = [_fragment("left", 1, phrase)]
    right = [_fragment("right", 1, "Иное"), _fragment("right", 2, phrase)]
    result = tc.compare_fragments(left, right, [_link()], right_page_count=2)
    overlays = tc.build_overlays(result["matches"], {"left": {}, "right": {2: "Лист 2"}})
    assert overlays["left"]["1"][0]["status"] == "found_on_other_sheet"
    assert overlays["right"]["2"][0]["counterpart_page"] == 1
    assert "другом листе" in overlays["left"]["1"][0]["title"]


def test_overlay_is_rendered_in_paged_and_continuous_pdf_viewer() -> None:
    root = Path(__file__).resolve().parents[1]
    template = (root / "frontend" / "index.html").read_text(encoding="utf-8")
    css = (root / "frontend" / "static" / "css" / "styles.css").read_text(encoding="utf-8")
    assert template.count("scTextComparisonOverlaysFor") >= 2
    assert "sc-text-comparison-mask" in template
    assert ".sc-text-comparison-mask.is-elsewhere::after" in css


def test_pdf_text_location_is_read_only(tmp_path: Path) -> None:
    pdf_path = tmp_path / "source.pdf"
    document = fitz.open()
    page = document.new_page()
    page.insert_text((72, 100), "Exact deterministic PDF text")
    document.save(pdf_path)
    document.close()
    before = hashlib.sha256(pdf_path.read_bytes()).hexdigest()
    fragment = _fragment("left", 1, "Exact deterministic PDF text")
    fragment["bboxes"] = []
    fragment["source_location"] = None
    tc.attach_pdf_locations([fragment], pdf_path, fitz)
    after = hashlib.sha256(pdf_path.read_bytes()).hexdigest()
    assert fragment["bboxes"]
    assert before == after

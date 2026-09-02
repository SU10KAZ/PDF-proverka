from __future__ import annotations

import copy
import hashlib
import json
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
    assert tc.canonicalize_text("6. Проектируемое здание") == tc.canonicalize_text(
        "6.Проектируемое здание"
    )
    assert tc.canonicalize_text("12.4 м²") != tc.canonicalize_text("12. 4 м²")


def test_repeated_technical_markdown_bullet_is_not_document_text() -> None:
    single = """## Page 1
### BLOCK #1 [TEXT]: one

- Ф3.1 – здания организаций торговли
"""
    repeated = single.replace("- Ф3.1", "- - Ф3.1").replace("one", "two")
    left = tc.parse_markdown_fragments(single, "stage_1")
    right = tc.parse_markdown_fragments(repeated, "stage_2")
    assert left[0]["canonical_text"] == right[0]["canonical_text"]


def test_numbered_notes_split_without_blank_lines_and_drop_duplicate_marker() -> None:
    markdown = """## Page 1
### BLOCK #1 [TEXT]: notes

1. 1. Первое примечание.
2. 2. Второе примечание начинается здесь.
Продолжение второго примечания.
3. 3. Третье примечание.
"""
    fragments = tc.parse_markdown_fragments(markdown, "stage_2")
    assert [item["text"] for item in fragments] == [
        "1. Первое примечание.",
        "2. Второе примечание начинается здесь. Продолжение второго примечания.",
        "3. Третье примечание.",
    ]


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


def test_exact_pdf_text_can_prove_match_when_structured_text_has_typo() -> None:
    left = [_fragment("left", 1, "Адрес: улица Летняя")]
    right = [_fragment("right", 1, "Адрес: улица Лётная")]
    pdf_text = tc.canonicalize_text("Адрес: улица Лётная")
    left[0]["pdf_canonical_text"] = pdf_text
    right[0]["pdf_canonical_text"] = pdf_text
    result = tc.compare_fragments(left, right, [_link()])
    assert len(result["matches"]) == 1
    assert result["matches"][0]["evidence"] == "pdf_text_layer"
    assert result["remaining"] == {"left": [], "right": []}


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


def test_text_evidence_overlay_is_not_rendered_in_pdf_viewer() -> None:
    """The removed TEXT panel cannot leave an interactive viewer layer behind."""
    root = Path(__file__).resolve().parents[1]
    template = (root / "frontend" / "index.html").read_text(encoding="utf-8")
    css = (root / "frontend" / "static" / "css" / "styles.css").read_text(encoding="utf-8")
    assert "scTextEvidenceOverlaysFor" not in template
    assert "sc-text-evidence-overlay" not in template
    assert ".sc-text-evidence-overlay" not in css


def test_pdf_text_location_is_read_only(tmp_path: Path) -> None:
    pdf_path = tmp_path / "source.pdf"
    document = fitz.open()
    page = document.new_page()
    page.insert_text((72, 100), "Exact deterministic PDF text")
    page.insert_text((72, 130), "6.Project building text is identical")
    document.save(pdf_path)
    document.close()
    before = hashlib.sha256(pdf_path.read_bytes()).hexdigest()
    fragment = _fragment("left", 1, "Exact deterministic PDF text")
    fragment["bboxes"] = []
    fragment["source_location"] = None
    numbered_fragment = _fragment(
        "left", 1, "6. Project building text is identical", index=1
    )
    numbered_fragment["bboxes"] = []
    numbered_fragment["source_location"] = None
    tc.attach_pdf_locations([fragment, numbered_fragment], pdf_path, fitz)
    after = hashlib.sha256(pdf_path.read_bytes()).hexdigest()
    assert fragment["bboxes"]
    assert numbered_fragment["bboxes"]
    assert fragment["pdf_canonical_text"] == tc.canonicalize_text(fragment["text"])
    assert numbered_fragment["pdf_canonical_text"] == tc.canonicalize_text(
        numbered_fragment["text"]
    )
    assert before == after


def test_pdf_line_comparison_masks_only_exact_visual_lines(tmp_path: Path) -> None:
    paths = {}
    for side, changed in (("left", "Площадь 15,2 м2"), ("right", "Площадь 15,8 м2")):
        path = tmp_path / f"{side}.pdf"
        document = fitz.open()
        page = document.new_page()
        page.insert_text((72, 100), "Exact visual line in both documents")
        page.insert_text((72, 130), changed)
        document.save(path)
        document.close()
        paths[side] = path
    comparison = tc.compare_pdf_text_lines(
        paths["left"], paths["right"], [_link()], fitz
    )
    matches = comparison["matches"]
    assert len(matches) == 1
    assert matches[0]["evidence"] == "pdf_text_line"
    assert matches[0]["left_bboxes"] and matches[0]["right_bboxes"]
    assert "exact visual line" in matches[0]["canonical_text"]
    assert comparison["summary"]["linked_percent"] < 100


def test_pdf_line_metrics_drive_the_displayed_percentages() -> None:
    metrics = [{
        "link_id": "manual_link",
        "combined": {
            "linked_percent": 12.5,
            "elsewhere_percent": 4.1,
            "remaining_percent": 83.4,
        },
    }]
    summary = {
        "linked_percent": 12.5,
        "found_elsewhere_percent": 4.1,
        "remaining_percent": 83.4,
    }
    pdf_comparison = {
        "link_metrics": [{
            "link_id": "manual_link", "total_chars": 1000,
            "matched_chars": 998, "matches": 20, "linked_percent": 99.8,
        }],
        "summary": {
            "total_chars": 1000, "matched_chars": 998,
            "matches": 20, "linked_percent": 99.8,
        },
    }
    tc.apply_pdf_line_metrics(metrics, summary, pdf_comparison)
    assert metrics[0]["combined"] == {
        "linked_percent": 99.8,
        "elsewhere_percent": 0.2,
        "remaining_percent": 0.0,
    }
    assert summary["linked_percent"] == 99.8
    assert summary["remaining_percent"] == 0.0


def test_pdf_line_overlays_replace_fragment_masks_but_keep_elsewhere_marker() -> None:
    structured = {
        "left": {"1": [
            {"id": "fragment", "x": 0.1, "y": 0.1, "status": "same_on_linked_sheet"},
            {"id": "elsewhere", "x": 0.2, "y": 0.2, "status": "found_on_other_sheet"},
        ]},
        "right": {},
    }
    pdf_lines = {
        "left": {"1": [
            {"id": "line", "x": 0.1, "y": 0.1, "status": "same_on_linked_sheet"},
        ]},
        "right": {},
    }
    result = tc.prefer_pdf_line_overlays(structured, pdf_lines)
    assert [item["id"] for item in result["left"]["1"]] == ["line", "elsewhere"]


def test_whole_exact_text_block_uses_uploaded_polygon(tmp_path: Path) -> None:
    left = [
        _fragment("left", 1, "Первая строка блока", index=0),
        _fragment("left", 1, "Вторая строка блока", index=1),
    ]
    right = [
        _fragment("right", 1, "Первая строка блока", index=0),
        _fragment("right", 1, "Вторая строка блока", index=1),
    ]
    for item in left:
        item["source_block_id"] = "left_block"
    for item in right:
        item["source_block_id"] = "right_block"
    manifests = {}
    for side, block_id, polygon in (
        ("left", "left_block", None),
        ("right", "right_block", [[0.2, 0.3], [0.7, 0.3], [0.7, 0.6]]),
    ):
        path = tmp_path / f"{side}_blocks.json"
        path.write_text(json.dumps({
            "coordinate_space": "normalized_page_top_left",
            "blocks": [{
                "block_id": block_id,
                "page_index": 0,
                "block_type": "text",
                "shape_type": "polygon" if polygon else "rectangle",
                "coords_norm": [0.2, 0.3, 0.7, 0.6],
                "polygon_points": polygon,
            }],
        }), encoding="utf-8")
        manifests[side] = path
    matches = tc.compare_exact_text_blocks(
        left, right, manifests["left"], manifests["right"], [_link()]
    )
    assert len(matches) == 1
    assert matches[0]["evidence"] == "uploaded_text_block"
    assert matches[0]["left_bboxes"][0] == {
        "x": 0.2, "y": 0.3, "width": 0.5, "height": 0.3,
    }
    assert matches[0]["right_bboxes"][0]["polygon"] == [
        [0.0, 0.0], [1.0, 0.0], [1.0, 1.0]
    ]
    right[1]["canonical_text"] = tc.canonicalize_text("Изменённая строка")
    assert tc.compare_exact_text_blocks(
        left, right, manifests["left"], manifests["right"], [_link()]
    ) == []


def test_exact_block_overlay_replaces_only_masks_inside_block() -> None:
    current = {
        "left": {"1": [
            {"id": "inside", "x": 0.2, "y": 0.2, "width": 0.1, "height": 0.02},
            {"id": "outside", "x": 0.8, "y": 0.8, "width": 0.1, "height": 0.02},
        ]},
        "right": {},
    }
    blocks = {
        "left": {"1": [
            {"id": "block", "x": 0.1, "y": 0.1, "width": 0.5, "height": 0.5},
        ]},
        "right": {},
    }
    result = tc.prefer_exact_block_overlays(current, blocks)
    assert {item["id"] for item in result["left"]["1"]} == {"block", "outside"}


def test_exclusion_contract_contains_only_unmasked_downstream_text() -> None:
    structured = {
        "used_left": {"left_matched"},
        "used_right": {"right_matched"},
        "remaining": {"left": ["left_open"], "right": ["right_open"]},
    }
    def line(line_id: str, x: float) -> dict:
        return {
            "id": line_id,
            "pdf_page": 1,
            "text": line_id,
            "canonical_text": line_id,
            "bboxes": [{"x": x, "y": 0.2, "width": 0.1, "height": 0.03}],
        }
    pdf_comparison = {
        "excluded_line_ids": {"left": ["same_l"], "right": ["same_r"]},
        "remaining_lines": {
            "left": [line("covered", 0.2), line("left_difference", 0.8)],
            "right": [line("right_difference", 0.8)],
        },
    }
    overlays = {
        "left": {"1": [{
            "id": "mask", "x": 0.1, "y": 0.1, "width": 0.5, "height": 0.5,
            "status": "same_on_linked_sheet", "evidence": "pdf_text_line",
        }]},
        "right": {},
    }
    contract = tc.build_text_exclusion_contract(
        pair_id="pair", source_signature="signature", generated_at="now",
        structured_comparison=structured, pdf_comparison=pdf_comparison,
        overlays=overlays,
    )
    assert contract["policy"]["matched_text_must_not_participate"] is True
    assert contract["excluded_fragment_ids"] == {
        "left": ["left_matched"], "right": ["right_matched"]
    }
    assert [
        item["id"] for item in contract["downstream_text_input"]["left"]
    ] == ["left_difference"]
    assert contract["counts"] == {
        "masks": 1,
        "excluded_fragments": 2,
        "excluded_pdf_lines": 2,
        "downstream_left_lines": 1,
        "downstream_right_lines": 1,
    }
    assert len(contract["contract_sha256"]) == 64
    public = tc.public_exclusion_view(contract, stale=True)
    assert public["stale"] is True
    assert public["valid"] is True
    contract["downstream_text_input"]["left"][0]["text"] = "tampered"
    assert tc.public_exclusion_view(contract)["valid"] is False

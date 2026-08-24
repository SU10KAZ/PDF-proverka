"""G2.1 common vector-evidence and rotation/page-index regressions."""
from __future__ import annotations

import json

import pytest

from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import (
    _result_blocks_vector_index,
)
from backend.app.pipeline.stages.block_grounding.vector_evidence import (
    extract_vector_evidence,
)


fitz = pytest.importorskip("fitz")


def _save_two_page_pdf(path):
    document = fitz.open()
    first = document.new_page(width=240, height=140)
    first.insert_text((30, 45), "FIRST_PAGE", fontsize=10)
    second = document.new_page(width=240, height=140)
    second.insert_text((30, 45), "SECOND_PAGE", fontsize=10)
    document.save(path)
    document.close()


@pytest.mark.parametrize("rotation", [90, 180, 270])
def test_words_and_geometry_share_visual_space_without_double_rotation(
    tmp_path, rotation
):
    pdf = tmp_path / f"rotate-{rotation}.pdf"
    document = fitz.open()
    page = document.new_page(width=240, height=140)
    page.insert_text((30, 45), "ROTATION_TOKEN", fontsize=10)
    page.draw_line((25, 50), (125, 50))
    page.set_rotation(rotation)
    document.save(pdf)
    document.close()

    with fitz.open(pdf) as source:
        page = source[0]
        raw_word = next(
            word for word in page.get_text("words") if word[4] == "ROTATION_TOKEN"
        )
        expected_word = fitz.Rect(raw_word[:4]) * page.rotation_matrix
        expected_word.normalize()
        line_start = fitz.Point(25, 50) * page.rotation_matrix
        line_end = fitz.Point(125, 50) * page.rotation_matrix
        # Prepared polygons are visual-space / page.rect normalized.
        raw_region = fitz.Rect(20, 25, 135, 60) * page.rotation_matrix
        raw_region.normalize()
        width, height = float(page.rect.width), float(page.rect.height)
        polygon = [
            [raw_region.x0 / width, raw_region.y0 / height],
            [raw_region.x1 / width, raw_region.y0 / height],
            [raw_region.x1 / width, raw_region.y1 / height],
            [raw_region.x0 / width, raw_region.y1 / height],
        ]

    evidence = extract_vector_evidence(
        pdf,
        page_index=0,
        block_id="rotated-block",
        polygon_norm=polygon,
    )

    assert evidence.extraction_ok, evidence.reasons
    actual_word = next(
        word for word in evidence.visual_words if word[4] == "ROTATION_TOKEN"
    )
    assert actual_word[:4] == pytest.approx(
        [expected_word.x0, expected_word.y0, expected_word.x1, expected_word.y1]
    )
    expected_line = [line_start.x, line_start.y, line_end.x, line_end.y]
    reverse_line = [line_end.x, line_end.y, line_start.x, line_start.y]
    assert any(
        line == pytest.approx(expected_line) or line == pytest.approx(reverse_line)
        for line in evidence.lines
    )
    assert evidence.coordinate_system == "visual"
    assert evidence.provenance["rotation_applied"] is True
    assert evidence.provenance["rotation_degrees"] == rotation


def test_prepared_page_index_is_authoritative_and_does_not_call_fallback(tmp_path):
    pdf = tmp_path / "pages.pdf"
    _save_two_page_pdf(pdf)

    def forbidden_fallback(_document, _text):
        raise AssertionError("legacy fallback must not run")

    evidence = extract_vector_evidence(
        pdf,
        vector_text="FIRST_PAGE would select the wrong page",
        page_index=1,
        fallback_page_finder=forbidden_fallback,
        include_drawings=False,
    )

    assert evidence.extraction_ok
    assert evidence.page_index == 1
    assert evidence.provenance["page_index_source"] == "prepared_block"
    assert [word[4] for word in evidence.visual_words] == ["SECOND_PAGE"]


def test_missing_page_index_uses_legacy_fallback(tmp_path):
    pdf = tmp_path / "pages.pdf"
    _save_two_page_pdf(pdf)
    calls = []

    def fallback(document, text):
        calls.append((document.page_count, text))
        return 1

    evidence = extract_vector_evidence(
        pdf,
        vector_text="legacy marker",
        fallback_page_finder=fallback,
        include_drawings=False,
    )

    assert calls == [(2, "legacy marker")]
    assert evidence.extraction_ok
    assert evidence.page_index == 1
    assert evidence.provenance["page_index_source"] == "legacy_fallback"


def test_extraction_failure_is_an_object_with_explicit_reason(tmp_path):
    pdf = tmp_path / "empty.pdf"
    document = fitz.open()
    document.new_page(width=100, height=100)
    document.save(pdf)
    document.close()

    evidence = extract_vector_evidence(pdf, page_index=0)

    assert evidence.extraction_ok is False
    assert evidence is not None
    assert evidence.extraction_gate["reason"] == "geometry_unavailable"
    assert evidence.extraction_gate["metrics"]["words_inside_block"] == 0
    assert evidence.extraction_gate["metrics"]["geometry_available"] is False


def test_result_index_prefers_block_page_index_then_page_metadata(tmp_path):
    result = tmp_path / "result.json"
    result.write_text(
        json.dumps(
            {
                "pages": [
                    {
                        "page_index": 7,
                        "blocks": [
                            {"id": "explicit", "page_index": 3, "pdfplumber_text": "x"},
                            {"id": "inherited", "pdfplumber_text": "y"},
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    _result_blocks_vector_index.cache_clear()

    index = _result_blocks_vector_index(str(result), result.stat().st_mtime)

    assert index["explicit"]["page_index"] == 3
    assert index["inherited"]["page_index"] == 7
    assert index["explicit"]["block_id"] == "explicit"

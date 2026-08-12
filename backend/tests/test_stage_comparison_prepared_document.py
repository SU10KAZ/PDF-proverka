from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison.prepared_document import (
    build_prepared_document,
    prepared_document_path,
    write_prepared_diagnostic_report,
    write_prepared_document,
)


def _version(tmp_path: Path, *, blocks: dict | None = None, md: str | None = None) -> Path:
    version = tmp_path / "documents" / "TEST" / "versions" / "v001"
    work = version / "02_work"
    work.mkdir(parents=True)
    (version.parent.parent / "document.json").write_text(
        json.dumps({"document_code": "TEST"}), encoding="utf-8"
    )
    (version / "version.json").write_text(json.dumps({"version_id": "v001"}), encoding="utf-8")
    import fitz
    pdf = fitz.open()
    page = pdf.new_page(width=200, height=100)
    page.insert_text((10, 20), "Native PDF text")
    page.draw_rect((20, 30, 80, 60))
    pdf.save(work / "document.pdf")
    pdf.close()
    if blocks is not None:
        (work / "blocks.json").write_text(json.dumps(blocks), encoding="utf-8")
    if md is not None:
        (work / "document.md").write_text(md, encoding="utf-8")
    return version


def _markdown(blocks: str, *, stamp: bool = True) -> str:
    stamp_line = (
        "**Stamp:** Code: TEST | Stage: P | Object: Object | Organization: Org\n"
        if stamp else ""
    )
    return "# Document: TEST.pdf\n" + stamp_line + "\n## Page 1\n" + blocks


def test_normalizes_coordinates_stamps_and_graphics(tmp_path: Path):
    text_id = "blk_00000000000000000000000000000001"
    image_id = "blk_00000000000000000000000000000002"
    blocks = {
        "pages": [{"page_index": 0, "width_px": 200, "height_px": 100}],
        "blocks": [
            {"block_id": text_id, "page_index": 0, "block_type": "text", "coords_norm": [0.1, 0.2, 0.5, 0.6]},
            {"block_id": image_id, "page_index": 0, "block_type": "image", "coords_norm": [0.5, 0.1, 0.9, 0.8], "crop_url": "/api/crops/a"},
        ],
    }
    md = _markdown(
        f"### BLOCK #1 [TEXT]: {text_id}\n"
        "> **Stamp:** Code: TEST | Stage: P | Sheet: A-01 | Object: Object | Name: Plan | Organization: Org | Revisions: R1\n"
        "Hello block\n\n"
        f"### BLOCK #2 [IMAGE]: {image_id}\n"
        "> **Stamp:** Code: TEST | Stage: P | Sheet: A-01 | Object: Object | Name: Plan | Organization: Org | Revisions: R1\n"
        "**[IMAGE]** | Type: Plan | Axes: 1-2\n"
        "**Summary:** summary\n**Description:** detail\n**Entities:** wall\n**Verification:** verified\n"
    )
    model = build_prepared_document(_version(tmp_path, blocks=blocks, md=md), stage_name="stage_1")

    page = model["pages"][0]
    assert page["sheet_number"] == "A-01"
    assert page["sheet_name"] == "Plan"
    assert page["blocks"][0]["normalized_bbox"] == [0.1, 0.2, 0.5, 0.6]
    assert page["blocks"][0]["bbox_pdf_visual"] == [20.0, 20.0, 100.0, 60.0]
    assert page["blocks"][1]["graphic_description"]["description"] == "detail"
    assert page["blocks"][1]["entities"] == "wall"
    assert page["blocks"][1]["quality"]["coordinates_valid"] is True


def test_missing_optional_artifacts_and_no_stamp_are_reported(tmp_path: Path):
    version = _version(tmp_path)
    model = build_prepared_document(version)
    assert model["document"]["available_sources"] == {
        "source_pdf": True, "markdown": False, "blocks_json": False, "ocr_html": False,
    }
    assert "blocks_json_missing" in model["warnings"]
    assert "document_md_missing" in model["warnings"]
    assert model["pages"][0]["quality"]["stamp_available"] is False


def test_multiple_blocks_and_repeated_build_are_deterministic(tmp_path: Path):
    first_id = "blk_00000000000000000000000000000011"
    second_id = "blk_00000000000000000000000000000012"
    blocks = {"pages": [{"page_index": 0}], "blocks": [
        {"block_id": first_id, "page_index": 0, "block_type": "text", "coords_norm": [0, 0, 1, 0.2]},
        {"block_id": second_id, "page_index": 0, "block_type": "stamp", "coords_norm": [0.7, 0.7, 1, 1]},
    ]}
    md = _markdown(
        f"### BLOCK #1 [TEXT]: {first_id}\nfirst\n\n"
        f"### BLOCK #2 [TEXT]: {second_id}\nsecond\n"
    )
    version = _version(tmp_path, blocks=blocks, md=md)
    first = build_prepared_document(version)
    second = build_prepared_document(version)
    assert first == second
    assert len(first["pages"][0]["blocks"]) == 2
    assert first["pages"][0]["blocks"][1]["semantic_type"] == "stamp"
    path = prepared_document_path(version)
    write_prepared_document(path, first)
    first_payload = path.read_bytes()
    write_prepared_document(path, second)
    assert path.read_bytes() == first_payload


def test_real_uploaded_v2_v3_are_readable_and_have_expected_pages():
    root = Path(__file__).resolve().parents[2]
    comparison = root / "projects_v2/objects/272_Sadovnicheskaya_76_Balchug_Esteyt/comparison"
    v2 = comparison / "stage_1/documents/13АВ-РД-АР0.1-ПА_V2/versions/v001"
    v3 = comparison / "stage_2/documents/13АВ-РД-АР0.1-ПА_V3/versions/v001"
    if not v2.is_dir() or not v3.is_dir():
        pytest.skip("real uploaded comparison fixtures are not present")
    models = [build_prepared_document(v2, stage_name="stage_1"), build_prepared_document(v3, stage_name="stage_2")]
    assert [model["document"]["page_count"] for model in models] == [19, 18]
    assert [model["summary"]["blocks"] for model in models] == [80, 79]
    assert all(model["summary"]["warnings_count"] == 0 for model in models)
    report = write_prepared_diagnostic_report(comparison, models)
    assert "| PDF page | Sheet | Name |" in report.read_text(encoding="utf-8")

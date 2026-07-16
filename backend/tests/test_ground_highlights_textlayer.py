from __future__ import annotations

import json
from pathlib import Path

import fitz
import pytest

from backend.app.pipeline.stages.findings_merge.ground_highlights_textlayer import (
    Anchor,
    Occurrence,
    SHADOW_FILENAME,
    _clusters,
    _find_occurrences,
    backfill_textlayer_highlights,
    extract_anchors,
)


def _write_fixture(project_dir: Path, finding: dict) -> Path:
    pdf_path = project_dir / "drawing.pdf"
    document = fitz.open()
    page = document.new_page(width=200, height=100)
    # Дубль за блоком проверяет, что fail-open штатного clip helper не протечёт
    # в подсветку. Нужное occurrence находится внутри coords_norm.
    page.insert_text((4, 12), "QF12.3", fontsize=8)
    page.insert_text((60, 50), "QF12.3 160 A", fontsize=10)
    document.save(pdf_path)
    document.close()

    result = {
        "pages": [{
            "page_number": 1,
            "width": 200,
            "height": 100,
            "blocks": [{
                "id": "IMG-1",
                "block_type": "image",
                "coords_norm": [0.25, 0.2, 0.9, 0.8],
                "pdfplumber_text": "QF12.3 160 A",
            }],
        }],
    }
    (project_dir / "drawing_result.json").write_text(
        json.dumps(result, ensure_ascii=False), encoding="utf-8",
    )
    output_dir = project_dir / "_output"
    output_dir.mkdir()
    (output_dir / "03_findings.json").write_text(
        json.dumps({"findings": [finding]}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return output_dir


def test_extract_anchors_prioritises_designators_and_keeps_units_weak():
    strong, weak = extract_anchors(
        "СВ-1.6: газобетон 100 мм в осях П.1-П.12",
        "Автомат ВА47-29, QF12.3, номинал 160 А",
    )

    assert [item.text for item in strong] == [
        "СВ-1.6", "П.1", "П.12", "ВА47-29", "QF12.3",
    ]
    assert [item.text for item in weak] == ["100 мм", "160 А"]

    no_strong, only_weak = extract_anchors("Толщина 100 мм", "")
    assert no_strong == []
    assert [item.text for item in only_weak] == ["100 мм"]


def test_extract_anchors_rejects_generic_refs_and_block_id_fragments():
    strong, _ = extract_anchors(
        "В спецификации поз.1, стр.23, изм.1; см. F-002",
        "Блок WQV4-FRLA-DXV, норма п.1.7.137, изделие арт.5106002",
    )

    assert [item.text for item in strong] == ["арт.5106002"]


def test_occurrence_match_joins_adjacent_fitz_words():
    words = [
        (10, 10, 20, 20, "СВ", 1, 2, 0),
        (21, 10, 35, 20, "-1.6", 1, 2, 1),
        (40, 10, 50, 20, "100", 1, 2, 2),
        (51, 10, 60, 20, "мм", 1, 2, 3),
    ]

    strong = _find_occurrences(words, [Anchor("СВ-1.6", "cb-1.6", "strong")])
    weak = _find_occurrences(words, [Anchor("100 мм", "100mm", "weak")])

    assert len(strong) == 1
    assert strong[0].bbox == (10.0, 10.0, 35.0, 20.0)
    assert len(weak) == 1
    assert weak[0].bbox == (40.0, 10.0, 60.0, 20.0)


def test_repeated_anchor_occurrences_stay_in_separate_clusters():
    anchor = Anchor("ВА-103", "ba-103", "strong")
    occurrences = [
        Occurrence(anchor, [(x, 10, x + 20, 20, "ВА-103", i, 0, 0)], (x, 10, x + 20, 20))
        for i, x in enumerate((10, 50, 90, 130))
    ]

    grouped = _clusters(occurrences, crop_w=200, crop_h=100)

    assert len(grouped) == 4
    assert all(len(cluster) == 1 for cluster in grouped)


def test_live_grounding_writes_crop_normalized_region(tmp_path: Path):
    finding = {
        "id": "F-001",
        "problem": "Для QF12.3 указан номинал 160 А",
        "description": "Проверить QF12.3 и 160 А",
        "norm_quote": "СП 999.999: посторонний текст нормы",
        "source_block_ids": ["IMG-1"],
        "related_block_ids": [],
        "highlight_regions": [],
    }
    output_dir = _write_fixture(tmp_path, finding)

    result = backfill_textlayer_highlights(
        tmp_path, output_dir=output_dir, enabled=True, shadow=False,
    )

    assert result["checked"] == 1
    assert result["grounded"] == 1
    assert result["fixed"] == 1
    saved = json.loads((output_dir / "03_findings.json").read_text(encoding="utf-8"))
    regions = saved["findings"][0]["highlight_regions"]
    assert len(regions) == 1
    region = regions[0]
    assert region["block_id"] == "IMG-1"
    assert region["label"] == "QF12.3; 160 А"
    assert all(0.0 <= region[key] <= 1.0 for key in ("x", "y", "w", "h"))
    # Внешний дубль x≈4 не должен попасть в union: crop начинается в x=50.
    assert region["x"] < 0.15
    assert region["w"] < 0.7


def test_shadow_preserves_findings_and_reports_agreement(tmp_path: Path):
    finding = {
        "id": "F-001",
        "problem": "Для QF12.3 указан номинал 160 А",
        "description": "",
        "source_block_ids": ["IMG-1"],
        "highlight_regions": [],
    }
    output_dir = _write_fixture(tmp_path, finding)
    backfill_textlayer_highlights(
        tmp_path, output_dir=output_dir, enabled=True, shadow=False,
    )
    live_payload = json.loads((output_dir / "03_findings.json").read_text(encoding="utf-8"))
    before = (output_dir / "03_findings.json").read_text(encoding="utf-8")

    result = backfill_textlayer_highlights(
        tmp_path, output_dir=output_dir, enabled=True, shadow=True,
    )

    assert result["fixed"] == 0
    assert result["agreement_iou_mean"] == pytest.approx(1.0)
    assert (output_dir / "03_findings.json").read_text(encoding="utf-8") == before
    report = json.loads((output_dir / SHADOW_FILENAME).read_text(encoding="utf-8"))
    assert report["mode"] == "shadow"
    assert report["summary"]["llm_tokens"] == 0
    assert report["summary"]["agreement_iou_mean"] == pytest.approx(1.0)
    assert report["records"][0]["computed_highlight_regions"] == (
        live_payload["findings"][0]["highlight_regions"]
    )


def test_no_strong_anchor_is_conservative_noop(tmp_path: Path):
    finding = {
        "id": "F-001",
        "problem": "Толщина газобетона 100 мм",
        "description": "",
        "source_block_ids": ["IMG-1"],
        "highlight_regions": [],
    }
    output_dir = _write_fixture(tmp_path, finding)

    result = backfill_textlayer_highlights(
        tmp_path, output_dir=output_dir, enabled=True, shadow=False,
    )

    assert result["grounded"] == 0
    assert result["fixed"] == 0
    saved = json.loads((output_dir / "03_findings.json").read_text(encoding="utf-8"))
    assert saved["findings"][0]["highlight_regions"] == []
    report = json.loads((output_dir / SHADOW_FILENAME).read_text(encoding="utf-8"))
    assert report["records"][0]["status"] == "no_strong_anchor"


def test_live_mode_keeps_existing_regions_without_override(tmp_path: Path):
    existing = [{
        "block_id": "IMG-1", "x": 0.7, "y": 0.7,
        "w": 0.1, "h": 0.1, "label": "LLM",
    }]
    finding = {
        "id": "F-001",
        "problem": "QF12.3, 160 А",
        "description": "",
        "source_block_ids": ["IMG-1"],
        "highlight_regions": existing,
    }
    output_dir = _write_fixture(tmp_path, finding)

    result = backfill_textlayer_highlights(
        tmp_path, output_dir=output_dir, enabled=True, shadow=False,
        override_existing=False,
    )

    assert result["grounded"] == 1
    assert result["fixed"] == 0
    saved = json.loads((output_dir / "03_findings.json").read_text(encoding="utf-8"))
    assert saved["findings"][0]["highlight_regions"] == existing

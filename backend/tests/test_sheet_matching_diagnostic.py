from __future__ import annotations

import json
from pathlib import Path

from backend.app.services.stage_comparison.sheet_matching_diagnostic import (
    build_sheet_matching_diagnostic,
    write_sheet_matching_report,
)


def _page(number: int, sheet: str, name: str, text: str) -> dict:
    return {
        "pdf_page": number,
        "sheet_number": sheet,
        "sheet_name": name,
        "stamp": {"stage": "P", "object": "Object", "organization": "Org"},
        "rotation": 0,
        "source_metrics": {"pdf_text_characters": len(text), "pdf_words": len(text.split()), "drawing_objects": 4, "image_placements": 0, "image_area_ratio_sum_capped": 0},
        "text": {"from_blocks": text},
        "blocks": [{"type": "text", "normalized_bbox": [0.1, 0.1, 0.9, 0.3]}],
    }


def test_candidate_scores_are_deterministic_and_do_not_apply_a_map(tmp_path: Path):
    left = {"document": {"code": "AR"}, "pages": [_page(1, "A-1", "Plan A", "unique alpha plan"), _page(2, "A-2", "Plan B", "unique beta plan")]}
    right = {"document": {"code": "AR"}, "pages": [_page(4, "A-2", "Plan B", "unique beta plan"), _page(5, "A-1", "Plan A", "unique alpha plan")]}
    first = build_sheet_matching_diagnostic(left, right)
    second = build_sheet_matching_diagnostic(left, right)
    assert first == second
    assert first["settings"]["llm_used"] is False
    assert first["settings"]["page_map_changed"] is False
    assert first["stage_1_to_stage_2"][0]["top_candidates"][0]["candidate_page"] == 5
    assert first["stage_1_to_stage_2"][1]["top_candidates"][0]["candidate_page"] == 4
    json_path, md_path = write_sheet_matching_report(tmp_path, first)
    assert json.loads(json_path.read_text(encoding="utf-8"))["kind"] == "stage_comparison_sheet_matching_diagnostic"
    assert "Margin to #2" in md_path.read_text(encoding="utf-8")


def test_real_v2_v3_diagnostic_has_all_candidates_and_expected_sheet_conflict():
    root = Path(__file__).resolve().parents[2]
    path = root / "projects_v2/objects/272_Sadovnicheskaya_76_Balchug_Esteyt/comparison/diagnostics/sheet_matching_diagnostic.json"
    if not path.is_file():
        return
    report = json.loads(path.read_text(encoding="utf-8"))
    assert len(report["stage_1_to_stage_2"]) == 19
    assert all(len(row["all_candidates"]) == 18 for row in report["stage_1_to_stage_2"])
    assert report["special_cases"]["top1_consistency"]["top1_collisions"] == [{"stage_1_claimants": [5, 6], "stage_2_page": 5}]

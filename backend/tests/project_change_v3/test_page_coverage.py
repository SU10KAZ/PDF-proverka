from __future__ import annotations

import pytest

from backend.app.services.project_change_v3.coverage import build_page_coverage, validate_page_coverage


def _page(side: str, number: int, *, blocks: bool = True):
    return {
        "side": side,
        "physical_page": number,
        "blocks": ([{"block_id": f"{side}-{number}", "modality": "TEXT"}] if blocks else []),
    }


def test_mapping_coverage_is_not_analysis_coverage():
    structure = [_page("OLD", 1), _page("OLD", 2), _page("NEW", 1), _page("NEW", 2)]
    mapping = {
        "pair": "pair-x",
        "regions": [{"region_id": "A-R001", "old_pages": [1], "new_pages": [1]}],
        "unmatched_old": [2],
        "unmatched_new": [2],
    }
    initial = build_page_coverage(
        pair_id="pair-x", run_id="run-x", structure=structure, semantic_map=mapping,
    )
    assert initial["summary"]["pages_analysed"] == 0
    assert initial["summary"]["pages_pending"] == 4
    assert initial["summary"]["content_unmatched"] == 2
    assert initial["summary"]["complete"] is False
    assert {row["work_item_id"] for row in initial["unmatched_work_items"]} == {
        "UNMATCHED-OLD-P0002", "UNMATCHED-NEW-P0002",
    }

    completed_region = build_page_coverage(
        pair_id="pair-x", run_id="run-x", structure=structure, semantic_map=mapping,
        analysed_region_ids=["A-R001"],
    )
    assert completed_region["summary"]["pages_analysed"] == 2
    assert completed_region["summary"]["pages_pending"] == 2
    validate_page_coverage(completed_region)

    reviewed = build_page_coverage(
        pair_id="pair-x", run_id="run-x", structure=structure, semantic_map=mapping,
        analysed_region_ids=["A-R001"], reviewed_unmatched_pages=[("OLD", 2), ("NEW", 2)],
    )
    assert reviewed["summary"]["complete"] is True
    assert reviewed["summary"]["content_unmatched"] == 2
    assert reviewed["summary"]["content_unmatched_pending"] == 0
    assert {row["status"] for row in reviewed["unmatched_work_items"]} == {"ANALYZED"}


def test_blank_unmatched_page_is_excluded_with_a_reason():
    structure = [_page("OLD", 1, blocks=False), _page("NEW", 1)]
    mapping = {"pair": "p", "regions": [], "unmatched_old": [1], "unmatched_new": [1]}
    records = {
        ("OLD", 1): {"native_page_text": ""},
        ("NEW", 1): {"native_page_text": "content"},
    }
    coverage = build_page_coverage(
        pair_id="p", run_id="r", structure=structure, semantic_map=mapping, page_records=records,
    )
    old = next(page for page in coverage["pages"] if page["side"] == "OLD")
    assert old["analysis_status"] == "EXCLUDED_WITH_REASON"
    assert old["exclusion_reason"] == "NO_EXTRACTABLE_CONTENT"
    assert coverage["summary"]["content_unmatched"] == 1


def test_rejects_page_that_is_both_mapped_and_unmatched():
    with pytest.raises(ValueError, match="not exclusive"):
        build_page_coverage(
            pair_id="p", run_id="r", structure=[_page("OLD", 1), _page("NEW", 1)],
            semantic_map={
                "pair": "p",
                "regions": [{"region_id": "A-R001", "old_pages": [1], "new_pages": [1]}],
                "unmatched_old": [1], "unmatched_new": [],
            },
        )

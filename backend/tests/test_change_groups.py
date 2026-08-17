from __future__ import annotations

import copy

from backend.app.services.stage_comparison.change_groups import build_change_groups


def _region(number, bbox, *, block="plan", role="drawing", kinds=("vector",)):
    return {
        "region_id": f"region_{number:03d}", "bbox": list(bbox),
        "left_block_ids": [block] if block else [], "right_block_ids": [],
        "region_role": role, "change_types": list(kinds),
    }


def _groups(regions, **kwargs):
    return build_change_groups(regions, page_width=1000, page_height=1000, block_types={"plan": "image", "plan_2": "image", "stamp": "stamp"}, **kwargs)


def test_close_atomic_regions_in_same_block_form_one_group():
    result = _groups([_region(1, [10, 10, 30, 30]), _region(2, [38, 12, 58, 32])])
    assert len(result) == 1
    assert result[0]["atomic_region_ids"] == ["region_001", "region_002"]
    assert any(reason == "same_block" for reason in result[0]["grouping_reasons"])


def test_two_remote_clusters_in_same_block_stay_separate():
    result = _groups([_region(1, [10, 10, 30, 30]), _region(2, [35, 10, 55, 30]), _region(3, [600, 600, 620, 620])])
    assert len(result) == 2


def test_supporting_line_never_stretches_group_bbox():
    regions = [_region(1, [10, 10, 30, 30]), _region(2, [42, 10, 62, 30])]
    support = [{"evidence_id": "support_line", "bbox": [0, 15, 80, 16]}]
    result = _groups(regions, supporting_evidence=support)
    assert len(result) == 1
    assert result[0]["bbox"] == [10.0, 10.0, 62.0, 30.0]
    assert any(reason.startswith("shared_vector_support") for reason in result[0]["grouping_reasons"])


def test_different_prepared_blocks_are_never_merged():
    result = _groups([_region(1, [10, 10, 30, 30], block="plan"), _region(2, [31, 10, 50, 30], block="plan_2")])
    assert len(result) == 2


def test_stamp_is_separate_from_drawing_and_stamp_items_can_group():
    regions = [_region(1, [10, 10, 30, 30]), _region(2, [31, 10, 50, 30], block="stamp", role="stamp"), _region(3, [51, 10, 70, 30], block="stamp", role="stamp")]
    result = _groups(regions)
    assert len(result) == 2
    stamp = next(group for group in result if group["region_role"] == "stamp")
    assert stamp["metrics"]["atomic_region_count"] == 2


def test_big_empty_space_blocks_false_merge_even_when_distance_is_small():
    # Diagonal islands: Euclidean gap is within threshold, but fill is < 2.5%.
    result = _groups([_region(1, [10, 10, 11, 11]), _region(2, [35, 35, 36, 36])])
    assert len(result) == 2


def test_one_atomic_region_becomes_one_group():
    result = _groups([_region(1, [10, 10, 30, 30])])
    assert len(result) == 1
    assert result[0]["grouping_reasons"] == ["single_atomic_region"]
    assert result[0]["metrics"]["fill_ratio"] == 1.0


def test_grouping_does_not_mutate_atomic_regions():
    regions = [_region(1, [10, 10, 30, 30]), _region(2, [38, 12, 58, 32])]
    before = copy.deepcopy(regions)
    _groups(regions)
    assert regions == before


def test_grouping_is_deterministic():
    regions = [_region(3, [600, 600, 620, 620]), _region(1, [10, 10, 30, 30]), _region(2, [38, 12, 58, 32])]
    assert _groups(regions) == _groups(regions)

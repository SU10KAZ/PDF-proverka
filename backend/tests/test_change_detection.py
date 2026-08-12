from __future__ import annotations

import copy

from backend.app.services.stage_comparison import change_detection


def _alignment():
    return {
        "items": [
            {"left_page": 1, "right_page": 1, "status": "aligned", "transform": {"matrix": [[1, 0, 0], [0, 1, 0], [0, 0, 1]]}, "quality": {}, "diagnostics": {}},
            {"left_page": 2, "right_page": 2, "status": "weak_alignment", "reason": "few anchors"},
            {"left_page": 3, "right_page": 3, "status": "failed", "reason": "conflict"},
            {"left_page": 4, "right_page": 4, "status": "aligned", "transform": {"matrix": [[1, 0, 0], [0, 1, 0], [0, 0, 1]]}, "quality": {}, "diagnostics": {}},
        ]
    }


def _install_stubs(monkeypatch):
    def analyze(_left_pdf, _right_pdf, _left, _right, alignment, **_kwargs):
        page = alignment["left_page"]
        return {
            "left_page": page, "right_page": alignment["right_page"],
            "raw_differences": [{"kind": "text", "change": "changed", "bbox": [1, 1, 2, 2]}],
            "raw_vector_differences": [{"kind": "vector", "change": "added", "bbox": [2, 2, 3, 3]}],
            "canonical_vector_differences": [{"kind": "vector", "change": "added", "bbox": [2, 2, 3, 3]}],
            "_images": object(),
        }
    def rebuild(cleanup, _left, _right):
        return {"items": [{"left_page": item["left_page"], "right_page": item["right_page"], "regions": [{"region_id": "region_001", "bbox": [1, 1, 3, 3]}], "supporting_vector_evidence": []} for item in cleanup["items"]]}
    def group(atomic, _left, _right):
        return {"items": [{
            "left_page": item["left_page"], "right_page": item["right_page"], "atomic_regions": item["regions"],
            "change_groups": [{"group_id": "group_001", "region_role": "drawing", "atomic_region_ids": ["region_001"], "metrics": {"atomic_region_count": 1, "page_area_ratio": .01, "fill_ratio": 1.0}}],
        } for item in atomic["items"]]}
    monkeypatch.setattr(change_detection.change_regions, "analyze_pair", analyze)
    monkeypatch.setattr(change_detection.change_regions, "rebuild_regions_after_canonical", rebuild)
    monkeypatch.setattr(change_detection.change_groups, "evaluate_change_groups", group)


def test_only_aligned_are_selected_and_weak_failed_require_fallback():
    aligned, fallback = change_detection.select_alignment_pairs(_alignment())
    assert [(item["left_page"], item["right_page"]) for item in aligned] == [(1, 1), (4, 4)]
    assert [item["status"] for item in fallback] == ["weak_alignment", "failed"]


def test_mass_orchestration_includes_all_aligned_pairs(monkeypatch):
    _install_stubs(monkeypatch)
    result = change_detection.run_change_detection("left.pdf", "right.pdf", {}, {}, _alignment())
    assert result["summary"]["aligned_pairs"] == 2
    assert [(item["left_page"], item["right_page"]) for item in result["items"]] == [(1, 1), (4, 4)]
    assert len(result["requires_alignment_fallback"]) == 2


def test_existing_pilot_and_new_aligned_results_share_same_schema(monkeypatch):
    _install_stubs(monkeypatch)
    result = change_detection.run_change_detection("left.pdf", "right.pdf", {}, {}, _alignment())
    assert all("atomic_regions" in item and "change_groups" in item and "evidence" in item for item in result["items"])


def test_repeat_run_is_deterministic_and_does_not_mutate_sources(monkeypatch):
    _install_stubs(monkeypatch)
    alignment = _alignment(); before = copy.deepcopy(alignment)
    first = change_detection.run_change_detection("left.pdf", "right.pdf", {}, {}, alignment)
    second = change_detection.run_change_detection("left.pdf", "right.pdf", {}, {}, alignment)
    assert first == second
    assert alignment == before


def test_diagnostic_threshold_marks_but_does_not_remove_results(monkeypatch):
    _install_stubs(monkeypatch)
    config = change_detection.ReviewConfig(atomic_regions_soft_limit=0)
    result = change_detection.run_change_detection("left.pdf", "right.pdf", {}, {}, _alignment(), review_config=config)
    assert all(item["status"] == "review_required" for item in result["items"])
    assert all(item["atomic_regions"] and item["change_groups"] for item in result["items"])


def test_orchestrator_does_not_create_findings(tmp_path, monkeypatch):
    _install_stubs(monkeypatch)
    sentinel = tmp_path / "source.json"; sentinel.write_text("unchanged", encoding="utf-8")
    change_detection.run_change_detection("left.pdf", "right.pdf", {}, {}, _alignment())
    assert sentinel.read_text(encoding="utf-8") == "unchanged"
    assert not (tmp_path / "findings.json").exists()

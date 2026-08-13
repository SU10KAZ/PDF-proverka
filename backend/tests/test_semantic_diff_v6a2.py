from __future__ import annotations

import copy
import json

from backend.app.services.stage_comparison import semantic_diff_v6a2 as semantic


def _analysis(level="strong", change_types=None, **updates):
    value = {
        "before": "A", "after": "B", "change_summary": "Локальная правка.",
        "change_kind": "changed", "source": "deterministic_v6a1",
        "evidence_level": level, "confidence": .88 if level == "strong" else .35,
        "requires_human_review": level not in {"exact", "strong"},
        "table_changes": [], "inserted_table_rows": [], "removed_table_rows": [],
        "numeric_context_changes": [], "localized_entities_left": [],
        "localized_entities_right": [], "entity_location_uncertain": 0,
        "stamp_field_changes": [],
        "table_model": {"left": {}, "right": {}, "comparison": {"applicable": False}},
        "entity_localization": {"left": [], "right": []},
        "number_contexts": {"left": [], "right": []},
        "evidence": [{"kind": "text", "left_value": "A", "right_value": "B"}],
        "context": {"left": {"blocks": []}, "right": {"blocks": []}},
    }
    value.update(updates)
    return value


def _pair(group_count=2, *, status="ok"):
    return {
        "left_page": 7, "right_page": 6, "status": status,
        "review_reasons": ["many_change_groups"] if status == "review_required" else [],
        "atomic_regions": [], "change_groups": [
            {"group_id": f"group_{index:03d}", "bbox": [0, 0, 10, 10],
             "change_types": ["text"], "region_role": "drawing",
             "atomic_region_ids": [], "block_ids": []}
            for index in range(1, group_count + 1)
        ],
    }


def test_orchestration_processes_every_group_once(tmp_path):
    counts = [19, 11, 7, 3, 19, 10, 3, 3, 8, 10]
    detection = {"items": [
        {**_pair(count), "left_page": index + 7, "right_page": index + 6}
        for index, count in enumerate(counts)
    ]}
    calls = []

    def analyzer(pair, group):
        calls.append((pair["left_page"], group["group_id"]))
        return _analysis()

    items = semantic.analyze_all_groups(detection, analyzer, tmp_path)
    assert len(items) == 93
    assert len(calls) == len(set(calls)) == 93


def test_exact_and_strong_are_resolved_contextual_and_insufficient_are_not(tmp_path):
    pair, group = _pair(1), _pair(1)["change_groups"][0]
    for level in ("exact", "strong"):
        assert semantic.build_output_item(pair, group, _analysis(level), tmp_path)["resolution_status"] == "deterministically_resolved"
    for level in ("contextual", "insufficient"):
        assert semantic.build_output_item(pair, group, _analysis(level), tmp_path)["resolution_status"] == "requires_additional_analysis"


def test_vector_only_keeps_geometry_and_does_not_invent_semantics(tmp_path):
    pair, group = _pair(1), _pair(1)["change_groups"][0]
    group["change_types"] = ["vector"]
    analysis = _analysis(
        "insufficient", before="Геометрия согласно V2", after="Геометрия изменена в V3",
        change_kind="geometric_change", evidence=[{"kind": "vector", "bbox": [1, 2, 3, 4]}],
    )
    item = semantic.build_output_item(pair, group, analysis, tmp_path)
    assert item["semantic_type"] == "vector"
    assert item["unresolved_reason"] == "vector_only"
    assert item["next_analysis"] == "vision"
    assert item["geometry_evidence"] == analysis["evidence"]
    assert item["change_kind"] == "geometric_change"


def test_stamp_is_a_separate_type_and_retains_region_role(tmp_path):
    pair, group = _pair(1), _pair(1)["change_groups"][0]
    group["region_role"] = "stamp"
    item = semantic.build_output_item(pair, group, _analysis("exact"), tmp_path)
    assert item["semantic_type"] == "stamp"
    assert item["region_role"] == "stamp"


def test_sheet_review_flag_does_not_downgrade_resolved_group(tmp_path):
    pair, group = _pair(1, status="review_required"), _pair(1)["change_groups"][0]
    item = semantic.build_output_item(pair, group, _analysis("exact"), tmp_path)
    assert item["sheet_review_required"] is True
    assert item["resolution_status"] == "deterministically_resolved"
    assert item["requires_human_review"] is False


def test_next_analysis_is_deterministic():
    group = {"change_types": ["text"]}
    analysis = _analysis("contextual")
    first = semantic.classify_unresolved(group, analysis, "text")
    assert first == semantic.classify_unresolved(group, copy.deepcopy(analysis), "text")
    assert first[1] in semantic.NEXT_ANALYSES


def test_pilot_parity_detects_no_change_and_reports_changed_field():
    old = {"left_page": 7, "right_page": 6, "group_id": "group_001", **_analysis()}
    old.pop("context")
    pilot = {"items": [old]}
    same = semantic.compare_with_v6a1_pilot([copy.deepcopy(old)], pilot)
    assert same == {"available": True, "compared": 1, "mismatches": [], "unchanged": True}
    changed = copy.deepcopy(old); changed["after"] = "C"
    assert semantic.compare_with_v6a1_pilot([changed], pilot)["mismatches"][0]["fields"] == ["after"]


def test_rerun_and_report_are_deterministic_and_create_no_findings(tmp_path):
    pair = _pair(2)
    detection = {"items": [pair]}
    analyzer = lambda _pair, _group: _analysis()
    first = semantic.analyze_all_groups(detection, analyzer, tmp_path)
    second = semantic.analyze_all_groups(copy.deepcopy(detection), analyzer, tmp_path)
    assert first == second
    summary = semantic.summarize(first)
    assert summary["type_statistics"]["text"] == {
        "total": 2, "exact": 0, "strong": 2, "contextual": 0,
        "insufficient": 0, "deterministically_resolved": 2,
        "automatic_resolution_rate": 1.0,
    }
    report = {
        "items": first, "summary": summary,
        "pilot_v6a1_parity": {"available": False, "compared": 0, "mismatches": []},
    }
    semantic.write_report(tmp_path, report)
    assert (tmp_path / "semantic_diff.json").exists()
    assert (tmp_path / "semantic_diff.md").exists()
    assert not list(tmp_path.rglob("*finding*"))
    assert "findings" not in json.loads((tmp_path / "semantic_diff.json").read_text())

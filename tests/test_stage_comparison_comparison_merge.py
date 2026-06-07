"""Tests for non-destructive comparison merge (preserve old findings + carry
expert verdicts via id reuse + tag genuinely-new findings)."""
import json

import pytest

from backend.app.services.stage_comparison import comparison_merge as cm


PREV = [
    {"id": "chg_a", "type": "changed", "title": "Сечение кабеля ВРУ-1 изменено",
     "left_page": 12, "old_value": "5x10", "new_value": "5x16"},
    {"id": "chg_b", "type": "removed", "title": "Щит ЩО-7 удалён", "left_page": 24},
]


def test_merge_carried_reuses_prev_id():
    new = [{"id": "chg_x", "type": "changed", "title": "Изменено сечение кабеля для ВРУ-1",
            "left_page": 12, "old_value": "5х10", "new_value": "5х16"}]  # reworded same finding
    merged, stats = cm.merge_changes(PREV, new)
    carried = [m for m in merged if m["change_origin"] == "carried"]
    assert len(carried) == 1
    assert carried[0]["id"] == "chg_a"          # id reused → v2_id stable → verdict preserved
    assert carried[0]["is_new"] is False
    assert carried[0]["title"] == "Изменено сечение кабеля для ВРУ-1"  # keeps fresh content
    assert stats["carried"] == 1


def test_merge_tags_genuinely_new():
    new = [
        {"id": "chg_x", "type": "changed", "title": "Сечение кабеля ВРУ-1 изменено", "left_page": 12},
        {"id": "chg_y", "type": "added", "title": "Добавлено Приложение А ТП", "right_page": 48},
    ]
    merged, stats = cm.merge_changes(PREV, new)
    new_items = [m for m in merged if m["is_new"]]
    assert len(new_items) == 1
    assert new_items[0]["title"].startswith("Добавлено Приложение")
    assert new_items[0]["change_origin"] == "new"
    assert stats["new_tagged"] == 1


def test_merge_keeps_previous_only_findings():
    # new run did NOT reproduce ЩО-7 → must be KEPT so its review is not lost
    new = [{"id": "chg_x", "type": "changed", "title": "Сечение кабеля ВРУ-1 изменено", "left_page": 12}]
    merged, stats = cm.merge_changes(PREV, new)
    prev_only = [m for m in merged if m["change_origin"] == "previous"]
    assert len(prev_only) == 1
    assert prev_only[0]["id"] == "chg_b"        # id preserved → verdict preserved
    assert stats["previous_kept"] == 1


def test_merge_first_comparison_no_baseline():
    new = [{"id": "chg_x", "type": "added", "title": "Что-то", "left_page": 1}]
    merged, stats = cm.merge_changes([], new)
    assert all(m["is_new"] is False for m in merged)   # nothing is "new" without a baseline
    assert stats["new_tagged"] == 0 and stats["previous_count"] == 0


def test_signature_sheet_sensitivity():
    a = {"type": "changed", "title": "Сечение кабеля ВРУ-1", "left_page": 12}
    b = {"type": "changed", "title": "Сечение кабеля ВРУ-1", "left_page": 30}   # other sheet
    assert cm.change_signature(a) != cm.change_signature(b)
    c = {"type": "changed", "title": "сечение  КАБЕЛЯ  вру-1!!", "left_page": 12}  # reword/ws/case
    assert cm.change_signature(a) == cm.change_signature(c)


def test_apply_merge_writes_file_and_metadata(tmp_path, monkeypatch):
    result_path = tmp_path / "comparison_result.json"
    monkeypatch.setattr(cm.paths_mod, "enriched_comparison_result_path",
                        lambda sid, pid: result_path)
    # fresh result on disk (as Opus just wrote it)
    result_path.write_text(json.dumps({
        "status": "done", "strategy": "x",
        "changes": [
            {"id": "chg_x", "type": "changed", "title": "Сечение кабеля ВРУ-1 изменено", "left_page": 12},
            {"id": "chg_y", "type": "added", "title": "Новое приложение", "left_page": 99},
        ],
    }), encoding="utf-8")
    stats = cm.apply_merge("sid", "pid", PREV)
    assert stats["merged"] is True
    out = json.loads(result_path.read_text(encoding="utf-8"))
    assert out["status"] == "done" and out["strategy"] == "x"   # other fields preserved
    assert out["merge"]["enabled"] is True
    origins = {c["id"]: c["change_origin"] for c in out["changes"]}
    assert origins["chg_a"] == "carried"        # reused id for the reworded ВРУ-1 finding
    assert origins["chg_b"] == "previous"       # ЩО-7 kept
    assert any(c["is_new"] for c in out["changes"])  # "Новое приложение" tagged new

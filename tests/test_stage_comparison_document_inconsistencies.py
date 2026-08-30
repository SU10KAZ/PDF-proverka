"""Внутренние противоречия чертежа — не расхождение редакций.

«1QF1 стоит во второй секции» — ошибка самого листа: обозначение относит
аппарат к первой секции, а геометрия шин — ко второй. Вектор-слой доказывает
это сам, без всякого сравнения версий.

Показать такое как «было → стало» нельзя. На другом листе этого аппарата в
таком виде не было вовсе, и любая пара значений оказалась бы выдуманной.
Поэтому у находки нет ни второй стороны, ни «до» и «после», и живёт она в
отдельном списке, куда перечень изменений не заглядывает.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.pipeline.stages.block_grounding.system_graph_comparator import (
    compare_system_graphs,
    document_inconsistencies,
)


ROOT = Path(__file__).resolve().parents[1]
STORE = ROOT / "projects_v2/objects/272_Sadovnicheskaya_76_Balchug_Esteyt/comparison"


def _graph(block_id: str, conflicts: list[dict]) -> dict:
    return {
        "schema_version": "system-graph.v1",
        "block": {"block_id": block_id},
        "nodes": [],
        "edges": [],
        "analysis": {"section_label_conflicts": conflicts},
    }


CONFLICT = {
    "label": "1QF1",
    "bbox": [1728.84, 923.035, 1742.999, 931.824],
    "label_section": "1",
    "geometric_section": "2",
    "reason": "label prefix conflicts with contiguous X partition",
}


def test_conflict_becomes_a_one_sided_finding():
    items = document_inconsistencies(_graph("l", []), _graph("r", [CONFLICT]))
    assert len(items) == 1
    item = items[0]
    assert item["side"] == "RIGHT"
    assert item["kind"] == "SECTION_LABEL_CONFLICT"
    assert item["subject"] == "1QF1"
    assert "секции 1" in item["summary"] and "секции 2" in item["summary"]


def test_finding_carries_no_before_and_after():
    """Пара значений здесь была бы выдумкой — её и нет."""
    item = document_inconsistencies(_graph("l", []), _graph("r", [CONFLICT]))[0]
    assert "before" not in item
    assert "after" not in item
    assert "left_nodes" not in item
    assert "right_nodes" not in item


def test_finding_points_at_a_place_on_the_sheet():
    item = document_inconsistencies(_graph("l", []), _graph("r", [CONFLICT]))[0]
    assert item["block_id"] == "r"
    assert item["evidence"]["bbox"] == CONFLICT["bbox"]
    assert item["evidence"]["reason"] == CONFLICT["reason"]


def test_incomplete_conflict_is_not_published():
    """Без обеих секций утверждать противоречие нечем."""
    assert document_inconsistencies(
        _graph("l", []), _graph("r", [{"label": "1QF1", "label_section": "1"}])
    ) == []


def test_identifier_is_stable_across_runs():
    first = document_inconsistencies(_graph("l", []), _graph("r", [CONFLICT]))
    second = document_inconsistencies(_graph("l", []), _graph("r", [CONFLICT]))
    assert first[0]["inconsistency_id"] == second[0]["inconsistency_id"]


def test_side_is_part_of_the_identifier():
    left_only = document_inconsistencies(_graph("x", [CONFLICT]), _graph("x", []))
    right_only = document_inconsistencies(_graph("x", []), _graph("x", [CONFLICT]))
    assert left_only[0]["inconsistency_id"] != right_only[0]["inconsistency_id"]


def test_inconsistencies_never_enter_the_change_list():
    result = compare_system_graphs(_graph("l", []), _graph("r", [CONFLICT]))
    assert result["document_inconsistencies"]
    summaries = " ".join(item["summary"] for item in result["changes"])
    assert "геометрически стоит в секции" not in summaries


# ── Боевая пара ГРЩ ───────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def grsh_graphs():
    payload = (
        STORE
        / "stage_1/documents/Страница_52_из_АА_БЭ-03-ДС3-ИОС1.1"
    )
    if not payload.is_dir():
        pytest.skip("корпус ГРЩ не установлен")
    run = (
        ROOT
        / "comparison/sessions/7cccec69bb0b4327/pairs/p11c797af90"
        / "production/direct_page_mode2.json"
    )
    if not run.is_file():
        pytest.skip("прогон боевой пары не выполнен")
    data = json.loads(run.read_text(encoding="utf-8"))
    return data["left_graph"], data["right_graph"]


def test_grsh_right_sheet_reports_two_conflicts(grsh_graphs):
    """На правом листе ГРЩ два аппарата с приставкой «1» стоят во второй секции."""
    left, right = grsh_graphs
    items = document_inconsistencies(left, right)
    assert {item["subject"] for item in items} == {"1QF1", "1QF12"}
    assert {item["side"] for item in items} == {"RIGHT"}

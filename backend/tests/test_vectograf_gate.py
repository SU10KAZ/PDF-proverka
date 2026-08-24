"""Гейт качества «Вектографа» (evaluate_vectograf_gate) — юнит-тесты на решение use/fallback.

Данные-пороги подобраны по 15 боевым однолинейкам (см. docs/vectograf.md «Полнота графа»):
жёсткие критерии — построен/линий≥5/физика≥0.8/привязка≥0.85/конфликты≤15%;
coverage/confidence НЕ блокируют (на честных листах гуляют — ГРЩ 50%, К7 0.36).
"""
from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import (
    evaluate_vectograf_gate,
)


def _graph(feeders_total=40, active=38, ambiguous=0, power_rate=1.0,
           geometry_conflicts=0, linked_occ=38, total_occ=40,
           confidence=0.95, status="ok"):
    return {
        "feeders_total": feeders_total,
        "confidence": confidence,
        "status": status,
        "validation": {
            "active": active, "ambiguous": ambiguous, "power_rate": power_rate,
            "geometry_conflicts": geometry_conflicts,
            "codes_linked_occurrences": linked_occ, "codes_total_occurrences": total_occ,
        },
    }


def test_gate_none_graph_rejected():
    g = evaluate_vectograf_gate(None)
    assert g["use"] is False
    assert g["reasons"]


def test_gate_good_graph_passes():
    g = evaluate_vectograf_gate(_graph())
    assert g["use"] is True
    assert g["reasons"] == []
    assert g["extraction_ok"] is True
    assert g["structure_ready"] is True
    assert g["structure"]["metrics"]["enough_devices"] is True
    assert g["structure"]["metrics"]["enough_feeders"] is True
    assert g["structure"]["metrics"]["enough_anchors"] is True


def test_gate_few_feeders_rejected():
    g = evaluate_vectograf_gate(_graph(feeders_total=3, active=3))
    assert g["use"] is False
    assert any("мало линий" in r for r in g["reasons"])
    assert g["extraction_ok"] is True
    assert g["structure_ready"] is False
    assert g["reason"]


def test_extraction_and_structure_gates_are_independent():
    graph = _graph()
    graph["quality_gates"] = {
        "extraction": {
            "extraction_ok": False,
            "reason": "no_words_inside_block",
            "reasons": ["no_words_inside_block"],
            "metrics": {"coordinates_valid": True, "words_inside_block": 0},
        }
    }

    gate = evaluate_vectograf_gate(graph)

    assert gate["use"] is False
    assert gate["extraction_ok"] is False
    assert gate["structure_ready"] is True
    assert gate["reason"] == "извлечение: no_words_inside_block"


def test_gate_bad_physics_rejected():
    g = evaluate_vectograf_gate(_graph(power_rate=0.5))
    assert g["use"] is False
    assert any("физика" in r for r in g["reasons"])


def test_gate_weak_binding_rejected():
    # честная привязка 20/(20+20) = 50% < 85%
    g = evaluate_vectograf_gate(_graph(active=20, ambiguous=20))
    assert g["use"] is False
    assert any("привязка" in r for r in g["reasons"])


def test_gate_many_conflicts_rejected():
    g = evaluate_vectograf_gate(_graph(geometry_conflicts=10))  # 10/40 = 25% > 15%
    assert g["use"] is False
    assert any("конфликтов" in r for r in g["reasons"])


def test_gate_low_coverage_warns_but_passes():
    # ГРЩ-кейс: coverage 50% — предупреждение, НЕ блокер
    g = evaluate_vectograf_gate(_graph(linked_occ=14, total_occ=28, power_rate=0.86,
                                       confidence=0.61, status="needs_review"))
    assert g["use"] is True
    assert any("покрытие" in w for w in g["warnings"])


def test_gate_reserve_only_sheet_passes():
    # К7-кейс: мало активных, много резервов, ambiguous=0 → честная привязка 100%
    g = evaluate_vectograf_gate(_graph(feeders_total=10, active=5, ambiguous=0,
                                       linked_occ=5, total_occ=14, confidence=0.36,
                                       status="needs_review"))
    assert g["use"] is True

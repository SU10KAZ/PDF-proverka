"""Регрессии трёх профилей структурных схем СОВ/СКУД."""
from pathlib import Path

import pytest

from backend.app.pipeline.stages.block_grounding.structural_access_geometry import (
    PROFILE_MULTITOWER,
    PROFILE_SKUD_SITE,
    PROFILE_TOWER_PAIR,
    build_structural_access_graph,
    classify_structural_access_profile,
    evaluate_structural_access_gate,
    render_structural_access_markdown,
)


ROOT = Path(__file__).resolve().parents[1]
SS_DIR = ROOT / "experiments" / "блоки разных дисциплин" / "СС"
PAIR_PDF = SS_DIR / "Структурная схема СОВ — К1-К2 — 6X4P-4EGH-VJ9.pdf"
MULTI_PDF = SS_DIR / "Структурная схема СОВ — К3-К6 — 6F7E-TCVU-KYW.pdf"
SKUD_PDF = SS_DIR / "Аналог — Структурная схема СКУД — 9AJM-YHWM-CV9.pdf"


def test_classifier_separates_three_structural_grammars():
    assert classify_structural_access_profile(
        "1 Корпус 2 Корпус ОСПД1.1 STR1.1.1.1"
    ) == PROFILE_TOWER_PAIR
    assert classify_structural_access_profile(
        "3 Корпус 4 Корпус 5 Корпус 6 Корпус ОСПД3.1 STR3.1.1.1"
    ) == PROFILE_MULTITOWER
    assert classify_structural_access_profile(
        "Жилой дом павильон Пожарный отсек 1 STR1.1"
    ) == PROFILE_SKUD_SITE
    assert classify_structural_access_profile("Схема электрических подключений") is None


@pytest.mark.skipif(not PAIR_PDF.exists(), reason="векторный crop К1–К2 отсутствует")
def test_tower_pair_preserves_empty_floor_bands_and_confirmed_edges():
    graph = build_structural_access_graph(PAIR_PDF, block_id="6X4P-4EGH-VJ9")

    assert graph is not None and graph["profile_id"] == PROFILE_TOWER_PAIR
    validation = graph["validation"]
    assert validation["buildings_total"] == 2
    assert validation["floor_bands_total"] == 14
    assert validation["shown_empty_floor_bands"] == 6
    assert validation["nodes_total"] == 57
    assert validation["nodes_floor_bound"] == 57
    assert validation["room_bind_rate"] == 0.965
    assert validation["networks_total"] == 16
    assert validation["confirmed_edges"] == 14
    assert validation["node_types"] == {
        "ospd_cabinet": 6,
        "access_controller": 6,
        "access_module": 33,
        "concierge_monitor": 2,
        "call_panel": 10,
    }
    assert [item["number"] for item in graph["buildings"]] == [1, 2]
    assert all(item.get("y_range") for item in graph["floor_bands"])
    gate = evaluate_structural_access_gate(graph)
    assert gate["use"] is True and gate["mode"] == "hierarchy_and_confirmed_edges"
    markdown = render_structural_access_markdown(graph)
    assert "shown_empty" in markdown and "К1.1.-1.1 → STR1.1.1.12" in markdown


@pytest.mark.skipif(not MULTI_PDF.exists(), reason="векторный crop К3–К6 отсутствует")
def test_multitower_builds_complete_control_domains():
    graph = build_structural_access_graph(MULTI_PDF, block_id="6F7E-TCVU-KYW")

    assert graph is not None and graph["profile_id"] == PROFILE_MULTITOWER
    validation = graph["validation"]
    assert validation["buildings_total"] == 4
    assert validation["floor_bands_total"] == 16
    assert validation["shown_empty_floor_bands"] == 4
    assert validation["nodes_total"] == 84
    assert validation["nodes_building_bound"] == 84
    assert validation["nodes_floor_bound"] == 84
    assert validation["control_domains_total"] == 6
    assert validation["control_domains_complete"] == 6
    assert validation["networks_total"] == 20
    assert validation["confirmed_edges"] == 15
    assert validation["node_types"]["access_module"] == 46
    assert validation["node_types"]["workstation"] == 1
    assert [item["number"] for item in graph["buildings"]] == [3, 4, 5, 6]
    assert {item["label"] for item in graph["control_zones"]} == {
        "ПО №1", "ПО №7", "ПО №8", "ПО №10", "ПО №11"
    }
    k63 = next(
        item for item in graph["control_domains"]
        if next(node for node in graph["nodes"] if node["id"] == item["controller_id"])["label"]
        == "К6.3.29.2"
    )
    assert len(k63["module_ids"]) == 3 and k63["domain_state"] == "present"
    assert evaluate_structural_access_gate(graph)["use"] is True


@pytest.mark.skipif(not SKUD_PDF.exists(), reason="векторный crop СКУД отсутствует")
def test_skud_site_keeps_fire_compartments_as_overlays():
    graph = build_structural_access_graph(SKUD_PDF, block_id="9AJM-YHWM-CV9")

    assert graph is not None and graph["profile_id"] == PROFILE_SKUD_SITE
    validation = graph["validation"]
    assert validation["structures_total"] == 4
    assert validation["floor_bands_total"] == 9
    assert validation["locations_total"] == 29
    assert validation["access_points_total"] == 21
    assert validation["access_devices_total"] == 116
    assert validation["access_devices_location_bound"] == 109
    assert validation["access_device_bind_rate"] == 0.94
    assert validation["infrastructure_nodes_total"] == 14
    assert validation["fire_compartments_total"] == 4
    assert len(graph["overlays"]) == 4
    compartment_2 = next(item for item in graph["overlays"] if item["id"] == "fire-compartment-2")
    assert len(compartment_2["anchor_bboxes_page"]) == 2
    gate = evaluate_structural_access_gate(graph)
    assert gate["use"] is True and gate["mode"] == "site_inventory"
    assert gate["metrics"]["topology_state"] == "visual_unverified"
    markdown = render_structural_access_markdown(graph)
    assert "Жилой дом" in markdown and "Точки доступа" in markdown

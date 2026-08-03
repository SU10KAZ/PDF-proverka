"""Регрессии детерминированного профиля СС."""
import json
from pathlib import Path

import pytest

from backend.app.pipeline.stages.block_grounding.low_voltage_geometry import (
    build_low_voltage_graph,
    classify_low_voltage_subtype,
    evaluate_low_voltage_gate,
    normalize_low_voltage_graph,
    profile_id_for_subtype,
    render_low_voltage_graph_markdown,
)


ROOT = Path(__file__).resolve().parents[1]
SS_DIR = ROOT / "experiments" / "блоки разных дисциплин" / "СС"
APS_PDF = SS_DIR / "02_13АВ-РД-АПЗ.АПС-К3_V1__6W3K-9C4Y-VPY.pdf"
TRAY_PDF = SS_DIR / "06_13АВ-РД-КК-К2__4KGV-FMLW-7V9.pdf"
WIRING_PDF = SS_DIR / "01_13АВ-РД-СОВ-К3-6__79PP-NE7C-VDH.pdf"
K5_RESULT = (ROOT / "projects" / "214. Alia (ASTERUS)" / "SS" / "13АВ-РД-АПЗ.АПС-К5"
             / "13АВ-РД-АПЗ.АПС-К5 (согл от 02.04.2026)_result.json")
K4_RESULT = (ROOT / "projects" / "214. Alia (ASTERUS)" / "SS"
             / "13АВ-РД-АПЗ.АПС-К4(main)" / "13АВ-РД-АПЗ.АПС-К4 V2"
             / "13АВ-РД-АПЗ.АПС-К4 V2_result.json")


def _text(pdf: Path) -> str:
    fitz = pytest.importorskip("fitz")
    with fitz.open(pdf) as doc:
        return "\n".join(page.get_text() for page in doc)


def _project_block(result_path: Path, block_id: str):
    data = json.loads(result_path.read_text(encoding="utf-8"))
    block = next(block for page in data["pages"] for block in page["blocks"]
                 if block.get("id") == block_id)
    pdf = next(result_path.parent.glob("*.pdf"))
    graph = build_low_voltage_graph(
        pdf, block["pdfplumber_text"], bbox_norm=block.get("coords_norm"),
        polygon_norm=block.get("polygon_points_norm"),
    )
    return block, graph


def test_subtype_classifier_separates_three_geometric_grammars():
    aps = "АЛС9.2\n27 этаж\n28 этаж\n" + " ".join(f"9A2.{n}" for n in range(1, 12))
    fragment = "АЛС1.1 " + " ".join(f"1BTH1.{n}({n})" for n in range(1, 12))
    tray = """Лестничный лоток СБ 300х100, L=1,52 м.
Листовой лоток СПЗ с перегородкой 200х100, L=4,88 м.
Гильзы в перекрытии 28 шт. ∅50, L=550 мм."""
    wiring = "Схема электрических подключений READER RS-485 DOOR EXIT SENS КСПВПнг(А)-HF 1х2х0,97"

    assert classify_low_voltage_subtype(aps) == "aps_structural"
    assert classify_low_voltage_subtype(fragment) == "aps_fragment"
    assert classify_low_voltage_subtype(tray) == "tray_axonometry"
    assert classify_low_voltage_subtype(wiring) == "terminal_wiring"
    assert classify_low_voltage_subtype("обычное примечание") is None


def test_low_voltage_subtypes_have_separate_ctx_profiles():
    assert profile_id_for_subtype("aps_structural") == "fire_alarm_loop_topology"
    assert profile_id_for_subtype("aps_fragment") == "fire_alarm_loop_topology"
    assert profile_id_for_subtype("tray_axonometry") == "cable_tray_axonometry"
    assert profile_id_for_subtype("terminal_wiring") == "low_voltage_terminal_wiring"


@pytest.mark.skipif(not APS_PDF.exists(), reason="локальный PDF-корпус СС отсутствует")
def test_real_aps_builds_complete_hierarchy():
    graph = build_low_voltage_graph(APS_PDF, _text(APS_PDF))

    assert graph is not None and graph["subtype"] == "aps_structural"
    assert graph["root"] == "ПО №7"
    assert graph["validation"]["address_points_total"] == 240
    assert graph["validation"]["address_labels_unique"] == 239
    assert graph["validation"]["address_slots_total"] == 305
    assert graph["validation"]["devices_type_bound"] == 240
    assert graph["validation"]["devices_floor_bound"] == 240
    assert graph["validation"]["floors_total"] == 6
    assert graph["validation"]["duplicate_address_labels"] == ["9A2.181"]
    assert [loop["id"] for loop in graph["loops"]] == ["АЛС9.1", "АЛС9.2", "АЛС10.1"]
    als92 = next(loop for loop in graph["loops"] if loop["id"] == "АЛС9.2")
    assert als92["address_slots"] == 195
    assert als92["scope_gaps"] == []
    assert {row["floor"] for row in als92["floors"]} == {27, 28, 29, 30}
    gate = evaluate_low_voltage_gate(graph)
    assert gate["use"] is True and gate["mode"] == "hierarchy"
    markdown = render_low_voltage_graph_markdown(graph)
    assert "ПО №7 → **АЛС9.2**" in markdown
    assert "9A2.181" in markdown

    normalize_low_voltage_graph(graph)
    assert graph["profile_id"] == "fire_alarm_loop_topology"
    assert len(graph["nodes"]) == (
        1 + len(graph["loops"]) + len(graph["floors"]) + len(graph["devices"])
    )
    assert len(graph["networks"]) == len(graph["loops"])
    assert graph["validation"]["nodes_total"] == len(graph["nodes"])
    assert all(edge.get("from") and edge.get("to") for edge in graph["edges"])


@pytest.mark.skipif(not TRAY_PDF.exists(), reason="локальный PDF-корпус СС отсутствует")
def test_real_tray_builds_exact_inventory_without_fake_topology():
    graph = build_low_voltage_graph(TRAY_PDF, _text(TRAY_PDF))

    assert graph is not None and graph["subtype"] == "tray_axonometry"
    assert graph["validation"]["elements_total"] == 10
    assert graph["validation"]["trays_total"] == 8
    assert graph["validation"]["sleeve_callouts_total"] == 2
    assert graph["connections"] == []
    assert graph["validation"]["topology_state"] == "visual_unverified"
    assert graph["validation"]["callouts_total"] == 10
    assert graph["validation"]["leader_targets_total"] == 16
    assert graph["validation"]["leader_targets_linked"] == 16
    assert graph["validation"]["callout_link_rate"] == 1.0
    gate = evaluate_low_voltage_gate(graph)
    assert gate["use"] is True and gate["mode"] == "inventory_only"

    normalize_low_voltage_graph(graph)
    assert graph["profile_id"] == "cable_tray_axonometry"
    assert len(graph["nodes"]) == len(graph["elements"]) == 10
    assert graph["edges"] == []
    assert graph["readiness"]["complete"] is False


@pytest.mark.skipif(not WIRING_PDF.exists(), reason="локальный PDF-корпус СС отсутствует")
def test_real_terminal_wiring_builds_only_confirmed_cross_component_paths():
    graph = build_low_voltage_graph(WIRING_PDF, _text(WIRING_PDF))

    assert graph is not None and graph["subtype"] == "terminal_wiring"
    assert graph["validation"]["components_total"] == 14
    assert graph["validation"]["cable_types_total"] == 4
    assert graph["status"] == "partial_topology"
    assert graph["validation"]["terminal_half_shapes"] == 222
    assert graph["validation"]["terminal_anchors"] == 111
    assert graph["validation"]["wire_segments_raw"] == 1057
    assert graph["validation"]["wire_segments"] == 577
    assert graph["validation"]["cross_component_networks"] == 5
    assert graph["validation"]["confirmed_pair_connections"] == 3
    assert graph["validation"]["multi_terminal_networks"] == 2
    assert graph["validation"]["cross_endpoint_parent_rate"] == 1.0
    assert graph["validation"]["cross_endpoint_label_rate"] == 1.0
    assert len(graph["confirmed_connections"]) == 3
    assert all(item["topology_state"] == "confirmed_pair"
               for item in graph["confirmed_connections"])
    assert all(len(item["endpoints"]) == 2 for item in graph["confirmed_connections"])
    gate = evaluate_low_voltage_gate(graph)
    assert gate["use"] is True and gate["mode"] == "confirmed_connections_only"
    assert gate["metrics"]["topology_complete"] is False
    markdown = render_low_voltage_graph_markdown(graph)
    assert "Подтверждённых пар: 3" in markdown
    assert "многоклеммная сеть требует проверки" in markdown


@pytest.mark.skipif(not K5_RESULT.exists(), reason="локальный проектный корпус АПС отсутствует")
def test_project_structural_block_supports_parenthetical_logical_addresses_and_bbox():
    _block, graph = _project_block(K5_RESULT, "7JNX-WC63-EL3")

    assert graph is not None and graph["subtype"] == "aps_structural"
    assert graph["validation"]["address_points_total"] == 317
    assert graph["validation"]["devices_type_bound"] == 316
    assert graph["validation"]["devices_floor_bound"] == 317
    device = next(item for item in graph["devices"] if item["address"] == "1BTH1.11(8)")
    assert device["tag_start"] == 11
    assert device["logical_address"] == 8
    assert device["address_slots"] == [8]
    assert evaluate_low_voltage_gate(graph)["use"] is True


@pytest.mark.skipif(not K4_RESULT.exists(), reason="локальный проектный корпус АПС отсутствует")
def test_project_riser_node_is_fragment_not_fake_floor_hierarchy():
    block, graph = _project_block(K4_RESULT, "44G4-QRKK-XN3")

    assert classify_low_voltage_subtype(block["pdfplumber_text"]) == "aps_fragment"
    assert graph is not None and graph["subtype"] == "aps_fragment"
    assert graph["validation"]["floors_total"] == 0
    gate = evaluate_low_voltage_gate(graph)
    assert gate["use"] is True and gate["mode"] == "address_inventory"

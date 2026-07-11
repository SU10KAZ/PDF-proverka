"""Общий слой vector paths для СОВ и кабельных лотков."""
from pathlib import Path

import pytest

from backend.app.pipeline.stages.block_grounding.vector_path_graph import (
    build_segment_components,
    extract_callout_leaders,
    terminal_network_diagnostics,
)


ROOT = Path(__file__).resolve().parents[1]
SS_DIR = ROOT / "experiments" / "блоки разных дисциплин" / "СС"
WIRING_PDF = SS_DIR / "01_13АВ-РД-СОВ-К3-6__79PP-NE7C-VDH.pdf"
TRAY_PDF = SS_DIR / "06_13АВ-РД-КК-К2__4KGV-FMLW-7V9.pdf"


def _segment(p1, p2, index):
    return {"id": f"s{index}", "p1": p1, "p2": p2,
            "length": ((p2[0] - p1[0]) ** 2 + (p2[1] - p1[1]) ** 2) ** 0.5}


def test_plain_crossing_does_not_connect_without_endpoint():
    graph = build_segment_components([
        _segment((0, 0), (10, 0), 1),
        _segment((5, -5), (5, 5), 2),
    ])
    assert len(graph["components"]) == 2


def test_t_junction_connects_endpoint_to_segment_interior():
    graph = build_segment_components([
        _segment((0, 0), (10, 0), 1),
        _segment((5, -5), (5, 0), 2),
    ])
    assert len(graph["components"]) == 1


@pytest.mark.skipif(not WIRING_PDF.exists(), reason="локальный PDF-корпус СС отсутствует")
def test_real_sov_path_diagnostics_are_stable():
    fitz = pytest.importorskip("fitz")
    with fitz.open(WIRING_PDF) as doc:
        metrics = terminal_network_diagnostics(doc[0])

    assert metrics["terminal_anchors"] == 111
    assert metrics["wire_segments"] == 1057
    assert metrics["terminals_attached"] == 65
    assert metrics["terminals_in_networks"] == 43
    assert metrics["terminal_networks"] == 18
    assert metrics["largest_terminal_network"] == 4


@pytest.mark.skipif(not TRAY_PDF.exists(), reason="локальный PDF-корпус СС отсутствует")
def test_real_tray_callouts_all_link_to_colored_geometry():
    fitz = pytest.importorskip("fitz")
    with fitz.open(TRAY_PDF) as doc:
        callouts = extract_callout_leaders(doc[0])

    leaders = [leader for callout in callouts for leader in callout["leaders"]]
    assert len(callouts) == 10
    assert len(leaders) == 16
    assert all(leader["geometry_linked"] for leader in leaders)
    colors = {leader["target_color"] for leader in leaders}
    assert {(0.0, 0.0, 1.0), (1.0, 0.0, 0.0), (1.0, 0.0, 1.0)} <= colors

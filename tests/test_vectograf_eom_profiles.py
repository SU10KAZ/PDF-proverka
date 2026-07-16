"""Регрессии ЭОМ-корпуса Вектографа: два диалекта расчётных строк."""
from pathlib import Path

import pytest

from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import (
    build_singleline_graph,
    evaluate_vectograf_gate,
)
from backend.app.pipeline.stages.block_grounding.singleline_structurer import (
    structure_singleline_text,
)


ROOT = Path(__file__).resolve().parents[1]
EOM_DIR = ROOT / "experiments" / "блоки разных дисциплин" / "ЭОМ"
EOM_ALT_PDF = (
    EOM_DIR
    / "05_13АВ-РД-ЭО2-ПА_V1__4LQJ-LYHX-M3T.pdf"
)


def test_separate_code_and_seven_value_formula_are_structured():
    text = """L1,L2,L3
QF1.1
ВА-103М
C 16 А
2РП1-1
ППГнг(А)-FRHF 5x4
Шкаф управления вентиляцией ЩУ-1
3 - 1 - 3 - 0,85 - 5,36 - 180 - 1,89
QF1.2
ВА-105
C 20 А
2РП1-2
ППГнг(А)-HF 5x6
Щит рабочего освещения ЩО-2
4 - 1 - 4 - 0,85 - 7,15 - 185 - 1,71
"""
    graph = structure_singleline_text(text)

    assert graph is not None
    assert graph["feeder_total"] == 2
    feeders = [f for s in graph["bus_sections"] for f in s["feeders"]]
    assert feeders[0]["circuit_code"] == "2РП1-1"
    assert feeders[0]["consumer"] == "Шкаф управления вентиляцией ЩУ-1"
    assert feeders[0]["P_inst_kw"] == 3.0
    assert feeders[0]["P_calc_kw"] == 3.0
    assert feeders[0]["cosphi"] == 0.85
    assert feeders[0]["I_a"] == 5.36
    assert feeders[0]["length_m"] == 180.0
    assert feeders[0]["voltage_drop_pct"] == 1.89
    assert feeders[0]["cable"] == "ППГнг(А)-FRHF 5x4"
    assert graph["validation"]["power_rate"] == 1.0
    assert graph["validation"]["current_rate"] == 1.0


def test_bare_seven_value_row_without_circuit_code_is_not_a_feeder():
    text = "\n".join(["3 - 1 - 3 - 0,85 - 5,36 - 180 - 1,89"] * 3)
    assert structure_singleline_text(text) is None


@pytest.mark.skipif(not EOM_ALT_PDF.exists(), reason="локальный PDF-корпус ЭОМ отсутствует")
def test_real_eom_separate_layout_passes_vectograf_gate():
    fitz = pytest.importorskip("fitz")
    with fitz.open(EOM_ALT_PDF) as doc:
        vector_text = doc[0].get_text()

    graph = build_singleline_graph(EOM_ALT_PDF, vector_text, panel_hint="ВРУ2")

    assert graph is not None
    assert graph["feeders_total"] == 50  # 51 физических подписей, QF4.13 повторён дважды
    assert graph["validation"]["qf_total_occurrences"] == 51
    assert graph["validation"]["duplicate_qf_labels"] == ["QF4.13"]
    assert graph["validation"]["codes_linked_occurrences"] == 44
    assert graph["validation"]["codes_total_occurrences"] == 45
    assert graph["validation"]["power_rate"] == 1.0
    assert graph["validation"]["current_rate"] >= 0.95
    assert graph["source"]["section"] == "13АВ-РД-ЭО2-ПА"
    assert any(line.startswith("РП4 (ПЭСПЗ) ->") for line in graph["hierarchy"]["tree_lines"])
    assert not any(line.startswith("РП4 (АВР) ->") for line in graph["hierarchy"]["tree_lines"])
    assert evaluate_vectograf_gate(graph)["use"] is True


EOM_CORPUS_EXPECTED = [
    ("01_13АВ-РД-ЭМ-К2_V1__466U-6UCY-6FY.pdf", 68, 69, 52, 53),
    ("02_13АВ-РД-ЭМ-К1__7HYD-CUDD-FPU.pdf", 68, 69, 59, 59),
    ("03_13АВ-РД-ЭМ-К2_V1__9CAF-FCQN-U6G.pdf", 62, 62, 58, 58),
    ("04_13АВ-РД-ЭМ-К1__9VCW-VLFL-G4F.pdf", 60, 60, 52, 52),
    ("05_13АВ-РД-ЭО2-ПА_V1__4LQJ-LYHX-M3T.pdf", 50, 51, 44, 45),
    ("06_13АВ-РД-ЭМ-К5__4PAG-Y4EV-HDR.pdf", 84, 86, 67, 71),
]


@pytest.mark.parametrize("filename,feeders,qf_occ,linked,total", EOM_CORPUS_EXPECTED)
def test_eom_corpus_regression(filename, feeders, qf_occ, linked, total):
    pdf = EOM_DIR / filename
    if not pdf.exists():
        pytest.skip("локальный PDF-корпус ЭОМ отсутствует")
    fitz = pytest.importorskip("fitz")
    with fitz.open(pdf) as doc:
        vector_text = "\n".join(page.get_text() for page in doc)

    graph = build_singleline_graph(pdf, vector_text, panel_hint="ЭОМ")

    assert graph is not None
    assert graph["feeders_total"] == feeders
    assert graph["validation"]["qf_total_occurrences"] == qf_occ
    assert graph["validation"]["codes_linked_occurrences"] == linked
    assert graph["validation"]["codes_total_occurrences"] == total
    assert graph["validation"]["ambiguous"] == 0
    assert graph["validation"]["geometry_conflicts"] == 0
    assert graph["validation"]["power_rate"] == 1.0
    assert graph["validation"]["current_rate"] >= 0.95
    assert evaluate_vectograf_gate(graph)["use"] is True

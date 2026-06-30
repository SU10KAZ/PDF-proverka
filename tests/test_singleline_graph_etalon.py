"""test_singleline_graph_etalon — регрессия графа однолинейной схемы (ВРУ-К1.2/К1.1).

Проверяет, что детерминированный `build_singleline_graph` + rich-renderer
`render_graph_etalon_markdown` выдают результат уровня эталона
`claude_etalon_vru_k1_2_v2_qf3_fix.md`.

Ключевая регрессия (РП3 листа ВРУ-К1.2): привязка QF↔код идёт по ГЕОМЕТРИИ КОЛОНКИ,
а не по порядку текста PDF. Эталонный кейс:
    QF3.10 → К1.2.4-2 → L=135м → «...(настен.) (правый)»   (НЕ К1.2.5-1 / 165м)

Данные (`projects/…/13АВ-РД-ЭМ-К1`) лежат вне git (projects/ в .gitignore), поэтому
тест пропускается, если их нет (локально/в окружении разработчика — выполняется).
Переопределить путь к result.json можно через env `SINGLELINE_K1_RESULT_JSON`.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

import pytest

from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import (
    build_singleline_graph,
    render_graph_etalon_markdown,
)

ROOT = Path(__file__).resolve().parents[1]


def _find_result_json():
    env = os.environ.get("SINGLELINE_K1_RESULT_JSON")
    cands = []
    if env:
        cands.append(Path(env))
    cands += sorted(ROOT.glob("projects/**/13АВ-РД-ЭМ-К1*result.json"))
    for rj in cands:
        if rj.exists():
            return rj
    return None


def _sibling_pdf(rj: Path):
    pdf = rj.with_name(rj.name.replace("_result.json", ".pdf"))
    if pdf.exists():
        return pdf
    pdfs = sorted(rj.parent.glob("*.pdf"))
    return pdfs[0] if pdfs else None


RJ = _find_result_json()
PDF = _sibling_pdf(RJ) if RJ else None

pytestmark = pytest.mark.skipif(
    not (RJ and PDF),
    reason="нет данных проекта 13АВ-РД-ЭМ-К1 (projects/ в .gitignore) — "
           "задайте SINGLELINE_K1_RESULT_JSON",
)


def _scheme_vector_text(code_prefix: str):
    """Вектор-текст блока однолинейной схемы по префиксу кодов (К1.2. → ВРУ-К1.2)."""
    d = json.loads(RJ.read_text(encoding="utf-8"))
    best, best_score = None, 0
    for pg in d.get("pages", []):
        for b in pg.get("blocks", []):
            vt = b.get("pdfplumber_text") or ""
            if code_prefix not in vt:
                continue
            score = len(set(re.findall(r"QF3\.\d+", vt)))
            if score > best_score:
                best_score, best = score, vt
    return best if best_score >= 10 else None


def _feeders_by_qf(graph):
    return {f["qf"]: f for f in graph["feeders_flat"]}


# ── ВРУ-К1.2 (эталонный лист, блок 9VCW) ──────────────────────────────────────────────

@pytest.fixture(scope="module")
def graph_k12():
    vt = _scheme_vector_text("К1.2.")
    if not vt:
        pytest.skip("в данных не найден блок схемы ВРУ-К1.2")
    g = build_singleline_graph(PDF, vt, panel_hint="ВРУ-К1.2")
    assert g is not None
    return g


@pytest.fixture(scope="module")
def md_k12(graph_k12):
    return render_graph_etalon_markdown(graph_k12)


def test_k12_graph_built_and_codes_linked(graph_k12):
    assert graph_k12["validation"]["codes_linked"] > 0
    assert graph_k12["validation"]["power_rate"] == 1.0
    assert graph_k12["validation"]["current_rate"] == 1.0


def test_k12_qf3_10_column_binding_regression(graph_k12):
    """Критический кейс: QF3.10 → К1.2.4-2 → 135м (а НЕ К1.2.5-1 / 165м)."""
    ff = _feeders_by_qf(graph_k12)
    f = ff["QF3.10"]
    assert f["circuit_code"] == "К1.2.4-2"
    assert f["length_m"] == 135
    assert "Освещение межкварт. коридора 21-30 эт. (настен.) (правый)" in (f["consumer"] or "")
    # и НЕ ошибочная привязка v1
    assert f["circuit_code"] != "К1.2.5-1"
    assert f["length_m"] != 165


def test_k12_qf3_11_follows_qf3_10(graph_k12):
    assert _feeders_by_qf(graph_k12)["QF3.11"]["circuit_code"] == "К1.2.5-1"


def test_k12_qf3_1_unbound_requires_review(graph_k12):
    """QF3.1 — отдельный аппарат без отходящего кода → не привязан, requires_review."""
    f = _feeders_by_qf(graph_k12)["QF3.1"]
    assert f["circuit_code"] is None
    assert f["status"] == "ambiguous"


def test_k12_markdown_has_sections(md_k12):
    for s in ("Текстовое дерево питания", "Панели и основные параметры",
              "Отходящие линии", "Реестр служебных элементов",
              "Таблица проверки трансформаторов тока", "Примечания"):
        assert s in md_k12, s


def test_k12_markdown_has_key_elements(md_k12):
    for el in ("ГРЩ с.ш.1", "ГРЩ с.ш.2", "ВП1", "ВП2", "РП3 (ОДН)", "РП4 (АВР)",
               "Wh", "TA1", "TA2", "TA3", "TA4", "QS1", "QS2", "АВР-303",
               "НАРТИС", "МК103", "УЗО03"):
        assert el in md_k12, el


def test_k12_markdown_no_wrong_qf3_10_binding(md_k12):
    for line in md_k12.split("\n"):
        if "QF3.10" in line:
            assert "К1.2.5-1" not in line, line
            assert "165" not in line, line


def test_k12_tt_check_table_extracted(graph_k12):
    tt = {r["panel"] for r in graph_k12["tt_check_table"]}
    assert {"ВП1", "ВП2", "ВП-АВР", "РП5 (ПЭСПЗ)", "РП5.1 (ПЭСПЗ)"} <= tt
    # «РП4 (ОДН)» как в ПД (молча не исправляем на РП3) + review-note
    rp4 = [r for r in graph_k12["tt_check_table"] if r["panel"] == "РП4 (ОДН)"]
    assert rp4 and rp4[0].get("review")


def test_k12_notes_full(graph_k12):
    ns = graph_k12["notes"]
    assert len(ns) == 18
    assert ns[0]["n"] == 1 and "IP31" in ns[0]["text"]


def test_k12_panel_calculations(graph_k12):
    pc = {p["id"]: p for p in graph_k12["panel_calculations"]}
    assert "РП3 (ОДН)" in pc and "РП4 (АВР)" in pc
    rp3 = pc["РП3 (ОДН)"]
    assert rp3["ikz3"] == 12.59
    modes = {m["mode"] for m in rp3["modes"]}
    assert "рабочий" in modes and "пожар" in modes


# ── ВРУ-К1.1 (смежный лист, блок 7HYD — не сломать) ───────────────────────────────────

@pytest.fixture(scope="module")
def graph_k11():
    vt = _scheme_vector_text("К1.1.")
    if not vt:
        pytest.skip("в данных не найден блок схемы ВРУ-К1.1")
    g = build_singleline_graph(PDF, vt, panel_hint="ВРУ-К1.1")
    assert g is not None
    return g


def test_k11_panels_and_feeders(graph_k11):
    names = [p["name"] for p in graph_k11["panels"]]
    # имена панелей берутся из штампа листа (РП3 → «РП3 (ОДН)»), поэтому префиксная проверка
    for pref in ("РП1", "РП2", "РП3", "РП4"):
        assert any(n.startswith(pref) for n in names), (pref, names)
    assert any(f["qf"].startswith("QF3.") for f in graph_k11["feeders_flat"])


def test_k11_physics_not_degraded(graph_k11):
    v = graph_k11["validation"]
    assert v["power_rate"] == 1.0
    assert v["current_rate"] == 1.0
    assert v["codes_linked"] > 0

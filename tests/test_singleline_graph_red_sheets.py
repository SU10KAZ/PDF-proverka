"""test_singleline_graph_red_sheets — фикс RED-листов окончаний РП5/РП5.1.

Покрывает доработки `singleline_graph_geometry` для двух проблемных листов «окончание»:
- ВРУ-К1.2 окончание (блок 7TLY): три-сегментные QF5.1.x → отдельная панель РП5.1;
- ВРУ-К1.1 окончание (блок 7A3T): Y-aware привязка (вторичные коды «ад/ан» не primary).

Плюс честные счётчики (occurrences/unique, дубли, пропущенные панели) и регрессия GREEN-листов
(9VCW QF3.10→К1.2.4-2→135м; 7HYD строится). Данные в projects/ (вне git) → skip-if-missing.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

import pytest

from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import (
    build_singleline_graph,
)

ROOT = Path(__file__).resolve().parents[1]


def _find_result_json():
    env = os.environ.get("SINGLELINE_K1_RESULT_JSON")
    cands = [Path(env)] if env else []
    cands += sorted(ROOT.glob("projects/**/13АВ-РД-ЭМ-К1*result.json"))
    return next((rj for rj in cands if rj.exists()), None)


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
    reason="нет данных проекта 13АВ-РД-ЭМ-К1 (projects/ в .gitignore) — задайте SINGLELINE_K1_RESULT_JSON",
)


def _scheme_vt(code_prefix: str, want_three_seg: bool = False):
    """Вектор-текст блока схемы по префиксу кодов; want_three_seg — выбрать лист с QF5.1.x."""
    d = json.loads(RJ.read_text(encoding="utf-8"))
    best, best_score = None, -1
    for pg in d.get("pages", []):
        for b in pg.get("blocks", []):
            vt = b.get("pdfplumber_text") or ""
            if code_prefix not in vt:
                continue
            has3 = bool(re.search(r"QF\d+\.\d+\.\d+", vt))
            if want_three_seg and not has3:
                continue
            score = len(set(re.findall(r"QF\d+(?:\.\d+){1,2}", vt)))
            if score > best_score:
                best_score, best = score, vt
    return best


def _build(vt, hint):
    assert vt, "блок схемы не найден в данных"
    g = build_singleline_graph(PDF, vt, panel_hint=hint)
    assert g is not None
    return g


def _ff(graph):
    return {f["qf"]: f for f in graph["feeders_flat"]}


# ── ВРУ-К1.2 окончание (7TLY): три-сегментные QF → панель РП5.1 ────────────────────────

@pytest.fixture(scope="module")
def graph_7tly():
    return _build(_scheme_vt("К1.2.1.", want_three_seg=True), "ВРУ-К1.2")


def test_singleline_qf_three_segments_7tly(graph_7tly):
    ff = _ff(graph_7tly)
    # QF5.1.1 ... QF5.1.17 присутствуют
    for n in range(1, 18):
        assert f"QF5.1.{n}" in ff, f"QF5.1.{n} отсутствует"
    # панель РП5.1 существует
    panel_names = [p["name"] for p in graph_7tly["panels"]]
    assert any("РП5.1" in n for n in panel_names), panel_names
    # эталонные пары (привязка по геометрии, со сдвигом на дубль К1.2.1.12а)
    assert ff["QF5.1.1"]["circuit_code"] == "К1.2.1.1а"
    assert ff["QF5.1.1"]["length_m"] == 180
    assert ff["QF5.1.6"]["circuit_code"] == "К1.2.1.6а"
    assert ff["QF5.1.14"]["circuit_code"] == "К1.2.1.13а"
    assert ff["QF5.1.15"]["circuit_code"] == "К1.2.1.14а"
    # secondary НЕ привязаны как primary
    assert ff["QF5.1.6"]["circuit_code"] not in ("К1.2.1.6ад", "К1.2.1.6ан")
    # нет missing-warning для РП5.1
    assert not graph_7tly["validation"]["missing_panel_warnings"]


def test_7tly_secondary_codes_not_primary(graph_7tly):
    """Коды «ад»/«ан» — вторичные, лежат в secondary_circuits, а не как circuit_code."""
    primary_codes = {f["circuit_code"] for f in graph_7tly["feeders_flat"] if f["circuit_code"]}
    sec_codes = {s["code"] for s in graph_7tly.get("secondary_circuits", [])}
    assert any(c.endswith("ад") or c.endswith("ан") for c in sec_codes)
    for c in primary_codes:
        assert not (c.endswith("ад") or c.endswith("ан")), c


# ── ВРУ-К1.1 окончание (7A3T): Y-aware привязка ───────────────────────────────────────

@pytest.fixture(scope="module")
def graph_7a3t():
    # именно лист-окончание (панель РП5, без QF3.*) — _scheme_vt дал бы начало (больше QF)
    return _build(_scheme_7a3t_vt(), "ВРУ-К1.1")


def _scheme_7a3t_vt():
    d = json.loads(RJ.read_text(encoding="utf-8"))
    best, best_score = None, -1
    for pg in d.get("pages", []):
        for b in pg.get("blocks", []):
            vt = b.get("pdfplumber_text") or ""
            if "К1.1." not in vt or not re.search(r"QF5\.\d+", vt):
                continue
            if re.search(r"QF3\.\d+", vt):   # это лист-начало, пропустить
                continue
            score = len(set(re.findall(r"QF5\.\d+", vt)))
            if score > best_score:
                best_score, best = score, vt
    return best


def test_singleline_y_aware_codes_7a3t():
    g = _build(_scheme_7a3t_vt(), "ВРУ-К1.1")
    ff = _ff(g)
    assert ff["QF5.28"]["circuit_code"] == "К1.1.18а"
    assert ff["QF5.37"]["circuit_code"] == "К1.1.27а"
    # запрещённые (secondary) привязки
    assert ff["QF5.28"]["circuit_code"] not in ("К1.1.18ад", "К1.1.17ан")
    assert ff["QF5.37"]["circuit_code"] not in ("К1.1.26ан",)
    # secondary сохранены отдельным слоем
    sec_codes = {s["code"] for s in g.get("secondary_circuits", [])}
    assert any(c.endswith("ад") for c in sec_codes)
    assert any(c.endswith("ан") for c in sec_codes)
    primary = {f["circuit_code"] for f in g["feeders_flat"] if f["circuit_code"]}
    assert not any(c.endswith("ад") or c.endswith("ан") for c in primary)
    # primary GEOMETRY_CONFLICT резко снижен (было 17)
    assert g["validation"]["geometry_conflicts"] <= 2


# ── Честные счётчики ──────────────────────────────────────────────────────────────────

def test_singleline_counts_are_honest(graph_7tly, graph_7a3t):
    for g in (graph_7tly, graph_7a3t):
        v = g["validation"]
        assert "codes_total_occurrences" in v
        assert "codes_linked_occurrences" in v
        assert isinstance(v["duplicate_param_codes"], list)
        assert isinstance(v["duplicate_bindings"], list)
        # status не ok при неполном покрытии occurrences
        if v["codes_linked_occurrences"] < v["codes_total_occurrences"]:
            assert g["status"] != "ok"
    # реальные дубли где они есть в ПД
    assert "К1.2.1.12а" in graph_7tly["validation"]["duplicate_param_codes"]
    # дубль кода в вектор-слое (К1.1.29а на два QF)
    assert "К1.1.29а" in graph_7a3t["validation"]["duplicate_bindings"]


# ── GREEN-регрессии: не сломать начальные листы ───────────────────────────────────────

def test_singleline_green_regressions():
    g9 = _build(_scheme_vt("К1.2.", want_three_seg=False), "ВРУ-К1.2")
    # выбрать именно начало (с РП1-РП4 / QF3.*)
    if not any(f["qf"].startswith("QF3.") for f in g9["feeders_flat"]):
        g9 = _build(_scheme_9vcw_vt(), "ВРУ-К1.2")
    ff9 = _ff(g9)
    assert ff9["QF3.10"]["circuit_code"] == "К1.2.4-2"
    assert ff9["QF3.10"]["length_m"] == 135
    assert ff9["QF3.1"]["circuit_code"] is None

    g7 = _build(_scheme_7hyd_vt(), "ВРУ-К1.1")
    assert g7["validation"]["power_rate"] == 1.0
    assert g7["validation"]["current_rate"] == 1.0
    names = [p["name"] for p in g7["panels"]]
    for pref in ("РП1", "РП2", "РП3", "РП4"):
        assert any(n.startswith(pref) for n in names), (pref, names)


def _scheme_9vcw_vt():
    d = json.loads(RJ.read_text(encoding="utf-8"))
    best, sc = None, -1
    for pg in d.get("pages", []):
        for b in pg.get("blocks", []):
            vt = b.get("pdfplumber_text") or ""
            if "К1.2." in vt and re.search(r"QF3\.\d+", vt) and not re.search(r"QF\d+\.\d+\.\d+", vt):
                s = len(set(re.findall(r"QF3\.\d+", vt)))
                if s > sc:
                    sc, best = s, vt
    return best


def _scheme_7hyd_vt():
    d = json.loads(RJ.read_text(encoding="utf-8"))
    best, sc = None, -1
    for pg in d.get("pages", []):
        for b in pg.get("blocks", []):
            vt = b.get("pdfplumber_text") or ""
            if "К1.1." in vt and re.search(r"QF3\.\d+", vt):
                s = len(set(re.findall(r"QF3\.\d+", vt)))
                if s > sc:
                    sc, best = s, vt
    return best


# ── Доп-аппараты линии (УЗО03/КМ/МК103 в вертикали QF после автомата) ──────────────────

def test_additional_line_devices_7hyd():
    """Стр.8 (7HYD): УЗО03/КМ/МК103 извлекаются в feeder.additional_devices по X-колонке."""
    g = _build(_scheme_7hyd_vt(), "ВРУ-К1.1")
    ff = _ff(g)

    def devs(q):
        return " || ".join(ff[q].get("additional_devices") or [])

    # QF3.27-QF3.34: УЗО03 4Р (100мА АС) + КМ (1НО+1НЗ)
    for q in ("QF3.27", "QF3.30", "QF3.33"):
        assert re.search(r"УЗО03.*4Р.*100мА.*АС", devs(q)), (q, devs(q))
        assert re.search(r"КМ.*1НО\+1НЗ", devs(q)), (q, devs(q))
    # QF3.35-QF3.36: УЗО03 2Р 16А 30мА АС
    for q in ("QF3.35", "QF3.36"):
        assert re.search(r"УЗО03.*2Р.*16А.*30мА.*АС", devs(q)), (q, devs(q))
    # QF3.37-QF3.38, QF3.10: УЗО03-2Р … 16А
    for q in ("QF3.37", "QF3.38", "QF3.10"):
        assert "УЗО03-2Р" in devs(q) and "16А" in devs(q), (q, devs(q))
    # МК103 где указан (QF3.4/QF3.8/QF3.18); QF3.5 — БЕЗ устройства (не выдумываем)
    for q in ("QF3.4", "QF3.8", "QF3.18"):
        assert "МК103" in devs(q), (q, devs(q))
    assert (ff["QF3.5"].get("additional_devices") or []) == []
    # не путаем с автоматом: устройства не содержат «ВА-»
    for f in g["feeders_flat"]:
        for d in (f.get("additional_devices") or []):
            assert "ВА-" not in d, (f["qf"], d)


def test_additional_devices_match_etalon_9vcw():
    """ВРУ-К1.2 (9VCW): совпадение с эталоном — QF3.10→МК103, QF3.26→УЗО03-2Р 30мА 20А."""
    vt = _scheme_9vcw_vt()
    if not vt:
        pytest.skip("нет блока 9VCW")
    g = _build(vt, "ВРУ-К1.2")
    ff = _ff(g)
    assert any("МК103" in d for d in (ff["QF3.10"].get("additional_devices") or []))
    assert any(re.search(r"УЗО03-2Р.*30мА.*20А", d) for d in (ff["QF3.26"].get("additional_devices") or []))

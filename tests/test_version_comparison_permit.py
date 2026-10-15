"""Разрешение «заявлено ↔ найдено» и контроль состава версии (план §15 п. 3, 6)."""
from __future__ import annotations

import fitz

from backend.app.services.version_comparison import permit, pdfio
from tests import version_comparison_fixtures as fx


def _page(no: str, title: str, designation: str = "ТЕСТ-01-ЭМ", page: int = 1) -> dict:
    return {"page": page, "kind": "drawing", "stamp": {"sheet_no": no, "title": title, "designation": designation}}


def test_sheet_refs_parsing():
    labels = lambda t: [permit.ref_label(r) if r["set"] != "unparsed" else "?" + r["sheet"] for r in permit.parse_sheet_refs(t)]
    assert labels("4,5") == ["л.4", "л.5"]
    assert labels("СО л. 2") == ["СО л.2"]
    assert labels("17-19") == ["л.17", "л.18", "л.19"]
    assert labels("2-3 14, 28 СО1- СО3") == ["л.2", "л.3", "л.14", "л.28", "СО л.1", "СО л.2", "СО л.3"]
    assert labels("все листы") == ["все листы"]


def test_permit_page_is_read_literally(tmp_path):
    rows = [("7", "4,5", "Добавлены обозначения фаз", "1"), ("7", "СО л. 2", "Добавлена розетка", "1")]
    pdf = fx.make_pdf(tmp_path / "p.pdf", [fx.sheet("1", "Разрешение", permit_rows=rows, permit_number="2010/26")])
    page = fitz.open(pdf)[0]
    res = permit.parse_page(page, pdfio.page_words(page))
    assert res["number"] == "2010/26"
    assert [(r["izm"], r["sheet"], r["text"], r["code"]) for r in res["rows"]] == \
        [(7, "4,5", "Добавлены обозначения фаз", "1"), (7, "СО л. 2", "Добавлена розетка", "1")]


def test_reconcile_declared_vs_found():
    permits = [{"page": 3, "number": "2010/26", "rows": [
        {"izm": 7, "sheet": "4,5", "text": "фазы", "refs": permit.parse_sheet_refs("4,5")},
        {"izm": 7, "sheet": "СО л. 2", "text": "розетка", "refs": permit.parse_sheet_refs("СО л. 2")},
        {"izm": 6, "sheet": "9", "text": "старое изменение", "refs": permit.parse_sheet_refs("9")}]}]
    pages_b = [_page("4", "Схема ВРУ-1", page=12), _page("5", "Схема ВРУ-1", page=13), _page("1", "Общие данные", page=10),
               _page("2", "Спецификация", designation="ТЕСТ-01-ЭМ.СО", page=51), _page("9", "План", page=20)]
    found = [{"b_page": 12, "status": "different", "title": "л.4"}, {"b_page": 13, "status": "same_zones_changed", "title": "л.5"},
             {"b_page": 10, "status": "different", "title": "л.1"}, {"b_page": 51, "status": "different", "title": "СО л.2"},
             {"b_page": 20, "status": "different", "title": "л.9"}]
    rec = permit.reconcile(permits, 6, pages_b, found)
    assert rec["izm_new"] == [7], "строки Изм.6 и раньше уже были в версии A"
    assert sorted(e["ref"] for e in rec["declared_and_found"]) == ["СО л.2", "л.4"]
    assert [e["ref"] for e in rec["declared_no_diff"]] == ["л.5"]
    assert sorted(e["ref"] for e in rec["diff_not_declared"]) == ["л.1", "л.9"]

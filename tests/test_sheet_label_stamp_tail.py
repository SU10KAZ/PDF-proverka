"""Столбец «Лист»: номер без хвоста штампа (20.10.2026).

Новый формат MD кладёт штамп одной строкой — «01 | Наименование проекта: … |
Наименование листа: …» — и весь текст уходил в sheet_no_raw и в столбец «Лист».
"""
from backend.app.pipeline.stages.prepare.graph_builder import (
    build_page_sheet_map,
    clean_sheet_label,
    clean_sheet_no,
    get_page_sheet_no,
)
from backend.app.pipeline.stages.block_analysis.gemma_findings_only import sheet_for_page
from backend.app.services.findings.findings_service import _enrich_sheet_page

STAMP = (
    " | Наименование проекта: Многофункциональная комплексная жилая застройка по "
    "адресу: г. Москва, ЗАО, район Раменки, квартал 3, Корпус 1 (рекламное название "
    "Событие 6.1) | Наименование листа: Общие данные"
)
GRAPH = {"pages": [
    {"page": 1, "sheet_no_raw": None, "sheet_no_normalized": None},
    {"page": 3, "sheet_no_raw": "01" + STAMP, "sheet_no_normalized": "01"},
    {"page": 26, "sheet_no_raw": "24" + STAMP, "sheet_no_normalized": "24"},
    {"page": 30, "sheet_no_raw": " | Наименование проекта: Не указано", "sheet_no_normalized": "5"},
    {"page": 40, "sheet_no": "7"},
]}


def test_clean_sheet_no_cuts_stamp_tail():
    assert clean_sheet_no("01" + STAMP) == "01"
    assert clean_sheet_no("31.11") == "31.11"
    assert clean_sheet_no(" | Наименование") is None
    assert clean_sheet_no(None) is None


def test_get_page_sheet_no_returns_short_number():
    pages = {p["page"]: p for p in GRAPH["pages"]}
    assert get_page_sheet_no(pages[3]) == "01"
    assert get_page_sheet_no(pages[30]) == "5"  # пустой номер в raw → normalized
    assert get_page_sheet_no(pages[40]) == "7"  # v1-граф не тронут
    assert build_page_sheet_map(GRAPH) == {3: "01", 26: "24", 30: "5", 40: "7"}
    assert sheet_for_page(GRAPH, 26) == "Лист 24"


def test_clean_sheet_label_for_already_written_findings():
    assert clean_sheet_label("Лист 01" + STAMP) == "Лист 01"
    assert clean_sheet_label(
        "Листы 2 | Наименование проекта: Не указано | Наименование листа: Не указано, "
        "5 | Наименование проекта: Не указано | Наименование листа: Не указано"
    ) == "Листы 2, 5"
    assert clean_sheet_label("Лист 24") == "Лист 24"
    assert clean_sheet_label(None) is None


def test_enrichment_rebuilds_dirty_sheet_from_graph():
    findings = [
        {"id": "F-001", "sheet": "Лист 24" + STAMP, "page": 26},
        {"id": "F-002", "sheet": "Листы 01" + STAMP + ", 24" + STAMP, "page": [3, 26]},
        {"id": "F-003", "sheet": "Лист 01" + STAMP, "page": 99},  # страницы нет в графе
        {"id": "F-004", "sheet": "Лист 9", "page": 26},  # чистое значение не трогаем
    ]
    _enrich_sheet_page(findings, "synthetic", graph_data=GRAPH)
    assert [f["sheet"] for f in findings] == ["Лист 24", "Лист 01, 24", "Лист 01", "Лист 9"]
    assert all("|" not in f["sheet"] for f in findings)

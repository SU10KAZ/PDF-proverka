from __future__ import annotations

from backend.app.services.stage_comparison.function_lineage_source import (
    extract_page_sources,
)


def test_extract_page_sources_builds_compact_structured_function_facts():
    markdown = """
## Page 1
> **Created:** 2026-01-01
> **Stamp:** Sheet: 2.1 | Name: Схема насосной ХВС корпуса 2
**Object:** Корпус 2
**Zone:** Зона 3
**Level:** -1 этаж
**Summary:** Насосная хозяйственно-питьевого водоснабжения питает квартиры; продолжение см. лист 3.
**Entities:** насос Н1; водомерный узел В1; Корпус 2
**[IMAGE]** | Type: инженерная схема | Description: Ввод В1 подает воду к потребителям.
"""

    source = extract_page_sources(markdown)[1]

    assert source["physical_page"] == 1
    assert source["graphic_sheet_number"] == "2.1"
    assert source["document_role"] == "GRAPHIC_SHEET"
    assert source["corpus"]
    assert source["zone"] == ["Зона 3"]
    assert source["floors"]
    assert source["consumers"]
    assert source["upstream"]
    assert source["downstream"]
    assert source["cross_sheet_functional_references"]
    assert "DOMESTIC_PRESSURE_BOOST" in {
        value["function_class"] for value in source["functions"]
    }
    assert source["source_content_signature"]
    assert "raw_excerpt" not in source


def test_unknown_source_fields_remain_null():
    source = extract_page_sources("""
## Page 4
> **Stamp:** Sheet: 4 | Name: Однолинейная схема ГРЩ
**Summary:** Схема электрического распределения.
""")[4]

    assert source["zone"] is None
    assert source["floors"] is None
    assert source["cross_sheet_functional_references"] is None

"""Значения позиций из текстового слоя PDF при структуре от Луны (без модели)."""
from backend.app.services.version_comparison import specification_pdf_values as pv


def md_row(name, page, mark="", qty="", unit="", section="Раздел", parent="", role="item"):
    return {"section": section, "parent": parent, "role": role,
            "fields": {"pos": "", "name": name, "mark": mark, "code": "", "supplier": "", "unit": unit, "qty": qty,
                       "mass": "", "note": ""},
            "sources": [{"page": page, "document": "a.pdf", "sheet": "4", "block_id": "b", "line_start": 1,
                         "line_end": 1, "quote": name}]}


def item(i, page, y, name, mark="", qty="", unit="", parent=None, note=""):
    return {"id": i, "kind": "child" if parent else "position", "page": page, "rows": [[page, 0, y, 0, y + 5]],
            "parent": parent, "pos": "", "name": name, "mark": mark, "code": "", "supplier": "", "unit": unit,
            "qty": qty, "mass": "", "note": note}


def test_empty_md_cells_take_pdf_values_and_pdf_only_rows_are_added():
    checked = {"rows": [md_row("Кабель силовой ППГнг(А)-HF сечением, мм2:", 51, role="group"),
                        md_row("3х2.5", 51, parent="Кабель силовой ППГнг(А)-HF сечением, мм2:"),
                        md_row("Сжим ответвительный", 51, mark="У-734М")]}
    spec = {"parents": [{"id": "g1", "page": 51, "rows": [[51, 0, 10, 0, 15]], "name": "Кабель силовой ППГнг(А)-HF",
                         "mark": "", "pos": "", "code": "", "supplier": "", "unit": "", "qty": "", "mass": "", "note": ""}],
            "items": [item("i1", 51, 20, "3х2.5", qty="540", unit="м", parent="g1"),
                      item("i2", 51, 30, "3х4", qty="655", unit="м", parent="g1"),
                      item("i3", 51, 40, "Сжим ответвительный", mark="У-734М", qty="30", unit="шт.", note="ЩР-1"),
                      item("i4", 51, 50, "или вентилятора дымоудаления", qty="3", unit="шт.")]}
    rep = pv.apply(checked, spec)
    rows = {r["fields"]["name"]: r for r in checked["rows"]}
    assert rows["3х2.5"]["fields"]["qty"] == "540" and rows["3х2.5"]["md_values"] == {"unit": "", "qty": ""}
    assert rows["Сжим ответвительный"]["fields"]["qty"] == "30" and rows["Сжим ответвительный"]["fields"]["note"] == "ЩР-1"
    added = rows["3х4"]                                   # строки нет в MD — добавлена из PDF с родителем
    assert added["pdf_only"] and added["fields"]["qty"] == "655"
    assert added["parent"] == "Кабель силовой ППГнг(А)-HF сечением, мм2:" and added["section"] == "Раздел"
    assert "или вентилятора дымоудаления" not in rows                         # обрывок фразы не добавлен
    assert rep["pdf_fragments"][0]["qty"] == "3" and rep["matched"] == 3


def test_without_confident_pdf_match_md_values_stay_and_are_reported():
    checked = {"rows": [md_row("Лоток перфорированный 100х100", 56, qty="90", unit="м")]}
    spec = {"parents": [], "items": [item("i1", 56, 10, "Совершенно другая позиция", qty="7", unit="шт")]}
    rep = pv.apply(checked, spec)
    row = checked["rows"][0]
    assert row["fields"]["qty"] == "90" and row["pdf_match"] is None
    assert rep["md_only"] and rep["pdf_only"][0]["name"].startswith("Совершенно")


def test_no_pdf_data_changes_nothing():
    checked = {"rows": [md_row("Кабель", 1, qty="5")]}
    assert pv.apply(checked, None)["pdf"] is False and checked["rows"][0]["fields"]["qty"] == "5"


def test_md_row_without_quantity_absent_in_pdf_table_is_not_a_position():
    """Запись штампа «6 зам. 924/26 04.2026», попавшая в таблицу MD, в сравнение не идёт."""
    checked = {"rows": [md_row("Розетка", 50, qty="", unit=""), md_row("6", 50, mark="зам. - 924/26 04.2026")]}
    spec = {"parents": [], "items": [item("i1", 50, 10, "Розетка", qty="4", unit="шт")]}
    rep = pv.apply(checked, spec)
    assert [r["fields"]["name"] for r in checked["rows"]] == ["Розетка"]
    assert rep["md_text_rows"][0]["mark"].startswith("зам.")


def test_note_row_that_is_a_pdf_table_row_is_dropped_and_pdf_row_used():
    checked = {"rows": [md_row("", 52, role="item")]}
    checked["rows"][0]["fields"]["note"] = "1х70"
    spec = {"parents": [], "items": [item("i1", 52, 10, "1х70", qty="660", unit="м")]}
    pv.apply(checked, spec)
    assert [(r["fields"]["name"], r["fields"]["qty"]) for r in checked["rows"]] == [("1х70", "660")]


def test_rows_of_a_duplicate_sheet_are_not_positions():
    """Тот же лист СО ещё раз в выпуске (движок помечает duplicate) — его строки не добавляются позициями."""
    checked = {"rows": [md_row("Щит ЩР-1", 50, qty="1", unit="шт"), md_row("Щит ЩР-1", 52, qty="1", unit="шт")]}
    spec = {"pages": [{"page": 50, "duplicate": False}, {"page": 52, "duplicate": True}],
            "parents": [], "items": [item("i1", 50, 10, "Щит ЩР-1", qty="1", unit="шт")]}
    rep = pv.apply(checked, spec)
    assert [(r["sources"][0]["page"]) for r in checked["rows"]] == [50] and rep["duplicate_pages"] == {52: 1}

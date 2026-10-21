"""Отказ по группе, а не по версии (docs/whole_specification_v2_plan.md, этап 2)."""
from pathlib import Path

from backend.app.services.version_comparison import specification_compare as cmp
from backend.app.services.version_comparison import specification_export as export
from backend.app.services.version_comparison.specification_source import load_source
from backend.app.services.version_comparison.specification_structure import validate

HEADER = ["| Поз. | Наименование | Марка | Код | Поставщик | Ед. изм. | Количество | Масса | Примечание |",
          "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
D1, D2 = "| Д 1 |  |  |  |  |  |  |  |  |", "| Д 2 |  |  |  |  |  |  |  |  |"


def _source(tmp_path: Path, rows: list[str], label="OLD", page_break_before=None):
    body = ["# Document: spec.pdf", "## Page 1", "### BLOCK #1 [TEXT]: blk_aaaaaaaa"]
    for i, r in enumerate([*HEADER, *rows]):
        if page_break_before is not None and i == page_break_before + len(HEADER):
            body += ["## Page 2", "### BLOCK #2 [TEXT]: blk_bbbbbbbb", *HEADER]
        body.append(r)
    path = tmp_path / f"{label}.md"
    path.write_text("\n".join(body) + "\n", encoding="utf-8")
    return load_source(path, label=label)


def _at(src, text):
    return [ln["line"] for b in src["blocks"] for ln in b["lines"] if ln["text"] == text]


def _rows(qty2="2"):
    return [D1, "|  | Насос | Mini Aqua |  |  | шт. | 1 |  |  |",
            "|  | Угольник 90° |  |  |  | шт. | 7 |  |  |",
            "|  | Тройник 25x20 |  |  |  | шт. | 1 |  |  |",
            "|  | Кран шаровой |  |  |  | шт. | 2 |  |  |",
            D2, "|  | Насос | Mini Aqua |  |  | шт. | " + qty2 + " |  |  |",
            "|  | Примечание: допускается замена |  |  |  |  |  |  |  |"]


def test_error_in_one_group_keeps_the_rest_of_the_version(tmp_path):
    src = _source(tmp_path, _rows())
    d1, d2 = _at(src, D1)[0], _at(src, D2)[0]
    note = _at(src, _rows()[-1])[0]
    # «Примечание» в строке таблицы помечено примечанием — ошибка только группы «Д 2».
    checked = validate(src, {"sections": [{"line": d1}, {"line": d2}], "parents": [], "continuations": [],
                             "notes": [{"line": note}]})
    assert not checked["complete"] and checked["partial"]
    assert [r["fields"]["name"] for r in checked["rows"] if r.get("role") != "section"] == ["Насос", "Угольник 90°", "Тройник 25x20", "Кран шаровой"]
    [group] = checked["unresolved_groups"]
    assert group["title"] == D2 and note in group["lines"] and d2 in group["lines"]
    assert any(i.startswith("Note must reference a plain source line") for i in checked["issues"])


def test_error_everywhere_is_still_a_failed_version(tmp_path):
    src = _source(tmp_path, _rows())
    lines = [ln["line"] for b in src["blocks"] for ln in b["lines"]][2:]
    checked = validate(src, {"sections": [], "parents": [], "continuations": [],
                             "notes": [{"line": n} for n in lines]})
    assert not checked["complete"] and not checked.get("partial") and checked["rows"] == []


def test_unresolved_group_is_not_compared_on_either_side_and_its_pages_are_delegated(tmp_path):
    old = _source(tmp_path, _rows("2"), label="A")
    new = _source(tmp_path, [*_rows("5"), "|  | Труба |  |  |  | м | 3 |  |  |"], label="B")
    ok = lambda src: validate(src, {"sections": [{"line": n} for n in _at(src, D1) + _at(src, D2)],
                                    "parents": [], "continuations": [], "notes": []})
    a = ok(old)
    b_src = new
    note = _at(b_src, _rows()[-1])[0]
    b = validate(b_src, {"sections": [{"line": n} for n in _at(b_src, D1) + _at(b_src, D2)], "parents": [],
                         "continuations": [], "notes": [{"line": note}]})
    assert a["complete"] and b["partial"]
    result = cmp.compare(a, b)
    assert result["complete"] and result["status"] == "partial"
    assert [g["title"] for g in result["not_compared"]] == [D2]
    assert result["not_compared"][0]["unresolved_in"] == ["new"]
    # Позиции «Д 2» не сравниваются: изменение количества 2 → 5 и «Труба» не публикуются построчно.
    texts = [e["text"] for e in cmp.change_entries(result)]
    assert not any("Насос" in t and "5" in t for t in texts) and not any("Труба" in t for t in texts)
    assert result["delegated_pages"] == {"old": [1], "new": [1]}
    assert "Не сравнены построчно" in cmp.render_markdown(result)


def test_delegated_pages_are_ordinary_sheets_for_sheet_ai():
    ctx = {"whole_spec": True, "kinds": {"a": {3: "spec", 4: "spec"}, "b": {3: "spec", 4: "spec"}},
           "spec_pages": {"a": {3, 4}, "b": {3, 4}}, "delegated": {"a": {4}, "b": set()}}
    assert export.contains_specification(ctx, {"a_page": 3, "b_page": 3})
    assert not export.contains_specification(ctx, {"a_page": 4, "b_page": 4})
    assert not export.is_specification(ctx, {"a_page": 4, "b_page": 4})

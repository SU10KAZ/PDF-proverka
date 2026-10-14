"""Спецификации по позициям через страницы (план §5 шаг 7; сценарии 14 и 33)."""
from __future__ import annotations

import hashlib
import os
import shutil
from pathlib import Path

import pytest

from backend.app.services.version_comparison import paths, profile, spec
from tests import version_comparison_fixtures as fx

OBJ = "obj-spec"
CABLE = {"pos": "1", "name": "Кабель силовой с медными жилами", "mark": "ППГнг(А)-HF", "code": "ГОСТ 31996-2012",
         "supplier": "ООО Авангард"}


@pytest.fixture
def vc_root(tmp_path, monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path / "vc"))
    monkeypatch.setenv("CPU_POOL_WORKERS", "1")
    return tmp_path


def _sub(size: str, qty: str, note: str = "") -> dict:
    return {"name": size, "center": True, "unit": "м", "qty": qty, "note": note}


def _build(tmp: Path, name: str, pages_rows: list[list[dict]]) -> dict:
    pages = [fx.sheet(str(k + 1), "Спецификация оборудования", so_rows=rows) for k, rows in enumerate(pages_rows)]
    pdf = fx.make_pdf(tmp / f"{name}.pdf", pages)
    sha = hashlib.sha256(pdf.read_bytes()).hexdigest()
    dst = paths.source_pdf(OBJ, sha)
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(pdf, dst)
    os.chmod(dst, 0o444)
    prof = profile.build(OBJ, sha)
    out = tmp / "spec" / name
    spec.build(str(dst), prof["pages"], str(out))
    return spec.load(str(out))


def _filler(n: int, start: int = 0) -> list[dict]:
    return [_sub(f"3х{k + 1}", str(100 + k)) for k in range(start, start + n)]


def test_33_rows_at_page_top_are_typed_by_columns_not_by_number(vc_root):
    page1 = [{"section": "Кабельно-проводниковая продукция"}, CABLE,
             {"name": "не распространяющий горение, сечением, мм2:"}] + _filler(fx.SO_ROWS_PER_PAGE - 3)
    page2 = [_sub("5х25", "245", "каб. обогрев"),                                  # подстрока родителя со стр. 1
             {"name": "Труба гофрированная ПВХ", "mark": "ПВХ 20", "unit": "м", "qty": "100"},   # без номера — новая
             {"name": "с зондом, легкая"}]                                          # продолжение предыдущей
    d = _build(vc_root, "v1", [page1, page2])
    assert all(p["parsed"] for p in d["pages"]) and len(d["pages"]) == 2
    child = next(it for it in d["items"] if it["page"] == 2 and it["name"] == "5х25")
    assert child["kind"] == "child" and child["parent_mark"] == "ППГнг(А)-HF" and child["qty"] == "245"
    assert "сечением" in child["parent_name"], "перенос наименования родителя дописан к нему"
    pipe = next(it for it in d["items"] if it["name"].startswith("Труба гофрированная"))
    assert pipe["kind"] == "position" and pipe["pos"] == "" and pipe["qty"] == "100"
    assert pipe["name"] == "Труба гофрированная ПВХ с зондом, легкая"
    assert all(it["section"] == "Кабельно-проводниковая продукция" for it in d["items"])


def test_14_position_moved_to_next_page_is_not_a_change_but_quantity_is(vc_root):
    rows_a = [{"section": "Кабельно-проводниковая продукция"}, CABLE, _sub("3х2.5", "3350")] \
        + _filler(fx.SO_ROWS_PER_PAGE - 4) + [_sub("5х25", "245")]
    a = _build(vc_root, "a", [rows_a, [_sub("5х35", "335")]])
    # B: вставлена строка — последняя подстрока стр. 1 переехала на стр. 2; 3350 → 3400.
    rows_b = [{"section": "Кабельно-проводниковая продукция"}, CABLE, _sub("3х2.5", "3400"), _sub("4х4", "50")] \
        + _filler(fx.SO_ROWS_PER_PAGE - 4)
    b = _build(vc_root, "b", [rows_b, [_sub("5х25", "245"), _sub("5х35", "335")]])
    cmp_ = spec.compare(a, b)
    changed = [c for c in cmp_["changes"] if c["kind"] == "changed"]
    added = [c for c in cmp_["changes"] if c["kind"] == "added"]
    assert [(c["fields"]["qty"], c["a_pages"], c["b_pages"]) for c in changed] == [(["3350", "3400"], [1], [1])]
    assert "3х2.5" in changed[0]["label"] and "ППГнг(А)-HF" in changed[0]["label"]
    assert [c["label"] for c in added] == ["ППГнг(А)-HF 4х4"]
    assert not [c for c in cmp_["changes"] if c["kind"] == "removed"]
    assert cmp_["moved_between_pages"] >= 1, "5х25 переехала на стр. 2 — перенос, не изменение"
    assert {t["label"]: (t["a"], t["b"]) for t in cmp_["totals"]}["ППГнг(А)-HF 3х2.5, м"] == (3350.0, 3400.0)


def test_spec_parse_and_compare_helpers():
    a = {"items": [{"id": "i1", "kind": "position", "section": "Р", "name": "Розетка", "mark": "IP20", "pos": "1",
                    "code": "", "supplier": "", "unit": "шт.", "qty": "52", "mass": "", "note": "", "rows": [[1, 0, 0, 1, 1]],
                    "parent": None, "ambiguous": False, "page": 1}],
         "pages": [{"page": 1, "sheet_no": "1"}]}
    b = {"items": [dict(a["items"][0], pos="2", qty="52,0")], "pages": [{"page": 1, "sheet_no": "1"}]}
    c = spec.compare(a, b)
    assert [x["kind"] for x in c["changes"]] == ["renumbered"], "только номер позиции — не изменение"
    assert spec._norm("3x2,5") == spec._norm("3х2.5")

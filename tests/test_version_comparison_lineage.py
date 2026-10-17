"""Строки матрицы: сопоставление листов между версиями (план §10.1 п. 7–9, 32)."""
from __future__ import annotations

import hashlib
import os
import shutil
from pathlib import Path

import pytest

from backend.app.services.version_comparison import lineage, paths, profile
from tests import version_comparison_fixtures as fx

OBJ = "obj-lin"

WORDS = {
    1: "квартирография студия комната этаж стояк мощность итого",
    2: "щит учета счетчик трансформатор ток автомат ввод резерв",
    3: "кабельный лоток трасса гофра стояк кровля обогрев",
    4: "розетка освещение светильник выключатель коробка группа",
    5: "насос дренаж приямок поплавок датчик уровень авария",
}
TITLES = {1: "Квартирография", 2: "Схема ВРУ-1", 3: "План кабельных трасс", 4: "План розеток", 5: "Схема дренажа"}


@pytest.fixture
def vc_root(tmp_path, monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path / "vc"))
    monkeypatch.setenv("CPU_POOL_WORKERS", "1")
    return tmp_path


def sheet(key: int, sheet_no: str, **kw) -> dict:
    return fx.sheet(sheet_no, TITLES[key], unique=key, marker=WORDS[key], **kw)


def version(tmp: Path, name: str, specs: list[dict]) -> dict:
    pdf = fx.make_pdf(tmp / f"{name}.pdf", specs)
    sha = hashlib.sha256(pdf.read_bytes()).hexdigest()
    dst = paths.source_pdf(OBJ, sha)
    dst.parent.mkdir(parents=True, exist_ok=True)
    if not dst.exists():
        shutil.copyfile(pdf, dst)
        os.chmod(dst, 0o444)
    profile.build(OBJ, sha)
    return {"version_id": name, "sha256": sha}


def row_of(res: dict, vid: str, page: int) -> dict:
    return next(r for r in res["rows"] if r["cells"][vid].get("page") == page)


def test_07_renumbering_keeps_sheets_in_their_rows(vc_root):
    a = version(vc_root, "A", [sheet(1, "1"), sheet(2, "2"), sheet(3, "3"), sheet(4, "4")])
    # В B листы 2 и 3 переставлены и перенумерованы.
    b = version(vc_root, "B", [sheet(1, "1"), sheet(3, "2"), sheet(2, "3"), sheet(4, "4")])
    res = lineage.build(OBJ, [a, b])
    assert len(res["rows"]) == 4
    assert row_of(res, "A", 2)["cells"]["B"]["page"] == 3
    assert row_of(res, "A", 3)["cells"]["B"]["page"] == 2
    assert all(c["state"] == "matched" for r in res["rows"] for c in r["cells"].values())


def test_08_added_and_removed_sheets_are_absent_cells(vc_root):
    a = version(vc_root, "A", [sheet(1, "1"), sheet(2, "2"), sheet(4, "3")])
    b = version(vc_root, "B", [sheet(1, "1"), sheet(2, "2"), sheet(5, "3")])
    res = lineage.build(OBJ, [a, b])
    removed = row_of(res, "A", 3)
    added = row_of(res, "B", 3)
    assert removed["cells"]["B"]["state"] == "absent"
    assert added["cells"]["A"]["state"] == "absent"
    assert removed is not added


def test_09_duplicate_sheet_is_kept_in_one_cell(vc_root):
    a = version(vc_root, "A", [sheet(1, "1"), sheet(2, "2")])
    b = version(vc_root, "B", [sheet(1, "1"), sheet(2, "2"), sheet(2, "2")])
    res = lineage.build(OBJ, [a, b])
    cell = row_of(res, "B", 2)["cells"]["B"]
    assert cell["dups"] == [3]
    assert res["unassigned"] == {}


def test_32_unmatched_candidate_is_not_removed(vc_root):
    a = version(vc_root, "A", [sheet(1, "1"), sheet(2, "2"), sheet(3, "3")])
    # Лист 3 в B сильно переработан: то же название и номер, но половина содержимого другая.
    reworked = fx.sheet("3", TITLES[3], unique=33, marker="кабельный лоток трасса " + WORDS[5])
    b = version(vc_root, "B", [sheet(1, "1"), sheet(2, "2"), reworked])
    res = lineage.build(OBJ, [a, b])
    row = row_of(res, "A", 3)
    cell = row["cells"]["B"]
    if cell["state"] == "matched":
        assert cell["confidence"] in ("medium", "high")
    else:
        assert cell["state"] == "unmatched", "не «удалён», если есть кандидат"
        assert cell["candidates"] and cell["candidates"][0]["page"] == 3


def test_manual_edits_move_detach_absent(vc_root):
    a = version(vc_root, "A", [sheet(1, "1"), sheet(2, "2"), sheet(3, "3")])
    b = version(vc_root, "B", [sheet(1, "1"), sheet(2, "2"), sheet(3, "3")])
    # Перенести B стр.3 в строку листа A стр.2 (ручная пара «л.2 ↔ л.3»).
    res = lineage.build(OBJ, [a, b], edits=[{"op": "move", "page": ["B", 3], "to_row_anchor": ["A", 2]}])
    row = row_of(res, "A", 2)
    assert row["cells"]["B"]["page"] == 3 and row["cells"]["B"]["manual"] is True
    displaced = row_of(res, "B", 2)
    assert displaced["cells"]["A"]["state"] in ("absent", "unmatched")
    # Отметить «листа нет» вручную.
    res2 = lineage.build(OBJ, [a, b], edits=[{"op": "absent", "row_anchor": ["A", 1], "version": "B"}])
    cell = row_of(res2, "A", 1)["cells"]["B"]
    assert cell["state"] == "absent" and cell.get("manual") is True
    assert any(r["cells"]["B"].get("page") == 1 and r["cells"]["A"]["state"] != "matched" for r in res2["rows"])


def _fake_pages(*nums):
    from types import SimpleNamespace
    return [SimpleNamespace(page=n, sheet_no="", designation="", kind="drawing", scan=False, title="", title_raw="")
            for n in nums]


def test_diverged_copies_keep_both_pairs(monkeypatch):
    """ВК2: в прошлой версии раздел повторён (39 и копия 86), в новой копии разошлись. Пара копии не затирает
    пару первичной страницы (иначе страница новой версии выпадает из матрицы), пары раздаются по положению."""
    import numpy as np
    feats = {"v1": _fake_pages(39, 86), "v2": _fake_pages(39, 86)}
    monkeypatch.setattr(lineage, "_duplicates", lambda pages: {86: 39} if pages is feats["v1"] else {})
    for crossed in (False, True):
        links = {0: (1, 0.95, {}), 1: (0, 0.9, {})} if crossed else {0: (0, 0.95, {}), 1: (1, 0.9, {})}
        monkeypatch.setattr(lineage, "_assign", lambda prev, cur, links=links: (
            links if len(prev) == 2 else {}, np.zeros((len(prev), len(cur)))))
        res = lineage.build(OBJ, [{"version_id": "v1"}, {"version_id": "v2"}], [], features=feats)
        pairs = {(r["cells"]["v1"]["page"], r["cells"]["v2"]["page"]) for r in res["rows"]}
        assert pairs == {(39, 39), (86, 86)} and res["unassigned"] == {}


def test_signature_changes_with_lineage_revision(monkeypatch):
    v = [{"version_id": "a", "sha256": "1"}, {"version_id": "b", "sha256": "2"}]
    before = lineage.signature(v)
    monkeypatch.setattr(lineage, "LINEAGE_REVISION", "next")
    assert lineage.signature(v) != before     # смена ревизии пересобирает сохранённые матрицы

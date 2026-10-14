"""API «Сравнения версий»: сценарий загрузка → матрица → пара → наложение (план §10.1 п. 17, 19, 20)."""
from __future__ import annotations

import hashlib
import os
import re
import stat
import time
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.routers import version_comparison as router_mod
from backend.app.services.version_comparison import jobs, paths
from tests import version_comparison_fixtures as fx
from tests.test_version_comparison_lineage import TITLES, WORDS

OBJ = "obj-api"
PACKAGE = Path(__file__).resolve().parents[1] / "backend" / "app" / "services" / "version_comparison"


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path / "vc"))
    monkeypatch.setenv("CPU_POOL_WORKERS", "1")
    app = FastAPI()
    app.include_router(router_mod.router)
    return TestClient(app)


def _sheet(key: int, no: str, **kw) -> dict:
    return fx.sheet(no, TITLES[key], unique=key, marker=WORDS[key], **kw)


def _versions(tmp: Path) -> list[Path]:
    v1 = fx.make_pdf(tmp / "src" / "ТЕСТ (1).pdf", [_sheet(1, "1"), _sheet(2, "2", release={"date": "01.02.2025"}),
                                                    _sheet(3, "3")])
    v2 = fx.make_pdf(tmp / "src" / "ТЕСТ (2).pdf", [_sheet(1, "1"), _sheet(2, "2", release={"date": "01.03.2025"}),
                                                    fx.sheet("3", TITLES[3], unique=3, marker=WORDS[3] + " Рр=324.26кВт")])
    v3 = fx.make_pdf(tmp / "src" / "ТЕСТ (3).pdf", [_sheet(1, "1"), _sheet(3, "2"), _sheet(2, "3"), _sheet(4, "4")])
    return [v1, v2, v3]


def _wait(fn, cond, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        data = fn()
        if cond(data):
            return data
        time.sleep(0.2)
    raise AssertionError(f"не дождались: {data}")


def _upload(client, files: list[Path]) -> tuple[str, dict]:
    uid = client.post(f"/api/version-comparison/objects/{OBJ}/uploads").json()["upload_id"]
    for f in files:
        r = client.post(f"/api/version-comparison/objects/{OBJ}/uploads/{uid}/files",
                        files={"file": (f.name, f.read_bytes(), "application/pdf")},
                        data={"relative_path": f"Папка/{f.name}"})
        assert r.status_code == 200, r.text
    prop = client.get(f"/api/version-comparison/objects/{OBJ}/uploads/{uid}/proposal").json()
    return uid, prop


def _setup_project(client, tmp_path) -> tuple[str, list[str], list[Path]]:
    files = _versions(tmp_path)
    uid, prop = _upload(client, files)
    assert len(prop["groups"]) == 1, "один шифр — один проект"
    group = prop["groups"][0]
    # Порядок задаёт пользователь: явно (1), (2), (3) по именам.
    ordered = sorted(group["files"], key=lambda f: f["file_name"])
    body = {"projects": [{"title": "Тест", "code": group["code"],
                          "files": [{"file_key": f["file_key"], "label": f["file_name"]} for f in ordered]}]}
    r = client.post(f"/api/version-comparison/objects/{OBJ}/uploads/{uid}/confirm", json=body)
    assert r.status_code == 200, r.text
    cat = client.get(f"/api/version-comparison/objects/{OBJ}/catalog").json()
    project = cat["projects"][0]
    return project["project_id"], project["versions"], files


def test_full_flow_matrix_pair_and_tiles(client, tmp_path):
    pid, vids, files = _setup_project(client, tmp_path)
    url = f"/api/version-comparison/objects/{OBJ}/projects/{pid}/matrix"
    mx = _wait(lambda: client.get(url).json(), lambda d: d["state"] in ("ready", "failed"))
    assert mx["state"] == "ready", mx
    assert [h["version_id"] for h in mx["versions"]] == vids
    # Лист 3 перенумерован в версии 3 (стр. 2), но остаётся в своей строке.
    row3 = next(r for r in mx["rows"] if r["cells"][vids[0]].get("page") == 3)
    assert row3["cells"][vids[2]]["page"] == 2
    new_row = next(r for r in mx["rows"] if r["cells"][vids[2]].get("page") == 4)
    assert new_row["cells"][vids[0]]["state"] == "absent"

    # Пара 1 → 2, все листы.
    run = client.post(f"/api/version-comparison/objects/{OBJ}/projects/{pid}/runs",
                      json={"a_version": vids[0], "b_version": vids[1], "scope": "all"}).json()
    run = _wait(lambda: client.get(f"/api/version-comparison/objects/{OBJ}/runs/{run['run_id']}").json(),
                lambda d: d["state"] in ("done", "failed", "cancelled"))
    assert run["state"] == "done" and run["a"]["version_id"] == vids[0] and run["b"]["version_id"] == vids[1]
    by_page = {it["a_page"]: it for it in run["items"]}
    assert by_page[1]["result_status"] == "same"
    assert by_page[2]["result_status"] == "same_zones_changed"   # только дата штампа выпуска
    assert by_page[3]["result_status"] == "different"            # цифра Рр
    res = client.get(f"/api/version-comparison/objects/{OBJ}/runs/{run['run_id']}/sheets/{by_page[3]['spid']}").json()
    assert res["result"]["status_label"] == "Есть отличия"
    assert any(rg["class"] == "text" for rg in res["result"]["regions"])
    base = f"/api/version-comparison/objects/{OBJ}/runs/{run['run_id']}/sheets/{by_page[3]['spid']}"
    for mode in ("overlay", "a", "b"):
        t = client.get(f"{base}/tile", params={"mode": mode, "level": 1, "x": 0, "y": 0})
        assert t.status_code == 200 and t.headers["content-type"] == "image/png" and t.content[:4] == b"\x89PNG"
    assert client.get(f"{base}/preview.png").status_code == 200
    thumb = client.get(f"/api/version-comparison/objects/{OBJ}/sources/{run['a']['sha256']}/pages/1/thumb")
    assert thumb.status_code == 200


def test_17_non_adjacent_and_manual_sheet_pair(client, tmp_path):
    pid, vids, _ = _setup_project(client, tmp_path)
    _wait(lambda: client.get(f"/api/version-comparison/objects/{OBJ}/projects/{pid}/matrix").json(),
          lambda d: d["state"] == "ready")
    # Несоседняя пара 1 → 3 и ручная пара «лист 2 ↔ лист 3» (стр. 2 → стр. 3).
    run = client.post(f"/api/version-comparison/objects/{OBJ}/projects/{pid}/runs",
                      json={"a_version": vids[0], "b_version": vids[2], "scope": "manual",
                            "sheet_pairs": [{"a_page": 2, "b_page": 3}]}).json()
    run = _wait(lambda: client.get(f"/api/version-comparison/objects/{OBJ}/runs/{run['run_id']}").json(),
                lambda d: d["state"] == "done")
    assert len(run["items"]) == 1
    item = run["items"][0]
    assert (item["a_page"], item["b_page"], item["confidence"]) == (2, 3, "manual")
    assert item["result_status"] in ("same", "same_zones_changed")


def test_19_sources_are_immutable_copies(client, tmp_path):
    pid, vids, files = _setup_project(client, tmp_path)
    before = {f.name: hashlib.sha256(f.read_bytes()).hexdigest() for f in files}
    _wait(lambda: client.get(f"/api/version-comparison/objects/{OBJ}/projects/{pid}/matrix").json(),
          lambda d: d["state"] == "ready")
    run = client.post(f"/api/version-comparison/objects/{OBJ}/projects/{pid}/runs",
                      json={"a_version": vids[0], "b_version": vids[2], "scope": "all"}).json()
    _wait(lambda: client.get(f"/api/version-comparison/objects/{OBJ}/runs/{run['run_id']}").json(),
          lambda d: d["state"] == "done")
    assert {f.name: hashlib.sha256(f.read_bytes()).hexdigest() for f in files} == before
    for src in paths.sources_dir(OBJ).glob("*.pdf"):
        assert hashlib.sha256(src.read_bytes()).hexdigest() == src.stem
        assert not (src.stat().st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH))


def test_19_package_never_saves_pdf_or_uses_network():
    forbidden = re.compile(r"\.save\(|saveIncr|ez_save|\.tobytes\(|requests|httpx|urllib|openai|anthropic|socket")
    for py in PACKAGE.glob("*.py"):
        text = py.read_text(encoding="utf-8")
        hits = [m.group(0) for m in forbidden.finditer(text)]
        assert not hits, f"{py.name}: {hits}"


def test_20_cancel_and_resume(client, tmp_path, monkeypatch):
    pid, vids, _ = _setup_project(client, tmp_path)
    _wait(lambda: client.get(f"/api/version-comparison/objects/{OBJ}/projects/{pid}/matrix").json(),
          lambda d: d["state"] == "ready")
    monkeypatch.setenv("VERSION_COMPARISON_WORKERS", "1")
    real = jobs.engine.compute_sheet_pair

    def slow(task):
        time.sleep(0.6)
        return real(task)
    monkeypatch.setattr(jobs.engine, "compute_sheet_pair", slow)
    run = client.post(f"/api/version-comparison/objects/{OBJ}/projects/{pid}/runs",
                      json={"a_version": vids[0], "b_version": vids[2], "scope": "all"}).json()
    rid = run["run_id"]
    time.sleep(0.2)
    client.post(f"/api/version-comparison/objects/{OBJ}/runs/{rid}/cancel")
    run = _wait(lambda: client.get(f"/api/version-comparison/objects/{OBJ}/runs/{rid}").json(),
                lambda d: d["state"] in ("cancelled", "done"))
    assert run["state"] == "cancelled"
    done_before = [it["spid"] for it in run["items"] if it["kind"] == "compare" and it["status"] == "done"]
    assert any(it["status"] == "cancelled" for it in run["items"])
    monkeypatch.setattr(jobs.engine, "compute_sheet_pair", real)
    client.post(f"/api/version-comparison/objects/{OBJ}/runs/{rid}/resume")
    run = _wait(lambda: client.get(f"/api/version-comparison/objects/{OBJ}/runs/{rid}").json(),
                lambda d: d["state"] == "done")
    assert all(it["status"] == "done" for it in run["items"] if it["kind"] == "compare")
    for it in run["items"]:
        if it["spid"] in done_before:
            assert it["status"] == "done"


def test_reorder_and_labels_do_not_change_sources(client, tmp_path):
    pid, vids, _ = _setup_project(client, tmp_path)
    r = client.put(f"/api/version-comparison/objects/{OBJ}/projects/{pid}/order", json={"version_ids": vids[::-1]})
    assert r.status_code == 200
    cat = client.get(f"/api/version-comparison/objects/{OBJ}/catalog").json()
    assert cat["projects"][0]["versions"] == vids[::-1]
    bad = client.put(f"/api/version-comparison/objects/{OBJ}/projects/{pid}/order", json={"version_ids": vids[:2]})
    assert bad.status_code == 400
    r = client.patch(f"/api/version-comparison/objects/{OBJ}/versions/{vids[0]}", json={"label": "Изм.1 от 01.02"})
    assert r.status_code == 200
    cat = client.get(f"/api/version-comparison/objects/{OBJ}/catalog").json()
    assert cat["projects"][0]["version_records"][-1]["user_label"] == "Изм.1 от 01.02"


def _counting(monkeypatch):
    calls = []
    real = jobs.engine.compute_sheet_pair

    def counted(task):
        calls.append((task["a"]["page"], task["b"]["page"]))
        return real(task)
    monkeypatch.setattr(jobs.engine, "compute_sheet_pair", counted)
    return calls


def test_series_all_adjacent_summary_and_cache(client, tmp_path, monkeypatch):
    pid, vids, _ = _setup_project(client, tmp_path)
    _wait(lambda: client.get(f"/api/version-comparison/objects/{OBJ}/projects/{pid}/matrix").json(),
          lambda d: d["state"] == "ready")
    calls = _counting(monkeypatch)
    base_url = f"/api/version-comparison/objects/{OBJ}"
    ser = client.post(f"{base_url}/projects/{pid}/series").json()
    assert [(p["a"], p["b"]) for p in ser["pairs"]] == [(vids[0], vids[1]), (vids[1], vids[2])]
    ser = _wait(lambda: client.get(f"{base_url}/series/{ser['series_id']}").json(),
                lambda d: d["state"] in ("done", "failed", "cancelled", "interrupted"))
    assert ser["state"] == "done" and ser["progress"] == {"total": 2, "done": 2}
    s12, s23 = ser["pairs"][0]["summary"], ser["pairs"][1]["summary"]
    # 1 → 2: лист 3 — цифра Рр, лист 2 — только дата выпуска.
    assert (s12["different"], s12["zones_only"], s12["same"]) == (1, 1, 1)
    assert s12["verdict"] == ["есть отличия на 1 л."]
    # 2 → 3: листы переставлены (без отличий), добавлен лист 4.
    assert s23["added"] == 1 and "изменён состав (+1 / −0 л.)" in s23["verdict"]
    first = len(calls)
    assert first > 0
    # Повтор серии без изменений — из кэша, без единого расчёта пары.
    again = client.post(f"{base_url}/projects/{pid}/series").json()
    again = _wait(lambda: client.get(f"{base_url}/series/{again['series_id']}").json(),
                  lambda d: d["state"] in ("done", "failed", "cancelled", "interrupted"))
    assert again["state"] == "done" and len(calls) == first
    run = client.get(f"{base_url}/runs/{again['pairs'][0]['run_id']}").json()
    assert all(it.get("cached") for it in run["items"] if it["kind"] == "compare")
    listed = client.get(f"{base_url}/projects/{pid}/series").json()["series"]
    assert {s["series_id"] for s in listed} == {ser["series_id"], again["series_id"]}


def test_20_link_edit_recomputes_only_the_affected_pair(client, tmp_path, monkeypatch):
    pid, vids, _ = _setup_project(client, tmp_path)
    murl = f"/api/version-comparison/objects/{OBJ}/projects/{pid}/matrix"
    _wait(lambda: client.get(murl).json(), lambda d: d["state"] == "ready")
    calls = _counting(monkeypatch)

    def run_all():
        run = client.post(f"/api/version-comparison/objects/{OBJ}/projects/{pid}/runs",
                          json={"a_version": vids[0], "b_version": vids[2], "scope": "all"}).json()
        return _wait(lambda: client.get(f"/api/version-comparison/objects/{OBJ}/runs/{run['run_id']}").json(),
                     lambda d: d["state"] == "done")
    run_all()
    before = list(calls)
    # Вручную: лист 2 версии 1 связан со стр. 4 версии 3 (вытесненная стр. 3 — в новую строку).
    r = client.post(f"/api/version-comparison/objects/{OBJ}/projects/{pid}/links",
                    json={"op": "move", "page": [vids[2], 4], "to_row_anchor": [vids[0], 2]})
    assert r.status_code == 200, r.text
    _wait(lambda: client.get(murl).json(), lambda d: d["state"] == "ready")
    run = run_all()
    assert calls[len(before):] == [(2, 4)], "пересчитана только затронутая пара"
    assert any(it["kind"] == "compare" and (it["a_page"], it["b_page"]) == (2, 4) for it in run["items"])


def test_series_cancel_and_resume_computes_only_missing(client, tmp_path, monkeypatch):
    pid, vids, _ = _setup_project(client, tmp_path)
    _wait(lambda: client.get(f"/api/version-comparison/objects/{OBJ}/projects/{pid}/matrix").json(),
          lambda d: d["state"] == "ready")
    monkeypatch.setenv("VERSION_COMPARISON_WORKERS", "1")
    seen = []
    real = jobs.engine.compute_sheet_pair

    def slow(task):
        seen.append((task["a"]["version_id"], task["a"]["page"], task["b"]["version_id"], task["b"]["page"]))
        time.sleep(0.4)
        return real(task)
    monkeypatch.setattr(jobs.engine, "compute_sheet_pair", slow)
    base_url = f"/api/version-comparison/objects/{OBJ}"
    ser = client.post(f"{base_url}/projects/{pid}/series").json()
    sid = ser["series_id"]
    _wait(lambda: client.get(f"{base_url}/series/{sid}").json(), lambda d: len(seen) >= 1, timeout=60)
    client.post(f"{base_url}/series/{sid}/cancel")
    ser = _wait(lambda: client.get(f"{base_url}/series/{sid}").json(),
                lambda d: d["state"] in ("cancelled", "done"))
    assert ser["state"] == "cancelled"
    assert "переход досчитан не полностью" in ser["pairs"][0]["summary"]["verdict"] or ser["pairs"][1]["run_id"] is None
    client.post(f"{base_url}/series/{sid}/resume")
    ser = _wait(lambda: client.get(f"{base_url}/series/{sid}").json(), lambda d: d["state"] in ("done", "failed"))
    assert ser["state"] == "done" and all(p["run_state"] == "done" for p in ser["pairs"])
    assert len(seen) == len(set(seen)), "готовые пары листов не пересчитываются"


def test_spec_sheets_get_status_by_positions(client, tmp_path):
    from tests.test_version_comparison_spec import CABLE, _filler, _sub
    rows1 = [{"section": "Кабельно-проводниковая продукция"}, CABLE, _sub("3х2.5", "3350")] \
        + _filler(fx.SO_ROWS_PER_PAGE - 4) + [_sub("5х25", "245")]
    rows1_b = [{"section": "Кабельно-проводниковая продукция"}, CABLE, _sub("3х2.5", "3400"), _sub("4х4", "50")] \
        + _filler(fx.SO_ROWS_PER_PAGE - 4)
    v1 = fx.make_pdf(tmp_path / "src" / "СО (1).pdf", [
        fx.sheet("1", "Спецификация оборудования", so_rows=rows1, marker="альфа бета"),
        fx.sheet("2", "Спецификация оборудования", so_rows=[_sub("5х35", "335")], marker="гамма дельта")])
    v2 = fx.make_pdf(tmp_path / "src" / "СО (2).pdf", [
        fx.sheet("1", "Спецификация оборудования", so_rows=rows1_b, marker="альфа бета"),
        fx.sheet("2", "Спецификация оборудования", so_rows=[_sub("5х25", "245"), _sub("5х35", "335")], marker="гамма дельта")])
    uid, prop = _upload(client, [v1, v2])
    group = prop["groups"][0]
    ordered = sorted(group["files"], key=lambda f: f["file_name"])
    body = {"projects": [{"title": "СО", "code": group["code"],
                          "files": [{"file_key": f["file_key"], "label": f["file_name"]} for f in ordered]}]}
    assert client.post(f"/api/version-comparison/objects/{OBJ}/uploads/{uid}/confirm", json=body).status_code == 200
    project = client.get(f"/api/version-comparison/objects/{OBJ}/catalog").json()["projects"][0]
    pid, vids = project["project_id"], project["versions"]
    _wait(lambda: client.get(f"/api/version-comparison/objects/{OBJ}/projects/{pid}/matrix").json(),
          lambda d: d["state"] == "ready")
    run = client.post(f"/api/version-comparison/objects/{OBJ}/projects/{pid}/runs",
                      json={"a_version": vids[0], "b_version": vids[1], "scope": "all"}).json()
    run = _wait(lambda: client.get(f"/api/version-comparison/objects/{OBJ}/runs/{run['run_id']}").json(),
                lambda d: d["state"] == "done")
    by_page = {it["a_page"]: it for it in run["items"] if it["kind"] == "compare"}
    assert by_page[1]["spec"]["basis"] == "positions" and by_page[1]["result_status"] == "different"
    # Стр. 2: позиция 5х25 переехала сюда со стр. 1 — по позициям изменений нет.
    assert by_page[2]["spec"]["changes"] == 0 and by_page[2]["result_status"] in ("same", "same_zones_changed")
    assert by_page[2]["overlay_status"] == "different", "постраничное наложение при этом различается"
    sp = client.get(f"/api/version-comparison/objects/{OBJ}/runs/{run['run_id']}/spec").json()
    assert sp["available"] and sp["moved_between_pages"] >= 1
    assert sorted(c["kind"] for c in sp["changes"] if c["kind"] != "renumbered") == ["added", "changed"]
    res = client.get(f"/api/version-comparison/objects/{OBJ}/runs/{run['run_id']}/sheets/{by_page[1]['spid']}").json()
    assert res["result"]["status"] == "different" and res["result"]["channels"]["spec_positions"]["state"] == "checked"
    assert any(c["fields"].get("qty") == ["3350", "3400"] for c in res["result"]["spec_page"]["items"])


def test_15_split_sheet_and_floor_plans_are_parts(client, tmp_path):
    v1 = fx.make_pdf(tmp_path / "src" / "ДЛ (1).pdf", [
        fx.sheet("4", "Схема ВРУ-1", split="whole"),
        fx.sheet("19", "План 2-4 этажей", unique=7, marker=WORDS[4])])
    v2 = fx.make_pdf(tmp_path / "src" / "ДЛ (2).pdf", [
        fx.sheet("4", "Схема ВРУ-1. Начало", split="left"),
        fx.sheet("5", "Схема ВРУ-1. Окончание", split="right"),
        fx.sheet("20", "План 2 этажа", unique=7, marker=WORDS[4]),
        fx.sheet("21", "План 3 этажа", unique=7, marker=WORDS[4]),
        fx.sheet("22", "План 4 этажа", unique=7, marker=WORDS[4])])
    uid, prop = _upload(client, [v1, v2])
    group = prop["groups"][0]
    ordered = sorted(group["files"], key=lambda f: f["file_name"])
    body = {"projects": [{"title": "ДЛ", "code": group["code"],
                          "files": [{"file_key": f["file_key"], "label": f["file_name"]} for f in ordered]}]}
    assert client.post(f"/api/version-comparison/objects/{OBJ}/uploads/{uid}/confirm", json=body).status_code == 200
    project = client.get(f"/api/version-comparison/objects/{OBJ}/catalog").json()["projects"][0]
    pid, vids = project["project_id"], project["versions"]
    mx = _wait(lambda: client.get(f"/api/version-comparison/objects/{OBJ}/projects/{pid}/matrix").json(),
               lambda d: d["state"] == "ready")
    assert sorted((p_["a_page"], p_["b_page"], p_["kind"]) for p_ in mx["parts"]) == \
        [(1, 1, "split"), (1, 2, "split"), (2, 3, "floors"), (2, 4, "floors"), (2, 5, "floors")]
    run = client.post(f"/api/version-comparison/objects/{OBJ}/projects/{pid}/runs",
                      json={"a_version": vids[0], "b_version": vids[1], "scope": "all"}).json()
    run = _wait(lambda: client.get(f"/api/version-comparison/objects/{OBJ}/runs/{run['run_id']}").json(),
                lambda d: d["state"] == "done")
    parts_ = {(it["a_page"], it["b_page"]): it for it in run["items"] if it.get("part")}
    assert set(parts_) == {(1, 1), (1, 2), (2, 3), (2, 4), (2, 5)}
    assert not [it for it in run["items"] if it["kind"] in ("only_a", "only_b")], "части — не «удалён/добавлен»"
    for key in ((1, 1), (1, 2)):
        assert parts_[key]["result_status"] in ("same", "same_zones_changed"), (key, parts_[key])
        assert parts_[key]["part_coverage"] >= 0.95
    assert all(parts_[k]["result_status"] in ("same", "same_zones_changed") for k in ((2, 3), (2, 4), (2, 5)))
    summary = {e["a_page"]: e for e in run["parts"]}
    assert summary[1]["kind"] == "split" and summary[1]["b_pages"] == [1, 2] and summary[1]["coverage"] >= 0.95
    assert summary[2]["kind"] == "floors" and summary[2]["b_pages"] == [3, 4, 5]

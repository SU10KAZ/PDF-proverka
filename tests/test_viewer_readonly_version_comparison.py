"""«Сравнение версий» для viewer: просмотр и выгрузки без записи и запусков (план доступа §5.3, §5.4).

Хранилище — временная папка; модели, Codex и очередь вычислителя не вызываются.
Чтение viewer сверяется снимком дерева хранилища (содержимое и mtime файлов) до и после.
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.core import access
from backend.app.services.common.atomic_json import atomic_write_json
from backend.app.services.version_comparison import jobs, paths, series, store, work_queue

OBJ = "obj-ro"
PID = "p1"
DEAD = {"pid": 99999999, "start": "0", "boot": "old"}


@pytest.fixture
def root(tmp_path, monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path / "vc"))
    monkeypatch.delenv("VERSION_COMPARISON_EXECUTOR", raising=False)
    paths.object_dir(OBJ).mkdir(parents=True)
    return tmp_path / "vc"


@pytest.fixture
def viewer(monkeypatch):
    """Контекст запроса viewer в режиме аккаунтов (как его ставит middleware)."""
    monkeypatch.setenv("PORTAL_AUTH_ENABLED", "true")
    monkeypatch.setenv("PORTAL_AUTH_MODE", "accounts")
    ctx = access.AccountContext(mode=access.MODE_ACCOUNTS, login="reader", role=access.ROLE_VIEWER, account_id=2)
    token = access.bind_current(ctx)
    assert access.read_only_request()
    yield ctx
    access.unbind_current(token)


def snapshot(base: Path) -> dict[str, tuple[str, int]]:
    return {str(p.relative_to(base)): (hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns)
            for p in sorted(base.rglob("*")) if p.is_file()}


def _catalog(shas=("a" * 64, "b" * 64)) -> list[dict]:
    versions = {f"v{i}": {"version_id": f"v{i}", "sha256": sha, "file_name": f"ТЕСТ ({i}).pdf"}
                for i, sha in enumerate(shas, 1)}
    cat = {"schema": "version_comparison.catalog/v1", "object_id": OBJ,
           "projects": [{"project_id": PID, "title": "Тест", "code": "ТЕСТ-01", "versions": list(versions)}],
           "versions": versions}
    atomic_write_json(paths.catalog_path(OBJ), cat)
    return list(versions.values())


def _profile(sha: str) -> None:
    atomic_write_json(paths.profile_dir(OBJ, sha) / "pages.json",
                      {"page_count": 1, "pages": [{"page": 1}], "summary": {}})


class _Mine:
    def submit(self, *_a, **_k):
        raise AssertionError("viewer запустил фоновую задачу")


def _no_jobs(monkeypatch):
    monkeypatch.setattr(jobs, "_threads", _Mine())
    monkeypatch.setattr(jobs, "_run_threads", _Mine())
    monkeypatch.setattr(jobs, "ensure_profiles", lambda *a, **k: (_ for _ in ()).throw(
        AssertionError("viewer запустил разбор версий")))


# ------------------------------------------------------------------ матрица без запуска

def test_matrix_view_profiles_missing_is_not_prepared_and_starts_nothing(root, monkeypatch):
    _catalog()
    _no_jobs(monkeypatch)
    before = snapshot(root)
    out = jobs.matrix_view(OBJ, PID)
    assert out["state"] == "not_prepared" and out["rows"] == [] and out["reason"] == "profiles"
    assert snapshot(root) == before
    monkeypatch.setitem(jobs._profile_state, (OBJ, "a" * 64), {"state": "running"})
    assert jobs.matrix_view(OBJ, PID)["state"] == "building"
    assert snapshot(root) == before


def test_matrix_view_lineage_states_and_ready_matches_admin_matrix(root, monkeypatch):
    versions = _catalog()
    for v in versions:
        _profile(v["sha256"])
    _no_jobs(monkeypatch)
    before = snapshot(root)
    out = jobs.matrix_view(OBJ, PID)
    assert out["state"] == "not_prepared" and out["rows"] == [] and "reason" not in out
    sig = jobs._signature(versions, [])
    monkeypatch.setitem(jobs._lineage_state, (OBJ, PID), {"state": "building", "signature": sig})
    assert jobs.matrix_view(OBJ, PID)["state"] == "building"
    monkeypatch.setitem(jobs._lineage_state, (OBJ, PID), {"state": "building", "signature": "старая"})
    assert jobs.matrix_view(OBJ, PID)["state"] == "not_prepared"
    assert snapshot(root) == before
    # Сопоставление построено администратором: viewer видит те же строки, что и админская матрица.
    rows = [{"row_id": "r1", "cells": {}}]
    atomic_write_json(paths.lineage_path(OBJ, PID), {"edits": [], "signature": sig, "built_at": "t",
                                                      "result": {"rows": rows, "unassigned": {}, "duplicates": {}}})
    monkeypatch.delitem(jobs._lineage_state, (OBJ, PID))
    before = snapshot(root)
    view = jobs.matrix_view(OBJ, PID)
    assert view["state"] == "ready" and view["rows"] == rows
    assert snapshot(root) == before
    monkeypatch.setattr(jobs, "ensure_profiles", lambda *a, **k: None)
    assert jobs.matrix(OBJ, PID) == view
    # Устаревшая подпись (добавлена версия) — снова «не подготовлено», без перестройки.
    _no_jobs(monkeypatch)
    _catalog(("a" * 64, "b" * 64, "c" * 64))
    _profile("c" * 64)
    assert jobs.matrix_view(OBJ, PID)["state"] == "not_prepared"


def test_matrix_view_route(root, monkeypatch):
    from backend.app.api.routers import version_comparison as router_mod
    _catalog()
    _no_jobs(monkeypatch)
    app = FastAPI()
    app.include_router(router_mod.router)
    r = TestClient(app).get(f"/api/version-comparison/objects/{OBJ}/projects/{PID}/matrix-view")
    assert r.status_code == 200 and r.json()["state"] == "not_prepared"


# ------------------------------------------------------------------ «ремонт» статуса только у admin

def _active_run(run_id: str = "run1", state: str = "running") -> Path:
    run = {"run_id": run_id, "object_id": OBJ, "project_id": PID, "state": state, "owner": "gone",
           "owner_process": DEAD, "scope": "all",
           "a": {"version_id": "v1", "label": "A", "sha256": "a" * 64},
           "b": {"version_id": "v2", "label": "B", "sha256": "b" * 64},
           "items": [{"spid": "s1", "kind": "compare", "status": "running", "a_page": 1, "b_page": 1}],
           "progress": {"total": 1, "done": 0}}
    path = jobs._run_path(OBJ, run_id)
    atomic_write_json(path, run)
    return path


@pytest.mark.parametrize("state,expected", [("running", "interrupted"), ("cancelling", "cancelled"),
                                            ("pausing", "paused")])
def test_get_run_without_repair_reports_state_but_writes_nothing(root, monkeypatch, state, expected):
    monkeypatch.setenv("VERSION_COMPARISON_EXECUTOR", "worker")
    monkeypatch.setattr(work_queue, "heartbeat", lambda: {"online": False})
    cancelled = []
    monkeypatch.setattr(work_queue, "cancel_run", lambda rid: cancelled.append(rid))
    path = _active_run(state=state)
    before = snapshot(root)
    run = jobs.get_run(OBJ, "run1", repair=False)
    assert run["state"] == expected
    assert [r["run_id"] for r in jobs.list_runs(OBJ, PID, repair=False)] == ["run1"]
    jobs.sheet_result(OBJ, "run1", "s1", repair=False)
    assert snapshot(root) == before and cancelled == []
    # Админское чтение по-прежнему чинит статус (регрессия).
    assert jobs.get_run(OBJ, "run1")["state"] == expected
    assert json.loads(path.read_text(encoding="utf-8"))["state"] == expected
    assert cancelled == (["run1"] if state == "cancelling" else [])


def test_viewer_context_never_repairs_even_with_default_argument(root, viewer):
    path = _active_run()
    before = snapshot(root)
    assert jobs.get_run(OBJ, "run1")["state"] == "interrupted"
    assert snapshot(root) == before
    assert json.loads(path.read_text(encoding="utf-8"))["state"] == "running"


def test_series_get_without_repair(root):
    _active_run()
    data = {"series_id": "ser1", "object_id": OBJ, "project_id": PID, "state": "running", "owner": "gone",
            "owner_process": DEAD, "created_at": "t", "pairs": [{"a": "v1", "b": "v2", "run_id": "run1",
                                                                 "state": "running"}]}
    spath = paths.series_path(OBJ, "ser1")
    atomic_write_json(spath, data)
    before = snapshot(root)
    got = series.get(OBJ, "ser1", repair=False)
    assert got["state"] == "interrupted" and got["pairs"][0]["run_state"] == "interrupted"
    assert [s["series_id"] for s in series.list_series(OBJ, PID, repair=False)] == ["ser1"]
    assert snapshot(root) == before
    assert series.get(OBJ, "ser1")["state"] == "interrupted"
    assert json.loads(spath.read_text(encoding="utf-8"))["state"] == "interrupted"
    assert json.loads(jobs._run_path(OBJ, "run1").read_text(encoding="utf-8"))["state"] == "interrupted"


def test_ai_job_get_without_repair(root):
    from backend.app.services.version_comparison_ai import jobs as ai_jobs
    _active_run(state="done")
    job = {"job_id": "job1", "state": "running", "owner": "gone", "sheets": [], "created_at": "t"}
    jpath = ai_jobs._job_path(OBJ, "run1", "job1")
    atomic_write_json(jpath, job)
    before = snapshot(root)
    assert ai_jobs.get_job(OBJ, "run1", "job1", repair=False)["state"] == "interrupted"
    assert ai_jobs.list_jobs(OBJ, "run1", repair=False)[0]["state"] == "interrupted"
    assert ai_jobs.latest_job(OBJ, "run1", repair=False)["job_id"] == "job1"
    assert snapshot(root) == before
    assert ai_jobs.get_job(OBJ, "run1", "job1")["state"] == "interrupted"
    assert json.loads(jpath.read_text(encoding="utf-8"))["state"] == "interrupted"


def _md_report(state: str = "done", pdf: bytes | None = b"%PDF old") -> Path:
    from backend.app.services.version_comparison_ai.md_report import runner
    rdir = runner.report_dir(OBJ, "mr1")
    rdir.mkdir(parents=True)
    atomic_write_json(rdir / "job.json", {"report_id": "mr1", "object_id": OBJ, "state": state, "stage": "pdf",
                                          "owner": "gone", "created_at": "2026-10-19T10:00:00+00:00",
                                          "source": {"sha256": "c" * 64, "stored_name": "s.md",
                                                     "project_code": "ТЕСТ"}})
    atomic_write_json(rdir / "report.json", {"report": "проверенный"})
    atomic_write_json(rdir / "checks.json", {"report_errors": [], "consistency": []})
    if pdf is not None:
        (rdir / "report.pdf").write_bytes(pdf)
    return rdir


def test_md_report_get_without_repair(root):
    from backend.app.services.version_comparison_ai.md_report import runner
    rdir = _md_report(state="running", pdf=None)
    before = snapshot(root)
    assert runner.get(OBJ, "mr1", repair=False)["state"] == "interrupted"
    assert runner.list_reports(OBJ, repair=False)[0]["state"] == "interrupted"
    assert snapshot(root) == before
    assert runner.get(OBJ, "mr1")["state"] == "interrupted"
    assert json.loads((rdir / "job.json").read_text(encoding="utf-8"))["state"] == "interrupted"


def test_sheet_match_status_without_repair(root):
    from backend.app.services.version_comparison_ai import sheet_match
    p = sheet_match.job_path(OBJ)
    atomic_write_json(p, {"state": "running", "owner": "gone", "owner_process": DEAD})
    before = snapshot(root)
    assert sheet_match.status(OBJ, repair=False)["state"] == "interrupted"
    assert snapshot(root) == before
    assert sheet_match.status(OBJ)["state"] == "interrupted"
    assert json.loads(p.read_text(encoding="utf-8"))["state"] == "interrupted"


# ------------------------------------------------------------------ маршруты: миниатюры и отчёты прогона

@pytest.fixture
def vc_client(root, monkeypatch):
    from backend.app.api.routers import version_comparison as router_mod
    app = FastAPI()
    app.include_router(router_mod.router)
    return TestClient(app), router_mod


@pytest.mark.parametrize("asked,viewer_w", [(64, 160), (220, 320), (320, 320), (321, 640), (1500, 2400),
                                            (2400, 2400)])
def test_thumb_width_quantized_for_viewer_only(vc_client, monkeypatch, asked, viewer_w):
    client, router_mod = vc_client
    from backend.app.services.version_comparison import profile
    sha = "a" * 64
    pdf = paths.source_pdf(OBJ, sha)
    pdf.parent.mkdir(parents=True, exist_ok=True)
    pdf.write_bytes(b"%PDF")
    png = paths.object_dir(OBJ) / "t.png"
    png.write_bytes(b"png")
    widths = []
    monkeypatch.setattr(profile, "thumbnail", lambda o, s, p, w: widths.append(w) or png)
    url = f"/api/version-comparison/objects/{OBJ}/sources/{sha}/pages/1/thumb?w={asked}"
    assert client.get(url).status_code == 200
    monkeypatch.setattr(router_mod, "read_only_request", lambda: True)
    assert client.get(url).status_code == 200
    assert widths == [asked, viewer_w]


def _done_run_with_reports() -> Path:
    path = _active_run(state="done")
    out = jobs._reports_dir(OBJ, "run1")
    out.mkdir(parents=True)
    (out / "report.xlsx").write_bytes(b"XLSX-old")
    (out / "report_html.zip").write_bytes(b"ZIP-old")
    return path


def test_report_file_409_during_rebuild_and_whole_file_otherwise(vc_client, monkeypatch):
    client, router_mod = vc_client
    _done_run_with_reports()
    url = f"/api/version-comparison/objects/{OBJ}/runs/run1/reports/report.xlsx"
    monkeypatch.setitem(jobs._report_state, "run1", {"state": "building"})
    r = client.get(url)
    assert r.status_code == 409 and "пересобирается" in r.json()["detail"]
    monkeypatch.setitem(jobs._report_state, "run1", {"state": "ready"})
    r = client.get(url)
    assert r.status_code == 200 and r.content == b"XLSX-old"
    (jobs._reports_dir(OBJ, "run1") / "report_html.zip").unlink()
    assert client.get(url).status_code == 400            # пара отчётов неполная — не отдаём


def test_report_job_replaces_files_atomically_and_failure_keeps_old_pair(root, monkeypatch):
    from backend.app.services.version_comparison import report
    _done_run_with_reports()
    out = jobs._reports_dir(OBJ, "run1")
    monkeypatch.setattr(report, "collect", lambda oid, run: {"run": run["run_id"]})
    monkeypatch.setattr(report, "build_xlsx", lambda data: b"XLSX-new")
    monkeypatch.setattr(report, "build_html_zip", lambda oid, data: (_ for _ in ()).throw(RuntimeError("сбой")))
    jobs._report_job(OBJ, "run1")
    assert jobs._report_state.pop("run1")["state"] == "failed"
    assert (out / "report.xlsx").read_bytes() == b"XLSX-old" and (out / "report_html.zip").read_bytes() == b"ZIP-old"
    monkeypatch.setattr(report, "build_html_zip", lambda oid, data: b"ZIP-new")
    jobs._report_job(OBJ, "run1")
    assert jobs._report_state.pop("run1")["state"] == "ready"
    assert (out / "report.xlsx").read_bytes() == b"XLSX-new" and (out / "report_html.zip").read_bytes() == b"ZIP-new"
    assert sorted(p.name for p in out.iterdir()) == ["report.xlsx", "report_html.zip"]   # временных нет


def test_viewer_run_routes_do_not_repair(vc_client, monkeypatch):
    client, router_mod = vc_client
    path = _active_run()
    monkeypatch.setattr(router_mod, "read_only_request", lambda: True)
    before = snapshot(paths.root())
    base = f"/api/version-comparison/objects/{OBJ}"
    assert client.get(f"{base}/runs/run1").json()["state"] == "interrupted"
    assert client.get(f"{base}/projects/{PID}/runs").json()["runs"][0]["state"] == "interrupted"
    assert client.get(f"{base}/runs/run1/sheets/s1").status_code == 200
    assert snapshot(paths.root()) == before
    assert json.loads(path.read_text(encoding="utf-8"))["state"] == "running"


# ------------------------------------------------------------------ PDF MD-отчёта

def _fake_pdf(monkeypatch, *, ok: bool = True, gate: threading.Event | None = None, started=None):
    from backend.app.services.version_comparison_ai.md_report import pdf
    names = []

    def render(report, tmp, **kw):
        names.append(Path(tmp).name)
        if started is not None:
            started.set()
        if gate is not None:
            assert gate.wait(10)
        Path(tmp).write_bytes(b"%PDF new")

    monkeypatch.setattr(pdf, "render", render)
    monkeypatch.setattr(pdf, "check", lambda tmp, report, preview, forbidden=(): {"ok": ok, "pages": 1, "words": 1,
                                                                                  "problems": [] if ok else ["шрифт"]})
    return names


def test_md_report_failed_rebuild_keeps_done_and_previous_pdf(root, monkeypatch):
    from backend.app.services.version_comparison_ai.md_report import runner
    rdir = _md_report()
    _fake_pdf(monkeypatch, ok=False)
    j = runner.rebuild_pdf(OBJ, "mr1")
    assert j["state"] == "done" and j["pdf_ready"] and "шрифт" in j["pdf_rebuild_error"]["error"]
    assert (rdir / "report.pdf").read_bytes() == b"%PDF old"
    assert runner.file_path(OBJ, "mr1", "report.pdf").read_bytes() == b"%PDF old"
    assert not any(p.name.endswith(".pdf") and p.name != "report.pdf" for p in rdir.iterdir())
    names = _fake_pdf(monkeypatch, ok=True)
    j = runner.rebuild_pdf(OBJ, "mr1")
    assert j["state"] == "done" and not j.get("pdf_rebuild_error")
    assert (rdir / "report.pdf").read_bytes() == b"%PDF new" and names[0] != "report.new.pdf"


def test_md_report_concurrent_rebuild_refused(root, monkeypatch):
    from backend.app.services.version_comparison_ai.md_report import runner
    _md_report()
    gate, started = threading.Event(), threading.Event()
    names = _fake_pdf(monkeypatch, gate=gate, started=started)
    errors = []
    t = threading.Thread(target=lambda: runner.rebuild_pdf(OBJ, "mr1"))
    t.start()
    try:
        assert started.wait(10)
        with pytest.raises(runner.ReportBusy):
            runner.rebuild_pdf(OBJ, "mr1")
        # То же через маршрут — 409.
        from backend.app.api.routers import version_comparison_ai as ai_router
        app = FastAPI()
        app.include_router(ai_router.router)
        r = TestClient(app).post(f"/api/version-comparison/objects/{OBJ}/md-reports/mr1/rebuild-pdf")
        errors.append(r.status_code)
    finally:
        gate.set()
        t.join(10)
    assert errors == [409] and len(names) == 1
    assert runner.get(OBJ, "mr1")["state"] == "done"


def test_md_report_json_requires_done_for_viewer_only(root, monkeypatch):
    from backend.app.api.routers import version_comparison_ai as ai_router
    from backend.app.services.version_comparison_ai.md_report import runner
    _md_report(state="incomplete", pdf=None)
    app = FastAPI()
    app.include_router(ai_router.router)
    client = TestClient(app)
    url = f"/api/version-comparison/objects/{OBJ}/md-reports/mr1/report.json"
    assert client.get(url).status_code == 200
    monkeypatch.setattr(ai_router, "read_only_request", lambda: True)
    assert client.get(url).status_code == 400
    with pytest.raises(runner.ReportError):
        runner.file_path(OBJ, "mr1", "report.json", require_done=True)


# ------------------------------------------------------------------ ИИ-описание: показ без построения слоёв

def test_ai_view_without_materialize_uses_only_existing_layers(tmp_path, monkeypatch):
    import shutil

    from backend.app.services.version_comparison_ai import executor as ex
    from backend.app.services.version_comparison_ai import jobs as ai_jobs
    from backend.app.services.version_comparison_ai import view
    from tests.test_version_comparison_ai import OBJ as AI_OBJ, describe_all, make_run, run_job, sheet

    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path / "vc"))
    monkeypatch.setenv("CPU_POOL_WORKERS", "1")
    monkeypatch.setenv("VERSION_COMPARISON_AI_ENABLED", "1")
    try:
        make_run(tmp_path, [sheet()], [sheet(values={"pp": "Рр=324.26кВт"})])
        job = run_job(ex.FakeDescriber(describe_all))
    finally:
        ex.set_override(None)
    sdir = ai_jobs.sheet_dir(AI_OBJ, "run1", job["job_id"], "r1")
    shutil.rmtree(sdir / "layers", ignore_errors=True)
    vc_root = tmp_path / "vc"
    before = snapshot(vc_root)
    v = view.job_view(AI_OBJ, "run1", job["job_id"], repair=False, materialize=False)
    s = v["sheets"][0]
    assert snapshot(vc_root) == before and not (sdir / "layers").exists()
    assert s["verification"]["state"] == "not_prepared" and v["verification"] == "not_prepared"
    assert sum(len(g["entries"]) for g in s["groups"]) > 0          # сам ответ показан
    assert "не подготовлена" in view.view_md(v)
    with pytest.raises(ai_jobs.NotPrepared):
        ai_jobs.ref_focus(AI_OBJ, "run1", job["job_id"], "r1", "V:I1:QF1", repair=False, materialize=False)
    assert snapshot(vc_root) == before
    # Админский показ строит слой; после этого viewer видит его без записи.
    assert view.job_view(AI_OBJ, "run1", job["job_id"])["verification"] == "ready"
    assert (sdir / "layers").exists()
    before = snapshot(vc_root)
    v = view.job_view(AI_OBJ, "run1", job["job_id"], repair=False, materialize=False)
    assert v["verification"] == "ready" and v["sheets"][0]["verification"]["state"] == "ready"
    assert snapshot(vc_root) == before

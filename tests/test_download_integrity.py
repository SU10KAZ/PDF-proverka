"""Скачивания: целостность, принадлежность объекту, лимит параллельности,
очистка временных файлов, отказ во время обработки, профиль viewer
(план доступа, этап 5). Временное хранилище, генератор Excel — заглушка или
настоящий детерминированный скрипт; модели не вызываются."""
from __future__ import annotations

import asyncio
import io
import json
import os
import tempfile
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote

import httpx
import pytest
from fastapi.testclient import TestClient

from tests.test_viewer_no_side_effects import CODE, OBJ_A, OBJ_B, world  # noqa: F401 — фикстура

PID = quote(f"EOM/{CODE}", safe="")
H = {"X-Requested-With": "AuditManager"}


def _client(app, login="reader", pw="reader-password-1", **headers):
    c = TestClient(app, headers={**H, **headers}, raise_server_exceptions=False)
    assert c.post("/api/auth/login", json={"username": login, "password": pw}).status_code == 200
    return c


@pytest.fixture
def app(world):  # noqa: F811
    from backend.app.main import app as _app
    return _app


def _leftovers() -> set[str]:
    root = Path(tempfile.gettempdir())
    return {p.name for p in root.glob("audit_package_*.zip")} | {p.name for p in root.glob("audit_report_*.xlsx")}


def _findings_count(content: bytes) -> int:
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        assert zf.testzip() is None
        return len(json.loads(zf.read("03_findings.json"))["findings"])


def test_package_complete_per_object_and_temp_cleaned(app):
    before = _leftovers()
    c = _client(app)
    for oid, n in ((OBJ_A, 2), (OBJ_B, 5)):
        r = c.get(f"/api/export/audit-package/{PID}", headers={"X-Object-Id": oid})
        assert r.status_code == 200 and _findings_count(r.content) == n
    assert _leftovers() <= before                     # временные файлы удалены


def test_parallel_downloads_never_mix_objects(app):
    from backend.app.api.routers import export
    export._PACKAGE_SLOTS = __import__("threading").BoundedSemaphore(2)
    clients = {OBJ_A: _client(app, **{"X-Object-Id": OBJ_A}), OBJ_B: _client(app, **{"X-Object-Id": OBJ_B})}
    expected = {OBJ_A: 2, OBJ_B: 5}

    def fetch(oid):
        for _ in range(20):
            r = clients[oid].get(f"/api/export/audit-package/{PID}")
            if r.status_code == 429:
                time.sleep(0.05)
                continue
            return oid, r
        raise AssertionError("слот так и не освободился")

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(fetch, [OBJ_A, OBJ_B] * 5))
    for oid, r in results:
        assert r.status_code == 200 and _findings_count(r.content) == expected[oid]


def test_slots_limit_returns_429(app):
    from backend.app.api.routers import export
    export._PACKAGE_SLOTS.acquire()
    export._PACKAGE_SLOTS.acquire()
    try:
        r = _client(app).get(f"/api/export/audit-package/{PID}", headers={"X-Object-Id": OBJ_A})
        assert r.status_code == 429 and r.json()["detail"]["code"] == "busy"
    finally:
        export._PACKAGE_SLOTS.release()
        export._PACKAGE_SLOTS.release()


def test_running_processing_returns_409(app, monkeypatch):
    from backend.app.pipeline.manager import pipeline_manager
    monkeypatch.setattr(pipeline_manager, "is_running", lambda pid, vid=None: True)
    r = _client(app).get(f"/api/export/audit-package/{PID}", headers={"X-Object-Id": OBJ_A})
    assert r.status_code == 409 and r.json()["detail"]["code"] == "processing"


def test_artifacts_changing_during_build_give_409_and_no_partial(world, app, monkeypatch):  # noqa: F811
    from backend.app.api.routers import export
    findings = (world["tmp"] / "projects_v2" / "objects" / f"obj_{OBJ_A}" / "disciplines" / "EOM"
                / "documents" / CODE / "versions" / "v001" / "03_analysis" / "latest" / "03_findings.json")

    def generator_while_admin_rewrites(version_dir, output_dir, out_path):
        data = json.loads(findings.read_text(encoding="utf-8"))
        data["findings"].append({"id": "F-NEW"})          # «новый анализ» пишет результат
        findings.write_text(json.dumps(data), encoding="utf-8")
        return False

    before = _leftovers()
    monkeypatch.setattr(export, "_run_excel_generator", generator_while_admin_rewrites)
    r = _client(app).get(f"/api/export/audit-package/{PID}", headers={"X-Object-Id": OBJ_A})
    assert r.status_code == 409 and r.json()["detail"]["code"] == "processing"
    assert _leftovers() <= before


def test_viewer_profile_excludes_model_chats(world, app):  # noqa: F811
    disc = (world["tmp"] / "projects_v2" / "objects" / f"obj_{OBJ_A}" / "disciplines" / "EOM"
            / "documents" / CODE / "versions" / "v001" / "_output" / "discussions")
    disc.mkdir(parents=True)
    (disc / "F-001.json").write_text('{"messages": ["чат с моделью"]}', encoding="utf-8")
    viewer = _client(app).get(f"/api/export/audit-package/{PID}", headers={"X-Object-Id": OBJ_A})
    admin = _client(app, "owner", "owner-password-1").get(
        f"/api/export/audit-package/{PID}", headers={"X-Object-Id": OBJ_A})
    names_v = zipfile.ZipFile(io.BytesIO(viewer.content)).namelist()
    names_a = zipfile.ZipFile(io.BytesIO(admin.content)).namelist()
    assert not any(n.startswith("discussions/") for n in names_v)
    assert "discussions/F-001.json" in names_a
    common = {"03_findings.json", "document.md", "README.md", "audit_report.xlsx"}
    assert common <= set(names_v)


def test_package_build_does_not_block_event_loop(app, monkeypatch):
    """Пока собирается архив (генератор Excel «думает» 2 с), healthcheck
    отвечает быстро: сборка идёт в потоке, а не в event loop."""
    from backend.app.api.routers import export

    def slow_generator(version_dir, output_dir, out_path):
        time.sleep(2.0)
        return False

    monkeypatch.setattr(export, "_run_excel_generator", slow_generator)

    async def scenario():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver", headers=H) as ac:
            r = await ac.post("/api/auth/login", json={"username": "reader", "password": "reader-password-1"})
            assert r.status_code == 200
            task = asyncio.create_task(ac.get(f"/api/export/audit-package/{PID}", headers={"X-Object-Id": OBJ_A}))
            await asyncio.sleep(0.3)
            latencies = []
            for _ in range(5):
                t0 = time.perf_counter()
                info = await ac.get("/api/info")
                latencies.append(time.perf_counter() - t0)
                assert info.status_code == 200
            pkg = await task
            assert pkg.status_code == 200
            return max(latencies)

    worst = asyncio.run(scenario())
    assert worst < 0.5, worst

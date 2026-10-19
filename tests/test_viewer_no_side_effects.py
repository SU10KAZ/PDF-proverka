"""Viewer не пишет и не запускает ничего ни на одном разрешённом маршруте
(план доступа, этап 4; поправки 0.5).

Временное хранилище: два объекта с ОДИНАКОВЫМ шифром документа, сессия
сравнения стадий, база знаний. Для каждого маршрута из перечня viewer'а —
два запроса (холодный и тёплый кэш). Проверяется:

* ни один файл временных хранилищ и рабочей копии не создан/не изменён;
* не запущен ни один подпроцесс, поток или пул исполнителей backend, ни
  одна asyncio-задача из кода backend; конвейер не вызывался.

Реальные модели и сервисы не вызываются: все «пускатели» — мины.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import json
import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from backend.app.core.access_policy import COMMON, DOWNLOAD, SELF, VIEW, policy_for
from backend.app.services.accounts import db, store
import backend.app.services.common.user_service as user_service

ROOT = Path(__file__).resolve().parents[1]
H = {"X-Requested-With": "AuditManager"}
OBJ_A, OBJ_B = "0b540226", "73a0e59a"
CODE = "ЭМ-1"


def _wj(p: Path, data) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _pdf(path: Path, text: str) -> None:
    import fitz
    path.parent.mkdir(parents=True, exist_ok=True)
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((72, 72), text)
    doc.save(str(path))
    doc.close()


def build_storage(tmp: Path) -> dict:
    """Два объекта, в каждом документ EOM/ЭМ-1 v001 с разными замечаниями."""
    v2 = tmp / "projects_v2"
    out = {"v2": v2}
    for oid, n in ((OBJ_A, 2), (OBJ_B, 5)):
        obj = v2 / "objects" / f"obj_{oid}"
        _wj(obj / "object.json", {"object_id": oid, "display_name": oid, "folder_name": obj.name})
        doc = obj / "disciplines" / "EOM" / "documents" / CODE
        _wj(doc / "document.json", {"document_code": CODE, "object_id": oid, "discipline": "EOM",
                                    "kind": "plain", "versions": [{"version_id": "v001", "version_no": 1}],
                                    "current_version": "v001"})
        (doc / "current_version.txt").write_text("v001\n", encoding="utf-8")
        ver = doc / "versions" / "v001"
        _wj(ver / "version.json", {"version_id": "v001", "version_no": 1, "analysis_status": "complete"})
        _pdf(ver / "01_input" / f"{CODE}.pdf", f"object {oid}")
        _pdf(ver / "02_work" / "document.pdf", f"object {oid}")
        (ver / "02_work" / "document.md").write_text(f"# {oid}\n\nТекст.\n", encoding="utf-8")
        _wj(ver / "01_input" / "project_info.json", {"project_id": CODE, "section": "EOM", "object_id": oid})
        latest = ver / "03_analysis" / "latest"
        _wj(latest / "03_findings.json", {"findings": [
            {"id": f"F-{i:03d}", "severity": "Критическое", "problem": f"{oid}-{i}", "page": 1}
            for i in range(1, n + 1)]})
        _wj(latest / "optimization.json", {"items": []})
        _wj(latest / "document_graph.json", {"pages": [{"page": 1, "sheet_no": "1"}]})
        (obj / "comparison" / "stage_1").mkdir(parents=True, exist_ok=True)
        (obj / "comparison" / "stage_2").mkdir(parents=True, exist_ok=True)
        out[oid] = obj
    return out


@pytest.fixture
def world(tmp_path, monkeypatch):
    from backend.app.core import config
    from backend.app.services.common import object_service

    storage = build_storage(tmp_path)
    reg = tmp_path / "objects.json"
    _wj(reg, {"objects": [
        {"id": OBJ_A, "name": "Объект А", "projects_dir": str(tmp_path / "legacy_a")},
        {"id": OBJ_B, "name": "Объект Б", "projects_dir": str(tmp_path / "legacy_b")},
    ], "current_id": OBJ_A})
    monkeypatch.setattr(object_service, "OBJECTS_FILE", reg)
    for name, value in {
        "AUDIT_PROJECTS_V2_DIR": str(storage["v2"]),
        "AUDIT_STORAGE_BACKEND": "projects_v2",
        "AUDIT_PROJECTS_V2_WRITE_MODE": "projects_v2_primary",
        "AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED": "true",
        "AUDIT_PROJECTS_V2_READ_CANARY_ENABLED": "true",
        "COMPARISON_ROOT": str(tmp_path / "comparison_root"),
        "VERSION_COMPARISON_ROOT": str(tmp_path / "vc_root"),
        "PORTAL_AUTH_ENABLED": "true", "PORTAL_AUTH_MODE": "accounts",
        "PORTAL_ACCOUNTS_DB": str(tmp_path / "accounts" / "acc.sqlite3"),
        "PORTAL_PASSWORD_ROUNDS": "1000", "PORTAL_COOKIE_SECURE": "false",
        "PORTAL_SESSION_SECRET": "side-effects-" + "s" * 40,
    }.items():
        monkeypatch.setenv(name, value)
    kb_dir = tmp_path / "knowledge_base"
    _wj(kb_dir / "decisions_log.json", {"entries": [
        {"id": "DEC-0001", "object_id": OBJ_A, "source_project": CODE, "item_id": "F-001",
         "item_type": "finding", "expert_decision": "accepted", "expert_reviewer": "Кулик А.С.",
         "expert_date": "2026-10-01T10:00:00"}]})
    for mod_name, mod in list(sys.modules.items()):
        if not mod_name.startswith("backend.app") or mod is None:
            continue
        for attr, target in (("DECISIONS_LOG_FILE", kb_dir / "decisions_log.json"),
                             ("PATTERNS_FILE", kb_dir / "patterns.json"),
                             ("KNOWLEDGE_BASE_DIR", kb_dir)):
            if hasattr(mod, attr):
                monkeypatch.setattr(mod, attr, target)
    monkeypatch.setattr(config, "APP_DATA_DIR", tmp_path / "app_data", raising=False)
    users = tmp_path / "users.json"
    _wj(users, {"users": [{"id": "kulik", "login": "kulik", "name": "Кулик А.С."}]})
    monkeypatch.setattr(user_service, "USERS_FILE", users)
    db.reset_for_tests()
    store.create_account(login="owner", display_name="Владелец", role="admin", password="owner-password-1")
    store.create_account(login="reader", display_name="Читатель", role="viewer", password="reader-password-1",
                         staff_id="kulik", sections={
                             "audit_results": {"view": True, "download": True},
                             "version_comparison": {"view": True, "download": True},
                             "stage_comparison": {"view": True, "download": True},
                             "knowledge_base": {"view": True}, "schedule": {"view": True}})
    # Админ готовит сессию сравнения стадий (это и есть «подготовлено администратором»).
    from backend.app.services.stage_comparison import store as sc_store
    a = storage[OBJ_A] / "comparison"
    session, _ = sc_store.create_session(str(a / "stage_1"), str(a / "stage_2"))
    yield {"tmp": tmp_path, "session_id": session["id"]}
    db.reset_for_tests()


# Исходники рабочей копии (их может править разработчик во время прогона) не
# сканируются; данные приложения внутри рабочей копии (backend/app/data,
# comparison, knowledge_base, logs, projects*) — сканируются.
_SOURCE_DIRS = {"tests", "frontend", "docs", "scripts", "audit_worker", "experiments", "norms",
                "prompts", "deliverables", "benchmarks", "contracts", "deploy", ".claude", ".codex"}


def _snapshot(roots) -> dict:
    out = {}
    for root in roots:
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if d not in ("__pycache__", ".git", "node_modules")]
            if Path(dirpath) == ROOT:
                dirnames[:] = [d for d in dirnames if d not in _SOURCE_DIRS]
            if Path(dirpath) == ROOT / "backend" / "app":
                dirnames[:] = [d for d in dirnames if d == "data"]
            if Path(dirpath) == ROOT / "backend":
                dirnames[:] = [d for d in dirnames if d == "app"]
            for name in filenames:
                p = Path(dirpath) / name
                if "accounts" in p.parts or "actions_log" in p.parts or p.suffix in (".pyc",) or \
                        (Path(dirpath) == ROOT and p.suffix in (".py", ".md", ".json", ".txt")):
                    continue
                try:
                    st = p.stat()
                except OSError:
                    continue
                out[str(p)] = (st.st_mtime_ns, st.st_size)
            out[dirpath] = ("dir",)
    return out


def _fill(path: str, ctx: dict) -> str:
    values = {"object_id": OBJ_A, "project_id": f"EOM/{CODE}", "version_id": "v001",
              "session_id": ctx["session_id"], "section_code": "EOM", "page_num": "1",
              "page": "1", "filename": "x.xlsx"}

    def repl(m):
        name = m.group(1).split(":")[0]
        return values.get(name, "zz-missing")
    return re.sub(r"\{([^}]+)\}", repl, path)


class _Mines:
    def __init__(self):
        self.hits: list[str] = []

    def hit(self, what):
        self.hits.append(what)
        raise RuntimeError(f"viewer запустил: {what}")


def test_viewer_routes_write_nothing_and_start_nothing(world, monkeypatch):
    from backend.app.main import app
    from backend.app.pipeline.manager import pipeline_manager

    mines = _Mines()
    real_thread_start = threading.Thread.start
    real_submit = concurrent.futures.ThreadPoolExecutor.submit
    real_create_task = asyncio.BaseEventLoop.create_task

    def thread_start(self):
        target = getattr(self, "_target", None)
        mod = getattr(target, "__module__", "") or ""
        if mod.startswith("backend.") or type(self).__module__.startswith("backend."):
            mines.hit(f"thread {mod}.{getattr(target, '__name__', '?')}")
        return real_thread_start(self)

    def submit(self, fn, *a, **k):
        prefix = getattr(self, "_thread_name_prefix", "") or ""
        if not prefix.startswith(("asyncio", "AnyIO", "ThreadPoolExecutor")):
            mines.hit(f"executor {prefix}")
        return real_submit(self, fn, *a, **k)

    def create_task(self, coro, *a, **k):
        code = getattr(coro, "cr_code", None)
        filename = getattr(code, "co_filename", "") or ""
        if "/backend/app/" in filename:
            mines.hit(f"task {code.co_name}")
        return real_create_task(self, coro, *a, **k)

    monkeypatch.setattr(threading.Thread, "start", thread_start)
    monkeypatch.setattr(concurrent.futures.ThreadPoolExecutor, "submit", submit)
    monkeypatch.setattr(asyncio.BaseEventLoop, "create_task", create_task)
    real_popen = subprocess.Popen
    inside_allowed = threading.local()

    def popen(*a, **k):
        if getattr(inside_allowed, "on", False):
            return real_popen(*a, **k)
        return mines.hit(f"Popen {a[:1]}")

    monkeypatch.setattr(subprocess, "Popen", popen)
    real_run = subprocess.run
    allowed_runs: list[list[str]] = []

    def run(cmd, *a, **k):
        # Разрешён ровно один генератор выгрузки: детерминированный Excel
        # из сохранённых результатов, фиксированный скрипт и флаги (поправка 0.5).
        argv = [str(x) for x in cmd] if isinstance(cmd, (list, tuple)) else []
        script = str(ROOT / "generate_excel_report.py")
        if len(argv) == 6 and argv[0] == sys.executable and argv[1] == script \
                and argv[3] == "--out" and argv[5] == "--no-summary" \
                and argv[2].startswith(str(world["tmp"])):
            allowed_runs.append(argv)
            inside_allowed.on = True
            try:
                return real_run(cmd, *a, **k)
            finally:
                inside_allowed.on = False
        return mines.hit(f"run {argv[:2]}")

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", lambda *a, **k: mines.hit("subprocess_exec"))
    for name in dir(pipeline_manager):
        if name.startswith(("start", "resume", "add_", "retry")) and callable(getattr(pipeline_manager, name)):
            monkeypatch.setattr(pipeline_manager, name, lambda *a, _n=name, **k: mines.hit(f"pipeline.{_n}"))

    client = TestClient(app, headers={**H, "X-Object-Id": OBJ_A}, raise_server_exceptions=False)
    assert client.post("/api/auth/login", json={"username": "reader", "password": "reader-password-1"}).status_code == 200

    routes = []
    for r in app.routes:
        for m in getattr(r, "methods", None) or []:
            if m != "GET":
                continue
            pol = policy_for(m, r.path)
            if pol.kind in (VIEW, DOWNLOAD, COMMON, SELF):
                routes.append(r.path)
    assert len(routes) > 100

    roots = [world["tmp"], ROOT]
    before = _snapshot(roots)
    statuses: dict[str, list[int]] = {}
    started = time.time()
    for path in sorted(set(routes)):
        url = _fill(path, world)
        for _ in range(2):                         # холодный и тёплый кэш
            try:
                resp = client.get(url, follow_redirects=False)
                statuses.setdefault(path, []).append(resp.status_code)
            except RuntimeError as exc:
                if "viewer запустил" in str(exc):
                    raise
                statuses.setdefault(path, []).append(599)
    after = _snapshot(roots)
    changed = sorted(k for k in set(before) | set(after) if before.get(k) != after.get(k))
    assert mines.hits == [], mines.hits
    assert allowed_runs, "архив аудита должен формироваться разрешённым генератором"
    assert changed == [], changed[:20]
    forbidden = {p: s for p, s in statuses.items() if 403 in s}
    assert forbidden == {}, forbidden                 # разрешённое не закрыто
    report = Path(os.environ.get("VIEWER_SIDE_EFFECTS_REPORT") or (world["tmp"] / "report.json"))
    report.write_text(json.dumps({"routes": len(statuses), "seconds": round(time.time() - started, 1),
                                  "statuses": statuses}, ensure_ascii=False, indent=1))

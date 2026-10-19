#!/usr/bin/env python3
"""Нагрузочный сценарий: N независимых читателей + длительная задача admin
(план доступа, этап 8; поправка 0.7 — только копии данных и заглушки).

1. Готовит базу аккаунтов стенда: admin + N viewer с правами на все разделы.
2. Поднимает отдельный uvicorn из рабочей копии на свободном порту, со всеми
   каталогами данных внутри ``--stand`` (боевые пути не используются).
3. Логинит всех, запускает заглушку длительной задачи администратора
   (``scripts/load/stand_app.py``), гоняет читателей ``--duration`` секунд.
4. Пишет метрики: p50/p95/p99 по маршрутам, коды, задержка event loop
   (зонд ``/api/info`` раз в 0,5 с), RSS и число открытых файлов процесса.

  python scripts/load/portal_viewers_load.py --stand <dir> --duration 300 --out <dir>/load.json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import re
import socket
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import quote

import httpx

ROOT = Path(__file__).resolve().parents[2]
H = {"X-Requested-With": "AuditManager"}
VIEWER_PW = "viewer-password-1"
ADMIN_PW = "admin-password-1"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def stand_env(stand: Path, port: int) -> dict:
    data = stand / "data"
    env = {k: v for k, v in os.environ.items() if not k.startswith(("AUDIT_", "PORTAL_", "DISTRIBUTED_"))}
    env.update({
        "AUDIT_DISABLE_DOTENV": "1",
        "AUDIT_DATA_DIR": str(data), "AUDIT_APP_DATA_DIR": str(data / "app_data"),
        "AUDIT_PROJECTS_DIR": str(data / "projects"), "AUDIT_PROJECTS_V2_DIR": str(data / "projects_v2"),
        "AUDIT_OBJECTS_FILE": str(stand / "objects.json"), "COMPARISON_ROOT": str(data / "comparison"),
        "VERSION_COMPARISON_ROOT": str(data / "version_comparison"),
        "AUDIT_ACTION_LOG_DIR": str(stand / "logs"), "AUDIT_STAGE_COMPARISON_ROOTS": str(data),
        "AUDIT_STORAGE_BACKEND": "projects_v2", "AUDIT_PROJECTS_V2_WRITE_MODE": "projects_v2_primary",
        "AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED": "true", "AUDIT_PROJECTS_V2_READ_CANARY_ENABLED": "true",
        "AUDIT_OPENROUTER_ENABLED": "false", "PAID_API_ENABLED": "false", "STAGE01_CLAUDE_LEG_ENABLED": "false",
        "BATCH_AUTO_RESUME_ENABLED": "false", "DISTRIBUTED_WORKERS_ENABLED": "false",
        "PORTAL_AUTH_ENABLED": "true", "PORTAL_AUTH_MODE": "accounts",
        "PORTAL_ACCOUNTS_DB": str(stand / "accounts" / "portal_accounts.sqlite3"),
        "PORTAL_SESSION_SECRET": "stand-" + os.urandom(16).hex(), "PORTAL_COOKIE_SECURE": "false",
        "PORTAL_PASSWORD_ROUNDS": "1000", "PORTAL_REGISTRATION_ENABLED": "false",
        "PYTHONUNBUFFERED": "1",
    })
    return env


def prepare_accounts(env: dict, users: int) -> None:
    os.environ.update({k: env[k] for k in ("PORTAL_ACCOUNTS_DB", "PORTAL_PASSWORD_ROUNDS")})
    sys.path.insert(0, str(ROOT))
    from backend.app.services.accounts import db, store

    db.ensure_ready()
    existing = {a["login"] for a in store.list_accounts()}
    if "admin" not in existing:
        store.create_account(login="admin", display_name="Админ стенда", role="admin", password=ADMIN_PW)
    full = {s: {"view": True, "download": s not in ("knowledge_base", "schedule")}
            for s in ("audit_results", "version_comparison", "stage_comparison", "knowledge_base", "schedule")}
    for i in range(users):
        login = f"viewer{i:02d}"
        if login not in existing:
            store.create_account(login=login, display_name=f"Читатель {i}", role="viewer",
                                 password=VIEWER_PW, sections=full)


def stand_targets(stand: Path) -> dict:
    manifest = json.loads((stand / "manifest.json").read_text(encoding="utf-8"))
    v2 = stand / "data" / "projects_v2" / "objects"
    audit, folder_oid = {}, {}
    for folder, docs in manifest["projects_v2"].items():
        oid = json.loads((v2 / folder / "object.json").read_text(encoding="utf-8"))["object_id"]
        audit[oid] = docs
        folder_oid[folder] = oid
    vc = manifest["version_comparison"]
    vroot = stand / "data" / "version_comparison" / "objects" / vc["object_id"]
    catalog = json.loads((vroot / "catalog.json").read_text(encoding="utf-8"))
    shas = [v["sha256"] for v in catalog["versions"].values()]
    pages = {v["sha256"]: int(v.get("page_count") or 1) for v in catalog["versions"].values()}
    runs = []
    for rid in vc["runs"]:
        rj = json.loads((vroot / "runs" / rid / "run.json").read_text(encoding="utf-8"))
        runs.append({"run_id": rid, "spids": [it["spid"] for it in rj.get("items") or [] if it.get("spid")][:12]})
    series = [p.name for p in (vroot / "series").iterdir()] if (vroot / "series").is_dir() else []
    sc = manifest["stage_comparison"]
    session = json.loads((stand / "data" / "comparison" / "sessions" / sc["session_id"] / "session.json").read_text())
    sc_object = next((oid for folder, oid in folder_oid.items() if f"/{folder}/" in session["stage_a_path"]), None)
    return {"audit": audit, "vc": {"object_id": vc["object_id"], "project_id": vc["project_id"], "shas": shas,
                                   "pages": pages, "runs": runs, "series": series},
            "sc": {"object_id": sc_object, "session_id": sc["session_id"], "pairs": sc["pairs"]}}


class Stats:
    def __init__(self):
        self.lat = defaultdict(list)
        self.codes = defaultdict(lambda: defaultdict(int))
        self.bytes = 0

    def add(self, name, status, seconds, size=0):
        self.lat[name].append(seconds)
        self.codes[name][status] += 1
        self.bytes += size

    def summary(self):
        def pct(xs, p):
            xs = sorted(xs)
            return round(xs[min(len(xs) - 1, int(len(xs) * p))], 3) if xs else None
        return {name: {"n": len(v), "p50": pct(v, 0.5), "p95": pct(v, 0.95), "p99": pct(v, 0.99),
                       "max": round(max(v), 3), "codes": dict(self.codes[name])}
                for name, v in sorted(self.lat.items())}


async def timed(client, stats, name, url, **kw):
    t0 = time.perf_counter()
    try:
        r = await client.get(url, **kw)
        stats.add(name, r.status_code, time.perf_counter() - t0, len(r.content))
        return r
    except httpx.HTTPError as exc:
        stats.add(name, f"EXC:{type(exc).__name__}", time.perf_counter() - t0)
        return None


async def viewer(i, base, targets, stats, stop_at, dl_every):
    rnd = random.Random(i)
    async with httpx.AsyncClient(base_url=base, headers=H, timeout=120) as c:
        r = await c.post("/api/auth/login", json={"username": f"viewer{i:02d}", "password": VIEWER_PW})
        assert r.status_code == 200, r.text
        objs = list(targets["audit"])
        oid = objs[i % len(objs)]
        docs = targets["audit"][oid]
        hdr = {"X-Object-Id": oid}
        it = 0
        while time.time() < stop_at:
            it += 1
            kind = ("audit", "vc", "sc")[i % 3] if it % 2 else ("audit", "vc", "sc")[rnd.randrange(3)]
            if kind == "audit":
                pid = quote(rnd.choice(docs), safe="")
                await timed(c, stats, "projects", "/api/projects", headers=hdr)
                await timed(c, stats, "project", f"/api/projects/{pid}", headers=hdr)
                await timed(c, stats, "findings", f"/api/findings/{pid}", headers=hdr)
                blocks = await timed(c, stats, "blocks", f"/api/tiles/{pid}/blocks", headers=hdr)
                ids = []
                if blocks is not None and blocks.status_code == 200:
                    ids = re.findall(r'"block_id"\s*:\s*"([^"]+)"', blocks.text)[:6]
                for bid in ids:
                    await timed(c, stats, "block_image", f"/api/tiles/{pid}/blocks/image/{quote(bid)}?object_id={oid}")
                await timed(c, stats, "doc_pages", f"/api/document/{pid}/pages", headers=hdr)
                for n in (1, 2):
                    await timed(c, stats, "doc_page", f"/api/document/{pid}/page/{n}", headers=hdr)
                await timed(c, stats, "doc_pdf", f"/api/document/{pid}/pdf?object_id={oid}")
                await timed(c, stats, "optimization", f"/api/optimization/{pid}", headers=hdr)
                if it % dl_every == 0:
                    await timed(c, stats, "DL_audit_package", f"/api/export/audit-package/{pid}", headers=hdr)
            elif kind == "vc":
                vc = targets["vc"]
                base_vc = f"/api/version-comparison/objects/{vc['object_id']}"
                h = {"X-Object-Id": ""}
                await timed(c, stats, "vc_catalog", f"{base_vc}/catalog", headers=h)
                await timed(c, stats, "vc_overview", f"{base_vc}/overview", headers=h)
                await timed(c, stats, "vc_matrix_view", f"{base_vc}/projects/{vc['project_id']}/matrix-view", headers=h)
                await timed(c, stats, "vc_runs", f"{base_vc}/projects/{vc['project_id']}/runs", headers=h)
                run = rnd.choice(vc["runs"])
                await timed(c, stats, "vc_run", f"{base_vc}/runs/{run['run_id']}", headers=h)
                for spid in rnd.sample(run["spids"], min(3, len(run["spids"]))):
                    await timed(c, stats, "vc_sheet", f"{base_vc}/runs/{run['run_id']}/sheets/{spid}", headers=h)
                    await timed(c, stats, "vc_sheet_tile", f"{base_vc}/runs/{run['run_id']}/sheets/{spid}/tile?level=0", headers=h)
                sha = rnd.choice(vc["shas"])
                for page in rnd.sample(range(1, vc["pages"][sha] + 1), min(4, vc["pages"][sha])):
                    await timed(c, stats, "vc_thumb", f"{base_vc}/sources/{sha}/pages/{page}/thumb?w=320", headers=h)
                await timed(c, stats, "vc_tile", f"{base_vc}/sources/{sha}/pages/1/tile?level=0", headers=h)
                if it % dl_every == 0 and vc["series"]:
                    await timed(c, stats, "DL_vc_series_xlsx", f"{base_vc}/series/{rnd.choice(vc['series'])}/report.xlsx", headers=h)
            else:
                sc = targets["sc"]
                h = {"X-Object-Id": sc["object_id"]}
                await timed(c, stats, "sc_objects", "/api/stage-comparison/objects", headers=h)
                await timed(c, stats, "sc_session_find", f"/api/stage-comparison/objects/{sc['object_id']}/session", headers=h)
                await timed(c, stats, "sc_session", f"/api/stage-comparison/sessions/{sc['session_id']}", headers=h)
                pair = rnd.choice(sc["pairs"])
                pp = f"/api/stage-comparison/sessions/{sc['session_id']}/pairs/{pair}"
                await timed(c, stats, "sc_pair", pp, headers=h)
                for page in rnd.sample(range(1, 6), 3):
                    side = rnd.choice(("left", "right"))
                    await timed(c, stats, "sc_page_svg", f"{pp}/page-svg?side={side}&page={page}", headers=h)
                    await timed(c, stats, "sc_page_thumb", f"{pp}/page-thumb?side={side}&page={page}", headers=h)
                await timed(c, stats, "sc_page_preview", f"{pp}/page-preview?side=left&page={rnd.randrange(1, 4)}&width=1400", headers=h)
            await asyncio.sleep(rnd.uniform(1.0, 5.0))


async def probe(base, stop_at, out):
    async with httpx.AsyncClient(base_url=base, timeout=30) as c:
        while time.time() < stop_at:
            t0 = time.perf_counter()
            try:
                await c.get("/api/info")
            except httpx.HTTPError:
                pass
            out.append(time.perf_counter() - t0)
            await asyncio.sleep(0.5)


async def sampler(pid, stop_at, out):
    while time.time() < stop_at:
        try:
            status = Path(f"/proc/{pid}/status").read_text()
            rss = int(re.search(r"VmRSS:\s+(\d+)", status).group(1)) // 1024
            fds = len(os.listdir(f"/proc/{pid}/fd"))
            threads = int(re.search(r"Threads:\s+(\d+)", status).group(1))
            out.append({"t": round(time.time(), 1), "rss_mb": rss, "fds": fds, "threads": threads})
        except (OSError, AttributeError):
            pass
        await asyncio.sleep(5)


async def run_load(base, targets, users, duration, admin_task, dl_every, pid):
    stats = Stats()
    probe_lat: list[float] = []
    samples: list[dict] = []
    async with httpx.AsyncClient(base_url=base, headers=H, timeout=60) as admin:
        r = await admin.post("/api/auth/login", json={"username": "admin", "password": ADMIN_PW})
        assert r.status_code == 200, r.text
        if admin_task:
            r = await admin.post(f"/api/_stand/long-admin-task?seconds={duration}&slots=4")
            assert r.status_code == 200, r.text
    stop_at = time.time() + duration
    started = time.time()
    await asyncio.gather(
        probe(base, stop_at, probe_lat), sampler(pid, stop_at, samples),
        *(viewer(i, base, targets, stats, stop_at, dl_every) for i in range(users)),
    )
    total = sum(len(v) for v in stats.lat.values())
    errors5xx = sum(n for v in stats.codes.values() for code, n in v.items()
                    if isinstance(code, int) and code >= 500 or isinstance(code, str))
    probe_sorted = sorted(probe_lat)
    return {
        "users": users, "duration_s": round(time.time() - started, 1), "admin_task": admin_task,
        "requests": total, "rps": round(total / max(1, duration), 1), "bytes_mb": round(stats.bytes / 2**20, 1),
        "errors_5xx_or_exc": errors5xx,
        "loop_probe": {"n": len(probe_lat), "p50": round(statistics.median(probe_lat), 4) if probe_lat else None,
                       "p99": round(probe_sorted[int(len(probe_sorted) * 0.99) - 1], 4) if probe_sorted else None,
                       "max": round(max(probe_lat), 4) if probe_lat else None},
        "process": {"rss_mb_max": max((s["rss_mb"] for s in samples), default=None),
                    "fds_max": max((s["fds"] for s in samples), default=None),
                    "threads_max": max((s["threads"] for s in samples), default=None)},
        "routes": stats.summary(),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stand", required=True)
    ap.add_argument("--users", type=int, default=15)
    ap.add_argument("--duration", type=int, default=300)
    ap.add_argument("--no-admin-task", action="store_true")
    ap.add_argument("--download-every", type=int, default=4, help="каждая N-я итерация читателя — скачивание")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    stand = Path(args.stand).resolve()
    port = free_port()
    env = stand_env(stand, port)
    prepare_accounts(env, args.users)
    targets = stand_targets(stand)
    log = open(stand / "server.log", "w")
    server = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "scripts.load.stand_app:app", "--host", "127.0.0.1", "--port", str(port)],
        cwd=str(ROOT), env=env, stdout=log, stderr=subprocess.STDOUT,
    )
    base = f"http://127.0.0.1:{port}"
    try:
        for _ in range(120):
            try:
                if httpx.get(base + "/api/info", timeout=2).status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            time.sleep(1)
        else:
            raise SystemExit("стенд не поднялся, см. server.log")
        result = asyncio.run(run_load(base, targets, args.users, args.duration,
                                      not args.no_admin_task, args.download_every, server.pid))
        result["port"] = port
        Path(args.out).write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
        print(json.dumps({k: v for k, v in result.items() if k != "routes"}, ensure_ascii=False, indent=1))
    finally:
        server.terminate()
        try:
            server.wait(timeout=30)
        except subprocess.TimeoutExpired:
            server.kill()
        log.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

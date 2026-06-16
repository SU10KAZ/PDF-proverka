#!/usr/bin/env python3
"""
http_smoke_shadow_api.py — controlled HTTP smoke для projects_v2 shadow API.

Поднимает МИНИМАЛЬНЫЙ FastAPI-app (только shadow-router + один read-only legacy
router как представитель) на эфемерном порту через uvicorn и бьёт по реальному
сокету. НЕ импортирует `backend.app.main` и НЕ запускает его lifespan
(cleanup_zombies / load_persisted_queue / recover_stale) — поэтому НИКАКИХ
побочных эффектов на общий state и на production backend (он живёт отдельно на
:8081, мы его не трогаем).

Проверяет:
  1. без флага  GET /api/projects-v2-shadow/health → 404;
  2. с флагом AUDIT_PROJECTS_V2_SHADOW_API_ENABLED=true:
     /health, /objects, /documents, /documents/{code}/snapshot, /parity/sample → 200;
  3. обычный legacy endpoint (/api/objects) работает и без флага, и с флагом;
  4. AUDIT_STORAGE_BACKEND по умолчанию остаётся legacy;
  5. shadow API ничего не пишет в projects_v2 (snapshot objects/ до/после == );
  6. (отдельно проверяется снаружи) projects_v2 не попадает в git.

Флаг читается на КАЖДЫЙ запрос → один сервер, тумблер env между фазами в том же
процессе. Порт по умолчанию эфемерный (0), чтобы не конфликтовать с :8081.

Runtime-отчёт:
  projects_v2/_system/shadow_api_http_smoke_report.json
  projects_v2/_system/shadow_api_http_smoke_report.md

READ-ONLY (кроме своего отчёта в _system). legacy projects/ и comparison/ не трогает.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_SCRIPT_DIR))            # v2lib
sys.path.insert(0, str(_SCRIPT_DIR.parents[1]))  # repo root
import v2lib  # noqa: E402

SHADOW = "/api/projects-v2-shadow"
FLAG = "AUDIT_PROJECTS_V2_SHADOW_API_ENABLED"
REQUIRED_STATUS_TYPES = ["complete", "partial", "none", "source_only", "legacy_partial"]


def build_smoke_app():
    """Минимальный app: shadow router + read-only legacy router (objects). Без lifespan."""
    from fastapi import FastAPI
    from backend.app.api.routers import projects_v2_shadow, objects
    app = FastAPI(title="projects_v2 shadow HTTP smoke", lifespan=None)
    app.include_router(projects_v2_shadow.router)
    app.include_router(objects.router)  # представитель legacy read-only API
    return app


def _http_get(base: str, path: str):
    url = base + path
    try:
        with urllib.request.urlopen(url, timeout=20) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, (json.loads(body) if body else None)
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode("utf-8"))
        except Exception:
            return e.code, None
    except Exception as e:
        return 0, {"error": str(e)}


def _snapshot(root: Path) -> dict:
    if not root.is_dir():
        return {}
    return {str(p): (p.stat().st_mtime_ns, p.stat().st_size)
            for p in root.rglob("*") if p.is_file()}


def _pick_sample_codes(parity_json: dict | None) -> list[dict]:
    out, seen_types = [], {}
    for r in (parity_json or {}).get("results", []):
        t = r.get("type")
        cap = 3 if t == "king_sons_legacy_preserve" else 1
        if seen_types.get(t, 0) < cap:
            out.append({"code": r.get("document_code"), "type": t})
            seen_types[t] = seen_types.get(t, 0) + 1
    return out


def run_smoke(v2_root: Path, port: int = 0) -> dict:
    import uvicorn

    # окружение: auth не нужен (middleware в smoke-app нет); backend остаётся legacy
    os.environ["PORTAL_AUTH_ENABLED"] = "false"
    os.environ.pop("AUDIT_STORAGE_BACKEND", None)  # подтверждаем default=legacy
    os.environ.pop(FLAG, None)                      # старт без флага

    app = build_smoke_app()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    t0 = time.time()
    while not getattr(server, "started", False) and time.time() - t0 < 20:
        time.sleep(0.05)
    if not getattr(server, "started", False):
        raise RuntimeError("uvicorn smoke server did not start")
    actual_port = server.servers[0].sockets[0].getsockname()[1]
    base = f"http://127.0.0.1:{actual_port}"

    checks: list[dict] = []
    documents: list[dict] = []

    def add(name, ok, detail):
        checks.append({"check": name, "ok": bool(ok), "detail": str(detail)})

    try:
        before = _snapshot(v2_root / "objects")

        # ---- phase A: флаг ВЫКЛЮЧЕН ----
        os.environ.pop(FLAG, None)
        st, _ = _http_get(base, f"{SHADOW}/health")
        add("health_404_without_flag", st == 404, f"status={st}")
        st_obj, _ = _http_get(base, "/api/objects")
        add("legacy_objects_without_flag_200", st_obj == 200, f"status={st_obj}")

        # ---- phase B: флаг ВКЛЮЧЁН ----
        os.environ[FLAG] = "true"
        sh, hj = _http_get(base, f"{SHADOW}/health")
        add("shadow_health_200", sh == 200, f"status={sh}")
        add("health_read_only_flag", bool(hj) and hj.get("read_only") is True,
            f"read_only={(hj or {}).get('read_only')}")
        add("health_backend_default_legacy",
            (hj or {}).get("storage_backend_default") == "legacy",
            f"default={(hj or {}).get('storage_backend_default')}")
        add("health_counts",
            bool(hj) and hj.get("object_count", 0) > 0 and hj.get("document_count", 0) > 0,
            f"objects={(hj or {}).get('object_count')} docs={(hj or {}).get('document_count')}")

        so, oj = _http_get(base, f"{SHADOW}/objects")
        add("shadow_objects_200", so == 200 and (oj or {}).get("count", 0) > 0,
            f"status={so} count={(oj or {}).get('count')}")
        sd, dj = _http_get(base, f"{SHADOW}/documents?limit=5")
        add("shadow_documents_200", sd == 200 and (dj or {}).get("total", 0) > 0,
            f"status={sd} total={(dj or {}).get('total')}")
        sp, pj = _http_get(base, f"{SHADOW}/parity/sample")
        add("shadow_parity_sample_200", sp == 200,
            f"status={sp} parity_ok={(pj or {}).get('parity_ok')}")

        # snapshots по выборке типов
        samples = _pick_sample_codes(pj if sp == 200 else None)
        statuses_seen = set()
        for s in samples:
            code = s["code"]
            ss, sj = _http_get(base, f"{SHADOW}/documents/{urllib.parse.quote(code, safe='')}/snapshot")
            cur_status = None
            if isinstance(sj, dict):
                for v in sj.get("versions", []):
                    if v.get("is_current"):
                        cur_status = v.get("analysis_status")
                if cur_status is None and sj.get("versions"):
                    cur_status = sj["versions"][0].get("analysis_status")
            ok = ss == 200 and isinstance(sj, dict) and cur_status is not None
            if ok and cur_status:
                statuses_seen.add(cur_status)
            documents.append({"code": code, "type": s["type"], "snapshot_status": ss,
                              "analysis_status": cur_status, "ok": ok})
        add("shadow_snapshots_200_with_status",
            all(d["ok"] for d in documents) if documents else False,
            f"{sum(1 for d in documents if d['ok'])}/{len(documents)} ok")
        add("status_variety_covered",
            set(REQUIRED_STATUS_TYPES).issubset(statuses_seen),
            f"seen={sorted(statuses_seen)}")

        # legacy endpoint ПРИ включённом флаге
        st_obj2, _ = _http_get(base, "/api/objects")
        add("legacy_objects_with_flag_200", st_obj2 == 200, f"status={st_obj2}")

        # ---- phase C: тумблер обратно ВЫКЛ ----
        os.environ[FLAG] = "false"
        st_off, _ = _http_get(base, f"{SHADOW}/health")
        add("health_404_after_disable", st_off == 404, f"status={st_off}")

        # read-only: objects/ не изменились
        after = _snapshot(v2_root / "objects")
        add("read_only_objects_unchanged", before == after,
            f"files_before={len(before)} files_after={len(after)} "
            f"changed={len({k for k in before if before.get(k) != after.get(k)}) + len(set(after)-set(before))}")
    finally:
        os.environ.pop(FLAG, None)  # не оставляем флаг включённым
        server.should_exit = True
        thread.join(timeout=10)

    from backend.app.services.storage.projects_v2_adapter import get_storage_backend
    add("adapter_backend_default_legacy", get_storage_backend() == "legacy",
        f"get_storage_backend()={get_storage_backend()}")

    ok = all(c["ok"] for c in checks)
    return {
        "schema_version": 1,
        "generated_at": v2lib.utc_now_iso(),
        "transport": "http",
        "base_url": base,
        "ok": ok,
        "checks": checks,
        "documents": documents,
        "summary": {
            "checks_total": len(checks),
            "checks_passed": sum(1 for c in checks if c["ok"]),
            "documents_checked": len(documents),
            "documents_ok": sum(1 for d in documents if d["ok"]),
        },
    }


def render_md(rep: dict) -> str:
    L, A = [], None
    out = []
    def A(s): out.append(s)
    A("# Shadow API — controlled HTTP smoke (projects_v2)")
    A("")
    A(f"**Сгенерировано:** {rep.get('generated_at')}  ")
    A(f"**Transport:** HTTP (реальный сокет, отдельный uvicorn, без lifespan)  ")
    A(f"**Итог:** {'✅ OK' if rep['ok'] else '❌ FAIL'}")
    s = rep["summary"]
    A("")
    A(f"- Проверок: {s['checks_passed']}/{s['checks_total']}")
    A(f"- Документов: {s['documents_ok']}/{s['documents_checked']}")
    A("")
    A("## Checks")
    A("")
    A("| check | ok | detail |")
    A("|---|---|---|")
    for c in rep["checks"]:
        A(f"| {c['check']} | {'✅' if c['ok'] else '❌'} | {c['detail']} |")
    A("")
    A("## Документы (snapshot по типам)")
    A("")
    A("| Документ | тип | HTTP | analysis_status | ok |")
    A("|---|---|---|---|---|")
    for d in rep["documents"]:
        A(f"| {d['code']} | {d['type']} | {d['snapshot_status']} | "
          f"{d['analysis_status']} | {'✅' if d['ok'] else '❌'} |")
    A("")
    A("> production backend (:8081) НЕ затрагивался: smoke поднимает отдельный "
      "минимальный app на эфемерном порту без lifespan. AUDIT_STORAGE_BACKEND "
      "остаётся legacy; флаг включается только в этом процессе.")
    return "\n".join(out)


def write_reports(rep: dict, v2_root: Path) -> tuple[Path, Path]:
    sys_dir = v2_root / "_system"
    sys_dir.mkdir(parents=True, exist_ok=True)
    jp = sys_dir / "shadow_api_http_smoke_report.json"
    mp = sys_dir / "shadow_api_http_smoke_report.md"
    jp.write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding="utf-8")
    mp.write_text(render_md(rep), encoding="utf-8")
    return jp, mp


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Controlled HTTP smoke для projects_v2 shadow API")
    ap.add_argument("--v2-root", default=None)
    ap.add_argument("--port", type=int, default=0, help="порт (0 = эфемерный, НЕ 8081)")
    args = ap.parse_args(argv)
    if args.port == 8081:
        print("[REFUSED] порт 8081 — production; выберите другой или 0", file=sys.stderr)
        return 2

    v2_root = Path(args.v2_root).resolve() if args.v2_root else v2lib.projects_v2_root()
    rep = run_smoke(v2_root, port=args.port)
    jp, mp = write_reports(rep, v2_root)

    print("=== shadow API HTTP smoke ===")
    print(f"base_url: {rep['base_url']}")
    s = rep["summary"]
    print(f"checks: {s['checks_passed']}/{s['checks_total']}  "
          f"documents: {s['documents_ok']}/{s['documents_checked']}  ok: {rep['ok']}")
    for c in rep["checks"]:
        print(f"  [{'OK ' if c['ok'] else 'FAIL'}] {c['check']}: {c['detail']}")
    print(f"-> {jp}\n-> {mp}")
    return 0 if rep["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

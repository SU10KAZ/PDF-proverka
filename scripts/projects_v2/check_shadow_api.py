#!/usr/bin/env python3
"""
check_shadow_api.py — проверка read-only shadow API над `projects_v2` БЕЗ UI.

Что делает:
  * дергает GET /api/projects-v2-shadow/health;
  * проверяет несколько документов РАЗНЫХ типов (complete / partial / none /
    source_only / legacy_partial / versioned / King&Sons preserve) через
    /documents/{code}/snapshot;
  * сравнивает с parity report (/parity/sample);
  * пишет runtime-отчёт:
      projects_v2/_system/shadow_api_check_report.json
      projects_v2/_system/shadow_api_check_report.md

Режимы:
  * по умолчанию — HTTP против локального backend (--base-url), требует, чтобы
    shadow API был включён в этом процессе (AUDIT_PROJECTS_V2_SHADOW_API_ENABLED);
  * `--in-process` — поднимает TestClient в ЭТОМ процессе (флаг форсится ON,
    portal-auth OFF), не трогая запущенный production backend. Удобно для
    офлайн-проверки без живого сервера и без рестарта.

READ-ONLY: сам скрипт не пишет в `projects_v2`, кроме своего отчёта в `_system`.
Через API данные тоже только читаются (adapter read-only).
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_SCRIPT_DIR))           # v2lib
sys.path.insert(0, str(_SCRIPT_DIR.parents[1]))  # repo root
import v2lib  # noqa: E402

SHADOW_PREFIX = "/api/projects-v2-shadow"
REQUIRED_TYPES = ["complete", "partial", "none", "source_only", "legacy_partial",
                  "versioned", "king_sons_legacy_preserve"]


# ---------------------------------------------------------------------------
# fetch backends
# ---------------------------------------------------------------------------


def build_http_fetch(base_url: str, cookie: str | None = None):
    base = base_url.rstrip("/")

    def fetch(path: str):
        url = base + path
        req = urllib.request.Request(url, method="GET")
        if cookie:
            req.add_header("Cookie", cookie)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read().decode("utf-8")
                return resp.status, (json.loads(body) if body else None)
        except urllib.error.HTTPError as e:
            try:
                return e.code, json.loads(e.read().decode("utf-8"))
            except Exception:
                return e.code, None
        except Exception as e:  # connection refused etc.
            return 0, {"error": str(e)}

    return fetch


def build_inprocess_fetch():
    """TestClient в этом процессе: флаг ON, portal-auth OFF (prod не трогаем)."""
    import os
    # throwaway in-process client: auth off + shadow on (НЕ влияет на running prod —
    # это отдельный процесс; lifespan/startup НЕ запускается без `with TestClient`).
    os.environ["PORTAL_AUTH_ENABLED"] = "false"
    os.environ["AUDIT_PROJECTS_V2_SHADOW_API_ENABLED"] = "true"
    from fastapi.testclient import TestClient
    from backend.app.main import app
    client = TestClient(app)

    def fetch(path: str):
        r = client.get(path)
        try:
            return r.status_code, r.json()
        except Exception:
            return r.status_code, None

    return fetch


# ---------------------------------------------------------------------------
# core check (fetch-agnostic → тестируемо)
# ---------------------------------------------------------------------------


def _doc_path(code: str, suffix: str = "") -> str:
    return f"{SHADOW_PREFIX}/documents/{urllib.parse.quote(code, safe='')}{suffix}"


def _pick_samples(parity: dict | None, per_type: int = 2) -> list[dict]:
    """Берём sample-документы из parity report (code+type).

    Для `king_sons_legacy_preserve` берём шире: source_only и legacy_partial —
    это version-level analysis_status ВНУТРИ King&Sons-типа (отдельным parity
    type'ом они не выделяются), поэтому нужно захватить вариативность статусов.
    """
    out: list[dict] = []
    buckets: dict[str, int] = {}
    for r in (parity or {}).get("results", []):
        t = r.get("type")
        cap = max(per_type, 4) if t == "king_sons_legacy_preserve" else per_type
        if t in REQUIRED_TYPES and buckets.get(t, 0) < cap:
            out.append({"document_code": r.get("document_code"), "type": t,
                        "parity_ok": r.get("ok")})
            buckets[t] = buckets.get(t, 0) + 1
    return out


def check_shadow(fetch, *, per_type: int = 2) -> dict:
    """Гоняет shadow API через `fetch(path)->(status,json)`. Возвращает отчёт-dict."""
    checks: list[dict] = []

    def add(name, ok, detail):
        checks.append({"check": name, "ok": bool(ok), "detail": detail})

    # health
    h_status, h_json = fetch(f"{SHADOW_PREFIX}/health")
    if h_status == 404:
        add("health", False, "shadow API disabled (404) — включите AUDIT_PROJECTS_V2_SHADOW_API_ENABLED")
        return {"shadow_enabled": False, "health_status": 404, "checks": checks,
                "documents": [], "ok": False,
                "summary": {"reason": "shadow_api_disabled"}}
    health_ok = h_status == 200 and bool(h_json) and h_json.get("shadow_api_enabled")
    add("health", health_ok, f"status={h_status} docs={(h_json or {}).get('document_count')}")
    add("read_only_flag", bool(h_json) and h_json.get("read_only") is True,
        f"read_only={(h_json or {}).get('read_only')}")
    add("storage_backend_default_legacy",
        (h_json or {}).get("storage_backend_default") == "legacy",
        f"default={(h_json or {}).get('storage_backend_default')}")

    # objects / documents
    o_status, o_json = fetch(f"{SHADOW_PREFIX}/objects")
    add("objects", o_status == 200, f"status={o_status} count={(o_json or {}).get('count')}")
    d_status, d_json = fetch(f"{SHADOW_PREFIX}/documents?limit=5")
    add("documents", d_status == 200 and (d_json or {}).get("total", 0) > 0,
        f"status={d_status} total={(d_json or {}).get('total')}")

    # parity
    p_status, p_json = fetch(f"{SHADOW_PREFIX}/parity/sample")
    parity_available = p_status == 200 and (p_json or {}).get("available")
    add("parity_sample", p_status == 200,
        f"status={p_status} available={(p_json or {}).get('available')} "
        f"parity_ok={(p_json or {}).get('parity_ok')}")

    # per-document snapshots across types
    samples = _pick_samples(p_json if parity_available else None, per_type)
    doc_results = []
    covered_types = set()
    for s in samples:
        code = s["document_code"]
        st, snap = fetch(_doc_path(code, "/snapshot"))
        vs, ver = fetch(_doc_path(code, "/versions"))
        ok = st == 200 and isinstance(snap, dict) and bool(snap.get("versions"))
        cur = snap.get("current_version") if isinstance(snap, dict) else None
        cur_status = None
        if isinstance(snap, dict):
            for v in snap.get("versions", []):
                if v.get("is_current"):
                    cur_status = v.get("analysis_status")
            if cur_status is None and snap.get("versions"):
                cur_status = snap["versions"][0].get("analysis_status")
        status_present = cur_status is not None
        doc_results.append({
            "document_code": code, "type": s["type"],
            "snapshot_status": st, "versions_status": vs,
            "http_ok": ok, "current_version": cur,
            "analysis_status": cur_status,
            "analysis_status_present": status_present,
            "version_count": (snap.get("version_count") if isinstance(snap, dict) else None),
            "parity_ok": s.get("parity_ok"),
            "ok": ok and status_present,
        })
        if ok:
            # покрытие считаем И по структурному type, И по version-level статусу
            covered_types.add(s["type"])
            if cur_status:
                covered_types.add(cur_status)

    add("doc_snapshots_ok", all(d["ok"] for d in doc_results) if doc_results else False,
        f"{sum(1 for d in doc_results if d['ok'])}/{len(doc_results)} ok")
    # source_only / legacy_partial (как version-level статусы) не должны падать
    so = [d for d in doc_results if d["analysis_status"] == "source_only"]
    lp = [d for d in doc_results if d["analysis_status"] == "legacy_partial"]
    add("source_only_ok", all(d["http_ok"] for d in so) if so else True,
        f"{len(so)} source_only docs")
    add("legacy_partial_ok", all(d["http_ok"] for d in lp) if lp else True,
        f"{len(lp)} legacy_partial docs")
    add("types_covered", set(REQUIRED_TYPES).issubset(covered_types) if parity_available else True,
        f"covered={sorted(covered_types)}")

    all_ok = all(c["ok"] for c in checks)
    return {
        "shadow_enabled": True,
        "health_status": h_status,
        "health": h_json,
        "checks": checks,
        "documents": doc_results,
        "covered_types": sorted(covered_types),
        "ok": all_ok,
        "summary": {
            "checks_total": len(checks),
            "checks_passed": sum(1 for c in checks if c["ok"]),
            "documents_checked": len(doc_results),
            "documents_ok": sum(1 for d in doc_results if d["ok"]),
            "parity_available": parity_available,
            "parity_ok": (p_json or {}).get("parity_ok") if parity_available else None,
        },
    }


def render_md(rep: dict) -> str:
    L = []
    A = L.append
    A("# Shadow API check — projects_v2 read-only")
    A("")
    A(f"**Сгенерировано:** {rep.get('generated_at')}  ")
    if not rep.get("shadow_enabled"):
        A("**Итог:** ⚠️ shadow API ВЫКЛЮЧЕН (404). "
          "Включите `AUDIT_PROJECTS_V2_SHADOW_API_ENABLED=true` в нужном процессе.")
        return "\n".join(L)
    A(f"**Итог:** {'✅ OK' if rep['ok'] else '❌ FAIL'}")
    s = rep["summary"]
    A("")
    A(f"- Проверок: {s['checks_passed']}/{s['checks_total']}")
    A(f"- Документов: {s['documents_ok']}/{s['documents_checked']} ok")
    A(f"- Покрытые типы: {rep.get('covered_types')}")
    A(f"- Parity: available={s['parity_available']} parity_ok={s['parity_ok']}")
    A("")
    A("## Checks")
    A("")
    A("| check | ok | detail |")
    A("|---|---|---|")
    for c in rep["checks"]:
        A(f"| {c['check']} | {'✅' if c['ok'] else '❌'} | {c['detail']} |")
    A("")
    A("## Документы")
    A("")
    A("| Документ | тип | snapshot | analysis_status | версий | parity |")
    A("|---|---|---|---|---|---|")
    for d in rep["documents"]:
        A(f"| {d['document_code']} | {d['type']} | {d['snapshot_status']} | "
          f"{d['analysis_status']} | {d['version_count']} | {d['parity_ok']} |")
    return "\n".join(L)


def write_reports(rep: dict, v2_root: Path) -> tuple[Path, Path]:
    sys_dir = v2_root / "_system"
    sys_dir.mkdir(parents=True, exist_ok=True)
    jp = sys_dir / "shadow_api_check_report.json"
    mp = sys_dir / "shadow_api_check_report.md"
    jp.write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding="utf-8")
    mp.write_text(render_md(rep), encoding="utf-8")
    return jp, mp


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Проверка read-only shadow API над projects_v2")
    ap.add_argument("--base-url", default="http://127.0.0.1:8081")
    ap.add_argument("--cookie", default=None, help="Cookie-заголовок (если включён portal auth)")
    ap.add_argument("--in-process", action="store_true",
                    help="поднять TestClient в этом процессе (без живого сервера)")
    ap.add_argument("--v2-root", default=None)
    ap.add_argument("--per-type", type=int, default=2)
    args = ap.parse_args(argv)

    v2_root = Path(args.v2_root).resolve() if args.v2_root else v2lib.projects_v2_root()
    fetch = build_inprocess_fetch() if args.in_process else build_http_fetch(args.base_url, args.cookie)

    rep = check_shadow(fetch, per_type=args.per_type)
    rep["generated_at"] = v2lib.utc_now_iso()
    rep["mode"] = "in_process" if args.in_process else f"http:{args.base_url}"
    jp, mp = write_reports(rep, v2_root)

    print("=== shadow API check ===")
    print(f"mode: {rep['mode']}")
    if not rep.get("shadow_enabled"):
        print("shadow API DISABLED (404) — нечего проверять (это ожидаемо в production).")
        print(f"-> {jp}\n-> {mp}")
        return 0  # disabled — не ошибка
    s = rep["summary"]
    print(f"checks: {s['checks_passed']}/{s['checks_total']}  "
          f"documents: {s['documents_ok']}/{s['documents_checked']}  ok: {rep['ok']}")
    print(f"covered types: {rep['covered_types']}")
    for d in rep["documents"]:
        print(f"  [{'OK ' if d['ok'] else 'FAIL'}] {d['type']:<26} {d['document_code']} "
              f"status={d['analysis_status']}")
    print(f"-> {jp}\n-> {mp}")
    return 0 if rep["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

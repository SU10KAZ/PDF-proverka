"""Результат листа «Изменения в листе» — единственный источник для окна, MD и сводки.

Строительный текст — пункты ``changes`` и вопросы ``unresolved``. Учёт указателей,
исключённое, расход и замеры — в ``diagnostics`` (в основной текст не попадают).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from backend.app.services.version_comparison import jobs as vc_jobs
from backend.app.services.version_comparison import permit as permit_mod

from .. import settings
from . import schema as sc
from . import validate as val

SCHEMA = "version_comparison_ai.sheet_changes/v1"
DISCLAIMER = "Описание составлено ИИ по исходным листам двух версий и инженером не подтверждено."


def _declared(p: dict) -> list[dict[str, Any]]:
    """Строки разрешения B, где упомянут этот лист, — «заявлено проектировщиком» (не вход модели)."""
    try:
        pm = vc_jobs.run_permit(p["object_id"], p["run_id"])
    except Exception:  # noqa: BLE001 — сведения необязательны
        return []
    if not pm.get("available"):
        return []
    from .. import context as ctx
    rec = ctx.Pages(p["object_id"], p["a"]["sha256"], p["b"]["sha256"]).page_rec("B", p["b"]["page"])
    ref = permit_mod.sheet_ref(rec) if rec else None
    if not ref:
        return []
    out = []
    for r in pm.get("rows") or []:
        if any(x.get("set") == ref["set"] and x.get("sheet") == ref["sheet"] for x in r.get("refs") or []):
            out.append({"izm": r.get("izm"), "text": r.get("text"), "permit": r.get("permit")})
    return out


def sheet_result(p: dict, chk: dict, state: str, job: dict, runtime: dict, diag: dict) -> dict[str, Any]:
    ledger_counts = p.get("counts") or {}
    excluded_engine: dict[str, int] = {}
    for e in (p.get("ledger") or {}).values():
        if e["status"] == "excluded":
            excluded_engine[e.get("reason") or "?"] = excluded_engine.get(e.get("reason") or "?", 0) + 1
    side = lambda s: {k: s.get(k) for k in ("label", "file_name", "sha256", "page", "sheet_no", "title")}  # noqa: E731
    ok = chk.get("ok")
    return {
        "schema": SCHEMA, "method": settings.sc_versions(), "key": diag.get("cache_key"),
        "object_id": p["object_id"], "project_id": job.get("project_id"), "run_id": p["run_id"],
        "job_id": job["job_id"], "spid": p["spid"], "trigger": job.get("trigger", "manual"),
        "title": p.get("title"), "project": p.get("project"), "a": side(p["a"]), "b": side(p["b"]),
        "engine": {k: p["engine"].get(k) for k in ("algo", "status", "status_label", "result_sha256", "notes")},
        "executor": runtime, "state": state, "state_label": val.STATE_LABELS.get(state, state),
        "packet_id": p["packet_id"],
        "changes": chk.get("changes", []) if ok else [],
        "unresolved": chk.get("unresolved", []) if ok else [],
        "excluded": chk.get("excluded", []) if ok else [],
        "declared": _declared(p),
        "coverage": chk.get("coverage") if ok else None,
        "transform": p.get("transform"), "canvas": p.get("canvas"),
        "diagnostics": diag | {"issues": chk.get("issues", []), "pointer_ledger_counts": ledger_counts,
                               "excluded_by_program": excluded_engine,
                               "candidates": [{"id": c["id"], "kind": c["kind"]} for c in p["candidates"]]},
        "disclaimer": DISCLAIMER,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def _fields(fields: list[dict]) -> str:
    parts = []
    for f in fields:
        unit = f" {f['unit']}" if f.get("unit") else ""
        parts.append(f"{f['name']}: {f['before'] or '—'} → {f['after'] or '—'}{unit}")
    return "; ".join(parts)


def sheet_md(res: dict[str, Any]) -> str:
    a, b = res["a"], res["b"]
    L = [f"### {res.get('title') or res['spid']} · A стр. PDF {a.get('page')} → B стр. PDF {b.get('page')}", "",
         f"A: {a.get('label')} (файл «{a.get('file_name')}»). B: {b.get('label')} (файл «{b.get('file_name')}»).",
         f"Состояние: {res['state_label']}.", ""]
    for n, c in enumerate(res["changes"], 1):
        L.append(f"{n}. **{c['object']}** — {c['summary']}")
        if c["members"]:
            for m in c["members"]:
                L.append(f"   - {m['object']}{' (' + m['place'] + ')' if m.get('place') else ''}: {_fields(m['fields'])}")
        if c.get("limitation"):
            L.append(f"   - Неясно: {c['limitation']}")
    if res["unresolved"]:
        L += ["", "**Требует уточнения**", ""]
        for u in res["unresolved"]:
            L.append(f"- {u['object']}{' (' + u['place'] + ')' if u.get('place') else ''}: {u['question']}")
    if not res["changes"] and not res["unresolved"]:
        L.append(f"_{res['state_label']}._")
    if res.get("declared"):
        L += ["", "Заявлено проектировщиком в разрешении: " + "; ".join(f"{d['izm']}: {d['text']}" for d in res["declared"])]
    L += ["", f"_{res['disclaimer']}_", ""]
    return "\n".join(L)


__all__ = ["sheet_result", "sheet_md", "DISCLAIMER", "sc"]

#!/usr/bin/env python3
"""
frontend_smoke_projects_v2.py — frontend-smoke для v2 read-canary.

Проверяет НЕ просто HTTP 200, а ФАКТИЧЕСКУЮ возможность прочитать ответы так,
как это делает frontend/static/js/app.js. Симулирует цепочку доступа UI и падает,
если ключ, который читает фронт, отсутствует/имеет неверный тип (== тот самый
инцидент: data.projects=undefined под v2-default).

Что проверяется (зеркало app.js):
  refreshProjects:   data.projects (Array, не пустой) + data.object_name
  карточка проекта:  p.project_id / name / section / findings_count /
                     findings_by_severity / pipeline_issues / version_label / ...
  loadProjectVersions: data.versions (Array) + data.latest_version_id ('vN')
  loadProject:       project.pipeline.<stage> (НЕ кидает TypeError) + project_id
  loadFindings:      data.findings (Array)
  loadBlocks:        Object.entries(data.blocks) → an.status (классиф. dict)
  block-map:         data.block_map / block_info / text_evidence

Запуск (ephemeral, без рестарта backend, read-only):
  AUDIT_DATA_DIR=/home/coder/projects/PDF-proverka \
  AUDIT_APP_DATA_DIR=/home/coder/projects/PDF-proverka/backend/app/data \
  python backend/scripts/frontend_smoke_projects_v2.py            # default-read ON
  python backend/scripts/frontend_smoke_projects_v2.py --legacy   # сравнить с legacy

Exit 0 — фронт прочитал бы данные без undefined; 1 — нашёлся бы сломанный доступ.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import urllib.parse

VN = re.compile(r"^v\d+$")


def _q(s):
    return urllib.parse.quote(s, safe="")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--legacy", action="store_true",
                    help="форсить legacy (?storage=legacy) — для сравнения")
    ap.add_argument("--project", default=None,
                    help="конкретный project_id (по умолчанию первый из списка)")
    args = ap.parse_args()

    # ephemeral env: read-only, canary+default-read ON (если не --legacy)
    os.environ.setdefault("PORTAL_AUTH_ENABLED", "false")
    os.environ.setdefault("AUDIT_PROJECTS_V2_SHADOW_API_ENABLED", "true")
    os.environ.setdefault("AUDIT_PROJECTS_V2_READ_CANARY_ENABLED", "true")
    if not args.legacy:
        os.environ.setdefault("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", "true")
    os.environ.pop("AUDIT_STORAGE_BACKEND", None)

    from fastapi.testclient import TestClient
    from backend.app.main import app
    c = TestClient(app, raise_server_exceptions=False)
    suffix = "?storage=legacy" if args.legacy else ""

    fails: list[str] = []
    checks = 0

    def ok(cond, msg):
        nonlocal checks
        checks += 1
        if not cond:
            fails.append(msg)

    def getj(path):
        sep = "&" if ("?" in path) else ""
        p = path + (sep + suffix[1:] if (suffix and "?" in path) else suffix)
        r = c.get(p)
        try:
            return r.status_code, r.json()
        except Exception:
            return r.status_code, None

    mode = "LEGACY" if args.legacy else "V2 default-read"
    print(f"== frontend-smoke ({mode}) ==")

    # 1) refreshProjects: projects.value = data.projects ; objectName = data.object_name
    sc, b = getj("/api/projects")
    ok(sc == 200, f"/api/projects http={sc}")
    ok(isinstance(b, dict) and isinstance(b.get("projects"), list),
       "data.projects must be Array (frontend: projects.value = data.projects)")
    proj_list = (b.get("projects") if isinstance(b, dict) else None) or []
    ok(isinstance(b, dict) and "object_name" in b, "data.object_name present")
    print(f"  projects: {len(proj_list)} | object_name: {b.get('object_name') if isinstance(b,dict) else '-'}")
    if not proj_list:
        print("  (no projects to drill into — list is empty)")
        return _finish(fails, checks)

    # карточка проекта (index.html) — ключи, читаемые в списке/дашборде
    card_keys = ["project_id", "name", "section", "findings_count",
                 "findings_by_severity", "pipeline_issues", "expert_review_status",
                 "findings_review_status", "optimization_review_status",
                 "version_label", "version_id", "latest_version_id",
                 "versions_summary", "has_pdf", "block_count"]
    p0 = next((p for p in proj_list if p.get("project_id") == args.project), proj_list[0])
    for k in card_keys:
        ok(k in p0, f"projects[].{k} missing (card render reads it)")
    ok(isinstance(p0.get("findings_by_severity"), dict), "projects[].findings_by_severity must be object")
    ok(isinstance(p0.get("versions_summary"), list), "projects[].versions_summary must be Array")
    ok(isinstance(p0.get("pipeline_issues"), list), "projects[].pipeline_issues must be Array")
    if not args.legacy:
        ok(VN.match(str(p0.get("version_id"))), f"projects[].version_id must be 'vN', got {p0.get('version_id')}")

    pid = p0["project_id"]
    e = _q(pid)
    print(f"  drill into: {pid}")

    # 2) loadProjectVersions: data.versions (Array), latest_version_id || 'v1'
    sc, v = getj(f"/api/projects/{e}/versions")
    ok(sc == 200, f"/versions http={sc}")
    ok(isinstance(v, dict) and isinstance(v.get("versions"), list),
       "data.versions must be Array (frontend: projectVersions.value = data.versions)")
    ok(isinstance(v, dict) and "latest_version_id" in v,
       "data.latest_version_id present (activeVersionId = data.latest_version_id || 'v1')")
    if isinstance(v, dict):
        for vv in (v.get("versions") or []):
            ok(isinstance(vv, dict) and "version_id" in vv, "versions[].version_id present")
            if not args.legacy:
                ok(VN.match(str(vv.get("version_id"))),
                   f"versions[].version_id must be 'vN' (frontend compares to 'v1'), got {vv.get('version_id')}")

    # 3) loadProject: project.pipeline.<stage> (НЕ кидает TypeError в шаблоне)
    sc, d = getj(f"/api/projects/{e}")
    ok(sc == 200, f"/projects/{{id}} http={sc}")
    pipe = d.get("pipeline") if isinstance(d, dict) else None
    ok(isinstance(pipe, dict),
       "currentProject.pipeline must be object (index.html derefs .gemma_enrichment unguarded)")
    if isinstance(pipe, dict):
        for stage in ("gemma_enrichment", "text_analysis", "blocks_analysis", "findings"):
            ok(stage in pipe, f"currentProject.pipeline.{stage} missing → template TypeError")
    ok(isinstance(d, dict) and "project_id" in d, "currentProject.project_id present (breadcrumb/nav)")

    # 4) loadFindings: data.findings (Array)
    sc, f = getj(f"/api/findings/{e}")
    ok(sc == 200, f"/findings/{{id}} http={sc}")
    ok(isinstance(f, dict) and isinstance(f.get("findings"), list),
       "data.findings must be Array (_findingsAll.value.findings)")

    # 5) loadBlocks: Object.entries(data.blocks) → an.status
    sc, ba = getj(f"/api/tiles/{e}/blocks/analysis")
    ok(sc == 200, f"/blocks/analysis http={sc}")
    blocks = ba.get("blocks") if isinstance(ba, dict) else None
    ok(isinstance(blocks, dict),
       "data.blocks must be object (frontend: Object.entries(data.blocks))")
    if isinstance(blocks, dict):
        sample = list(blocks.values())[:50]
        ok(all(isinstance(x, dict) and "status" in x for x in sample),
           "blocks[*].status present (an.status read per block)")

    # 6) block-map: block_map / block_info / text_evidence
    sc, bm = getj(f"/api/findings/{e}/block-map")
    ok(sc == 200, f"/block-map http={sc}")
    if isinstance(bm, dict):
        for k in ("block_map", "block_info", "text_evidence"):
            ok(k in bm, f"block-map.{k} present")

    return _finish(fails, checks)


def _finish(fails, checks) -> int:
    print(f"  checks: {checks} | broken-access: {len(fails)}")
    for m in fails:
        print(f"  [FAIL] {m}")
    if fails:
        print("FRONTEND-SMOKE: FAIL — UI would break on these reads")
        return 1
    print("FRONTEND-SMOKE: PASS — UI can read all data without undefined")
    return 0


if __name__ == "__main__":
    sys.exit(main())

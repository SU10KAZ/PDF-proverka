"""Таблица политик маршрутов (план доступа, этап 3).

* у каждого маршрута приложения есть явное правило;
* перечень, открытый viewer'у, совпадает с эталонным файлом (любое
  расширение видно в diff ревью);
* ни один изменяющий метод и ни один известный «пишущий на GET» маршрут не
  открыт viewer'у;
* матрица: аноним / viewer без разделов / viewer с просмотром / viewer с
  просмотром и скачиванием / admin — по каждому маршруту, без вызова
  обработчиков (проверяется сама зависимость).
"""
from __future__ import annotations

import asyncio
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from backend.app.core import access
from backend.app.core.access_policy import (
    ADMIN, COMMON, DOWNLOAD, MACHINE, PUBLIC, SELF, VIEW,
    enforce_route_policy, has_explicit_rule, policy_for,
)

SNAPSHOT = Path(__file__).parent / "fixtures" / "access_policy_viewer_allowlist.txt"

# GET-маршруты, которые по коду пишут или запускают обработку: viewer'у закрыты.
KNOWN_MUTATING_GETS = {
    "/api/version-comparison/objects/{object_id}/projects/{project_id}/matrix",
    "/api/version-comparison/ai/settings",
    "/api/version-comparison/md-reports/runtime",
    "/api/version-comparison/ai-sheet-changes/queue",
    "/api/version-comparison/worker-status",
    "/api/version-comparison/objects/{object_id}/series/{series_id}/ai-sheet-changes/estimate",
    "/api/version-comparison/objects/{object_id}/runs/{run_id}/ai/jobs/{job_id}/sheets/{spid}/packet/{name:path}",
    "/api/version-comparison/objects/{object_id}/runs/{run_id}/sheets/{spid}/changes/packet/{name:path}",
    "/api/version-comparison/objects/{object_id}/runs/{run_id}/ai/jobs/{job_id}/sheets/{spid}/check-crop/{name}",
    "/api/version-comparison/objects/{object_id}/stage-queue",
    "/api/version-comparison/objects/{object_id}/ai-sheet-match",
    "/api/version-comparison/objects/{object_id}/batches",
    "/api/stage-comparison/sessions",
    "/api/stage-comparison/sessions/{session_id}/production/batch",
    "/api/stage-comparison/production/ai-modes",
    "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/block-mapping/runs/{run_id}/bridge-check",
    "/api/audit/live-status", "/api/audit/batch/status", "/api/audit/account",
    "/api/audit/{project_id:path}/log", "/api/audit/{project_id:path}/prompts", "/api/audit/templates",
    "/api/audit/{project_id:path}/status", "/api/audit/{project_id:path}/resume-info",
    "/api/optimization/section/{section_code}/pipeline/status",
    "/api/tiles/{project_id:path}/blocks/llm-text/{block_id}",
    "/api/usage/global", "/api/action-log", "/api/users",
    "/api/discussions/{project_id:path}/{item_id}",
    "/api/discussions/{project_id:path}/list",
    "/api/export/download/{filename}",
    "/api/projects/scan",
}


@pytest.fixture(scope="module")
def routes():
    # Маршруты исполнителей монтируются только при включённом флаге; флаг
    # ставится на время импорта и снимается, чтобы не протечь в другие тесты.
    prev = os.environ.get("DISTRIBUTED_WORKERS_ENABLED")
    os.environ.setdefault("DISTRIBUTED_WORKERS_ENABLED", "true")
    try:
        from backend.app.main import app
    finally:
        if prev is None:
            os.environ.pop("DISTRIBUTED_WORKERS_ENABLED", None)
        else:
            os.environ["DISTRIBUTED_WORKERS_ENABLED"] = prev
    out = []
    for r in app.routes:
        for m in sorted(getattr(r, "methods", None) or []):
            if m != "HEAD":
                out.append((m, r.path, r))
    return out


def test_every_route_has_explicit_rule(routes):
    missing = [(m, p) for m, p, _ in routes if not has_explicit_rule(m, p)]
    assert missing == []


def _viewer_allowlist(routes) -> list[str]:
    lines = []
    for m, p, _ in routes:
        pol = policy_for(m, p)
        if pol.kind in (VIEW, DOWNLOAD, COMMON, SELF, PUBLIC):
            lines.append(f"{pol}\t{m}\t{p}")
    return sorted(set(lines))


def test_viewer_allowlist_matches_snapshot(routes):
    current = _viewer_allowlist(routes)
    if os.environ.get("UPDATE_ACCESS_SNAPSHOT") == "1":
        SNAPSHOT.parent.mkdir(parents=True, exist_ok=True)
        SNAPSHOT.write_text("\n".join(current) + "\n", encoding="utf-8")
    expected = SNAPSHOT.read_text(encoding="utf-8").splitlines()
    assert current == expected, "перечень, открытый viewer'у, изменился — обновите эталон осознанно"


def test_no_mutating_route_open_to_viewer(routes):
    for m, p, _ in routes:
        pol = policy_for(m, p)
        if m != "GET":
            assert pol.kind in (ADMIN, PUBLIC, SELF, MACHINE), (m, p, pol)
        if p in KNOWN_MUTATING_GETS:
            assert pol.kind == ADMIN, (m, p, pol)
    known = {p for _, p, _ in routes}
    stale = sorted(KNOWN_MUTATING_GETS - known)
    assert stale == [], f"эталон пишущих GET устарел: {stale}"


def _ctx(kind):
    if kind == "admin":
        return access.AccountContext(mode="accounts", login="a", role="admin", account_id=1)
    sections = {
        "viewer_none": {},
        "viewer_view": {s: (True, False) for s in access.SECTIONS},
        "viewer_full": {s: (True, s not in access.SECTIONS_WITHOUT_DOWNLOADS) for s in access.SECTIONS},
    }[kind]
    return access.AccountContext(mode="accounts", login="v", role="viewer", account_id=2, sections=sections)


def _conn(method, path, route, ctx):
    return SimpleNamespace(
        scope={"type": "http", "route": route, "method": method, "path_params": {}},
        url=SimpleNamespace(path=path), headers={}, query_params={},
        state=SimpleNamespace(portal_account=ctx), cookies={},
    )


def _allowed(conn) -> bool:
    try:
        asyncio.run(enforce_route_policy(conn))
        return True
    except HTTPException as exc:
        assert exc.status_code in (401, 403)
        return False


@pytest.mark.parametrize("kind", ["viewer_none", "viewer_view", "viewer_full", "admin"])
def test_access_matrix(routes, kind, monkeypatch):
    monkeypatch.setenv("PORTAL_AUTH_ENABLED", "true")
    monkeypatch.setenv("PORTAL_AUTH_MODE", "accounts")
    ctx = _ctx(kind)
    for m, p, route in routes:
        pol = policy_for(m, p)
        got = _allowed(_conn(m, p, route, ctx))
        if kind == "admin" or pol.kind in (PUBLIC, MACHINE, SELF, COMMON):
            expected = True
        elif pol.kind == ADMIN:
            expected = False
        elif pol.kind == VIEW:
            expected = kind in ("viewer_view", "viewer_full")
        else:  # DOWNLOAD
            expected = kind == "viewer_full"
        assert got is expected, (kind, m, p, str(pol))


def test_anonymous_gets_401_on_protected(routes, monkeypatch):
    monkeypatch.setenv("PORTAL_AUTH_ENABLED", "true")
    monkeypatch.setenv("PORTAL_AUTH_MODE", "accounts")
    monkeypatch.setattr("backend.app.core.portal_auth.resolve_request_account_sync", lambda r: None)
    for m, p, route in routes:
        pol = policy_for(m, p)
        if pol.kind in (PUBLIC, MACHINE):
            continue
        conn = _conn(m, p, route, None)
        with pytest.raises(HTTPException) as exc:
            asyncio.run(enforce_route_policy(conn))
        assert exc.value.status_code == 401

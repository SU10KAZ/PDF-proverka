from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from backend.app.api.routers import projects as projects_router
from backend.app.core import portal_auth
from backend.app.main import app
from backend.app.services.storage.storage_write_facade import V2Target
from backend.app.services.storage.v2_primary_wiring import backup_version_before_destructive

_WMODE = "AUDIT_PROJECTS_V2_WRITE_MODE"
_V2DIR = "AUDIT_PROJECTS_V2_DIR"


def _tree_digest(root: Path) -> str:
    h = hashlib.sha256()
    for p in sorted(root.rglob("*")):
        rel = p.relative_to(root).as_posix()
        h.update(rel.encode("utf-8"))
        if p.is_dir():
            h.update(b"/\0")
        else:
            h.update(b"\0")
            h.update(p.read_bytes())
    return h.hexdigest()


def _make_v2_doc(v2_root: Path, code: str = "DOC-REST") -> tuple[V2Target, Path]:
    target = V2Target("OBJ", "EOM", code, "v001")
    doc = target.doc_dir(v2_root)
    doc.mkdir(parents=True, exist_ok=True)
    (doc / "document.json").write_text(json.dumps({
        "schema_version": 1,
        "document_code": code,
        "object_id": "obj-rest",
        "current_version": "v001",
        "versions": [{"version_id": "v001", "version_no": 1}],
    }), encoding="utf-8")
    vdir = target.version_dir(v2_root)
    (vdir / "01_input").mkdir(parents=True, exist_ok=True)
    (vdir / "03_analysis" / "latest").mkdir(parents=True, exist_ok=True)
    (vdir / "01_input" / f"{code}.pdf").write_bytes(b"%PDF-1.4 restore")
    (vdir / "03_analysis" / "latest" / "03_findings.json").write_text(
        json.dumps({"findings": [{"id": "F-REST"}]}), encoding="utf-8",
    )
    (vdir / "version.json").write_text(
        json.dumps({"version_id": "v001", "project_info": {"name": code}}),
        encoding="utf-8",
    )
    return target, vdir


def test_restore_clean_endpoint_restores_backup(monkeypatch, tmp_path):
    v2 = tmp_path / "projects_v2"
    target, vdir = _make_v2_doc(v2)
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    monkeypatch.setenv(_V2DIR, str(v2))

    backup_id = backup_version_before_destructive(target, v2, "clean_project_data")
    expected = _tree_digest(v2 / "_system" / "destructive_backups" / backup_id)
    shutil.rmtree(vdir / "03_analysis")

    client = TestClient(app)
    resp = client.post(
        "/api/projects/DOC-REST/restore-clean",
        json={"backup_id": backup_id, "version_id": "v001"},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["backup_id"] == backup_id
    assert body["pre_restore_backup_id"]
    assert _tree_digest(vdir) == expected


def test_restore_clean_auth_dependency_requires_session_when_enabled(monkeypatch):
    settings = portal_auth.PortalSettings(
        enabled=True,
        users={},
        secret="secret",
        ttl_seconds=3600,
        cookie_secure_mode="false",
        cookie_name="portal_session",
    )
    monkeypatch.setattr(portal_auth, "get_settings", lambda: settings)
    monkeypatch.setattr(portal_auth, "request_username", lambda request, settings: None)

    with pytest.raises(HTTPException) as exc:
        projects_router._require_restore_clean_auth(object())

    assert exc.value.status_code == 401

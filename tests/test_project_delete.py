"""Тесты жёсткого удаления проекта (кнопка «Удалить» в меню «Изменить»).

Покрывает:
  - storage_write_facade.remove_project_from_v2: удаляет v2-документ + записи
    old_to_new_map по границе сегмента пути (НЕ задевает `X V1` при удалении `X`);
    no-op в legacy-режиме;
  - project_service.delete_project: удаляет legacy-папку, зовёт v2-remove, чистит
    hidden, raises на отсутствующем проекте;
  - DELETE /api/projects/{id}: 200 ok / 409 во время аудита / 404 если нет.

Run:
    python -m pytest tests/test_project_delete.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.services.common import project_service, version_service  # noqa: E402
import backend.app.services.storage.storage_write_facade as swf  # noqa: E402


# ─── facade.remove_project_from_v2: boundary matching + legacy no-op ──────────


def _make_v2(tmp_path):
    """Фейковый v2-root: 2 документа X и 'X V1' + map с их записями."""
    v2 = tmp_path / "projects_v2"
    docs = v2 / "objects" / "OBJ" / "disciplines" / "AR" / "documents"
    legacy = tmp_path / "projects" / "OBJ" / "AR"
    for code in ("X", "X V1"):
        (docs / code).mkdir(parents=True)
        (docs / code / "document.json").write_text("{}", encoding="utf-8")
        (legacy / code).mkdir(parents=True)
    (v2 / "_system").mkdir(parents=True)
    mp = {"migrations": [
        {"object_id": "o", "discipline": "AR", "document_code": "X", "kind": "plain",
         "version_id": "v001", "legacy_folder_path": str(legacy / "X"),
         "v2_document_dir": str(docs / "X")},
        {"object_id": "o", "discipline": "AR", "document_code": "X V1", "kind": "plain",
         "version_id": "v001", "legacy_folder_path": str(legacy / "X V1"),
         "v2_document_dir": str(docs / "X V1")},
    ]}
    (v2 / "_system" / "old_to_new_map.json").write_text(json.dumps(mp), encoding="utf-8")
    return v2, docs, legacy


def test_remove_from_v2_boundary_does_not_touch_sibling(tmp_path, monkeypatch):
    v2, docs, legacy = _make_v2(tmp_path)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "dual_write_shadow")
    f = swf.StorageWriteFacade()
    f._v2_root_override = v2

    res = f.remove_project_from_v2(legacy / "X")
    assert res.v2_ok is True
    # X удалён, 'X V1' НЕ задет (граница сегмента)
    assert not (docs / "X").exists()
    assert (docs / "X V1").exists()
    mp = json.loads((v2 / "_system" / "old_to_new_map.json").read_text(encoding="utf-8"))
    codes = [e["document_code"] for e in mp["migrations"]]
    assert codes == ["X V1"]


def test_remove_from_v2_legacy_mode_is_noop(tmp_path, monkeypatch):
    v2, docs, legacy = _make_v2(tmp_path)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "legacy")
    f = swf.StorageWriteFacade()
    f._v2_root_override = v2
    # safe-обёртка: в legacy режиме ничего не делает
    res = swf.remove_project_from_v2_safe(legacy / "X")
    assert res is None
    assert (docs / "X").exists()  # не тронут


def test_remove_from_v2_container_shared_doc(tmp_path, monkeypatch):
    """Контейнер: 2 версии делят один v2_document_dir → удаляется один раз,
    обе map-записи уходят."""
    v2 = tmp_path / "projects_v2"
    docs = v2 / "objects" / "OBJ" / "disciplines" / "AR" / "documents"
    (docs / "C").mkdir(parents=True)
    (docs / "C" / "document.json").write_text("{}", encoding="utf-8")
    (v2 / "_system").mkdir(parents=True)
    cont = tmp_path / "projects" / "OBJ" / "AR" / "C(main)"
    (cont / "C").mkdir(parents=True)
    (cont / "C V2").mkdir(parents=True)
    mp = {"migrations": [
        {"document_code": "C", "version_id": "v001", "legacy_folder_path": str(cont / "C"),
         "v2_document_dir": str(docs / "C")},
        {"document_code": "C", "version_id": "v002", "legacy_folder_path": str(cont / "C V2"),
         "v2_document_dir": str(docs / "C")},
    ]}
    (v2 / "_system" / "old_to_new_map.json").write_text(json.dumps(mp), encoding="utf-8")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "dual_write_shadow")
    f = swf.StorageWriteFacade(); f._v2_root_override = v2
    f.remove_project_from_v2(cont)
    assert not (docs / "C").exists()
    mp2 = json.loads((v2 / "_system" / "old_to_new_map.json").read_text(encoding="utf-8"))
    assert mp2["migrations"] == []


# ─── project_service.delete_project ──────────────────────────────────────────


def test_delete_project_removes_legacy_and_calls_v2(tmp_path, monkeypatch):
    proj = tmp_path / "projects" / "AR" / "MISTAKE"
    (proj / "_output").mkdir(parents=True)
    (proj / "doc.pdf").write_bytes(b"%PDF")
    (proj / "project_info.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(project_service, "resolve_project_dir", lambda pid, **kw: proj)
    monkeypatch.setattr(version_service, "container_dir_for", lambda d: None)
    monkeypatch.setattr(project_service, "invalidate_project_cache", lambda: None)
    calls = []
    monkeypatch.setattr(swf, "remove_project_from_v2_safe", lambda p, **kw: calls.append(str(p)) or None)

    res = project_service.delete_project("AR/MISTAKE")
    assert res["project_id"] == "AR/MISTAKE"
    assert not proj.exists()              # legacy удалён
    assert calls and calls[0].endswith("MISTAKE")  # v2-remove вызван


def test_delete_project_missing_raises_valueerror(tmp_path, monkeypatch):
    """resolve поднимает ProjectNotResolvedError (RuntimeError) → delete_project
    конвертит в ValueError, чтобы endpoint вернул 404, а не 500."""
    def _raise(pid, **kw):
        raise project_service.ProjectNotResolvedError(f"not found: {pid}")
    monkeypatch.setattr(project_service, "resolve_project_dir", _raise)
    with pytest.raises(ValueError):
        project_service.delete_project("AR/NOPE")


def test_delete_project_v2_primary_backs_up_and_removes(tmp_path, monkeypatch):
    """В v2-primary удаление идёт v2-native: backup версии → удаление doc_dir,
    без зависимости от legacy (legacy уже недоступен)."""
    from backend.app.services.storage.storage_write_facade import V2Target
    from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter

    v2root = tmp_path / "projects_v2"
    target = V2Target(object_folder="OBJ_F", discipline="EOM",
                      document_code="CODE-1", version_id="v001")
    vdir = target.version_dir(v2root)
    vdir.mkdir(parents=True)
    (vdir / "03_findings.json").write_text("{}", encoding="utf-8")
    doc_dir = target.doc_dir(v2root)

    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2root))
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "projects_v2_primary")

    doc = {"object_folder": "OBJ_F", "discipline": "EOM", "document_code": "CODE-1",
           "versions": [{"version_id": "v001"}]}
    monkeypatch.setattr(ProjectsV2Adapter, "find_document_by_project_id",
                        lambda self, pid, **kw: doc)
    monkeypatch.setattr(ProjectsV2Adapter, "resolve_version_id",
                        lambda self, d, vid: "v001")
    # legacy уже удалён → resolve поднимает not-found (best-effort ветка глотает)
    def _no_legacy(pid, **kw):
        raise project_service.ProjectNotResolvedError("legacy gone")
    monkeypatch.setattr(project_service, "resolve_project_dir", _no_legacy)
    monkeypatch.setattr(project_service, "invalidate_project_cache", lambda: None)

    res = project_service.delete_project("EOM/CODE-1")

    assert res["deleted_v2_doc"] is not None
    assert not doc_dir.exists()                    # v2-документ удалён
    assert res["backup_ids"]                       # backup создан
    backups = list((v2root / "_system" / "destructive_backups").iterdir())
    assert backups and (backups[0] / "03_findings.json").exists()  # backup восстановим


# ─── DELETE endpoint mapping ─────────────────────────────────────────────────


@pytest.fixture
def client():
    from backend.app.main import app
    return TestClient(app)


def test_http_delete_ok(client, monkeypatch):
    import backend.app.api.routers.projects as r
    monkeypatch.setattr(r.project_service, "delete_project",
                        lambda pid: {"project_id": pid, "deleted_legacy": "/x", "v2": None})
    from backend.app.pipeline.manager import pipeline_manager
    monkeypatch.setattr(pipeline_manager, "is_running", lambda pid: False)
    resp = client.delete("/api/projects/AR%2FMISTAKE")
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "ok"


def test_http_delete_running_audit_409(client, monkeypatch):
    from backend.app.pipeline.manager import pipeline_manager
    monkeypatch.setattr(pipeline_manager, "is_running", lambda pid: True)
    resp = client.delete("/api/projects/AR%2FBUSY")
    assert resp.status_code == 409, resp.text


def test_http_delete_missing_404(client, monkeypatch):
    import backend.app.api.routers.projects as r
    def _raise(pid):
        raise ValueError("not found")
    monkeypatch.setattr(r.project_service, "delete_project", _raise)
    from backend.app.pipeline.manager import pipeline_manager
    monkeypatch.setattr(pipeline_manager, "is_running", lambda pid: False)
    resp = client.delete("/api/projects/AR%2FGHOST")
    assert resp.status_code == 404, resp.text

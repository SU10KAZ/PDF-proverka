from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.routers import stage_comparison as router_mod


def test_folder_upload_forwards_intermediate_backup_flag(monkeypatch):
    captured: dict[str, object] = {}

    def fake_replace(object_id, stage_name, uploads, folder_name, retain_backup):
        captured.update(
            object_id=object_id,
            stage_name=stage_name,
            paths=[path for _, path in uploads],
            folder_name=folder_name,
            retain_backup=retain_backup,
        )
        return {"status": "ok"}

    monkeypatch.setattr(router_mod.stage_upload_mod, "replace_stage_from_folder", fake_replace)
    app = FastAPI()
    app.include_router(router_mod.router)

    response = TestClient(app).post(
        "/api/stage-comparison/objects/object-1/stages/stage_1/upload-folder",
        data={
            "relative_paths": '["selected/project.pdf"]',
            "folder_name": "project",
            "retain_backup": "false",
        },
        files=[("files", ("project.pdf", b"%PDF-test", "application/pdf"))],
    )

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert captured == {
        "object_id": "object-1",
        "stage_name": "stage_1",
        "paths": ["selected/project.pdf"],
        "folder_name": "project",
        "retain_backup": False,
    }

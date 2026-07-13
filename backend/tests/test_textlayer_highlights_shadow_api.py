import asyncio
import json

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from backend.app.api.routers import findings as findings_router
from backend.app.services.findings import findings_service


def test_service_reads_textlayer_shadow_from_version_output(tmp_path, monkeypatch):
    payload = {
        "mode": "shadow",
        "records": [
            {
                "finding_id": "F-003",
                "computed_highlight_regions": [
                    {"block_id": "4AWC-VWW9-7AH", "x": 0.1, "y": 0.2, "w": 0.03, "h": 0.04},
                ],
            },
        ],
    }
    artifact = tmp_path / findings_service.TEXTLAYER_HIGHLIGHTS_SHADOW_FILENAME
    artifact.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(findings_service, "_get_version_output_dir", lambda *_args, **_kwargs: tmp_path)

    assert findings_service.get_textlayer_highlights_shadow("project", version_id="v2") == payload


def test_service_returns_none_when_textlayer_shadow_is_absent(tmp_path, monkeypatch):
    monkeypatch.setattr(findings_service, "_get_version_output_dir", lambda *_args, **_kwargs: tmp_path)
    monkeypatch.setattr(
        findings_service.version_service,
        "resolve_project_version_context",
        lambda *_args, **_kwargs: {"version_dir": tmp_path},
    )

    assert findings_service.get_textlayer_highlights_shadow("project") is None


def test_service_reads_migrated_v1_shadow_from_its_legacy_folder(tmp_path, monkeypatch):
    version_output = tmp_path / "v2-output"
    version_output.mkdir()
    version_dir = tmp_path / "v001"
    version_dir.mkdir()
    legacy_dir = tmp_path / "legacy-project"
    legacy_output = legacy_dir / "_output"
    legacy_output.mkdir(parents=True)
    payload = {"mode": "shadow", "records": [{"finding_id": "F-003"}]}
    (legacy_output / findings_service.TEXTLAYER_HIGHLIGHTS_SHADOW_FILENAME).write_text(
        json.dumps(payload), encoding="utf-8",
    )
    (version_dir / "version.json").write_text(
        json.dumps({"version_id": "v001", "legacy_folder_path": str(legacy_dir)}),
        encoding="utf-8",
    )
    monkeypatch.setattr(findings_service, "_get_version_output_dir", lambda *_args, **_kwargs: version_output)
    monkeypatch.setattr(
        findings_service.version_service,
        "resolve_project_version_context",
        lambda *_args, **_kwargs: {"version_dir": version_dir},
    )

    assert findings_service.get_textlayer_highlights_shadow("project", version_id="v001") == payload


def test_router_exposes_textlayer_shadow_without_writing(monkeypatch):
    payload = {"mode": "shadow", "records": []}
    monkeypatch.setattr(findings_router, "_validate_version_id", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        findings_router.findings_service,
        "get_textlayer_highlights_shadow",
        lambda *_args, **_kwargs: payload,
    )

    app = FastAPI()
    app.include_router(findings_router.router)
    response = TestClient(app).get("/api/findings/project/textlayer-highlights-shadow")

    assert response.status_code == 200
    assert response.json() == payload


def test_router_returns_404_when_shadow_is_absent(monkeypatch):
    monkeypatch.setattr(findings_router, "_validate_version_id", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        findings_router.findings_service,
        "get_textlayer_highlights_shadow",
        lambda *_args, **_kwargs: None,
    )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(findings_router.get_textlayer_highlights_shadow("project", version_id=None))

    assert exc_info.value.status_code == 404

"""Ручные сопоставления сохраняются целиком и запускают один пересчёт."""
from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.routers import version_comparison as router
from backend.app.services.version_comparison import jobs, store


@pytest.fixture
def batch_client(tmp_path, monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path / "vc"))
    monkeypatch.setattr(store, "load_catalog", lambda _: {})
    monkeypatch.setattr(store, "project", lambda *_: {})
    rebuild = Mock(return_value={"state": "building"})
    monkeypatch.setattr(jobs, "matrix", rebuild)
    app = FastAPI()
    app.include_router(router.router)
    return TestClient(app), rebuild


URL = "/api/version-comparison/objects/obj/projects/project/links/batch"
EDITS = [{"op": "move", "page": ["b", 3], "to_row_anchor": ["a", 1]},
         {"op": "absent", "row_anchor": ["a", 2], "version": "c"}]


def test_batch_saves_all_edits_before_one_rebuild(batch_client):
    client, rebuild = batch_client
    response = client.post(URL, json={"edits": EDITS})
    assert response.status_code == 200
    assert response.json()["state"] == "building"
    rebuild.assert_called_once_with("obj", "project")
    saved = jobs._load_lineage("obj", "project")["edits"]
    assert [{k: v for k, v in e.items() if k != "at"} for e in saved] == EDITS


@pytest.mark.parametrize("bad", [{"op": "move", "page": ["b", 4]},
                                 {"op": "detach", "page": ["b", 0]},
                                 {"op": "absent", "row_anchor": ["a", 1]}])
def test_invalid_batch_does_not_partially_save(batch_client, bad):
    client, rebuild = batch_client
    response = client.post(URL, json={"edits": [EDITS[0], bad]})
    assert response.status_code == 400
    assert jobs._load_lineage("obj", "project").get("edits", []) == []
    rebuild.assert_not_called()


def test_empty_batch_is_rejected(batch_client):
    client, rebuild = batch_client
    assert client.post(URL, json={"edits": []}).status_code == 422
    rebuild.assert_not_called()

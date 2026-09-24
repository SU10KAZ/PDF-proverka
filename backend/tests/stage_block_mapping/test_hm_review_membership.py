"""B5: POST review accepts only blocks of the region (old/new_block_ids ⊆ allowed_block_ids).

One review with a block outside its region makes the run's history unusable for the bridge
forever (human_mapping_bridge BLOCK_OUTSIDE_REGION), so the Human Mapping router refuses it
with the same error shape as REGION_NOT_FOUND. Runs on the synthetic world of conftest.py.
"""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.tests.stage_block_mapping.conftest import OID, PID, RUN, SID


@pytest.fixture
def client(world, monkeypatch):
    from backend.app.api.routers import human_mapping
    from backend.app.services.project_change_v3 import scope
    monkeypatch.setattr(scope, "sessions_for_object", lambda object_id: [SID] if object_id == OID else [])
    app = FastAPI()
    app.include_router(human_mapping.api_router)
    return TestClient(app)


def post(client, body):
    return client.post(f"/api/human-mapping/objects/{OID}/comparisons/{PID}/reviews?session_id={SID}&run_id={RUN}",
                       json={"status": "HUMAN_CONFIRMED", "comment": "", **body})


def reviews(world):
    path = world["run_dir"] / "human_mapping" / "reviews.jsonl"
    return path.read_bytes() if path.exists() else b""


@pytest.mark.parametrize("old, new", [(["o1"], ["n1"]), (["o3"], ["n2"]), (["o3", "o1"], ["n1"]), (["o3"], ["ns"])])
def test_block_outside_the_region_is_refused_and_nothing_is_written(world, client, old, new):
    before = reviews(world)
    response = post(client, {"region_id": "R2", "old_block_ids": old, "new_block_ids": new})
    assert response.status_code == 400
    assert response.json() == {"detail": {"error": "REVIEW_BLOCK_NOT_IN_REGION", "ok": False}}
    assert reviews(world) == before


def test_non_string_ids_are_refused_the_same_way(world, client):
    response = post(client, {"region_id": "R2", "old_block_ids": [{"id": "o3"}], "new_block_ids": ["n1"]})
    assert response.status_code == 400
    assert response.json() == {"detail": {"error": "REVIEW_BLOCK_NOT_IN_REGION", "ok": False}}


def test_unknown_region_keeps_its_own_error(world, client):
    response = post(client, {"region_id": "R9", "old_block_ids": ["o1"], "new_block_ids": ["n1"]})
    assert response.status_code == 400
    assert response.json() == {"detail": {"error": "REGION_NOT_FOUND", "ok": False}}


def test_members_of_the_region_are_accepted(world, client):
    response = post(client, {"region_id": "R1", "old_block_ids": ["o1", "o2"], "new_block_ids": ["n2"]})
    assert response.status_code == 200, response.text
    row = response.json()
    assert (row["region_id"], row["old_block_ids"], row["new_block_ids"], row["run_id"]) == ("R1", ["o1", "o2"], ["n2"], RUN)
    assert b'"review_id"' in reviews(world)

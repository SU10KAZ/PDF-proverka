from copy import deepcopy

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.routers import optimization
from backend.app.services import section_optimization_pipeline_service as pipeline
from backend.app.services import section_optimization_replication_service as replication
from backend.app.services import section_optimization_service as service


@pytest.mark.parametrize("cached", [True, False])
def test_section_endpoint_returns_library_and_readiness_for_cached_and_new_snapshot(monkeypatch, cached):
    snapshot = {
        "meta": {"project_count": 1}, "analysis_stages": [],
        "specification_rows": [{"project_id": "P1", "version_id": "v002", "row_id": "R1"}],
        "historical_optimizations": [{"history_id": "H1", "status": "requires_review"}],
        "capabilities": {"optimization_history": True},
    }
    seen = []

    def checked(value):
        def read(section, *, object_id):
            assert (section, object_id) == ("EOM", "O1")
            return deepcopy(value)
        return read

    monkeypatch.setattr(pipeline, "get_pipeline_state", checked({"status": "completed"}))
    monkeypatch.setattr(pipeline, "get_latest_snapshot", checked(snapshot if cached else None))
    monkeypatch.setattr(service, "build_section_optimization", checked(snapshot))
    monkeypatch.setattr(pipeline, "store_latest_snapshot", lambda *a, **kw: seen.append((a, kw)))
    monkeypatch.setattr(replication, "list_replications", checked([]))
    app = FastAPI()
    app.include_router(optimization.router)
    with TestClient(app) as client:
        response = client.get("/api/optimization/section/EOM?object_id=O1")

    assert response.status_code == 200
    data = response.json()
    assert data["solution_library"]["entries"] == []
    assert data["pilot_readiness"]["metrics"]["specification_rows"] == 1
    assert data["historical_optimizations"] == snapshot["historical_optimizations"]
    assert data["capabilities"]["optimization_history"] is True
    assert len(seen) == (0 if cached else 1)

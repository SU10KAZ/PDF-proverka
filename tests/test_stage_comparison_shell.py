from __future__ import annotations

import io
from pathlib import Path

import fitz
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.routers import stage_comparison as router_mod
from backend.app.services.stage_comparison import store


def _pdf_bytes(label: str) -> bytes:
    document = fitz.open()
    page = document.new_page(width=240, height=160)
    page.insert_text((24, 48), label)
    payload = document.tobytes()
    document.close()
    return payload


def _app() -> FastAPI:
    app = FastAPI()
    app.include_router(router_mod.router)
    return app


def test_shell_imports_and_object_list_opens(monkeypatch):
    expected = {"roots": ["/safe"], "items": [{"id": "o1", "name": "Object"}], "count": 1}
    monkeypatch.setattr(router_mod.objects_mod, "list_objects", lambda: expected)

    response = TestClient(_app()).get("/api/stage-comparison/objects")

    assert response.status_code == 200
    assert response.json() == expected
    assert store.SHELL_KIND == "stage_comparison_shell"


def test_pdf_list_pair_and_vector_page_only(tmp_path, monkeypatch):
    comparison_root = tmp_path / "comparison-runtime"
    stage_1 = tmp_path / "object" / "comparison" / "stage_1"
    stage_2 = tmp_path / "object" / "comparison" / "stage_2"
    stage_1.mkdir(parents=True)
    stage_2.mkdir(parents=True)
    left_pdf = stage_1 / "project.pdf"
    right_pdf = stage_2 / "working.pdf"
    left_pdf.write_bytes(_pdf_bytes("stage 1"))
    right_pdf.write_bytes(_pdf_bytes("stage 2"))
    # The matcher reads only the ready-made Page/Sheet index from results HTML.
    html_index = '<ol><li><a href="#page-0">Sheet 7 — Корпус 4. План 1 этажа</a></li></ol>'
    (stage_1 / "project_results.html").write_text(html_index, encoding="utf-8")
    (stage_2 / "working_results.html").write_text(
        html_index.replace("Sheet 7", "Sheet 99"), encoding="utf-8"
    )
    (stage_2 / "blocks.json").write_text("not-json-on-purpose", encoding="utf-8")
    monkeypatch.setenv("COMPARISON_ROOT", str(comparison_root))
    monkeypatch.setenv("AUDIT_STAGE_COMPARISON_ROOTS", str(tmp_path))

    client = TestClient(_app())
    created = client.post(
        "/api/stage-comparison/sessions",
        json={"stage_a_path": str(stage_1), "stage_b_path": str(stage_2)},
    )
    assert created.status_code == 200
    session = created.json()
    assert [item["filename"] for item in session["documents"]["stage_1"]] == ["project.pdf"]
    assert [item["filename"] for item in session["documents"]["stage_2"]] == ["working.pdf"]
    assert session["documents"]["stage_1"][0]["html_path"] == str(stage_1 / "project_results.html")

    pair_response = client.post(
        f"/api/stage-comparison/sessions/{session['id']}/pairs",
        json={"left_pdf": str(left_pdf), "right_pdf": str(right_pdf)},
    )
    assert pair_response.status_code == 200
    pair_view = pair_response.json()
    assert pair_view["left_page_count"] == 1
    assert pair_view["right_page_count"] == 1

    pair_id = pair_view["pair"]["id"]
    vector = client.get(
        f"/api/stage-comparison/sessions/{session['id']}/pairs/{pair_id}/page-svg",
        params={"side": "left", "page": 1},
    )
    assert vector.status_code == 200
    assert vector.headers["content-type"].startswith("image/svg+xml")
    assert b"<svg" in vector.content

    processed = client.post(
        f"/api/stage-comparison/sessions/{session['id']}/pairs/{pair_id}/sheet-match-suggestions"
    )
    assert processed.status_code == 200
    assert processed.json()["suggestions"]["suggestions"][0]["primary_right_page"] == 1

    saved = client.put(
        f"/api/stage-comparison/sessions/{session['id']}/pairs/{pair_id}/sheet-links",
        json={
            "links": [{
                "left_pages": [1], "right_pages": [1], "source": "manual",
                "confidence": "manual", "reason": ["user_corrected"],
            }],
            "unlinked_left_pages": [],
        },
    )
    assert saved.status_code == 200
    assert saved.json()["links"]["links"][0]["source"] == "manual"

    removed_paths = [
        f"/api/stage-comparison/sessions/{session['id']}/pairs/{pair_id}/page-image?side=left&page=1",
        f"/api/stage-comparison/sessions/{session['id']}/comparison-statuses",
        "/api/stage-comparison/change-regions-cleanup-pilot",
        "/api/stage-comparison/change-groups-pilot",
        "/api/stage-comparison/semantic-diff-pilot",
        "/api/stage-comparison/semantic-diff-v6a1-pilot",
        "/api/stage-comparison/semantic-diff-v6a2-mass",
        "/api/stage-comparison/pipeline-v2/run",
    ]
    for path in removed_paths:
        assert client.get(path).status_code == 404, path

    stored = list(comparison_root.rglob("*.json"))
    assert stored
    assert any(path.name == "sheet_match_suggestions.json" for path in stored)
    assert any(path.name == "sheet_links.json" for path in stored)
    assert not any(token in path.as_posix() for path in stored for token in ("findings", "diagnostic"))

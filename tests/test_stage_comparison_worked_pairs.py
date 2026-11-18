"""Отметка «Отработано» у пары доски сравнения стадий (18.11.2026)."""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.routers import stage_comparison as router_mod
from backend.app.services.stage_comparison import board_state


def _pdf(path: Path, label: str) -> Path:
    import fitz
    document = fitz.open()
    document.new_page(width=200, height=100).insert_text((20, 40), label)
    document.save(path)
    document.close()
    return path


@pytest.fixture
def client_env(tmp_path, monkeypatch):
    stage_1 = tmp_path / "object" / "comparison" / "stage_1"
    stage_2 = tmp_path / "object" / "comparison" / "stage_2"
    stage_1.mkdir(parents=True)
    stage_2.mkdir(parents=True)
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "runtime"))
    monkeypatch.setenv("AUDIT_STAGE_COMPARISON_ROOTS", str(tmp_path))
    app = FastAPI()
    app.include_router(router_mod.router)
    client = TestClient(app)
    left = _pdf(stage_1 / "P.pdf", "left")
    right = _pdf(stage_2 / "R.pdf", "right")
    session = client.post("/api/stage-comparison/sessions", json={
        "stage_a_path": str(stage_1), "stage_b_path": str(stage_2),
    }).json()
    return client, session, left, right


def test_marked_pair_shows_engineer_and_survives_reload(client_env, monkeypatch):
    client, session, left, right = client_env
    monkeypatch.setattr("backend.app.core.attribution.actor_label",
                        lambda _request, prefer="name": "Иванов И. И." if prefer == "name" else "staff-7")
    url = f"/api/stage-comparison/sessions/{session['id']}/worked-pairs"
    assert session["document_pairing"]["worked_pairs"] == []

    marked = client.post(url, json={"left_pdf": str(left), "right_pdf": str(right)})

    assert marked.status_code == 200, marked.text
    [mark] = marked.json()["worked_pairs"]
    assert mark["left_pdf"] == str(left) and mark["right_pdf"] == str(right)
    assert mark["by"] == "Иванов И. И." and mark["by_id"] == "staff-7" and mark["at"]
    reloaded = client.get(f"/api/stage-comparison/sessions/{session['id']}").json()
    assert reloaded["document_pairing"]["worked_pairs"] == [mark]


def test_first_mark_is_kept_and_foreign_documents_are_refused(client_env, monkeypatch):
    client, session, left, right = client_env
    url = f"/api/stage-comparison/sessions/{session['id']}/worked-pairs"
    monkeypatch.setattr("backend.app.core.attribution.actor_label", lambda _request, prefer="name": "Первый")
    client.post(url, json={"left_pdf": str(left), "right_pdf": str(right)})
    monkeypatch.setattr("backend.app.core.attribution.actor_label", lambda _request, prefer="name": "Второй")

    again = client.post(url, json={"left_pdf": str(left), "right_pdf": str(right)})
    foreign = client.post(url, json={"left_pdf": str(left), "right_pdf": "/elsewhere/R.pdf"})
    missing = client.post("/api/stage-comparison/sessions/nope/worked-pairs",
                          json={"left_pdf": str(left), "right_pdf": str(right)})

    assert [m["by"] for m in again.json()["worked_pairs"]] == ["Первый"]
    assert foreign.status_code == 400
    assert missing.status_code == 404


def test_saved_layout_does_not_store_worked_marks(client_env):
    client, session, left, right = client_env
    client.post(f"/api/stage-comparison/sessions/{session['id']}/worked-pairs",
                json={"left_pdf": str(left), "right_pdf": str(right)})

    saved = client.put(f"/api/stage-comparison/sessions/{session['id']}/document-pairing", json={
        "expected_revision": session["document_pairing"]["revision"],
        "left_order": [str(left)], "right_order": [str(right)], "confirmed_pairs": [],
    })

    assert saved.status_code == 200, saved.text
    assert len(saved.json()["worked_pairs"]) == 1
    meta = {"stage_a_path": session["stage_a_path"], "stage_b_path": session["stage_b_path"]}
    assert "worked_pairs" not in board_state.load({**meta, "id": session["id"]})["pairing"]

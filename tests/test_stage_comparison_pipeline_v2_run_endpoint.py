# -*- coding: utf-8 -*-
"""Тесты controlled-run endpoint'ов Pipeline V2.

POST /api/stage-comparison/pipeline-v2/{sid}/pairs/{pid}/run
GET  /api/stage-comparison/pipeline-v2/{sid}/pairs/{pid}/run-status/{job_id}
GET  /api/stage-comparison/pipeline-v2/{sid}/pairs/{pid}/run-active

Проверяем HTTP-маппинг гейтов. Runner/background замоканы — реальный
pipeline/модели НЕ запускаются.
"""
from __future__ import annotations

import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.services.stage_comparison import pipeline_v2_run_jobs as run_mod
from backend.app.services.stage_comparison import store as store_mod
from backend.app.services.stage_comparison import (
    pipeline_v2_payload_service as payload_mod,
)

SID = "sess1"
PID = "pairA"
RUN_EP = f"/api/stage-comparison/pipeline-v2/{SID}/pairs/{PID}/run"


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path))

    monkeypatch.setattr(store_mod, "get_session",
                        lambda sid: {"id": sid} if sid == SID else None)

    def fake_find_pair(sid, pid):
        if sid == SID and pid == PID:
            return {"id": PID,
                    "left": {"result_json_path": "/x/l.json", "md_path": "/x/l.md"},
                    "right": {"result_json_path": "/x/r.json", "md_path": "/x/r.md"}}
        return None

    monkeypatch.setattr(store_mod, "_find_pair_meta", fake_find_pair)
    # фоновый запуск — no-op (тест проверяет accepted-ответ, не сам прогон)
    monkeypatch.setattr(run_mod, "start_job_in_background",
                        lambda sid, jid: jid)
    run_mod._active_tasks.clear()

    from backend.app.api.routers import stage_comparison as router_mod
    app = FastAPI()
    app.include_router(router_mod.router)
    return TestClient(app)


def _body(**kw):
    b = {"confirm": True, "confirm_session_id": SID, "confirm_pair_id": PID}
    b.update(kw)
    return b


def test_reject_without_confirm(client):
    r = client.post(RUN_EP, json={"confirm": False,
                                  "confirm_session_id": SID,
                                  "confirm_pair_id": PID})
    assert r.status_code == 422


def test_reject_wrong_confirm_pair_id(client):
    r = client.post(RUN_EP, json=_body(confirm_pair_id="WRONG"))
    assert r.status_code == 422


def test_reject_nonexistent_session(client):
    ep = f"/api/stage-comparison/pipeline-v2/NOPE/pairs/{PID}/run"
    r = client.post(ep, json={"confirm": True, "confirm_session_id": "NOPE",
                              "confirm_pair_id": PID})
    assert r.status_code == 404


def test_reject_nonexistent_pair(client):
    ep = f"/api/stage-comparison/pipeline-v2/{SID}/pairs/NOPE/run"
    r = client.post(ep, json={"confirm": True, "confirm_session_id": SID,
                              "confirm_pair_id": "NOPE"})
    assert r.status_code == 404


def test_409_when_artifacts_exist(client, tmp_path):
    art = payload_mod.pipeline_v2_artifacts_dir(SID, PID)
    art.mkdir(parents=True, exist_ok=True)
    (art / payload_mod.SUMMARY_FILENAME).write_text("{}", encoding="utf-8")
    r = client.post(RUN_EP, json=_body(rerun_existing=False))
    assert r.status_code == 409


def test_accept_valid_run(client):
    r = client.post(RUN_EP, json=_body())
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert data["status"] == "queued"
    assert data["pair_id"] == PID and data["session_id"] == SID
    assert data["job_id"].startswith("pv2run_")
    assert f"/pairs/{PID}/run-status/{data['job_id']}" in data["status_url"]


def test_run_status_roundtrip(client):
    job_id = client.post(RUN_EP, json=_body()).json()["job_id"]
    r = client.get(f"/api/stage-comparison/pipeline-v2/{SID}/pairs/{PID}"
                   f"/run-status/{job_id}")
    assert r.status_code == 200
    assert r.json()["id"] == job_id


def test_run_status_unknown_job_404(client):
    r = client.get(f"/api/stage-comparison/pipeline-v2/{SID}/pairs/{PID}"
                   f"/run-status/pv2run_doesnotexist")
    assert r.status_code == 404


def test_invalid_id_returns_400_not_500(client, monkeypatch):
    # _safe_id отвергает '..'/'/' (empty after sanitize) → ValueError в store;
    # endpoint должен отдать 400, а не 500.
    def raise_invalid(_sid):
        raise ValueError("invalid id")
    monkeypatch.setattr(store_mod, "get_session", raise_invalid)
    r = client.post(RUN_EP, json=_body())
    assert r.status_code == 400


def test_run_active_reports_lock(client):
    job_id = client.post(RUN_EP, json=_body()).json()["job_id"]
    r = client.get(f"/api/stage-comparison/pipeline-v2/{SID}/pairs/{PID}/run-active")
    assert r.status_code == 200
    assert (r.json().get("job") or {}).get("id") == job_id
    # второй POST на ту же пару → 409 (lock)
    r2 = client.post(RUN_EP, json=_body())
    assert r2.status_code == 409

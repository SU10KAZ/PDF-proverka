"""Очереди анализа выбранных пар: по одной в дорожке, дорожки Codex и Claude параллельно.

Без моделей: прогон подменён."""
from __future__ import annotations

import json
import threading

import pytest

from backend.tests.project_change_v3 import generic_fixture as gf


@pytest.fixture
def env(tmp_path, monkeypatch):
    root = tmp_path / "comparison"
    root.mkdir()
    monkeypatch.setenv("COMPARISON_ROOT", str(root))
    monkeypatch.setenv("PROJECT_COMPARISON_V3_MODEL_CHOICES", "astra62,sol62,opus55,sonnet55")
    built = gf.build_comparison(tmp_path)
    from backend.app.services.stage_comparison import production_batch as B
    yield built, B
    for key, thread in list(B._threads.items()):
        if key[0] == built["session_id"]:
            thread.join(5)


def _pairs(built, n=1):
    one = {"left_pdf": built["left"]["pdf_path"], "right_pdf": built["right"]["pdf_path"], "label": "OV"}
    return [dict(one) for _ in range(n)]


def _wait(B, session_id, lane=None):
    for key, thread in list(B._threads.items()):
        if key[0] == session_id and (lane is None or key[1] == lane):
            thread.join(10)
    return B.status(session_id, lane)


def test_pairs_run_one_after_another_with_chosen_mode_and_model(env):
    built, B = env
    calls, active = [], []

    def runner(session_id, pair_id, **kwargs):
        active.append(pair_id)
        assert len(active) == 1  # never two at once
        calls.append((pair_id, kwargs))
        active.pop()
        return {"status": "COMPLETED", "run_id": "r" * 32, "message": "ok"}

    batch = B.start(built["session_id"], _pairs(built, 2), ai_mode="STANDARD", model_profile="opus55", runner=runner)
    assert len(batch["items"]) == 1  # the same pair twice is queued once
    done = _wait(B, built["session_id"])
    assert done["status"] == "DONE"
    assert [i["status"] for i in done["items"]] == ["COMPLETED"]
    assert done["items"][0]["pair_id"] and done["items"][0]["run_id"] == "r" * 32
    assert calls == [(done["items"][0]["pair_id"],
                      {"input_mode": "DOCUMENT", "ai_mode": "STANDARD", "model_profile": "opus55", "parallelism": 5,
                       "comparison_mode": "standard"})]


def test_a_failing_pair_does_not_stop_the_queue_and_second_queue_is_refused(env):
    built, B = env
    gate = threading.Event()

    def runner(session_id, pair_id, **kwargs):
        gate.wait(5)
        raise RuntimeError("boom")

    B.start(built["session_id"], _pairs(built), ai_mode=None, model_profile=None, runner=runner)
    with pytest.raises(B.BatchConflictError):
        B.start(built["session_id"], _pairs(built), ai_mode=None, model_profile=None, runner=runner)
    gate.set()
    done = _wait(B, built["session_id"])
    assert done["items"][0]["status"] == "FAILED" and "RuntimeError" in done["items"][0]["message"]
    assert done["status"] == "DONE"


def test_stop_leaves_remaining_pairs_unstarted(env, monkeypatch):
    built, B = env
    gate = threading.Event()
    items = [{"left_pdf": built["left"]["pdf_path"], "right_pdf": built["right"]["pdf_path"], "label": "A"},
             {"left_pdf": built["left"]["pdf_path"] + "x", "right_pdf": built["right"]["pdf_path"], "label": "B"}]

    entered = threading.Event()

    def runner(session_id, pair_id, **kwargs):
        entered.set()
        gate.wait(5)
        return {"status": "COMPLETED"}

    B.start(built["session_id"], items, ai_mode=None, model_profile=None, runner=runner)
    assert entered.wait(5)
    B.stop(built["session_id"])
    gate.set()
    done = _wait(B, built["session_id"])
    assert [i["status"] for i in done["items"]] == ["COMPLETED", "STOPPED"]
    assert done["status"] == "STOPPED"


def test_queue_lost_by_restart_is_reported_interrupted(env):
    built, B = env
    path = B._legacy_path(built["session_id"])  # прежний единый файл очереди
    path.write_text(json.dumps({"kind": B.KIND, "status": "RUNNING", "batch_id": "x", "model_profile": "astra",
                                "items": [{"label": "A", "status": "RUNNING"}, {"label": "B", "status": "QUEUED"}]}),
                    encoding="utf-8")
    state = B.status(built["session_id"])
    assert state["status"] == "INTERRUPTED" and state["lane"] == "codex"
    assert {i["status"] for i in state["items"]} == {"INTERRUPTED"}
    assert B.status(built["session_id"], "claude") is None


def test_empty_selection_is_rejected(env):
    built, B = env
    with pytest.raises(ValueError):
        B.start(built["session_id"], [{"left_pdf": "", "right_pdf": ""}], ai_mode=None, model_profile=None)


def test_endpoints_start_report_and_refuse_bad_input(tmp_path, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.app.api.routers import stage_comparison
    from backend.app.services.stage_comparison import production_batch as B

    root = tmp_path / "comparison"
    root.mkdir()
    monkeypatch.setenv("COMPARISON_ROOT", str(root))
    monkeypatch.setenv("PROJECT_COMPARISON_V3_MODEL_CHOICES", "astra62,sol62,opus55,sonnet55")
    built = gf.build_comparison(tmp_path)
    started = threading.Event()

    def fake_run(session_id, pair_id, **kwargs):
        started.set()
        return {"status": "COMPLETED", "run_id": "a" * 32}

    monkeypatch.setattr("backend.app.services.stage_comparison.production_orchestrator.run_production_comparison", fake_run)
    app = FastAPI()
    app.include_router(stage_comparison.router)
    client = TestClient(app)
    base = f"/api/stage-comparison/sessions/{built['session_id']}/production/batch"
    assert client.get(base).json() == {"batch": None, "batches": []}
    assert client.post(base, json={"pairs": []}).status_code == 422
    assert client.post(base, json={"pairs": _pairs(built), "model_profile": "no-such-model"}).status_code == 400
    response = client.post(base, json={"pairs": _pairs(built), "ai_mode": "STANDARD", "model_profile": "sonnet55"})
    assert response.status_code == 200, response.text
    assert response.json()["lane"] == "claude"
    B._threads[(built["session_id"], "claude")].join(10)
    assert started.is_set()
    view = client.get(base).json()
    state = view["batch"]
    assert state["status"] == "DONE" and state["items"][0]["status"] == "COMPLETED"
    assert [b["lane"] for b in view["batches"]] == ["claude"]
    assert client.post(base + "/stop").json()["batch"]["status"] == "DONE"
    assert client.post(base + "/stop?lane=codex").status_code == 200
    assert client.post(base + "/stop?lane=gemini").status_code == 422
    assert client.get("/api/stage-comparison/sessions/..%2F/production/batch").status_code in (404, 405)


@pytest.mark.parametrize('new_status,reason,expected', [
    ('REVIEW', 'v3_completed', 'REVIEW'),
    ('RUNNING', '', 'RUNNING'),
    ('FAILED', 'v3_cancelled', 'CANCELLED'),
])
def test_status_uses_newer_individual_run_without_rewriting_history(env, monkeypatch, new_status, reason, expected):
    built, B = env
    sid = built['session_id']
    original = {'kind': B.KIND, 'status': 'DONE', 'items': [{
        'pair_id': 'pair', 'status': 'FAILED', 'run_id': 'old',
        'finished_at': '2026-10-13T22:04:00+00:00', 'message': 'old failure'}]}
    B._write(sid, {**original, 'lane': 'codex'})
    monkeypatch.setattr(B.production_store, 'load_artifact', lambda *a: {
        'status': new_status, 'reason_code': reason, 'run_id': 'new',
        'started_at': '2026-10-14T05:33:46+00:00', 'message': 'current'})
    assert B.status(sid, 'codex')['items'][0]['status'] == expected
    assert B._read(sid, 'codex') == {**original, 'lane': 'codex'}


def test_status_keeps_queued_item_and_does_not_use_older_run(env, monkeypatch):
    built, B = env
    batch = {'items': [
        {'pair_id': 'pair', 'status': 'QUEUED'},
        {'pair_id': 'pair', 'status': 'FAILED', 'run_id': 'new',
         'finished_at': '2026-10-14T10:00:00+00:00'}]}
    monkeypatch.setattr(B.production_store, 'load_artifact', lambda *a: {
        'status': 'REVIEW', 'run_id': 'old', 'started_at': '2026-10-13T10:00:00+00:00'})
    B._refresh_pair_statuses(built['session_id'], batch)
    assert [i['status'] for i in batch['items']] == ['QUEUED', 'FAILED']


def test_explicit_parallelism_is_persisted_and_forwarded(env):
    built, B = env
    received = []
    def runner(session_id, pair_id, **kwargs):
        received.append(kwargs['parallelism'])
        return {'status': 'COMPLETED'}
    B.start(built['session_id'], _pairs(built), ai_mode=None, model_profile=None,
            parallelism=10, runner=runner)
    done = _wait(B, built['session_id'])
    assert done['parallelism'] == 10
    assert received == [10]


def test_capacity_pause_is_not_reported_as_completed(env):
    built, B = env
    def runner(*args, **kwargs):
        return {"status": "PAUSED", "run_id": "paused_run", "message": "Прогресс сохранён"}
    B.start(built["session_id"], _pairs(built), ai_mode=None, model_profile=None, runner=runner)
    done = _wait(B, built["session_id"])
    assert done["items"][0]["status"] == "PAUSED"
    assert done["items"][0]["run_id"] == "paused_run"


def _fake_pairs(B, monkeypatch):
    monkeypatch.setattr(B.store, "create_pair",
                        lambda session_id, left, right: {"id": "p" + str(abs(hash((left, right))))})


def test_codex_and_claude_queues_run_in_parallel_one_pair_at_a_time_each(env, monkeypatch):
    built, B = env
    _fake_pairs(B, monkeypatch)
    sid = built["session_id"]
    both_inside = threading.Barrier(2, timeout=5)
    seen = []

    def runner(session_id, pair_id, **kwargs):
        seen.append(kwargs["model_profile"])
        both_inside.wait()  # обе дорожки одновременно внутри прогона
        return {"status": "COMPLETED"}

    codex = B.start(sid, _pairs(built), ai_mode=None, model_profile="sol62", runner=runner)
    other = [{"left_pdf": built["left"]["pdf_path"] + "x", "right_pdf": built["right"]["pdf_path"], "label": "B"}]
    claude = B.start(sid, other, ai_mode=None, model_profile="opus55", runner=runner)
    assert (codex["lane"], claude["lane"]) == ("codex", "claude")
    assert _wait(B, sid, "codex")["status"] == "DONE"
    assert _wait(B, sid, "claude")["status"] == "DONE"
    assert sorted(seen) == ["opus55", "sol62"]
    assert [b["lane"] for b in B.statuses(sid)] == ["codex", "claude"]


def test_same_lane_is_refused_and_a_pair_cannot_sit_in_both_queues(env, monkeypatch):
    built, B = env
    _fake_pairs(B, monkeypatch)
    sid = built["session_id"]
    gate = threading.Event()

    def runner(session_id, pair_id, **kwargs):
        gate.wait(5)
        return {"status": "COMPLETED"}

    B.start(sid, _pairs(built), ai_mode=None, model_profile="astra62", runner=runner)
    other = [{"left_pdf": built["left"]["pdf_path"] + "x", "right_pdf": built["right"]["pdf_path"], "label": "B"}]
    with pytest.raises(B.BatchConflictError, match="Codex"):
        B.start(sid, other, ai_mode=None, model_profile="sol62", runner=runner)
    with pytest.raises(B.BatchConflictError, match="другой очереди"):
        B.start(sid, _pairs(built), ai_mode=None, model_profile="sonnet55", runner=runner)
    gate.set()
    assert _wait(B, sid, "codex")["status"] == "DONE"


def test_stop_by_lane_leaves_the_other_queue_running(env, monkeypatch):
    built, B = env
    _fake_pairs(B, monkeypatch)
    sid = built["session_id"]
    gate = threading.Event()
    entered = threading.Barrier(3, timeout=5)

    def runner(session_id, pair_id, **kwargs):
        entered.wait()
        gate.wait(5)
        return {"status": "COMPLETED"}

    B.start(sid, _pairs(built), ai_mode=None, model_profile="astra62", runner=runner)
    other = [{"left_pdf": built["left"]["pdf_path"] + "x", "right_pdf": built["right"]["pdf_path"], "label": "B"}]
    B.start(sid, other, ai_mode=None, model_profile="sonnet55", runner=runner)
    entered.wait()
    B.stop(sid, "claude")
    assert B.status(sid, "claude")["stop_requested"] is True
    assert B.status(sid, "codex")["stop_requested"] is False
    gate.set()
    assert _wait(B, sid, "claude")["status"] == "STOPPED"
    assert _wait(B, sid, "codex")["status"] == "DONE"

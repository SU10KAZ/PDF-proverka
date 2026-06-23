"""Тесты latest_pair_timings + endpoint /pipeline-qwen-opus/pair-timings.

Закрывает баг: после refresh страницы колонки 🟦 Qwen / 🟪 Opus показывали «—»,
потому что фронт читал времена только из in-memory job. Теперь они берутся из
персистентных qopipe job-файлов.

Покрытие:
  1. completed job → Qwen/Opus времена + длительности;
  2. missing pair → нет в ответе (остаётся «—» на фронте);
  3. running job → qwen_started без finished, duration=None;
  4. failed job → status отражён;
  5. latest job по паре перезаписывает старый (re-run);
  6. job без timestamps (rejected/queued) не попадает;
  7. endpoint возвращает {"timings": {...}}.

Файлы — в tmp (COMPARISON_ROOT). Без Qwen/Opus/тяжёлых артефактов.
"""
from __future__ import annotations

import json

import pytest

from backend.app.services.stage_comparison import pipeline_queue as pq

SID = "sess_pt"


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    return tmp_path


def _write_job(job: dict) -> None:
    # пишем напрямую в _jobs_dir (как делает _write_job, но без mutate updated_at)
    p = pq._job_path(SID, job["job_id"])
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(job, f, ensure_ascii=False)


def _job(job_id, created_at, status, items):
    return {"job_id": job_id, "type": "qwen_opus_pipeline", "status": status,
            "session_id": SID, "created_at": created_at, "updated_at": created_at,
            "pair_ids": [it["pair_id"] for it in items], "items": items}


def _item(pair_id, **kw):
    base = {"pair_id": pair_id, "qwen_status": None, "opus_status": None,
            "status": None, "qwen_started_at": None, "qwen_finished_at": None,
            "opus_started_at": None, "opus_finished_at": None,
            "qwen_error": None, "opus_error": None, "changes_count": 0}
    base.update(kw)
    return base


def test_1_completed_job_durations(env):
    _write_job(_job("qopipe_a", "2026-06-08T14:16:10Z", "done", [
        _item("p1", qwen_status="done", opus_status="done", status="done",
              qwen_started_at="2026-06-08T14:16:10Z", qwen_finished_at="2026-06-08T14:38:56Z",
              opus_started_at="2026-06-08T14:38:56Z", opus_finished_at="2026-06-08T14:43:00Z",
              changes_count=7),
    ]))
    t = pq.latest_pair_timings(SID)
    assert "p1" in t
    d = t["p1"]
    assert d["qwen_status"] == "done" and d["opus_status"] == "done"
    assert d["qwen_duration_sec"] == pytest.approx(1366.0)  # 22.77 мин
    assert d["opus_duration_sec"] == pytest.approx(244.0)   # 4.07 мин
    assert d["changes_count"] == 7
    assert d["job_id"] == "qopipe_a"


def test_2_missing_pair_absent(env):
    _write_job(_job("qopipe_a", "2026-06-08T14:16:10Z", "done", [
        _item("p1", qwen_status="done", status="done",
              qwen_started_at="2026-06-08T14:16:10Z", qwen_finished_at="2026-06-08T14:18:00Z"),
    ]))
    t = pq.latest_pair_timings(SID)
    assert "pX" not in t  # пары без прогона нет → фронт покажет «—»


def test_3_running_no_finish(env, monkeypatch):
    # держим job «живым» (есть незавершённая task), иначе list_jobs пометит его
    # interrupted (нет asyncio-task в тесте).
    class _FakeTask:
        def done(self):
            return False
    monkeypatch.setitem(pq._active_tasks, SID, {"qopipe_a": _FakeTask()})
    _write_job(_job("qopipe_a", "2026-06-08T14:16:10Z", "running", [
        _item("p1", qwen_status="running", opus_status="waiting_qwen", status="qwen_running",
              qwen_started_at="2026-06-08T14:16:10Z"),  # нет finished
    ]))
    t = pq.latest_pair_timings(SID)["p1"]
    assert t["qwen_status"] == "running"
    assert t["qwen_duration_sec"] is None
    assert t["opus_duration_sec"] is None


def test_4_failed_status(env):
    _write_job(_job("qopipe_a", "2026-06-08T14:16:10Z", "failed", [
        _item("p1", qwen_status="failed", status="failed",
              qwen_started_at="2026-06-08T14:16:10Z", qwen_finished_at="2026-06-08T14:17:00Z",
              qwen_error="boom"),
    ]))
    t = pq.latest_pair_timings(SID)["p1"]
    assert t["qwen_status"] == "failed"
    assert t["qwen_error"] == "boom"


def test_5_latest_job_wins(env):
    _write_job(_job("qopipe_old", "2026-06-08T10:00:00Z", "done", [
        _item("p1", qwen_status="done", status="done",
              qwen_started_at="2026-06-08T10:00:00Z", qwen_finished_at="2026-06-08T10:05:00Z"),
    ]))
    _write_job(_job("qopipe_new", "2026-06-08T14:00:00Z", "done", [
        _item("p1", qwen_status="done", status="done",
              qwen_started_at="2026-06-08T14:00:00Z", qwen_finished_at="2026-06-08T14:02:00Z"),
    ]))
    t = pq.latest_pair_timings(SID)["p1"]
    assert t["job_id"] == "qopipe_new"
    assert t["qwen_duration_sec"] == pytest.approx(120.0)


def test_6_no_timestamps_excluded(env):
    _write_job(_job("qopipe_rej", "2026-06-08T14:16:10Z", "rejected_no_confirm", [
        _item("p1", qwen_status="queued", opus_status="waiting_qwen", status="queued"),
    ]))
    t = pq.latest_pair_timings(SID)
    assert "p1" not in t  # пара не стартовала → нет timing


def test_7_latest_does_not_overwrite_with_empty(env):
    # старый job реально прогнал пару, новый — rejected без ts: должен остаться старый
    _write_job(_job("qopipe_done", "2026-06-08T10:00:00Z", "done", [
        _item("p1", qwen_status="done", status="done",
              qwen_started_at="2026-06-08T10:00:00Z", qwen_finished_at="2026-06-08T10:05:00Z"),
    ]))
    _write_job(_job("qopipe_rej", "2026-06-08T14:00:00Z", "rejected_no_confirm", [
        _item("p1", qwen_status="queued", status="queued"),
    ]))
    t = pq.latest_pair_timings(SID)
    assert "p1" in t and t["p1"]["job_id"] == "qopipe_done"


# ─── endpoint ───────────────────────────────────────────────────────────────

def _client():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.app.api.routers import stage_comparison as router_mod
    app = FastAPI()
    app.include_router(router_mod.router)
    return TestClient(app)


def test_endpoint_returns_timings(env):
    _write_job(_job("qopipe_a", "2026-06-08T14:16:10Z", "done", [
        _item("p1", qwen_status="done", opus_status="done", status="done",
              qwen_started_at="2026-06-08T14:16:10Z", qwen_finished_at="2026-06-08T14:38:56Z",
              opus_started_at="2026-06-08T14:38:56Z", opus_finished_at="2026-06-08T14:43:00Z"),
    ]))
    r = _client().get(f"/api/stage-comparison/sessions/{SID}/pipeline-qwen-opus/pair-timings")
    assert r.status_code == 200
    body = r.json()
    assert "timings" in body
    assert body["timings"]["p1"]["qwen_duration_sec"] == pytest.approx(1366.0)


def test_endpoint_not_shadowed_by_job_id_route(env):
    # pair-timings не должен попасть в {job_id} (вернул бы 404 "Job не найден")
    r = _client().get(f"/api/stage-comparison/sessions/{SID}/pipeline-qwen-opus/pair-timings")
    assert r.status_code == 200
    assert "timings" in r.json()


# ─── repair overlay: manual / selected workflow (НЕ qopipe) ───────────────────
# Закрывает баг: пары, восстановленные через large_sheet + md rebuild +
# unified-analysis (а не qopipe), показывали в колонках устаревший
# failed/skipped из упавшего qopipe-батча. Overlay подтягивает их актуальный
# статус из session-level job'ов + comparison_result.json.

def _write_session_job(job: dict) -> None:
    """Session-level job (md_enrichment / large_sheet / unified) в <sid>/jobs/."""
    d = pq.paths_mod.session_dir(SID) / "jobs"
    d.mkdir(parents=True, exist_ok=True)
    with open(d / f"{job['id']}.json", "w", encoding="utf-8") as f:
        json.dump(job, f, ensure_ascii=False)


def _write_comparison_result(pid: str, payload: dict) -> None:
    p = pq.paths_mod.enriched_comparison_result_path(SID, pid)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)


def test_overlay_md_and_unified_repair(env):
    # qopipe оставил пару failed/skipped; ручной repair (large_sheet + md + unified)
    # делает её done — overlay это отражает.
    _write_job(_job("qopipe_batch", "2026-06-08T18:00:00Z", "failed_interrupted", [
        _item("pR", qwen_status="failed", opus_status="skipped", status="failed",
              qwen_started_at="2026-06-08T18:00:00Z", qwen_finished_at="2026-06-08T20:00:00Z",
              qwen_error="validation:large_sheet_placeholders_remain"),
    ]))
    _write_session_job({"id": "lsj_x", "type": "large_sheet_enrichment", "status": "done",
        "created_at": "2026-06-09T08:30:00Z", "updated_at": "2026-06-09T08:40:00Z",
        "items": [{"pair_id": "pR", "side": "right", "page": 28, "status": "done",
                   "tiles_total": 15, "tiles_failed": 0}]})
    _write_session_job({"id": "mdenrich_x", "type": "md_enrichment_batch", "status": "done",
        "created_at": "2026-06-09T08:45:00Z", "updated_at": "2026-06-09T08:51:00Z",
        "items": [{"pair_id": "pR", "side": "right", "status": "done"}]})
    _write_session_job({"id": "uajob_x", "type": "unified_stage_comparison", "status": "done",
        "created_at": "2026-06-09T09:00:00Z", "updated_at": "2026-06-09T09:05:00Z",
        "items": [{"pair_id": "pR", "status": "done", "duration_sec": 244.0,
                   "changes_count": 23}]})
    t = pq.latest_pair_timings(SID)["pR"]
    assert t["qwen_status"] == "done"                       # repair поднял Qwen из failed
    assert t["qwen_source"] in ("md_enrichment", "large_sheet")
    assert t["opus_status"] == "done"                       # unified дал Opus
    assert t["opus_source"] == "unified"
    assert t["opus_duration_sec"] == pytest.approx(244.0)
    assert t["changes_count"] == 23
    assert t["status"] == "done"


def test_overlay_comparison_result_fallback(env):
    # нет unified-job, но есть comparison_result.json → Opus done/duration из него.
    _write_job(_job("qopipe_batch", "2026-06-08T18:00:00Z", "failed_interrupted", [
        _item("pC", qwen_status="failed", opus_status="skipped", status="failed",
              qwen_started_at="2026-06-08T18:00:00Z", qwen_finished_at="2026-06-08T19:00:00Z"),
    ]))
    _write_comparison_result("pC", {"status": "done", "strategy": "evidence_first_s2_fallback",
        "duration_sec": 1292.9, "updated_at": "2026-06-09T10:00:00Z",
        "changes": [{"x": 1}] * 5})
    t = pq.latest_pair_timings(SID)["pC"]
    assert t["opus_status"] == "done"
    assert t["opus_source"] == "comparison_result"
    assert t["opus_duration_sec"] == pytest.approx(1292.9)
    assert t["changes_count"] == 5
    assert t["status"] == "done"


def test_overlay_does_not_downgrade_fresh_qopipe(env):
    # свежий qopipe-прогон (Opus done) НЕ перебивается СТАРЫМ comparison_result.
    _write_job(_job("qopipe_fresh", "2026-06-09T12:00:00Z", "done", [
        _item("pF", qwen_status="done", opus_status="done", status="done",
              qwen_started_at="2026-06-09T12:00:00Z", qwen_finished_at="2026-06-09T12:20:00Z",
              opus_started_at="2026-06-09T12:20:00Z", opus_finished_at="2026-06-09T12:24:00Z",
              changes_count=9),
    ]))
    _write_comparison_result("pF", {"status": "done", "duration_sec": 5.0,
        "updated_at": "2026-06-08T01:00:00Z", "changes": [{"x": 1}]})  # старее qopipe
    t = pq.latest_pair_timings(SID)["pF"]
    assert t["opus_duration_sec"] == pytest.approx(240.0)   # qopipe сохранён
    assert t["changes_count"] == 9
    assert "opus_source" not in t                            # overlay не трогал


def test_overlay_manual_only_pair_added(env):
    # пара, которой нет ни в одном qopipe job, но есть unified repair → появляется.
    _write_session_job({"id": "uajob_y", "type": "unified_stage_comparison", "status": "done",
        "created_at": "2026-06-09T09:00:00Z", "updated_at": "2026-06-09T09:05:00Z",
        "items": [{"pair_id": "pManual", "status": "done", "duration_sec": 100.0,
                   "changes_count": 4}]})
    t = pq.latest_pair_timings(SID)
    assert "pManual" in t
    assert t["pManual"]["opus_status"] == "done"
    assert t["pManual"]["opus_duration_sec"] == pytest.approx(100.0)


def test_overlay_large_sheet_legacy_gives_qwen(env):
    # large_sheet job без поля type (legacy) распознаётся по items → Qwen done.
    _write_session_job({"id": "lsj_legacy", "status": "done",
        "created_at": "2026-06-09T08:00:00Z", "updated_at": "2026-06-09T08:10:00Z",
        "items": [{"pair_id": "pLS", "side": "left", "page": 30, "status": "done",
                   "tiles_total": 4, "tiles_failed": 0}]})
    t = pq.latest_pair_timings(SID)["pLS"]
    assert t["qwen_status"] == "done"
    assert t["qwen_source"] == "large_sheet"


def test_overlay_missing_pair_still_absent(env):
    # пара без любого сигнала → её нет в timings (фронт покажет «—»).
    _write_job(_job("qopipe_batch", "2026-06-08T18:00:00Z", "done", [
        _item("pHas", qwen_status="done", status="done",
              qwen_started_at="2026-06-08T18:00:00Z", qwen_finished_at="2026-06-08T18:05:00Z"),
    ]))
    t = pq.latest_pair_timings(SID)
    assert "pNope" not in t

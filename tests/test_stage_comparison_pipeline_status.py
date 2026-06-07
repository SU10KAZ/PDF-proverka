"""Service-level tests for the per-pair PROGRESS / STATUS surface of the
Qwen→Opus pipeline queue (``pipeline_queue.run_qwen_opus_pipeline``).

This locks in the backend status contract that the (future) UI consumes:
per-pair stage status, Qwen/Opus start/finish timers, the ``block_equivalence``
surface, and fail-soft status so a single failure never crashes the run. All
Qwen/Opus work is injected as fakes — NO external API / LM Studio / Claude Code
is called, no router/frontend/runtime data.

The status machinery itself already ships on origin/main; these tests assert it
explicitly so later stages can rely on the contract.
"""
import asyncio

import pytest

from backend.app.services.stage_comparison import pipeline_queue as pq


@pytest.fixture
def sess(tmp_path, monkeypatch):
    sid = "statussess"
    base = tmp_path / "sessions" / sid
    (base / "pairs").mkdir(parents=True)
    session = {
        "id": sid,
        "pairs": [
            {"id": "p1", "label": "ИОС1", "left": {"has_md": True}, "right": {"has_md": True}},
            {"id": "p2", "label": "ИОС2", "left": {"has_md": True}, "right": {"has_md": True}},
            {"id": "p3", "label": "ИОС3", "left": {"has_md": True}, "right": {"has_md": True}},
        ],
    }
    monkeypatch.setattr(pq.store_mod, "get_session", lambda s: session if s == sid else None)
    monkeypatch.setattr(pq.paths_mod, "session_dir", lambda s: base)
    monkeypatch.setattr(pq.paths_mod, "pair_dir", lambda s, pid: base / "pairs" / pid)
    return sid, session, base


async def _ctx_ok():
    return (True, 16000)


def _validate_ok(session_id, pair_id):
    return (True, "ok", {"total_blocks": 1})


async def _qwen_ok(s, pid, *, force_qwen, prebuild_large_sheets):
    return {"status": "done", "error": None}


async def _opus_ok(s, pid, *, force_opus):
    return {"status": "done", "changes_count": 1, "error": None}


def _items(res):
    return {it["pair_id"]: it for it in res["items"]}


# ── 1. happy path: status reaches done; Qwen/Opus timers stamped ─────────────
@pytest.mark.asyncio
async def test_status_done_and_timers_stamped(sess):
    sid, _, _ = sess
    job = pq.create_job(sid, scope="selected", pair_ids=["p1", "p2", "p3"], confirm=True)

    res = await pq.run_qwen_opus_pipeline(
        sid, job["job_id"], qwen_fn=_qwen_ok, opus_fn=_opus_ok,
        validate_fn=_validate_ok, ctx_fn=_ctx_ok)

    assert res["status"] == "done"
    items = _items(res)
    for pid in ("p1", "p2", "p3"):
        it = items[pid]
        assert it["status"] == "done"
        assert it["qwen_status"] == "done"
        assert it["opus_status"] == "done"
        # timers stamped for both lanes
        assert it["qwen_started_at"] and it["qwen_finished_at"]
        assert it["opus_started_at"] and it["opus_finished_at"]
    # worker progress summary
    assert res["qwen_worker"]["done"] == 3
    assert res["opus_worker"]["done"] == 3
    assert res["qwen_worker"]["failed"] == 0 and res["opus_worker"]["failed"] == 0


# ── 2. qwen failure → pair status failed, opus skipped, run partial ──────────
@pytest.mark.asyncio
async def test_qwen_failure_status_failed_opus_skipped(sess):
    sid, _, _ = sess
    job = pq.create_job(sid, scope="selected", pair_ids=["p1", "p2", "p3"], confirm=True)

    async def qwen_fn(s, pid, *, force_qwen, prebuild_large_sheets):
        if pid == "p2":
            return {"status": "failed", "error": "boom"}
        return {"status": "done", "error": None}

    res = await pq.run_qwen_opus_pipeline(
        sid, job["job_id"], qwen_fn=qwen_fn, opus_fn=_opus_ok,
        validate_fn=_validate_ok, ctx_fn=_ctx_ok)

    items = _items(res)
    assert items["p2"]["status"] == "failed"
    assert items["p2"]["qwen_status"] == "failed"
    assert items["p2"]["opus_status"] == "skipped"
    assert items["p2"]["qwen_finished_at"]            # finish time stamped even on failure
    # summary still coherent: other pairs done, run is partial
    assert items["p1"]["status"] == "done" and items["p3"]["status"] == "done"
    assert res["status"] == "partial"
    assert res["qwen_worker"]["failed"] == 1


# ── 3. block_equivalence diagnostic surfaced into per-pair status ────────────
@pytest.mark.asyncio
async def test_block_equivalence_surfaced_per_pair(sess):
    sid, _, _ = sess
    job = pq.create_job(sid, scope="selected", pair_ids=["p1", "p2", "p3"], confirm=True)
    be_payload = {"enabled": True, "mode": "observe", "potential_qwen_saved": 4}

    async def qwen_fn(s, pid, *, force_qwen, prebuild_large_sheets):
        # only p1 carries a block_equivalence report
        if pid == "p1":
            return {"status": "done", "error": None, "block_equivalence": be_payload}
        return {"status": "done", "error": None}

    res = await pq.run_qwen_opus_pipeline(
        sid, job["job_id"], qwen_fn=qwen_fn, opus_fn=_opus_ok,
        validate_fn=_validate_ok, ctx_fn=_ctx_ok)

    items = _items(res)
    assert items["p1"]["block_equivalence"] == be_payload
    # pairs without a report do not get a phantom block_equivalence key
    assert "block_equivalence" not in items["p2"]


# ── 4. opus failure is fail-soft: Qwen finishes every pair, status surfaced ──
@pytest.mark.asyncio
async def test_opus_failure_failsoft_status(sess):
    sid, _, _ = sess
    job = pq.create_job(sid, scope="selected", pair_ids=["p1", "p2", "p3"], confirm=True)
    qwen_seen = []

    async def qwen_fn(s, pid, *, force_qwen, prebuild_large_sheets):
        qwen_seen.append(pid)
        return {"status": "done", "error": None}

    async def opus_fn(s, pid, *, force_opus):
        if pid == "p1":
            return {"status": "failed", "error": "opus boom"}
        return {"status": "done", "changes_count": 2, "error": None}

    res = await pq.run_qwen_opus_pipeline(
        sid, job["job_id"], qwen_fn=qwen_fn, opus_fn=opus_fn,
        validate_fn=_validate_ok, ctx_fn=_ctx_ok)

    assert qwen_seen == ["p1", "p2", "p3"]          # opus failure never stalls Qwen lane
    items = _items(res)
    assert items["p1"]["opus_status"] == "failed"
    assert items["p1"]["qwen_status"] == "done"     # qwen side still recorded done
    assert items["p2"]["status"] == "done" and items["p3"]["status"] == "done"
    assert res["opus_worker"]["failed"] == 1


# ── 5. summary exposes a per-pair progress list with stage statuses ──────────
@pytest.mark.asyncio
async def test_summary_has_per_pair_progress(sess):
    sid, _, _ = sess
    job = pq.create_job(sid, scope="selected", pair_ids=["p1", "p2"], confirm=True)

    res = await pq.run_qwen_opus_pipeline(
        sid, job["job_id"], qwen_fn=_qwen_ok, opus_fn=_opus_ok,
        validate_fn=_validate_ok, ctx_fn=_ctx_ok)

    assert isinstance(res["items"], list) and len(res["items"]) == 2
    for it in res["items"]:
        assert {"pair_id", "status", "qwen_status", "opus_status"} <= set(it)
    assert res["qwen_worker"]["total"] == 2 and res["opus_worker"]["total"] == 2

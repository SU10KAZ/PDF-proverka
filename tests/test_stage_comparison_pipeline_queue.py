"""Tests for the Qwen→Opus pipeline queue (stage comparison).

All Qwen/Opus work is injected as fakes — NO external API / LM Studio / Claude
Code is ever called. Covers: preflight, confirm-gate, decoupled lanes, fail
isolation, cancel, comparison-only-after-Opus, V2 safety, UI buttons present,
and a no-external-API guard.
"""
import asyncio
import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import pipeline_queue as pq

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def sess(tmp_path, monkeypatch):
    sid = "testsess"
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


# ── 1. preflight ────────────────────────────────────────────────────────────
def test_preflight_selected_pairs(sess):
    sid, _, _ = sess
    pf = pq.preflight(sid, scope="selected", pair_ids=["p1", "p3"],
                      force_qwen=True, force_opus=True)
    assert pf["total_pairs"] == 2
    assert pf["pair_ids"] == ["p1", "p3"]
    assert pf["can_run"] is True
    # estimate fields present
    for k in ("estimated_qwen_calls", "estimated_qwen_duration_sec",
              "estimated_opus_duration_sec", "too_large_pairs", "risks"):
        assert k in pf


# ── 2. start without confirm → rejected ─────────────────────────────────────
def test_start_without_confirm_rejected(sess):
    sid, _, _ = sess
    job = pq.create_job(sid, scope="selected", pair_ids=["p1"], confirm=False)
    assert job["status"] == "rejected_no_confirm"
    # and a confirmed empty selection is not "running"
    empty = pq.create_job(sid, scope="selected", pair_ids=[], confirm=True)
    assert empty["status"] == "done"


# ── 3 & 4. decoupling: Qwen does not wait for Opus; lanes overlap ────────────
@pytest.mark.asyncio
async def test_qwen_does_not_wait_for_opus(sess):
    sid, _, _ = sess
    job = pq.create_job(sid, scope="selected", pair_ids=["p1", "p2", "p3"], confirm=True)
    events = []

    async def qwen_fn(s, pid, *, force_qwen, prebuild_large_sheets):
        events.append(("qwen_start", pid))
        await asyncio.sleep(0.01)
        events.append(("qwen_end", pid))
        return {"status": "done", "error": None}

    async def opus_fn(s, pid, *, force_opus):
        events.append(("opus_start", pid))
        await asyncio.sleep(0.06)  # Opus much slower than Qwen
        events.append(("opus_end", pid))
        return {"status": "done", "changes_count": 3, "error": None}

    res = await pq.run_qwen_opus_pipeline(
        sid, job["job_id"], qwen_fn=qwen_fn, opus_fn=opus_fn,
        validate_fn=_validate_ok, ctx_fn=_ctx_ok)
    assert res["status"] == "done"
    # Qwen finished ALL three pairs before Opus finished even the first
    assert events.index(("qwen_end", "p3")) < events.index(("opus_end", "p1"))
    # Opus started p1 while Qwen was still working (overlap) — before qwen_end p3
    assert events.index(("opus_start", "p1")) < events.index(("qwen_end", "p3"))
    # all pairs completed with changes_count from Opus
    items = {it["pair_id"]: it for it in res["items"]}
    assert all(items[p]["status"] == "done" and items[p]["changes_count"] == 3 for p in ("p1", "p2", "p3"))


@pytest.mark.asyncio
async def test_opus_processes_pair1_while_qwen_does_pair2(sess):
    sid, _, _ = sess
    job = pq.create_job(sid, scope="selected", pair_ids=["p1", "p2", "p3"], confirm=True)
    order = []

    async def qwen_fn(s, pid, *, force_qwen, prebuild_large_sheets):
        order.append(("q", pid))
        await asyncio.sleep(0.02)
        return {"status": "done", "error": None}

    async def opus_fn(s, pid, *, force_opus):
        order.append(("o", pid))
        await asyncio.sleep(0.005)
        return {"status": "done", "changes_count": 1, "error": None}

    await pq.run_qwen_opus_pipeline(sid, job["job_id"], qwen_fn=qwen_fn, opus_fn=opus_fn,
                                    validate_fn=_validate_ok, ctx_fn=_ctx_ok)
    # Opus(p1) must appear before Qwen(p3) starts → they run concurrently
    assert order.index(("o", "p1")) < order.index(("q", "p3"))


# ── 5. Qwen failure skips Opus for that pair ────────────────────────────────
@pytest.mark.asyncio
async def test_qwen_failure_skips_opus_for_pair(sess):
    sid, _, _ = sess
    job = pq.create_job(sid, scope="selected", pair_ids=["p1", "p2", "p3"], confirm=True)
    opus_called = []

    async def qwen_fn(s, pid, *, force_qwen, prebuild_large_sheets):
        if pid == "p2":
            return {"status": "failed", "error": "boom"}
        return {"status": "done", "error": None}

    async def opus_fn(s, pid, *, force_opus):
        opus_called.append(pid)
        return {"status": "done", "changes_count": 0, "error": None}

    res = await pq.run_qwen_opus_pipeline(sid, job["job_id"], qwen_fn=qwen_fn, opus_fn=opus_fn,
                                          validate_fn=_validate_ok, ctx_fn=_ctx_ok)
    items = {it["pair_id"]: it for it in res["items"]}
    assert items["p2"]["qwen_status"] == "failed"
    assert items["p2"]["opus_status"] == "skipped"
    assert "p2" not in opus_called          # Opus never ran for the failed pair
    assert set(opus_called) == {"p1", "p3"}
    assert res["status"] == "partial"       # some done, one failed


# ── 6. Opus failure does not stop Qwen ──────────────────────────────────────
@pytest.mark.asyncio
async def test_opus_failure_does_not_stop_qwen(sess):
    sid, _, _ = sess
    job = pq.create_job(sid, scope="selected", pair_ids=["p1", "p2", "p3"], confirm=True)
    qwen_done = []

    async def qwen_fn(s, pid, *, force_qwen, prebuild_large_sheets):
        qwen_done.append(pid)
        return {"status": "done", "error": None}

    async def opus_fn(s, pid, *, force_opus):
        if pid == "p1":
            return {"status": "failed", "error": "opus boom"}
        return {"status": "done", "changes_count": 2, "error": None}

    res = await pq.run_qwen_opus_pipeline(sid, job["job_id"], qwen_fn=qwen_fn, opus_fn=opus_fn,
                                          validate_fn=_validate_ok, ctx_fn=_ctx_ok)
    assert qwen_done == ["p1", "p2", "p3"]   # Qwen processed every pair
    items = {it["pair_id"]: it for it in res["items"]}
    assert items["p1"]["opus_status"] == "failed"
    assert items["p2"]["status"] == "done" and items["p3"]["status"] == "done"


# ── 7. cancel does not start new pairs ──────────────────────────────────────
@pytest.mark.asyncio
async def test_cancel_does_not_start_new_pairs(sess):
    sid, _, _ = sess
    job = pq.create_job(sid, scope="selected", pair_ids=["p1", "p2", "p3"], confirm=True)
    jid = job["job_id"]
    qwen_started = []

    async def qwen_fn(s, pid, *, force_qwen, prebuild_large_sheets):
        qwen_started.append(pid)
        if pid == "p1":
            pq.cancel_job(s, jid)            # request cancel after first pair starts
        return {"status": "done", "error": None}

    async def opus_fn(s, pid, *, force_opus):
        return {"status": "done", "changes_count": 0, "error": None}

    res = await pq.run_qwen_opus_pipeline(sid, jid, qwen_fn=qwen_fn, opus_fn=opus_fn,
                                          validate_fn=_validate_ok, ctx_fn=_ctx_ok)
    assert res["status"] == "cancelled"
    assert qwen_started == ["p1"]            # p2, p3 never started
    items = {it["pair_id"]: it for it in res["items"]}
    assert items["p2"]["qwen_status"] == "queued"
    assert items["p3"]["qwen_status"] == "queued"


# ── 8. comparison_result updated only after Opus success ────────────────────
@pytest.mark.asyncio
async def test_completion_only_after_opus_success(sess):
    sid, _, _ = sess
    job = pq.create_job(sid, scope="selected", pair_ids=["p1", "p2"], confirm=True)

    async def qwen_fn(s, pid, *, force_qwen, prebuild_large_sheets):
        return {"status": "done", "error": None}

    async def opus_fn(s, pid, *, force_opus):
        if pid == "p1":
            return {"status": "done", "changes_count": 5, "error": None}
        return {"status": "failed", "error": "provider_unavailable"}

    res = await pq.run_qwen_opus_pipeline(sid, job["job_id"], qwen_fn=qwen_fn, opus_fn=opus_fn,
                                          validate_fn=_validate_ok, ctx_fn=_ctx_ok)
    items = {it["pair_id"]: it for it in res["items"]}
    # p1: opus success → completed + changes_count set
    assert items["p1"]["status"] == "done" and items["p1"]["changes_count"] == 5
    assert "p1" in res["queues"]["completed"]
    # p2: opus failed → NOT completed, changes_count stays 0 (comparison not finalized)
    assert items["p2"]["status"] == "failed" and items["p2"]["changes_count"] == 0
    assert "p2" not in res["queues"]["completed"]
    assert "p2" in res["queues"]["failed"]


# ── 9. V2 / expert statuses are never deleted ───────────────────────────────
@pytest.mark.asyncio
async def test_v2_and_expert_statuses_not_deleted(sess):
    sid, _, base = sess
    # sentinel V2/expert artifacts
    (base / "expert_review.json").write_text('{"x":1}', encoding="utf-8")
    (base / "pairs" / "p1").mkdir(parents=True, exist_ok=True)
    (base / "pairs" / "p1" / "v2_review_status.json").write_text('{"y":2}', encoding="utf-8")
    job = pq.create_job(sid, scope="selected", pair_ids=["p1"], confirm=True)

    async def qwen_fn(s, pid, *, force_qwen, prebuild_large_sheets):
        return {"status": "done", "error": None}

    async def opus_fn(s, pid, *, force_opus):
        return {"status": "done", "changes_count": 1, "error": None}

    await pq.run_qwen_opus_pipeline(sid, job["job_id"], qwen_fn=qwen_fn, opus_fn=opus_fn,
                                    validate_fn=_validate_ok, ctx_fn=_ctx_ok)
    assert (base / "expert_review.json").exists()
    assert (base / "pairs" / "p1" / "v2_review_status.json").exists()


# ── validation fail-gates (file-based, no live) ─────────────────────────────
def test_validation_fail_gates(sess):
    sid, _, base = sess
    te = base / "pairs" / "p1" / "text_enrichment"
    te.mkdir(parents=True)
    # placeholder remaining → fail
    (te / "left_image_descriptions.json").write_text(json.dumps({
        "items": [{"status": "large_sheet_not_prepared", "block_type": "dense_scheme"}]}), encoding="utf-8")
    (te / "right_image_descriptions.json").write_text(json.dumps({"items": []}), encoding="utf-8")
    ok, reason, _ = pq._validate_qwen_pair(sid, "p1")
    assert not ok and reason == "large_sheet_placeholders_remain"

    # GRSH artificial verified row → fail
    (te / "left_image_descriptions.json").write_text(json.dumps({
        "items": [{"status": "done", "block_type": "dense_grsh_singleline",
                   "description": {"verified_anchors": {"labels": [f"ТП{i}" for i in range(1, 10)]}}}]}),
        encoding="utf-8")
    ok, reason, _ = pq._validate_qwen_pair(sid, "p1")
    assert not ok and reason == "grsh_artificial_series_verified"

    # clean → ok
    (te / "left_image_descriptions.json").write_text(json.dumps({
        "items": [{"status": "done", "block_type": "dense_grsh_singleline",
                   "description": {"verified_anchors": {"labels": ["ВРУ1", "ВРУ2", "ГРЩ"]}}}]}),
        encoding="utf-8")
    ok, reason, _ = pq._validate_qwen_pair(sid, "p1")
    assert ok and reason == "ok"


# ── 10. UI has the buttons «Обработать» and «Обработать выбранные» ───────────
def test_ui_has_process_buttons():
    html = (ROOT / "frontend" / "index.html").read_text(encoding="utf-8")
    assert "Обработать выбранные" in html
    assert "Обработать" in html


# ── 11. no external API referenced in the pipeline module ───────────────────
def test_no_external_api_in_pipeline_module():
    src = (ROOT / "backend" / "app" / "services" / "stage_comparison" / "pipeline_queue.py").read_text(encoding="utf-8")
    for forbidden in ("openrouter", "generativelanguage", "api.openai.com",
                      "api.anthropic.com", "googleapis"):
        assert forbidden not in src, f"external API reference leaked: {forbidden}"

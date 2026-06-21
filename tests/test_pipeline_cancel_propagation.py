"""reserc.md #68 — пропагация cancel в дочернюю Qwen-пару.

Раньше отмена pipeline-job'а проверялась только МЕЖДУ парами; уже запущенная
Qwen-пара (дочерний md-enrichment job) проверяла свой собственный статус и не
знала про отмену родителя → крутилась до конца. Теперь:
  - run_md_enrichment_job принимает parent_cancel_check и останавливается между
    сторонами, помечая дочерний job cancelled;
  - _qwen_lane прокидывает state.cancelled в дочерний раннер (через introspection,
    не ломая узкие тест-фейки) и сохраняет child_job_id в item.
"""
from __future__ import annotations

import asyncio

import pytest

from backend.app.services.stage_comparison import pipeline_queue as pq
from backend.app.services.stage_comparison import md_enrichment_jobs as mdj


def _run(coro):
    return asyncio.run(coro)


# ── _accepts_kwarg ───────────────────────────────────────────────────────

def test_accepts_kwarg_detects_keyword_only():
    async def real(s, pid, *, force_qwen, prebuild_large_sheets, parent_cancel_check=None):
        return {}
    assert pq._accepts_kwarg(real, "parent_cancel_check") is True


def test_accepts_kwarg_false_for_narrow_fake():
    async def fake(s, pid, *, force_qwen, prebuild_large_sheets):
        return {}
    assert pq._accepts_kwarg(fake, "parent_cancel_check") is False


def test_accepts_kwarg_true_for_var_keyword():
    async def flexible(s, pid, **kwargs):
        return {}
    assert pq._accepts_kwarg(flexible, "parent_cancel_check") is True


# ── run_md_enrichment_job parent-cancel ──────────────────────────────────

def test_parent_cancelled_helper_fail_soft():
    assert mdj._parent_cancelled(None) is False
    assert mdj._parent_cancelled(lambda: True) is True
    assert mdj._parent_cancelled(lambda: False) is False

    def _boom():
        raise RuntimeError("x")
    # ошибка callback не валит job — трактуется как «не отменён»
    assert mdj._parent_cancelled(_boom) is False


def test_run_md_job_stops_on_parent_cancel(tmp_path, monkeypatch):
    """Дочерний job с двумя сторонами: parent_cancel_check=True → job помечается
    cancelled до обработки первой стороны, enrich_side не зовётся."""
    sid = "sess_cancel"
    jid = "job_child_1"
    job = {
        "id": jid, "job_id": jid, "session_id": sid, "status": "queued",
        "items": [
            {"pair_id": "p1", "side": "left", "status": "queued"},
            {"pair_id": "p1", "side": "right", "status": "queued"},
        ],
        "progress": {"done": 0, "failed": 0},
    }
    store: dict[str, dict] = {jid: dict(job)}
    monkeypatch.setattr(mdj, "_read_job", lambda s, j: store.get(j))
    monkeypatch.setattr(mdj, "_write_job", lambda s, jb: store.__setitem__(jb.get("job_id") or jb.get("id"), jb))

    called = {"enrich": 0}

    async def _enrich_should_not_run(*a, **k):
        called["enrich"] += 1
        raise AssertionError("enrich_side must not run when parent cancelled")

    monkeypatch.setattr(mdj, "_maybe_run_block_equivalence_precheck",
                        lambda *a, **k: asyncio.sleep(0))
    # отключаем preflight загрузки модели
    cfg = mdj.graphic_local_mod.load_local_graphic_llm_config()
    monkeypatch.setattr(cfg, "enable_model_load", False, raising=False)
    monkeypatch.setattr(mdj.graphic_local_mod, "load_local_graphic_llm_config", lambda: cfg)
    monkeypatch.setattr(mdj.md_mod, "enrich_side", _enrich_should_not_run, raising=False)

    res = _run(mdj.run_md_enrichment_job(sid, jid, parent_cancel_check=lambda: True))
    assert res["status"] == "cancelled"
    assert "cancelled_by_parent_pipeline" in (res.get("warnings") or [])
    assert called["enrich"] == 0


def test_run_md_job_no_parent_cancel_runs_normally(tmp_path, monkeypatch):
    """parent_cancel_check=None или False → поведение прежнее (enrich_side зовётся)."""
    sid = "sess_ok"
    jid = "job_child_2"
    job = {
        "id": jid, "job_id": jid, "session_id": sid, "status": "queued",
        "items": [{"pair_id": "p1", "side": "left", "status": "queued"}],
        "progress": {"done": 0, "failed": 0},
    }
    store: dict[str, dict] = {jid: dict(job)}
    monkeypatch.setattr(mdj, "_read_job", lambda s, j: store.get(j))
    monkeypatch.setattr(mdj, "_write_job", lambda s, jb: store.__setitem__(jb.get("job_id") or jb.get("id"), jb))
    monkeypatch.setattr(mdj, "_maybe_run_block_equivalence_precheck",
                        lambda *a, **k: asyncio.sleep(0))
    monkeypatch.setattr(mdj, "_resolve_pair_paths", lambda s, p, sd: ("md", "rj"))
    monkeypatch.setattr(mdj, "_make_render_callback", lambda *a, **k: (lambda **kw: None))
    monkeypatch.setattr(mdj, "_pair_label", lambda s, p: "L")

    cfg = mdj.graphic_local_mod.load_local_graphic_llm_config()
    monkeypatch.setattr(cfg, "enable_model_load", False, raising=False)
    monkeypatch.setattr(mdj.graphic_local_mod, "load_local_graphic_llm_config", lambda: cfg)

    ran = {"n": 0}

    class _Summary:
        image_blocks = 1
        described = 1
        from_cache = 0
        errors = []
        pending = []
        status = "done"
        warnings = []

    async def _enrich_ok(*a, **k):
        ran["n"] += 1
        return _Summary()

    monkeypatch.setattr(mdj.md_mod, "enrich_side", _enrich_ok, raising=False)

    res = _run(mdj.run_md_enrichment_job(sid, jid, parent_cancel_check=lambda: False))
    assert res["status"] == "done"
    assert ran["n"] == 1


# ── _qwen_lane wires cancel + stores child_job_id ────────────────────────

@pytest.fixture
def sess(tmp_path, monkeypatch):
    sid = "testsess68"
    base = tmp_path / "sessions" / sid
    (base / "pairs").mkdir(parents=True)
    session = {
        "id": sid,
        "pairs": [
            {"id": "p1", "label": "ИОС1", "left": {"has_md": True}, "right": {"has_md": True}},
        ],
    }
    monkeypatch.setattr(pq.store_mod, "get_session", lambda s: session if s == sid else None)
    monkeypatch.setattr(pq.paths_mod, "session_dir", lambda s: base)
    monkeypatch.setattr(pq.paths_mod, "pair_dir", lambda s, pid: base / "pairs" / pid)
    return sid


async def _ctx_ok():
    return (True, 16000)


def _validate_ok(session_id, pair_id):
    return (True, "ok", {"total_blocks": 1})


@pytest.mark.asyncio
async def test_qwen_lane_passes_cancel_and_stores_child_id(sess):
    """qwen_fn с широкой сигнатурой получает callable parent_cancel_check,
    а возвращённый child_job_id сохраняется в item."""
    sid = sess
    job = pq.create_job(sid, scope="selected", pair_ids=["p1"], confirm=True)
    received = {}

    async def qwen_fn(s, pid, *, force_qwen, prebuild_large_sheets, parent_cancel_check=None):
        received["cb"] = parent_cancel_check
        received["callable"] = callable(parent_cancel_check)
        # вызовем — должно вернуть bool (не упасть)
        received["cb_value"] = bool(parent_cancel_check()) if parent_cancel_check else None
        return {"status": "done", "error": None, "child_job_id": "child123"}

    async def opus_fn(s, pid, *, force_opus):
        return {"status": "done", "changes_count": 1, "error": None}

    res = await pq.run_qwen_opus_pipeline(
        sid, job["job_id"], qwen_fn=qwen_fn, opus_fn=opus_fn,
        validate_fn=_validate_ok, ctx_fn=_ctx_ok)
    assert res["status"] == "done"
    assert received["callable"] is True
    assert received["cb_value"] is False  # не отменён в этом прогоне
    it = next(i for i in res["items"] if i["pair_id"] == "p1")
    assert it.get("child_md_job_id") == "child123"


@pytest.mark.asyncio
async def test_qwen_lane_narrow_fake_still_works(sess):
    """Узкий фейк без parent_cancel_check не падает (introspection не пробрасывает)."""
    sid = sess
    job = pq.create_job(sid, scope="selected", pair_ids=["p1"], confirm=True)

    async def qwen_fn(s, pid, *, force_qwen, prebuild_large_sheets):
        return {"status": "done", "error": None}

    async def opus_fn(s, pid, *, force_opus):
        return {"status": "done", "changes_count": 1, "error": None}

    res = await pq.run_qwen_opus_pipeline(
        sid, job["job_id"], qwen_fn=qwen_fn, opus_fn=opus_fn,
        validate_fn=_validate_ok, ctx_fn=_ctx_ok)
    assert res["status"] == "done"

"""Runtime-integration tests for non-destructive findings preservation in the
Qwen→Opus pipeline (``pipeline_queue._opus_process_pair``).

All Opus / comparison work is injected as fakes — NO external API, LM Studio,
Claude Code, router, frontend, or runtime data is ever touched. These tests
verify only the runtime glue that decides WHETHER and WHEN
``comparison_merge.apply_merge`` runs (the merge logic itself is covered by
``test_stage_comparison_comparison_merge.py``).
"""
import types

import pytest

from backend.app.services.stage_comparison import pipeline_queue as pq
from backend.app.services.stage_comparison import unified_analysis as ua
from backend.app.services.stage_comparison import enriched_comparison as ec
from backend.app.services.stage_comparison import comparison_merge as cm


def _patch_common(monkeypatch, calls, *, prev_changes, run_status="done"):
    """Patch the lazy deps of ``_opus_process_pair`` and record call order in
    ``calls``. ``_read_changes_count`` is forced to 7 so we can assert the count
    is re-read AFTER merge (Opus' own changes_count=1 would be stale)."""
    async def fake_run_pair(session_id, pair_id, **kwargs):
        calls.append(("run_pair", pair_id))
        return types.SimpleNamespace(status=run_status, changes_count=1, error=None)

    def fake_get_prev(session_id, pair_id):
        calls.append(("get_prev", pair_id))
        return {"changes": prev_changes}

    def fake_read_count(session_id, pair_id):
        return 7

    monkeypatch.setattr(ua, "run_pair", fake_run_pair)
    monkeypatch.setattr(ec, "get_comparison_result", fake_get_prev)
    monkeypatch.setattr(pq, "_read_changes_count", fake_read_count)


@pytest.mark.asyncio
async def test_preserve_true_calls_apply_merge(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_PRESERVE_FINDINGS", "true")
    calls = []
    prev = [{"id": "chg_a", "type": "changed", "title": "Сечение кабеля ВРУ-1"}]
    _patch_common(monkeypatch, calls, prev_changes=prev)
    merged = []
    monkeypatch.setattr(cm, "apply_merge",
                        lambda s, p, pc: merged.append((s, p, pc)) or {"merged": True})

    res = await pq._opus_process_pair("sid", "p1", force_opus=True)

    assert res["status"] == "done"
    assert res["changes_count"] == 7              # re-read AFTER merge, not res.changes_count(=1)
    assert len(merged) == 1
    assert merged[0] == ("sid", "p1", prev)        # prev changes handed to the merge verbatim


@pytest.mark.asyncio
async def test_preserve_false_skips_merge_and_snapshot(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_PRESERVE_FINDINGS", "false")
    calls = []
    _patch_common(monkeypatch, calls, prev_changes=[{"id": "chg_a"}])

    def boom(*a, **k):
        raise AssertionError("apply_merge must not be called when preserve is off")

    monkeypatch.setattr(cm, "apply_merge", boom)

    res = await pq._opus_process_pair("sid", "p1", force_opus=True)

    assert res["status"] == "done"
    assert ("get_prev", "p1") not in calls         # no previous snapshot taken
    assert ("run_pair", "p1") in calls             # behaves like old main


@pytest.mark.asyncio
async def test_apply_merge_error_is_fail_soft(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_PRESERVE_FINDINGS", "true")
    calls = []
    _patch_common(monkeypatch, calls, prev_changes=[{"id": "chg_a"}])

    def boom(*a, **k):
        raise RuntimeError("merge blew up")

    monkeypatch.setattr(cm, "apply_merge", boom)

    res = await pq._opus_process_pair("sid", "p1", force_opus=True)

    assert res["status"] == "done"                 # pair still succeeds despite merge error
    assert res["changes_count"] == 7


@pytest.mark.asyncio
async def test_prev_snapshot_taken_before_rerun(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_PRESERVE_FINDINGS", "true")
    calls = []
    _patch_common(monkeypatch, calls, prev_changes=[{"id": "chg_a"}])
    monkeypatch.setattr(cm, "apply_merge", lambda *a, **k: {"merged": True})

    await pq._opus_process_pair("sid", "p1", force_opus=True)

    # previous changes must be snapshotted BEFORE Opus overwrites the result
    assert calls.index(("get_prev", "p1")) < calls.index(("run_pair", "p1"))


@pytest.mark.asyncio
async def test_no_prev_changes_skips_merge(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_PRESERVE_FINDINGS", "true")
    calls = []
    _patch_common(monkeypatch, calls, prev_changes=[])   # first comparison — nothing to preserve

    def boom(*a, **k):
        raise AssertionError("apply_merge must not run with empty prev_changes")

    monkeypatch.setattr(cm, "apply_merge", boom)

    res = await pq._opus_process_pair("sid", "p1", force_opus=True)

    assert res["status"] == "done"


@pytest.mark.asyncio
async def test_failed_opus_skips_merge(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_PRESERVE_FINDINGS", "true")
    calls = []
    _patch_common(monkeypatch, calls, prev_changes=[{"id": "chg_a"}], run_status="failed")

    def boom(*a, **k):
        raise AssertionError("apply_merge must not run when Opus failed")

    monkeypatch.setattr(cm, "apply_merge", boom)

    res = await pq._opus_process_pair("sid", "p1", force_opus=True)

    assert res["status"] == "failed"               # no merge onto a failed/un-rewritten result


@pytest.mark.asyncio
async def test_default_is_preserve_on(monkeypatch):
    # flag UNSET → default ON (production behaviour); apply_merge must run
    monkeypatch.delenv("STAGE_COMPARISON_PRESERVE_FINDINGS", raising=False)
    calls = []
    _patch_common(monkeypatch, calls, prev_changes=[{"id": "chg_a"}])
    merged = []
    monkeypatch.setattr(cm, "apply_merge", lambda *a, **k: merged.append(a) or {"merged": True})

    res = await pq._opus_process_pair("sid", "p1", force_opus=True)

    assert res["status"] == "done"
    assert len(merged) == 1                         # default-on preserves findings

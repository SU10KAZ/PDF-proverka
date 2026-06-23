"""Tests for production Qwen problem-block tiled retry.

Covers detection, tiling, merge, cache, feature-flag gating, baseline
preservation, fail-soft, and diagnostics. No live Qwen calls (describe/render
are fakes).
"""
from __future__ import annotations

import asyncio
import io
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import problem_block_retry as P

PIL = pytest.importorskip("PIL")
from PIL import Image  # noqa: E402


# ── helpers ──────────────────────────────────────────────────────────────────

def _cfg(**kw):
    base = dict(enabled=True, max_tiles=24, tile_width=1600, tile_height=1600,
                tile_overlap=200, render_long_side=4000, min_long_side_for_tiling=1400)
    base.update(kw)
    return P.ProblemBlockRetryConfig(**base)


def _png(w, h, color="white") -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (w, h), color).save(buf, format="PNG")
    return buf.getvalue()


class _FakeRes:
    def __init__(self, parsed=None, status="done", error=None):
        self.parsed = parsed
        self.status = status
        self.error = error


def _good_desc():
    return {"visible_text": [{"text": "a"}], "labels": [{"raw_text": "ЩР-1а"}],
            "materials": [{"name": "B25"}], "numeric_parameters": [{"value": "1000А"}],
            "confidence": 0.9}


def _render_fn_factory(w=4000, h=1200):
    def fake_render(block_id, target_long_side=4000):
        import tempfile
        d = Path(tempfile.mkdtemp())
        fp = d / "hi.png"
        Image.new("RGB", (w, h), "white").save(fp)
        return fp
    return fake_render


# ── 1-7: should_retry_problem_block ──────────────────────────────────────────

def test_retry_on_timeout():
    ok, r = P.should_retry_problem_block(
        {"status": "error", "description": {"status": "error", "error": "timeout:ReadError"}},
        None, {}, _cfg())
    assert ok and r == "timeout"


def test_retry_on_http_error():
    ok, r = P.should_retry_problem_block(
        {"status": "error", "description": {"status": "error", "error": "http_400"}},
        None, {}, _cfg())
    assert ok and r == "http_error"


def test_retry_on_invalid_json():
    ok, r = P.should_retry_problem_block(
        {"status": "error", "warnings": ["json_parse_failed"],
         "description": {"status": "error", "error": "no_parsed_json"}},
        None, {}, _cfg())
    assert ok and r in ("invalid_json", "baseline_error")


def test_retry_on_not_usable():
    ok, r = P.should_retry_problem_block(
        {"status": "done", "usable_for_diff": False,
         "description": {"visible_text": [{"text": "x"}], "labels": [{"raw_text": "y"}],
                         "materials": [{"name": "z"}], "confidence": 0.9}},
        None, {}, _cfg())
    assert ok and r == "not_usable"


def test_retry_on_low_confidence():
    ok, r = P.should_retry_problem_block(
        {"status": "done", "usable_for_diff": True, "confidence_adjusted": 0.2,
         "description": {"visible_text": [{"text": "x"}], "labels": [{"raw_text": "y"}],
                         "materials": [{"name": "z"}], "confidence": 0.2}},
        None, {}, _cfg())
    assert ok and r == "low_confidence"


def test_retry_on_empty_and_generic():
    ok, r = P.should_retry_problem_block(
        {"status": "done", "usable_for_diff": True, "description": {"confidence": 0.9}},
        None, {}, _cfg())
    assert ok and r == "empty_facts"
    ok2, r2 = P.should_retry_problem_block(
        {"status": "done", "usable_for_diff": True,
         "description": {"visible_text": [{"text": "чертёж"}, {"text": "схема"}, {"text": "таблица"}],
                         "confidence": 0.9}},
        None, {}, _cfg())
    assert ok2 and r2 in ("generic_output", "near_empty_facts")


def test_no_retry_on_good_baseline():
    ok, r = P.should_retry_problem_block(
        {"status": "done", "usable_for_diff": True, "confidence_adjusted": 0.9,
         "description": _good_desc()},
        None, {}, _cfg())
    assert not ok and r == "ok"


def test_disabled_flag_never_retries():
    ok, r = P.should_retry_problem_block(
        {"status": "error", "description": {"error": "timeout"}},
        None, {}, _cfg(enabled=False))
    assert not ok and r == "disabled"


def test_proactive_tiling_for_dense_scheme_when_flag_on():
    """Чистый dense_scheme-блок (прошёл все problem-проверки) тайлится
    проактивно, если STAGE_COMPARISON_QWEN_TILE_PROACTIVE_FOR_DENSE on."""
    good_block = {"status": "done", "usable_for_diff": True,
                  "confidence_adjusted": 0.9, "description": _good_desc(),
                  "block_type": "dense_scheme"}
    ok, r = P.should_retry_problem_block(
        good_block, None, {}, _cfg(proactive_for_dense=True))
    assert ok and r == "large_graphic_proactive"
    # scheme — тоже dense-семейство
    ok2, r2 = P.should_retry_problem_block(
        {**good_block, "block_type": "scheme"}, None, {},
        _cfg(proactive_for_dense=True))
    assert ok2 and r2 == "large_graphic_proactive"


def test_proactive_tiling_off_by_default_and_skips_non_dense():
    good_dense = {"status": "done", "usable_for_diff": True,
                  "confidence_adjusted": 0.9, "description": _good_desc(),
                  "block_type": "dense_scheme"}
    # флаг выключен → ok
    ok, r = P.should_retry_problem_block(good_dense, None, {}, _cfg())
    assert not ok and r == "ok"
    # флаг включён, но это не схема (план/таблица/общее) → не тайлим проактивно
    for bt in ("plan", "table_legend", "photo_or_general", "stamp"):
        ok2, r2 = P.should_retry_problem_block(
            {**good_dense, "block_type": bt}, None, {},
            _cfg(proactive_for_dense=True))
        assert not ok2 and r2 == "ok", bt


# ── 8-10: tiling ─────────────────────────────────────────────────────────────

def test_tile_splitter_creates_overlapping_tiles():
    tiles, meta = P.split_image_into_tiles(_png(4000, 1600), tile_width=1600,
                                           tile_height=1600, overlap=200, max_tiles=24)
    assert len(tiles) >= 3
    assert meta["overlap"] == 200
    assert all(t[:4] == b"\x89PNG" for t in tiles)


def test_tile_splitter_respects_max_tiles():
    tiles, meta = P.split_image_into_tiles(_png(8000, 6000), tile_width=1000,
                                           tile_height=1000, overlap=100, max_tiles=8)
    assert 1 <= len(tiles) <= 8
    assert meta["downscaled"] is True


def test_small_image_single_tile():
    tiles, meta = P.split_image_into_tiles(_png(800, 600), tile_width=1600,
                                           tile_height=1600, overlap=200, max_tiles=24)
    assert len(tiles) == 1


# ── 11-12: merge ─────────────────────────────────────────────────────────────

def test_merge_dedup_and_provenance():
    merged = P.merge_tiled_qwen_results([
        {"tile_id": "t0", "labels": [{"raw_text": "ЩР-1а"}], "visible_text": [{"text": "a"}], "confidence": 0.7},
        {"tile_id": "t1", "labels": [{"raw_text": "ЩР-1а"}, {"raw_text": "ВРУ-2"}], "confidence": 0.6},
    ])
    assert len(merged["labels"]) == 2  # ЩР-1а deduped
    # ЩР-1а confirmed by 2 tiles
    conf_counts = [l.get("_confirmations") for l in merged["labels"] if l.get("raw_text") == "ЩР-1а"]
    assert conf_counts and conf_counts[0] == 2
    assert merged["usable_for_diff"] is True


def test_merge_empty_is_not_usable():
    merged = P.merge_tiled_qwen_results([{"tile_id": "t0", "confidence": 0.5}])
    assert merged["usable_for_diff"] is False
    assert P._facts_total(merged) == 0


# ── 13-15: orchestration / pipeline safety ───────────────────────────────────

def test_retry_replaces_bad_baseline():
    async def fake_describe(p, prompt):
        return _FakeRes(parsed={"labels": [{"raw_text": "ЩР-1а"}], "visible_text": [{"text": "L"}], "confidence": 0.7})
    item = {"status": "error", "side_block_id": "B1", "usable_for_diff": False,
            "description": {"status": "error", "error": "timeout"}, "warnings": []}
    out = asyncio.run(P.maybe_run_problem_block_retry(
        item=item, side_block={"id": "B1"}, error=None,
        render_crop=_render_fn_factory(), describe_fn=fake_describe, cfg=_cfg(), model="qwen"))
    assert out["status"] == "done"
    assert out["method_used"] == "tiled_retry"
    assert out["usable_for_diff"] is True
    assert out["problem_block_retry"]["retry_improved"] is True


def test_good_baseline_not_retried():
    called = {"n": 0}
    async def fake_describe(p, prompt):
        called["n"] += 1
        return _FakeRes(parsed={"labels": [{"raw_text": "x"}]})
    item = {"status": "done", "side_block_id": "B1", "usable_for_diff": True,
            "confidence_adjusted": 0.9, "description": _good_desc(), "warnings": []}
    out = asyncio.run(P.maybe_run_problem_block_retry(
        item=item, side_block={"id": "B1"}, error=None,
        render_crop=_render_fn_factory(), describe_fn=fake_describe, cfg=_cfg(), model="qwen"))
    assert called["n"] == 0  # retry never ran
    assert "method_used" not in out or out.get("method_used") != "tiled_retry"
    assert "problem_block_retry" not in out  # untouched good block


def test_retry_failure_does_not_break_and_preserves_baseline():
    async def boom(p, prompt):
        raise RuntimeError("tile blew up")
    item = {"status": "error", "side_block_id": "B1", "usable_for_diff": False,
            "description": {"status": "error", "error": "timeout"}, "warnings": []}
    out = asyncio.run(P.maybe_run_problem_block_retry(
        item=item, side_block={"id": "B1"}, error=None,
        render_crop=_render_fn_factory(), describe_fn=boom, cfg=_cfg(), model="qwen"))
    # pipeline not broken; baseline preserved; retry failed recorded
    assert out["baseline_result"]["status"] == "error"
    assert out["problem_block_retry"]["retry_status"] == "failed"
    assert out["method_used"] == "baseline"
    assert out["description"]["error"] == "timeout"  # baseline kept


# ── 16-17: cache ─────────────────────────────────────────────────────────────

def test_cache_key_changes_with_params():
    img = _png(100, 100)
    base = dict(session_id="s", pair_id="p", side="left", block_id="B1",
                image_bytes=img, model="qwen")
    # render_long_side actually changes the rendered input (render_dpi is
    # informational), so it must change the key; tile size and model too.
    k1 = P.compute_retry_cache_key(cfg=_cfg(render_long_side=3000), **base)
    k2 = P.compute_retry_cache_key(cfg=_cfg(render_long_side=4000), **base)
    k3 = P.compute_retry_cache_key(cfg=_cfg(tile_width=1000), **base)
    k4 = P.compute_retry_cache_key(cfg=_cfg(), **{**base, "model": "other"})
    k5 = P.compute_retry_cache_key(cfg=_cfg(), **{**base, "image_bytes": b"y" * 99})
    assert len({k1, k2, k3, k4, k5}) == 5


def test_cache_hit_skips_qwen():
    store = {}
    calls = {"n": 0}
    async def fake_describe(p, prompt):
        calls["n"] += 1
        return _FakeRes(parsed={"labels": [{"raw_text": "ЩР-1а"}], "confidence": 0.7})
    cfg = _cfg()
    render = _render_fn_factory()

    def cr(k):
        return store.get(k)

    def cw(k, v):
        store[k] = v

    item1 = {"status": "error", "side_block_id": "B1", "usable_for_diff": False,
             "description": {"status": "error", "error": "timeout"}, "warnings": []}
    asyncio.run(P.maybe_run_problem_block_retry(
        item=item1, side_block={"id": "B1"}, error=None, render_crop=render,
        describe_fn=fake_describe, cfg=cfg, model="qwen",
        cache_read=cr, cache_write=cw))
    first_calls = calls["n"]
    assert first_calls > 0 and store

    item2 = {"status": "error", "side_block_id": "B1", "usable_for_diff": False,
             "description": {"status": "error", "error": "timeout"}, "warnings": []}
    out2 = asyncio.run(P.maybe_run_problem_block_retry(
        item=item2, side_block={"id": "B1"}, error=None, render_crop=render,
        describe_fn=fake_describe, cfg=cfg, model="qwen",
        cache_read=cr, cache_write=cw))
    assert calls["n"] == first_calls  # no new Qwen calls
    assert out2["problem_block_retry"].get("cache_hit") is True


# ── 18: diagnostics ──────────────────────────────────────────────────────────

def test_diagnostics_fields_present():
    async def fake_describe(p, prompt):
        return _FakeRes(parsed={"labels": [{"raw_text": "ЩР-1а"}], "confidence": 0.7})
    item = {"status": "error", "side_block_id": "B1", "usable_for_diff": False,
            "description": {"status": "error", "error": "timeout"}, "warnings": []}
    out = asyncio.run(P.maybe_run_problem_block_retry(
        item=item, side_block={"id": "B1"}, error=None,
        render_crop=_render_fn_factory(), describe_fn=fake_describe, cfg=_cfg(), model="qwen"))
    d = out["problem_block_retry"]
    for f in ("retry_reason", "retry_method", "tiles_count", "final_method_used",
              "retry_status", "retry_improved"):
        assert f in d, f
    assert d["final_method_used"] == "tiled_retry"


# ── 19-20: feature flag gating at orchestration level ────────────────────────

def test_flag_disabled_orchestration_noop():
    async def fake_describe(p, prompt):
        return _FakeRes(parsed={"labels": [{"raw_text": "x"}]})
    item = {"status": "error", "side_block_id": "B1",
            "description": {"status": "error", "error": "timeout"}, "warnings": []}
    out = asyncio.run(P.maybe_run_problem_block_retry(
        item=item, side_block={"id": "B1"}, error=None,
        render_crop=_render_fn_factory(), describe_fn=fake_describe,
        cfg=_cfg(enabled=False), model="qwen"))
    assert "problem_block_retry" not in out  # disabled → untouched
    assert out["status"] == "error"


def test_flag_enabled_triggers_for_problem_block():
    async def fake_describe(p, prompt):
        return _FakeRes(parsed={"labels": [{"raw_text": "ЩР-1а"}], "visible_text": [{"text": "L"}], "confidence": 0.7})
    item = {"status": "error", "side_block_id": "B1", "usable_for_diff": False,
            "description": {"status": "error", "error": "timeout"}, "warnings": []}
    out = asyncio.run(P.maybe_run_problem_block_retry(
        item=item, side_block={"id": "B1"}, error=None,
        render_crop=_render_fn_factory(), describe_fn=fake_describe,
        cfg=_cfg(enabled=True), model="qwen"))
    assert out["problem_block_retry"]["retry_attempted"] is True
    assert out["method_used"] == "tiled_retry"


def test_summary_aggregates():
    descs = [
        {"problem_block_retry": {"retry_attempted": True, "retry_status": "done", "retry_improved": True, "cache_hit": False}},
        {"problem_block_retry": {"retry_attempted": True, "retry_status": "failed", "retry_improved": False}},
        {"problem_block_retry": {"retry_attempted": True, "retry_status": "done", "retry_improved": True, "cache_hit": True}},
        {"status": "done"},  # no retry
    ]
    s = P.summarize_problem_block_retry(descs, _cfg())
    assert s["blocks_checked"] == 4
    assert s["retry_attempted"] == 3
    assert s["retry_done"] == 2
    assert s["retry_failed"] == 1
    assert s["improved"] == 2
    assert s["cache_hits"] == 1

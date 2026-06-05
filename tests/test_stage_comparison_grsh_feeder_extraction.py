# -*- coding: utf-8 -*-
"""Tests for GRSH (ГРЩ/ВРУ) dense single-line feeder extraction.

Covers spec cases:
  8. GRSH feeder extraction uses block-PDF render, not page crop
  9. tile prompt receives only tile-local OCR vocabulary
 10. no live Qwen/Opus in tests (describe_fn fully mocked, no network)
  + deterministic merge feeders[] + recall vs text-layer anchors
  + anti-hallucination: visual_unverified + rejected_artificial_series
  + flags default OFF

NO network calls and NO real LLM calls anywhere in this file.
"""
from __future__ import annotations

import asyncio
import io

import pytest

from backend.app.services.stage_comparison import grsh_feeder_extraction as gfe


# ─── flags ────────────────────────────────────────────────────────────────


def test_flags_default_off(monkeypatch):
    monkeypatch.delenv("STAGE_COMPARISON_GRSH_FEEDER_EXTRACTION_ENABLED", raising=False)
    monkeypatch.delenv("STAGE_COMPARISON_GRSH_FEEDER_USE_BLOCK_PDF", raising=False)
    assert gfe.grsh_feeder_extraction_enabled() is False
    assert gfe.grsh_feeder_use_block_pdf() is True  # block-PDF preferred WITHIN the (OFF) mode


def test_config_env_override(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_GRSH_FEEDER_TILE_CONCURRENCY", "1")
    monkeypatch.setenv("STAGE_COMPARISON_GRSH_FEEDER_RENDER_LONG_SIDE", "7000")
    cfg = gfe.load_grsh_feeder_config()
    assert cfg.concurrency == 1 and cfg.render_long_side == 7000


# ─── case 9: tile-local OCR vocabulary ────────────────────────────────────


def test_tile_local_vocabulary_is_spatially_filtered():
    """case 9: only words whose centre falls inside the tile bbox are returned."""
    words = [
        {"text": "LEFTWORD", "bbox": [10, 10, 50, 20], "page": 1},     # centre ~ (30,15)
        {"text": "RIGHTWORD", "bbox": [650, 10, 690, 20], "page": 1},  # centre ~ (670,15)
    ]
    render_size = (7000, 3460)
    pdf_page_size = (700, 346)  # scale 10x
    left_tile = [0, 0, 1500, 2284]
    right_tile = [5500, 0, 7000, 2284]
    left_vocab = gfe.tile_local_vocabulary(words, left_tile, render_size=render_size, pdf_page_size=pdf_page_size)
    right_vocab = gfe.tile_local_vocabulary(words, right_tile, render_size=render_size, pdf_page_size=pdf_page_size)
    assert "LEFTWORD" in left_vocab and "RIGHTWORD" not in left_vocab
    assert "RIGHTWORD" in right_vocab and "LEFTWORD" not in right_vocab


def test_tile_vocabulary_empty_without_words():
    assert gfe.tile_local_vocabulary([], [0, 0, 100, 100], render_size=(100, 100), pdf_page_size=(100, 100)) == []


# ─── case 8 + 10: block-PDF render drives tiled extraction, Qwen mocked ────


def _white_png(w: int, h: int) -> bytes:
    from PIL import Image
    buf = io.BytesIO()
    Image.new("RGB", (w, h), "white").save(buf, format="PNG")
    return buf.getvalue()


def test_extract_feeders_uses_given_render_and_mocked_qwen():
    """case 8/10: extraction tiles the provided block-PDF render and calls only the
    injected describe_fn (no live Qwen). Each tile prompt carries its tile-local vocab."""
    render = _white_png(7000, 3460)  # a block-PDF render (not a page crop)
    # Unique markers (NOT present in the static prompt template) so the locality
    # assertion tests the per-tile OCR_VOCAB prefix, not the prompt examples.
    words = [
        {"text": "ZLEFT9", "bbox": [10, 10, 60, 20], "page": 1},
        {"text": "ГРЩ1-РП1-1", "bbox": [12, 30, 120, 40], "page": 1},
        {"text": "ZRIGHT9", "bbox": [660, 10, 700, 20], "page": 1},
    ]
    seen_prompts: list[str] = []
    calls = {"n": 0}

    async def fake_describe(png_bytes: bytes, prompt: str) -> dict:
        calls["n"] += 1
        seen_prompts.append(prompt)
        assert isinstance(png_bytes, (bytes, bytearray)) and len(png_bytes) > 0
        # left tile gets ВРУ1, right tile gets ВРУ4 — verify vocab is local
        return {"status": "done", "parsed": {"status": "done", "feeders": [
            {"consumer": "ВРУ1", "designation": "ГРЩ1-РП1-1", "breaker": "1QF1",
             "breaker_rating": "3P 800A", "cable_section": "5х150"}], "connections": [], "equipment": []}}

    cfg = gfe.GrshFeederConfig(n_cols=3, n_rows=1, max_tiles=3, concurrency=1)
    res = asyncio.run(gfe.extract_feeders_for_block(
        render_png_bytes=render, text_layer_words=words, pdf_page_size=(700, 346),
        describe_fn=fake_describe, cfg=cfg, image_size=(7000, 3460)))

    assert res["n_tiles"] == 3
    assert calls["n"] == 3  # exactly 3 tiles, all via the mock — no live Qwen
    # tile-local vocab: leftmost prompt carries ZLEFT9, rightmost carries ZRIGHT9
    assert any("ZLEFT9" in p for p in seen_prompts)
    assert any("ZRIGHT9" in p for p in seen_prompts)
    # no single tile prompt contains BOTH far-apart markers (locality holds)
    assert not any(("ZLEFT9" in p and "ZRIGHT9" in p) for p in seen_prompts)


# ─── merge + recall + anti-hallucination ──────────────────────────────────


def test_merge_recall_and_anti_hallucination():
    anchors = gfe.extract_text_layer_anchors(
        "ВРУ1 ГРЩ1-РП1-1 ВРУ4 ГРЩ1-РП1-4 ВРУ-ХЦ ГРЩ1-РП1-7")
    tile_results = {"render_size": [7000, 3460], "n_tiles": 2, "tiles": [
        {"tile_id": "r0_c0", "status": "done", "parsed": {"feeders": [
            {"consumer": "ВРУ1", "designation": "ГРЩ1-РП1-1", "breaker_rating": "800A"},
            {"consumer": "ВРУ4", "designation": "ГРЩ1-РП1-4", "breaker_rating": "630A"},
        ], "connections": [{"from": "ТП1", "to": "ГРЩ1 РП1"}], "equipment": []}},
        {"tile_id": "r0_c1", "status": "done", "parsed": {"feeders": [
            {"consumer": "ВРУ-ХЦ", "designation": "ГРЩ1-РП1-7", "breaker_rating": "200A"},
            # hallucinated artificial-series row NOT in the text layer:
            {"consumer": "ЩА-1.40", "designation": "ГРЩ1-РП1-99", "breaker_rating": "16A"},
        ], "connections": [], "equipment": []}},
    ]}
    merged = gfe.merge_tile_feeders(tile_results, anchors)
    d = merged["diagnostics"]
    assert d["designation_recall"] == 1.0          # all 3 text-layer designations recovered
    assert d["consumer_recall"] >= 0.8
    assert d["connections_count"] == 1
    # the hallucinated ГРЩ1-РП1-99 row is flagged, real ones verified
    statuses = {f["designation"]: f["anchor_status"] for f in merged["feeders"]}
    assert statuses.get("ГРЩ1-РП1-1") == "verified"
    assert "ГРЩ1-РП1-99" in d["rejected_artificial_series"]


def test_merge_marks_unverified_not_deleted():
    """Unread/visual-only labels are kept as visual_unverified, never silently deleted."""
    anchors = gfe.extract_text_layer_anchors("ВРУ1 ГРЩ1-РП1-1")
    tile_results = {"render_size": [100, 100], "n_tiles": 1, "tiles": [
        {"tile_id": "r0_c0", "status": "done", "parsed": {"feeders": [
            {"consumer": "ВРУ1", "designation": "ГРЩ1-РП1-1"},
            {"consumer": "НЕКТО", "designation": "ВЫДУМКА-2"},
        ], "connections": [], "equipment": []}},
    ]}
    merged = gfe.merge_tile_feeders(tile_results, anchors)
    desigs = {f["designation"] for f in merged["feeders"]}
    assert "ВЫДУМКА-2" in desigs  # kept, not deleted
    statuses = {f["designation"]: f["anchor_status"] for f in merged["feeders"]}
    assert statuses["ВЫДУМКА-2"] == "visual_unverified"


def test_make_feeder_tiles_respects_max_tiles():
    cfg = gfe.GrshFeederConfig(n_cols=7, n_rows=2, max_tiles=16)
    tiles = gfe.make_feeder_tiles(7000, 3460, cfg)
    assert 1 <= len(tiles) <= 16
    for t in tiles:
        x0, y0, x1, y1 = t["bbox_render_px"]
        assert 0 <= x0 < x1 <= 7000 and 0 <= y0 < y1 <= 3460


def test_render_feeder_table_md_has_feeders():
    merged = {"feeders": [
        {"consumer": "ВРУ1", "designation": "ГРЩ1-РП1-1", "breaker": "1QF1",
         "breaker_rating": "3P 800A", "cable_section": "5х150", "p_calc_kw": 449.3,
         "i_calc_a": 717.3, "anchor_status": "verified"}],
        "diagnostics": {"designation_recall": 1.0, "consumer_recall": 1.0}}
    md = gfe.render_feeder_table_md(merged)
    assert "GRSH_FEEDERS" in md and "ВРУ1" in md and "ГРЩ1-РП1-1" in md and "3P 800A" in md


# ─── contour B wiring into md_image_enrichment (mocked Qwen, local PDF) ────


def test_enrich_wiring_renders_feeder_table_in_md():
    """The feeder table produced by contour B renders into the enriched MD body."""
    from backend.app.services.stage_comparison import md_image_enrichment as mie
    payload = {"status": "done", "image_kind": "scheme",
               "grsh_feeder_table": gfe.render_feeder_table_md({"feeders": [
                   {"consumer": "ВРУ1", "designation": "ГРЩ1-РП1-1", "breaker_rating": "800A",
                    "anchor_status": "verified"}], "diagnostics": {}})}
    body = mie._format_qwen_description_md(payload, model="qwen", page=21, block_id="B")
    assert "GRSH_FEEDERS" in body and "ВРУ1" in body


def test_grsh_feeder_helper_end_to_end_local_pdf(tmp_path, monkeypatch):
    """case 8: _run_grsh_feeder_extraction_for_block tiles a local block-PDF render
    and calls only the (mocked) Qwen — no network, no live LLM. Produces a feeder table."""
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "cmp"))
    monkeypatch.setenv("STAGE_COMPARISON_GRSH_FEEDER_RENDER_LONG_SIDE", "2400")
    monkeypatch.setenv("STAGE_COMPARISON_GRSH_FEEDER_TILE_LONG_SIDE", "1000")
    monkeypatch.setenv("STAGE_COMPARISON_GRSH_FEEDER_N_COLS", "2")
    monkeypatch.setenv("STAGE_COMPARISON_GRSH_FEEDER_N_ROWS", "1")
    from backend.app.services.stage_comparison import md_image_enrichment as mie
    from backend.app.services.stage_comparison import graphic_llm_local as g

    import fitz
    doc = fitz.open()
    page = doc.new_page(width=600, height=300)
    page.insert_text((40, 40), "VRU1 GRSCH1-RP1-1 QF1 3P 800A", fontsize=10)
    pdf = tmp_path / "block.pdf"
    pdf.write_bytes(doc.tobytes())
    doc.close()
    side_block = {"id": "B", "raw": {"image_file": str(pdf),
                                     "pdfplumber_text": "VRU1 GRSCH1-RP1-1 QF1 800A"}}

    class _Res:
        parsed = {"status": "done", "feeders": [
            {"consumer": "VRU1", "designation": "GRSCH1-RP1-1", "breaker_rating": "800A"}],
            "connections": [{"from": "TP1", "to": "RP1"}], "equipment": []}
        status = "done"

    calls = {"n": 0}

    async def fake_once(**kw):
        calls["n"] += 1
        return _Res(), ""

    monkeypatch.setattr(g, "_describe_image_once", fake_once)
    cfg = g.load_local_graphic_llm_config()
    out = asyncio.run(mie._run_grsh_feeder_extraction_for_block(
        "sid", "pid", "left", side_block, cfg=cfg))
    assert out is not None
    assert calls["n"] >= 1                                  # mocked Qwen used, no live call
    assert "grsh_feeder_table" in out["desc_payload"]
    assert out["diagnostics"]["method"] == "grsh_feeder_tiled"
    assert out["diagnostics"]["block_source"] == "image_file"  # local PDF, no network
    assert "grsh_feeders_extracted" in out["diagnostics"]


def test_grsh_feeder_helper_failsoft_without_block_pdf(tmp_path, monkeypatch):
    """Fail-soft: no crop_url/image_file/PDF → helper returns None → caller keeps single-shot."""
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "cmp"))
    from backend.app.services.stage_comparison import md_image_enrichment as mie
    from backend.app.services.stage_comparison import graphic_llm_local as g
    cfg = g.load_local_graphic_llm_config()
    out = asyncio.run(mie._run_grsh_feeder_extraction_for_block(
        "sid", "pid", "left", {"id": "B", "raw": {}}, cfg=cfg))
    assert out is None

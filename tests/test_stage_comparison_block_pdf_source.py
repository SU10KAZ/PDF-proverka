# -*- coding: utf-8 -*-
"""Tests for the universal block-PDF source helper (block_pdf_source.py) and its
gated integration into md_image_enrichment.

Covers spec cases:
  1. block with crop_url uses block-PDF source
  2. crop_url PDF preferred over page crop (gated helper returns block-PDF render)
  3. text layer is extracted before Qwen prompt (resolve returns text_layer_text)
  4. usable pdfplumber_text becomes OCR vocabulary
  5. garbled text layer falls back (usable=False) to Chandra OCR
  6. Qwen-only label not in text layer becomes visual_unverified
  7. text-layer-only important anchor becomes missing_text_layer_anchor
 10. no live Qwen/Opus in tests (no network; http_get injected)
 11. existing ordinary image block flow still works with fallback (flag OFF)

These tests perform NO network calls and NO LLM calls.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from backend.app.services.stage_comparison import block_pdf_source as bps
from backend.app.services.stage_comparison import blocks as blocks_mod


# ─── helpers ──────────────────────────────────────────────────────────────


def _make_pdf_with_text(words: list[str]) -> bytes:
    """Build a tiny single-page PDF carrying a real text layer."""
    import fitz
    doc = fitz.open()
    page = doc.new_page(width=600, height=400)
    y = 40
    for w in words:
        page.insert_text((40, y), w, fontsize=11)
        y += 24
    data = doc.tobytes()
    doc.close()
    return data


@pytest.fixture
def block_pdf_bytes() -> bytes:
    # Latin tokens: PyMuPDF's base-14 Helvetica has no Cyrillic glyphs, and the
    # normalizer transliterates Latin look-alikes (VRU->ВРУ) anyway, so the
    # validation/anchor logic is exercised identically.
    return _make_pdf_with_text(["VRU1", "GRSCH1-RP1-1", "QF1 3P 800A", "PPGNG 5x150mm"])


# ─── case 1, 2, 3, 4, 10: resolve + extract + render via crop_url ─────────


def test_crop_url_resolves_block_pdf_source(tmp_path, block_pdf_bytes):
    """case 1/10: a block with crop_url resolves to a block-PDF (no network — http_get injected)."""
    block = {"id": "B1", "raw": {"crop_url": "https://example.r2.dev/crops/B1.pdf"}}
    calls = {"n": 0}

    def fake_get(url):
        calls["n"] += 1
        assert "B1.pdf" in url
        return (200, "application/pdf", block_pdf_bytes)

    src = bps.resolve_block_pdf_source(block, cache_dir=tmp_path, http_get=fake_get)
    assert src.ok and src.source == "crop_url"
    assert src.crop_url_status == 200 and not src.fallback_used
    assert src.pdf_path.exists()
    assert calls["n"] == 1  # exactly one (injected) fetch, no live network


def test_crop_url_preferred_render_over_page_crop(tmp_path, block_pdf_bytes):
    """case 2: render comes from the block-PDF, independent of any page crop."""
    block = {"id": "B2", "raw": {"crop_url": "https://x.r2.dev/B2.pdf"}}
    src = bps.resolve_block_pdf_source(block, cache_dir=tmp_path,
                                       http_get=lambda u: (200, "application/pdf", block_pdf_bytes))
    out = tmp_path / "render.png"
    rb = bps.render_block_pdf(src.pdf_path, long_side=1200, out_path=out)
    assert rb.ok and rb.source == "block_pdf" and out.exists()
    assert max(rb.width, rb.height) == 1200


def test_text_layer_extracted_from_block_pdf(tmp_path, block_pdf_bytes):
    """case 3/4: the PDF text layer is extracted and is usable as OCR vocabulary."""
    pdf = tmp_path / "b.pdf"
    pdf.write_bytes(block_pdf_bytes)
    tl = bps.extract_block_text_layer(pdf, result_json_text=None)
    assert tl.ok and tl.usable
    assert "VRU1" in tl.text and "GRSCH1-RP1-1" in tl.text
    assert tl.quality["chars"] > 0 and tl.quality["word_count"] > 0
    anchors = bps.build_ocr_literal_anchors(tl)
    assert any("VRU1" in t for t in anchors["tokens"])


def test_result_json_pdfplumber_is_fast_path(tmp_path):
    """case 4: usable result.json pdfplumber_text becomes the text layer without opening the PDF."""
    tl = bps.extract_block_text_layer(
        None, result_json_text="ВРУ1 ГРЩ1-РП1-1 QF1 800А ППГнг(А)-HF 5х150")
    assert tl.source == "result_json" and tl.usable
    assert "ВРУ1" in tl.text


# ─── case 5: garbled text layer falls back ────────────────────────────────


def test_garbled_text_layer_not_usable(tmp_path):
    """case 5: a garbled text layer is marked not-usable so callers fall back to Chandra OCR."""
    garbled = "@#$%^&*()_+{}|:<>?~`" * 6
    tl = bps.extract_block_text_layer(None, result_json_text=garbled)
    assert tl.ok  # text present
    assert tl.usable is False  # but not usable → caller falls back
    assert tl.quality["garbled_ratio"] > 0.5


def test_garbled_result_json_falls_back_to_pdf(tmp_path, block_pdf_bytes):
    """case 5: garbled result.json text is skipped; the real PDF text layer is used instead."""
    pdf = tmp_path / "b.pdf"
    pdf.write_bytes(block_pdf_bytes)
    tl = bps.extract_block_text_layer(pdf, result_json_text="@@@@@@@@@@@@@@@@@@@@@")
    assert tl.source in ("pymupdf", "pdfplumber") and tl.usable
    assert "VRU1" in tl.text


# ─── case 6, 7: anti-hallucination validation ─────────────────────────────


def test_qwen_only_label_is_visual_unverified(tmp_path, block_pdf_bytes):
    """case 6: a Qwen label not present in the text layer → visual_unverified (not deleted)."""
    pdf = tmp_path / "b.pdf"
    pdf.write_bytes(block_pdf_bytes)
    tl = bps.extract_block_text_layer(pdf)
    val = bps.validate_anchors_against_text_layer(["VRU1", "ZZZ-1.99"], tl)
    assert "VRU1" in val["verified_by_text_layer"]
    assert "ZZZ-1.99" in val["visual_unverified"]
    assert "ZZZ-1.99" in val["rejected_artificial_series"]


def test_text_layer_only_anchor_is_missing(tmp_path, block_pdf_bytes):
    """case 7: a text-layer anchor that Qwen did not extract → missing_text_layer_anchor."""
    pdf = tmp_path / "b.pdf"
    pdf.write_bytes(block_pdf_bytes)
    tl = bps.extract_block_text_layer(pdf)
    val = bps.validate_anchors_against_text_layer(
        ["VRU1"], tl, expected_anchors=["VRU1", "GRSCH1-RP1-1"])
    assert "GRSCH1-RP1-1" in val["missing_text_layer_anchors"]
    assert "VRU1" not in val["missing_text_layer_anchors"]


# ─── case 11: priority + fallback + flag OFF (ordinary flow intact) ────────


def test_image_file_used_when_no_crop_url(tmp_path, block_pdf_bytes):
    """Priority #2: local image_file PDF is used when crop_url is absent."""
    pdf = tmp_path / "frag.pdf"
    pdf.write_bytes(block_pdf_bytes)
    block = {"id": "B3", "raw": {"image_file": str(pdf)}}
    src = bps.resolve_block_pdf_source(block, cache_dir=tmp_path)
    assert src.ok and src.source == "image_file" and src.pdf_path == pdf


def test_no_source_means_fallback(tmp_path):
    """case 11: no crop_url/image_file → source=none, fallback_used=True (caller uses page crop)."""
    src = bps.resolve_block_pdf_source({"id": "B4", "raw": {}}, cache_dir=tmp_path)
    assert src.source == "none" and src.fallback_used and not src.ok


def test_normalize_blocks_preserves_pdf_fields(tmp_path):
    """case 11: normalize_blocks_from_result_json preserves crop_url/image_file/pdfplumber_text."""
    import json
    rj = {"pages": [{"page_number": 1, "width": 1000, "height": 800, "blocks": [
        {"id": "X1", "block_type": "image", "coords_px": [10, 10, 500, 400],
         "crop_url": "https://x.r2.dev/X1.pdf", "image_file": "/tmp/X1.pdf",
         "pdfplumber_text": "ВРУ1 ГРЩ1-РП1-1"},
        {"id": "X2", "block_type": "image", "coords_px": [0, 0, 100, 100]},  # no pdf fields
    ]}]}
    p = tmp_path / "result.json"
    p.write_text(json.dumps(rj), encoding="utf-8")
    blocks, _meta = blocks_mod.normalize_blocks_from_result_json(p)
    by_id = {b["id"]: b for b in blocks}
    assert by_id["X1"]["raw"]["crop_url"] == "https://x.r2.dev/X1.pdf"
    assert by_id["X1"]["raw"]["pdfplumber_text"] == "ВРУ1 ГРЩ1-РП1-1"
    # absent fields are simply not present (ordinary flow unaffected)
    assert "crop_url" not in by_id["X2"]["raw"]


def test_enrichment_block_pdf_flag_off_by_default(monkeypatch):
    """case 11: block-PDF source path is OFF by default → ordinary page-crop flow intact."""
    monkeypatch.delenv("STAGE_COMPARISON_BLOCK_PDF_SOURCE_ENABLED", raising=False)
    from backend.app.services.stage_comparison import md_image_enrichment as mie
    assert mie.block_pdf_source_enabled() is False
    # helper returns None for a block with no crop_url/image_file/pdfplumber_text
    out = mie.resolve_block_pdf_for_enrichment(
        "sid", "pid", "left", {"id": "B", "raw": {}}, render_target_long_side=1200)
    assert out is None

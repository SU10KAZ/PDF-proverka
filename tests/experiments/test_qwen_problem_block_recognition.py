"""Tests for the qwen_problem_block_recognition experiment utilities.

These cover the offline-testable surface: input builders, render path planning,
fallback/oversize handling, result normalization (done/error/unsupported), and
the quality scorer's best-pick. No live Qwen calls.
"""
from __future__ import annotations

import base64
import io
import json
import sys
from pathlib import Path

import pytest

EXP_DIR = Path(__file__).resolve().parents[2] / "experiments" / "qwen_problem_block_recognition"
if not EXP_DIR.exists():
    pytest.skip(
        "experiments/qwen_problem_block_recognition удалён (cb764e1b) — "
        "исходники эксперимента недоступны",
        allow_module_level=True,
    )
if str(EXP_DIR) not in sys.path:
    sys.path.insert(0, str(EXP_DIR))

import input_builders as IB          # noqa: E402
import result_normalizer as RN       # noqa: E402
import quality_scorer as QS          # noqa: E402
import prompt_variants as PV         # noqa: E402

try:
    import fitz  # PyMuPDF
    _HAVE_FITZ = True
except Exception:  # pragma: no cover
    fitz = None
    _HAVE_FITZ = False

try:
    import PIL  # noqa: F401
    _HAVE_PIL = True
except Exception:  # pragma: no cover
    _HAVE_PIL = False

requires_fitz = pytest.mark.skipif(not _HAVE_FITZ, reason="PyMuPDF (fitz) not installed")
requires_pil = pytest.mark.skipif(not _HAVE_PIL, reason="Pillow not installed")


# ── helpers ───────────────────────────────────────────────────────────────

def _tiny_pdf_bytes(width=400, height=200, text="ЩР-1а 1000А") -> bytes:
    doc = fitz.open()
    page = doc.new_page(width=width, height=height)
    page.insert_text((20, 60), text)
    b = doc.tobytes()
    doc.close()
    return b


def _block_for(pdf_path: str):
    return {"bbox": None, "bbox_norm": [0.05, 0.1, 0.95, 0.9],
            "page_width": None, "page_height": None}


# ── input builders ─────────────────────────────────────────────────────────

def test_image_data_url_builder():
    png = b"\x89PNG\r\n\x1a\nFAKE"
    content, meta = IB.build_image_message("hi", png)
    assert content[0]["type"] == "text"
    assert content[1]["type"] == "image_url"
    assert content[1]["image_url"]["url"].startswith("data:image/png;base64,")
    decoded = base64.b64decode(content[1]["image_url"]["url"].split(",", 1)[1])
    assert decoded == png
    assert meta["input_mode"] == "image_data_url"
    assert meta["input_size_bytes"] == len(png)


def test_pdf_base64_builder_shapes_and_mime():
    pdf = b"%PDF-1.4 fake"
    shapes, meta = IB.build_pdf_base64_messages("hi", pdf)
    names = [s[0] for s in shapes]
    assert "image_url_pdf_data" in names
    assert "file_filedata" in names
    # the image_url shape must carry the application/pdf data URL
    img_shape = dict(shapes)["image_url_pdf_data"]
    assert img_shape[1]["image_url"]["url"].startswith("data:application/pdf;base64,")
    assert meta["input_mode"] == "pdf_base64"
    assert meta["input_size_bytes"] == len(pdf)


def test_pdf_url_builder():
    shapes, meta = IB.build_pdf_url_messages("hi", "https://x/y.pdf")
    assert meta["input_mode"] == "pdf_url"
    img = dict(shapes)["image_url_remote_pdf"]
    assert img[1]["image_url"]["url"] == "https://x/y.pdf"


def test_multi_image_builder_counts():
    pngs = [b"a", b"bb", b"ccc"]
    content, meta = IB.build_multi_image_message("hi", pngs)
    assert meta["n_images"] == 3
    assert meta["input_size_bytes"] == 6
    assert sum(1 for c in content if c.get("type") == "image_url") == 3


# ── pdf_tools render path builders + fallbacks ───────────────────────────────

def test_high_dpi_render_path(tmp_path):
    import pdf_tools as PT
    pdf = tmp_path / "x.pdf"
    pdf.write_bytes(_tiny_pdf_bytes())
    block = _block_for(str(pdf))
    rr300 = PT.render_block_at_dpi(str(pdf), 1, block, 300)
    rr72 = PT.render_block_at_dpi(str(pdf), 1, block, 72)
    assert rr300.png_bytes[:4] == b"\x89PNG"
    # higher DPI => more pixels
    assert rr300.width_px > rr72.width_px
    assert rr300.dpi == pytest.approx(300, rel=0.01)


def test_render_respects_max_long_side_cap(tmp_path):
    import pdf_tools as PT
    pdf = tmp_path / "x.pdf"
    pdf.write_bytes(_tiny_pdf_bytes(width=2000, height=100))
    block = {"bbox": None, "bbox_norm": [0.0, 0.0, 1.0, 1.0],
             "page_width": None, "page_height": None}
    rr = PT.render_block_at_dpi(str(pdf), 1, block, 900, max_long_side_px=1500)
    assert rr.width_px <= 1500
    assert "dpi_capped" in rr.note


def test_production_clamp_baseline_caps_scale(tmp_path):
    """current_image_crop baseline must reproduce the production 6x clamp."""
    import pdf_tools as PT
    pdf = tmp_path / "x.pdf"
    pdf.write_bytes(_tiny_pdf_bytes(width=50, height=50))  # tiny -> needs >6x
    block = {"bbox": None, "bbox_norm": [0.0, 0.0, 1.0, 1.0],
             "page_width": None, "page_height": None}
    rr = PT.render_block_resized_long_side(str(pdf), 1, block, 4000)
    # 4000/50pt would be 80x; clamped to 6x => 72*6 dpi
    assert rr.dpi <= 6.0 * 72 + 1
    assert rr.note == "scale_clamped_6x"


def test_extract_block_pdf_bytes_is_valid_pdf(tmp_path):
    import pdf_tools as PT
    pdf = tmp_path / "x.pdf"
    pdf.write_bytes(_tiny_pdf_bytes())
    block = _block_for(str(pdf))
    out = PT.extract_block_pdf_bytes(str(pdf), 1, block)
    assert out[:4] == b"%PDF"
    d = fitz.open(stream=out, filetype="pdf")
    assert d.page_count == 1
    d.close()


def test_tile_block_respects_max_tiles(tmp_path):
    import pdf_tools as PT
    pdf = tmp_path / "x.pdf"
    pdf.write_bytes(_tiny_pdf_bytes(width=3000, height=1200))
    block = {"bbox": None, "bbox_norm": [0.0, 0.0, 1.0, 1.0],
             "page_width": None, "page_height": None}
    tiles = PT.tile_block(str(pdf), 1, block, 300, max_tile_long_side_px=400, max_tiles=8)
    assert 1 <= len(tiles) <= 8
    assert all(t.png_bytes[:4] == b"\x89PNG" for t in tiles)


def test_missing_pdf_raises(tmp_path):
    import pdf_tools as PT
    with pytest.raises(Exception):
        PT.render_block_at_dpi(str(tmp_path / "nope.pdf"), 1, _block_for("x"), 300)


# ── result normalizer ────────────────────────────────────────────────────────

def test_normalize_done_counts_facts_and_evidence():
    raw = json.dumps({
        "block_type": "scheme",
        "labels": [{"raw_text": "ЩР-1а", "evidence_snippet": "ЩР-1а"},
                   {"raw_text": "ВРУ-2", "evidence_snippet": "ВРУ-2"}],
        "numeric_parameters": [{"name": "ток", "value": "1000А", "evidence_snippet": "1000А"}],
        "visible_text": [{"text": "ЩР-1а"}],
        "confidence": 0.8, "usable_for_diff": True, "warnings": [],
    })
    n = RN.normalize(block_id="b1", method="pdf_render_600dpi", prompt_variant="scheme_mode",
                     provider="local", model="qwen", parameters={}, status="done",
                     latency_sec=12.3, input_size_bytes=1000, raw_text=raw)
    assert n["status"] == "done"
    assert n["json_valid"] is True
    assert n["fact_counts"]["labels"] == 2
    assert n["fact_counts"]["numeric_parameters"] == 1
    assert n["total_facts"] >= 3
    assert n["evidence_coverage"] > 0.9
    assert n["usable_for_diff"] is True


def test_normalize_error_status():
    n = RN.normalize(block_id="b1", method="pdf_url", prompt_variant="scheme_mode",
                     provider="local", model="qwen", parameters={}, status="unsupported",
                     latency_sec=0.5, input_size_bytes=0, error="http_400")
    assert n["status"] == "unsupported"
    assert n["total_facts"] == 0
    assert n["error"] == "http_400"


def test_normalize_salvages_truncated_json():
    # truncated (no closing brace) but valid prefix
    raw = '{"labels":[{"raw_text":"ЩР-1а","evidence_snippet":"ЩР-1а"}],"numeric_parameters":[{"value":"1000А"'
    n = RN.normalize(block_id="b1", method="tiled", prompt_variant="scheme_mode",
                     provider="local", model="qwen", parameters={}, status="done",
                     latency_sec=1.0, input_size_bytes=10, raw_text=raw, finish_reason="length")
    assert n["json_valid"] is False
    # salvage should still recover at least the labels
    assert n["fact_counts"]["labels"] >= 1
    assert "truncated_output" in n["warnings"] or n["salvaged"]


def test_normalize_maps_v5_diff_anchors():
    raw = json.dumps({
        "diff_anchors": {
            "labels": [{"raw_text": "ЩА-1.1"}],
            "ratings": [{"raw_text": "160А", "value_type": "current_rating"}],
            "connections": [{"from_raw": "ВРУ-2", "to_raw": "ЩА-1.1", "relation": "питает"}],
        },
        "confidence": 0.6,
    })
    n = RN.normalize(block_id="b1", method="x", prompt_variant="scheme_mode",
                     provider="local", model="qwen", parameters={}, status="done",
                     latency_sec=1.0, input_size_bytes=10, raw_text=raw)
    assert n["fact_counts"]["labels"] == 1
    assert n["fact_counts"]["numeric_parameters"] == 1
    assert n["fact_counts"]["connections"] == 1


# ── quality scorer ────────────────────────────────────────────────────────────

def _norm_with(facts, **kw):
    base = dict(block_id="b", method=kw.get("method", "m"), prompt_variant="p",
                provider="local", model="qwen", parameters={}, status="done",
                latency_sec=kw.get("latency", 10.0), input_size_bytes=kw.get("size", 1000),
                json_valid=True, salvaged=False, finish_reason=None, usage={},
                facts=facts, fact_counts={k: len(v) for k, v in facts.items()},
                total_facts=sum(len(v) for v in facts.values()),
                evidence_coverage=kw.get("ev", 1.0), confidence=kw.get("conf", 0.7),
                usable_for_diff=True, warnings=kw.get("warnings", []), error=None)
    return base


def test_scorer_prefers_rich_evidence_over_empty():
    rich = _norm_with({"labels": [{"raw_text": "ЩР-1а", "evidence_snippet": "ЩР-1а"}],
                       "materials": [], "numeric_parameters": [{"value": "1000А", "evidence_snippet": "1000А"}],
                       "visible_text": [], "elevations": [], "dimensions": [],
                       "equipment": [], "connections": [], "tables": []})
    empty = _norm_with({k: [] for k in RN.EMPTY_FACTS})
    assert QS.score_result(rich)["score"] > QS.score_result(empty)["score"]
    assert QS.score_result(empty)["score"] == 0  # no facts -> heavy penalty floors to 0


def test_scorer_penalizes_artificial_series():
    series = {"labels": [{"raw_text": f"ЩА-1.{i}"} for i in range(1, 12)],
              "materials": [], "numeric_parameters": [], "visible_text": [],
              "elevations": [], "dimensions": [], "equipment": [], "connections": [], "tables": []}
    n = _norm_with(series, ev=0.0)
    s = QS.score_result(n)
    assert s["penalties"].get("artificial_series")
    assert s["hallucination_risk"] is True


def test_pick_best_selects_highest_score():
    a = _norm_with({"labels": [{"raw_text": "X", "evidence_snippet": "X"}],
                    "materials": [], "numeric_parameters": [], "visible_text": [],
                    "elevations": [], "dimensions": [], "equipment": [], "connections": [], "tables": []},
                   method="current_image_crop")
    b = _norm_with({"labels": [{"raw_text": "X", "evidence_snippet": "X"},
                               {"raw_text": "Y", "evidence_snippet": "Y"}],
                    "materials": [{"name": "B25", "evidence_snippet": "B25"}],
                    "numeric_parameters": [{"value": "1000А", "evidence_snippet": "1000А"}],
                    "visible_text": [], "elevations": [], "dimensions": [],
                    "equipment": [], "connections": [], "tables": []},
                   method="pdf_render_600dpi")
    best = QS.pick_best([a, b])
    assert best is not None
    assert best["result"]["method"] == "pdf_render_600dpi"


def test_pick_best_returns_none_when_all_zero():
    z1 = _norm_with({k: [] for k in RN.EMPTY_FACTS})
    z1["status"] = "error"
    assert QS.pick_best([z1]) is None


# ── prompt variants ───────────────────────────────────────────────────────────

def test_prompt_variants_all_present_and_json_only():
    for name in ("general_engineering_facts", "ocr_strict", "stamp_mode",
                 "table_mode", "scheme_mode", "material_numeric_mode"):
        p = PV.get_single_pass(name)
        assert isinstance(p, str) and len(p) > 50
        assert "JSON" in p
    # two-pass second stage embeds the OCR text
    fp = PV.two_pass_facts("ЩР-1а\n1000А")
    assert "ЩР-1а" in fp and "1000А" in fp

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
    # B1 wiring: ядро ГРЩ передаётся ШТАТНО в payload (без отдельного rebuild-скрипта)
    dp = out["desc_payload"]
    assert isinstance(dp.get("core_systems"), dict) and isinstance(dp["core_systems"].get("categories"), dict)
    assert isinstance(dp.get("core_diagnostics"), dict)
    assert dp.get("source_side") == "left"
    assert "text_layer_stats" in dp and "block_source" in dp and "field_state" in dp


def test_grsh_feeder_helper_failsoft_without_block_pdf(tmp_path, monkeypatch):
    """Fail-soft: no crop_url/image_file/PDF → helper returns None → caller keeps single-shot."""
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "cmp"))
    from backend.app.services.stage_comparison import md_image_enrichment as mie
    from backend.app.services.stage_comparison import graphic_llm_local as g
    cfg = g.load_local_graphic_llm_config()
    out = asyncio.run(mie._run_grsh_feeder_extraction_for_block(
        "sid", "pid", "left", {"id": "B", "raw": {}}, cfg=cfg))
    assert out is None


# ─── GRSH_CORE_SYSTEMS (B1) + cross-side guard (B2) — no Qwen/Opus, no network ──

from backend.app.services.stage_comparison import grsh_core_systems as gcs  # noqa: E402

# Synthetic core sources covering the ядро-checklist (text_layer passed as a
# plain string → no PDF/fitz/network).
_CORE_STRUCTURED = {
    "profile": "electrical_singleline", "subtype": "grsh",
    "compensation": [{"ref": "АУКРМ-1", "detail": "180 кВАр", "field_state": "present"},
                     {"ref": "АУКРМ-2", "detail": "150 кВАр", "field_state": "present"}],
    "earthing": [{"name": "ГЗШ", "detail": "Главная заземляющая шина", "field_state": "present"}],
    "metering": [{"consumer": "ВРУ-ИТП", "ct_ratio": "40/5", "field_state": "present"}],
}
_CORE_CONNECTIONS = [
    {"from": "ТП1", "to": "ГРЩ1", "via": "Шинопровод 3L/PEN Al 3200A, L=6м",
     "evidence_text": "Ввод 1 к ТП1 Шинопровод 3L/PEN Al 3200A", "confidence": 0.95},
]
_CORE_TEXT_LAYER = "\n".join([
    "QF1 3Р", "3200А", "50кА", "QF2 3Р", "3200А", "50кА",
    "АВР", "ГРЩ1 ПСВ",
    "УЗИП1", "УЗИП2", "ОПН Тип 1", "FU1..FU3 125А",
    "1ТА1...1ТА3", "3хТШП-0,66", "40/5, 0,5S", "1500/5",
    "10хМеркурий 234 ARTX2-03", "Анализатор качества ЭС", "к TS1", "к TS2",
    "Ввод 1 к ТП1 Шинопровод 3L/PEN Al 3200А, L=6м",
    "ГЗШ", "К металлоконструкциям", "Металлические трубы водопровода",
    "Стадия П", "Граница балансовой принадлежности",
])


def test_build_core_systems_all_categories_present():
    cs = gcs.build_core_systems(_CORE_STRUCTURED, _CORE_CONNECTIONS, _CORE_TEXT_LAYER,
                                source_side="right")
    cats = cs["categories"]
    # все 11 фиксированных категорий присутствуют как ключи
    assert set(cats.keys()) == set(gcs.CORE_CATEGORY_KEYS)
    diag = cs["diagnostics"]
    assert diag["source_side"] == "right"
    # ядро реально наполнено (не пусто)
    assert "main_breakers" in diag["categories_present"]
    assert "surge_protection" in diag["categories_present"]


def test_render_core_systems_md_covers_checklist():
    cs = gcs.build_core_systems(_CORE_STRUCTURED, _CORE_CONNECTIONS, _CORE_TEXT_LAYER,
                                source_side="right")
    md = gcs.render_core_systems_md({"source_side": cs["source_side"], "categories": cs["categories"]})
    assert "GRSH_CORE_SYSTEMS" in md
    # 1 QF 3200/50кА · 2 шинопровод · 3 АВР/ПСВ · 4 УЗИП/ОПН · 5 ТТ/ТШП · 6 учёт · 7 ГЗШ
    assert "3200" in md and "50кА" in md
    assert "Шинопровод" in md
    assert "АВР" in md and "ПСВ" in md
    assert "УЗИП" in md and "ОПН" in md
    assert "ТШП" in md
    assert "Меркурий" in md and "TS1" in md
    assert "ГЗШ" in md
    assert "АУКРМ" in md


def test_grsh_core_systems_renders_in_format_qwen_md():
    """Payload c core_systems → _format_qwen_description_md рендерит секцию."""
    from backend.app.services.stage_comparison import md_image_enrichment as mie
    cs = gcs.build_core_systems(_CORE_STRUCTURED, _CORE_CONNECTIONS, _CORE_TEXT_LAYER,
                                source_side="right")
    payload = {"status": "done", "image_kind": "scheme",
               "graphic_profile": "electrical_singleline",
               "core_systems": {"source_side": "right", "categories": cs["categories"]}}
    body = mie._format_qwen_description_md(payload, model="qwen", page=21, block_id="B")
    assert "GRSH_CORE_SYSTEMS" in body
    assert "3200" in body and "УЗИП" in body and "ТШП" in body and "Меркурий" in body


def test_grsh_core_systems_in_enriched_md_and_diff_index():
    """build_enriched_md: GRSH_CORE_SYSTEMS в теле + core anchors в IMAGE_DIFF_INDEX."""
    from backend.app.services.stage_comparison import md_image_enrichment as mie
    cs = gcs.build_core_systems(_CORE_STRUCTURED, _CORE_CONNECTIONS, _CORE_TEXT_LAYER,
                                source_side="right")
    payload = {"status": "done", "image_kind": "scheme",
               "graphic_profile": "electrical_singleline",
               "grsh_feeder_table": "GRSH_FEEDERS\n- потребитель=ВРУ1",
               "core_systems": {"source_side": "right", "categories": cs["categories"]}}
    blocks = [mie.MdBlock(kind="image", text="[IMAGE]", page=21, block_id="B",
                          order=0, image_order_on_page=0)]
    descriptions = [{"order": 0, "status": "done", "block_type": "dense_grsh_singleline",
                     "page": 21, "side_block_id": "B", "description": payload}]
    md = mie.build_enriched_md(blocks, descriptions)
    assert "GRSH_CORE_SYSTEMS" in md
    assert "GRSH_FEEDERS" in md
    # IMAGE_DIFF_INDEX core anchors
    assert "core:" in md
    assert "АВР/ПСВ" in md and "УЗИП/ОПН" in md and "ГЗШ/ДСУП" in md


def test_core_not_extracted_is_not_removed():
    """Пустые источники → каждая категория not_extracted, НЕ removed/added."""
    cs = gcs.build_core_systems({}, [], "", source_side="left")
    md = gcs.render_core_systems_md({"source_side": "left", "categories": cs["categories"]})
    assert "not_extracted" in md
    # ни один элемент не классифицирован как removed/added (формат "value | state | …").
    # (слово "removed" в шапке секции — это пояснение «НЕ трактовать как removed».)
    assert " | removed" not in md and " | added" not in md
    # все категории помечены not_extracted в диагностике
    assert set(cs["diagnostics"]["categories_not_extracted"]) == set(gcs.CORE_CATEGORY_KEYS)


def test_b2_extraction_guard_flags_ocr_only_categories():
    """Категория есть только в text-layer (Qwen structured пусто) → requires_human_review."""
    # surge_protection нет ни в structured, ни в connections — только в text-layer
    cs = gcs.build_core_systems({}, [], "УЗИП1\nОПН Тип 1\nFU1..FU3 125А", source_side="left")
    diag = cs["diagnostics"]
    assert "surge_protection" in diag["ocr_only_categories"]
    assert "surge_protection" in diag["requires_human_review_categories"]
    # и при этом surge_protection присутствует (не removed)
    assert "surge_protection" in diag["categories_present"]


def test_b2_cross_side_guard_aukrm_not_false_added():
    """AUKRM помечен added, но есть в text-layer старой стороны → requires_human_review."""
    changes = [{"type": "added", "title": "Добавлены установки компенсации АУКРМ-1 и АУКРМ-2",
                "new_value": "АУКРМ-1, АУКРМ-2",
                "evidence_right": {"quote": "АУКРМ-1 180 кВАр; АУКРМ-2 150 кВАр"}}]
    left_tl = "ГРЩ1-КУ1 ППГнг(А)-HF 5х150\nАУКРМ-1\nQр=200 кВАр"  # АУКРМ есть в OLD text-layer
    right_tl = "АУКРМ №1\nАУКРМ №2\n180 кВАр\n150 кВАр"
    out, stats = gcs.apply_cross_side_guard(changes, left_tl, right_tl)
    assert stats["guarded"] == 1
    assert out[0].get("requires_human_review") is True
    assert out[0]["cross_side_guard"]["original_type"] == "added"
    assert out[0]["cross_side_guard"]["present_in_text_layer_side"] == "left"


def test_b2_cross_side_guard_genuine_add_not_flagged():
    """Реально новый элемент (нет в other-side text-layer) НЕ помечается guard'ом."""
    changes = [{"type": "added", "title": "Добавлены УЗИП1, УЗИП2",
                "new_value": "УЗИП1 УЗИП2",
                "evidence_right": {"quote": "УЗИП1 УЗИП2"}}]
    left_tl = "ОПН Тип 1 FU1 125А"   # УЗИП НЕТ в старой стадии → честный added
    right_tl = "УЗИП1 УЗИП2 ОПН"
    out, stats = gcs.apply_cross_side_guard(changes, left_tl, right_tl)
    assert stats["guarded"] == 0
    assert "cross_side_guard" not in out[0]


# ─── live-defaults после benchmark 2026-06-05 ──────────────────────────────


def test_default_tile_long_side_is_1600(monkeypatch):
    """Task 4.7: default tile_long_side = 1600 (2000 — только override/debug)."""
    monkeypatch.delenv("STAGE_COMPARISON_GRSH_FEEDER_TILE_LONG_SIDE", raising=False)
    assert gfe.GrshFeederConfig().tile_long_side == 1600
    assert gfe.load_grsh_feeder_config().tile_long_side == 1600


def test_tile_long_side_2000_override_still_works(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_GRSH_FEEDER_TILE_LONG_SIDE", "2000")
    assert gfe.load_grsh_feeder_config().tile_long_side == 2000


def test_default_concurrency_is_one(monkeypatch):
    """Task 4.8: GRSH tile concurrency = 1 по умолчанию и не опускается ниже 1."""
    monkeypatch.delenv("STAGE_COMPARISON_GRSH_FEEDER_TILE_CONCURRENCY", raising=False)
    assert gfe.GrshFeederConfig().concurrency == 1
    assert gfe.load_grsh_feeder_config().concurrency == 1
    monkeypatch.setenv("STAGE_COMPARISON_GRSH_FEEDER_TILE_CONCURRENCY", "0")
    assert gfe.load_grsh_feeder_config().concurrency == 1

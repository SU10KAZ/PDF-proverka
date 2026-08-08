# -*- coding: utf-8 -*-
"""Тесты pre-Qwen block equivalence gate (Stage 1: observe).

Покрывает:
  * extract_blocks_for_equivalence (формат pages/blocks);
  * bbox_iou_norm + pair_blocks_by_iou (one-to-one, added/deleted);
  * detect_split_merge_candidates (один↔много → uncertain, не skip);
  * canonicalize_text + compare_text_blocks (equal / changed);
  * compare_visual_blocks (identical / shifted-but-identical / changed) — cv2;
  * load_or_render failure → uncertain/qwen_required;
  * observe mode НИКОГДА не ставит qwen_skip_candidate на сомнении и
    SKIP_QWEN форсится в False при mode=observe.
"""
from __future__ import annotations

import numpy as np
import pytest

from backend.app.services.stage_comparison import block_equivalence_precheck as be

cv2 = pytest.importorskip("cv2")  # визуальные тесты требуют opencv


# ─── helpers ────────────────────────────────────────────────────────────────


def _result(blocks_by_page: dict[int, list[dict]], w: int = 1000, h: int = 1000) -> dict:
    pages = []
    for pn in sorted(blocks_by_page):
        pages.append({"page_number": pn, "width": w, "height": h,
                      "blocks": blocks_by_page[pn]})
    return {"pages": pages}


def _blk(bid: str, btype: str, norm: list[float], text: str = "") -> dict:
    return {"id": bid, "block_type": btype, "coords_norm": norm, "ocr_text": text}


def _drawing(w: int = 300, h: int = 200) -> np.ndarray:
    """Непериодичный «чертёж» с уникальными деталями — у ECC однозначное
    выравнивание (без аляйзинга от регулярной сетки)."""
    img = np.full((h, w, 3), 255, np.uint8)
    cv2.rectangle(img, (15, 15), (w - 15, h - 15), (0, 0, 0), 2)
    cv2.rectangle(img, (30, 30), (90, 70), (0, 0, 0), -1)        # filled block top-left
    cv2.circle(img, (w - 50, h - 45), 22, (0, 0, 0), 2)          # circle bottom-right
    cv2.line(img, (30, h - 30), (w - 40, 40), (0, 0, 0), 1)      # diagonal
    cv2.putText(img, "GRSH-1", (110, h // 2), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 0, 0), 2)
    cv2.putText(img, "ABC", (40, h - 35), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 0), 1)
    return img


# ═══════════════════════════════════════════════════════════════════════════
# extract / IoU / pairing
# ═══════════════════════════════════════════════════════════════════════════


def test_extract_blocks_format_pages():
    r = _result({1: [_blk("A", "text", [0, 0, 0.5, 0.5], "hi"),
                     _blk("B", "image", [0.5, 0.5, 1, 1])]})
    blocks = be.extract_blocks_for_equivalence(r)
    assert [b.block_id for b in blocks] == ["A", "B"]
    assert blocks[0].is_text_like and blocks[1].is_image_like
    assert blocks[0].page == 1
    assert blocks[0].coords_norm == [0, 0, 0.5, 0.5]


def test_extract_blocks_format_flat():
    r = {"blocks": [{"id": "X", "block_type": "image", "page_index": 3,
                     "coords_norm": [0, 0, 1, 1], "page_width": 100, "page_height": 100}]}
    blocks = be.extract_blocks_for_equivalence(r)
    assert len(blocks) == 1 and blocks[0].page == 3 and blocks[0].block_id == "X"


def test_bbox_iou_norm():
    assert be.bbox_iou_norm([0, 0, 1, 1], [0, 0, 1, 1]) == pytest.approx(1.0)
    assert be.bbox_iou_norm([0, 0, 0.5, 1], [0.5, 0, 1, 1]) == 0.0
    iou = be.bbox_iou_norm([0, 0, 1, 1], [0, 0, 0.5, 0.5])
    assert iou == pytest.approx(0.25)
    assert be.bbox_iou_norm(None, [0, 0, 1, 1]) == 0.0


def test_pair_one_to_one():
    old = be.extract_blocks_for_equivalence(_result({1: [_blk("A", "image", [0.1, 0.1, 0.4, 0.4])]}))
    new = be.extract_blocks_for_equivalence(_result({1: [_blk("A2", "image", [0.11, 0.11, 0.41, 0.41])]}))
    res = be.pair_blocks_by_iou(old, new, iou_threshold=0.5)
    assert len(res.paired) == 1
    assert res.paired[0]["old_id"] == "A" and res.paired[0]["new_id"] == "A2"
    assert not res.added and not res.deleted and not res.split_merge


def test_pair_unmatched_added_deleted():
    old = be.extract_blocks_for_equivalence(_result({1: [
        _blk("A", "image", [0.0, 0.0, 0.3, 0.3]),
        _blk("D", "image", [0.6, 0.6, 0.9, 0.9])]}))  # D only in old
    new = be.extract_blocks_for_equivalence(_result({1: [
        _blk("A2", "image", [0.0, 0.0, 0.3, 0.3]),
        _blk("N", "image", [0.6, 0.0, 0.9, 0.3])]}))  # N only in new
    res = be.pair_blocks_by_iou(old, new, iou_threshold=0.5)
    assert [p["old_id"] for p in res.paired] == ["A"]
    assert res.deleted == ["D"]
    assert res.added == ["N"]


def test_split_merge_produces_uncertain_not_skip():
    # один OLD-блок перекрывает ДВА NEW-блока → split
    old = be.extract_blocks_for_equivalence(_result({1: [_blk("BIG", "image", [0.0, 0.0, 1.0, 0.5])]}))
    new = be.extract_blocks_for_equivalence(_result({1: [
        _blk("L", "image", [0.0, 0.0, 0.5, 0.5]),
        _blk("R", "image", [0.5, 0.0, 1.0, 0.5])]}))
    res = be.pair_blocks_by_iou(old, new, iou_threshold=0.5, overlap_threshold=0.2)
    assert len(res.split_merge) == 1
    g = res.split_merge[0]
    assert g["old_ids"] == ["BIG"] and sorted(g["new_ids"]) == ["L", "R"]
    assert g["reason"] == "split"
    # split/merge → qwen_required, никогда не skip
    d = be.decide_block_pair(None, None, kind="split_merge")
    assert d["decision"] == be.DECISION_SPLIT_MERGE
    assert d["qwen_action"] == be.QWEN_REQUIRED


def test_page_pairs_shifted_pages():
    # OLD page 5 ↔ NEW page 2 (лист уехал) — pairing должен матчить через карту
    old = be.extract_blocks_for_equivalence(_result({5: [_blk("A", "image", [0.1, 0.1, 0.4, 0.4])]}))
    new = be.extract_blocks_for_equivalence(_result({2: [_blk("A2", "image", [0.1, 0.1, 0.4, 0.4])]}))
    # без карты (identity) — не матчится
    res0 = be.pair_blocks_by_iou(old, new, iou_threshold=0.5)
    assert not res0.paired
    # с картой страниц — матчится
    res = be.pair_blocks_by_iou(old, new, iou_threshold=0.5, page_pairs=[(5, 2)])
    assert len(res.paired) == 1 and res.paired[0]["old_page"] == 5 and res.paired[0]["new_page"] == 2


def test_one_sided_old_page_blocks_become_deleted():
    # page_pairs покрывает только (3,3); страница 7 есть только в OLD → её блоки
    # должны попасть в deleted (а не выпасть из отчёта).
    old = be.extract_blocks_for_equivalence(_result({
        3: [_blk("O1", "image", [0.1, 0.1, 0.4, 0.4])],
        7: [_blk("O2", "image", [0.0, 0.0, 0.5, 0.5]),
            _blk("O3", "text", [0.5, 0.5, 1.0, 1.0], "x")]}))
    new = be.extract_blocks_for_equivalence(_result({
        3: [_blk("N1", "image", [0.1, 0.1, 0.4, 0.4])]}))
    res = be.pair_blocks_by_iou(old, new, iou_threshold=0.5, page_pairs=[(3, 3)])
    assert [p["old_id"] for p in res.paired] == ["O1"]
    assert sorted(res.deleted) == ["O2", "O3"]   # one-sided OLD page 7 → deleted
    assert res.added == []


def test_one_sided_new_page_blocks_become_added():
    # page_pairs покрывает только (3,3); страница 9 есть только в NEW → added.
    old = be.extract_blocks_for_equivalence(_result({
        3: [_blk("O1", "image", [0.1, 0.1, 0.4, 0.4])]}))
    new = be.extract_blocks_for_equivalence(_result({
        3: [_blk("N1", "image", [0.1, 0.1, 0.4, 0.4])],
        9: [_blk("N2", "image", [0.0, 0.0, 0.5, 0.5]),
            _blk("N3", "image", [0.6, 0.6, 0.9, 0.9])]}))
    res = be.pair_blocks_by_iou(old, new, iou_threshold=0.5, page_pairs=[(3, 3)])
    assert [p["new_id"] for p in res.paired] == ["N1"]
    assert sorted(res.added) == ["N2", "N3"]     # one-sided NEW page 9 → added
    assert res.deleted == []


def test_report_one_sided_pages_full_coverage_qwen_required():
    # Разреженный page_alignment: только (3,3) двусторонняя; стр.7 — OLD-only,
    # стр.9 — NEW-only. Все блоки должны быть учтены (полный охват) и все →
    # qwen_required; potential_qwen_saved=0 (идентичных нет, pdf не задан).
    old = _result({
        3: [_blk("O1", "image", [0.1, 0.1, 0.4, 0.4])],
        7: [_blk("O2", "image", [0.0, 0.0, 0.5, 0.5]),
            _blk("O3", "text", [0.5, 0.5, 1.0, 1.0], "удалённый лист")]})
    new = _result({
        3: [_blk("N1", "image", [0.1, 0.1, 0.4, 0.4])],
        9: [_blk("N2", "image", [0.0, 0.0, 0.5, 0.5])]})
    rep = be.build_block_equivalence_report(
        old, new, cfg=be.BlockEquivalenceConfig(enabled=True), page_pairs=[(3, 3)])
    s = rep["summary"]
    assert s["total_old_blocks"] == 3 and s["total_new_blocks"] == 2
    assert s["deleted_candidates"] == 2   # O2, O3 (one-sided OLD page 7)
    assert s["added_candidates"] == 1     # N2 (one-sided NEW page 9)
    assert s["paired"] == 1               # O1↔N1
    # полный охват: paired + added + deleted == все блоки (с учётом двойного
    # счёта paired = 1 пара из 1 old + 1 new)
    covered_old = s["paired"] + s["deleted_candidates"]
    covered_new = s["paired"] + s["added_candidates"]
    assert covered_old == s["total_old_blocks"]
    assert covered_new == s["total_new_blocks"]
    # observe never skips: всё qwen_required, ничего не сохранено
    assert s["potential_qwen_saved"] == 0
    for rec in rep["pairs"] + rep["added"] + rep["deleted"]:
        assert rec["qwen_action"] == be.QWEN_REQUIRED


def test_two_sided_pages_unchanged_with_one_sided_fix():
    # Регрессия: двусторонние страницы продолжают работать как раньше —
    # leftover на ОБЩЕЙ странице тоже корректно идёт в added/deleted.
    old = be.extract_blocks_for_equivalence(_result({1: [
        _blk("A", "image", [0.0, 0.0, 0.3, 0.3]),
        _blk("D", "image", [0.6, 0.6, 0.9, 0.9])]}))
    new = be.extract_blocks_for_equivalence(_result({1: [
        _blk("A2", "image", [0.0, 0.0, 0.3, 0.3]),
        _blk("N", "image", [0.6, 0.0, 0.9, 0.3])]}))
    res = be.pair_blocks_by_iou(old, new, iou_threshold=0.5, page_pairs=[(1, 1)])
    assert [p["old_id"] for p in res.paired] == ["A"]
    assert res.deleted == ["D"] and res.added == ["N"]


# ═══════════════════════════════════════════════════════════════════════════
# text compare
# ═══════════════════════════════════════════════════════════════════════════


def test_canonicalize_text_whitespace_linebreaks():
    a = be.canonicalize_text("  Hello   world  \r\n\r\n")
    b = be.canonicalize_text("Hello world")
    assert a == b == "Hello world"


def test_compare_text_equal_and_changed():
    eq = be.compare_text_blocks(
        be.EqBlock("o", 1, "text", text="Кабель ВВГнг 5x10"),
        be.EqBlock("n", 1, "text", text="Кабель ВВГнг 5x10\n"))
    assert eq["text_equal"] is True
    ch = be.compare_text_blocks(
        be.EqBlock("o", 1, "text", text="Кабель ВВГнг 5x10"),
        be.EqBlock("n", 1, "text", text="Кабель ВВГнг 5x16"))
    assert ch["text_equal"] is False
    assert 0.0 < ch["similarity"] < 1.0


def test_compare_text_strips_html_aligns_with_text_block_equivalence():
    """reserc.md #60/#13: HTML-обёрнутый ocr_text не должен давать ложный
    'changed'; слой block_equivalence согласован с text_block_equivalence."""
    from backend.app.services.stage_comparison.text_block_equivalence import (
        normalize_block_text,
    )

    plain = "Кабель ВВГнг 5x10"
    wrapped = '<div data-bbox="1,2,3,4">Кабель ВВГнг 5x10</div>'
    eq = be.compare_text_blocks(
        be.EqBlock("o", 1, "text", text=plain),
        be.EqBlock("n", 1, "text", text=wrapped))
    assert eq["text_equal"] is True
    # оба слоя нормализуют один текст одинаково (дедуп нормализаторов)
    assert normalize_block_text(plain) == normalize_block_text(wrapped)
    # реальное изменение числа всё ещё ловится сквозь HTML-обёртку
    ch = be.compare_text_blocks(
        be.EqBlock("o", 1, "text", text=wrapped),
        be.EqBlock("n", 1, "text", text='<div>Кабель ВВГнг 5x16</div>'))
    assert ch["text_equal"] is False


def test_decide_text_pair_identical_is_skip_candidate():
    ob = be.EqBlock("o", 1, "text", text="A B C")
    nb = be.EqBlock("n", 1, "text", text="A B C")
    tc = be.compare_text_blocks(ob, nb)
    d = be.decide_block_pair(ob, nb, text_cmp=tc, kind="paired")
    assert d["decision"] == be.DECISION_IDENTICAL_TEXT
    assert d["qwen_action"] == be.QWEN_SKIP_CANDIDATE


def test_decide_text_pair_changed_is_qwen_required():
    ob = be.EqBlock("o", 1, "text", text="A B C")
    nb = be.EqBlock("n", 1, "text", text="A B D")
    tc = be.compare_text_blocks(ob, nb)
    d = be.decide_block_pair(ob, nb, text_cmp=tc, kind="paired")
    assert d["decision"] == be.DECISION_CHANGED_TEXT
    assert d["qwen_action"] == be.QWEN_REQUIRED


# ═══════════════════════════════════════════════════════════════════════════
# visual compare (cv2)
# ═══════════════════════════════════════════════════════════════════════════


def test_visual_identical_crops():
    img = _drawing()
    r = be.compare_visual_blocks(img, img.copy())
    assert r["status"] == be.DECISION_IDENTICAL_VISUAL
    assert r["total_diff_ratio"] == 0.0


def test_visual_shifted_crops_aligns_identical():
    img = _drawing()
    M = np.float32([[1, 0, 3], [0, 1, 2]])
    shifted = cv2.warpAffine(img, M, (img.shape[1], img.shape[0]), borderValue=(255, 255, 255))
    r = be.compare_visual_blocks(img, shifted)
    # ECC должен выровнять сдвиг → identical
    assert r["status"] == be.DECISION_IDENTICAL_VISUAL
    assert r["alignment_score"] is not None and r["alignment_score"] > 0.9


def test_visual_changed_crops():
    img = _drawing()
    chg = img.copy()
    cv2.rectangle(chg, (200, 120), (260, 160), (0, 0, 255), -1)
    cv2.putText(chg, "NEW", (60, 160), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 255), 2)
    r = be.compare_visual_blocks(img, chg)
    assert r["status"] == be.DECISION_CHANGED_VISUAL
    assert r["total_diff_ratio"] > 0.0
    assert r["diff_bbox"] is not None


def test_visual_resized_identical_robust_to_dpi():
    # тот же чертёж, отрендеренный «крупнее» (другой DPI) → не должен быть changed
    img = _drawing()
    big = cv2.resize(img, (img.shape[1] * 2, img.shape[0] * 2))
    r = be.compare_visual_blocks(img, big)
    assert r["status"] == be.DECISION_IDENTICAL_VISUAL


def test_visual_debug_png_written(tmp_path):
    img = _drawing()
    chg = img.copy()
    cv2.rectangle(chg, (200, 120), (260, 160), (0, 0, 255), -1)
    out = tmp_path / "blk_diff.png"
    r = be.compare_visual_blocks(img, chg, debug_path=out)
    assert r["status"] == be.DECISION_CHANGED_VISUAL
    assert out.exists() and out.stat().st_size > 0


# ═══════════════════════════════════════════════════════════════════════════
# render / image-load failure → uncertain (qwen_required)
# ═══════════════════════════════════════════════════════════════════════════


def test_render_failure_no_source_pdf_is_uncertain():
    blk = be.EqBlock("X", 1, "image", coords_norm=[0, 0, 1, 1])
    img, meta = be.load_or_render_block_image(blk, source_pdf_path=None, render_long_side=500)
    assert img is None
    assert meta["status"] == "render_failed"
    # визуальное сравнение с None → render_failed → qwen_required
    vc = be.compare_visual_blocks(None, None)
    d = be._decide_by_visual(vc, {"has_text_old": False, "has_text_new": False})
    assert d["qwen_action"] == be.QWEN_REQUIRED
    assert d["decision"] in (be.DECISION_RENDER_FAILED, be.DECISION_UNCERTAIN)


def test_render_failure_missing_pdf_path():
    blk = be.EqBlock("X", 1, "image", coords_norm=[0, 0, 1, 1])
    img, meta = be.load_or_render_block_image(blk, source_pdf_path="/no/such/file.pdf")
    assert img is None and meta["status"] == "render_failed"


def test_image_block_render_failed_falls_back_to_ocr_text():
    # image-блок: визуал недоступен, но OCR-текст различается → changed_text
    vc = {"status": be.DECISION_RENDER_FAILED}
    tc = {"has_text_old": True, "has_text_new": True, "text_equal": False, "similarity": 0.5}
    d = be._decide_by_visual(vc, tc)
    assert d["decision"] == be.DECISION_CHANGED_TEXT
    assert d["qwen_action"] == be.QWEN_REQUIRED


# ═══════════════════════════════════════════════════════════════════════════
# build_block_equivalence_report + observe-never-skip
# ═══════════════════════════════════════════════════════════════════════════


def test_report_added_deleted_split_all_require_qwen():
    old = _result({1: [
        _blk("A", "image", [0.0, 0.0, 0.3, 0.3]),       # paired (no pdf → uncertain)
        _blk("DEL", "image", [0.6, 0.6, 0.9, 0.9]),     # deleted
        _blk("BIG", "image", [0.0, 0.4, 1.0, 0.7])]})    # split parent
    new = _result({1: [
        _blk("A2", "image", [0.0, 0.0, 0.3, 0.3]),
        _blk("ADD", "image", [0.6, 0.0, 0.9, 0.3]),     # added
        _blk("S1", "image", [0.0, 0.4, 0.5, 0.7]),       # split child
        _blk("S2", "image", [0.5, 0.4, 1.0, 0.7])]})     # split child
    rep = be.build_block_equivalence_report(old, new, cfg=be.BlockEquivalenceConfig())
    s = rep["summary"]
    assert s["added_candidates"] == 1
    assert s["deleted_candidates"] == 1
    assert s["split_merge"] == 1
    # Без pdf-путей визуал не выполняется → paired image-block остаётся
    # uncertain/qwen_required, не skip.
    assert s["potential_qwen_saved"] == 0
    for rec in rep["pairs"] + rep["added"] + rep["deleted"] + rep["split_merge"]:
        assert rec["qwen_action"] == be.QWEN_REQUIRED


def test_observe_mode_forces_skip_qwen_false(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_BLOCK_EQUIVALENCE_PRECHECK_ENABLED", "true")
    monkeypatch.setenv("STAGE_COMPARISON_BLOCK_EQUIVALENCE_PRECHECK_MODE", "observe")
    monkeypatch.setenv("STAGE_COMPARISON_BLOCK_EQUIVALENCE_SKIP_QWEN", "true")  # попытка включить skip
    cfg = be.BlockEquivalenceConfig.from_env()
    assert cfg.enabled is True
    assert cfg.mode == "observe"
    # Stage 1 hard guard: skip принудительно False в observe-режиме
    assert cfg.skip_qwen is False


def test_disabled_by_default():
    cfg = be.BlockEquivalenceConfig.from_env()
    assert cfg.enabled is False
    assert cfg.skip_qwen is False


def test_page_pairs_from_alignment_nested_and_flat(monkeypatch):
    from backend.app.services.stage_comparison import store as store_mod
    # store.get_alignment вкладывает items в ключ "alignment"
    monkeypatch.setattr(store_mod, "get_alignment", lambda s, p: {
        "alignment": {"items": [
            {"slot": 1, "left_page": 5, "right_page": 2},
            {"slot": 2, "left_page": 6, "right_page": None},  # пропускается
            {"slot": 3, "left_page": 7, "right_page": 4}]}})
    pp = be._page_pairs_from_alignment("s", "p")
    assert pp == [(5, 2), (7, 4)]
    # плоский вид
    monkeypatch.setattr(store_mod, "get_alignment", lambda s, p: {
        "items": [{"left_page": 1, "right_page": 1}]})
    assert be._page_pairs_from_alignment("s", "p") == [(1, 1)]
    # ошибка → None (identity fallback)
    monkeypatch.setattr(store_mod, "get_alignment", lambda s, p: (_ for _ in ()).throw(RuntimeError()))
    assert be._page_pairs_from_alignment("s", "p") is None


def test_text_pair_report_identical_is_skip_candidate():
    old = _result({1: [_blk("T", "text", [0.0, 0.0, 1.0, 1.0], "Общие данные\nЛист 1")]})
    new = _result({1: [_blk("T2", "text", [0.0, 0.0, 1.0, 1.0], "Общие данные\nЛист 1")]})
    rep = be.build_block_equivalence_report(old, new, cfg=be.BlockEquivalenceConfig(enabled=True))
    s = rep["summary"]
    assert s["identical_text"] == 1
    assert s["potential_qwen_saved"] == 1
    assert rep["pairs"][0]["qwen_action"] == be.QWEN_SKIP_CANDIDATE

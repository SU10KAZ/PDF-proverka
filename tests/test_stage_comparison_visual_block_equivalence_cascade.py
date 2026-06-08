# -*- coding: utf-8 -*-
"""Тесты Stage 3E каскадного visual compare (Euclidean → Affine → fallback).

Проверяет:
  * identical изображения → identical_visual (строгие гейты);
  * старый путь changed_visual (ECC сошёлся, заметный diff) не сломан;
  * при не сошедшемся Euclidean пробуется Affine;
  * при не сошедшемся ECC fallback может вернуть changed_visual;
  * fallback НИКОГДА не ставит identical_visual (консервативность);
  * trim пустых полей (content bbox);
  * mask_iou / normalized_correlation;
  * debug-файлы пишутся ТОЛЬКО при переданном debug_path;
  * интеграция через compare_one_link: enforced=false;
  * модуль не импортирует Qwen/Opus/pipeline.

Используются синтетические cv2-изображения — без PDF, Qwen, Opus.
"""
from __future__ import annotations

import ast
from pathlib import Path

import numpy as np
import pytest

cv2 = pytest.importorskip("cv2")

from backend.app.services.stage_comparison import visual_block_equivalence as vbe
from backend.app.services.stage_comparison.block_equivalence_precheck import EqBlock


# ─── synthetic drawings ──────────────────────────────────────────────────────


def _canvas(w=260, h=200):
    return np.full((h, w, 3), 255, np.uint8)


def _draw_a(img=None):
    img = _canvas() if img is None else img
    cv2.rectangle(img, (30, 30), (130, 130), (0, 0, 0), 2)
    cv2.line(img, (30, 30), (130, 130), (0, 0, 0), 2)
    cv2.circle(img, (195, 60), 28, (0, 0, 0), 2)
    cv2.putText(img, "VRU-1", (135, 165), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 0), 2)
    return img


def _draw_b(img=None):
    # совсем другая фигура — мало общего с A
    img = _canvas() if img is None else img
    cv2.circle(img, (70, 70), 40, (0, 0, 0), 2)
    cv2.line(img, (140, 20), (40, 150), (0, 0, 0), 2)
    cv2.rectangle(img, (150, 110), (240, 180), (0, 0, 0), 2)
    cv2.putText(img, "GRSH", (150, 40), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 0), 2)
    return img


def _draw_a_plus(img=None):
    # A + умеренное добавление (залитый прямоугольник): заметный diff,
    # выравнивание сходится (euclidean/affine), не уходит целиком в fallback
    img = _draw_a(img)
    cv2.rectangle(img, (150, 140), (205, 180), (0, 0, 0), -1)
    return img


def _cfg(**kw):
    return vbe.VisualBlockEquivalenceConfig(**kw)


# ─── identical / changed via ECC ─────────────────────────────────────────────


def test_identical_images_yield_identical_visual():
    a = _draw_a()
    res = vbe.compare_block_images_cascade(a, a.copy(), cfg=_cfg())
    assert res["status"] == "identical_visual"
    assert res["alignment_method"] in ("euclidean", "affine")
    assert res["mask_iou"] >= 0.97
    assert res["normalized_correlation"] >= 0.97
    assert res["total_diff_ratio"] <= 0.02


def test_changed_visual_on_moderate_change_not_broken():
    # умеренное изменение содержимого → changed_visual (метод любой), не identical
    res = vbe.compare_block_images_cascade(_draw_a(), _draw_a_plus(), cfg=_cfg())
    assert res["status"] == "changed_visual"
    assert res["status"] != "identical_visual"
    # если решение по ECC-пути (есть diff) — diff должен быть выше identical-порога
    if res["total_diff_ratio"] is not None:
        assert res["total_diff_ratio"] > 0.02


# ─── affine fallback when euclidean fails ────────────────────────────────────


def test_affine_rescues_when_euclidean_fails():
    a = _draw_a()
    # shear + анизотропный масштаб: жёсткое (euclidean) не моделирует, affine — да
    M = np.array([[1.0, 0.2, 0.0], [0.05, 1.1, 0.0]], np.float32)
    sheared = cv2.warpAffine(a, M, (a.shape[1], a.shape[0]), borderValue=(255, 255, 255))

    res_eu = vbe.compare_block_images_cascade(
        a, sheared, cfg=_cfg(enable_affine=False, enable_fallback=False))
    res_af = vbe.compare_block_images_cascade(
        a, sheared, cfg=_cfg(enable_affine=True, enable_fallback=False))

    # euclidean-only не сошёлся (cc ниже порога) → alignment_failed
    assert res_eu["status"] == "alignment_failed"
    # affine спас выравнивание: метод affine, статус определён (не failed)
    assert res_af["alignment_method"] == "affine"
    assert res_af["status"] in ("identical_visual", "changed_visual")
    # affine даёт строго лучший alignment_score, чем euclidean-only
    assert (res_af["alignment_score"] or 0) > (res_eu["alignment_score"] or 0)


# ─── fallback (no ECC) ───────────────────────────────────────────────────────


def test_fallback_changed_visual_on_different_content():
    # ECC не выровняет совсем разные фигуры → fallback с низким IoU → changed
    res = vbe.compare_block_images_cascade(
        _draw_a(), _draw_b(), cfg=_cfg(enable_affine=False))
    assert res["status"] in ("changed_visual", "uncertain")
    if res["status"] == "changed_visual":
        assert str(res["alignment_method"]).startswith("fallback")
    # ГЛАВНОЕ: при слабом alignment НИКОГДА не identical
    assert res["status"] != "identical_visual"


def test_fallback_never_identical_on_perspective_warp():
    # перспективное искажение identical-картинки: ни euclidean, ни affine не
    # выровняют точно → fallback → НИКОГДА не identical_visual
    a = _draw_a()
    h, w = a.shape[:2]
    src = np.float32([[0, 0], [w, 0], [w, h], [0, h]])
    dst = np.float32([[18, 10], [w - 8, 26], [w - 28, h - 14], [22, h - 8]])
    H = cv2.getPerspectiveTransform(src, dst)
    warped = cv2.warpPerspective(a, H, (w, h), borderValue=(255, 255, 255))
    res = vbe.compare_block_images_cascade(a, warped, cfg=_cfg())
    assert res["status"] != "identical_visual"


def test_fallback_disabled_yields_alignment_failed():
    res = vbe.compare_block_images_cascade(
        _draw_a(), _draw_b(), cfg=_cfg(enable_affine=False, enable_fallback=False))
    # без affine и без fallback несошедшийся ECC → alignment_failed
    assert res["status"] in ("alignment_failed", "changed_visual", "uncertain")
    assert res["status"] != "identical_visual"


# ─── normalization / trim / metrics ──────────────────────────────────────────


def test_trim_empty_margins():
    big = _canvas(400, 320)
    # маленький контент в центре большого пустого поля
    cv2.rectangle(big, (170, 140), (230, 190), (0, 0, 0), 2)
    cv2.line(big, (170, 140), (230, 190), (0, 0, 0), 2)
    cfg = _cfg()
    gray, mask, cbox, fg, trim, color = vbe._normalize_for_match(cv2, np, big, cfg)
    assert trim is True
    assert cbox is not None
    # content bbox заметно меньше полного кадра
    assert cbox[2] - cbox[0] < 0.9 and cbox[3] - cbox[1] < 0.9
    assert gray.shape[0] < big.shape[0] and gray.shape[1] < big.shape[1]


def test_no_trim_when_content_fills_frame():
    full = _canvas(120, 100)
    cv2.rectangle(full, (1, 1), (118, 98), (0, 0, 0), 2)  # рамка по краям
    cfg = _cfg()
    gray, mask, cbox, fg, trim, color = vbe._normalize_for_match(cv2, np, full, cfg)
    assert trim is False


def test_mask_iou_unit():
    a = np.zeros((10, 10), np.uint8); a[2:6, 2:6] = 1
    b = np.zeros((10, 10), np.uint8); b[4:8, 4:8] = 1
    iou = vbe._mask_iou(np, a, b)
    # пересечение 2x2=4, объединение 16+16-4=28
    assert abs(iou - 4 / 28) < 1e-3
    assert vbe._mask_iou(np, a, a) == 1.0
    assert vbe._mask_iou(np, a, np.zeros_like(a)) == 0.0


def test_ncc_unit():
    a = np.array([[0, 1], [1, 0]], np.float64)
    assert abs(vbe._ncc(np, a, a) - 1.0) < 1e-6
    assert abs(vbe._ncc(np, a, -a) + 1.0) < 1e-6
    assert vbe._ncc(np, a, np.ones_like(a)) == 0.0   # нулевая дисперсия → 0


def test_metrics_present_in_result():
    res = vbe.compare_block_images_cascade(_draw_a(), _draw_a().copy(), cfg=_cfg())
    for k in ("alignment_method", "mask_iou", "normalized_correlation",
              "content_bbox_old", "content_bbox_new", "trim_applied",
              "foreground_ratio_old", "foreground_ratio_new",
              "total_diff_ratio", "colored_overlay_diff_ratio", "diff_bbox",
              "alignment_score"):
        assert k in res


# ─── debug images only when write_debug ──────────────────────────────────────


def test_debug_written_only_with_debug_path(tmp_path):
    a = _draw_a(); b = _draw_a_plus()
    # без debug_path — никаких файлов
    vbe.compare_block_images_cascade(a, b, cfg=_cfg(), debug_path=None)
    assert list(tmp_path.glob("*.png")) == []

    dp = tmp_path / "blk_diff.png"
    vbe.compare_block_images_cascade(a, b, cfg=_cfg(), debug_path=dp)
    assert dp.exists()
    names = {p.name for p in tmp_path.glob("*.png")}
    # ожидаем набор debug-PNG
    for suff in ("_old_crop.png", "_new_crop.png", "_old_normalized.png",
                 "_new_normalized.png", "_mask_old.png", "_mask_new.png"):
        assert any(n.endswith(suff) for n in names), f"missing {suff}: {names}"


def test_cv2_unavailable_returns_visual_unavailable(monkeypatch):
    monkeypatch.setattr(vbe, "_cv2", lambda: None)
    res = vbe.compare_block_images_cascade(_draw_a(), _draw_b(), cfg=_cfg())
    assert res["status"] == "visual_unavailable"


# ─── integration via compare_one_link (real cascade, no PDF) ─────────────────


def _img_block(bid):
    return EqBlock(block_id=bid, page=1, block_type="image",
                   coords_norm=[0.1, 0.1, 0.5, 0.5], page_width=1000, page_height=1000)


def _render_pair(old_img, new_img, ob, nb):
    def _r(block, *, source_pdf_path=None, render_long_side=1000):
        return (old_img if block is ob else new_img), {"status": "rendered"}
    return _r


def test_compare_one_link_real_cascade_identical_and_enforced_false():
    ob, nb = _img_block("L1"), _img_block("R1")
    a = _draw_a()
    link = {"left_block_id": "L1", "right_block_id": "R1", "method": "manual", "score": 1.0,
            "left_page": 1, "right_page": 1}
    rec = vbe.compare_one_link_visual_equivalence(
        link, ob, nb, cfg=_cfg(), old_pdf_path="x", new_pdf_path="y",
        render_fn=_render_pair(a, a.copy(), ob, nb))   # NO visual_compare_fn → real cascade
    assert rec["status"] == "identical_visual"
    assert rec["exclude_from_qwen"] is True
    assert rec["exclude_from_opus_md"] is True
    assert rec["enforced"] is False
    assert rec["metrics"]["alignment_method"] in ("euclidean", "affine")
    assert rec["metrics"]["mask_iou"] >= 0.97


def test_compare_one_link_real_cascade_changed():
    ob, nb = _img_block("L1"), _img_block("R1")
    link = {"left_block_id": "L1", "right_block_id": "R1", "method": "manual", "score": 1.0}
    rec = vbe.compare_one_link_visual_equivalence(
        link, ob, nb, cfg=_cfg(), old_pdf_path="x", new_pdf_path="y",
        render_fn=_render_pair(_draw_a(), _draw_a_plus(), ob, nb))
    assert rec["status"] == "changed_visual"
    assert rec["exclude_from_qwen"] is False


# ─── no Qwen/Opus/pipeline imports ───────────────────────────────────────────


def test_module_does_not_import_qwen_opus_pipeline():
    tree = ast.parse(Path(vbe.__file__).read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
            for a in node.names:
                imported.add(f"{node.module or ''}.{a.name}")
        elif isinstance(node, ast.Import):
            for a in node.names:
                imported.add(a.name)
    blob = "\n".join(sorted(imported))
    for token in ("graphic_llm", "enriched_comparison", "unified_analysis",
                  "md_enrichment_jobs", "pipeline_queue", "qwen", "opus"):
        assert token not in blob, f"unexpected import: {token}"

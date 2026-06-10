# -*- coding: utf-8 -*-
"""Тесты Pipeline V2 Visual Equivalence Gate (mark-only, до vision).

Synthetic изображения/модели, без сети/LLM/vision. Покрытие по spec:
  1.  идентичные crops → identical_visual, exclude_from_vision;
  2.  явно изменённые crops → changed_visual, send_to_vision;
  3.  минорное отличие → minor/changed по threshold, без crash;
  4.  отсутствующий crop → render_failed, manual_review;
  5.  разные размеры image → compare работает;
  6.  whitespace-рамка срезается trim'ом;
  7.  matched non-graphic блоки игнорируются;
  8.  low readiness блоки обрабатываются безопасно;
  9.  summary counts корректны;
 10.  без Qwen/Opus/Claude импортов/вызовов;
 11.  dry_run пишет visual_equivalence_gate_report.json;
 12.  dry_run fail-soft при падении gate;
 13.  summary.json/md содержат counts gate;
 14.  существующие dry-run тесты живы (отдельный файл, прогоняются вместе);
 15.  UI payload backward compatible.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from backend.app.services.stage_comparison import (
    pipeline_v2_visual_equivalence_gate as vg,
)
from backend.app.services.stage_comparison import pipeline_v2_dry_run as dr
from backend.app.services.stage_comparison import pipeline_v2_ui_payload as ui


# ─── synthetic crops ─────────────────────────────────────────────────────────


def _img_scheme(w=320, h=240, extra_line=False, border=0):
    """BGR line-art «схема»: рамка + диагональ (+опц. доп. линия)."""
    img = np.full((h + 2 * border, w + 2 * border, 3), 255, dtype=np.uint8)
    y0, x0 = border, border
    img[y0 + 20:y0 + 22, x0 + 20:x0 + w - 20] = 0          # горизонталь
    img[y0 + 20:y0 + h - 20, x0 + 20:x0 + 22] = 0          # вертикаль
    for i in range(min(w, h) - 60):                        # диагональ
        img[y0 + 30 + i, x0 + 30 + i] = 0
    if extra_line:
        img[y0 + h // 2:y0 + h // 2 + 3, x0 + 10:x0 + w - 10] = 0
    return img


def _png(tmp_path, name, img):
    import cv2
    p = tmp_path / name
    cv2.imwrite(str(p), img)
    return str(p)


# ─── synthetic models/reports ────────────────────────────────────────────────


def _block(bid, page=1, *, image_file=None, block_type="image",
           semantic_type="scheme"):
    return {
        "block_id": bid, "page_number": page, "block_type": block_type,
        "semantic_type": semantic_type,
        "coords_norm": [0.1, 0.1, 0.9, 0.9], "coords_px": [10, 10, 90, 90],
        "crop_url": None, "image_file": image_file,
        "text_excerpt": "",
    }


def _model(blocks, pdf_path=None):
    return {
        "blocks": {b["block_id"]: b for b in blocks},
        "pages": [{"page_number": 1, "width": 100, "height": 100}],
        "source": {"pdf_path": pdf_path},
    }


def _matching(pairs):
    return {"block_matches": [
        {"left_block_id": l, "right_block_id": r, "confidence": "medium",
         "risk_flags": []} for l, r in pairs]}


def _graphic_matched(pairs, risk=None):
    return {"matched": [
        {"left_block_id": l, "right_block_id": r, "match_quality": "medium",
         "risk_flags": list(risk or [])} for l, r in pairs]}


def _run(tmp_path, left_imgs, right_imgs, pairs, **kw):
    """Собрать модели с PNG-кропами и прогнать gate."""
    lb = [_block(f"L{i}", image_file=p) for i, p in enumerate(left_imgs)]
    rb = [_block(f"R{i}", image_file=p) for i, p in enumerate(right_imgs)]
    return vg.run_visual_equivalence_gate(
        _model(lb), _model(rb), _matching(pairs),
        graphic_matched_report=_graphic_matched(pairs), **kw)


# ─── 1-2: identical / changed ────────────────────────────────────────────────


def test_1_identical_crops_excluded(tmp_path):
    a = _png(tmp_path, "a.png", _img_scheme())
    b = _png(tmp_path, "b.png", _img_scheme())
    rep = _run(tmp_path, [a], [b], [("L0", "R0")])
    pair = rep["block_pairs"][0]
    assert pair["status"] == "identical_visual"
    assert pair["decision"] == "exclude_from_vision"
    assert pair["confidence"] > 0.9
    assert rep["summary"]["identical_visual"] == 1
    assert rep["summary"]["exclude_from_vision"] == 1


def test_2_changed_crops_sent_to_vision(tmp_path):
    a = _png(tmp_path, "a.png", _img_scheme())
    b = _png(tmp_path, "b.png", _img_scheme(extra_line=True))
    rep = _run(tmp_path, [a], [b], [("L0", "R0")])
    pair = rep["block_pairs"][0]
    assert pair["status"] == "changed_visual"
    assert pair["decision"] == "send_to_vision"
    assert rep["summary"]["send_to_vision"] == 1


# ─── 3: минорное отличие не валит ────────────────────────────────────────────


def test_3_minor_difference_no_crash(tmp_path):
    img_a = _img_scheme()
    img_b = _img_scheme()
    img_b[5:7, 5:7] = 0          # пара пикселей шума
    a = _png(tmp_path, "a.png", img_a)
    b = _png(tmp_path, "b.png", img_b)
    rep = _run(tmp_path, [a], [b], [("L0", "R0")])
    pair = rep["block_pairs"][0]
    assert pair["status"] in ("identical_visual", "minor_visual",
                              "changed_visual")
    assert pair["decision"] in ("exclude_from_vision", "send_to_vision",
                                "manual_review")


def test_3b_minor_decision_by_threshold():
    # высокая уверенность → exclude; низкая → manual_review (осторожный default)
    d_hi, _ = vg.decide_from_status("minor_visual", 0.95, None)
    d_lo, _ = vg.decide_from_status("minor_visual", 0.4, None)
    assert d_hi == "exclude_from_vision"
    assert d_lo == "manual_review"
    # порог настраивается
    d_opt, _ = vg.decide_from_status(
        "minor_visual", 0.5, {"minor_exclude_min_confidence": 0.4})
    assert d_opt == "exclude_from_vision"


# ─── 4: отсутствующий crop ───────────────────────────────────────────────────


def test_4_missing_crop_manual_review(tmp_path):
    a = _png(tmp_path, "a.png", _img_scheme())
    rep = _run(tmp_path, [a], ["/nonexistent/crop.png"], [("L0", "R0")])
    pair = rep["block_pairs"][0]
    assert pair["status"] == "render_failed"
    assert pair["decision"] == "manual_review"
    assert rep["summary"]["render_failed"] == 1
    assert rep["summary"]["manual_review"] == 1


# ─── 5-6: размеры и trim ─────────────────────────────────────────────────────


def test_5_different_sizes_compared(tmp_path):
    a = _png(tmp_path, "a.png", _img_scheme(320, 240))
    b = _png(tmp_path, "b.png", _img_scheme(480, 360))
    rep = _run(tmp_path, [a], [b], [("L0", "R0")])
    assert rep["block_pairs"][0]["status"] != "render_failed"
    assert rep["summary"]["compared_total"] == 1


def test_6_whitespace_border_trimmed(tmp_path):
    a = _png(tmp_path, "a.png", _img_scheme())
    b = _png(tmp_path, "b.png", _img_scheme(border=40))  # белая рамка
    rep = _run(tmp_path, [a], [b], [("L0", "R0")])
    pair = rep["block_pairs"][0]
    # после trim содержимое совпадает → identical/minor, НЕ changed
    assert pair["status"] in ("identical_visual", "minor_visual")
    assert pair["decision"] in ("exclude_from_vision", "manual_review")


# ─── 7: non-graphic игнорируются ─────────────────────────────────────────────


def test_7_non_graphic_pairs_ignored(tmp_path):
    a = _png(tmp_path, "a.png", _img_scheme())
    b = _png(tmp_path, "b.png", _img_scheme())
    lb = [_block("L0", image_file=a),
          _block("LT", block_type="text", semantic_type="text")]
    rb = [_block("R0", image_file=b),
          _block("RT", block_type="text", semantic_type="text")]
    # graphic_matched НЕ передан → fallback-отбор из block_matching
    rep = vg.run_visual_equivalence_gate(
        _model(lb), _model(rb),
        _matching([("L0", "R0"), ("LT", "RT")]))
    keys = [p["pair_key"] for p in rep["block_pairs"]]
    assert keys == ["L0__R0"]
    assert rep["summary"]["matched_graphic_blocks_total"] == 1


# ─── 8: low readiness безопасно ──────────────────────────────────────────────


def test_8_low_readiness_flagged_not_crashed(tmp_path):
    a = _png(tmp_path, "a.png", _img_scheme())
    b = _png(tmp_path, "b.png", _img_scheme())
    gr_left = {"descriptors": [{"block_id": "L0",
                                "diff_readiness": {"readiness": "not_usable"},
                                "quality_flags": []}]}
    lb = [_block("L0", image_file=a)]
    rb = [_block("R0", image_file=b)]
    rep = vg.run_visual_equivalence_gate(
        _model(lb), _model(rb), _matching([("L0", "R0")]),
        left_graphic_report=gr_left,
        graphic_matched_report=_graphic_matched([("L0", "R0")]))
    pair = rep["block_pairs"][0]
    assert "left_readiness_not_usable" in pair["risk_flags"]
    assert pair["status"] == "identical_visual"   # визуал авторитетен


# ─── 9: summary counts ───────────────────────────────────────────────────────


def test_9_summary_counts_correct(tmp_path):
    a = _png(tmp_path, "a.png", _img_scheme())
    b = _png(tmp_path, "b.png", _img_scheme())
    c = _png(tmp_path, "c.png", _img_scheme(extra_line=True))
    rep = _run(tmp_path, [a, a], [b, c],
               [("L0", "R0"), ("L1", "R1")])
    s = rep["summary"]
    assert s["matched_graphic_blocks_total"] == 2
    assert s["compared_total"] == 2
    assert s["identical_visual"] + s["minor_visual"] + s["changed_visual"] \
        + s["uncertain"] + s["render_failed"] + s["skipped"] == 2
    assert s["exclude_from_vision"] + s["send_to_vision"] \
        + s["manual_review"] == 2
    assert rep["kind"] == vg.REPORT_KIND


def test_9b_max_pairs_cap_not_silent(tmp_path):
    a = _png(tmp_path, "a.png", _img_scheme())
    b = _png(tmp_path, "b.png", _img_scheme())
    rep = _run(tmp_path, [a, a, a], [b, b, b],
               [("L0", "R0"), ("L1", "R1"), ("L2", "R2")],
               options={"max_pairs": 1})
    s = rep["summary"]
    assert s["compared_total"] == 1
    assert s["skipped"] == 2
    assert any("max_pairs" in w for w in rep["warnings"])
    skipped = [p for p in rep["block_pairs"] if p["status"] == "skipped"]
    assert all(p["decision"] == "manual_review" for p in skipped)


# ─── 10: офлайн-гарантии ─────────────────────────────────────────────────────


def test_10_no_llm_imports_and_no_network(tmp_path, monkeypatch):
    src = Path(vg.__file__).read_text(encoding="utf-8")
    for forbidden in ("graphic_llm", "text_llm", "llm_runner",
                      "ClaudeCodeProvider", "claude -p", "qwen", "opus",
                      "httpx", "requests", "urllib", "subprocess"):
        assert forbidden.lower() not in src.lower(), \
            f"gate references {forbidden!r}"
    import socket

    def _boom(*a, **k):  # pragma: no cover
        raise AssertionError("network in visual gate")

    monkeypatch.setattr(socket, "socket", _boom)
    a = _png(tmp_path, "a.png", _img_scheme())
    b = _png(tmp_path, "b.png", _img_scheme())
    rep = _run(tmp_path, [a], [b], [("L0", "R0")])
    assert rep["summary"]["compared_total"] == 1


# ─── 11-13: интеграция в dry_run ─────────────────────────────────────────────


def _result_json(tmp_path, name, blocks):
    p = tmp_path / name
    p.write_text(json.dumps({
        "pages": [{"page_number": 1, "width": 100, "height": 100,
                   "blocks": blocks}],
    }, ensure_ascii=False), encoding="utf-8")
    return p


@pytest.fixture()
def gate_dry_run(tmp_path):
    img_a = _png(tmp_path, "ca.png", _img_scheme())
    img_b = _png(tmp_path, "cb.png", _img_scheme())
    blocks_l = [{"block_id": "BL1", "block_type": "image",
                 "coords_norm": [0.1, 0.1, 0.9, 0.9],
                 "image_file": img_a, "ocr_text": ""}]
    blocks_r = [{"block_id": "BL1", "block_type": "image",
                 "coords_norm": [0.1, 0.1, 0.9, 0.9],
                 "image_file": img_b, "ocr_text": ""}]
    left = {"result_json_path": str(_result_json(tmp_path, "l.json", blocks_l))}
    right = {"result_json_path": str(_result_json(tmp_path, "r.json", blocks_r))}
    out = tmp_path / "out"
    summary = dr.run_pipeline_v2_dry_run(left, right, out, options={},
                                         llm_runner=None)
    return summary, out


def test_11_dry_run_writes_gate_report(gate_dry_run):
    summary, out = gate_dry_run
    p = out / "visual_equivalence_gate_report.json"
    assert p.exists()
    rep = json.loads(p.read_text(encoding="utf-8"))
    assert rep["kind"] == vg.REPORT_KIND
    assert "block_pairs" in rep


def test_12_dry_run_failsoft_when_gate_raises(tmp_path, monkeypatch):
    def _boom(*a, **k):
        raise RuntimeError("gate exploded")

    monkeypatch.setattr(dr, "run_visual_equivalence_gate", _boom)
    blocks = [{"block_id": "B1", "block_type": "text", "ocr_text": "т"}]
    left = {"result_json_path": str(_result_json(tmp_path, "l.json", blocks))}
    right = {"result_json_path": str(_result_json(tmp_path, "r.json", blocks))}
    summary = dr.run_pipeline_v2_dry_run(left, right, tmp_path / "out",
                                         options={}, llm_runner=None)
    assert summary["status"] != "failed"          # дальше этапы отработали
    assert summary["visual_equivalence_gate"]["status"] == "failed"
    assert any("visual_gate" in w and "RuntimeError" in w
               for w in summary["warnings"])
    assert summary["stages"]["entity_diff"] is not None


def test_12b_gate_can_be_disabled(tmp_path):
    blocks = [{"block_id": "B1", "block_type": "text", "ocr_text": "т"}]
    left = {"result_json_path": str(_result_json(tmp_path, "l.json", blocks))}
    right = {"result_json_path": str(_result_json(tmp_path, "r.json", blocks))}
    out = tmp_path / "out"
    summary = dr.run_pipeline_v2_dry_run(
        left, right, out, options={"visual_gate": {"enabled": False}},
        llm_runner=None)
    assert summary["visual_equivalence_gate"]["status"] == "disabled"
    assert not (out / "visual_equivalence_gate_report.json").exists()


def test_13_summary_json_and_md_have_gate_counts(gate_dry_run):
    summary, out = gate_dry_run
    ve = summary["visual_equivalence_gate"]
    assert ve["enabled"] is True
    assert ve["matched_graphic_blocks_total"] >= 1
    assert ve["compared_total"] >= 1
    md = (out / "pipeline_v2_summary.md").read_text(encoding="utf-8")
    assert "## Visual equivalence gate" in md
    assert "excluded_from_vision=" in md
    js = json.loads((out / "pipeline_v2_summary.json").read_text(encoding="utf-8"))
    assert "visual_equivalence_gate" in js


# ─── 15: UI payload backward compatible ──────────────────────────────────────


def test_15_ui_payload_backward_compatible(gate_dry_run):
    summary, out = gate_dry_run
    diff = json.loads((out / "entity_diff_report.json").read_text(encoding="utf-8"))
    de = json.loads((out / "delta_explanation_report.json").read_text(encoding="utf-8"))
    payload = ui.build_pipeline_v2_ui_payload(summary, diff, de)
    # новая под-секция появляется при наличии gate в summary
    ve = payload["graphic_readiness"].get("visual_equivalence")
    assert ve is not None
    assert set(ve) == {"status", "compared_total", "exclude_from_vision",
                       "send_to_vision", "manual_review", "changed_visual",
                       "uncertain"}
    assert ve["status"] in ("ok", "completed_with_warnings")
    # старый summary БЕЗ секции → ключа нет (полная совместимость)
    old_summary = dict(summary)
    old_summary.pop("visual_equivalence_gate")
    payload_old = ui.build_pipeline_v2_ui_payload(old_summary, diff, de)
    assert "visual_equivalence" not in payload_old["graphic_readiness"]
    # контракт секций/headline не тронут
    assert [s["key"] for s in payload_old["sections"]] == \
        [s["key"] for s in payload["sections"]]


def test_15b_ui_payload_disabled_gate_no_section(gate_dry_run):
    summary, out = gate_dry_run
    diff = json.loads((out / "entity_diff_report.json").read_text(encoding="utf-8"))
    de = json.loads((out / "delta_explanation_report.json").read_text(encoding="utf-8"))
    disabled = dict(summary)
    disabled["visual_equivalence_gate"] = {"enabled": False,
                                           "status": "disabled"}
    payload = ui.build_pipeline_v2_ui_payload(disabled, diff, de)
    assert "visual_equivalence" not in payload["graphic_readiness"]


def test_15c_ui_payload_failed_gate_distinguishable(gate_dry_run):
    summary, out = gate_dry_run
    diff = json.loads((out / "entity_diff_report.json").read_text(encoding="utf-8"))
    de = json.loads((out / "delta_explanation_report.json").read_text(encoding="utf-8"))
    failed = dict(summary)
    failed["visual_equivalence_gate"] = {
        "enabled": True, "status": "failed", "error": "RuntimeError: boom",
        "compared_total": 0, "exclude_from_vision": 0, "send_to_vision": 0,
        "manual_review": 0, "changed_visual": 0, "uncertain": 0,
    }
    ve = ui.build_pipeline_v2_ui_payload(failed, diff, de)[
        "graphic_readiness"]["visual_equivalence"]
    assert ve["status"] == "failed"
    assert "RuntimeError" in ve["error"]


# ─── kill-тесты адверсариального ревью ───────────────────────────────────────


def test_k1_uncertain_never_excluded():
    """КРИТИЧНО: uncertain НИКОГДА не исключается, любая confidence."""
    for conf in (0.0, 0.3, 0.8, 1.0):
        d, _ = vg.decide_from_status(vg.GS_UNCERTAIN, conf, None)
        assert d == vg.DECISION_VISION
    d, _ = vg.decide_from_status(vg.GS_RENDER_FAILED, 1.0, None)
    assert d == vg.DECISION_MANUAL


def _run_with_engine(tmp_path, monkeypatch, engine_result):
    """Прогнать gate с замоканным движком (один matched graphic pair)."""
    a = _png(tmp_path, "ka.png", _img_scheme())
    b = _png(tmp_path, "kb.png", _img_scheme())
    monkeypatch.setattr(vg, "compare_block_images_cascade",
                        lambda *args, **kw: dict(engine_result))
    return _run(tmp_path, [a], [b], [("L0", "R0")])


@pytest.mark.parametrize("engine_status", [
    "visual_unavailable", "alignment_failed", "totally_unknown_status"])
def test_k2_unknown_engine_status_never_identical(tmp_path, monkeypatch,
                                                  engine_status):
    rep = _run_with_engine(tmp_path, monkeypatch, {"status": engine_status})
    pair = rep["block_pairs"][0]
    assert pair["status"] == "uncertain"
    assert pair["decision"] == "send_to_vision"


def test_k3_engine_minor_maps_to_minor_exactly(tmp_path, monkeypatch):
    rep = _run_with_engine(tmp_path, monkeypatch, {
        "status": "minor_render_noise", "total_diff_ratio": 0.021,
        "mask_iou": 0.99, "normalized_correlation": 0.99,
        "alignment_method": "euclidean"})
    pair = rep["block_pairs"][0]
    assert pair["status"] == "minor_visual"
    assert rep["summary"]["minor_visual"] == 1
    assert rep["summary"]["identical_visual"] == 0


def test_k4_minor_confidence_formula_drives_decision(tmp_path, monkeypatch):
    """Minor у границы identical → exclude; у границы changed → manual."""
    near_identical = _run_with_engine(tmp_path, monkeypatch, {
        "status": "changed_visual", "total_diff_ratio": 0.021,
        "colored_overlay_diff_ratio": 0.0,
        "mask_iou": 0.97, "normalized_correlation": 0.98,
        "alignment_method": "euclidean"})
    p1 = near_identical["block_pairs"][0]
    assert p1["status"] == "minor_visual"          # band-реклассификация
    assert p1["confidence"] > 0.9
    assert p1["decision"] == "exclude_from_vision"
    near_changed = _run_with_engine(tmp_path, monkeypatch, {
        "status": "changed_visual", "total_diff_ratio": 0.049,
        "colored_overlay_diff_ratio": 0.0,
        "mask_iou": 0.97, "normalized_correlation": 0.98,
        "alignment_method": "euclidean"})
    p2 = near_changed["block_pairs"][0]
    assert p2["status"] == "minor_visual"
    assert p2["confidence"] < 0.8
    assert p2["decision"] == "manual_review"


def test_k5_summary_counts_by_status_not_decision(tmp_path, monkeypatch):
    # minor excluded: status=minor_visual, decision=exclude — счётчики
    # НЕ должны путать статусы с решениями
    rep = _run_with_engine(tmp_path, monkeypatch, {
        "status": "minor_render_noise", "total_diff_ratio": 0.021,
        "mask_iou": 0.99, "normalized_correlation": 0.99,
        "alignment_method": "euclidean"})
    s = rep["summary"]
    assert s["minor_visual"] == 1
    assert s["identical_visual"] == 0
    assert s["exclude_from_vision"] == 1


def test_k6_cv2_unavailable_degrades_with_warning(tmp_path, monkeypatch):
    # без cv2 движок возвращает visual_unavailable (моделируем оба уровня:
    # gate-проверку и engine-ответ реального no-cv2 окружения)
    monkeypatch.setattr(vg, "cv2_available", lambda: False)
    monkeypatch.setattr(vg, "compare_block_images_cascade",
                        lambda *a, **k: {"status": "visual_unavailable"})
    a = _png(tmp_path, "a.png", _img_scheme())
    b = _png(tmp_path, "b.png", _img_scheme())
    rep = _run(tmp_path, [a], [b], [("L0", "R0")])
    assert any("cv2" in w for w in rep["warnings"])
    assert rep["summary"]["cv2_available"] is False
    pair = rep["block_pairs"][0]
    assert pair["status"] == "uncertain"
    assert pair["decision"] == "send_to_vision"
    # ложного exclude нет ни у одной пары
    assert rep["summary"]["exclude_from_vision"] == 0


def test_k7_dry_run_forwards_gate_report_warnings(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import pipeline_v2_dry_run as drm

    def _fake_gate(*a, **k):
        return {"version": 1, "kind": vg.REPORT_KIND,
                "status": "completed_with_warnings",
                "summary": {}, "block_pairs": [],
                "warnings": ["upstream gate warning X"]}

    monkeypatch.setattr(drm, "run_visual_equivalence_gate", _fake_gate)
    blocks = [{"block_id": "B1", "block_type": "text", "ocr_text": "т"}]
    left = {"result_json_path": str(_result_json(tmp_path, "l.json", blocks))}
    right = {"result_json_path": str(_result_json(tmp_path, "r.json", blocks))}
    summary = drm.run_pipeline_v2_dry_run(left, right, tmp_path / "out",
                                          options={}, llm_runner=None)
    assert any("visual_gate: upstream gate warning X" in w
               for w in summary["warnings"])


# ─── регрессии фиксов ревью ──────────────────────────────────────────────────


def test_r1_localized_residual_diff_not_excluded(tmp_path):
    """Анти-dilution: смена «номинала» на большом блоке НЕ исключается."""
    import cv2
    big_a = np.full((1000, 1000, 3), 255, dtype=np.uint8)
    # плотная сетка линий (схема)
    for y in range(50, 1000, 60):
        big_a[y:y + 2, 20:980] = 0
    for x in range(50, 1000, 80):
        big_a[20:980, x:x + 2] = 0
    big_b = big_a.copy()
    cv2.putText(big_a, "160A", (495, 500), cv2.FONT_HERSHEY_SIMPLEX, 0.6, 0, 2)
    cv2.putText(big_b, "250A", (495, 500), cv2.FONT_HERSHEY_SIMPLEX, 0.6, 0, 2)
    a = _png(tmp_path, "big_a.png", big_a)
    b = _png(tmp_path, "big_b.png", big_b)
    rep = _run(tmp_path, [a], [b], [("L0", "R0")])
    pair = rep["block_pairs"][0]
    # сам статус может быть identical (ratio-пороги), но решение — НЕ exclude
    assert pair["decision"] != "exclude_from_vision"
    if pair["status"] == "identical_visual":
        assert "localized_residual_diff" in pair["risk_flags"]
        assert pair["confidence"] <= 0.95


def test_r2_minor_band_reachable_end_to_end(tmp_path, monkeypatch):
    """Minor-полоса достижима через engine-результат changed (band)."""
    rep = _run_with_engine(tmp_path, monkeypatch, {
        "status": "changed_visual", "total_diff_ratio": 0.03,
        "colored_overlay_diff_ratio": 0.005,
        "mask_iou": 0.96, "normalized_correlation": 0.97,
        "alignment_method": "euclidean"})
    assert rep["block_pairs"][0]["status"] == "minor_visual"
    assert rep["summary"]["minor_visual"] == 1


def test_r3_graphic_matched_priority_path_filters_non_graphic(tmp_path):
    a = _png(tmp_path, "a.png", _img_scheme())
    b = _png(tmp_path, "b.png", _img_scheme())
    lb = [_block("L0", image_file=a),
          _block("LT", block_type="text", semantic_type="text")]
    rb = [_block("R0", image_file=b),
          _block("RT", block_type="text", semantic_type="text")]
    # priority-путь: graphic_matched содержит и text-пару — она отфильтрована
    rep = vg.run_visual_equivalence_gate(
        _model(lb), _model(rb), _matching([]),
        graphic_matched_report=_graphic_matched([("L0", "R0"), ("LT", "RT")]))
    assert [p["pair_key"] for p in rep["block_pairs"]] == ["L0__R0"]
# -*- coding: utf-8 -*-
"""Тесты Pipeline V2 Graphic Vision Enrichment (offline, runner injectable).

Покрываемые spec-кейсы задачи:
  1.  exclude_from_vision не выбирается;
  2.  send_to_vision выбирается;
  3.  manual_review выбирается только при include_manual_review=true;
  4.  max_items соблюдается (+warning, no silent caps);
  5.  missing visual gate → skipped_no_visual_gate;
  6.  no runner → skipped_no_runner, prompts сохранены;
  7.  fake runner → result нормализуется;
  8.  bad runner JSON → failed item, отчёт не падает;
  9.  prompts содержат запрет придумывать;
  10. crop refs/path сохраняются;
  11. dry-run пишет graphic_vision_enrichment_report.json;
  12. dry-run fail-soft при ошибке;
  13. ui_payload summary появляется при наличии report;
  14. никаких vision/LLM imports/calls.

Реальные vision-модели/сеть не используются.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import (
    pipeline_v2_graphic_vision_enrichment as gv,
)


# ─── синтетика в форме реальных артефактов ────────────────────────────────────


def _model(side: str) -> dict:
    p = side[0].upper()
    blocks = {}
    for i in range(1, 5):
        blocks[f"{p}_S{i}"] = {
            "block_id": f"{p}_S{i}", "page_number": i,
            "block_type": "image", "semantic_type": "scheme",
            "coords_norm": [0.1, 0.1, 0.9, 0.9],
            "image_file": f"/nonexistent/{p}_S{i}.png",
        }
    return {
        "version": 1,
        "kind": "stage_comparison_pipeline_v2_normalized_document_model",
        "source": {"pdf_path": f"/nonexistent/{side}.pdf"},
        "pages": [{"page_number": i, "sheet_name": f"Лист {i}",
                   "width": 1000, "height": 700} for i in range(1, 5)],
        "blocks": blocks,
        "warnings": [],
    }


def _gate_pair(i: int, decision: str, status: str = "changed_visual") -> dict:
    return {
        "pair_key": f"L_S{i}__R_S{i}",
        "left_block_id": f"L_S{i}", "right_block_id": f"R_S{i}",
        "left_page_number": i, "right_page_number": i,
        "status": status, "decision": decision, "confidence": 0.9,
        "risk_flags": [],
        "metrics": {"mask_iou": 0.4, "normalized_correlation": 0.5,
                    "total_diff_ratio": 0.2,
                    "alignment_method": "ecc_euclidean"},
    }


def _gate(pairs: list[dict]) -> dict:
    return {
        "version": 1,
        "kind": "stage_comparison_pipeline_v2_visual_equivalence_gate",
        "status": "ok", "summary": {}, "block_pairs": pairs, "warnings": [],
    }


def _graphic_report(side: str) -> dict:
    p = side[0].upper()
    return {"descriptors": [
        {"block_id": f"{p}_S{i}", "graphic_type": "single_line_scheme",
         "discipline": "EOM",
         "diff_readiness": {"readiness": "medium", "usable_for_diff": True}}
        for i in range(1, 5)]}


_DEFAULT_GATE = _gate([
    _gate_pair(1, "send_to_vision"),
    _gate_pair(2, "manual_review", status="uncertain"),
    _gate_pair(3, "exclude_from_vision", status="identical_visual"),
])


def _run(gate=None, *, options=None, vision_runner=None, crops_dir=None):
    return gv.run_graphic_vision_enrichment(
        _model("left"), _model("right"),
        _DEFAULT_GATE if gate is None else gate,
        left_graphic_report=_graphic_report("left"),
        right_graphic_report=_graphic_report("right"),
        options={"render_crops": False, **(options or {})},
        vision_runner=vision_runner, crops_dir=crops_dir)


def _ok_runner(prompt, left_path, right_path, options):
    return {
        "old_description": "Однолинейная схема ВРУ-2, 1000А",
        "new_description": "Однолинейная схема ВРУ-2, фидеры пересмотрены",
        "observed_changes": ["QF3 160А → 250А"],
        "engineering_entities_old": ["ВРУ-2", "QF3 160А"],
        "engineering_entities_new": ["ВРУ-2", "QF3 250А"],
        "possible_risks": ["проверить селективность"],
        "confidence": "high",
    }


# ─── 1-4: selection ──────────────────────────────────────────────────────────


def test_1_exclude_from_vision_not_selected():
    rep = _run()
    ids = [i["item_id"] for i in rep["items"]]
    assert "gv_L_S3__R_S3" not in ids
    assert rep["summary"]["excluded_by_visual_gate"] == 1


def test_2_send_to_vision_selected():
    rep = _run()
    ids = [i["item_id"] for i in rep["items"]]
    assert "gv_L_S1__R_S1" in ids
    item = rep["items"][0]
    assert item["visual_decision"] == "send_to_vision"
    assert item["visual_status"] == "changed_visual"


def test_3_manual_review_included_only_when_enabled():
    rep = _run(options={"include_manual_review": True})
    assert "gv_L_S2__R_S2" in [i["item_id"] for i in rep["items"]]
    assert rep["summary"]["manual_review_included"] == 1

    rep_off = _run(options={"include_manual_review": False})
    assert "gv_L_S2__R_S2" not in [i["item_id"] for i in rep_off["items"]]
    assert rep_off["summary"]["manual_review_included"] == 0
    # send_to_vision при этом остаётся
    assert "gv_L_S1__R_S1" in [i["item_id"] for i in rep_off["items"]]


def test_3b_send_to_vision_prioritized_over_manual_at_cap():
    gate = _gate([_gate_pair(1, "manual_review"),
                  _gate_pair(2, "send_to_vision"),
                  _gate_pair(4, "send_to_vision")])
    rep = _run(gate, options={"max_items": 2})
    ids = [i["item_id"] for i in rep["items"]]
    # под cap сначала send_to_vision (подтверждённое изменение)
    assert ids == ["gv_L_S2__R_S2", "gv_L_S4__R_S4"]


def test_4_max_items_respected_with_warning():
    gate = _gate([_gate_pair(i, "send_to_vision") for i in range(1, 5)])
    rep = _run(gate, options={"max_items": 2})
    assert rep["summary"]["selected_total"] == 2
    assert rep["summary"]["candidates_total"] == 4
    assert any("max_items" in w for w in rep["warnings"])


# ─── 5: missing visual gate ──────────────────────────────────────────────────


def test_5_missing_visual_gate_is_skipped():
    rep = _run(gate={"no_block_pairs": True})
    assert rep["status"] == "skipped_no_visual_gate"
    assert rep["items"] == []
    assert rep["summary"]["selected_total"] == 0
    rep2 = gv.run_graphic_vision_enrichment(_model("left"), _model("right"),
                                            None)
    assert rep2["status"] == "skipped_no_visual_gate"


# ─── 6: no runner ────────────────────────────────────────────────────────────


def test_6_no_runner_keeps_candidates_and_prompts():
    rep = _run(vision_runner=None)
    assert rep["status"] == "skipped_no_runner"
    assert rep["summary"]["selected_total"] == 2
    assert rep["summary"]["vision_calls_attempted"] == 0
    assert rep["summary"]["skipped_no_runner"] == 2
    for item in rep["items"]:
        assert item["vision_status"] == "skipped_no_runner"
        assert item["result"] is None
        assert item["prompt"]  # prompt записан
        assert item["left_crop_ref"]  # refs записаны


def test_6b_write_prompts_false_omits_prompt():
    rep = _run(options={"write_prompts": False})
    assert all(i["prompt"] is None for i in rep["items"])


# ─── 7: fake runner ──────────────────────────────────────────────────────────


def test_7_fake_runner_result_normalized():
    rep = _run(vision_runner=_ok_runner)
    assert rep["status"] == "ok"
    s = rep["summary"]
    assert s["vision_calls_attempted"] == 2
    assert s["vision_calls_succeeded"] == 2
    assert s["vision_calls_failed"] == 0
    item = rep["items"][0]
    assert item["vision_status"] == "ok"
    r = item["result"]
    assert r["old_description"].startswith("Однолинейная")
    assert r["observed_changes"] == ["QF3 160А → 250А"]
    assert r["confidence"] == "high"


def test_7b_runner_json_string_is_parsed():
    def _str_runner(prompt, lp, rp, options):
        return json.dumps({"old_description": "OLD", "new_description": "NEW",
                           "confidence": "medium"})
    rep = _run(vision_runner=_str_runner)
    assert rep["items"][0]["vision_status"] == "ok"
    assert rep["items"][0]["result"]["confidence"] == "medium"


def test_7c_invalid_confidence_coerced_to_low_with_warning():
    def _weird(prompt, lp, rp, options):
        return {"old_description": "a", "new_description": "b",
                "confidence": "sure!"}
    rep = _run(vision_runner=_weird)
    item = rep["items"][0]
    assert item["result"]["confidence"] == "low"
    assert any("confidence" in w for w in item["warnings"])


# ─── 8: bad runner output ────────────────────────────────────────────────────


def test_8_bad_runner_json_fails_item_not_report():
    calls = {"n": 0}

    def _flaky(prompt, lp, rp, options):
        calls["n"] += 1
        if calls["n"] == 1:
            return "not a json {{{"
        return _ok_runner(prompt, lp, rp, options)

    rep = _run(vision_runner=_flaky)
    assert rep["status"] == "completed_with_warnings"
    statuses = [i["vision_status"] for i in rep["items"]]
    assert statuses.count("failed") == 1
    assert statuses.count("ok") == 1
    assert rep["summary"]["vision_calls_failed"] == 1


def test_8b_runner_exception_fails_item_not_report():
    def _boom(prompt, lp, rp, options):
        raise RuntimeError("vision exploded")
    rep = _run(vision_runner=_boom)
    assert rep["status"] == "failed"  # все вызовы упали
    for item in rep["items"]:
        assert item["vision_status"] == "failed"
        assert any("vision exploded" in w for w in item["warnings"])


def test_8c_runner_empty_descriptions_failed():
    def _empty(prompt, lp, rp, options):
        return {"observed_changes": ["x"]}
    rep = _run(vision_runner=_empty)
    assert all(i["vision_status"] == "failed" for i in rep["items"])


# ─── 9: prompt contract ──────────────────────────────────────────────────────


def test_9_prompt_contains_strict_rules():
    rep = _run()
    prompt = rep["items"][0]["prompt"]
    assert "НЕ придумывай" in prompt
    assert "юридических" in prompt
    assert "нечитаемо" in prompt.lower()
    assert "JSON" in prompt
    # контекст блока подставлен
    assert "single_line_scheme" in prompt
    assert "EOM" in prompt
    assert "стр. 1" in prompt


def test_9b_prompt_via_builder_function():
    p = gv.build_vision_prompt_for_block_pair(
        {"left_page_number": 7, "right_page_number": 9,
         "status": "changed_visual"},
        graphic_type="plan", discipline="AR",
        left_sheet_name="План 2 этажа")
    assert "plan" in p and "AR" in p
    assert "стр. 7 (План 2 этажа)" in p
    assert "стр. 9" in p


# ─── 10: crop refs ───────────────────────────────────────────────────────────


def test_10_crop_refs_and_sources_recorded():
    rep = _run()
    item = rep["items"][0]
    src = item["left_crop_source"]
    assert src["image_file"] == "/nonexistent/L_S1.png"
    assert src["pdf_path"] == "/nonexistent/left.pdf"
    assert src["page_number"] == 1
    assert src["bbox_norm"] == [0.1, 0.1, 0.9, 0.9]
    # ref — image_file приоритетнее pdf-ссылки
    assert item["left_crop_ref"] == "/nonexistent/L_S1.png"
    # без image_file ref деградирует к pdf#page
    gate = _gate([_gate_pair(1, "send_to_vision")])
    lm = _model("left")
    lm["blocks"]["L_S1"]["image_file"] = None
    rep2 = gv.run_graphic_vision_enrichment(
        lm, _model("right"), gate, options={"render_crops": False})
    assert rep2["items"][0]["left_crop_ref"] == "/nonexistent/left.pdf#page=1"


def test_10b_render_failure_with_runner_fails_item_fail_soft(tmp_path):
    # render_crops=True + несуществующие источники → рендер падает →
    # item failed с warning, runner НЕ вызван, отчёт жив
    calls = {"n": 0}

    def _runner(prompt, lp, rp, options):
        calls["n"] += 1
        return _ok_runner(prompt, lp, rp, options)

    rep = _run(options={"render_crops": True}, vision_runner=_runner,
               crops_dir=tmp_path / "crops")
    assert calls["n"] == 0
    assert all(i["vision_status"] == "failed" for i in rep["items"])
    assert all(any("no crop image available" in w for w in i["warnings"])
               for i in rep["items"])
    # killer ревью: ВСЁ упало до вызова runner'а — это failed, не warnings
    assert rep["status"] == "failed"
    assert rep["summary"]["vision_calls_attempted"] == 0
    assert rep["summary"]["vision_calls_failed"] == len(rep["items"])
    # сводный warning о падениях поднят на уровень отчёта (для dry-run)
    assert any("vision items failed" in w for w in rep["warnings"])


def _png(path: Path, color=(255, 255, 255)) -> str:
    import numpy as np
    import cv2
    img = np.full((80, 120, 3), color, dtype="uint8")
    cv2.rectangle(img, (10, 10), (110, 70), (0, 0, 0), 2)
    path.parent.mkdir(parents=True, exist_ok=True)
    cv2.imwrite(str(path), img)
    return str(path)


def test_10c_render_success_e2e_paths_and_refs(tmp_path):
    """Killer ревью: успешный рендер — runner получает СУЩЕСТВУЮЩИЕ файлы,
    left/right не перепутаны, crop refs апгрейдятся до rendered PNG."""
    pytest.importorskip("cv2")
    lm, rm = _model("left"), _model("right")
    lm["blocks"]["L_S1"]["image_file"] = _png(tmp_path / "src_left.png")
    rm["blocks"]["R_S1"]["image_file"] = _png(tmp_path / "src_right.png",
                                              color=(200, 200, 200))
    gate = _gate([_gate_pair(1, "send_to_vision")])
    seen = {}

    def _runner(prompt, lp, rp, options):
        seen["left"], seen["right"] = lp, rp
        return _ok_runner(prompt, lp, rp, options)

    rep = gv.run_graphic_vision_enrichment(
        lm, rm, gate, options={"render_crops": True},
        vision_runner=_runner, crops_dir=tmp_path / "crops")
    assert rep["status"] == "ok"
    assert Path(seen["left"]).is_file() and Path(seen["right"]).is_file()
    assert seen["left"].endswith("_left.png")
    assert seen["right"].endswith("_right.png")
    item = rep["items"][0]
    assert item["left_crop_ref"] == seen["left"]
    assert item["right_crop_ref"] == seen["right"]
    assert item["vision_status"] == "ok"


def test_10d_one_sided_render_still_calls_runner(tmp_path):
    """Killer ревью: блок только с одной стороны (added/deleted графика) —
    runner вызывается с (path, None), item не теряется."""
    pytest.importorskip("cv2")
    lm = _model("left")
    lm["blocks"]["L_S1"]["image_file"] = _png(tmp_path / "src_left.png")
    rm = _model("right")
    del rm["blocks"]["R_S1"]   # правый блок отсутствует в модели
    gate = _gate([_gate_pair(1, "manual_review", status="skipped")])
    seen = {}

    def _runner(prompt, lp, rp, options):
        seen["left"], seen["right"] = lp, rp
        return _ok_runner(prompt, lp, rp, options)

    rep = gv.run_graphic_vision_enrichment(
        lm, rm, gate, options={"render_crops": True},
        vision_runner=_runner, crops_dir=tmp_path / "crops")
    assert seen["right"] is None
    assert Path(seen["left"]).is_file()
    item = rep["items"][0]
    assert item["vision_status"] == "ok"
    assert rep["summary"]["vision_calls_attempted"] == 1
    assert any("right block missing" in w for w in item["warnings"])


def test_10e_render_requested_without_crops_dir_fails_item():
    """Killer ревью: render_crops=True (default) + crops_dir=None + runner —
    runner НЕ вызывается с (None, None), item честно failed."""
    calls = {"n": 0}

    def _runner(prompt, lp, rp, options):
        calls["n"] += 1
        return _ok_runner(prompt, lp, rp, options)

    rep = _run(options={"render_crops": True}, vision_runner=_runner,
               crops_dir=None)
    assert calls["n"] == 0
    assert rep["status"] == "failed"
    for item in rep["items"]:
        assert item["vision_status"] == "failed"
        assert any("crops_dir not provided" in w for w in item["warnings"])


def test_10f_mixed_render_failure_is_completed_with_warnings(tmp_path):
    """1 render-fail + 1 успех → completed_with_warnings, не failed."""
    pytest.importorskip("cv2")
    lm, rm = _model("left"), _model("right")
    lm["blocks"]["L_S1"]["image_file"] = _png(tmp_path / "a.png")
    rm["blocks"]["R_S1"]["image_file"] = _png(tmp_path / "b.png")
    # S4: источники несуществующие → render fail
    gate = _gate([_gate_pair(1, "send_to_vision"),
                  _gate_pair(4, "send_to_vision")])
    rep = gv.run_graphic_vision_enrichment(
        lm, rm, gate, options={"render_crops": True},
        vision_runner=_ok_runner, crops_dir=tmp_path / "crops")
    statuses = sorted(i["vision_status"] for i in rep["items"])
    assert statuses == ["failed", "ok"]
    assert rep["status"] == "completed_with_warnings"


def test_15_include_exclude_from_vision_debug_path():
    """Killer ревью: debug-опция реально включает excluded пары — ПОСЛЕ
    send+manual, счётчик excluded отражает решение gate, не выборку."""
    selected, stats, _ = gv.select_blocks_for_vision(
        _DEFAULT_GATE, {"include_exclude_from_vision": True})
    keys = [p["pair_key"] for p in selected]
    assert "L_S3__R_S3" in keys
    assert keys.index("L_S3__R_S3") > keys.index("L_S1__R_S1")
    assert keys.index("L_S3__R_S3") > keys.index("L_S2__R_S2")
    assert stats["excluded_by_visual_gate"] == 1


def test_16_visual_metrics_passthrough_and_copy():
    """Killer ревью: metrics из gate доходят до item КОПИЕЙ; non-dict → None."""
    gate = _gate([_gate_pair(1, "send_to_vision")])
    rep = _run(gate)
    item = rep["items"][0]
    assert item["visual_metrics"] == {
        "mask_iou": 0.4, "normalized_correlation": 0.5,
        "total_diff_ratio": 0.2, "alignment_method": "ecc_euclidean"}
    # копия, не alias входного отчёта
    item["visual_metrics"]["mask_iou"] = 999
    assert gate["block_pairs"][0]["metrics"]["mask_iou"] == 0.4
    gate2 = _gate([{**_gate_pair(1, "send_to_vision"),
                    "metrics": "not-a-dict"}])
    assert _run(gate2)["items"][0]["visual_metrics"] is None


def test_17_graphic_type_discipline_fallback_left_to_right():
    """Killer ревью: left приоритетен; пустой/unknown left → right."""
    gate = _gate([_gate_pair(1, "send_to_vision"),
                  _gate_pair(2, "send_to_vision")])
    left_g = {"descriptors": [
        # S1: у left нет типа / unknown дисциплина → возьмём right
        {"block_id": "L_S1", "graphic_type": "unknown", "discipline": ""},
        # S2: у обеих сторон разные значения → побеждает left
        {"block_id": "L_S2", "graphic_type": "plan", "discipline": "AR"},
    ]}
    right_g = {"descriptors": [
        {"block_id": "R_S1", "graphic_type": "scheme", "discipline": "EOM"},
        {"block_id": "R_S2", "graphic_type": "table", "discipline": "OV"},
    ]}
    rep = gv.run_graphic_vision_enrichment(
        _model("left"), _model("right"), gate,
        left_graphic_report=left_g, right_graphic_report=right_g,
        options={"render_crops": False})
    by_id = {i["item_id"]: i for i in rep["items"]}
    assert by_id["gv_L_S1__R_S1"]["graphic_type"] == "scheme"
    assert by_id["gv_L_S1__R_S1"]["discipline"] == "EOM"
    assert by_id["gv_L_S2__R_S2"]["graphic_type"] == "plan"
    assert by_id["gv_L_S2__R_S2"]["discipline"] == "AR"


def test_18_selection_stats_closed_form():
    """Killer ревью: manual_review_skipped считается; included — по
    ФАКТИЧЕСКОЙ выборке после cap; max_items=0 = unlimited."""
    _, stats, _ = gv.select_blocks_for_vision(
        _DEFAULT_GATE, {"include_manual_review": False})
    assert stats["manual_review_skipped"] == 1
    assert stats["manual_review_included"] == 0
    # cap режет manual (send первыми) → included пересчитан по выборке
    gate = _gate([_gate_pair(1, "send_to_vision"),
                  _gate_pair(2, "send_to_vision"),
                  _gate_pair(4, "manual_review")])
    rep = _run(gate, options={"max_items": 2})
    assert rep["summary"]["selected_total"] == 2
    assert rep["summary"]["manual_review_included"] == 0
    assert rep["summary"]["dropped_by_cap"] == 1
    # max_items=0 → unlimited, без truncation warning
    rep0 = _run(gate, options={"max_items": 0})
    assert rep0["summary"]["selected_total"] == 3
    assert not any("max_items" in w for w in rep0["warnings"])


def test_19_runner_prompt_identical_regardless_of_write_prompts():
    """Killer ревью: write_prompts управляет только персистенцией —
    runner получает полный prompt (со статусом gate и листом)."""
    captured = {}

    def _runner(prompt, lp, rp, options):
        captured.setdefault("prompts", []).append(prompt)
        return _ok_runner(prompt, lp, rp, options)

    gate = _gate([_gate_pair(1, "send_to_vision")])
    _run(gate, options={"write_prompts": True}, vision_runner=_runner)
    _run(gate, options={"write_prompts": False}, vision_runner=_runner)
    p_on, p_off = captured["prompts"]
    assert p_on == p_off
    assert "changed_visual" in p_off       # вердикт gate, не 'unknown'
    assert "Лист 1" in p_off               # имя листа из модели


def test_20_str_list_keeps_falsy_scalars_and_caps():
    values, warnings = gv._str_list([0, False, "", None, "ok", "x" * 600])
    assert "0" in values and "False" in values and "ok" in values
    assert all(len(v) <= 501 for v in values)
    assert any("truncated" in w for w in warnings)
    long_list, warnings2 = gv._str_list([f"v{i}" for i in range(80)])
    assert len(long_list) == 50
    assert any("list truncated" in w for w in warnings2)


# ─── candidate selection v2 (entity-aware) ───────────────────────────────────


def _desc(block_id, sheet_name, *, gt="single_line_scheme", disc="EOM",
          equipment=None, raw=None):
    return {"block_id": block_id, "sheet_name": sheet_name,
            "graphic_type": gt, "discipline": disc,
            "tokens": {"equipment": equipment or [],
                       "raw_key_entities": raw or []}}


def _matched(lid, rid, *, quality="medium", gt_match=True, disc_match=True,
             eq_overlap=0.0, risks=None):
    return {"left_block_id": lid, "right_block_id": rid,
            "match_quality": quality, "graphic_type_match": gt_match,
            "discipline_match": disc_match,
            "token_overlap": {"equipment": eq_overlap},
            "risk_flags": risks or []}


# Реконструкция пилотных пар ИОС1.1 (синтетика с реальными сигналами)
_PILOT_DESCS_L = {
    "L_GRSH": _desc("L_GRSH", "Однолинейная расчетная схема ГРЩ",
                    gt="cabinet_scheme",
                    equipment=["грщ", "вру", "QF1"], raw=["1600А", "2500А"]),
    "L_VRU1": _desc("L_VRU1", "Однолинейная расчетная схема ВРУ-1",
                    equipment=["грщ", "вру"]),
    "L_VRU3": _desc("L_VRU3", "Однолинейная расчетная схема ВРУ-3",
                    gt="cabinet_scheme", equipment=["вру", "qs"],
                    raw=["ВРУ-3"]),
    "L_YAK": _desc("L_YAK", "Однолинейная расчетная схема щита квартирного ЯК5",
                   equipment=[], raw=["ЯК1"]),
}
_PILOT_DESCS_R = {
    "R_GRSH": _desc("R_GRSH", "Однолинейная схема ГРЩ",
                    equipment=["ГРЩ1", "ВРУ1", "ВРУ2"], raw=["3200А"]),
    "R_PLAN": _desc("R_PLAN", "План расположения помещений ТП на -1 этаже",
                    disc="SOT", equipment=["ГРЩ"]),
    # bare 'вру'/'грщ'/'авр' — как в РЕАЛЬНЫХ дескрипторах pf06effb7
    # (critical-находка: generic-токен не должен маскировать конфликт)
    "R_VRU2": _desc("R_VRU2", "Однолинейная схема ВРУ-2",
                    equipment=["ВРУ2-РП1", "QF1", "вру", "грщ", "авр"]),
    "R_SHO3": _desc("R_SHO3", "Однолинейная схема ЩО-3",
                    gt="cabinet_scheme", equipment=["qs", "qf1"]),
}


def _pilot_ctx():
    gate = _gate([
        {**_gate_pair(1, "send_to_vision"), "pair_key": "L_GRSH__R_GRSH",
         "left_block_id": "L_GRSH", "right_block_id": "R_GRSH"},
        {**_gate_pair(2, "send_to_vision"), "pair_key": "L_VRU1__R_PLAN",
         "left_block_id": "L_VRU1", "right_block_id": "R_PLAN"},
        {**_gate_pair(3, "send_to_vision"), "pair_key": "L_VRU3__R_VRU2",
         "left_block_id": "L_VRU3", "right_block_id": "R_VRU2"},
        {**_gate_pair(4, "send_to_vision"), "pair_key": "L_YAK__R_SHO3",
         "left_block_id": "L_YAK", "right_block_id": "R_SHO3"},
    ])
    lg = {"descriptors": list(_PILOT_DESCS_L.values())}
    rg = {"descriptors": list(_PILOT_DESCS_R.values())}
    matched = {"matched_graphic_blocks": [
        _matched("L_GRSH", "R_GRSH", quality="strong", gt_match=False,
                 eq_overlap=0.33),
        _matched("L_VRU1", "R_PLAN", gt_match=True, disc_match=False,
                 eq_overlap=0.5),
        _matched("L_VRU3", "R_VRU2", quality="weak", gt_match=False,
                 eq_overlap=0.07),
        _matched("L_YAK", "R_SHO3", gt_match=False, eq_overlap=0.0),
    ]}
    return gate, lg, rg, matched


def _score_pair(key):
    gate, lg, rg, matched = _pilot_ctx()
    bp = next(p for p in gate["block_pairs"] if p["pair_key"] == key)
    lid, rid = bp["left_block_id"], bp["right_block_id"]
    return gv.score_vision_candidate(
        bp,
        left_desc=next(d for d in lg["descriptors"] if d["block_id"] == lid),
        right_desc=next(d for d in rg["descriptors"] if d["block_id"] == rid),
        matched_entry=next(m for m in matched["matched_graphic_blocks"]
                           if m["left_block_id"] == lid))


def test_v2_same_entity_signals_raise_score():
    """spec 1: совпадение entity/листа/типа/дисциплины повышает score."""
    good = _score_pair("L_GRSH__R_GRSH")
    bad = _score_pair("L_YAK__R_SHO3")
    assert good["candidate_score"] > bad["candidate_score"]
    assert good["candidate_kind"] == gv.CANDIDATE_SAME
    # ГРЩ↔ГРЩ — bare-family совпадение (слабое, но положительное)
    assert "entity_family_match" in good["candidate_reasons"]
    assert "sheet_kind_match:scheme" in good["candidate_reasons"]


def test_v2_graphic_type_and_discipline_mismatch_lower_score():
    """spec 2-3: mismatch-флаги понижают score и попадают в risk_flags."""
    base = {**_gate_pair(1, "send_to_vision")}
    d1 = _desc("a", "Схема X")
    same = gv.score_vision_candidate(
        base, left_desc=d1, right_desc=_desc("b", "Схема X"),
        matched_entry=_matched("a", "b", gt_match=True, disc_match=True))
    worse = gv.score_vision_candidate(
        base, left_desc=d1, right_desc=_desc("b", "Схема X"),
        matched_entry=_matched("a", "b", gt_match=False, disc_match=False))
    assert worse["candidate_score"] < same["candidate_score"]
    assert "graphic_type_mismatch" in worse["candidate_risk_flags"]
    assert "discipline_mismatch" in worse["candidate_risk_flags"]


def test_v2_pilot_false_pairs_classified():
    """spec 4-6: пилотные false-пары распознаются."""
    vru1_plan = _score_pair("L_VRU1__R_PLAN")
    assert vru1_plan["candidate_kind"] == gv.CANDIDATE_MISMATCH
    assert any(r.startswith("sheet_kind_mismatch")
               for r in vru1_plan["candidate_risk_flags"])

    vru3_vru2 = _score_pair("L_VRU3__R_VRU2")
    assert vru3_vru2["candidate_kind"] in (gv.CANDIDATE_MISMATCH,
                                           gv.CANDIDATE_VALIDATION)
    assert "entity_id_conflict" in vru3_vru2["candidate_risk_flags"]

    yak_sho = _score_pair("L_YAK__R_SHO3")
    assert yak_sho["candidate_kind"] == gv.CANDIDATE_MISMATCH
    assert any("conflict" in r for r in yak_sho["candidate_risk_flags"])


def test_v2_enrichment_mode_excludes_mismatch():
    """spec 7: enrichment не берёт mismatch_likely при наличии хороших."""
    gate, lg, rg, matched = _pilot_ctx()
    selected, stats, _ = gv.select_vision_candidates_v2(
        gate, left_graphic_report=lg, right_graphic_report=rg,
        graphic_matched_report=matched,
        options={"selection_mode": "enrichment", "max_items": 5})
    keys = [p["pair_key"] for p in selected]
    assert "L_GRSH__R_GRSH" in keys
    assert "L_VRU1__R_PLAN" not in keys
    assert "L_YAK__R_SHO3" not in keys
    assert stats["mismatch_excluded"] >= 2
    assert stats["by_candidate_kind"].get(gv.CANDIDATE_SAME, 0) >= 1
    # GRSH первым (лучший score)
    assert keys[0] == "L_GRSH__R_GRSH"


def test_v2_link_validation_mode_picks_mismatch_first():
    """spec 8: link_validation целенаправленно берёт подозрительные пары."""
    gate, lg, rg, matched = _pilot_ctx()
    selected, stats, _ = gv.select_vision_candidates_v2(
        gate, left_graphic_report=lg, right_graphic_report=rg,
        graphic_matched_report=matched,
        options={"selection_mode": "link_validation", "max_items": 2})
    kinds = [p["candidate_kind"] for p in selected]
    assert all(k in (gv.CANDIDATE_MISMATCH, gv.CANDIDATE_VALIDATION)
               for k in kinds)
    assert "L_GRSH__R_GRSH" not in [p["pair_key"] for p in selected]


def test_v2_max_items_after_scoring():
    """spec 9: cap применяется ПОСЛЕ ранжирования (лучшие выживают)."""
    gate, lg, rg, matched = _pilot_ctx()
    selected, stats, _ = gv.select_vision_candidates_v2(
        gate, left_graphic_report=lg, right_graphic_report=rg,
        graphic_matched_report=matched,
        options={"selection_mode": "enrichment", "max_items": 1})
    assert len(selected) == 1
    assert selected[0]["pair_key"] == "L_GRSH__R_GRSH"
    assert selected[0]["candidate_rank"] == 1
    # link_validation: 3 подозрительных кандидата, cap=1 → честный warning
    sel2, stats2, warnings2 = gv.select_vision_candidates_v2(
        gate, left_graphic_report=lg, right_graphic_report=rg,
        graphic_matched_report=matched,
        options={"selection_mode": "link_validation", "max_items": 1})
    assert len(sel2) == 1
    assert stats2["dropped_by_cap"] >= 2
    assert any("max_items" in w for w in warnings2)


def test_v2_reasons_and_risks_populated_in_items():
    """spec 10-11: candidate_* поля доходят до items отчёта."""
    gate, lg, rg, matched = _pilot_ctx()
    rep = gv.run_graphic_vision_enrichment(
        _model("left"), _model("right"), gate,
        left_graphic_report=lg, right_graphic_report=rg,
        graphic_matched_report=matched,
        options={"candidate_selection": "entity_aware", "max_items": 5,
                 "render_crops": False})
    assert rep["summary"]["candidate_selection"] == "entity_aware"
    assert rep["summary"]["by_candidate_kind"]
    for item in rep["items"]:
        assert isinstance(item.get("candidate_score"), float)
        assert item.get("candidate_kind")
        assert isinstance(item.get("candidate_reasons"), list)
        assert isinstance(item.get("candidate_risk_flags"), list)
        assert isinstance(item.get("candidate_rank"), int)


def test_v2_legacy_selection_unchanged_by_default():
    """spec 13/14: default candidate_selection=legacy — поведение прежнее."""
    rep = _run()   # _DEFAULT_GATE, legacy
    assert rep["summary"]["candidate_selection"] == "legacy"
    for item in rep["items"]:
        assert "candidate_score" not in item


def test_v2_entity_helpers():
    ids = gv.extract_entity_ids("Однолинейная расчетная схема ВРУ-1",
                                ["грщ", "ЩО-3"], ["ЯК1"])
    assert "ВРУ-1" in ids and "ГРЩ" in ids and "ЩО-3" in ids and "ЯК-1" in ids
    # точное нумерованное совпадение
    assert gv.entity_identity_signal({"ВРУ-2"}, {"ВРУ-2", "ГРЩ"}) == "match"
    # bare-family совпадение — слабый сигнал, НЕ полноценный match
    assert gv.entity_identity_signal({"ГРЩ"}, {"ГРЩ", "ВРУ-1"}) == \
        "family_only_match"
    assert gv.entity_identity_signal({"ВРУ-3"}, {"ВРУ-2"}) == "numbered_conflict"
    # critical-регрессия ревью: generic 'вру' на обеих сторонах НЕ маскирует
    # конфликт номеров (реальные дескрипторы pf06effb7)
    assert gv.entity_identity_signal({"ВРУ", "ВРУ-3"},
                                     {"ВРУ", "ВРУ-2"}) == "numbered_conflict"
    assert gv.entity_identity_signal({"ЯК-5"}, {"ЩО-3"}) == "family_conflict"
    assert gv.entity_identity_signal(set(), {"ЩО-3"}) == "none"
    assert gv.sheet_kind_of("План расположения помещений ТП") == "plan"
    assert gv.sheet_kind_of("Однолинейная расчетная схема ВРУ-1") == "scheme"
    assert gv.sheet_kind_of("Спецификация оборудования") == "table"
    # комбинированное имя: РАННИЙ маркер решает (nit ревью)
    assert gv.sheet_kind_of("План ТП и схема вентиляции") == "plan"


def test_v2_entity_extraction_hardening():
    """Ревью-фиксы regex: рейтинги, letter-суффиксы, false-positive слова,
    feeder leak, все families."""
    # ампер-рейтинг через пробел — НЕ номер единицы
    assert gv.extract_entity_ids("АВР 100А") == {"АВР"}
    # letter-суффиксные серии различимы
    assert gv.extract_entity_ids("ЩР-ТХ1") == {"ЩР-тх1"}
    assert gv.extract_entity_ids("ЩР-ТХ2") == {"ЩР-тх2"}
    assert gv.entity_identity_signal({"ЩР-тх1"}, {"ЩР-тх2"}) == \
        "numbered_conflict"
    assert gv.extract_entity_ids("ЩС-ДР") == {"ЩС-др"}
    assert gv.extract_entity_ids("ШУ-В1ас") == {"ШУ-в1ас"}
    # «ВРУ-А» и «ВРУа» — одна серия
    assert gv.extract_entity_ids("ВРУ-А") == gv.extract_entity_ids("ВРУа") \
        == {"ВРУ-а"}
    # обычные слова не дают сущностей
    for word in ("вручную", "шум", "якорь", "щуп", "ТПУ", "Якорь", "врут"):
        assert gv.extract_entity_ids(word) == set(), word
    # feeder leak: назначение не попадает в идентичность листа
    assert gv.extract_entity_ids("ВРУ2-РП1") == {"ВРУ-2"}
    # суффикс нормализуется по регистру; запятая-вольтаж намеренно
    # отсекается (consistent со stamp_matching: «0,4кВ» — не номер)
    assert gv.extract_entity_ids("ЩР-1А") == {"ЩР-1а"}
    assert gv.extract_entity_ids("РУСН-0,4") == {"РУСН-0"}
    # все объявленные families извлекаются
    for fam in gv._ENTITY_FAMILIES:
        got = gv.extract_entity_ids(f"{fam}-7")
        assert f"{fam}-7" in got or got == {f"{fam}-7"}, fam


def _kinds_ctx():
    """Gate со всеми четырьмя candidate kinds (killer-тесты ревью)."""
    descs_l = {
        "L_A": _desc("L_A", "Однолинейная схема ВРУ-1"),
        "L_B": _desc("L_B", ""),
        "L_C": _desc("L_C", ""),
        "L_D": _desc("L_D", "Однолинейная схема щита квартирного ЯК5"),
    }
    descs_r = {
        "R_A": _desc("R_A", "Однолинейная схема ВРУ-1"),
        "R_B": _desc("R_B", ""),
        "R_C": _desc("R_C", ""),
        "R_D": _desc("R_D", "Однолинейная схема ЩО-3"),
    }
    pairs = []
    # SAME: общий numbered entity
    pairs.append({**_gate_pair(1, "send_to_vision"), "pair_key": "A",
                  "left_block_id": "L_A", "right_block_id": "R_A"})
    # UNCERTAIN: пустые дескрипторы, без gate-бонусов и метрик
    pairs.append({**_gate_pair(2, "manual_review", status="uncertain"),
                  "pair_key": "B", "left_block_id": "L_B",
                  "right_block_id": "R_B", "metrics": {}})
    # VALIDATION: риски без прямого конфликта
    pairs.append({**_gate_pair(3, "manual_review", status="uncertain"),
                  "pair_key": "C", "left_block_id": "L_C",
                  "right_block_id": "R_C", "metrics": {}})
    # MISMATCH: family conflict
    pairs.append({**_gate_pair(4, "send_to_vision"), "pair_key": "D",
                  "left_block_id": "L_D", "right_block_id": "R_D"})
    gate = _gate(pairs)
    matched = {"matched_graphic_blocks": [
        _matched("L_A", "R_A", quality="strong", eq_overlap=0.5),
        _matched("L_C", "R_C", quality="weak", gt_match=False),
    ]}
    return gate, {"descriptors": list(descs_l.values())}, \
        {"descriptors": list(descs_r.values())}, matched


def test_v2_killer_backfill_order_and_link_validation_order():
    """Killer ревью: enrichment backfill same→uncertain→validation;
    link_validation: mismatch→validation→uncertain→same."""
    gate, lg, rg, matched = _kinds_ctx()
    sel, _, _ = gv.select_vision_candidates_v2(
        gate, left_graphic_report=lg, right_graphic_report=rg,
        graphic_matched_report=matched,
        options={"selection_mode": "enrichment", "max_items": 3})
    assert [c["candidate_kind"] for c in sel] == [
        gv.CANDIDATE_SAME, gv.CANDIDATE_UNCERTAIN, gv.CANDIDATE_VALIDATION]
    sel2, _, _ = gv.select_vision_candidates_v2(
        gate, left_graphic_report=lg, right_graphic_report=rg,
        graphic_matched_report=matched,
        options={"selection_mode": "link_validation", "max_items": 4})
    assert [c["candidate_kind"] for c in sel2] == [
        gv.CANDIDATE_MISMATCH, gv.CANDIDATE_VALIDATION,
        gv.CANDIDATE_UNCERTAIN, gv.CANDIDATE_SAME]


def test_v2_killer_defaults_and_optout():
    """Killer ревью: default selection_mode=enrichment; opt-out
    exclude_mismatch_likely=False оставляет mismatch последними."""
    gate, lg, rg, matched = _kinds_ctx()
    sel, stats, _ = gv.select_vision_candidates_v2(
        gate, left_graphic_report=lg, right_graphic_report=rg,
        graphic_matched_report=matched, options={"max_items": 5})
    assert stats["selection_mode"] == "enrichment"
    assert stats["mismatch_excluded"] == 1
    assert all(c["candidate_kind"] != gv.CANDIDATE_MISMATCH for c in sel)
    assert sel[0]["candidate_kind"] == gv.CANDIDATE_SAME
    sel2, stats2, _ = gv.select_vision_candidates_v2(
        gate, left_graphic_report=lg, right_graphic_report=rg,
        graphic_matched_report=matched,
        options={"exclude_mismatch_likely": False, "max_items": 0})
    assert stats2["mismatch_excluded"] == 0
    kinds2 = [c["candidate_kind"] for c in sel2]
    assert kinds2[-1] == gv.CANDIDATE_MISMATCH
    # неизвестный режим → warning + enrichment
    _, stats3, w3 = gv.select_vision_candidates_v2(
        gate, left_graphic_report=lg, right_graphic_report=rg,
        graphic_matched_report=matched,
        options={"selection_mode": "frobnicate"})
    assert stats3["selection_mode"] == "enrichment"
    assert any("unknown selection_mode" in w for w in w3)


def test_v2_killer_score_weights_and_clamp():
    """Killer ревью: вес entity match (+0.2) и clamp 0..1 закреплены."""
    base = {"decision": "manual_review", "status": "uncertain",
            "risk_flags": [], "metrics": {}}
    with_match = gv.score_vision_candidate(
        base, left_desc=_desc("a", "Схема ВРУ-1"),
        right_desc=_desc("b", "Схема ВРУ-1"))
    without = gv.score_vision_candidate(
        base, left_desc=_desc("a", "Схема"), right_desc=_desc("b", "Схема"))
    assert with_match["candidate_score"] - without["candidate_score"] == \
        pytest.approx(0.2)
    # clamp: максимум сигналов → ровно 1.0
    maxed = gv.score_vision_candidate(
        {**_gate_pair(1, "send_to_vision")},
        left_desc=_desc("a", "Однолинейная схема ВРУ-1",
                        equipment=["ВРУ-1", "QF1"]),
        right_desc=_desc("b", "Однолинейная схема ВРУ-1",
                         equipment=["ВРУ-1", "QF1"]),
        matched_entry=_matched("a", "b", quality="strong", eq_overlap=0.9))
    assert maxed["candidate_score"] == 1.0


def test_v2_killer_high_score_numbered_conflict_never_same():
    """Killer ревью: numbered conflict с сильными прочими сигналами всё
    равно MISMATCH (hard guard перебивает score)."""
    verdict = gv.score_vision_candidate(
        {**_gate_pair(1, "send_to_vision")},
        left_desc=_desc("a", "Однолинейная схема ВРУ-3",
                        equipment=["вру", "QF1"]),
        right_desc=_desc("b", "Однолинейная схема ВРУ-2",
                         equipment=["вру", "QF1"]),
        matched_entry=_matched("a", "b", quality="strong", eq_overlap=0.5))
    assert "entity_id_conflict" in verdict["candidate_risk_flags"]
    assert verdict["candidate_kind"] == gv.CANDIDATE_MISMATCH


def test_v2_killer_exact_kind_counts_and_stamp_magnitude():
    """Killer ревью: by_candidate_kind аккумулируется точно; величина
    stamp-штрафа 0.35 закреплена в uncapped-области."""
    gate, lg, rg, matched = _pilot_ctx()
    _, stats, _ = gv.select_vision_candidates_v2(
        gate, left_graphic_report=lg, right_graphic_report=rg,
        graphic_matched_report=matched, options={"max_items": 5})
    assert stats["by_candidate_kind"] == {gv.CANDIDATE_SAME: 1,
                                          gv.CANDIDATE_MISMATCH: 3}
    base = {"decision": "manual_review", "status": "uncertain",
            "risk_flags": [], "metrics": {}}
    scheme = gv.score_vision_candidate(
        base, left_desc=_desc("a", "Схема ВРУ-1"),
        right_desc=_desc("b", "Схема ВРУ-1"))
    stamp = gv.score_vision_candidate(
        base, left_desc=_desc("a", "Схема ВРУ-1", gt="stamp"),
        right_desc=_desc("b", "Схема ВРУ-1", gt="stamp"))
    assert scheme["candidate_score"] - stamp["candidate_score"] == \
        pytest.approx(0.35)


def test_v2_killer_generic_equipment_overlap_not_rewarded():
    """Killer ревью: overlap=1.0 на ['вру']↔['вру'] бессодержателен."""
    base = {**_gate_pair(1, "send_to_vision"), "metrics": {}}
    verdict = gv.score_vision_candidate(
        base,
        left_desc=_desc("a", "", equipment=["вру"]),
        right_desc=_desc("b", "", equipment=["вру"]),
        matched_entry=_matched("a", "b", eq_overlap=1.0))
    assert not any(r.startswith("equipment_overlap")
                   for r in verdict["candidate_reasons"])
    # информативные токены — бонус есть
    verdict2 = gv.score_vision_candidate(
        base,
        left_desc=_desc("a", "", equipment=["ВРУ-1", "QF1"]),
        right_desc=_desc("b", "", equipment=["ВРУ-1", "QF1"]),
        matched_entry=_matched("a", "b", eq_overlap=1.0))
    assert any(r.startswith("equipment_overlap")
               for r in verdict2["candidate_reasons"])


@pytest.mark.parametrize("gtype", sorted(gv._DENSE_GRAPHIC_TYPES))
def test_v2_killer_dense_types_each_get_dense_long_side(gtype):
    opts = {"mode": "high_res", "long_side": 1000, "dense_long_side": 2400}
    assert gv._item_render_long_side({"graphic_type": gtype}, opts) == 2400


@pytest.mark.parametrize("gtype", ["photo", "plan", "", None, "stamp"])
def test_v2_killer_non_dense_keeps_base_in_high_res(gtype):
    opts = {"mode": "high_res", "long_side": 1000, "dense_long_side": 2400}
    assert gv._item_render_long_side({"graphic_type": gtype}, opts) == 1000


# ─── render options (spec 12) ────────────────────────────────────────────────


def test_render_high_res_uses_dense_long_side_for_dense_types(tmp_path):
    pytest.importorskip("cv2")
    lm, rm = _model("left"), _model("right")
    lm["blocks"]["L_S1"]["image_file"] = _png(tmp_path / "l.png")
    rm["blocks"]["R_S1"]["image_file"] = _png(tmp_path / "r.png")
    gate = _gate([_gate_pair(1, "send_to_vision")])
    lg = {"descriptors": [
        {"block_id": "L_S1", "graphic_type": "cabinet_scheme",
         "discipline": "EOM"}]}
    rep = gv.run_graphic_vision_enrichment(
        lm, rm, gate, left_graphic_report=lg,
        options={"render_crops": True,
                 "render": {"mode": "high_res", "long_side": 1000,
                            "dense_long_side": 2000}},
        vision_runner=_ok_runner, crops_dir=tmp_path / "crops")
    item = rep["items"][0]
    assert item["graphic_type"] == "cabinet_scheme"
    assert item["render_long_side_used"] == 2000
    assert rep["summary"]["render_mode"] == "high_res"
    assert item["vision_status"] == "ok"


def test_render_normal_mode_keeps_base_long_side(tmp_path):
    pytest.importorskip("cv2")
    lm, rm = _model("left"), _model("right")
    lm["blocks"]["L_S1"]["image_file"] = _png(tmp_path / "l.png")
    rm["blocks"]["R_S1"]["image_file"] = _png(tmp_path / "r.png")
    gate = _gate([_gate_pair(1, "send_to_vision")])
    rep = gv.run_graphic_vision_enrichment(
        lm, rm, gate,
        options={"render_crops": True,
                 "render": {"mode": "normal", "long_side": 1234,
                            "dense_long_side": 2000}},
        vision_runner=_ok_runner, crops_dir=tmp_path / "crops")
    assert rep["items"][0]["render_long_side_used"] == 1234


def test_render_tiled_non_dense_degrades_to_high_res(tmp_path):
    """tiled имеет смысл только для плотных типов; не-dense item эффективно
    high_res (без плиток), normal/high_res путь не ломается."""
    pytest.importorskip("cv2")
    lm, rm = _model("left"), _model("right")
    lm["blocks"]["L_S1"]["image_file"] = _png(tmp_path / "l.png")
    rm["blocks"]["R_S1"]["image_file"] = _png(tmp_path / "r.png")
    gate = _gate([_gate_pair(1, "send_to_vision")])
    lg = {"descriptors": [
        {"block_id": "L_S1", "graphic_type": "scheme",
         "discipline": "EOM"}]}   # НЕ в _DENSE_GRAPHIC_TYPES
    rep = gv.run_graphic_vision_enrichment(
        lm, rm, gate, left_graphic_report=lg,
        options={"render_crops": True,
                 "render": {"mode": "tiled", "dense_long_side": 2100}},
        vision_runner=_ok_runner, crops_dir=tmp_path / "crops")
    item = rep["items"][0]
    # не-dense → высокого-разрешения один рендер, без плиток
    assert "render" not in item or not item.get("render")
    assert item["render_long_side_used"] == 1600  # base (тип не в dense set)
    assert item["vision_status"] == "ok"


def test_render_legacy_render_long_side_still_works():
    opts, warnings = gv._render_options({"render_long_side": 1777})
    assert opts["long_side"] == 1777
    assert opts["mode"] == "normal"
    assert warnings == []
    # явный приоритет: render.long_side перебивает legacy-ключ
    opts2, _ = gv._render_options({"render_long_side": 1777,
                                   "render": {"long_side": 1234}})
    assert opts2["long_side"] == 1234
    # невалидный mode → normal + warning (не молча)
    opts3, w3 = gv._render_options({"render": {"mode": "hi_res"}})
    assert opts3["mode"] == "normal"
    assert any("invalid render mode" in w for w in w3)
    # tiled теперь реализован — режим сохраняется, без warning/fallback
    opts4, w4 = gv._render_options({"render": {"mode": "tiled"}})
    assert opts4["mode"] == "tiled"
    assert opts4["mode_requested"] == "tiled"
    assert not any("tiled" in w for w in w4)
    # tiled-специфичные опции читаются (float/bool)
    opts5, _ = gv._render_options(
        {"render": {"mode": "tiled", "tile_long_side": 1500,
                    "max_tiles": 4, "tile_overlap": 0.2,
                    "include_full_image": False}})
    assert opts5["tile_long_side"] == 1500
    assert opts5["max_tiles"] == 4
    assert opts5["tile_overlap"] == 0.2
    assert opts5["include_full_image"] is False


# ─── dry-run интеграция (11-12) ──────────────────────────────────────────────


def _result_json(tmp_path: Path, name: str) -> Path:
    payload = {
        "pages": [
            {"page_number": 1, "width": 1000, "height": 700, "blocks": [
                {"block_id": f"{name}_b1", "block_type": "image",
                 "coords_norm": [0.1, 0.1, 0.9, 0.9],
                 "ocr_text": "ЩР-1 кабель ВВГнг 5x10"},
            ]},
        ],
    }
    p = tmp_path / f"{name}_result.json"
    p.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return p


def _run_dry(tmp_path, options=None, vision_runner=None):
    from backend.app.services.stage_comparison.pipeline_v2_dry_run import (
        run_pipeline_v2_dry_run,
    )
    left = _result_json(tmp_path, "left")
    right = _result_json(tmp_path, "right")
    out = tmp_path / "out"
    summary = run_pipeline_v2_dry_run(
        {"result_json_path": str(left)}, {"result_json_path": str(right)},
        out, options=options, vision_runner=vision_runner)
    return summary, out


def test_11_dry_run_writes_graphic_vision_report(tmp_path):
    summary, out = _run_dry(
        tmp_path,
        options={"graphic_vision": {"enabled": True, "render_crops": False}},
        vision_runner=_ok_runner)
    rep_path = out / "graphic_vision_enrichment_report.json"
    assert rep_path.is_file()
    rep = json.loads(rep_path.read_text(encoding="utf-8"))
    assert rep["kind"] == gv.REPORT_KIND
    sec = summary["graphic_vision"]
    assert sec["enabled"] is True
    assert sec["status"] == rep["status"]
    # артефакт в манифесте
    manifest = json.loads((out / "pipeline_v2_manifest.json")
                          .read_text(encoding="utf-8"))
    assert "graphic_vision_enrichment_report.json" in json.dumps(manifest)
    # MD-раздел
    md = (out / "pipeline_v2_summary.md").read_text(encoding="utf-8")
    assert "Graphic vision enrichment" in md


def test_11b_dry_run_disabled_by_default(tmp_path):
    summary, out = _run_dry(tmp_path)
    assert summary["graphic_vision"]["enabled"] is False
    assert summary["graphic_vision"]["status"] == "disabled"
    assert not (out / "graphic_vision_enrichment_report.json").exists()


def test_12_dry_run_fail_soft_on_enrichment_error(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import pipeline_v2_dry_run as dr

    def _boom(*a, **k):
        raise RuntimeError("gv exploded")

    monkeypatch.setattr(dr, "run_graphic_vision_enrichment", _boom)
    summary, out = _run_dry(
        tmp_path, options={"graphic_vision": {"enabled": True}})
    assert summary["status"] in ("ok", "completed_with_warnings")
    assert summary["graphic_vision"]["status"] == "failed"
    assert "gv exploded" in summary["graphic_vision"]["error"]
    assert any("graphic_vision" in w for w in summary["warnings"])
    # downstream этапы не пострадали
    assert (out / "entity_diff_report.json").is_file()


def test_12b_dry_run_deterministic_deltas_invariant(tmp_path):
    base_summary, _ = _run_dry(tmp_path)
    gv_summary, _ = _run_dry(
        tmp_path,
        options={"graphic_vision": {"enabled": True, "render_crops": False}},
        vision_runner=_ok_runner)
    assert (base_summary["stages"]["entity_diff"]
            == gv_summary["stages"]["entity_diff"])


# ─── 13: ui_payload ──────────────────────────────────────────────────────────


def test_13_ui_payload_graphic_vision_section(tmp_path):
    from backend.app.services.stage_comparison.pipeline_v2_ui_payload import (
        build_pipeline_v2_ui_payload,
    )
    summary, _ = _run_dry(
        tmp_path,
        options={"graphic_vision": {"enabled": True, "render_crops": False}},
        vision_runner=_ok_runner)
    payload = build_pipeline_v2_ui_payload(summary, None, None)
    sec = payload.get("graphic_vision")
    assert isinstance(sec, dict)
    assert sec["available"] is True
    assert "selected_total" in sec
    assert "vision_calls_succeeded" in sec
    assert "vision_calls_failed" in sec
    assert "skipped_no_runner" in sec


def test_13b_ui_payload_backward_compatible_without_section(tmp_path):
    from backend.app.services.stage_comparison.pipeline_v2_ui_payload import (
        build_pipeline_v2_ui_payload,
    )
    summary, _ = _run_dry(tmp_path)  # gv disabled
    payload = build_pipeline_v2_ui_payload(summary, None, None)
    assert "graphic_vision" not in payload
    # старый summary вообще без секции — тоже совместим
    summary.pop("graphic_vision", None)
    payload2 = build_pipeline_v2_ui_payload(summary, None, None)
    assert "graphic_vision" not in payload2


# ─── 14: офлайн-гарантии ─────────────────────────────────────────────────────


def test_14_no_vision_or_llm_imports():
    src = Path(gv.__file__).read_text(encoding="utf-8").lower()
    for forbidden in ("qwen", "gemma", "opus", "claude", "llm_runner",
                      "graphic_llm", "text_llm", "lmstudio", "subprocess",
                      "httpx", "requests", "urllib"):
        assert forbidden not in src, f"module references {forbidden!r}"


def test_14b_no_network_during_run(monkeypatch):
    import socket

    def _boom(*a, **k):  # pragma: no cover
        raise AssertionError("network access attempted")

    monkeypatch.setattr(socket, "socket", _boom)
    monkeypatch.setattr(socket, "create_connection", _boom)
    rep = _run(vision_runner=_ok_runner)
    assert rep["status"] == "ok"


def test_v2_stamp_blocks_deprioritized_for_enrichment():
    """Штамп — та же сущность, но для vision-enrichment инженерная графика
    приоритетнее (дельты штампа ловит текстовый слой)."""
    base = {**_gate_pair(1, "send_to_vision")}
    scheme = gv.score_vision_candidate(
        base,
        left_desc=_desc("a", "Однолинейная схема ВРУ-1"),
        right_desc=_desc("b", "Однолинейная схема ВРУ-1"),
        matched_entry=_matched("a", "b", eq_overlap=0.4))
    stamp = gv.score_vision_candidate(
        base,
        left_desc=_desc("a", "Однолинейная схема ВРУ-1", gt="stamp"),
        right_desc=_desc("b", "Однолинейная схема ВРУ-1", gt="stamp"),
        matched_entry=_matched("a", "b", eq_overlap=0.4))
    assert stamp["candidate_score"] < scheme["candidate_score"]
    assert "stamp_block_low_vision_value" in stamp["candidate_risk_flags"]


# ─── legend/domain hardening (Section 2) ─────────────────────────────────────


def test_legend_one_side_not_same_by_family_alone():
    """spec 2.1: legend на одной стороне НЕ даёт same_entity_likely только
    по семейству/листу — нужна сильная идентичность (pilot v2: 7VMV)."""
    base = {**_gate_pair(1, "send_to_vision")}
    # одинаковое семейство схем, но правый — легенда/условные обозначения
    res = gv.score_vision_candidate(
        base,
        left_desc=_desc("a", "Условные обозначения", gt="legend",
                        equipment=["вру"]),
        right_desc=_desc("b", "Условные обозначения", gt="legend",
                         equipment=["вру"]),
        matched_entry=_matched("a", "b", gt_match=True, disc_match=True,
                               eq_overlap=0.3))
    assert res["candidate_kind"] != gv.CANDIDATE_SAME
    assert res["candidate_kind"] in (gv.CANDIDATE_VALIDATION,
                                     gv.CANDIDATE_MISMATCH)


def test_legend_with_strong_identity_can_be_same():
    """legend сам по себе не запрещает SAME — но требует СИЛЬНОЙ идентичности
    (numbered match), а не одного семейства."""
    base = {**_gate_pair(1, "send_to_vision")}
    res = gv.score_vision_candidate(
        base,
        left_desc=_desc("a", "Условные обозначения ЩО-1", gt="legend",
                        equipment=["що-1"], raw=["ЩО-1"]),
        right_desc=_desc("b", "Условные обозначения ЩО-1", gt="legend",
                         equipment=["що-1"], raw=["ЩО-1"]),
        matched_entry=_matched("a", "b", gt_match=True, disc_match=True,
                               eq_overlap=0.6))
    # numbered identity match → legend не понижает до validation
    if "entity_id_match" in res["candidate_reasons"]:
        assert res["candidate_kind"] == gv.CANDIDATE_SAME


def test_domain_mismatch_downgrades_to_validation():
    """spec 2.2: разные инженерные ДОМЕНЫ (при совпадении дисциплины) →
    domain_mismatch risk → НЕ same_entity_likely."""
    base = {**_gate_pair(1, "send_to_vision")}
    res = gv.score_vision_candidate(
        base,
        left_desc=_desc("a", "Схема ОЗДС охранная (БВУ/БПИ)",
                        equipment=["бву"], raw=["ОЗДС"]),
        right_desc=_desc("b", "Схема освещения квартир",
                         equipment=["светильник"], raw=["освещение"]),
        matched_entry=_matched("a", "b", gt_match=True, disc_match=True,
                               eq_overlap=0.0))
    assert "domain_mismatch" in res["candidate_risk_flags"]
    assert res["candidate_kind"] in (gv.CANDIDATE_VALIDATION,
                                     gv.CANDIDATE_MISMATCH)


def test_genuine_grsh_pair_with_type_jitter_stays_same():
    """Регрессия: cabinet vs single_line (vision-jitter) одной ГРЩ НЕ должен
    падать из SAME — graphic_type_mismatch это СЛАБЫЙ сигнал (pilot v2: 7EMD)."""
    base = {**_gate_pair(1, "send_to_vision")}
    res = gv.score_vision_candidate(
        base,
        left_desc=_desc("a", "Однолинейная расчетная схема ГРЩ",
                        gt="cabinet_scheme", equipment=["грщ"], raw=["ГРЩ"]),
        right_desc=_desc("b", "Однолинейная схема ГРЩ",
                         gt="single_line_scheme", equipment=["грщ"],
                         raw=["ГРЩ"]),
        matched_entry=_matched("a", "b", gt_match=False, disc_match=True,
                               eq_overlap=0.5))
    assert "graphic_type_mismatch" in res["candidate_risk_flags"]
    assert res["candidate_kind"] == gv.CANDIDATE_SAME


def test_legend_false_pair_excluded_from_top_enrichment():
    """spec 2.3 + top5: при наличии настоящих инженерных кандидатов
    legend/domain-mismatch пара (7VMV-подобная) НЕ попадает в enrichment."""
    legend_pair = {**_gate_pair(9, "send_to_vision"),
                   "pair_key": "L_LEG__R_LEG",
                   "left_block_id": "L_LEG", "right_block_id": "R_LEG"}
    gate = _gate([
        {**_gate_pair(1, "send_to_vision"), "pair_key": "L_GRSH__R_GRSH",
         "left_block_id": "L_GRSH", "right_block_id": "R_GRSH"},
        legend_pair,
    ])
    lg = {"descriptors": [
        _desc("L_GRSH", "Однолинейная расчетная схема ГРЩ",
              gt="cabinet_scheme", equipment=["грщ"], raw=["ГРЩ"]),
        _desc("L_LEG", "Условные обозначения (ОЗДС)", gt="legend",
              disc="SOT", equipment=["оздс"], raw=["ОЗДС", "20кВ"]),
    ]}
    rg = {"descriptors": [
        _desc("R_GRSH", "Однолинейная схема ГРЩ", equipment=["грщ"],
              raw=["ГРЩ"]),
        _desc("R_LEG", "Условные обозначения (квартирные ящики)", gt="legend",
              disc="EOM", equipment=["щк", "меркурий"], raw=["ШК", "ВРУ"]),
    ]}
    matched = {"matched_graphic_blocks": [
        _matched("L_GRSH", "R_GRSH", quality="strong", gt_match=False,
                 disc_match=True, eq_overlap=0.5),
        _matched("L_LEG", "R_LEG", quality="weak", gt_match=True,
                 disc_match=False, eq_overlap=0.0),
    ]}
    selected, stats, _ = gv.select_vision_candidates_v2(
        gate, left_graphic_report=lg, right_graphic_report=rg,
        graphic_matched_report=matched,
        options={"selection_mode": "enrichment", "max_items": 5})
    keys = [p["pair_key"] for p in selected]
    assert "L_GRSH__R_GRSH" in keys
    assert "L_LEG__R_LEG" not in keys
    assert stats["mismatch_excluded"] >= 1


# ─── tiled render MVP (Section 3) ────────────────────────────────────────────


def _dense_pair(tmp_path):
    lm, rm = _model("left"), _model("right")
    lm["blocks"]["L_S1"]["image_file"] = _png(tmp_path / "src_l.png")
    rm["blocks"]["R_S1"]["image_file"] = _png(tmp_path / "src_r.png",
                                              color=(180, 180, 180))
    gate = _gate([_gate_pair(1, "send_to_vision")])
    lg = {"descriptors": [
        {"block_id": "L_S1", "graphic_type": "dense_scheme",
         "discipline": "EOM"}]}
    return lm, rm, gate, lg


def test_tiled_dense_creates_tiles_and_aggregates(tmp_path):
    pytest.importorskip("cv2")
    lm, rm, gate, lg = _dense_pair(tmp_path)
    rep = gv.run_graphic_vision_enrichment(
        lm, rm, gate, left_graphic_report=lg,
        options={"render_crops": True,
                 "render": {"mode": "tiled", "max_tiles": 4}},
        vision_runner=_ok_runner, crops_dir=tmp_path / "crops")
    item = rep["items"][0]
    rnd = item["render"]
    assert rnd["requested_mode"] == "tiled"
    assert rnd["effective_mode"] == "tiled"
    assert rnd["tiles_total"] >= 1
    assert len(rnd["tiles"]) == rnd["tiles_total"]
    # каждая плитка несёт refs + bbox + статус
    for t in rnd["tiles"]:
        assert t["tile_id"]
        assert len(t["bbox_norm"]) == 4
        assert t["vision_status"] == "ok"
    # full crop refs сохранены
    assert rnd["full_left_crop_ref"] and rnd["full_right_crop_ref"]
    # агрегированный результат собран
    assert item["vision_status"] == "ok"
    assert item["result"]["observed_changes"]
    assert "tile_results_summary" in item["result"]
    assert rep["summary"]["tiled_items"] == 1
    assert rep["summary"]["tiles_total"] == rnd["tiles_total"]
    assert rep["summary"]["render_mode_requested"] == "tiled"


def test_tiled_no_runner_saves_tile_plan(tmp_path):
    pytest.importorskip("cv2")
    lm, rm, gate, lg = _dense_pair(tmp_path)
    rep = gv.run_graphic_vision_enrichment(
        lm, rm, gate, left_graphic_report=lg,
        options={"render_crops": True, "render": {"mode": "tiled"}},
        vision_runner=None, crops_dir=tmp_path / "crops")
    item = rep["items"][0]
    rnd = item["render"]
    assert item["vision_status"] == "skipped_no_runner"
    assert rnd["tiles_total"] >= 1
    # план плиток существует, но без vision-вызовов
    for t in rnd["tiles"]:
        assert t["vision_status"] == "skipped_no_runner"
        assert t["result"] is None
    assert rep["summary"]["skipped_no_runner"] == 1
    assert rep["status"] == "skipped_no_runner"


def test_tiled_runner_called_per_tile_with_prompts(tmp_path):
    pytest.importorskip("cv2")
    lm, rm, gate, lg = _dense_pair(tmp_path)
    calls = []

    def _cap(prompt, lp, rp, options):
        calls.append({"prompt": prompt, "lp": lp, "rp": rp,
                      "tile": options.get("tile")})
        return _ok_runner(prompt, lp, rp, options)

    rep = gv.run_graphic_vision_enrichment(
        lm, rm, gate, left_graphic_report=lg,
        options={"render_crops": True,
                 "render": {"mode": "tiled", "max_tiles": 4}},
        vision_runner=_cap, crops_dir=tmp_path / "crops")
    rnd = rep["items"][0]["render"]
    # ровно один вызов на плитку
    assert len(calls) == rnd["tiles_total"]
    # tile-промпт: номер плитки, оговорка про фрагмент, маркер нечитаемого
    for n, c in enumerate(calls, start=1):
        assert f"{n} из {len(calls)}" in c["prompt"]
        assert "фрагмент" in c["prompt"].lower()
        assert "[нечитаемо]" in c["prompt"]
        # tile-файлы существуют и переданы runner'у (не перепутаны)
        assert c["lp"] and Path(c["lp"]).exists()
        assert c["rp"] and Path(c["rp"]).exists()
        assert "_left.png" in c["lp"] and "_right.png" in c["rp"]


def test_tiled_aggregation_merges_dedups_and_min_confidence():
    tiles = [
        {"vision_status": "ok", "result": {
            "old_description": "ВРУ-2 1000А", "new_description": "ВРУ-2",
            "observed_changes": ["QF3 160А→250А"],
            "engineering_entities_old": ["ВРУ-2", "QF3"],
            "engineering_entities_new": ["ВРУ-2", "QF3"],
            "possible_risks": ["селективность"], "confidence": "high"}},
        {"vision_status": "ok", "result": {
            "old_description": "ЩО-1", "new_description": "ЩО-1 доб. линия",
            "observed_changes": ["QF3 160А→250А", "добавлена линия L5"],
            "engineering_entities_old": ["ЩО-1"],
            "engineering_entities_new": ["ЩО-1", "L5"],
            "possible_risks": ["селективность"], "confidence": "low"}},
        {"vision_status": "failed", "result": None},
    ]
    agg = gv.aggregate_tile_results(tiles)
    # dedup observed_changes (QF3 повторялся)
    assert agg["observed_changes"].count("QF3 160А→250А") == 1
    assert "добавлена линия L5" in agg["observed_changes"]
    # dedup risks
    assert agg["possible_risks"] == ["селективность"]
    # confidence = минимум по плиткам
    assert agg["confidence"] == "low"
    assert agg["tile_results_summary"]["tiles_ok"] == 2
    assert agg["tile_results_summary"]["tiles_failed"] == 1


def test_tiled_grid_respects_max_tiles_and_overlap():
    # широкая схема, cap=3
    grid = gv.plan_tile_grid(3000, 800, max_tiles=3, overlap=0.1)
    assert 1 <= len(grid) <= 3
    # высокая схема
    grid2 = gv.plan_tile_grid(700, 2400, max_tiles=4, overlap=0.1)
    assert 1 <= len(grid2) <= 4
    # каждая bbox нормирована и валидна
    for g in grid + grid2:
        x0, y0, x1, y1 = g["bbox_norm"]
        assert 0.0 <= x0 < x1 <= 1.0
        assert 0.0 <= y0 < y1 <= 1.0
    # cap=1 → ровно одна плитка (полный кадр)
    grid3 = gv.plan_tile_grid(2000, 2000, max_tiles=1, overlap=0.1)
    assert len(grid3) == 1


def test_tiled_one_tile_failure_does_not_fail_item(tmp_path):
    pytest.importorskip("cv2")
    lm, rm, gate, lg = _dense_pair(tmp_path)
    state = {"n": 0}

    def _flaky(prompt, lp, rp, options):
        state["n"] += 1
        if state["n"] == 1:
            raise RuntimeError("tile boom")
        return _ok_runner(prompt, lp, rp, options)

    rep = gv.run_graphic_vision_enrichment(
        lm, rm, gate, left_graphic_report=lg,
        options={"render_crops": True,
                 "render": {"mode": "tiled", "max_tiles": 4}},
        vision_runner=_flaky, crops_dir=tmp_path / "crops")
    item = rep["items"][0]
    rnd = item["render"]
    statuses = [t["vision_status"] for t in rnd["tiles"]]
    assert "failed" in statuses          # одна плитка упала
    assert "ok" in statuses              # остальные прошли
    # item жив за счёт остальных плиток
    assert item["vision_status"] == "ok"
    assert item["result"]["tile_results_summary"]["tiles_failed"] >= 1


def test_tiled_all_tiles_fail_marks_item_failed(tmp_path):
    pytest.importorskip("cv2")
    lm, rm, gate, lg = _dense_pair(tmp_path)

    def _boom(prompt, lp, rp, options):
        raise RuntimeError("nope")

    rep = gv.run_graphic_vision_enrichment(
        lm, rm, gate, left_graphic_report=lg,
        options={"render_crops": True,
                 "render": {"mode": "tiled", "max_tiles": 4}},
        vision_runner=_boom, crops_dir=tmp_path / "crops")
    item = rep["items"][0]
    assert item["vision_status"] == "failed"
    assert all(t["vision_status"] == "failed" for t in item["render"]["tiles"])


def test_tile_prompt_builder_contents():
    p = gv.build_tile_prompt(2, 5, [0.1, 0.0, 0.6, 0.5])
    assert "2 из 5" in p
    assert "[нечитаемо]" in p
    assert "фрагмент" in p.lower()
    # буквальная выписка номиналов/обозначений упомянута
    assert "номинал" in p.lower() or "обознач" in p.lower()

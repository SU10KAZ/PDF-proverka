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

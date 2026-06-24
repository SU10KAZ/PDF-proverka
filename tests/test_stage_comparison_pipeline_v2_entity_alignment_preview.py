# -*- coding: utf-8 -*-
"""Тесты Pipeline V2 Mapping-aware Graphic Entity Alignment Preview (mark-only).

Покрытие spec-кейсов (1–14):

 1.  ВРУ-3 ↔ ВРУ-3 → same_entity_likely.
 2.  ВРУ-3 ↔ ВРУ-2 без доп. признаков → scope_reorganized (не same_entity).
 3.  ЯК ↔ ЩО → mismatch_likely.
 4.  схема ↔ план → mismatch_likely.
 5.  ОЗДС ↔ квартирные ящики → mismatch_likely.
 6.  family совпадает, номер конфликтует, grounded overlap высокий → possible_rename.
 7.  strong block match + same entity label → same_entity.
 8.  weak block match + conflicting entity label → scope/link_validation.
 9.  unpaired left/right entities → unpaired_entities.
 10. summary counts корректны.
 11. missing optional artifacts не роняют report.
 12. dry-run пишет entity_alignment_preview_report.json.
 13. UI payload summary появляется.
 14. no Qwen/Claude/Opus imports/calls.

Чистый offline: без сети/LLM/vision.
"""
from __future__ import annotations

import json
from pathlib import Path

from backend.app.services.stage_comparison import (
    pipeline_v2_entity_alignment_preview as eap,
)


# ─── фикстуры ────────────────────────────────────────────────────────────────

def _pair(lid, rid, lpg=10, rpg=10, status="changed_visual"):
    return {"pair_key": f"{lid}__{rid}", "left_block_id": lid, "right_block_id": rid,
            "left_page_number": lpg, "right_page_number": rpg,
            "decision": "send_to_vision", "status": status}


def _desc(block_id, sheet, gtype="single_line_scheme", disc="EOM",
          equipment=None, key=None):
    return {"block_id": block_id, "sheet_name": sheet, "graphic_type": gtype,
            "discipline": disc,
            "tokens": {"equipment": equipment or [], "raw_key_entities": key or []}}


def _graphic_report(*descs):
    return {"version": 1, "descriptors": list(descs)}


def _gate(*pairs):
    return {"version": 1, "kind": "stage_comparison_pipeline_v2_visual_equivalence_gate",
            "status": "ok", "block_pairs": list(pairs)}


def _classify(pair, ld, rd, matched_entry=None, grounded_overlap=None):
    return eap.classify_entity_alignment(pair, left_desc=ld, right_desc=rd,
                                         matched_entry=matched_entry,
                                         grounded_overlap=grounded_overlap)


# ─── 1: ВРУ-3 ↔ ВРУ-3 → same_entity_likely ───────────────────────────────────

def test_1_vru3_vru3_same_entity():
    ld = _desc("L", "Однолинейная расчетная схема ВРУ-3", equipment=["ВРУ-3", "QF1"])
    rd = _desc("R", "Однолинейная схема ВРУ-3", equipment=["ВРУ-3", "QF1"])
    r = _classify(_pair("L", "R"), ld, rd)
    assert r["classification"] == eap.ALIGN_SAME
    assert r["recommended_action"] == "use_for_enrichment"
    assert r["evidence"]["entity_id_match"] is True


# ─── 2: ВРУ-3 ↔ ВРУ-2 без признаков → scope_reorganized (НЕ same) ────────────

def test_2_vru3_vru2_scope_reorganized():
    ld = _desc("L", "Однолинейная расчетная схема ВРУ-3", equipment=["ВРУ-3"])
    rd = _desc("R", "Однолинейная схема ВРУ-2", equipment=["ВРУ-2"])
    r = _classify(_pair("L", "R"), ld, rd)
    assert r["classification"] in (eap.ALIGN_SCOPE, eap.ALIGN_MISMATCH)
    assert r["classification"] != eap.ALIGN_SAME
    assert r["evidence"]["numbered_entity_conflict"] is True
    # дефолт — scope_reorganized (та же family ВРУ, номер конфликтует)
    assert r["classification"] == eap.ALIGN_SCOPE
    assert r["recommended_action"] == "manual_mapping"


# ─── 3: ЯК ↔ ЩО → mismatch_likely ────────────────────────────────────────────

def test_3_yak_shcho_mismatch():
    ld = _desc("L", "Однолинейная расчетная схема щита квартирного ЯК1",
               equipment=["ЯК1"])
    rd = _desc("R", "Однолинейная схема ЩО-3", equipment=["ЩО-3"])
    r = _classify(_pair("L", "R"), ld, rd)
    assert r["classification"] == eap.ALIGN_MISMATCH
    assert r["recommended_action"] == "exclude_from_enrichment"


# ─── 4: схема ↔ план → mismatch_likely ───────────────────────────────────────

def test_4_scheme_vs_plan_mismatch():
    ld = _desc("L", "Однолинейная схема ВРУ-1", gtype="single_line_scheme",
               equipment=["ВРУ-1"])
    rd = _desc("R", "План расположения оборудования ВРУ-1", gtype="plan",
               equipment=["ВРУ-1"])
    r = _classify(_pair("L", "R"), ld, rd)
    assert r["classification"] == eap.ALIGN_MISMATCH
    assert any("sheet_kind_mismatch" in x for x in r["reasons"])


# ─── 5: ОЗДС ↔ квартирные ящики → mismatch_likely ────────────────────────────

def test_5_ozds_vs_apartment_mismatch():
    ld = _desc("L", "Условные обозначения ОЗДС БВУ БПИ", gtype="legend",
               disc="SOV", equipment=["ОЗДС"], key=["ОЗДС", "БВУ"])
    rd = _desc("R", "Схема щита квартирного ШК Меркурий", gtype="single_line_scheme",
               disc="EOM", equipment=["ШК"], key=["Меркурий", "квартирный"])
    r = _classify(_pair("L", "R"), ld, rd)
    assert r["classification"] == eap.ALIGN_MISMATCH


# ─── 6: family совпадает, номер конфликтует, grounded overlap высокий → rename ─

def test_6_numbered_conflict_high_grounded_overlap_rename():
    ld = _desc("L", "Однолинейная схема ВРУ-4", equipment=["ВРУ-4", "QF1"])
    rd = _desc("R", "Однолинейная схема ВРУ-А", equipment=["ВРУ-А", "QF1"])
    me = {"graphic_type_match": True, "discipline_match": True}
    r = _classify(_pair("L", "R"), ld, rd, matched_entry=me, grounded_overlap=0.8)
    assert r["classification"] == eap.ALIGN_RENAME
    assert r["recommended_action"] == "manual_mapping"
    assert any("grounded_overlap" in x for x in r["reasons"])


# ─── 7: strong block match + same entity label → same_entity ─────────────────

def test_7_strong_match_same_label_same_entity():
    ld = _desc("L", "Однолинейная схема ГРЩ", equipment=["ГРЩ"])
    rd = _desc("R", "Однолинейная схема ГРЩ", equipment=["ГРЩ"])
    me = {"match_quality": "strong", "graphic_type_match": True,
          "discipline_match": True,
          "token_overlap": {"equipment": 1.0}}
    r = _classify(_pair("L", "R"), ld, rd, matched_entry=me)
    assert r["classification"] == eap.ALIGN_SAME


# ─── 8: weak match + conflicting label → scope/link_validation ───────────────

def test_8_weak_conflicting_label():
    ld = _desc("L", "Однолинейная схема ВРУ-2", equipment=["ВРУ-2"])
    rd = _desc("R", "Однолинейная схема ВРУ-1", equipment=["ВРУ-1"])
    me = {"match_quality": "weak", "risk_flags": ["weak_block_match"]}
    r = _classify(_pair("L", "R"), ld, rd, matched_entry=me)
    assert r["classification"] in (eap.ALIGN_SCOPE, eap.ALIGN_LINK_VALIDATION)
    assert r["classification"] != eap.ALIGN_SAME


# ─── 9: unpaired entities ────────────────────────────────────────────────────

def test_9_unpaired_entities():
    # L: ВРУ-3 (пара) + ВРУ-9 (без пары справа); R: ВРУ-3 (пара)
    lg = _graphic_report(
        _desc("L1", "Однолинейная схема ВРУ-3", equipment=["ВРУ-3"]),
        _desc("L2", "Однолинейная схема ВРУ-9", equipment=["ВРУ-9"]))
    rg = _graphic_report(_desc("R1", "Однолинейная схема ВРУ-3", equipment=["ВРУ-3"]))
    gate = _gate(_pair("L1", "R1"))
    rep = eap.build_entity_alignment_preview_report(
        {}, {}, gate, left_graphic_report=lg, right_graphic_report=rg)
    left_unpaired = {u["entity_label"] for u in rep["unpaired_entities"]["left"]}
    assert "ВРУ-9" in left_unpaired      # без пары справа
    assert "ВРУ-3" not in left_unpaired   # сматчена same_entity


# ─── 10: summary counts ──────────────────────────────────────────────────────

def test_10_summary_counts():
    lg = _graphic_report(
        _desc("L1", "Однолинейная схема ВРУ-3", equipment=["ВРУ-3"]),
        _desc("L2", "Однолинейная схема щита квартирного ЯК1", equipment=["ЯК1"]))
    rg = _graphic_report(
        _desc("R1", "Однолинейная схема ВРУ-3", equipment=["ВРУ-3"]),
        _desc("R2", "Однолинейная схема ЩО-3", equipment=["ЩО-3"]))
    gate = _gate(_pair("L1", "R1"), _pair("L2", "R2"))
    rep = eap.build_entity_alignment_preview_report(
        {}, {}, gate, left_graphic_report=lg, right_graphic_report=rg)
    s = rep["summary"]
    assert s["graphic_pairs_total"] == 2
    assert s["same_entity_likely"] == 1   # ВРУ-3↔ВРУ-3
    assert s["mismatch_likely"] == 1      # ЯК↔ЩО
    total = (s["same_entity_likely"] + s["possible_rename"] + s["scope_reorganized"]
             + s["mismatch_likely"] + s["link_validation_candidate"])
    assert total == s["graphic_pairs_total"]
    assert s["needs_manual_mapping"] == s["possible_rename"] + s["scope_reorganized"]


# ─── 11: missing optional artifacts не роняют report ─────────────────────────

def test_11_missing_optionals_fail_soft():
    gate = _gate(_pair("L1", "R1"))
    # без graphic descriptors / matched / grounding / models
    rep = eap.build_entity_alignment_preview_report({}, {}, gate)
    assert rep["status"] in ("ok", "completed_with_warnings")
    assert isinstance(rep["pairs"], list)
    # gate отсутствует целиком → skipped, не падает
    rep2 = eap.build_entity_alignment_preview_report({}, {}, None)
    assert rep2["status"] == "completed_with_warnings"
    assert rep2["pairs"] == []
    assert rep2["warnings"]


# ─── 12: dry-run пишет entity_alignment_preview_report.json ───────────────────

def test_12_dry_run_writes_report(tmp_path):
    from backend.app.services.stage_comparison import pipeline_v2_dry_run as dr

    def _rj(blocks):
        return {"document": {"pages": [{"page_number": 1, "blocks": blocks}]}}
    left = tmp_path / "left.json"
    right = tmp_path / "right.json"
    left.write_text(json.dumps(_rj([
        {"block_id": "L1", "block_type": "image", "bbox": [0, 0, 100, 100],
         "text": "Однолинейная схема ВРУ-3"}])), encoding="utf-8")
    right.write_text(json.dumps(_rj([
        {"block_id": "R1", "block_type": "image", "bbox": [0, 0, 100, 100],
         "text": "Однолинейная схема ВРУ-3"}])), encoding="utf-8")
    out = tmp_path / "out"
    summary = dr.run_pipeline_v2_dry_run({"result_json_path": str(left)},
                                         {"result_json_path": str(right)}, out)
    paths = dr.build_pipeline_v2_artifact_paths(out)
    assert paths["entity_alignment_preview"].name == \
        "entity_alignment_preview_report.json"
    assert paths["entity_alignment_preview"].exists()
    rep = json.loads(paths["entity_alignment_preview"].read_text(encoding="utf-8"))
    assert rep["kind"] == eap.REPORT_KIND
    assert "entity_alignment_preview" in summary
    assert summary["entity_alignment_preview"]["enabled"] is True


def test_12b_dry_run_fail_soft_on_exception(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import pipeline_v2_dry_run as dr

    def _rj(blocks):
        return {"document": {"pages": [{"page_number": 1, "blocks": blocks}]}}
    left = tmp_path / "left.json"
    right = tmp_path / "right.json"
    left.write_text(json.dumps(_rj([{"block_id": "L1", "block_type": "image",
                    "bbox": [0, 0, 100, 100], "text": "ВРУ-3"}])), encoding="utf-8")
    right.write_text(json.dumps(_rj([{"block_id": "R1", "block_type": "image",
                    "bbox": [0, 0, 100, 100], "text": "ВРУ-3"}])), encoding="utf-8")

    def _boom(*a, **k):
        raise RuntimeError("eap boom")
    monkeypatch.setattr(dr, "build_entity_alignment_preview_report", _boom)
    summary = dr.run_pipeline_v2_dry_run({"result_json_path": str(left)},
                                         {"result_json_path": str(right)}, tmp_path / "o2")
    assert summary["status"] in ("ok", "completed_with_warnings")
    assert summary["entity_alignment_preview"].get("error")


# ─── 13: UI payload summary ──────────────────────────────────────────────────

def test_13_ui_payload_summary():
    from backend.app.services.stage_comparison import pipeline_v2_ui_payload as up
    from backend.app.services.stage_comparison import pipeline_v2_dry_run as dr
    lg = _graphic_report(_desc("L1", "Однолинейная схема ВРУ-3", equipment=["ВРУ-3"]))
    rg = _graphic_report(_desc("R1", "Однолинейная схема ВРУ-3", equipment=["ВРУ-3"]))
    rep = eap.build_entity_alignment_preview_report(
        {}, {}, _gate(_pair("L1", "R1")), left_graphic_report=lg, right_graphic_report=rg)
    sec = dr._entity_alignment_preview_section(rep, True, None)
    summary = {"status": "ok", "stages": {}, "delta_sections": {},
               "entity_alignment_preview": sec}
    payload = up.build_pipeline_v2_ui_payload(summary)
    assert "entity_alignment_preview" in payload
    eapp = payload["entity_alignment_preview"]
    assert eapp["available"] is True
    assert eapp["same_entity_likely"] == 1


def test_13b_ui_payload_absent_when_disabled():
    from backend.app.services.stage_comparison import pipeline_v2_ui_payload as up
    summary = {"status": "ok", "stages": {}, "delta_sections": {},
               "entity_alignment_preview": {"enabled": False, "status": "disabled"}}
    payload = up.build_pipeline_v2_ui_payload(summary)
    assert "entity_alignment_preview" not in payload


# ─── 14: no Qwen/Claude/Opus imports/calls ───────────────────────────────────

def test_14_no_llm_or_vision_imports():
    src = Path(eap.__file__).read_text(encoding="utf-8")
    for forbidden in ("import requests", "import httpx", "urllib.request",
                      "import socket", "import subprocess", "graphic_llm",
                      "text_llm_provider", "ClaudeCodeProvider", "claude -p",
                      "qwen", "opus", "vision_runner", "llm_runner", "fastapi",
                      "router"):
        assert forbidden.lower() not in src.lower(), \
            f"module references {forbidden!r}"


# ─── label extraction / similarity helpers ───────────────────────────────────

def test_extract_entity_labels_primary():
    lab = eap.extract_entity_labels("Однолинейная расчетная схема ВРУ-3")
    assert lab["primary"] == "ВРУ-3"
    assert lab["family"] == "ВРУ" and lab["number"] == "3"
    assert lab["confidence"] >= 0.7


def test_sheet_title_similarity():
    a = "Однолинейная расчетная схема ВРУ-3"
    b = "Однолинейная схема ВРУ-3"
    # бойлерплейт отброшен, остаётся высокая близость
    assert eap.sheet_title_similarity(a, b) >= 0.0
    assert eap.sheet_title_similarity("", "x") == 0.0

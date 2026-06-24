# -*- coding: utf-8 -*-
"""Тесты Pipeline V2 Exclusion Preview v2 (mark-only).

Покрытие spec-кейсов §9:
  1. link_validation reject/different → candidate_exclude;
  2. repeated reject transition → boost severity/confidence;
  3. valid_mapping → keep (НЕ exclude);
  4. manual rejected_mapping → candidate_exclude;
  5. manual no_match → candidate_exclude;
  6. manual confirmed_same_entity → keep;
  7. manual confirmed_reorganized без validation → link_validation_required;
  8. manual confirmed_reorganized + validation reject → high sev manual_vision_conflict;
  9. mismatch_likely → candidate_exclude/review_only с reason;
  10. scope_reorganized без validation → link_validation_required;
  11. uncertain/manual_review → review_only;
  12. инварианты auto_apply/enforce_allowed/use_as_grounded_fact = false;
  13. missing optional artifacts fail-soft;
  14. dry-run stage disabled by default;
  15. dry-run enabled writes artifact;
  16. ui_payload summary reads report;
  17. NO Qwen/Gemma/Claude/Opus imports/calls.

Чистый offline-слой: модели не запускаются.
"""
from __future__ import annotations

import json
from pathlib import Path

from backend.app.services.stage_comparison import pipeline_v2_exclusion_preview as xp


# ─── фикстуры-строители ──────────────────────────────────────────────────────

def _lv_item(lid, rid, ll, rl, decision, relation, conf, *, manual=None,
             conflict=False):
    return {
        "item_id": f"lv_{lid}__{rid}", "mapping_id": f"m_{lid}",
        "left_block_id": lid, "right_block_id": rid,
        "left_entity_label": ll, "right_entity_label": rl,
        "manual_decision": manual, "status": "done",
        "validation": {"decision": decision, "entity_relation": relation,
                       "confidence": conf, "do_not_use_as_fact": True},
        "agreement": {"agrees_with_manual_mapping": False,
                      "conflicts_with_manual_mapping": conflict, "reason": ""},
        "use_as_grounded_fact": False, "use_for_delta_explanation": False,
    }


def _lv_report(items):
    return {"version": 1, "kind": "stage_comparison_pipeline_v2_link_validation",
            "status": "ok", "items": items, "summary": {}, "warnings": []}


def _align_pair(lid, rid, ll, rl, classification, fam="ВРУ", conf=0.5,
                risk=None):
    return {"pair_key": f"{lid}__{rid}", "left_block_id": lid, "right_block_id": rid,
            "left_entity_label": ll, "right_entity_label": rl, "entity_family": fam,
            "classification": classification, "confidence": conf,
            "reasons": [], "risk_flags": risk or []}


def _align_report(pairs):
    return {"version": 1,
            "kind": "stage_comparison_pipeline_v2_entity_alignment_preview",
            "status": "ok", "pairs": pairs,
            "unpaired_entities": {"left": [], "right": []}, "warnings": []}


def _overrides(mappings=None, rejected=None, no_match=None):
    return {"version": 1, "kind": "stage_comparison_pipeline_v2_entity_mapping_overrides",
            "mappings": mappings or [], "rejected": rejected or [],
            "no_match": no_match or []}


def _build(**kw):
    return xp.build_exclusion_preview_report(session_id="s", pair_id="p", **kw)


def _by_pair(report, lid, rid):
    return next(i for i in report["items"]
               if i["left_block_id"] == lid and i["right_block_id"] == rid)


# ─── 1: lv reject/different → candidate_exclude ──────────────────────────────

def test_1_lv_reject_to_candidate_exclude():
    r = _build(link_validation_report=_lv_report([
        _lv_item("L1", "R1", "ВРУ-3", "ВРУ-2", "reject_mapping", "different_entity", 0.95)]))
    it = _by_pair(r, "L1", "R1")
    assert it["classification"] == xp.CLS_EXCLUDE
    assert it["recommended_action"] == "exclude_from_enrichment"
    assert it["severity"] == "high"            # confidence 0.95 ≥ threshold


# ─── 2: repeated reject transition boost ─────────────────────────────────────

def test_2_repeated_reject_transition_boost():
    r = _build(link_validation_report=_lv_report([
        _lv_item("L1", "R1", "ВРУ-3", "ВРУ-2", "reject_mapping", "different_entity", 0.7),
        _lv_item("L2", "R2", "вру-3", "вру-2", "reject_mapping", "different_entity", 0.7),
    ]))
    assert r["summary"]["repeated_reject_transitions"] == 2
    for lid, rid in (("L1", "R1"), ("L2", "R2")):
        it = _by_pair(r, lid, rid)
        assert "repeated_reject_mapping_transition" in it["risk_flags"]
        assert it["severity"] == "high"
        assert it["confidence"] >= 0.75        # boosted from 0.7


# ─── 3: valid_mapping → keep, not exclude ────────────────────────────────────

def test_3_valid_mapping_keep_not_exclude():
    r = _build(link_validation_report=_lv_report([
        _lv_item("L1", "R1", "ВРУ-4", "ВРУ-а", "valid_mapping", "reorganized_same_entity", 0.85)]))
    it = _by_pair(r, "L1", "R1")
    assert it["classification"] == xp.CLS_KEEP
    assert it["recommended_action"] == "keep_for_enrichment"
    assert r["summary"]["candidate_exclude"] == 0


# ─── 4: manual rejected_mapping → candidate_exclude ──────────────────────────

def test_4_manual_rejected_to_exclude():
    r = _build(overrides_report=_overrides(rejected=[
        {"mapping_id": "m1", "left_block_id": "L1", "right_block_id": "R1",
         "left_entity_label": "ЯК-3", "right_entity_label": "ЩО-1"}]))
    it = _by_pair(r, "L1", "R1")
    assert it["classification"] == xp.CLS_EXCLUDE
    assert it["manual_mapping"]["decision"] == "rejected_mapping"


# ─── 5: manual no_match → candidate_exclude ──────────────────────────────────

def test_5_manual_no_match_to_exclude():
    r = _build(overrides_report=_overrides(no_match=[
        {"mapping_id": "m1", "left_block_id": "L1", "right_block_id": "R1",
         "left_entity_label": "ЩР-9", "right_entity_label": None}]))
    it = _by_pair(r, "L1", "R1")
    assert it["classification"] == xp.CLS_EXCLUDE
    assert it["manual_mapping"]["decision"] == "no_match"


# ─── 6: manual confirmed_same_entity → keep ──────────────────────────────────

def test_6_manual_confirmed_same_keep():
    r = _build(overrides_report=_overrides(mappings=[
        {"mapping_id": "m1", "left_block_id": "L1", "right_block_id": "R1",
         "left_entity_label": "ВРУ-1", "right_entity_label": "ВРУ-1",
         "manual_decision": "confirmed_same_entity"}]))
    it = _by_pair(r, "L1", "R1")
    assert it["classification"] == xp.CLS_KEEP


# ─── 7: manual confirmed_reorganized w/o validation → link_validation_required

def test_7_manual_reorganized_no_validation_link_required():
    r = _build(overrides_report=_overrides(mappings=[
        {"mapping_id": "m1", "left_block_id": "L1", "right_block_id": "R1",
         "left_entity_label": "ВРУ-3", "right_entity_label": "ВРУ-2",
         "manual_decision": "confirmed_reorganized"}]))
    it = _by_pair(r, "L1", "R1")
    assert it["classification"] == xp.CLS_LINK_VALIDATION
    assert it["recommended_action"] == "run_link_validation"


# ─── 8: manual confirmed_reorganized + validation reject → conflict ──────────

def test_8_manual_reorganized_plus_reject_conflict():
    r = _build(
        overrides_report=_overrides(mappings=[
            {"mapping_id": "m1", "left_block_id": "L1", "right_block_id": "R1",
             "left_entity_label": "ВРУ-3", "right_entity_label": "ВРУ-2",
             "manual_decision": "confirmed_reorganized"}]),
        link_validation_report=_lv_report([
            _lv_item("L1", "R1", "ВРУ-3", "ВРУ-2", "reject_mapping",
                     "different_entity", 0.95, manual="confirmed_reorganized",
                     conflict=True)]))
    it = _by_pair(r, "L1", "R1")
    assert it["severity"] == "high"
    assert "manual_vision_conflict" in it["risk_flags"]
    assert it["recommended_action"] == "manual_review"   # не молчаливый exclude
    assert it["classification"] == xp.CLS_EXCLUDE
    assert r["summary"]["manual_vision_conflict"] == 1


# ─── 9: mismatch_likely → exclude/review with reason ─────────────────────────

def test_9_mismatch_likely_exclude_or_review():
    r = _build(entity_alignment_report=_align_report([
        _align_pair("L1", "R1", "ЯК-3", "ЩО-1", "mismatch_likely", fam="ЯК")]))
    it = _by_pair(r, "L1", "R1")
    assert it["classification"] in (xp.CLS_EXCLUDE, xp.CLS_REVIEW)
    assert any("mismatch" in rs for rs in it["reasons"])


# ─── 10: scope_reorganized w/o validation → link_validation_required ─────────

def test_10_scope_reorganized_no_validation_link_required():
    r = _build(entity_alignment_report=_align_report([
        _align_pair("L1", "R1", "ВРУ-4", "ВРУ-а", "scope_reorganized")]))
    it = _by_pair(r, "L1", "R1")
    assert it["classification"] == xp.CLS_LINK_VALIDATION


# ─── 11: uncertain / manual_review → review_only ─────────────────────────────

def test_11_uncertain_to_review_only():
    r = _build(link_validation_report=_lv_report([
        _lv_item("L1", "R1", "ЩС-1", "ЩС-1", "manual_review", "uncertain", 0.5)]))
    it = _by_pair(r, "L1", "R1")
    assert it["classification"] == xp.CLS_REVIEW
    assert it["recommended_action"] == "manual_review"


# ─── 12: invariants ──────────────────────────────────────────────────────────

def test_12_mark_only_invariants():
    r = _build(
        link_validation_report=_lv_report([
            _lv_item("L1", "R1", "ВРУ-3", "ВРУ-2", "reject_mapping", "different_entity", 0.95),
            _lv_item("L2", "R2", "ВРУ-4", "ВРУ-а", "valid_mapping", "renamed_same_entity", 0.85)]),
        entity_alignment_report=_align_report([
            _align_pair("L3", "R3", "ЯК", "ЩО", "mismatch_likely")]))
    assert r["summary"]["auto_enforce_enabled"] is False
    for it in r["items"]:
        assert it["use_as_grounded_fact"] is False
        assert it["auto_apply"] is False
        assert it["enforce_allowed"] is False


# ─── 13: missing optional artifacts fail-soft ────────────────────────────────

def test_13_missing_artifacts_fail_soft():
    r = _build()   # вообще без артефактов
    assert r["kind"] == xp.REPORT_KIND
    assert r["status"] == "completed_with_warnings"
    assert r["summary"]["items_total"] == 0
    assert r["warnings"]
    # частично: только link_validation
    r2 = _build(link_validation_report=_lv_report([
        _lv_item("L1", "R1", "ВРУ-3", "ВРУ-2", "reject_mapping", "different_entity", 0.95)]))
    assert r2["summary"]["items_total"] == 1
    assert any("missing" in w for w in r2["warnings"])


# ─── 14: dry-run stage disabled by default ───────────────────────────────────

def test_14_dry_run_stage_disabled_by_default(tmp_path):
    from backend.app.services.stage_comparison import pipeline_v2_dry_run as dry
    # дефолтные опции → exclusion_preview не включён
    sec = dry._exclusion_preview_section(None, False, None)
    assert sec["enabled"] is False
    assert sec["status"] == "disabled"
    assert sec["auto_enforce_enabled"] is False


# ─── 15: dry-run enabled writes artifact ─────────────────────────────────────

def test_15_run_writes_artifact(tmp_path):
    # положить минимальные входы в out_dir
    (tmp_path / "link_validation_report.json").write_text(
        json.dumps(_lv_report([
            _lv_item("L1", "R1", "ВРУ-3", "ВРУ-2", "reject_mapping",
                     "different_entity", 0.95)])), encoding="utf-8")
    out = tmp_path / "exclusion_preview_v2_report.json"
    report = xp.run_pipeline_v2_exclusion_preview(
        tmp_path, session_id="s", pair_id="p", output_path=out)
    assert out.is_file()
    disk = json.loads(out.read_text(encoding="utf-8"))
    assert disk["kind"] == xp.REPORT_KIND
    assert disk["summary"]["candidate_exclude"] == 1
    assert report["summary"]["candidate_exclude"] == 1
    # входной артефакт НЕ изменён
    lv_in = json.loads((tmp_path / "link_validation_report.json").read_text(encoding="utf-8"))
    assert lv_in["kind"] == "stage_comparison_pipeline_v2_link_validation"


# ─── 16: ui_payload summary reads report ─────────────────────────────────────

def test_16_ui_payload_reads_report():
    from backend.app.services.stage_comparison import pipeline_v2_ui_payload as uip
    summary = {"status": "ok", "exclusion_preview_v2": {
        "enabled": True, "status": "ok", "items_total": 54,
        "candidate_exclude": 21, "review_only": 0, "keep": 13,
        "link_validation_required": 20, "high_confidence_exclude": 4,
        "manual_vision_conflict": 1, "repeated_reject_transitions": 2}}
    p = uip.build_pipeline_v2_ui_payload(summary)
    sec = p["exclusion_preview_v2"]
    assert sec["available"] is True
    assert sec["candidate_exclude"] == 21
    assert sec["keep"] == 13
    assert sec["link_validation_required"] == 20
    assert sec["auto_enforce_enabled"] is False
    # отсутствует, когда секции нет
    assert "exclusion_preview_v2" not in uip.build_pipeline_v2_ui_payload({"status": "ok"})


# ─── 17: no Qwen/Gemma/Claude/Opus imports/calls ─────────────────────────────

def test_17_no_model_imports():
    src = Path(xp.__file__).read_text(encoding="utf-8")
    # проверяем РЕАЛЬНЫЕ импорты/вызовы, а не упоминания в docstring
    import_lines = [ln.strip().lower() for ln in src.splitlines()
                    if ln.strip().startswith(("import ", "from "))]
    for ln in import_lines:
        for banned in ("qwen", "gemma", "opus", "claude", "openai", "httpx",
                       "graphic_llm_local", "subprocess", "anthropic",
                       "ensure_lmstudio", "describe_image", "vision_runner"):
            assert banned not in ln, f"unexpected import: {ln}"
    # никаких runtime call-маркеров сетевых/модельных вызовов
    low = src.lower()
    for marker in ("chat/completions", ".post(", "subprocess.", "httpx.",
                   "lmstudio", "load_local_graphic_llm_config", "claude -p"):
        assert marker not in low, f"unexpected runtime call marker: {marker}"

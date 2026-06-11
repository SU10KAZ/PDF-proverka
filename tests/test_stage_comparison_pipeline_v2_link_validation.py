# -*- coding: utf-8 -*-
"""Тесты Pipeline V2 Link Validation Report (mark-only vision-проверка мэппинга).

Покрытие spec-кейсов:
  1.  runner=None → skipped_no_runner, кандидаты построены;
  2.  confirmed_reorganized candidate selected;
  3.  prompt содержит validation-правила, НЕ enrichment;
  4.  parsed valid_mapping response;
  5.  parsed reject_mapping response;
  6.  manual confirmed_reorganized + reject_mapping → conflict;
  7.  manual confirmed_reorganized + reorganized_same_entity → agrees;
  8.  old_new_orientation_ok=false → orientation_failed;
  9.  use_as_grounded_fact=false всегда;
  10. use_for_delta_explanation=false всегда;
  11. broken JSON response → failed item, no crash;
  12. dry-run stage disabled by default;
  13. dry-run enabled without runner → no model calls (skipped_no_runner);
  14. UI payload summary reads report;
  15. модуль без Qwen/Gemma/Claude/Opus импортов/вызовов.

Offline: реальные vision/LLM НЕ вызываются.
"""
from __future__ import annotations

from backend.app.services.stage_comparison.pipeline_v2_link_validation import (
    ACTION_KEEP,
    ACTION_REVIEW,
    CANDIDATE_MANUAL_REORG,
    build_link_validation_prompt,
    parse_link_validation_response,
    run_pipeline_v2_link_validation,
)


def _gate():
    return {"block_pairs": [
        {"left_block_id": "6XDP-JLWQ-KNX", "right_block_id": "3T6X-4PHG-D96",
         "decision": "send_to_vision", "status": "changed_visual",
         "pair_key": "6XDP-JLWQ-KNX__3T6X-4PHG-D96",
         "left_page_number": 27, "right_page_number": 26, "metrics": {}}]}


def _overrides(decision="confirmed_reorganized"):
    return {"mappings": [{
        "mapping_id": "m_e1bf5687249a", "left_block_id": "6XDP-JLWQ-KNX",
        "right_block_id": "3T6X-4PHG-D96",
        "pair_key": "6XDP-JLWQ-KNX__3T6X-4PHG-D96",
        "left_entity_label": "ВРУ-3", "right_entity_label": "ВРУ-2",
        "manual_decision": decision, "comment": "smoke"}]}


def _runner(payload):
    def run(prompt, l, r, o):
        return payload
    return run


# ─── 1: runner=None → skipped_no_runner ──────────────────────────────────────

def test_1_runner_none_skipped():
    r = run_pipeline_v2_link_validation(_gate(), _overrides(),
                                        session_id="s", pair_id="p")
    assert r["status"] == "skipped_no_runner"
    assert r["summary"]["candidates_total"] == 1
    assert r["items"][0]["validation"] is None
    assert r["items"][0]["status"] == "skipped_no_runner"


# ─── 2: confirmed_reorganized candidate selected ─────────────────────────────

def test_2_confirmed_reorganized_selected():
    r = run_pipeline_v2_link_validation(_gate(), _overrides(),
                                        session_id="s", pair_id="p")
    it = r["items"][0]
    assert it["candidate_kind"] == CANDIDATE_MANUAL_REORG
    assert it["left_entity_label"] == "ВРУ-3" and it["right_entity_label"] == "ВРУ-2"
    assert it["mapping_id"] == "m_e1bf5687249a"
    # rejected_mapping override → НЕ кандидат link-validation (MVP scope)
    r2 = run_pipeline_v2_link_validation(_gate(), _overrides("rejected_mapping"),
                                         session_id="s", pair_id="p")
    assert r2["summary"]["candidates_total"] == 0


# ─── 3: prompt validation-oriented, not enrichment ───────────────────────────

def test_3_prompt_validation_rules():
    p = build_link_validation_prompt(left_label="ВРУ-3", right_label="ВРУ-2",
                                     left_page=27, right_page=26)
    assert "являются ли эти блоки одной и той же" in p
    assert "Сохранять разделение OLD и NEW" in p
    assert "Не использовать результат как grounded fact" in p
    assert "enrichment" not in p.lower()
    assert '"entity_relation"' in p and '"decision"' in p


# ─── 4-5: parsed valid / reject ──────────────────────────────────────────────

def test_4_parse_valid_mapping():
    v = parse_link_validation_response(
        {"entity_relation": "reorganized_same_entity", "decision": "valid_mapping",
         "confidence": 0.8, "old_new_orientation_ok": True})
    assert v["entity_relation"] == "reorganized_same_entity"
    assert v["decision"] == "valid_mapping" and v["confidence"] == 0.8
    assert v["do_not_use_as_fact"] is True


def test_5_parse_reject_mapping():
    v = parse_link_validation_response(
        {"content": '{"entity_relation":"different_entity",'
                    '"decision":"reject_mapping","confidence":0.95}'})
    assert v["entity_relation"] == "different_entity"
    assert v["decision"] == "reject_mapping" and v["confidence"] == 0.95


# ─── 6: confirmed_reorganized + reject → conflict ────────────────────────────

def test_6_reorganized_reject_conflict():
    r = run_pipeline_v2_link_validation(
        _gate(), _overrides(), session_id="s", pair_id="p",
        runner=_runner({"entity_relation": "different_entity",
                        "decision": "reject_mapping", "confidence": 0.95,
                        "old_new_orientation_ok": True}))
    it = r["items"][0]
    assert it["status"] == "done"
    assert it["agreement"]["conflicts_with_manual_mapping"] is True
    assert it["agreement"]["agrees_with_manual_mapping"] is False
    assert it["recommended_action"] == ACTION_REVIEW
    assert r["summary"]["reject_mapping"] == 1
    assert r["summary"]["conflicts_with_manual_mapping"] == 1


# ─── 7: confirmed_reorganized + reorganized_same_entity → agrees ─────────────

def test_7_reorganized_valid_agrees():
    r = run_pipeline_v2_link_validation(
        _gate(), _overrides(), session_id="s", pair_id="p",
        runner=_runner({"entity_relation": "reorganized_same_entity",
                        "decision": "valid_mapping", "confidence": 0.8,
                        "old_new_orientation_ok": True}))
    it = r["items"][0]
    assert it["agreement"]["agrees_with_manual_mapping"] is True
    assert it["agreement"]["conflicts_with_manual_mapping"] is False
    assert it["recommended_action"] == ACTION_KEEP
    assert r["summary"]["valid_mapping"] == 1
    assert r["summary"]["agrees_with_manual_mapping"] == 1


# ─── 8: orientation failed ───────────────────────────────────────────────────

def test_8_orientation_failed():
    r = run_pipeline_v2_link_validation(
        _gate(), _overrides(), session_id="s", pair_id="p",
        runner=_runner({"entity_relation": "different_entity",
                        "decision": "reject_mapping",
                        "old_new_orientation_ok": False}))
    it = r["items"][0]
    assert r["summary"]["orientation_failed"] == 1
    assert it["recommended_action"] == ACTION_REVIEW
    # перепутанные стороны → не считаем ни agree, ни conflict
    assert it["agreement"]["conflicts_with_manual_mapping"] is False


# ─── 9-10: grounded-fact invariants ──────────────────────────────────────────

def test_9_use_as_grounded_fact_always_false():
    for payload in ({"entity_relation": "reorganized_same_entity", "decision": "valid_mapping"},
                    {"entity_relation": "different_entity", "decision": "reject_mapping"},
                    {"entity_relation": "uncertain", "decision": "manual_review"}):
        r = run_pipeline_v2_link_validation(_gate(), _overrides(), session_id="s",
                                            pair_id="p", runner=_runner(payload))
        assert r["items"][0]["use_as_grounded_fact"] is False
        assert r["items"][0]["validation"]["do_not_use_as_fact"] is True


def test_10_use_for_delta_explanation_always_false():
    r = run_pipeline_v2_link_validation(
        _gate(), _overrides(), session_id="s", pair_id="p",
        runner=_runner({"entity_relation": "reorganized_same_entity",
                        "decision": "valid_mapping"}))
    assert r["items"][0]["use_for_delta_explanation"] is False
    # даже для skipped (runner=None)
    r2 = run_pipeline_v2_link_validation(_gate(), _overrides(), session_id="s", pair_id="p")
    assert r2["items"][0]["use_for_delta_explanation"] is False
    assert r2["items"][0]["use_as_grounded_fact"] is False


# ─── 11: broken JSON → failed item ───────────────────────────────────────────

def test_11_broken_json_failed_item():
    r = run_pipeline_v2_link_validation(
        _gate(), _overrides(), session_id="s", pair_id="p",
        runner=_runner({"content": "totally not json <<<>>>"}))
    it = r["items"][0]
    assert it["status"] == "failed" and it["validation"] is None
    assert r["summary"]["failed"] == 1 and r["status"] == "ok"
    assert it["use_as_grounded_fact"] is False
    # runner, бросающий исключение, тоже не валит отчёт
    def boom(prompt, l, rr, o):
        raise RuntimeError("runner exploded")
    r2 = run_pipeline_v2_link_validation(_gate(), _overrides(), session_id="s",
                                         pair_id="p", runner=boom)
    assert r2["items"][0]["status"] == "failed" and "error" in r2["items"][0]


# ─── 12-13: dry-run integration ──────────────────────────────────────────────

def test_12_dry_run_stage_disabled_by_default():
    from backend.app.services.stage_comparison.pipeline_v2_dry_run import (
        _link_validation_section)
    # default: enabled=false → секция disabled
    sec = _link_validation_section(None, False, None)
    assert sec["enabled"] is False and sec["status"] == "disabled"


def test_13_dry_run_enabled_no_runner_no_model_calls():
    # enabled, но runner=None → skipped_no_runner, никаких вызовов модели
    r = run_pipeline_v2_link_validation(
        _gate(), _overrides(), session_id="s", pair_id="p",
        options={"enabled": True}, runner=None)
    assert r["status"] == "skipped_no_runner"
    assert r["summary"]["attempted"] == 0  # модель не вызывалась
    from backend.app.services.stage_comparison.pipeline_v2_dry_run import (
        _link_validation_section)
    sec = _link_validation_section(r, True, None)
    assert sec["enabled"] is True and sec["status"] == "skipped_no_runner"


# ─── 14: UI payload reads report ─────────────────────────────────────────────

def test_14_ui_payload_summary():
    from backend.app.services.stage_comparison.pipeline_v2_ui_payload import (
        build_pipeline_v2_ui_payload)
    summary = {
        "status": "ok", "headline": {}, "warnings": [], "artifacts": {},
        "link_validation": {"enabled": True, "status": "ok", "candidates_total": 1,
                            "attempted": 1, "valid_mapping": 0, "manual_review": 0,
                            "reject_mapping": 1, "conflicts_with_manual_mapping": 1},
    }
    payload = build_pipeline_v2_ui_payload(summary)
    lv = payload.get("link_validation")
    assert lv is not None and lv["available"] is True
    assert lv["reject_mapping"] == 1 and lv["conflicts_with_manual_mapping"] == 1
    # disabled → секции нет
    summary["link_validation"]["enabled"] = False
    assert build_pipeline_v2_ui_payload(summary).get("link_validation") is None


# ─── 15: no Qwen/Gemma/Claude/Opus imports ───────────────────────────────────

def test_15_no_model_imports():
    import ast
    import inspect
    from backend.app.services.stage_comparison import pipeline_v2_link_validation as mod
    tree = ast.parse(inspect.getsource(mod))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            imported.append(node.module or "")
    for name in imported:
        low = name.lower()
        assert not any(f in low for f in (
            "qwen", "gemma", "opus", "claude", "httpx", "requests",
            "graphic_llm", "subprocess", "openai")), f"unexpected import: {name}"

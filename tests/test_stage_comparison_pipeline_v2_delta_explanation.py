# -*- coding: utf-8 -*-
"""Тесты для Pipeline V2 — LLM Delta Explanation / Critic.

Synthetic entity_diff_report + synthetic graphic_descriptor_report + FAKE runner.
Никаких реальных LLM/Claude/network/subprocess вызовов.

Покрываемые spec-кейсы 1..16 (см. строки тестов).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import pipeline_v2_delta_explanation as de


# ─── synthetic builders ─────────────────────────────────────────────────────


def _delta(delta_id, *, delta_type="changed", entity_type="cable", old_value="x",
           new_value="y", confidence=0.85, quality_flags=None, left_block_id="L1",
           right_block_id="R1", left_quote="old", right_quote="new",
           subject="кабель"):
    return {
        "delta_id": delta_id, "delta_type": delta_type, "entity_type": entity_type,
        "semantic_group": entity_type, "left_entity_id": "el", "right_entity_id": "er",
        "left_block_id": left_block_id, "right_block_id": right_block_id,
        "block_match_id": "bm_1", "page_numbers": {"left": 1, "right": 1},
        "subject": subject, "field": "value", "old_value": old_value,
        "new_value": new_value,
        "change_summary": f"{entity_type}: {old_value} → {new_value}",
        "confidence": confidence,
        "evidence": {"left": {"quote": left_quote, "source": "text_excerpt",
                              "block_id": left_block_id, "page_number": 1},
                     "right": {"quote": right_quote, "source": "text_excerpt",
                               "block_id": right_block_id, "page_number": 1}},
        "match": {"method": "exact_key", "score": 1.0, "reasons": []},
        "quality_flags": quality_flags or [],
    }


def _diff_report(deltas):
    return {
        "version": 1, "kind": "stage_comparison_pipeline_v2_entity_diff",
        "summary": {"deltas_total": len(deltas)},
        "deltas": deltas, "matched_entity_pairs": [],
        "unmatched_left_entities": [], "unmatched_right_entities": [],
        "block_summaries": [], "warnings": [],
    }


def _graphic_report(*descriptors, matched=None):
    return {
        "version": 1, "kind": "stage_comparison_pipeline_v2_graphic_block_descriptor",
        "document": {}, "summary": {},
        "descriptors": list(descriptors),
        "matched_graphic_blocks": matched or [],
        "warnings": [],
    }


def _descriptor(block_id, *, readiness="high", flags=None):
    return {
        "descriptor_id": f"gdesc_x_{block_id}", "block_id": block_id,
        "page_number": 1, "graphic_type": "scheme", "discipline": "SS",
        "diff_readiness": {"readiness": readiness, "usable_for_diff": readiness in ("high", "medium")},
        "quality_flags": flags or [],
    }


# ─── fake runners ────────────────────────────────────────────────────────────


def _runner_accept(prompt: str) -> str:
    return json.dumps({
        "summary": "Изменено сечение кабеля", "engineering_meaning": "увеличена нагрузка",
        "contractor_impact": "пересчёт спецификации", "risk_level": "medium",
        "groundedness": {"verdict": "grounded", "reason": "обе цитаты есть",
                         "uses_left_evidence": True, "uses_right_evidence": True},
        "critic": {"verdict": "accept", "reason": "обоснованно",
                   "should_show_to_engineer": True},
    }, ensure_ascii=False)


def _runner_ocr_noise(prompt: str) -> str:
    return json.dumps({
        "summary": "разница похожа на OCR-шум", "risk_level": "low",
        "groundedness": {"verdict": "unclear", "reason": "шум"},
        "critic": {"verdict": "possible_ocr_noise", "reason": "ё/е разметка",
                   "should_show_to_engineer": False},
    }, ensure_ascii=False)


def _runner_nhr(prompt: str) -> str:
    return json.dumps({
        "summary": "недостаточно данных", "risk_level": "unknown",
        "critic": {"verdict": "needs_human_review", "reason": "мало evidence"},
    }, ensure_ascii=False)


def _runner_broken(prompt: str) -> str:
    return "это не JSON, просто текст без скобок"


# ─── tests ──────────────────────────────────────────────────────────────────


def test_1_select_priority_deltas():
    deltas = [
        _delta("delta_clear", confidence=0.9),                       # high-conf clear → skip
        _delta("delta_flagged", confidence=0.9,
               quality_flags=["needs_human_review"]),                # flag → priority
        _delta("delta_lowconf", confidence=0.4),                     # low-conf changed → priority
    ]
    sel = de.select_deltas_for_explanation(_diff_report(deltas))
    ids = {d["delta_id"] for d in sel}
    assert "delta_flagged" in ids and "delta_lowconf" in ids


def test_2_high_confidence_clear_skipped_by_default():
    deltas = [_delta("delta_clear", confidence=0.95, quality_flags=[])]
    sel = de.select_deltas_for_explanation(_diff_report(deltas))
    assert sel == []
    # но включается при include_high_confidence
    sel2 = de.select_deltas_for_explanation(
        _diff_report(deltas), {"include_high_confidence": True})
    assert len(sel2) == 1


def test_3_max_deltas_limits():
    deltas = [_delta(f"d{i}", confidence=0.4) for i in range(50)]
    sel = de.select_deltas_for_explanation(_diff_report(deltas), {"max_deltas": 5})
    assert len(sel) == 5


def test_4_prompt_forbids_searching_new_diffs():
    prompt = de.build_delta_explanation_prompt(_delta("d1"))
    assert "НЕ ищешь новые отличия" in prompt or "НЕ ищи" in prompt
    assert "НЕ добавляешь новые замечания" in prompt
    assert "ТОЛЬКО переданную" in prompt


def test_5_prompt_contains_evidence():
    prompt = de.build_delta_explanation_prompt(
        _delta("d1", left_quote="ВВГ 3x2.5", right_quote="ВВГ 3x4"))
    assert "ВВГ 3x2.5" in prompt
    assert "ВВГ 3x4" in prompt
    assert "EVIDENCE" in prompt


def test_6_fake_runner_json_creates_explanation():
    e = de.explain_single_delta(_delta("d1", confidence=0.4), None, None, _runner_accept)
    assert e["status"] == "explained"
    assert e["critic"]["verdict"] == "accept"
    assert e["summary"] == "Изменено сечение кабеля"
    # string-runner НЕ выдаётся за claude: provider не хардкодится
    assert e["model"]["provider"] == "custom_runner"


def test_7_broken_json_fail_soft():
    e = de.explain_single_delta(_delta("d1", confidence=0.4), None, None, _runner_broken)
    assert e["status"] in ("failed", "needs_human_review")
    assert "llm_response_parse_failed" in e["quality_flags"]
    assert e["critic"]["verdict"] == "needs_human_review"


def test_8_no_runner_skipped_not_crash():
    e = de.explain_single_delta(_delta("d1", confidence=0.4), None, None, None)
    assert e["status"] == "skipped_no_runner"
    assert "skipped_no_runner" in e["quality_flags"]
    assert e["model"]["provider"] == "none"


def test_9_ocr_noise_counted_in_summary():
    rep = de.explain_entity_diff_report(
        _diff_report([_delta("d1", confidence=0.4)]), None, None, _runner_ocr_noise)
    assert rep["summary"]["possible_ocr_noise_total"] == 1
    assert rep["explanations"][0]["status"] == "needs_human_review"


def test_10_needs_human_review_counted():
    rep = de.explain_entity_diff_report(
        _diff_report([_delta("d1", confidence=0.4)]), None, None, _runner_nhr)
    assert rep["summary"]["needs_human_review_total"] >= 1


def test_11_weak_graphic_adds_possible_weak_graphic():
    delta = _delta("d1", confidence=0.4, left_block_id="L9", right_block_id="R9")
    gr = _graphic_report(_descriptor("L9", readiness="not_usable",
                                     flags=["needs_vision_enrichment", "graphic_without_key_entities"]))
    rep = de.explain_entity_diff_report(_diff_report([delta]), gr, None, _runner_accept)
    e = rep["explanations"][0]
    assert "possible_weak_graphic" in e["quality_flags"]
    assert e["graphic_context"]["readiness"] == "not_usable"
    assert rep["summary"]["possible_weak_graphic_total"] >= 1


def test_12_coverage_notes_without_llm():
    delta = _delta("d1", confidence=0.4, left_block_id="L9")
    gr = _graphic_report(
        _descriptor("L9", readiness="not_usable", flags=["needs_vision_enrichment"]),
        matched=[{"block_match_id": "bm_9", "left_block_id": "L9", "right_block_id": "R9",
                  "risk_flags": ["one_side_not_usable", "low_token_overlap"]}])
    # llm_runner=None → объяснения skipped, но coverage_notes всё равно есть
    rep = de.explain_entity_diff_report(_diff_report([delta]), gr, None, None)
    kinds = {n["kind"] for n in rep["coverage_notes"]}
    assert "weak_graphic" in kinds
    assert "matched_risk" in kinds
    assert rep["explanations"][0]["status"] == "skipped_no_runner"


def test_13_write_report_valid_json(tmp_path: Path):
    rep = de.explain_entity_diff_report(
        _diff_report([_delta("d1", confidence=0.4)]), None, None, _runner_accept)
    out = tmp_path / "sub" / "delta_explanation_report.json"
    returned = de.write_delta_explanation_report(out, rep)
    assert returned == out and out.exists()
    reloaded = json.loads(out.read_text(encoding="utf-8"))
    assert reloaded["kind"] == de.REPORT_KIND
    assert reloaded == rep


def test_14_no_network_in_tests(monkeypatch):
    import socket

    def _boom(*a, **k):  # pragma: no cover
        raise AssertionError("network access attempted in delta explanation")

    monkeypatch.setattr(socket, "socket", _boom)
    monkeypatch.setattr(socket, "create_connection", _boom)

    rep = de.explain_entity_diff_report(
        _diff_report([_delta("d1", confidence=0.4)]), None, None, _runner_accept)
    assert rep["summary"]["selected_total"] == 1


def test_15_no_provider_imports():
    src = Path(de.__file__).read_text(encoding="utf-8")
    for forbidden in ("import requests", "import httpx", "urllib.request",
                      "import socket", "import subprocess", "graphic_llm",
                      "text_llm_provider", "ClaudeCodeProvider", "claude -p",
                      "qwen", "opus"):
        assert forbidden not in src, f"module references {forbidden!r}"


def test_16_integration_full_flow():
    deltas = [
        _delta("d_changed", delta_type="changed", confidence=0.4,
               old_value="ВВГ 3x2.5", new_value="ВВГ 3x4"),
        _delta("d_clear", confidence=0.95),                        # пропустится
        _delta("d_added", delta_type="added", entity_type="equipment",
               new_value="видеорегистратор", confidence=0.7,
               left_block_id="L9", right_block_id="R9"),
    ]
    gr = _graphic_report(_descriptor("L9", readiness="low",
                                     flags=["needs_vision_enrichment"]))
    rep = de.explain_entity_diff_report(_diff_report(deltas), gr, None, _runner_accept)
    assert rep["kind"] == de.REPORT_KIND
    assert rep["summary"]["deltas_total"] == 3
    # d_clear high-conf пропущен → выбраны 2
    assert rep["summary"]["selected_total"] == 2
    sel_ids = set(rep["selection"]["selected_delta_ids"])
    assert "d_clear" not in sel_ids
    assert {"d_changed", "d_added"} <= sel_ids
    # explained есть, coverage note по L9 (low + needs_vision)
    assert rep["summary"]["explained_total"] >= 1
    assert any(n["kind"] == "weak_graphic" for n in rep["coverage_notes"])


# ─── доп. юнит-проверки ──────────────────────────────────────────────────────


def test_parse_caps_and_enums():
    out = de.parse_delta_explanation_response(json.dumps({
        "summary": "x" * 5000, "risk_level": "СУПЕР-ВЫСОКИЙ",
        "critic": {"verdict": "weird"}}))
    assert out["parse_ok"] is True
    assert len(out["summary"]) <= 1201
    assert out["risk_level"] == "unknown"           # невалидный enum → unknown
    assert out["critic"]["verdict"] == "needs_human_review"  # невалидный → дефолт


def test_parse_broken_returns_fail_soft():
    out = de.parse_delta_explanation_response("no json here")
    assert out["parse_ok"] is False
    assert out["critic"]["verdict"] == "needs_human_review"


def test_runner_dict_return_and_exception():
    # dict-возврат провайдера
    e1 = de.explain_single_delta(_delta("d1", confidence=0.4), None, None,
                                 lambda p: {"status": "ok", "raw_response": _runner_accept(p)})
    assert e1["status"] == "explained"
    # исключение раннера → failed, не падает
    def _raises(p):
        raise RuntimeError("boom")
    e2 = de.explain_single_delta(_delta("d2", confidence=0.4), None, None, _raises)
    assert e2["status"] == "failed"
    assert e2["model"]["raw_status"] == "failed"


# ─── runner provider metadata (2026-06-10) ───────────────────────────────────


def test_runner_dict_provider_mock_propagates():
    def runner(prompt: str) -> dict:
        return {"status": "ok", "raw_response": _runner_accept(prompt),
                "provider": "mock", "model": "fake-model-1"}

    e = de.explain_single_delta(_delta("d1", confidence=0.4), None, None, runner)
    assert e["status"] == "explained"
    assert e["model"]["provider"] == "mock"
    assert e["model"]["model"] == "fake-model-1"


def test_runner_string_provider_is_custom_not_claude():
    e = de.explain_single_delta(_delta("d1", confidence=0.4), None, None, _runner_accept)
    assert e["model"]["provider"] in ("custom_runner", "injected")
    assert e["model"]["provider"] != "claude"


def test_runner_dict_without_provider_is_custom():
    def runner(prompt: str) -> dict:
        return {"status": "ok", "raw_response": _runner_accept(prompt)}

    e = de.explain_single_delta(_delta("d1", confidence=0.4), None, None, runner)
    assert e["model"]["provider"] == "custom_runner"


def test_runner_claude_wrapper_can_declare_itself():
    def runner(prompt: str) -> dict:
        return {"status": "ok", "raw_response": _runner_accept(prompt),
                "provider": "claude"}

    e = de.explain_single_delta(_delta("d1", confidence=0.4), None, None, runner)
    assert e["model"]["provider"] == "claude"

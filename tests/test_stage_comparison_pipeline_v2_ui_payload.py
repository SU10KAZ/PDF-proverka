# -*- coding: utf-8 -*-
"""Тесты для Pipeline V2 — offline UI payload adapter (контракт портала).

Synthetic summary + diff + explanations. Никаких реальных LLM/network.

Покрываемые spec-кейсы:
  1.  payload создаётся из synthetic summary + diff + explanations;
  2.  accept + show=true → confirmed_changes;
  3.  needs_human_review → needs_review;
  4.  possible_weak_graphic → weak_graphic_review;
  5.  possible_ocr_noise + show=false → noise + default_visible=false;
  6.  skipped_no_runner/failed → llm_failed_or_skipped;
  7.  одна delta не дублируется в нескольких секциях;
  8.  cards содержат compact fields;
  9.  длинные old/new/summary обрезаются;
 10.  filters собирают entity_type/risk/verdict/delta_type;
 11.  missing explanation не ломает payload;
 12.  missing entity_diff_report не ломает payload, но даёт warning;
 13.  write_pipeline_v2_ui_payload пишет валидный JSON;
 14.  модуль без сетевых вызовов и без Qwen/Opus/provider-импортов.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import pipeline_v2_dry_run as dr
from backend.app.services.stage_comparison import pipeline_v2_ui_payload as ui


# ─── builders (по образу summary_sections тестов) ────────────────────────────


def _delta(did, *, entity_type="stamp_field", delta_type="changed",
           subject="organization", field="value",
           old="ARTEL", new="ИНПАД", confidence=0.85):
    return {
        "delta_id": did, "delta_type": delta_type, "entity_type": entity_type,
        "semantic_group": "stamp", "left_entity_id": "el", "right_entity_id": "er",
        "left_block_id": "L1", "right_block_id": "R1", "block_match_id": "bm_1",
        "page_numbers": {"left": 1, "right": 2}, "subject": subject,
        "field": field, "old_value": old, "new_value": new,
        "change_summary": f"{entity_type}: {old} → {new}", "confidence": confidence,
        "evidence": {"left": {"quote": old, "source": "stamp_data",
                              "block_id": "L1", "page_number": 1},
                     "right": {"quote": new, "source": "stamp_data",
                               "block_id": "R1", "page_number": 2}},
        "match": {"method": "subject_type", "score": 1.0, "reasons": []},
        "quality_flags": [],
    }


def _expl(did, *, status="explained", verdict="accept", show=True,
          grounded="grounded", risk="medium", raw_status="ok",
          graphic_context=None, quality_flags=None, summary=None,
          input_delta=None):
    return {
        "explanation_id": f"expl_{did}", "delta_id": did,
        "mode": "explain_and_critic",
        "summary": summary if summary is not None else f"Объяснение {did}",
        "engineering_meaning": "…",
        "contractor_impact": f"Влияние {did}", "risk_level": risk,
        "groundedness": {"verdict": grounded, "reason": "",
                         "uses_left_evidence": True, "uses_right_evidence": True},
        "critic": {"verdict": verdict, "reason": "",
                   "should_show_to_engineer": show},
        "graphic_context": graphic_context or {
            "readiness": "medium", "needs_vision_enrichment": False,
            "manual_review_recommended": False, "notes": []},
        # как в реальном explain_single_delta: input_delta хранит срез дельты
        "input_delta": input_delta if input_delta is not None else {
            "delta_type": "changed", "entity_type": "stamp_field",
            "old_value": "ARTEL", "new_value": "ИНПАД"},
        "model": {"provider": "mock", "raw_status": raw_status, "error": None},
        "quality_flags": quality_flags or [], "status": status,
    }


def _diff_report(deltas):
    return {
        "version": 1, "kind": "stage_comparison_pipeline_v2_entity_diff",
        "summary": {"deltas_total": len(deltas)}, "deltas": deltas,
        "matched_entity_pairs": [], "unmatched_left_entities": [],
        "unmatched_right_entities": [], "block_summaries": [], "warnings": [],
    }


def _de_report(explanations, coverage_notes=None):
    return {
        "version": 1, "kind": "stage_comparison_pipeline_v2_delta_explanation",
        "summary": {"deltas_total": 99, "selected_total": len(explanations)},
        "selection": {"strategy": "changed_only",
                      "selected_delta_ids": [e["delta_id"] for e in explanations]},
        "explanations": explanations,
        "coverage_notes": coverage_notes or [], "warnings": [],
    }


def _summary(diff_report, de_report, *, status="ok", warnings=None,
             artifacts=None, graphic_descriptor=None):
    """Минимальный summary в форме pipeline_v2_summary.json."""
    return {
        "version": 1, "kind": "stage_comparison_pipeline_v2_dry_run_summary",
        "status": status,
        "artifacts": artifacts or {"summary_json": "pipeline_v2_summary.json",
                                   "entity_diff": "entity_diff_report.json"},
        "inputs": {},
        "stages": {"entity_diff": {
            "deltas_total": (diff_report.get("summary") or {}).get("deltas_total", 0)}},
        "graphic_descriptor": graphic_descriptor or {
            "left_graphic_blocks_total": 2, "right_graphic_blocks_total": 2,
            "left_usable_for_diff_total": 2, "right_usable_for_diff_total": 1,
            "left_needs_vision_enrichment_total": 0,
            "right_needs_vision_enrichment_total": 1,
            "left_manual_review_recommended_total": 0,
            "right_manual_review_recommended_total": 0,
            "by_readiness": {"high": 3, "low": 1},
        },
        "delta_explanation": {},
        "delta_sections": dr.build_delta_sections(diff_report, de_report),
        "warnings": warnings or [],
        "next_recommended_stage": "delta_explanation",
    }


_FIVE_KEYS = ("confirmed_changes", "needs_review", "weak_graphic_review",
              "likely_noise_hidden_by_default", "llm_failed_or_skipped")


def _full_reports():
    """Синтетика, покрывающая все 5 секций."""
    deltas = [
        _delta("d_conf"),
        _delta("d_review", subject="document_code",
               old="АА/БЭ-03-П-С", new="АА/БЭ-03-ДСЗ-С"),
        _delta("d_weak", entity_type="contents_item"),
        _delta("d_noise", subject="project_name",
               old='"Жилой комплекс"', new="«Жилой комплекс»"),
        _delta("d_fail", delta_type="added", old="", new="видеорегистратор"),
    ]
    expls = [
        _expl("d_conf", verdict="accept", show=True, grounded="grounded"),
        _expl("d_review", status="needs_human_review",
              verdict="needs_human_review", risk="medium"),
        _expl("d_weak", status="needs_human_review", verdict="possible_weak_graphic",
              graphic_context={"readiness": "not_usable",
                               "needs_vision_enrichment": True,
                               "manual_review_recommended": True, "notes": []}),
        _expl("d_noise", status="needs_human_review", verdict="possible_ocr_noise",
              show=False, risk="none"),
        _expl("d_fail", status="failed", verdict="needs_human_review",
              raw_status="failed"),
    ]
    notes = [{"kind": "weak_graphic", "block_id": "B", "message": "m"}] * 3 + \
            [{"kind": "matched_risk", "block_match_id": "bm", "message": "m"}] * 2
    return _diff_report(deltas), _de_report(expls, notes)


def _full_payload():
    diff, de = _full_reports()
    return ui.build_pipeline_v2_ui_payload(_summary(diff, de), diff, de)


def _section(payload, key):
    return next(sec for sec in payload["sections"] if sec["key"] == key)


# ─── 1: payload строится ─────────────────────────────────────────────────────


def test_1_payload_builds_from_synthetic_reports():
    payload = _full_payload()
    assert payload["version"] == ui.PAYLOAD_VERSION
    assert payload["kind"] == ui.PAYLOAD_KIND
    assert payload["status"] == "ok"
    keys = [sec["key"] for sec in payload["sections"]]
    assert keys == list(_FIVE_KEYS)
    hl = payload["headline"]
    assert hl["deltas_total"] == 5
    assert hl["selected_for_explanation_total"] == 5
    assert (hl["confirmed_total"] + hl["needs_review_total"]
            + hl["weak_graphic_total"] + hl["hidden_noise_total"]
            + hl["failed_or_skipped_total"]) == 5
    assert hl["coverage_notes_total"] == 5
    assert payload["artifact_refs"]["summary_json"] == "pipeline_v2_summary.json"


# ─── 2-6: маршрутизация по секциям (через payload) ───────────────────────────


def test_2_accept_show_lands_in_confirmed():
    payload = _full_payload()
    sec = _section(payload, "confirmed_changes")
    assert sec["count"] == 1 and sec["delta_ids"] == ["d_conf"]
    card = sec["cards"][0]
    assert card["delta_id"] == "d_conf"
    assert card["critic_verdict"] == "accept"
    assert card["section"] == "confirmed_changes"
    assert sec["default_visible"] is True


def test_3_needs_human_review_lands_in_needs_review():
    payload = _full_payload()
    sec = _section(payload, "needs_review")
    assert sec["delta_ids"] == ["d_review"]
    assert sec["default_visible"] is True
    assert sec["cards"][0]["critic_verdict"] == "needs_human_review"


def test_4_possible_weak_graphic_lands_in_weak_section():
    payload = _full_payload()
    sec = _section(payload, "weak_graphic_review")
    assert sec["delta_ids"] == ["d_weak"]
    # показывается по умолчанию, но как предупреждение
    assert sec["default_visible"] is True
    assert sec["display_hint"] == "warning"


def test_5_ocr_noise_hidden_by_default():
    payload = _full_payload()
    sec = _section(payload, "likely_noise_hidden_by_default")
    assert sec["delta_ids"] == ["d_noise"]
    assert sec["default_visible"] is False
    assert sec["cards"][0]["should_show_to_engineer"] is False


def test_6_failed_and_skipped_land_in_llm_failed_section():
    diff, de = _full_reports()
    de["explanations"].append(
        _expl("d_skip", status="skipped_no_runner",
              verdict="needs_human_review", raw_status="skipped",
              quality_flags=["skipped_no_runner"]))
    diff["deltas"].append(_delta("d_skip"))
    diff["summary"]["deltas_total"] = 6
    payload = ui.build_pipeline_v2_ui_payload(_summary(diff, de), diff, de)
    sec = _section(payload, "llm_failed_or_skipped")
    assert set(sec["delta_ids"]) == {"d_fail", "d_skip"}
    # скрыта по умолчанию, но видна в диагностике
    assert sec["default_visible"] is False
    assert sec["show_in_diagnostics"] is True


# ─── 7: нет дублирования ─────────────────────────────────────────────────────


def test_7_delta_never_duplicated_across_sections():
    payload = _full_payload()
    all_ids: list = []
    for sec in payload["sections"]:
        all_ids.extend(sec["delta_ids"])
        assert [c["delta_id"] for c in sec["cards"]] == sec["delta_ids"]
    assert len(all_ids) == len(set(all_ids)) == 5


# ─── 8: compact card fields ──────────────────────────────────────────────────


def test_8_cards_contain_compact_fields():
    payload = _full_payload()
    card = _section(payload, "confirmed_changes")["cards"][0]
    for key in ("delta_id", "section", "title", "subtitle", "entity_type",
                "delta_type", "field", "subject", "old_value", "new_value",
                "confidence", "risk_level", "critic_verdict", "groundedness",
                "should_show_to_engineer", "summary", "contractor_impact",
                "quality_flags", "page_numbers", "block_ids"):
        assert key in card, f"card lacks {key}"
    assert card["subtitle"] == "stamp_field · changed · medium risk"
    assert card["page_numbers"] == {"left": 1, "right": 2}
    assert card["block_ids"] == {"left": "L1", "right": "R1"}
    # гигантские quotes не тащим — evidence только через page/block ids
    assert "evidence" not in card
    assert card["badge"] == "confirmed"


# ─── 9: truncation ───────────────────────────────────────────────────────────


def test_9_long_values_truncated():
    long_old = "А" * 500
    long_new = "Б" * 500
    long_summary = "Очень длинное объяснение. " * 50
    diff = _diff_report([_delta("d_long", old=long_old, new=long_new)])
    de = _de_report([_expl("d_long", summary=long_summary)])
    payload = ui.build_pipeline_v2_ui_payload(_summary(diff, de), diff, de)
    card = _section(payload, "confirmed_changes")["cards"][0]
    for field in ("old_value", "new_value", "summary", "contractor_impact"):
        assert len(card[field]) <= 160
    assert card["old_value"].endswith("…")
    assert card["summary"].endswith("…")
    # title тоже ограничен
    assert len(card["title"]) <= 120


def test_truncate_ui_text_unit():
    assert ui.truncate_ui_text(None) == ""
    assert ui.truncate_ui_text("  короткий  ") == "короткий"
    out = ui.truncate_ui_text("x" * 300, 100)
    assert len(out) == 100 and out.endswith("…")


# ─── 10: filters ─────────────────────────────────────────────────────────────


def test_10_filters_collect_values():
    payload = _full_payload()
    f = payload["filters"]
    assert set(f.keys()) == {"entity_types", "risk_levels",
                             "critic_verdicts", "delta_types"}
    assert "stamp_field" in f["entity_types"]
    assert "contents_item" in f["entity_types"]
    assert {"medium", "none"} <= set(f["risk_levels"])
    assert {"accept", "needs_human_review", "possible_ocr_noise",
            "possible_weak_graphic"} <= set(f["critic_verdicts"])
    assert {"changed", "added"} <= set(f["delta_types"])


# ─── 11-12: деградация на неполных входах ────────────────────────────────────


def test_11_missing_explanation_does_not_break_payload():
    diff, de = _full_reports()
    # у d_conf пропало explanation (например, частичный отчёт)
    de["explanations"] = [e for e in de["explanations"]
                          if e["delta_id"] != "d_conf"]
    payload = ui.build_pipeline_v2_ui_payload(_summary(*_full_reports()), diff, de)
    sec = _section(payload, "confirmed_changes")
    # id остался (из summary.delta_sections), карточка построена из дельты
    assert sec["delta_ids"] == ["d_conf"]
    card = sec["cards"][0]
    assert card["delta_id"] == "d_conf"
    assert card["critic_verdict"] is None
    assert card["old_value"] == "ARTEL"


def test_12_missing_diff_report_warns_but_builds():
    diff, de = _full_reports()
    payload = ui.build_pipeline_v2_ui_payload(_summary(diff, de), None, de)
    assert any("entity_diff_report_missing" in w for w in payload["warnings"])
    # статус честно деградирует с ok → completed_with_warnings
    assert payload["status"] == "completed_with_warnings"
    # карточки построены из explanation.input_delta — фолбэк реально работает
    sec = _section(payload, "confirmed_changes")
    assert sec["count"] == 1
    card = sec["cards"][0]
    assert card["entity_type"] == "stamp_field"
    assert card["delta_type"] == "changed"
    assert card["old_value"] == "ARTEL" and card["new_value"] == "ИНПАД"


def test_12b_missing_both_reports_keeps_counts_from_summary():
    diff, de = _full_reports()
    payload = ui.build_pipeline_v2_ui_payload(_summary(diff, de), None, None)
    hl = payload["headline"]
    assert hl["confirmed_total"] == 1 and hl["failed_or_skipped_total"] == 1
    # карточек нет, но id и counts сохранены + warnings о расхождении
    sec = _section(payload, "confirmed_changes")
    assert sec["count"] == 1 and sec["cards"] == []
    assert any("without card data" in w for w in payload["warnings"])


def test_12c_summary_without_delta_sections_rebuilds_from_reports():
    diff, de = _full_reports()
    summary = _summary(diff, de)
    summary.pop("delta_sections")
    payload = ui.build_pipeline_v2_ui_payload(summary, diff, de)
    assert any("delta_sections_rebuilt_from_reports" in w
               for w in payload["warnings"])
    assert _section(payload, "confirmed_changes")["delta_ids"] == ["d_conf"]


def test_12d_invalid_summary_fails_soft():
    payload = ui.build_pipeline_v2_ui_payload(None)  # type: ignore[arg-type]
    assert payload["status"] == "failed"
    assert any("summary_missing_or_invalid" in w for w in payload["warnings"])
    assert [sec["key"] for sec in payload["sections"]] == list(_FIVE_KEYS)
    assert payload["headline"]["deltas_total"] == 0


# ─── 13: запись JSON ─────────────────────────────────────────────────────────


def test_13_write_payload_valid_json(tmp_path):
    payload = _full_payload()
    out = ui.write_pipeline_v2_ui_payload(
        tmp_path / "ui" / "pipeline_v2_ui_payload.json", payload)
    assert out.exists()
    reloaded = json.loads(out.read_text(encoding="utf-8"))
    assert reloaded == payload
    assert reloaded["kind"] == ui.PAYLOAD_KIND
    # tmp-файлов не осталось
    assert not list(out.parent.glob("*.tmp"))


# ─── 14: офлайн-гарантии ─────────────────────────────────────────────────────


def test_14_no_network_during_build(monkeypatch):
    import socket

    def _boom(*a, **k):  # pragma: no cover
        raise AssertionError("network access attempted in ui payload")

    monkeypatch.setattr(socket, "socket", _boom)
    monkeypatch.setattr(socket, "create_connection", _boom)
    payload = _full_payload()
    assert payload["headline"]["selected_for_explanation_total"] == 5


def test_14b_no_provider_imports():
    src = Path(ui.__file__).read_text(encoding="utf-8")
    for forbidden in ("import requests", "import httpx", "urllib.request",
                      "import socket", "import subprocess", "graphic_llm",
                      "text_llm_provider", "ClaudeCodeProvider", "claude -p",
                      "qwen", "opus", "llm_runner", "fastapi", "router"):
        assert forbidden.lower() not in src.lower(), \
            f"module references {forbidden!r}"


# ─── дополнительные контрактные проверки ─────────────────────────────────────


def test_badge_mapping_and_unknown_section():
    assert ui.classify_ui_badge("confirmed_changes") == "confirmed"
    assert ui.classify_ui_badge("needs_review") == "review"
    assert ui.classify_ui_badge("weak_graphic_review") == "weak_graphic"
    assert ui.classify_ui_badge("likely_noise_hidden_by_default") == "noise"
    assert ui.classify_ui_badge("llm_failed_or_skipped") == "failed"
    assert ui.classify_ui_badge("something_new") == "review"


def test_ux_defaults_contract():
    payload = _full_payload()
    expected = {
        "confirmed_changes": (True, "normal", False),
        "needs_review": (True, "normal", False),
        "weak_graphic_review": (True, "warning", False),
        "likely_noise_hidden_by_default": (False, "hidden", False),
        "llm_failed_or_skipped": (False, "diagnostics", True),
    }
    for sec in payload["sections"]:
        vis, hint, diag = expected[sec["key"]]
        assert sec["default_visible"] is vis
        assert sec["display_hint"] == hint
        assert sec["show_in_diagnostics"] is diag
        assert sec["title"] and sec["description"]


def test_graphic_readiness_section():
    diff, de = _full_reports()
    payload = ui.build_pipeline_v2_ui_payload(_summary(diff, de), diff, de)
    gr = payload["graphic_readiness"]
    assert gr["graphic_blocks_total"] == 4
    assert gr["status"] == "needs_vision_enrichment"
    assert gr["by_readiness"] == {"high": 3, "low": 1}
    # weak_blocks_preview появляется только если передан graphic-отчёт
    assert "weak_blocks_preview" not in gr
    gdr = {"descriptors": [
        {"block_id": "G1", "page_number": 3,
         "diff_readiness": {"readiness": "not_usable"}, "quality_flags": []},
        {"block_id": "G2", "page_number": 4,
         "diff_readiness": {"readiness": "high"}, "quality_flags": []},
    ]}
    payload2 = ui.build_pipeline_v2_ui_payload(_summary(diff, de), diff, de,
                                               graphic_descriptor_reports=gdr)
    preview = payload2["graphic_readiness"]["weak_blocks_preview"]
    assert preview == [{"block_id": "G1", "page_number": 3,
                        "readiness": "not_usable"}]


def test_unknown_future_section_is_kept_with_warning():
    diff, de = _full_reports()
    summary = _summary(diff, de)
    summary["delta_sections"]["future_section"] = {
        "count": 1, "delta_ids": ["d_conf"], "description": "future"}
    payload = ui.build_pipeline_v2_ui_payload(summary, diff, de)
    assert any("unknown_delta_section:future_section" in w
               for w in payload["warnings"])
    extra = _section(payload, "future_section")
    assert extra["badge"] == "review" and extra["count"] == 1


def test_payload_json_serializable_unicode():
    payload = _full_payload()
    text = json.dumps(payload, ensure_ascii=False)
    assert "ИНПАД" in text and "«Жилой комплекс»" in text


# ─── kill-тесты по адверсариальному ревью ────────────────────────────────────


def test_quality_flags_union_and_dedup():
    d = _delta("d1")
    d["quality_flags"] = ["needs_human_review", "possible_ocr_noise"]
    e = _expl("d1", quality_flags=["possible_ocr_noise", "skipped_no_runner"])
    card = ui.format_ui_delta_card(d, e)
    assert card["quality_flags"] == [
        "needs_human_review", "possible_ocr_noise", "skipped_no_runner"]


def test_headline_per_section_mapping_distinct_counts():
    """Счётчики headline указывают на СВОИ секции (1/3/1/2/1 — без swap'ов)."""
    deltas, expls = [], []
    deltas.append(_delta("c1")); expls.append(_expl("c1"))
    for i in range(3):
        deltas.append(_delta(f"r{i}"))
        expls.append(_expl(f"r{i}", status="needs_human_review",
                           verdict="needs_human_review"))
    deltas.append(_delta("w1"))
    expls.append(_expl("w1", status="needs_human_review",
                       verdict="possible_weak_graphic",
                       graphic_context={"readiness": "not_usable",
                                        "needs_vision_enrichment": True,
                                        "manual_review_recommended": True,
                                        "notes": []}))
    for i in range(2):
        deltas.append(_delta(f"n{i}"))
        expls.append(_expl(f"n{i}", status="needs_human_review",
                           verdict="possible_ocr_noise", show=False, risk="none"))
    deltas.append(_delta("f1"))
    expls.append(_expl("f1", status="failed", verdict="needs_human_review",
                       raw_status="failed"))
    diff, de = _diff_report(deltas), _de_report(expls)
    hl = ui.build_pipeline_v2_ui_payload(_summary(diff, de), diff, de)["headline"]
    assert hl["confirmed_total"] == 1
    assert hl["needs_review_total"] == 3
    assert hl["weak_graphic_total"] == 1
    assert hl["hidden_noise_total"] == 2
    assert hl["failed_or_skipped_total"] == 1


def test_max_cards_per_section_unit_and_payload():
    deltas = [_delta(f"d{i}") for i in range(3)]
    expls = [_expl(f"d{i}") for i in range(3)]
    by_id = {d["delta_id"]: d for d in deltas}
    e_by_id = {e["delta_id"]: e for e in expls}
    ids = [d["delta_id"] for d in deltas]
    # unit: явный cap режет карточки, порядок первых сохраняется
    cards = ui.build_ui_section_cards("confirmed_changes", ids, by_id, e_by_id,
                                      {"max_cards_per_section": 2})
    assert [c["delta_id"] for c in cards] == ["d0", "d1"]
    # без cap — все
    cards_all = ui.build_ui_section_cards("confirmed_changes", ids, by_id, e_by_id)
    assert len(cards_all) == 3
    # payload: ЯВНЫЙ cap при полных данных — НЕ ложный warning и НЕ деградация
    diff, de = _diff_report(deltas), _de_report(expls)
    payload = ui.build_pipeline_v2_ui_payload(
        _summary(diff, de), diff, de, options={"max_cards_per_section": 2})
    sec = _section(payload, "confirmed_changes")
    assert len(sec["cards"]) == 2 and sec["count"] == 3
    assert not any("without card data" in w for w in payload["warnings"])
    assert not any("truncated" in w for w in payload["warnings"])
    assert payload["status"] == "ok"


def test_default_cap_truncation_is_not_silent():
    deltas = [_delta(f"d{i}") for i in range(101)]
    expls = [_expl(f"d{i}") for i in range(101)]
    diff, de = _diff_report(deltas), _de_report(expls)
    payload = ui.build_pipeline_v2_ui_payload(_summary(diff, de), diff, de)
    sec = _section(payload, "confirmed_changes")
    assert len(sec["cards"]) == 100
    assert sec["count"] == 101 and len(sec["delta_ids"]) == 101
    assert any("cards truncated to 100 of 101" in w for w in payload["warnings"])
    assert payload["status"] == "completed_with_warnings"


def test_missing_card_data_warning_counts_only_examined():
    """Warning «without card data» считает реально отсутствующие данные."""
    deltas = [_delta("d0"), _delta("d1")]          # d2 нигде нет
    expls = [_expl("d0"), _expl("d1"), _expl("d2")]
    diff = _diff_report(deltas)
    de = _de_report(expls)
    summary = _summary(diff, de)
    # выкинем d2 из обоих отчётов, но оставим в delta_sections
    de["explanations"] = de["explanations"][:2]
    payload = ui.build_pipeline_v2_ui_payload(
        summary, diff, de, options={"max_cards_per_section": 150})
    warns = [w for w in payload["warnings"] if "without card data" in w]
    assert warns == ["section_confirmed_changes: 1 delta(s) without card data"]


@pytest.mark.parametrize("status,expected,unknown_warn", [
    ("ok", "ok", False),
    ("completed_with_warnings", "completed_with_warnings", False),
    ("failed", "failed", False),
    ("running", "completed_with_warnings", True),
])
def test_summary_status_passthrough(status, expected, unknown_warn):
    diff, de = _full_reports()
    payload = ui.build_pipeline_v2_ui_payload(
        _summary(diff, de, status=status), diff, de)
    assert payload["status"] == expected
    has_warn = any(w.startswith("unknown_summary_status") for w in payload["warnings"])
    assert has_warn is unknown_warn


def test_summary_warnings_propagate_without_status_escalation():
    diff, de = _full_reports()
    payload = ui.build_pipeline_v2_ui_payload(
        _summary(diff, de, warnings=["upstream_warn_x"]), diff, de)
    assert "upstream_warn_x" in payload["warnings"]
    # warnings самого summary НЕ деградируют статус (dry run уже учёл их)
    assert payload["status"] == "ok"


def test_graphic_readiness_status_priorities():
    base = {"left_graphic_blocks_total": 1, "right_graphic_blocks_total": 1,
            "left_usable_for_diff_total": 1, "right_usable_for_diff_total": 0}
    # manual_review > vision
    gd = dict(base, right_manual_review_recommended_total=1,
              left_needs_vision_enrichment_total=2)
    sec = ui._graphic_readiness_section(gd)
    assert sec["status"] == "manual_review_required"
    assert sec["usable_for_diff_total"] == 1
    # not_usable в by_readiness тоже эскалирует
    sec2 = ui._graphic_readiness_section(dict(base, by_readiness={"not_usable": 1}))
    assert sec2["status"] == "manual_review_required"
    # нет блоков
    assert ui._graphic_readiness_section({})["status"] == "no_graphic_blocks"
    # всё чисто
    assert ui._graphic_readiness_section(base)["status"] == "ok"


def test_weak_blocks_preview_flags_limit_and_combined_form():
    # flag-based отбор при readiness=high (канонический weak-набор)
    flagged = {"block_id": "F1", "page_number": 7,
               "diff_readiness": {"readiness": "high"},
               "quality_flags": ["graphic_without_text_layer"]}
    many = [{"block_id": f"W{i}", "page_number": i,
             "diff_readiness": {"readiness": "low"}, "quality_flags": []}
            for i in range(25)]
    combined = {"left": {"descriptors": [flagged]},
                "right": {"descriptors": many}}
    preview = ui._collect_weak_blocks_preview(combined)
    assert preview[0]["block_id"] == "F1"          # combined-форма + flag-отбор
    assert len(preview) == 20                       # лимит ≤20


def test_format_ui_delta_card_standalone():
    # без options.section — lazy-классификация по explanation
    card = ui.format_ui_delta_card(_delta("d1"), _expl("d1"))
    assert card["section"] == "confirmed_changes"
    assert card["badge"] == "confirmed"
    # delta без explanation — section неизвестен, безопасный badge
    card2 = ui.format_ui_delta_card(_delta("d1"))
    assert card2["section"] is None
    assert card2["badge"] == "review"
    assert card2["critic_verdict"] is None


def test_section_count_trusts_summary_value():
    diff, de = _full_reports()
    summary = _summary(diff, de)
    summary["delta_sections"]["confirmed_changes"]["count"] = 7
    payload = ui.build_pipeline_v2_ui_payload(summary, diff, de)
    assert _section(payload, "confirmed_changes")["count"] == 7
    assert payload["headline"]["confirmed_total"] == 7


def test_deltas_total_fallback_to_diff_summary():
    diff, de = _full_reports()
    summary = _summary(diff, de)
    summary.pop("stages")
    payload = ui.build_pipeline_v2_ui_payload(summary, diff, de)
    assert payload["headline"]["deltas_total"] == 5


def test_truncate_small_limits():
    assert ui.truncate_ui_text("abc", 1) == "a"
    assert ui.truncate_ui_text("abc", 0) == ""


def test_filters_skip_empty_values():
    d = _delta("d1")
    diff, de = _diff_report([d]), _de_report([])  # карточка без explanation
    summary = _summary(diff, de)
    summary["delta_sections"] = {
        "selected_total": 1,
        "confirmed_changes": {"count": 1, "delta_ids": ["d1"],
                              "description": "", "examples": []},
    }
    payload = ui.build_pipeline_v2_ui_payload(summary, diff, de)
    f = payload["filters"]
    assert "" not in f["risk_levels"] and "" not in f["critic_verdicts"]
    assert f["risk_levels"] == [] and f["critic_verdicts"] == []
    assert f["entity_types"] == ["stamp_field"]


def test_rebuild_sections_from_explanations_alone():
    diff, de = _full_reports()
    summary = _summary(diff, de)
    summary.pop("delta_sections")
    payload = ui.build_pipeline_v2_ui_payload(summary, None, de)
    assert any("delta_sections_rebuilt_from_reports" in w
               for w in payload["warnings"])
    sec = _section(payload, "confirmed_changes")
    assert sec["delta_ids"] == ["d_conf"]
    # карточка из input_delta (diff отсутствует)
    assert sec["cards"][0]["old_value"] == "ARTEL"


def test_failsoft_on_junk_artifact_fields():
    """Junk в полях summary/отчётов → warning/деградация, но НЕ краш."""
    diff, de = _full_reports()
    summary = _summary(diff, de)
    summary["artifacts"] = ["not", "a", "dict"]
    summary["warnings"] = "oops-string"
    summary["graphic_descriptor"] = {
        "left_graphic_blocks_total": None,
        "right_graphic_blocks_total": "2",
        "by_readiness": ["low"],
    }
    summary["delta_sections"]["selected_total"] = "abc"
    summary["delta_sections"]["coverage_notes"] = {"count": "xyz"}
    summary["delta_sections"]["needs_review"]["delta_ids"] = "d_review"
    summary["delta_sections"]["weak_graphic_review"]["delta_ids"] = [
        {"bad": 1}, "d_weak", None]
    diff["summary"] = ["junk"]
    de["explanations"][0]["quality_flags"] = ["ok_flag", 7, None]
    payload = ui.build_pipeline_v2_ui_payload(summary, diff, de)
    assert payload["status"] == "completed_with_warnings"
    assert payload["artifact_refs"] == {}
    assert any("artifact_refs_invalid_ignored" in w for w in payload["warnings"])
    assert any("summary_warnings_invalid_ignored" in w for w in payload["warnings"])
    # строка-«список» delta_ids не разбирается посимвольно
    assert _section(payload, "needs_review")["delta_ids"] == []
    assert any("invalid delta_ids ignored" in w for w in payload["warnings"])
    # junk-элементы выкинуты, валидный id остался
    assert _section(payload, "weak_graphic_review")["delta_ids"] == ["d_weak"]
    assert any("invalid delta_ids dropped" in w for w in payload["warnings"])
    # счётчики не падают: None → 0, числовая строка "2" восстанавливается
    assert payload["headline"]["selected_for_explanation_total"] == 0
    assert payload["headline"]["coverage_notes_total"] == 0
    assert payload["graphic_readiness"]["graphic_blocks_total"] == 2
    assert payload["graphic_readiness"]["by_readiness"] == {}
    # mixed-type quality_flags коэрцированы в строки
    card = _section(payload, "confirmed_changes")["cards"][0]
    assert card["quality_flags"] == ["7", "ok_flag"]
    # payload остаётся JSON-сериализуемым
    json.dumps(payload, ensure_ascii=False)


def test_weak_graphic_flags_match_coverage_notes_definition():
    """Канонический weak-набор един с delta_explanation (coverage_notes)."""
    from backend.app.services.stage_comparison import (
        pipeline_v2_delta_explanation as de_mod,
    )
    assert ui._WEAK_GRAPHIC_FLAGS == de_mod._WEAK_GRAPHIC_FLAGS
    assert "graphic_without_text_layer" in ui._WEAK_GRAPHIC_FLAGS

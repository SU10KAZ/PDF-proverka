# -*- coding: utf-8 -*-
"""Тесты Pipeline V2 Grounded Vision Evidence Integration (mark-only).

Покрытие spec-кейсов (1–18):

  1.  Exact grounded change → дельта grounded.
  2.  Grounded old/new entities → changed delta grounded.
  3.  Weakly grounded → weak evidence.
  4.  Ungrounded не маппится как usable evidence.
  5.  Rejected designator range не факт.
  6.  Rejected noop не факт (conflict-сигнал на changed).
  7.  «QF5 400А → 200А» grounded (designator-anchored).
  8.  «400 А» матчит «400А» (нормализация).
  9.  «ТА1-ТА9» матчит «TA1-TA9» (гомоглифы).
  10. Block/page mismatch снижает score / не маппит (вне scope → нет карточки).
  11. Missing grounding report → skipped_no_grounding (не падает).
  12. Dry-run пишет grounded_evidence_report.json.
  13. Dry-run fail-soft (битый grounding не валит pipeline).
  14. Delta explanation prompt включает grounded evidence.
  15. Prompt помечает weak evidence как WEAK(hint).
  16. Prompt не включает rejected как факт.
  17. UI payload summary appears.
  18. Старые delta-explanation тесты зелёные (отдельный файл, проверяется в CI).

Чистый offline: без сети/Qwen/Opus/реального LLM-runner'а.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import pipeline_v2_grounded_evidence as ge
from backend.app.services.stage_comparison import pipeline_v2_delta_explanation as de


# ─── фикстуры данных ─────────────────────────────────────────────────────────

LB = "7EMD-DT4R-6TN"   # OLD graphic block (cabinet scheme)
RB = "763U-YFTA-DVQ"   # NEW graphic block


def _grounding_report():
    """Grounding с QF5(400А)→QF5(200А) grounded + weak + rejected кейсами."""
    return {
        "version": 1,
        "kind": "stage_comparison_pipeline_v2_graphic_vision_grounding",
        "status": "ok",
        "summary": {"items_total": 1},
        "items": [{
            "item_id": f"gv_{LB}__{RB}",
            "left_block_id": LB, "right_block_id": RB,
            "graphic_type": "cabinet_scheme", "vision_status": "ok",
            "left_anchors": {"block_id": LB, "available": True, "source": "full_text",
                             "ratings": ["400a", "800a", "63a"]},
            "right_anchors": {"block_id": RB, "available": True, "source": "full_text",
                              "ratings": ["200a", "630a"]},
            "grounded_entities_old": [
                {"value": "QF5 (400А)", "normalized": "qf5 (400a)", "status": "grounded",
                 "matched_values": ["400a"], "missing_values": [], "reason": "grounded"},
                {"value": "QF6 (800А)", "normalized": "qf6 (800a)", "status": "grounded",
                 "matched_values": ["800a"], "missing_values": [], "reason": "grounded"},
                {"value": "QF7 (63А)", "normalized": "qf7 (63a)", "status": "grounded",
                 "matched_values": ["63a"], "missing_values": [], "reason": "grounded"},
            ],
            "grounded_entities_new": [
                {"value": "QF5 (200А)", "normalized": "qf5 (200a)", "status": "grounded",
                 "matched_values": ["200a"], "missing_values": [], "reason": "grounded"},
                {"value": "QF6 (630А)", "normalized": "qf6 (630a)", "status": "grounded",
                 "matched_values": ["630a"], "missing_values": [], "reason": "grounded"},
                {"value": "QF1 (16А)", "normalized": "qf1 (16a)", "status": "weakly_grounded",
                 "matched_values": [], "missing_values": ["16a"], "reason": "partial_match"},
            ],
            "grounded_changes": [
                {"value": "QF6: 800А → 630А", "status": "grounded",
                 "old_values": ["800a"], "new_values": ["630a"], "reason": "grounded"},
            ],
            "rejected_entities": [
                {"value": "ТТ1-ТТ19", "normalized": "tt1-tt19",
                 "status": "rejected_designator_range",
                 "reason": "artificial_designator_range"},
            ],
            "rejected_changes": [
                {"value": "ППГнг 4x2,5 → 4x2,5 (без изменений)",
                 "status": "rejected_noop", "old_values": ["4x2.5"],
                 "new_values": ["4x2.5"], "reason": "noop_change"},
            ],
        }],
        "warnings": [],
    }


def _diff_report(deltas):
    return {"version": 1, "kind": "stage_comparison_pipeline_v2_entity_diff",
            "summary": {"deltas_total": len(deltas)}, "deltas": deltas, "warnings": []}


def _delta(delta_id, dtype, old_v, new_v, *, lb=LB, rb=RB, etype="power_supply"):
    return {"delta_id": delta_id, "delta_type": dtype, "entity_type": etype,
            "semantic_group": "power", "old_value": old_v, "new_value": new_v,
            "left_block_id": lb, "right_block_id": rb,
            "page_numbers": {"left": 52, "right": 21}}


def _gate():
    return {"version": 1, "kind": "stage_comparison_pipeline_v2_visual_equivalence_gate",
            "status": "ok", "block_pairs": [
                {"left_block_id": LB, "right_block_id": RB,
                 "left_page_number": 52, "right_page_number": 21}]}


def _card_for(report, delta_id):
    return next((c for c in report["delta_evidence"] if c["delta_id"] == delta_id), None)


# ─── 1: exact grounded change → grounded ─────────────────────────────────────

def test_1_exact_grounded_change_maps():
    d = _delta("qf6", "changed", "800А", "630А")
    rep = ge.build_grounded_evidence_report(_diff_report([d]), _grounding_report())
    card = _card_for(rep, "qf6")
    assert card and card["evidence_level"] == ge.LEVEL_GROUNDED
    assert card["use_in_critic"] is True
    assert any(e["fact_level"] == ge.FACT_CONFIRMED for e in card["evidence"])


# ─── 2: grounded old/new entities → changed grounded ─────────────────────────

def test_2_grounded_entities_pair_maps_changed():
    d = _delta("qf5", "changed", "400А", "200А")
    rep = ge.build_grounded_evidence_report(_diff_report([d]), _grounding_report())
    card = _card_for(rep, "qf5")
    assert card["evidence_level"] == ge.LEVEL_GROUNDED
    ev = card["evidence"][0]
    assert ev["old_anchor"] == "QF5 (400А)" and ev["new_anchor"] == "QF5 (200А)"


# ─── 3: weakly grounded → weak ───────────────────────────────────────────────

def test_3_weakly_grounded_maps_weak():
    # added 16А → только weak QF1 (16А) на NEW
    d = _delta("qf1add", "added", "", "16А")
    rep = ge.build_grounded_evidence_report(_diff_report([d]), _grounding_report())
    card = _card_for(rep, "qf1add")
    assert card["evidence_level"] == ge.LEVEL_WEAK
    assert card["use_in_critic"] is True
    assert all(e["fact_level"] != ge.FACT_CONFIRMED for e in card["evidence"])


# ─── 4: ungrounded не usable ─────────────────────────────────────────────────

def test_4_ungrounded_not_usable():
    # значение, которого нет ни в одном anchor → none
    d = _delta("ghost", "changed", "999А", "888А")
    rep = ge.build_grounded_evidence_report(_diff_report([d]), _grounding_report())
    card = _card_for(rep, "ghost")
    assert card["evidence_level"] == ge.LEVEL_NONE
    assert card["use_in_critic"] is False
    assert card["evidence"] == []


# ─── 5: rejected designator range не факт ─────────────────────────────────────

def test_5_rejected_designator_range_not_fact():
    d = _delta("ttrange", "added", "", "ТТ1-ТТ19", etype="equipment")
    rep = ge.build_grounded_evidence_report(_diff_report([d]), _grounding_report())
    card = _card_for(rep, "ttrange")
    assert card["evidence_level"] in (ge.LEVEL_REJECTED_ONLY, ge.LEVEL_CONFLICT)
    assert card["use_in_critic"] is False
    assert all(e["fact_level"] == ge.FACT_REJECTED for e in card["evidence"])


# ─── 6: rejected noop не факт (conflict) ─────────────────────────────────────

def test_6_rejected_noop_not_fact():
    d = _delta("noop", "changed", "4х2,5", "4х2,5", etype="cable")
    rep = ge.build_grounded_evidence_report(_diff_report([d]), _grounding_report())
    card = _card_for(rep, "noop")
    assert card["use_in_critic"] is False
    assert card["evidence_level"] in (ge.LEVEL_CONFLICT, ge.LEVEL_REJECTED_ONLY)
    assert any(e.get("status") == "rejected_noop" for e in card["evidence"])


# ─── 7: QF5 400А→200А grounded (designator-anchored) ─────────────────────────

def test_7_qf5_designator_anchored_grounded():
    d = _delta("qf5d", "changed", "400А", "200А")
    rep = ge.build_grounded_evidence_report(_diff_report([d]), _grounding_report())
    card = _card_for(rep, "qf5d")
    top = card["evidence"][0]
    assert top["kind"] == "designator_pair" and top["designator"] == "qf5"
    assert card["evidence_level"] == ge.LEVEL_GROUNDED


# ─── 8: «400 А» матчит «400А» (нормализация) ─────────────────────────────────

def test_8_spaced_amp_normalizes():
    assert ge.normalize_evidence_token("400 А") == ge.normalize_evidence_token("400А")
    d = _delta("spaced", "changed", "400 А", "200 А")
    rep = ge.build_grounded_evidence_report(_diff_report([d]), _grounding_report())
    assert _card_for(rep, "spaced")["evidence_level"] == ge.LEVEL_GROUNDED


# ─── 9: «ТА1-ТА9» матчит «TA1-TA9» ───────────────────────────────────────────

def test_9_homoglyph_designator_normalizes():
    # кириллическая ТА и латинская TA → один токен
    assert ge.normalize_evidence_token("ТА1-ТА9", compact=True) == \
        ge.normalize_evidence_token("TA1-TA9", compact=True)
    assert "ta1-ta9" in ge.normalize_evidence_token("ТА1–ТА9", compact=True)


# ─── 10: block/page mismatch — вне scope → нет карточки ───────────────────────

def test_10_block_mismatch_out_of_scope():
    # дельта на блоках, которых нет в grounding → не vision-relevant
    d = _delta("other", "changed", "400А", "200А", lb="OTHER-L", rb="OTHER-R")
    rep = ge.build_grounded_evidence_report(_diff_report([d]), _grounding_report())
    assert _card_for(rep, "other") is None
    assert rep["summary"]["deltas_total"] == 0


# ─── 11: missing grounding → skipped_no_grounding ────────────────────────────

def test_11_missing_grounding_skipped():
    d = _delta("x", "changed", "400А", "200А")
    rep = ge.build_grounded_evidence_report(_diff_report([d]), None)
    assert rep["status"] == "skipped_no_grounding"
    assert rep["delta_evidence"] == []
    rep2 = ge.build_grounded_evidence_report(_diff_report([d]), {"items": []})
    assert rep2["status"] == "skipped_no_grounding"


# ─── 12: dry-run пишет grounded_evidence_report.json ─────────────────────────

def _seed_packages(tmp_path: Path):
    """Минимальные left/right result.json для offline dry-run."""
    def _rj(blocks):
        return {"document": {"pages": [{"page_number": 1, "blocks": blocks}]}}
    left = tmp_path / "left.json"
    right = tmp_path / "right.json"
    left.write_text(json.dumps(_rj([
        {"block_id": LB, "block_type": "image", "bbox": [0, 0, 100, 100],
         "text": "QF5 400А"}])), encoding="utf-8")
    right.write_text(json.dumps(_rj([
        {"block_id": RB, "block_type": "image", "bbox": [0, 0, 100, 100],
         "text": "QF5 200А"}])), encoding="utf-8")
    return {"result_json_path": str(left)}, {"result_json_path": str(right)}


def test_12_dry_run_writes_report(tmp_path):
    from backend.app.services.stage_comparison import pipeline_v2_dry_run as dr
    lp, rp = _seed_packages(tmp_path)
    out = tmp_path / "out"
    summary = dr.run_pipeline_v2_dry_run(lp, rp, out)
    # артефакт может быть skipped_no_grounding (vision off в offline), но stage
    # секция и путь должны существовать в карте артефактов
    paths = dr.build_pipeline_v2_artifact_paths(out)
    assert paths["grounded_evidence"].name == "grounded_evidence_report.json"
    assert "grounded_evidence" in summary
    assert summary["grounded_evidence"]["enabled"] in (True, False)


def test_12b_dry_run_writes_report_with_seeded_grounding(tmp_path, monkeypatch):
    """Принудительно вернуть grounding-итемы → grounded_evidence пишется на диск."""
    from backend.app.services.stage_comparison import pipeline_v2_dry_run as dr
    lp, rp = _seed_packages(tmp_path)
    out = tmp_path / "out2"

    # seed vision (чтобы grounding включился) + grounding с нашими итемами
    monkeypatch.setattr(dr, "run_graphic_vision_enrichment",
                        lambda *a, **k: {"status": "ok", "items": [{"item_id": "x"}],
                                         "summary": {}, "warnings": []})
    monkeypatch.setattr(dr, "build_graphic_vision_grounding_report",
                        lambda *a, **k: _grounding_report())

    # дельта QF5 400→200 на нужных блоках появится из реального diff? нет —
    # поэтому прямо подменим diff, добавив нашу дельту
    real_diff = dr.diff_entity_extraction_report
    def _patched_diff(entity_report, opts=None):
        rep = real_diff(entity_report, opts)
        rep.setdefault("deltas", []).append(_delta("qf5seed", "changed", "400А", "200А"))
        rep["summary"]["deltas_total"] = len(rep["deltas"])
        return rep
    monkeypatch.setattr(dr, "diff_entity_extraction_report", _patched_diff)

    summary = dr.run_pipeline_v2_dry_run(
        lp, rp, out, options={"graphic_vision": {"enabled": True}})
    report_path = out / "grounded_evidence_report.json"
    assert report_path.exists()
    rep = json.loads(report_path.read_text(encoding="utf-8"))
    assert rep["kind"] == ge.REPORT_KIND
    card = _card_for(rep, "qf5seed")
    assert card and card["evidence_level"] == ge.LEVEL_GROUNDED
    assert summary["grounded_evidence"]["deltas_with_grounded_evidence"] >= 1


# ─── 13: dry-run fail-soft ────────────────────────────────────────────────────

def test_13_build_report_fail_soft_on_broken_grounding():
    # grounding с битым item (не dict в items) не должен ронять билдер
    broken = {"status": "ok", "items": [None, 123, {"left_block_id": LB,
              "right_block_id": RB, "grounded_entities_old": "not-a-list"}]}
    d = _delta("x", "changed", "400А", "200А")
    rep = ge.build_grounded_evidence_report(_diff_report([d]), broken)
    assert rep["status"] in ("ok", "completed_with_warnings", "skipped_no_grounding")
    assert isinstance(rep["delta_evidence"], list)


def test_13b_dry_run_fail_soft_on_grounding_exception(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import pipeline_v2_dry_run as dr
    lp, rp = _seed_packages(tmp_path)
    out = tmp_path / "out3"
    monkeypatch.setattr(dr, "run_graphic_vision_enrichment",
                        lambda *a, **k: {"status": "ok", "items": [{"item_id": "x"}],
                                         "summary": {}, "warnings": []})
    monkeypatch.setattr(dr, "build_graphic_vision_grounding_report",
                        lambda *a, **k: _grounding_report())
    def _boom(*a, **k):
        raise RuntimeError("grounded evidence boom")
    monkeypatch.setattr(dr, "build_grounded_evidence_report", _boom)
    summary = dr.run_pipeline_v2_dry_run(
        lp, rp, out, options={"graphic_vision": {"enabled": True}})
    # pipeline НЕ падает; статус не failed из-за этого слоя
    assert summary["status"] in ("ok", "completed_with_warnings")
    assert summary["grounded_evidence"].get("error")


# ─── 14: prompt включает grounded evidence ───────────────────────────────────

def test_14_prompt_includes_grounded_evidence():
    d = _delta("p", "changed", "400А", "200А")
    card = ge.build_delta_evidence_card(
        d, [{"source": "graphic_vision_grounding", "fact_level": "confirmed",
             "status": "grounded", "kind": "designator_pair", "designator": "qf5",
             "old_anchor": "QF5 (400А)", "new_anchor": "QF5 (200А)",
             "match_score": 0.97}])
    prompt = de.build_delta_explanation_prompt(d, grounded_evidence=card)
    assert "GROUNDED VISION EVIDENCE" in prompt
    assert "qf5" in prompt and "QF5 (400А)" in prompt
    assert "supporting" in prompt.lower() or "ПОДТВЕРЖДАЮЩИЙ" in prompt


# ─── 15: prompt помечает weak как WEAK ───────────────────────────────────────

def test_15_prompt_labels_weak():
    d = _delta("p", "added", "", "16А")
    card = {"evidence_level": "weak", "evidence": [
        {"fact_level": "weak", "status": "weakly_grounded", "kind": "entity_single",
         "new_anchor": "QF1 (16А)", "match_score": 0.55}]}
    prompt = de.build_delta_explanation_prompt(d, grounded_evidence=card)
    assert "WEAK(hint)" in prompt
    assert "ПОДСКАЗКУ" in prompt   # weak = hint requiring manual review


# ─── 16: prompt не включает rejected как факт ────────────────────────────────

def test_16_prompt_excludes_rejected_as_fact():
    # anchor отличается от значения дельты, чтобы проверить именно НЕ-всплытие
    # rejected-якоря в evidence-секции (значение дельты само по себе в prompt'е)
    d = _delta("p", "added", "", "63А")
    card = {"evidence_level": "rejected_only", "evidence": [
        {"fact_level": "rejected", "status": "rejected_designator_range",
         "old_anchor": "ТТ1-ТТ19-RANGE", "new_anchor": "ТТ1-ТТ19-RANGE",
         "match_score": 0.4}]}
    prompt = de.build_delta_explanation_prompt(d, grounded_evidence=card)
    # rejected-якорь НЕ всплывает как факт; вместо этого предупреждение
    assert "ТТ1-ТТ19-RANGE" not in prompt
    assert "НЕ использовать" in prompt and "НЕ факт" in prompt


def test_16b_prompt_without_evidence_unchanged():
    """Без grounded_evidence prompt идентичен прежнему (backward-compat)."""
    d = _delta("p", "changed", "400А", "200А")
    prompt = de.build_delta_explanation_prompt(d)
    assert "GROUNDED VISION EVIDENCE" not in prompt
    assert "ПРАВИЛА ПО GROUNDED VISION EVIDENCE" not in prompt


# ─── 17: UI payload summary appears ──────────────────────────────────────────

def test_17_ui_payload_summary_appears():
    from backend.app.services.stage_comparison import pipeline_v2_ui_payload as up
    from backend.app.services.stage_comparison import pipeline_v2_dry_run as dr
    d = _delta("qf5", "changed", "400А", "200А")
    ge_rep = ge.build_grounded_evidence_report(_diff_report([d]), _grounding_report(),
                                               visual_gate_report=_gate())
    sec = dr._grounded_evidence_section(ge_rep, True, None)
    summary = {"status": "ok", "stages": {"entity_diff": {"deltas_total": 1}},
               "grounded_evidence": sec, "delta_sections": {}}
    payload = up.build_pipeline_v2_ui_payload(summary)
    assert "grounded_evidence" in payload
    assert payload["grounded_evidence"]["available"] is True
    assert payload["grounded_evidence"]["deltas_with_grounded_evidence"] == 1


def test_17b_ui_payload_no_section_when_disabled():
    from backend.app.services.stage_comparison import pipeline_v2_ui_payload as up
    summary = {"status": "ok", "stages": {}, "delta_sections": {},
               "grounded_evidence": {"enabled": False, "status": "disabled"}}
    payload = up.build_pipeline_v2_ui_payload(summary)
    assert "grounded_evidence" not in payload


# ─── 18: backward-compat (старый delta_explanation путь) ─────────────────────

def test_18_explain_report_without_evidence_still_works():
    """explain_entity_diff_report без grounded_evidence_report = прежнее поведение."""
    diff = _diff_report([_delta("d1", "changed", "400А", "200А")])
    rep = de.explain_entity_diff_report(diff, llm_runner=None)
    assert rep["kind"] == de.REPORT_KIND
    # без runner'а — skipped_no_runner, поле grounded_evidence_level отсутствует
    expl = rep["explanations"][0]
    assert expl["status"] == "skipped_no_runner"
    assert "grounded_evidence_level" not in expl


def test_18b_explain_report_with_evidence_threads_level():
    """С grounded_evidence_report уровень прокидывается в explanation (fake runner)."""
    diff = _diff_report([_delta("d1", "changed", "400А", "200А")])
    ge_rep = ge.build_grounded_evidence_report(diff, _grounding_report())

    def fake_runner(prompt):
        # подтверждаем, что evidence реально дошёл до prompt'а
        assert "GROUNDED VISION EVIDENCE" in prompt
        return json.dumps({
            "summary": "smena nominala", "engineering_meaning": "x",
            "contractor_impact": "y", "risk_level": "medium",
            "groundedness": {"verdict": "grounded", "reason": "ok",
                             "uses_left_evidence": True, "uses_right_evidence": True},
            "critic": {"verdict": "accept", "reason": "grounded vision",
                       "should_show_to_engineer": True}})

    rep = de.explain_entity_diff_report(
        diff, options={"selection_strategy": "all"},
        llm_runner=fake_runner, grounded_evidence_report=ge_rep)
    expl = next(e for e in rep["explanations"] if e["delta_id"] == "d1")
    assert expl.get("grounded_evidence_level") == "grounded"
    assert expl.get("grounded_evidence_used") is True


# ─── score / normalize unit-level ────────────────────────────────────────────

def test_score_changed_requires_both_sides():
    d = _delta("s", "changed", "400А", "200А")
    cand_both = {"origin": "change", "old_values": {"400a"}, "new_values": {"200a"},
                 "text_tokens": set()}
    cand_one = {"origin": "change", "old_values": {"400a"}, "new_values": {"999a"},
                "text_tokens": set()}
    assert ge.score_grounded_evidence_match(d, cand_both) > \
        ge.score_grounded_evidence_match(d, cand_one)


def test_normalize_section_token():
    assert ge.normalize_evidence_token("4х185") == "4x185"
    assert ge.normalize_evidence_token("4×185") == "4x185"


# ─── UI badges: ui_payload per-delta evidence + compact cards ─────────────────

def _ui_inputs():
    """diff с дельтами на grounding-блоке + summary + ge-report."""
    from backend.app.services.stage_comparison import pipeline_v2_dry_run as dr
    deltas = [
        _delta("qf5", "changed", "400А", "200А"),
        _delta("qf6", "changed", "800А", "630А"),
        _delta("noop", "changed", "4х2,5", "4х2,5", etype="cable"),
        _delta("ghost", "changed", "999А", "888А"),
    ]
    diff = _diff_report(deltas)
    ge_rep = ge.build_grounded_evidence_report(diff, _grounding_report(),
                                               visual_gate_report=_gate())
    sec = dr._grounded_evidence_section(ge_rep, True, None)
    # делаем все 4 дельты секционными карточками (selection=all через fake de)
    de_rep = de.explain_entity_diff_report(
        diff, options={"selection_strategy": "all"}, llm_runner=None)
    summary = {"status": "ok", "stages": {"entity_diff": {"deltas_total": len(deltas)}},
               "grounded_evidence": sec,
               "delta_sections": dr.build_delta_sections(diff, de_rep)}
    return summary, diff, de_rep, ge_rep


# spec-1: UI payload contains grounded_evidence summary

def test_ui_1_payload_contains_ge_summary():
    from backend.app.services.stage_comparison import pipeline_v2_ui_payload as up
    summary, diff, de_rep, ge_rep = _ui_inputs()
    payload = up.build_pipeline_v2_ui_payload(
        summary, entity_diff_report=diff, delta_explanation_report=de_rep,
        grounded_evidence_report=ge_rep)
    g = payload["grounded_evidence"]
    assert g["available"] is True
    assert g["deltas_with_grounded_evidence"] >= 2     # qf5 + qf6
    assert isinstance(g["cards"], list) and g["cards"]
    # cards содержат badge + label + top_anchors
    grounded = [c for c in g["cards"] if c["evidence_level"] == "grounded"]
    assert grounded and grounded[0]["badge"] == "grounded"
    assert grounded[0]["label"] == "Grounded vision evidence"


# spec-2: UI payload attaches grounded_evidence to matching delta card

def test_ui_2_attaches_to_delta_card():
    from backend.app.services.stage_comparison import pipeline_v2_ui_payload as up
    summary, diff, de_rep, ge_rep = _ui_inputs()
    payload = up.build_pipeline_v2_ui_payload(
        summary, entity_diff_report=diff, delta_explanation_report=de_rep,
        grounded_evidence_report=ge_rep)
    cards = [c for sec in payload["sections"] for c in sec["cards"]
             if c.get("delta_id") == "qf5"]
    assert cards, "qf5 delta card must exist"
    ge_attached = cards[0].get("grounded_evidence")
    assert ge_attached and ge_attached["evidence_level"] == "grounded"
    assert ge_attached["badge"] == "grounded"
    assert ge_attached["top_anchors"]
    # attached_to_cards счётчик в секции
    assert payload["grounded_evidence"].get("attached_to_cards", 0) >= 1


# spec-6: missing grounded_evidence не ломает старые карточки

def test_ui_6_missing_ge_does_not_break():
    from backend.app.services.stage_comparison import pipeline_v2_ui_payload as up
    summary, diff, de_rep, ge_rep = _ui_inputs()
    summary.pop("grounded_evidence", None)
    payload = up.build_pipeline_v2_ui_payload(
        summary, entity_diff_report=diff, delta_explanation_report=de_rep)
    # секции есть, payload валиден, grounded_evidence отсутствует
    assert payload["status"] in ("ok", "completed_with_warnings")
    assert "grounded_evidence" not in payload
    assert any(sec["cards"] for sec in payload["sections"])
    for sec in payload["sections"]:
        for c in sec["cards"]:
            assert "grounded_evidence" not in c   # ничего не приклеено


# spec-7: rejected evidence не отдаётся как факт (нет top_anchors)

def test_ui_7_rejected_not_shown_as_fact():
    from backend.app.services.stage_comparison import pipeline_v2_ui_payload as up
    summary, diff, de_rep, ge_rep = _ui_inputs()
    payload = up.build_pipeline_v2_ui_payload(
        summary, entity_diff_report=diff, delta_explanation_report=de_rep,
        grounded_evidence_report=ge_rep)
    cards = {c["delta_id"]: c for c in payload["grounded_evidence"]["cards"]}
    noop = cards.get("noop")
    assert noop is not None
    assert noop["evidence_level"] in ("conflict", "rejected_only")
    assert noop["use_in_critic"] is False
    assert noop["top_anchors"] == []   # rejected — НЕ факт, anchors не отдаём


# compact helpers unit-level

def test_ui_compact_badge_and_label():
    from backend.app.services.stage_comparison import pipeline_v2_ui_payload as up
    assert up._ge_badge("grounded") == "grounded"
    assert up._ge_badge("weak") == "weak"
    assert up._ge_badge("conflict") == "conflict"
    assert up._ge_badge("rejected_only") == "conflict"
    assert up._ge_badge("none") == "none"
    assert up._ge_label("grounded") == "Grounded vision evidence"
    assert up._ge_label("none") == ""


def test_ui_compact_card_no_raw_or_fulltext():
    from backend.app.services.stage_comparison import pipeline_v2_ui_payload as up
    card = {"evidence_level": "grounded", "use_in_critic": True,
            "evidence": [{"fact_level": "confirmed", "status": "grounded",
                          "old_anchor": "QF5 (400А)" + " x" * 200,  # длинный
                          "new_anchor": "QF5 (200А)", "match_score": 0.97}]}
    compact = up.build_grounded_evidence_compact(card)
    assert compact["badge"] == "grounded"
    assert len(compact["top_anchors"]) == 1
    # длинный anchor обрезан
    assert len(compact["top_anchors"][0]["old_anchor"]) <= up._GE_ANCHOR_TEXT_MAX + 1


def test_ui_cards_capped_interesting_first():
    """cap=100, но grounded/weak/conflict не отрезаются (сортировка вперёд)."""
    from backend.app.services.stage_comparison import pipeline_v2_ui_payload as up
    # 120 none + 1 grounded в хвосте
    deltas = [_delta(f"n{i}", "changed", "111А", "222А") for i in range(120)]
    diff = _diff_report(deltas + [_delta("qf5", "changed", "400А", "200А")])
    ge_rep = ge.build_grounded_evidence_report(diff, _grounding_report())
    sec = up.build_grounded_evidence_ui(ge_rep)
    assert len(sec["cards"]) <= up._GE_CARDS_MAX
    ids = {c["delta_id"] for c in sec["cards"]}
    assert "qf5" in ids   # grounded не отрезан несмотря на 120 none перед ним

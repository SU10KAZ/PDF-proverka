"""Офлайн-тесты движка Evidence Agent v2 (EV2) — без ngrok/сети.

Портированы из experiments/evidence_agent_v2/test_*_offline.py после переноса
ядра в backend. Покрывают: парсер восприятия + guard, политику голосования
_aggregate, слияние F1–F9 + инвариант «норма не даёт reject», норм-путь,
кросс-блок эвристики и KB-фильтр запуска.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from backend.app.pipeline.stages.findings_review.evidence_agent_v2 import (
    cross_block as cb,
)
from backend.app.pipeline.stages.findings_review.evidence_agent_v2.cross_block import (
    CrossBlockSignal,
)
from backend.app.pipeline.stages.findings_review.evidence_agent_v2.extract import (
    Perception,
    _parse,
    _to_perception,
)
from backend.app.pipeline.stages.findings_review.evidence_agent_v2.fusion import fuse
from backend.app.pipeline.stages.findings_review.evidence_agent_v2.kb_routing import (
    should_run_evidence_verifier,
)
from backend.app.pipeline.stages.findings_review.evidence_agent_v2.norm_check import (
    HINT_ACCEPT_FLAG,
    HINT_NEUTRAL,
    HINT_NONE,
    HINT_SOFT_HUMAN,
    NormSignal,
    _classify_status,
    extract_norm_codes,
)
from backend.app.pipeline.stages.findings_review.evidence_agent_v2.verify import _aggregate


def P(contradicts, legible=True, model="m"):
    return Perception(contradicts, legible, "v", "s", "q", "n", {"x": 1}, "", model)


# ─────────────────────────── парсер восприятия ───────────────────────────
def test_parse_plain_object():
    obj = _parse('{"contradicts_finding":"yes","region_legible":true}')
    assert obj and obj["contradicts_finding"] == "yes"


def test_parse_object_with_surrounding_text():
    obj = _parse('Вот ответ: {"contradicts_finding":"no"} конец')
    assert obj and obj["contradicts_finding"] == "no"


def test_to_perception_invalid_contradicts_falls_back():
    p = _to_perception({"contradicts_finding": "maybe"}, "m")
    assert p.contradicts == "cannot_tell"


def test_yes_without_quote_downgraded_to_cannot_tell():
    p = _to_perception({"contradicts_finding": "yes", "evidence_quote": ""}, "m")
    assert p.contradicts == "cannot_tell"


def test_yes_with_quote_survives():
    p = _to_perception({"contradicts_finding": "yes", "evidence_quote": "h=2100"}, "m")
    assert p.contradicts == "yes"


# ─────────────────────────── политика голосования ───────────────────────────
def test_two_yes_gives_confident_reject():
    v = _aggregate("F-1", [P("yes"), P("yes")], "graphic", ["B1"])
    assert v.decision == "reject" and v.confidence == 1.0


def test_single_yes_never_rejects():
    v = _aggregate("F-1", [P("yes")], "graphic", ["B1"])
    assert v.decision == "borderline"


def test_one_yes_one_no_is_borderline():
    v = _aggregate("F-1", [P("yes"), P("no")], "graphic", ["B1"])
    assert v.decision == "borderline"


def test_no_majority_accepts():
    v = _aggregate("F-1", [P("no"), P("no")], "graphic", ["B1"])
    assert v.decision == "accept"


def test_cannot_tell_dominant_needs_human():
    v = _aggregate("F-1", [P("cannot_tell"), P("cannot_tell")], "graphic", ["B1"])
    assert v.decision == "needs_human"


def test_all_invalid_needs_human():
    bad = Perception("cannot_tell", False, "", "", "", "", None, "no_json", "m")
    v = _aggregate("F-1", [bad, bad], "graphic", ["B1"])
    assert v.decision == "needs_human" and v.votes["invalid"] == 2


def test_yes_with_one_cannot_tell_still_rejects_if_two_yes():
    v = _aggregate("F-1", [P("yes"), P("yes"), P("cannot_tell")], "graphic", ["B1"])
    assert v.decision == "reject"


# ─────────────────────────── слияние F1–F9 ───────────────────────────
@dataclass
class _Vis:
    decision: str
    confidence: float = 0.8
    votes: dict = field(default_factory=dict)


def test_f1_strong_visual_reject():
    v = fuse(_Vis("reject"), None, None, finding_id="F-1")
    assert v.decision == "reject" and v.source == "visual_strong"


def test_f2_crossblock_refute_plus_visual_borderline():
    xb = CrossBlockSignal(kind="xref_refutes", decision_hint="reject_candidate")
    v = fuse(_Vis("borderline"), None, xb, finding_id="F-1")
    assert v.decision == "reject" and v.source == "cross_block_strong"


def test_f3_visual_accept():
    v = fuse(_Vis("accept"), None, None, finding_id="F-1")
    assert v.decision == "accept"


def test_f4_norm_replaced_is_accept_not_reject():
    ns = NormSignal(kind="norm_replaced_flag", decision_hint="accept_with_flag",
                    flags=["norm_replaced:СП X"])
    v = fuse(None, ns, None, finding_id="F-1")
    assert v.decision == "accept" and v.taxonomy == "norm_superseded"
    assert "norm_replaced:СП X" in v.norm_flags


def test_f6_conflict_needs_human():
    xb = CrossBlockSignal(kind="xref_refutes", decision_hint="reject_candidate")
    v = fuse(_Vis("accept"), None, xb, finding_id="F-1")
    assert v.decision == "needs_human" and v.source == "conflict"


def test_f7_visual_abstain():
    v = fuse(_Vis("needs_human"), None, None, finding_id="F-1")
    assert v.decision == "needs_human"


def test_f8_norm_not_indexed():
    ns = NormSignal(kind="norm_not_indexed", decision_hint="soft_human")
    v = fuse(None, ns, None, finding_id="F-1")
    assert v.decision == "needs_human" and v.taxonomy == "norm_coverage_gap"


def test_f9_no_signal():
    v = fuse(None, None, None, finding_id="F-1")
    assert v.decision == "needs_human" and v.taxonomy == "insufficient_evidence"


def test_invariant_norm_cannot_cause_reject():
    for kind, hint in [
        ("norm_ok", "neutral"), ("norm_edition_flag", "accept_with_flag"),
        ("norm_replaced_flag", "accept_with_flag"), ("norm_cancelled_flag", "accept_with_flag"),
        ("norm_not_indexed", "soft_human"), ("norm_unsupported", "neutral"),
    ]:
        ns = NormSignal(kind=kind, decision_hint=hint)
        assert fuse(None, ns, None, finding_id="F").decision != "reject"
        assert fuse(_Vis("accept"), ns, None, finding_id="F").decision == "accept"


def test_invariant_crossblock_refute_alone_no_reject_without_visual():
    xb = CrossBlockSignal(kind="xref_refutes", decision_hint="reject_candidate")
    assert fuse(None, None, xb, finding_id="F").decision != "reject"


# ─────────────────────────── норм-путь ───────────────────────────
def _R(**kw):
    base = {"found": True, "status": "active", "resolution_reason": "exact"}
    base.update(kw)
    return base


def test_active_is_neutral():
    assert _classify_status(_R(), False) == ("norm_ok", HINT_NEUTRAL, 0.8)


def test_replaced_is_accept_with_flag_not_reject():
    kind, hint, _ = _classify_status(_R(status="replaced"), False)
    assert kind == "norm_replaced_flag" and hint == HINT_ACCEPT_FLAG


def test_cancelled_is_accept_with_flag():
    kind, hint, _ = _classify_status(_R(status="cancelled"), False)
    assert kind == "norm_cancelled_flag" and hint == HINT_ACCEPT_FLAG


def test_outdated_edition_flag():
    kind, hint, _ = _classify_status(_R(status="outdated_edition"), False)
    assert kind == "norm_edition_flag" and hint == HINT_ACCEPT_FLAG


def test_not_in_index_soft_human():
    kind, hint, _ = _classify_status(
        _R(found=False, status="unknown", resolution_reason="not_in_index"), False)
    assert kind == "norm_not_indexed" and hint == HINT_SOFT_HUMAN


def test_inferred_is_low_neutral():
    kind, hint, conf = _classify_status(_R(), True)
    assert kind == "norm_inferred" and hint == HINT_NEUTRAL and conf < 0.5


def test_norm_invariant_no_reject_hint_ever():
    for st in ("active", "replaced", "cancelled", "outdated_edition", "unknown"):
        for rr in ("exact", "alias", "not_in_index", "unsupported_family", "error"):
            _, hint, _ = _classify_status(_R(found=(st == "active"), status=st,
                                             resolution_reason=rr), False)
            assert hint in {HINT_NEUTRAL, HINT_ACCEPT_FLAG, HINT_SOFT_HUMAN, HINT_NONE}


def test_normsignal_coerces_bad_hint():
    s = NormSignal(decision_hint="reject")
    assert s.decision_hint == HINT_SOFT_HUMAN


def test_extract_codes_strips_explanation():
    codes, para, inferred = extract_norm_codes(
        {"norm": "СП 256.1325800.2016 (ред. 2024), п.7.1 — розетки в санузлах"})
    assert "СП 256.1325800.2016" in codes and para == "7.1" and inferred is False


def test_extract_codes_inferred_from_problem():
    codes, para, inferred = extract_norm_codes(
        {"norm": "", "problem": "нарушение ГОСТ 34028-2016 по арматуре"})
    assert codes and inferred is True


# ─────────────────────────── кросс-блок ───────────────────────────
def test_absence_detected():
    assert cb._ABSENCE_RE.search("марки отсутствуют в ведомости")
    assert cb._ABSENCE_RE.search("площадь не указана")
    assert not cb._ABSENCE_RE.search("толщина утеплителя 150 мм верна")


def test_salient_extracts_marks_and_numbers():
    s = cb._salient("марки 85, 86, 88, 89 и ⌀10")
    assert "85" in s and "86" in s


def test_empty_graph_returns_none():
    sig = cb.run_cross_block({"problem": "x"}, {})
    assert sig.kind == "none"


def test_crossblock_hint_never_reject():
    sig = cb.run_cross_block({"problem": "y"}, {})
    assert sig.decision_hint in ("neutral", "reject_candidate", "accept_candidate")


# ─────────────────────────── KB-фильтр запуска ───────────────────────────
def test_kb_routing_borderline_always_runs():
    run, reason = should_run_evidence_verifier({"id": "F-1"}, kb_decision={"llm_decision": "borderline"})
    assert run is True and reason == "kb_borderline"


def test_kb_routing_accept_skips():
    run, reason = should_run_evidence_verifier({"id": "F-1"}, kb_decision={"llm_decision": "accept"})
    assert run is False and reason == "kb_accept_skip"


def test_kb_routing_reject_graphic():
    finding = {"id": "F-1", "evidence": [{"type": "image", "block_id": "ABC-1"}]}
    run, reason = should_run_evidence_verifier(finding, kb_decision={"llm_decision": "reject"})
    assert run is True and reason == "kb_reject_graphic"


def test_kb_routing_critical_always_runs():
    # критическое замечание проверяем всегда, даже без KB-вердикта и прецедента
    finding = {"id": "F-1", "severity": "КРИТИЧЕСКОЕ"}
    run, reason = should_run_evidence_verifier(finding, use_precedent=False)
    assert run is True and reason == "critical"


def test_kb_routing_plain_finding_not_selected():
    # обычное некритическое замечание без прецедента — визуально НЕ проверяем (быстрый пропуск)
    finding = {"id": "F-1", "severity": "РЕКОМЕНДАТЕЛЬНОЕ"}
    run, reason = should_run_evidence_verifier(finding, use_precedent=False)
    assert run is False and reason == "not_selected"

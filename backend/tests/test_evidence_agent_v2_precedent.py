"""Тесты precedent-источника EV и ГЛАВНОГО ИНВАРИАНТА безопасности.

Проверяется машинно: precedent-сигнал НИКОГДА не приводит к reject и НИКОГДА не
понижает подтверждённый accept — как бы он ни комбинировался с visual/norm/cross_block.
Это страховка от переноса «перекоса в reject» самого KB-агента в удаление замечаний.
"""
from __future__ import annotations

from itertools import product

import pytest

from backend.app.pipeline.stages.findings_review.evidence_agent_v2.fusion import fuse
from backend.app.pipeline.stages.findings_review.evidence_agent_v2.precedent import (
    PrecedentSignal,
    run_precedent_check,
)


class _Visual:
    def __init__(self, decision):
        self.decision = decision
        self.confidence = 0.8
        self.votes = {}
        self.perceptions = []


class _Norm:
    def __init__(self, hint="none", kind="none"):
        self.decision_hint = hint
        self.kind = kind
        self.flags = []
        self.suggestions = {}
        self.confidence = 0.5
        self.reason = ""


class _Xb:
    def __init__(self, kind="none"):
        self.kind = kind
        self.candidate_block_ids = []


def _strong_precedent(enforce):
    return PrecedentSignal(
        kind="precedent_reject", hint="suspect_flag", enforce=enforce,
        confidence=0.7, taxonomy="matches_rejected_precedent",
        reason="3 похожих отклонённых", flags=["precedent_reject:DEC-1"],
        examples=[{"decision_id": "DEC-1", "score": 0.7}], top_score=0.7, n_matches=3,
    )


VIS = [None, _Visual("accept"), _Visual("reject"), _Visual("borderline"), _Visual("needs_human")]
NORM = [None, _Norm(), _Norm("accept_with_flag", "norm_replaced_flag"), _Norm("soft_human", "norm_not_indexed")]
XB = [None, _Xb(), _Xb("xref_refutes"), _Xb("xref_supports")]


@pytest.mark.parametrize("vis,norm,xb,enforce", list(product(VIS, NORM, XB, [False, True])))
def test_precedent_never_produces_reject(vis, norm, xb, enforce):
    """Инвариант №1: прецедент НЕ может породить reject ни в одной комбинации.

    reject допустим только если его дал сам визуал/кросс-блок — тогда прецедент к
    нему непричастен (он не трогает reject-исходы)."""
    base = fuse(vis, norm, xb, finding_id="F")
    withp = fuse(vis, norm, xb, _strong_precedent(enforce), finding_id="F")
    if withp.decision == "reject":
        # reject мог прийти ТОЛЬКО из базового вердикта, не из прецедента
        assert base.decision == "reject"
        assert withp.source != "precedent_flag"


@pytest.mark.parametrize("vis,norm,xb", list(product(VIS, NORM, XB)))
def test_precedent_never_downgrades_accept_or_reject(vis, norm, xb):
    """Инвариант №2: при enforce прецедент не меняет базовый accept/reject."""
    base = fuse(vis, norm, xb, finding_id="F")
    withp = fuse(vis, norm, xb, _strong_precedent(True), finding_id="F")
    if base.decision in ("accept", "reject"):
        assert withp.decision == base.decision


def test_shadow_does_not_change_decision():
    """SHADOW (enforce=False): вердикт идентичен базовому, но precedent-поля записаны."""
    vis = _Visual("needs_human")
    base = fuse(vis, None, None, finding_id="F")
    shadow = fuse(vis, None, None, _strong_precedent(False), finding_id="F")
    assert shadow.decision == base.decision
    assert shadow.precedent_hint == "suspect_flag"
    assert shadow.precedent_flags == ["precedent_reject:DEC-1"]


def test_enforce_lifts_needs_human_to_borderline():
    """ENFORCE: сильный прецедент поднимает needs_human → borderline + требует эксперта."""
    withp = fuse(None, None, None, _strong_precedent(True), finding_id="F")
    assert withp.decision == "borderline"
    assert withp.source == "precedent_flag"
    assert withp.requires_human_review is True
    assert "precedent" in withp.sources_used


def test_alone_precedent_is_borderline_not_reject():
    """Прецедент как ЕДИНСТВЕННЫЙ сигнал → максимум borderline, никогда не reject."""
    withp = fuse(None, None, None, _strong_precedent(True), finding_id="F")
    assert withp.decision != "reject"


def test_precedent_signal_hint_is_safe_by_type():
    """Нельзя сконструировать reject-хинт: недопустимое значение схлопывается."""
    sig = PrecedentSignal(hint="reject")
    assert sig.hint != "reject"


def test_run_precedent_check_failsoft_without_kb(monkeypatch):
    """Если ретривер недоступен — сигнал нейтральный, без исключения."""
    import backend.app.pipeline.stages.findings_review.evidence_agent_v2.precedent as pmod
    monkeypatch.setattr(pmod, "get_default_retriever", lambda: None)
    sig = pmod.run_precedent_check({"id": "F-1", "problem": "x"}, section="TX", enforce=True)
    assert sig.hint == "none"
    assert sig.kind == "none"


def test_run_precedent_check_with_stub_retriever():
    """Сильный матч → suspect_flag; примеры и флаги заполнены."""
    from backend.app.pipeline.stages.findings_review.evidence_agent_v2.precedent import (
        PrecedentRetriever,
    )
    entries = [{
        "id": "DEC-99", "section": "TX", "category": "documentation",
        "severity": "РЕКОМЕНДАТЕЛЬНОЕ", "expert_decision": "rejected",
        "summary": "дубль сечения кабеля указан в примечаниях",
        "expert_reason": "уже указано в общих примечаниях",
    }]
    retr = PrecedentRetriever(entries)
    finding = {"id": "F-1", "section": "TX", "category": "documentation",
               "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
               "problem": "дубль сечения кабеля в примечаниях"}
    sig = run_precedent_check(finding, section="TX", enforce=True, retriever=retr, min_score=0.45)
    assert sig.hint == "suspect_flag"
    assert sig.examples and sig.examples[0]["decision_id"] == "DEC-99"
    assert sig.flags == ["precedent_reject:DEC-99"]

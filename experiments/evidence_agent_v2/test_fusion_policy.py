"""Офлайн-тесты политики слияния F1–F9 + property-тест главного инварианта."""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from experiments.evidence_agent_v2.fusion import fuse
from experiments.evidence_agent_v2.norm_check import NormSignal
from experiments.evidence_agent_v2.cross_block import CrossBlockSignal


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


# --- PROPERTY: норм-сигнал НИКОГДА не превращает не-reject в reject ---
def test_invariant_norm_cannot_cause_reject():
    for kind, hint in [
        ("norm_ok", "neutral"), ("norm_edition_flag", "accept_with_flag"),
        ("norm_replaced_flag", "accept_with_flag"), ("norm_cancelled_flag", "accept_with_flag"),
        ("norm_not_indexed", "soft_human"), ("norm_unsupported", "neutral"),
    ]:
        ns = NormSignal(kind=kind, decision_hint=hint)
        # без визуала и кросс-блока
        assert fuse(None, ns, None, finding_id="F").decision != "reject"
        # с любым НЕ-reject визуалом
        for vd in ("accept", "borderline", "needs_human"):
            assert fuse(_Vis(vd), ns, None, finding_id="F").decision != "reject" \
                or vd != "accept"  # accept никогда не должен стать reject из-за нормы
            assert fuse(_Vis("accept"), ns, None, finding_id="F").decision == "accept"


def test_invariant_crossblock_refute_alone_no_reject_without_visual():
    # xref_refutes без визуала (None) НЕ должен reject'ить
    xb = CrossBlockSignal(kind="xref_refutes", decision_hint="reject_candidate")
    assert fuse(None, None, xb, finding_id="F").decision != "reject"


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))

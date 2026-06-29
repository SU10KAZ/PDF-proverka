"""Офлайн-тесты кросс-блок сигнала (эвристики, без реального графа)."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from experiments.evidence_agent_v2 import cross_block as cb


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


def test_signal_hint_never_reject():
    # кросс-блок сам не reject'ит: максимум reject_candidate
    sig = cb.run_cross_block({"problem": "y"}, {})
    assert sig.decision_hint in ("neutral", "reject_candidate", "accept_candidate")


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))

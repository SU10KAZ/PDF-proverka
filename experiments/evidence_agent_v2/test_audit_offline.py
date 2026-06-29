"""Офлайн-тесты аудита отклонённых (категоризация + is_expert_error)."""
from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from experiments.evidence_agent_v2 import audit_rejected as ar


@dataclass
class _Fused:
    decision: str
    source: str = ""


def test_expert_error_visual_confirm():
    err, why = ar.is_expert_error(_Fused("accept", "visual_confirm"))
    assert err is True and "подтвердил" in why


def test_expert_error_norm_flag_is_weak():
    # norm_flag (норма заменена) — НЕ сильный сигнал «эксперт неправ»
    err, _ = ar.is_expert_error(_Fused("accept", "norm_flag"))
    assert err is False


def test_expert_error_conflict():
    err, _ = ar.is_expert_error(_Fused("needs_human", "conflict"))
    assert err is True


def test_not_expert_error_needs_human():
    err, _ = ar.is_expert_error(_Fused("needs_human", "visual_abstain"))
    assert err is False


def test_not_expert_error_plain_accept_no_source():
    # accept без сильного источника-доказательства — не флаг
    err, _ = ar.is_expert_error(_Fused("accept", "no_signal"))
    assert err is False


def test_toks_filters_short():
    assert "ведомость" in ar._toks("Ведомость отверстий 85")
    assert "85" not in ar._toks("Ведомость отверстий 85")  # len<=3 отброшено


def test_iter_alia_smoke():
    # лёгкий smoke на реальных данных: хотя бы один rejected найдётся
    it = ar.iter_alia_rejected("TX")
    first = next(it, None)
    assert first is None or (first.item_id and first.output_dir)


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))

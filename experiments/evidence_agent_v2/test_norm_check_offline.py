"""Офлайн-тесты норм-пути (без реального индекса — синтетические resolved-dict)."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from experiments.evidence_agent_v2.norm_check import (
    HINT_ACCEPT_FLAG, HINT_NEUTRAL, HINT_NONE, HINT_SOFT_HUMAN,
    NormSignal, _classify_status, extract_norm_codes,
)


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


def test_invariant_no_reject_hint_ever():
    # любой статус → decision_hint никогда не «reject»
    for st in ("active", "replaced", "cancelled", "outdated_edition", "unknown"):
        for rr in ("exact", "alias", "not_in_index", "unsupported_family", "error"):
            _, hint, _ = _classify_status(_R(found=(st == "active"), status=st,
                                             resolution_reason=rr), False)
            assert hint in {HINT_NEUTRAL, HINT_ACCEPT_FLAG, HINT_SOFT_HUMAN, HINT_NONE}


def test_normsignal_coerces_bad_hint():
    s = NormSignal(decision_hint="reject")  # запрещённое → soft_human
    assert s.decision_hint == HINT_SOFT_HUMAN


def test_extract_codes_strips_explanation():
    codes, para, inferred = extract_norm_codes(
        {"norm": "СП 256.1325800.2016 (ред. 2024), п.7.1 — розетки в санузлах"})
    assert "СП 256.1325800.2016" in codes and para == "7.1" and inferred is False


def test_extract_codes_inferred_from_problem():
    codes, para, inferred = extract_norm_codes(
        {"norm": "", "problem": "нарушение ГОСТ 34028-2016 по арматуре"})
    assert codes and inferred is True


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))

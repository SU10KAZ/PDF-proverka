"""
test_dedup_safety.py
--------------------
Production safety invariants for Phase 0 dedup.

Critical contract (production rollout depends on these being green):
  - КРИТИЧЕСКОЕ findings count never decreases through dedup.
  - Output count never exceeds input count.
  - Deterministic output for identical input.
  - Same severity / problem_class / affected_system semantics work in both
    research-grade format (`ПРОВЕРИТЬ_ПО_СМЕЖНЫМ` with underscore) and
    production format (`ПРОВЕРИТЬ ПО СМЕЖНЫМ` with space).

Run: python -m pytest tests/findings/dedup/test_dedup_safety.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.services.findings.dedup import (  # noqa: E402
    collapse_to_canonical,
    fuzzy_dedup,
)


def _f(idx: int, severity: str = "ЭКСПЛУАТАЦИОННОЕ", problem: str = "default problem") -> dict:
    return {
        "id": f"T-{idx:03d}",
        "problem_class": "missing_mandatory_schedule",
        "affected_system": "ВРУ",
        "severity": severity,
        "category": "completeness",
        "problem": problem,
        "description": "Some description",
        "evidence_quote": "цитата",
        "confidence": 0.8,
    }


def test_critical_count_invariant_class_dedup():
    findings = [
        _f(0, "РЕКОМЕНДАТЕЛЬНОЕ"),
        _f(1, "КРИТИЧЕСКОЕ"),
        _f(2, "КРИТИЧЕСКОЕ"),
        _f(3, "ЭКОНОМИЧЕСКОЕ"),
        _f(4, "ЭКСПЛУАТАЦИОННОЕ"),
    ]
    crit_in = sum(1 for f in findings if f["severity"] == "КРИТИЧЕСКОЕ")
    kept, report = collapse_to_canonical(findings)
    crit_out = sum(1 for f in kept if f["severity"] == "КРИТИЧЕСКОЕ")
    assert crit_out >= crit_in, (
        f"class_dedup dropped critical: in={crit_in}, out={crit_out}"
    )


def test_critical_count_invariant_fuzzy_dedup():
    findings = [
        _f(0, "РЕКОМЕНДАТЕЛЬНОЕ", "X"),
        _f(1, "КРИТИЧЕСКОЕ", "X"),
        _f(2, "КРИТИЧЕСКОЕ", "X"),
        _f(3, "ЭКОНОМИЧЕСКОЕ", "X"),
        _f(4, "ЭКСПЛУАТАЦИОННОЕ", "X"),
    ]
    crit_in = sum(1 for f in findings if f["severity"] == "КРИТИЧЕСКОЕ")
    kept, report = fuzzy_dedup(findings)
    crit_out = sum(1 for f in kept if f["severity"] == "КРИТИЧЕСКОЕ")
    assert crit_out >= crit_in


def test_count_invariant_class_dedup():
    findings = [_f(i) for i in range(50)]
    kept, _ = collapse_to_canonical(findings)
    assert len(kept) <= 50


def test_count_invariant_fuzzy_dedup():
    findings = [_f(i, problem=f"problem {i}") for i in range(50)]
    kept, _ = fuzzy_dedup(findings)
    assert len(kept) <= 50


def test_chained_class_then_fuzzy_preserves_criticals():
    """Mirror the production runner: class_dedup → fuzzy_dedup."""
    items = [
        _f(0, "КРИТИЧЕСКОЕ", "Перегрузка кабеля"),
        _f(1, "КРИТИЧЕСКОЕ", "Перегрузка кабеля ВВГнг-FRLS"),
        _f(2, "РЕКОМЕНДАТЕЛЬНОЕ", "Перегрузка кабеля?"),
        _f(3, "ЭКОНОМИЧЕСКОЕ", "Спецификация неполная"),
        _f(4, "ЭКОНОМИЧЕСКОЕ", "Спецификация неполная"),
    ]
    crit_in = sum(1 for f in items if f["severity"] == "КРИТИЧЕСКОЕ")
    kept, _ = collapse_to_canonical(items)
    kept, _ = fuzzy_dedup(kept)
    crit_out = sum(1 for f in kept if f["severity"] == "КРИТИЧЕСКОЕ")
    assert crit_out >= crit_in, "chained dedup dropped a critical"


def test_production_severity_format_supported():
    """Production severity uses a space; research used an underscore."""
    a = {
        "id": "T-001",
        "problem_class": "missing_schedule",
        "affected_system": "ВРУ",
        "severity": "ПРОВЕРИТЬ ПО СМЕЖНЫМ",
        "category": "completeness",
        "problem": "Кабельный журнал",
        "description": "desc",
        "evidence_quote": "ев",
    }
    b = dict(a, id="T-002")
    kept, _ = collapse_to_canonical([a, b])
    assert len(kept) == 1


def test_research_severity_format_supported():
    a = {
        "id": "T-001",
        "problem_class": "missing_schedule",
        "affected_system": "ВРУ",
        "severity": "ПРОВЕРИТЬ_ПО_СМЕЖНЫМ",
        "category": "completeness",
        "problem": "Кабельный журнал",
        "description": "desc",
        "evidence_quote": "ев",
    }
    b = dict(a, id="T-002")
    kept, _ = collapse_to_canonical([a, b])
    assert len(kept) == 1


def test_dedup_handles_missing_fields_gracefully():
    """Baseline findings lack problem_class, affected_system etc."""
    items = [
        {"id": "T-001", "problem": "Outdated norm", "severity": "РЕКОМЕНДАТЕЛЬНОЕ"},
        {"id": "T-002", "problem": "Outdated norm", "severity": "РЕКОМЕНДАТЕЛЬНОЕ"},
    ]
    kept, _ = collapse_to_canonical(items)
    assert len(kept) <= 2  # at minimum can't grow
    kept2, _ = fuzzy_dedup(items)
    assert len(kept2) <= 2


def test_dedup_handles_none_values():
    """Production findings sometimes have None for optional fields."""
    items = [
        {
            "id": "T-001",
            "problem_class": "missing_schedule",
            "affected_system": "ВРУ",
            "severity": "ЭКСПЛУАТАЦИОННОЕ",
            "category": "completeness",
            "problem": None,
            "description": None,
            "evidence_quote": None,
            "confidence": None,
            "norm": None,
        },
        {
            "id": "T-002",
            "problem_class": "missing_schedule",
            "affected_system": "ВРУ",
            "severity": "ЭКСПЛУАТАЦИОННОЕ",
            "category": "completeness",
            "problem": "OK",
            "description": "OK",
            "evidence_quote": "цитата",
            "confidence": 0.7,
            "norm": "СП X",
        },
    ]
    kept, _ = collapse_to_canonical(items)
    assert len(kept) == 1
    assert kept[0]["id"] == "T-002"  # the one with norm/desc filled wins


def test_class_dedup_critical_collapsed_count_zero_for_distinct_classes():
    """Distinct (problem_class, affected_system) keys produce 0 collapse."""
    items = [
        _f(0, "КРИТИЧЕСКОЕ"),
    ]
    items[0]["problem_class"] = "missing_schedule"
    items.append(_f(1, "КРИТИЧЕСКОЕ"))
    items[1]["problem_class"] = "outdated_norm_reference"
    _, report = collapse_to_canonical(items)
    assert report.critical_collapsed_count == 0
    assert report.same_class_drops == 0

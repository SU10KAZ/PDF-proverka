"""
test_fuzzy_dedup.py
-------------------
Phase 0 unit tests for backend/app/services/findings/dedup/fuzzy_dedup.py.

Run: python -m pytest tests/findings/dedup/test_fuzzy_dedup.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pytest  # noqa: E402

from backend.app.services.findings.dedup import (  # noqa: E402
    DEFAULT_SIM_THRESHOLD,
    fuzzy_dedup,
)


def _f(**kw) -> dict:
    base = {
        "id": kw.get("id", "T-001"),
        "category": kw.get("category", "normative"),
        "severity": kw.get("severity", "ЭКСПЛУАТАЦИОННОЕ"),
        "problem": kw.get("problem", "Some problem"),
        "description": kw.get("description", "Some description"),
        "affected_system": kw.get("affected_system", "ВРУ"),
        "evidence_quote": kw.get("evidence_quote", "цитата"),
        "confidence": kw.get("confidence", 0.8),
    }
    base.update(kw)
    return base


def test_similar_strings_collapse_at_default_threshold():
    a = _f(id="T-001", problem="Кабельный журнал отсутствует в томе ЭОМ")
    b = _f(id="T-002", problem="Кабельный журнал отсутствует в томе ЭОМ.")
    kept, report = fuzzy_dedup([a, b])
    assert len(kept) == 1
    assert report.same_class_drops == 1


def test_dissimilar_strings_do_not_collapse():
    a = _f(id="T-001", problem="Кабельный журнал отсутствует")
    b = _f(id="T-002", problem="Спецификация электрооборудования неполная", affected_system="ГРЩ")
    kept, report = fuzzy_dedup([a, b])
    assert len(kept) == 2
    assert report.same_class_drops == 0


def test_critical_never_collapses_into_non_critical():
    crit = _f(id="T-001", severity="КРИТИЧЕСКОЕ", problem="Перегрузка кабеля")
    nc = _f(id="T-002", severity="РЕКОМЕНДАТЕЛЬНОЕ", problem="Перегрузка кабеля (?)")
    kept, report = fuzzy_dedup([crit, nc])
    # both kept — critical-protect
    assert len(kept) == 2
    assert report.critical_collapsed_count >= 1


def test_two_criticals_kept_even_if_similar():
    a = _f(id="T-001", severity="КРИТИЧЕСКОЕ", problem="Перегрузка кабеля ВВГнг-FRLS 5x10")
    b = _f(id="T-002", severity="КРИТИЧЕСКОЕ", problem="Перегрузка кабеля ВВГнг-FRLS 5x10.")
    kept, _ = fuzzy_dedup([a, b])
    assert len(kept) == 2, "two КРИТ MUST be kept even when signatures match"


def test_disputed_detector_finding_never_collapses():
    disputed = _f(
        id="T-001",
        problem="На схеме указан автомат 16 А",
        detector_comparison={"primary_relation": "disputed"},
    )
    ordinary = _f(id="T-002", problem="На схеме указан автомат 16 А.")
    kept, report = fuzzy_dedup([disputed, ordinary])
    assert len(kept) == 2
    assert report.disputed_protected_count == 1


def test_threshold_zero_collapses_almost_everything_but_keeps_criticals():
    items = [
        _f(id=f"T-{i:03d}", problem=f"completely different finding {i}", severity="РЕКОМЕНДАТЕЛЬНОЕ")
        for i in range(5)
    ] + [_f(id="T-CRIT", severity="КРИТИЧЕСКОЕ", problem="critical")]
    kept, report = fuzzy_dedup(items, sim_threshold=0.0)
    # at threshold 0.0, similarity check trivially passes; non-crit fold into 1
    # КРИТ stays separate
    severities = {k["severity"] for k in kept}
    assert "КРИТИЧЕСКОЕ" in severities


def test_threshold_one_means_no_collapse_unless_identical():
    # Punctuation is stripped by signature normalisation, so a period doesn't
    # break the match. Use content with real difference to test threshold=1.0.
    a = _f(
        id="T-001",
        category="normative",
        problem="Кабельный журнал отсутствует в томе ЭОМ",
        affected_system="ВРУ",
        evidence_quote="ev_a",
    )
    b = _f(
        id="T-002",
        category="completeness",
        problem="Расчёт нагрузок не выполнен в томе ОВ",
        affected_system="Приточные системы",
        evidence_quote="ev_b",
    )
    kept, report = fuzzy_dedup([a, b], sim_threshold=1.0)
    assert len(kept) == 2


def test_threshold_validation():
    with pytest.raises(ValueError):
        fuzzy_dedup([], sim_threshold=-0.1)
    with pytest.raises(ValueError):
        fuzzy_dedup([], sim_threshold=1.5)


def test_output_count_never_exceeds_input():
    items = [_f(id=f"T-{i:03d}", problem=f"problem {i}") for i in range(15)]
    kept, report = fuzzy_dedup(items)
    assert len(kept) <= 15
    assert report.total_out <= report.total_in


def test_critical_count_never_decreases():
    items = [
        _f(id="T-001", severity="КРИТИЧЕСКОЕ", problem="A"),
        _f(id="T-002", severity="КРИТИЧЕСКОЕ", problem="A"),  # same sig but КРИТ
        _f(id="T-003", severity="РЕКОМЕНДАТЕЛЬНОЕ", problem="A"),
    ]
    crit_in = sum(1 for f in items if f.get("severity") == "КРИТИЧЕСКОЕ")
    kept, _ = fuzzy_dedup(items)
    crit_out = sum(1 for f in kept if f.get("severity") == "КРИТИЧЕСКОЕ")
    assert crit_out >= crit_in


def test_empty_input():
    kept, report = fuzzy_dedup([])
    assert kept == []
    assert report.total_in == 0
    assert report.total_out == 0


def test_single_finding_passes_through():
    kept, report = fuzzy_dedup([_f(id="T-001")])
    assert len(kept) == 1
    assert report.same_class_drops == 0


def test_dedup_report_includes_threshold():
    _, report = fuzzy_dedup([_f(id="T-001")], sim_threshold=0.65)
    assert abs(report.sim_threshold - 0.65) < 1e-9


def test_no_op_on_unique_findings():
    # Use semantically distinct content (different categories + completely
    # different problem strings) so signatures stay below the 0.7 threshold.
    distinct_problems = [
        ("normative", "Устаревшая ссылка на СП 31-110-2003 в томе ЭОМ", "ВРУ"),
        ("calculations", "Арифметическая ошибка в таблице нагрузок: 56 кВт ≠ 47 кВт", "ГРЩ"),
        ("completeness", "Кабельный журнал отсутствует", "Кабели"),
        ("contradictions", "ПЗ упоминает 380 В, спецификация — 220 В", "Питание"),
        ("safety", "Отсутствует молниезащита по СО-153", "Молниезащита"),
        ("normative", "Класс защиты IP не указан для светильников аварийного освещения", "Освещение"),
        ("cross_discipline", "Тепловые завесы П2 не учтены в ОВ-ЭОМ балансе", "Тепловые завесы"),
        ("completeness", "Расчёт токов короткого замыкания не приведён", "Токи КЗ"),
    ]
    items = [
        _f(
            id=f"T-{i:03d}",
            category=cat,
            problem=problem,
            affected_system=system,
            evidence_quote=f"unique evidence {i}",
        )
        for i, (cat, problem, system) in enumerate(distinct_problems)
    ]
    kept, report = fuzzy_dedup(items)
    assert len(kept) == len(items), (
        f"semantically distinct findings should not collapse, got "
        f"{len(items)} → {len(kept)}"
    )
    assert report.same_class_drops == 0
    assert report.critical_collapsed_count == 0


def test_deterministic_for_same_input():
    items = [_f(id=f"T-{i:03d}", problem=f"problem {i % 4}") for i in range(10)]
    a, ra = fuzzy_dedup(list(items))
    b, rb = fuzzy_dedup(list(items))
    assert [x.get("id") for x in a] == [x.get("id") for x in b]
    assert ra.total_out == rb.total_out


def test_default_threshold_is_07():
    assert abs(DEFAULT_SIM_THRESHOLD - 0.7) < 1e-9

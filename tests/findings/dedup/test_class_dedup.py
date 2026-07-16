"""
test_class_dedup.py
-------------------
Phase 0 unit tests for backend/app/services/findings/dedup/class_dedup.py.

Covers:
  - Same (problem_class, affected_system) → collapsed.
  - Two КРИТИЧЕСКОЕ never collapsed (critical-protect invariant).
  - Output count ≤ input count.
  - Canonical preference (severity / confidence / norm / description).
  - merge_across_methods priority + source_agents aggregation.
  - Baseline (no problem_class) fallback works.

Run: python -m pytest tests/findings/dedup/test_class_dedup.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.services.findings.dedup import (  # noqa: E402
    DedupReport,
    collapse_to_canonical,
    derive_class_key,
    mark_duplicates,
    merge_across_methods,
)


def _f(**kw) -> dict:
    base = {
        "id": kw.get("id", "T-001"),
        "problem_class": kw.get("problem_class", "outdated_norm_reference"),
        "affected_system": kw.get("affected_system", "ВРУ"),
        "severity": kw.get("severity", "ЭКСПЛУАТАЦИОННОЕ"),
        "category": kw.get("category", "normative"),
        "problem": kw.get("problem", "Some problem"),
        "description": kw.get("description", "Some description"),
        "evidence_quote": kw.get("evidence_quote", "ев."),
        "norm": kw.get("norm", ""),
        "confidence": kw.get("confidence", 0.8),
    }
    base.update(kw)
    return base


def test_same_class_collapses_to_one():
    f1 = _f(id="T-001", description="short")
    f2 = _f(id="T-002", description="longer description wins as canonical")
    kept, report = collapse_to_canonical([f1, f2])
    assert len(kept) == 1
    assert kept[0]["id"] == "T-002"  # longer desc, same severity/conf
    assert report.total_in == 2
    assert report.total_out == 1
    assert report.same_class_drops == 1
    assert report.critical_collapsed_count == 0


def test_two_critical_never_collapse():
    f1 = _f(id="T-001", severity="КРИТИЧЕСКОЕ", description="first crit")
    f2 = _f(id="T-002", severity="КРИТИЧЕСКОЕ", description="second crit")
    kept, report = collapse_to_canonical([f1, f2])
    assert len(kept) == 2, "two КРИТ in same cluster MUST be kept as 2 canonicals"
    # critical_collapsed_count counts the secondary КРИТ that got disambiguated
    assert report.critical_collapsed_count >= 1
    # both ids should still appear
    ids = {x["id"] for x in kept}
    assert ids == {"T-001", "T-002"}


def test_disputed_detector_finding_never_collapses():
    disputed = _f(
        id="T-001",
        detector_comparison={"primary_relation": "disputed"},
    )
    ordinary = _f(id="T-002")
    kept, report = collapse_to_canonical([disputed, ordinary])
    assert {item["id"] for item in kept} == {"T-001", "T-002"}
    assert report.disputed_protected_count == 1


def test_critical_protects_against_non_critical_collapse():
    crit = _f(id="T-001", severity="КРИТИЧЕСКОЕ", description="crit")
    rec = _f(id="T-002", severity="РЕКОМЕНДАТЕЛЬНОЕ", description="rec")
    kept, report = collapse_to_canonical([crit, rec])
    assert len(kept) == 2  # different severity tiers don't both keep, but crit-protect splits them
    # critical_collapsed_count counts the КРИТ being split from a cluster
    # (this case has no actual mixing because both are in same class but one is crit)
    # Output must contain the critical one
    severities = [k["severity"] for k in kept]
    assert "КРИТИЧЕСКОЕ" in severities


def test_output_count_never_exceeds_input():
    findings = [_f(id=f"T-{i:03d}", problem_class=f"pc_{i%3}") for i in range(20)]
    kept, report = collapse_to_canonical(findings)
    assert len(kept) <= 20
    assert report.total_out <= report.total_in


def test_canonical_prefers_higher_severity():
    a = _f(id="T-001", severity="РЕКОМЕНДАТЕЛЬНОЕ", description="rec")
    b = _f(id="T-002", severity="ЭКОНОМИЧЕСКОЕ", description="econ")
    # NOTE: with critical-protect logic, ЭКОН and РЕКОМ are both non-critical →
    # canonical_score picks the higher severity, returns 1 finding.
    kept, _ = collapse_to_canonical([a, b])
    assert len(kept) == 1
    assert kept[0]["severity"] == "ЭКОНОМИЧЕСКОЕ"


def test_canonical_prefers_higher_confidence():
    a = _f(id="T-001", confidence=0.5)
    b = _f(id="T-002", confidence=0.9)
    kept, _ = collapse_to_canonical([a, b])
    assert len(kept) == 1
    assert kept[0]["id"] == "T-002"


def test_canonical_prefers_norm_filled():
    a = _f(id="T-001", norm="", confidence=0.7)
    b = _f(id="T-002", norm="СП 256.1325800.2016", confidence=0.7)
    kept, _ = collapse_to_canonical([a, b])
    assert len(kept) == 1
    assert kept[0]["id"] == "T-002"


def test_class_key_baseline_fallback_uses_category_signature():
    # Baseline-style finding without problem_class
    a = {
        "id": "T-001",
        "category": "normative",
        "problem": "Outdated reference СП 31-110-2003",
        "affected_system": "",
        "evidence_quote": "СП 31-110-2003",
        "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
    }
    b = {
        "id": "T-002",
        "category": "normative",
        "problem": "Outdated reference СП 31-110-2003",
        "affected_system": "",
        "evidence_quote": "СП 31-110-2003 ещё раз",
        "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
    }
    key_a = derive_class_key(a)
    key_b = derive_class_key(b)
    assert key_a.problem_class == key_b.problem_class, "fallback signature must match"


def test_mark_duplicates_preserves_count():
    f1 = _f(id="T-001")
    f2 = _f(id="T-002")
    f3 = _f(id="T-003", problem_class="other_class")
    annotated, report = mark_duplicates([f1, f2, f3])
    assert len(annotated) == 3, "mark_duplicates MUST preserve input count"
    canonical_ids = {x["id"] for x in annotated if x.get("is_canonical")}
    duplicate_ids = {x["id"] for x in annotated if not x.get("is_canonical") and x.get("internal_duplicate_of")}
    assert len(canonical_ids) + len(duplicate_ids) == 3


def test_merge_across_methods_aggregates_source_agents():
    by_method = {
        "current_method": [_f(id="A", source_agent="current_method")],
        "completeness": [_f(id="B", source_agent="completeness")],
    }
    kept, report = merge_across_methods(by_method, priority=["current_method", "completeness"])
    # Same class → collapsed; canonical from priority leader (current_method)
    assert len(kept) == 1
    assert kept[0]["id"] == "A"
    assert "completeness" in kept[0]["source_agents"]
    assert "current_method" in kept[0]["source_agents"]


def test_merge_across_methods_keeps_two_criticals():
    by_method = {
        "current_method": [_f(id="A", severity="КРИТИЧЕСКОЕ")],
        "completeness": [_f(id="B", severity="КРИТИЧЕСКОЕ")],
    }
    kept, report = merge_across_methods(by_method, priority=["current_method", "completeness"])
    assert len(kept) == 2, "two КРИТ in merge_across_methods MUST be kept"


def test_empty_input():
    kept, report = collapse_to_canonical([])
    assert kept == []
    assert report.total_in == 0
    assert report.total_out == 0


def test_single_finding_passes_through():
    f = _f(id="T-001")
    kept, report = collapse_to_canonical([f])
    assert len(kept) == 1
    assert kept[0]["id"] == "T-001"
    assert report.same_class_drops == 0


def test_dedup_report_to_dict_is_json_serialisable():
    import json
    _, report = collapse_to_canonical([_f(id="T-001"), _f(id="T-002")])
    payload = report.to_dict()
    assert isinstance(payload, dict)
    json.dumps(payload)  # must not raise


def test_deterministic_output_for_same_input():
    findings = [_f(id=f"T-{i:03d}", problem_class=f"pc_{i%4}") for i in range(15)]
    a, ra = collapse_to_canonical(list(findings))
    b, rb = collapse_to_canonical(list(findings))
    assert [x.get("id") for x in a] == [x.get("id") for x in b]
    assert ra.total_out == rb.total_out
    assert ra.same_class_drops == rb.same_class_drops

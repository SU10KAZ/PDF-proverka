"""Smoke tests for the class_dedup module. No LLM calls."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from runners.class_dedup import (
    derive_class_key, collapse_to_canonical, mark_duplicates, merge_across_methods,
)


def t_assert(name, cond, detail=""):
    if cond:
        print(f"OK  {name}")
    else:
        print(f"FAIL {name}: {detail}")
        sys.exit(1)


def main():
    # Case 1: explicit class keys collapse
    findings = [
        {"id": "A", "problem_class": "obsolete_norm",
         "affected_system": "СНиП 41-01",
         "category": "normative", "problem": "obsolete A",
         "confidence": 0.9, "severity": "КРИТИЧЕСКОЕ",
         "evidence_quote": "..."},
        {"id": "B", "problem_class": "obsolete_norm",
         "affected_system": "СНиП 41-01",
         "category": "normative", "problem": "obsolete B",
         "confidence": 0.6, "severity": "КРИТИЧЕСКОЕ",
         "evidence_quote": "..."},
        {"id": "C", "problem_class": "missing_journal",
         "affected_system": "cable journal",
         "category": "completeness", "problem": "missing cable journal",
         "confidence": 0.8, "severity": "ЭКСПЛУАТАЦИОННОЕ",
         "evidence_quote": "..."},
    ]
    collapsed, report = collapse_to_canonical(findings)
    t_assert("explicit class collapses", len(collapsed) == 2, f"got {len(collapsed)}")
    t_assert("higher-confidence kept", any(f["id"] == "A" for f in collapsed))
    t_assert("report.drops correct", report.same_class_drops == 1)

    # Case 2: fallback (no problem_class) still works
    fallback = [
        {"id": "X", "category": "calculation",
         "problem": "wrong total", "evidence_quote": "row sum = 12.2",
         "confidence": 0.8, "severity": "ЭКОНОМИЧЕСКОЕ"},
        {"id": "Y", "category": "calculation",
         "problem": "wrong total", "evidence_quote": "row sum = 12.2",
         "confidence": 0.7, "severity": "ЭКОНОМИЧЕСКОЕ"},
    ]
    collapsed2, _ = collapse_to_canonical(fallback)
    t_assert("fallback collapses identical", len(collapsed2) == 1)

    # Case 3: cross_discipline differentiated by interface_type
    xd = [
        {"id": "P", "problem_class": "electrical_load_mismatch",
         "affected_system": "ЩВ-ОВ", "interface_type": "electrical_load",
         "discipline_pair": ["ЭОМ", "ОВ"],
         "category": "cross_discipline", "problem": "X1",
         "confidence": 0.9, "severity": "КРИТИЧЕСКОЕ",
         "evidence_quote": "..."},
        {"id": "Q", "problem_class": "electrical_load_mismatch",
         "affected_system": "ЩВ-ОВ", "interface_type": "startup_current",
         "discipline_pair": ["ЭОМ", "ОВ"],
         "category": "cross_discipline", "problem": "X2",
         "confidence": 0.85, "severity": "КРИТИЧЕСКОЕ",
         "evidence_quote": "..."},
    ]
    collapsed3, _ = collapse_to_canonical(xd)
    t_assert("interface_type splits class", len(collapsed3) == 2)

    # Case 4: mark_duplicates preserves count, marks dupes
    marked, _ = mark_duplicates(findings)
    t_assert("mark_duplicates preserves count", len(marked) == 3)
    canonicals = [m for m in marked if m["is_canonical"]]
    t_assert("two canonicals identified", len(canonicals) == 2)
    nons = [m for m in marked if not m["is_canonical"]]
    t_assert("one non-canonical identified", len(nons) == 1)
    t_assert("non-canonical points to canonical",
             nons[0]["internal_duplicate_of"] in {f["id"] for f in canonicals})

    # Case 5: merge_across_methods honours priority
    merged, _ = merge_across_methods(
        {"current_method": [findings[0]], "completeness": [findings[1]]},
        priority=["current_method", "completeness"],
    )
    t_assert("merge canonical from priority method", merged[0]["id"] == "A")
    t_assert("merge tracks sources", "current_method" in merged[0]["source_agents"]
             and "completeness" in merged[0]["source_agents"])

    print("\nAll class_dedup tests passed.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Emit completeness_requirements_matrix.{json,csv} from `_data.py`.

Single source of truth: _data.ITEMS. Run from anywhere; outputs land
alongside this script.
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from _data import ITEMS  # noqa: E402

OUT_JSON = HERE / "completeness_requirements_matrix.json"
OUT_CSV  = HERE / "completeness_requirements_matrix.csv"

# Stable column order for CSV.
COLUMNS = [
    "id",
    "discipline",
    "item_name",
    "current_section",
    "current_severity",
    "current_applies",
    "current_problem_class",
    "current_norm_reference",
    "normative_status",
    "applicable_document_types",
    "applicable_stages",
    "applicability_conditions",
    "normative_basis",
    "exact_clause_or_section",
    "confidence",
    "can_be_reported_as_missing",
    "recommended_severity",
    "do_not_report_if",
    "example_valid_missing_case",
    "example_invalid_missing_case",
    "current_norm_issues",
]


def main() -> None:
    # JSON: pretty, UTF-8, sorted by id.
    items_sorted = sorted(ITEMS, key=lambda it: it["id"])
    OUT_JSON.write_text(
        json.dumps(items_sorted, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    # CSV: stable column order; lists → "; "-joined.
    with OUT_CSV.open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, quoting=csv.QUOTE_ALL)
        w.writerow(COLUMNS)
        for it in items_sorted:
            row = []
            for col in COLUMNS:
                v = it.get(col, "")
                if isinstance(v, list):
                    v = "; ".join(str(x) for x in v)
                elif isinstance(v, bool):
                    v = "true" if v else "false"
                row.append(v if v is not None else "")
            w.writerow(row)

    print(f"wrote {OUT_JSON} ({len(items_sorted)} items)")
    print(f"wrote {OUT_CSV} ({len(items_sorted)} items)")

    # Quick aggregate for sanity.
    by_disc: dict[str, int] = {}
    by_status: dict[str, int] = {}
    can_report: dict[str, int] = {"true": 0, "false": 0}
    for it in items_sorted:
        by_disc[it["discipline"]] = by_disc.get(it["discipline"], 0) + 1
        by_status[it["normative_status"]] = by_status.get(it["normative_status"], 0) + 1
        key = "true" if it["can_be_reported_as_missing"] else "false"
        can_report[key] += 1

    print()
    print("By discipline:", by_disc)
    print("By normative_status:", by_status)
    print("can_be_reported_as_missing:", can_report)


if __name__ == "__main__":
    main()

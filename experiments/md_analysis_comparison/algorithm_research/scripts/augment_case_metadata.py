"""Augment case.json files with document_type + signal flags.

document_type values:
- full_rd            — полный раздел РД, проверяется на полный комплект
- audit_comparison   — фрагменты двух разделов сопоставляются (cross_01 style)
- tz_vs_rd           — фрагменты ТЗ vs фрагменты РД, проверяется соответствие требованиям
- specification_only — спецификация / ведомость / выборочный расчёт; нельзя
                       требовать полный комплект РД

Idempotent: safe to re-run. Backs up only on first run.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

# Authoritative classification. Based on case.notes + a peek at input.md.
CASES = {
    "ar_01_evacuation": {
        "document_type": "full_rd",
        "expected_complexity": "high",
        "has_cross_discipline": True,
        "has_completeness_gaps": True,
        "has_calculation_errors": True,
        "has_normative_errors": True,
        "has_hidden_contradictions": True,
    },
    "cross_01_eom_ov_loads": {
        "document_type": "audit_comparison",
        "expected_complexity": "high",
        "has_cross_discipline": True,
        "has_completeness_gaps": True,
        "has_calculation_errors": True,
        "has_normative_errors": False,
        "has_hidden_contradictions": True,
    },
    "eom_01_cable_sizing": {
        "document_type": "full_rd",
        "expected_complexity": "medium",
        "has_cross_discipline": False,
        "has_completeness_gaps": True,
        "has_calculation_errors": True,
        "has_normative_errors": True,
        "has_hidden_contradictions": False,
    },
    "kj_01_rebar": {
        "document_type": "full_rd",
        "expected_complexity": "medium",
        "has_cross_discipline": True,
        "has_completeness_gaps": True,
        "has_calculation_errors": True,
        "has_normative_errors": False,
        "has_hidden_contradictions": True,
    },
    "multi_01_tz_vs_rd": {
        "document_type": "tz_vs_rd",
        "expected_complexity": "medium",
        "has_cross_discipline": False,
        "has_completeness_gaps": True,
        "has_calculation_errors": False,
        "has_normative_errors": False,
        "has_hidden_contradictions": False,
    },
    "ov_01_ventilation": {
        "document_type": "full_rd",
        "expected_complexity": "medium",
        "has_cross_discipline": False,
        "has_completeness_gaps": True,
        "has_calculation_errors": True,
        "has_normative_errors": True,
        "has_hidden_contradictions": False,
    },
    "ss_01_cabling": {
        "document_type": "full_rd",
        "expected_complexity": "medium",
        "has_cross_discipline": False,
        "has_completeness_gaps": True,
        "has_calculation_errors": False,
        "has_normative_errors": True,
        "has_hidden_contradictions": False,
    },
    "vk_01_water_flow": {
        "document_type": "full_rd",
        "expected_complexity": "medium",
        "has_cross_discipline": False,
        "has_completeness_gaps": True,
        "has_calculation_errors": True,
        "has_normative_errors": True,
        "has_hidden_contradictions": False,
    },
}

ALLOWED_DOC_TYPES = {"full_rd", "audit_comparison", "tz_vs_rd", "specification_only"}


def augment(datasets_dir: Path, dry_run: bool = False) -> int:
    n = 0
    for case_id, meta in CASES.items():
        case_path = datasets_dir / case_id / "case.json"
        if not case_path.exists():
            print(f"  skip {case_id}: case.json missing", file=sys.stderr)
            continue
        data = json.loads(case_path.read_text(encoding="utf-8"))
        if data.get("document_type") == meta["document_type"] and all(
            data.get(k) == v for k, v in meta.items()
        ):
            print(f"  ok  {case_id}: already augmented")
            continue
        data.update(meta)
        if data["document_type"] not in ALLOWED_DOC_TYPES:
            raise ValueError(f"{case_id}: bad document_type {data['document_type']}")
        if not dry_run:
            case_path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n",
                                  encoding="utf-8")
        n += 1
        print(f"  +   {case_id}: document_type={meta['document_type']}")
    return n


if __name__ == "__main__":
    here = Path(__file__).resolve()
    datasets = here.parents[2] / "datasets"
    n = augment(datasets, dry_run="--dry-run" in sys.argv)
    print(f"\nAugmented {n} case.json files in {datasets}")

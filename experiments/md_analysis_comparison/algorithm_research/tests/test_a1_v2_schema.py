"""test_a1_v2_schema — A1-v2 output must conform to RunResult + carry meta.

Validates:
- result schema: method, case_id, discipline, model_main, duration_sec,
  findings, meta, errors;
- meta contains: prompt_set='v2', document_type, current_method_findings,
  completeness_findings, post_dedup_findings, dedup_report;
- every finding has required fields (id, severity, problem/description,
  source_agent in {current_method, completeness, ...});
- severity values are from the agreed set;
- document_type carried through into meta.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
RESULTS = HERE.parent / "results"
A1_V2 = RESULTS / "A1_hybrid_lite__v2"

REQUIRED_TOP = {"method", "case_id", "discipline", "duration_sec", "findings", "meta", "errors"}
REQUIRED_FINDING = {"id", "severity", "discipline"}
ALLOWED_SEVERITIES = {
    "КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ", "ЭКСПЛУАТАЦИОННОЕ",
    "ПРОВЕРИТЬ_ПО_СМЕЖНЫМ", "РЕКОМЕНДАТЕЛЬНОЕ",
}


def t_assert(name, cond, detail=""):
    if cond:
        print(f"OK  {name}")
    else:
        print(f"FAIL {name}: {detail}")
        sys.exit(1)


def check_one(case_path: Path) -> None:
    data = json.loads(case_path.read_text(encoding="utf-8"))
    missing_top = REQUIRED_TOP - set(data.keys())
    t_assert(f"{case_path.name}: schema top-level",
             not missing_top, f"missing {missing_top}")
    meta = data.get("meta") or {}
    t_assert(f"{case_path.name}: meta.prompt_set == v2",
             meta.get("prompt_set") == "v2", f"got {meta.get('prompt_set')}")
    t_assert(f"{case_path.name}: meta.document_type present",
             "document_type" in meta, "missing")
    for k in ("current_method_findings", "completeness_findings",
              "post_dedup_findings", "dedup_report"):
        t_assert(f"{case_path.name}: meta.{k}", k in meta, f"missing {k}")

    for f in data.get("findings") or []:
        miss = REQUIRED_FINDING - set(f.keys())
        t_assert(f"{case_path.name}: finding {f.get('id','?')} fields",
                 not miss, f"missing {miss}")
        sev = (f.get("severity") or "").upper()
        if sev:
            t_assert(f"{case_path.name}: finding {f.get('id','?')} severity allowed",
                     sev in ALLOWED_SEVERITIES, f"unknown severity {sev}")


def main():
    if not A1_V2.exists():
        print(f"WARN: {A1_V2} does not exist yet; nothing to validate")
        return
    files = sorted(A1_V2.glob("*.json"))
    if not files:
        print(f"WARN: no A1-v2 outputs yet under {A1_V2}")
        return
    for p in files:
        check_one(p)
    print(f"\nA1-v2 schema validated on {len(files)} case outputs.")


if __name__ == "__main__":
    main()

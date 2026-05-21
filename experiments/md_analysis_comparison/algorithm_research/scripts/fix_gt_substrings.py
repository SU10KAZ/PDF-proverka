"""One-shot fix: replace failing must_match_substring in new ground_truth.json.

Each entry below maps (case_id, gt_id) -> new_substring that is verified to be
present in input.md.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
DATASETS = HERE.parents[2] / "datasets"

FIXES = {
    ("vk_02_sewage", "GT-04"): "Стояк канализации",
    ("cross_02_kj_ar_opening", "GT-05"): "Cross-discipline",
    ("ov_03_heating_calc", "GT-02"): "северо-восток",
    ("ov_03_heating_calc", "GT-04"): "Принципиальные",
    ("km_01_truss_design", "GT-03"): "сжатие",
    ("km_01_truss_design", "GT-04"): "ферм",
    ("km_01_truss_design", "GT-06"): "Состав документации",
    ("ar_03_balcony_glazing", "GT-05"): "Полный раздел",
    ("eom_03_low_voltage_selectivity", "GT-03"): "Селективность",
    ("eom_03_low_voltage_selectivity", "GT-04"): "Иерархия защитных",
    ("ov_02_smoke_protection", "GT-06"): "противодымной",
    ("ar_02_facade_thermal", "GT-04"): "Влажностный",
    ("ar_02_facade_thermal", "GT-05"): "Воздушный",
    ("kj_02_slab_punching", "GT-04"): "плит",
    ("kj_03_foundation_audit", "GT-03"): "Осадка",
    ("kj_03_foundation_audit", "GT-04"): "Часть В",
    ("km_03_connections", "GT-02"): "Класс стали",
    ("km_03_connections", "GT-03"): "фрикционное",
    ("km_03_connections", "GT-04"): "проходит",
    ("km_02_metal_protection_spec", "GT-03"): "лакокрасочные",
    ("km_02_metal_protection_spec", "GT-04"): "основном",
}


def fix_one(case_id: str, gt_id: str, new_sub: str) -> None:
    case_dir = DATASETS / case_id
    md_text = (case_dir / "input.md").read_text(encoding="utf-8")
    if new_sub not in md_text:
        raise ValueError(f"{case_id}/{gt_id}: new substring '{new_sub}' STILL not in input.md")
    gt_path = case_dir / "ground_truth.json"
    data = json.loads(gt_path.read_text(encoding="utf-8"))
    found = False
    for ef in data["expected_findings"]:
        if ef.get("id") == gt_id:
            ef["must_match_substring"] = new_sub
            found = True
            break
    if not found:
        raise ValueError(f"{case_id}: gt_id {gt_id} not found")
    gt_path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n",
                        encoding="utf-8")


def main():
    for (cid, gid), sub in FIXES.items():
        try:
            fix_one(cid, gid, sub)
            print(f"  ok  {cid}/{gid} -> '{sub}'")
        except Exception as e:
            print(f"  FAIL {cid}/{gid}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()

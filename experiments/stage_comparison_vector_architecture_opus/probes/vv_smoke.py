#!/usr/bin/env python3
"""VV — smoke test: run the real verifier on two manifest cases.

One clean control and one ``deleted_object`` mutation on the SAME block, so the
only difference between the two calls is the corruption.

    cd /home/coder/projects/PDF-proverka
    python -m experiments.stage_comparison_vector_architecture_opus.probes.vv_smoke
    python -m ...vv_smoke --cases vv009 vv027
"""
from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from experiments.stage_comparison_vector_architecture_opus.probes import vv_harness as vv

DEFAULT_CASES = ["vv009", "vv027"]


def run_case(case: dict, manifest: dict) -> dict:
    crop = vv.ROOT / case["crop_png"]
    sheet = {"text": case["fact_sheet"]["text"],
             "characters": case["fact_sheet"]["characters"],
             "block_id": manifest["blocks"][case["block"]]["block_id"]}
    out = vv.VERIFY_DIR / f"{case['case_id']}.json"
    record = vv.verify(crop, sheet, out, timeout=420, retries=1)
    record["case_id"] = case["case_id"]
    record["block"] = case["block"]
    record["mutation"] = case["mutation"]
    record["expected_status"] = case["expected_status"]
    record["acceptable_status"] = case["acceptable_status"]
    record["must_name"] = case["must_name"]
    record["changed_claims"] = case["changed_claims"]
    out.write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return record


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cases", nargs="*", default=DEFAULT_CASES)
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    manifest = json.loads(vv.CASES_JSON.read_text(encoding="utf-8"))
    selected = [c for c in manifest["cases"] if c["case_id"] in args.cases]
    if len(selected) != len(args.cases):
        raise SystemExit(f"case ids not found: {set(args.cases) - {c['case_id'] for c in selected}}")
    vv.VERIFY_DIR.mkdir(parents=True, exist_ok=True)
    with ThreadPoolExecutor(max_workers=min(args.workers, 6)) as pool:
        records = list(pool.map(lambda c: run_case(c, manifest), selected))

    rows = []
    for record in records:
        verdict = record.get("verdict") or {}
        rows.append({
            "case_id": record["case_id"],
            "mutation": record["mutation"],
            "expected_status": record["expected_status"],
            "status": record.get("status"),
            "status_acceptable": record.get("status") in record["acceptable_status"],
            "confidence": verdict.get("confidence"),
            "verified": verdict.get("verified"),
            "missing": verdict.get("missing"),
            "suspicious": verdict.get("suspicious"),
            "changed_claims": record["changed_claims"],
            "must_name": record["must_name"],
            "usage_raw": record.get("usage_raw"),
            "usage_payload_attributable": record.get("usage_payload_attributable"),
            "duration_ms": record.get("duration_ms"),
            "wall_seconds": record.get("wall_seconds"),
            "crop_bytes": record.get("crop_bytes"),
            "fact_sheet_characters": record.get("fact_sheet_characters"),
        })
    out = vv.ARTIFACTS / "vv_smoke.json"
    out.write_text(json.dumps({"cases": rows,
                               "usage_note": records[0]["usage_note"]},
                              ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(rows, ensure_ascii=False, indent=2))
    print("written:", out)
    for record in records:
        print("\n=== raw model text", record["case_id"], "===")
        print((record["attempts"][-1].get("model_text") or "")[:2000])


if __name__ == "__main__":
    main()

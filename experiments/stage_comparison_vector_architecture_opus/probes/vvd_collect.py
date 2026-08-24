#!/usr/bin/env python3
"""VVD — collect raw verifier records into one reviewable table.

Emits ``artifacts/vvd_results_<tag>.json`` (machine) and prints a compact per-case block
(status, suspicious, missing, ground truth) for hand scoring.
"""
from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path

from experiments.stage_comparison_vector_architecture_opus.probes import vv_harness as vv

RUNS = vv.ARTIFACTS / "vvd_runs"


def gt_summary(case: dict) -> dict:
    gt = case.get("ground_truth") or {}
    det = gt.get("detail") or {}
    keep = {}
    for k in ("where", "component_id", "segments_removed", "texts_removed", "removed_texts",
              "true_count", "stated_count", "delta", "target_field", "mode", "before", "after",
              "dropped", "broken_count", "target_selection", "samples_before", "samples_after",
              "segment_fraction_kept", "primitives_before", "primitives_after",
              "segments_before", "segments_after", "measured", "not_adjusted", "region_bbox"):
        if k in det:
            keep[k] = det[k]
    return {"kind": gt.get("kind"), "corrupted": gt.get("corrupted"), "detail": keep}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="main")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()
    manifest = json.loads(vv.CASES_JSON.read_text(encoding="utf-8"))
    by_id = {c["case_id"]: c for c in manifest["cases"]}
    out = []
    for f in sorted((RUNS / args.tag).glob("*.json")):
        r = json.loads(f.read_text(encoding="utf-8"))
        case = by_id[r["case_id"]]
        v = r.get("verdict") or {}
        row = {
            "record": r.get("record_name", r["case_id"]),
            "case_id": r["case_id"],
            "tag": args.tag,
            "block": r["block"],
            "family": r["family"],
            "mutation": r["mutation"],
            "disclose_limits": r["disclose_limits"],
            "strength": r.get("strength"),
            "expected_status": r["expected_status"],
            "acceptable_status": r["acceptable_status"],
            "status": r.get("status"),
            "status_acceptable": r.get("status") in r["acceptable_status"],
            "confidence": v.get("confidence"),
            "verified_claims": v.get("verified") or [],
            "suspicious": v.get("suspicious") or [],
            "missing": v.get("missing") or [],
            "changed_claims": r["changed_claims"],
            "must_name": r["must_name"],
            "ground_truth": gt_summary(case),
            "fact_sheet": case["fact_sheet"]["text"],
            "crop_used": r.get("crop_used"),
            "crop_bytes": r.get("crop_bytes"),
            "usage_raw": r.get("usage_raw"),
            "usage_payload_attributable": r.get("usage_payload_attributable"),
            "wall_seconds": r.get("wall_seconds"),
            "duration_ms": r.get("duration_ms"),
            "attempts": len(r.get("attempts") or []),
            "ok": r.get("ok"),
        }
        sus_ids = {s.get("claim_id") for s in row["suspicious"] if isinstance(s, dict)}
        row["suspicious_ids"] = sorted(i for i in sus_ids if i)
        row["auto_claim_hit"] = bool(sus_ids & set(row["changed_claims"]))
        out.append(row)
    dest = vv.ARTIFACTS / f"vvd_results_{args.tag}.json"
    dest.write_text(json.dumps({"tag": args.tag, "n": len(out), "rows": out},
                               ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    pays = [r["usage_payload_attributable"] for r in out if r["usage_payload_attributable"]]
    walls = [r["wall_seconds"] for r in out if r["wall_seconds"]]
    print(f"tag={args.tag} n={len(out)} parsed_ok={sum(1 for r in out if r['ok'])}")
    if pays:
        pays_s = sorted(pays); walls_s = sorted(walls)
        q = lambda a, p: a[min(len(a) - 1, int(round(p * (len(a) - 1))))]
        print(f"payload tokens  median={statistics.median(pays):.0f} p90={q(pays_s,0.9)} "
              f"min={pays_s[0]} max={pays_s[-1]}")
        print(f"wall seconds    median={statistics.median(walls):.1f} p90={q(walls_s,0.9)} "
              f"min={walls_s[0]} max={walls_s[-1]}")
    if not args.quiet:
        for r in out:
            print("=" * 100)
            print(f"{r['record']} | {r['block']} | {r['mutation']} | exp={r['expected_status']} "
                  f"-> {r['status']} ({r['confidence']}) | changed={r['changed_claims']} | "
                  f"suspicious_ids={r['suspicious_ids']}")
            print(f"  GT: {json.dumps(r['ground_truth'], ensure_ascii=False)[:700]}")
            print(f"  must_name: {r['must_name']}")
            for s in r["suspicious"]:
                if isinstance(s, dict):
                    print(f"  SUS {s.get('claim_id')}: {s.get('why')}")
                else:
                    print(f"  SUS ?: {s}")
            for mtxt in r["missing"]:
                print(f"  MISS: {mtxt}")
    print("written:", dest)


if __name__ == "__main__":
    main()

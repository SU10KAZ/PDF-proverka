#!/usr/bin/env python3
"""Assemble the artefacts the brief asks for from the stored runs.

registration_results.json, routing_results.json, negative_controls.json,
local_graphic_diff.json (one full example of the output contract).
"""
from __future__ import annotations

import json
import pathlib
import statistics
from collections import Counter

ART = pathlib.Path(__file__).resolve().parents[1] / "artifacts"


def main():
    gt = {r["pair_id"]: r for r in json.loads((ART / "human_ground_truth.json").read_text(encoding="utf-8"))["pairs"]}
    runs = {}
    for f in sorted((ART / "diff_runs").glob("*.json")):
        d = json.loads(f.read_text(encoding="utf-8"))
        runs[d["pair_id"]] = d

    # ---- registration ---------------------------------------------------
    rows = []
    for pid, r in runs.items():
        reg = r["registration"]
        rows.append({
            "pair_id": pid, "bucket": r["bucket"], "discipline": r["discipline"],
            "method": reg["method"], "transform": reg["transform"],
            "success": reg["success"], "failure_reason": reg["failure_reason"],
            "confidence": reg["confidence"],
            "coverage": reg["coverage"], "residual": reg["residual"], "anchors": reg["anchors"],
            "left_ink_pt": reg["left_ink_pt"], "right_ink_pt": reg["right_ink_pt"],
            "page_rotation": [r["extraction"]["left"]["page_rotation"], r["extraction"]["right"]["page_rotation"]],
            "gt_label": gt[pid]["human_label"],
        })
    scales = [r["transform"]["scale"] for r in rows]
    ok = [r for r in rows if r["success"]]
    reg_summary = {
        "pairs": len(rows),
        "success": len(ok),
        "success_rate": round(len(ok) / max(1, len(rows)), 3),
        "methods": dict(Counter(r["method"] for r in rows)),
        "failure_reasons": dict(Counter(r["failure_reason"] for r in rows if not r["success"])),
        "sym_cov": {"median": round(statistics.median(r["coverage"]["sym_cov"] for r in rows), 4),
                    "p10": round(sorted(r["coverage"]["sym_cov"] for r in rows)[max(0, len(rows) // 10)], 4)},
        "residual_pt": {"median": round(statistics.median([r["residual"]["median"] for r in rows
                                                           if r["residual"]["median"] is not None]), 4)},
        "scale_deviation_from_1": {"max": round(max(abs(s - 1.0) for s in scales), 5),
                                   "pairs_with_scale_change": sum(1 for s in scales if abs(s - 1.0) > 0.001)},
        "rotation_used": sum(1 for r in rows if abs(r["transform"]["theta_deg"]) > 1e-6),
    }
    (ART / "registration_results.json").write_text(json.dumps(
        {"probe": "assemble.registration", "research_only": True,
         "summary": reg_summary, "pairs": rows}, ensure_ascii=False, indent=1), encoding="utf-8")

    # ---- routing --------------------------------------------------------
    def desired(g):
        if g["scale"] == "major":
            return "MODE_2_REQUIRED"
        return "MODE_1_APPLICABLE"

    rrows = []
    for pid, r in runs.items():
        g = gt[pid]
        want = desired(g)
        got = r["route"]
        rrows.append({
            "pair_id": pid, "bucket": r["bucket"], "gt_label": g["human_label"], "gt_scale": g["scale"],
            "route": got, "reason": r["route_reason"], "desired_route": want,
            "matches_desired": bool(got == want),
            "degenerate_input": got in ("VISION_REQUIRED", "NO_GRAPHIC_COMPARISON"),
            "sym_cov": r["registration"]["coverage"]["sym_cov"],
            "changed_ink_fraction": r["diff"]["changed_ink_fraction"],
            "regions": r["diff"]["n_regions_published"],
        })
    strict = [x for x in rrows if not x["degenerate_input"]]
    routing = {
        "routes": dict(Counter(x["route"] for x in rrows)),
        "desired_vs_got": dict(Counter(f"{x['desired_route']} -> {x['route']}" for x in rrows)),
        "correct_excluding_degenerate": sum(1 for x in strict if x["matches_desired"]),
        "total_excluding_degenerate": len(strict),
        "major_rebuilds_kept_in_mode1": sum(1 for x in rrows
                                            if x["gt_scale"] == "major" and x["route"] == "MODE_1_APPLICABLE"),
        "local_changes_handed_to_mode2": sum(1 for x in rrows
                                             if x["gt_scale"] in ("local", "many_local")
                                             and x["route"] == "MODE_2_REQUIRED"),
        "gates": {
            "min_ink_pt": 200.0, "min_sym_cov": 0.80, "max_changed_fraction": 0.25,
            "max_regions": 40, "min_region_ink_pt": 8.0,
            "calibrated_on": "benchmark_pairs.json (56 пар), см. calibration.json",
        },
    }
    (ART / "routing_results.json").write_text(json.dumps(
        {"probe": "assemble.routing", "research_only": True,
         "summary": routing, "pairs": rrows}, ensure_ascii=False, indent=1), encoding="utf-8")

    # ---- negative controls ---------------------------------------------
    neg = {}
    for bucket in ("unchanged", "text_only", "table_only", "repack"):
        items = [r for pid, r in runs.items() if r["bucket"] == bucket]
        neg[bucket] = {
            "pairs": len(items),
            "verdicts": dict(Counter(r["verdict"] for r in items)),
            "routes": dict(Counter(r["route"] for r in items)),
            "published_regions_total": sum(r["diff"]["n_regions_published"] for r in items),
            "pairs_with_any_published_region": sum(1 for r in items if r["diff"]["n_regions_published"] > 0),
            "pair_ids": [r["pair_id"] for r in items],
        }
    repack = json.loads((ART / "repack_results.json").read_text(encoding="utf-8"))["rows"]
    neg["repack_extra_real_pairs"] = {
        "pairs": len(repack),
        "published_regions_total": sum(r.get("published", 0) for r in repack),
        "routes": dict(Counter(r.get("route") for r in repack)),
        "segment_ratio_range": [min(r.get("segment_ratio", 0) for r in repack),
                                max(r.get("segment_ratio", 0) for r in repack)],
    }
    crop = json.loads((ART / "crop_boundary_results.json").read_text(encoding="utf-8"))
    neg["crop_drift"] = {"runs": crop["runs"], "runs_with_published_regions": crop["runs_with_published_regions"],
                         "fractions": sorted({r["fraction"] for r in crop["rows"]}),
                         "kinds": sorted({r["kind"] for r in crop["rows"]})}
    (ART / "negative_controls.json").write_text(json.dumps(
        {"probe": "assemble.negative_controls", "research_only": True, "controls": neg},
        ensure_ascii=False, indent=1), encoding="utf-8")

    # ---- example output contract ---------------------------------------
    example = runs["eom_dense_small_change_29"]
    (ART / "local_graphic_diff.json").write_text(json.dumps(example, ensure_ascii=False, indent=1), encoding="utf-8")

    print(json.dumps({"registration": reg_summary, "routing": routing,
                      "negative_controls": {k: (v if not isinstance(v, dict) else
                                                {kk: vv for kk, vv in v.items() if kk != "pair_ids"})
                                            for k, v in neg.items()}},
                     ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()

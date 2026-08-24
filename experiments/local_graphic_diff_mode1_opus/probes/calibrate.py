#!/usr/bin/env python3
"""Calibrate the MODE 1 gates on the benchmark (§20).

Nothing here is chosen by taste: every threshold is swept and the trade-off is
printed, so the chosen value can be read off the table.

Two independent knobs:

* `min_region_ink_pt` — how much unmatched ink a region needs before it is
  published at all (the false-positive / recall knob);
* the routing gates `min_sym_cov`, `max_changed_fraction`, `max_regions` — when
  MODE 1 is allowed to answer at all.

The "desired route" used as the target: a pair a human called NO_CHANGE,
CROP_DIFFERENCE or a local/many-local change should be answered by MODE 1;
a pair the human called a major rebuild should be handed to MODE 2.
"""
from __future__ import annotations

import json
import pathlib
from collections import Counter

ART = pathlib.Path(__file__).resolve().parents[1] / "artifacts"


def touches(a, b, pad=6.0):
    return not (a[2] + pad < b[0] or a[0] - pad > b[2] or a[3] + pad < b[1] or a[1] - pad > b[3])


def load():
    gt = {r["pair_id"]: r for r in json.loads((ART / "human_ground_truth.json").read_text(encoding="utf-8"))["pairs"]}
    runs = {}
    for f in sorted((ART / "diff_runs").glob("*.json")):
        d = json.loads(f.read_text(encoding="utf-8"))
        runs[d["pair_id"]] = d
    return gt, runs


def sweep_min_ink(gt, runs):
    rows = []
    for T in (0, 2, 3, 5, 8, 12, 16, 20, 25, 30, 40, 60, 90):
        tp = fp = fn = tn = 0
        hit = tot = false_regions = 0
        for pid, r in runs.items():
            g = gt[pid]
            if r["route"] != "MODE_1_APPLICABLE":
                continue
            pub = [x for x in r["change_regions"] if x["ink_pt"] >= T]
            pb = [x["bbox"] for x in pub]
            gtb = [x["bbox_pt"] for x in g["gt_regions"] if (x.get("text_share") or 0.0) < 0.5]
            gt_change = g["human_label"] == "LOCAL_CHANGE"
            pred = len(pub) > 0
            if g["human_label"] != "UNSURE":
                tp += gt_change and pred
                fp += (not gt_change) and pred
                fn += gt_change and not pred
                tn += (not gt_change) and not pred
            tot += len(gtb)
            hit += sum(1 for a in gtb if any(touches(a, b) for b in pb))
            false_regions += sum(1 for b in pb if not any(touches(a, b) for a in gtb))
        rows.append({"min_region_ink_pt": T, "pairs_TP": tp, "pairs_FP": fp, "pairs_FN": fn, "pairs_TN": tn,
                     "region_recall": round(hit / max(1, tot), 3), "gt_regions": tot, "gt_hit": hit,
                     "false_regions": false_regions})
    return rows


def desired_route(g):
    if g["human_label"] == "UNSURE":
        return None
    if g["scale"] == "major":
        return "MODE_2_REQUIRED"
    return "MODE_1_APPLICABLE"


def sweep_routes(gt, runs):
    rows = []
    for min_cov in (0.70, 0.80, 0.85, 0.90, 0.95):
        for max_chg in (0.05, 0.10, 0.25, 0.40):
            for max_reg in (10, 20, 40, 80, 160):
                ok = wrong_m2 = wrong_m1 = skipped = 0
                for pid, r in runs.items():
                    g = gt[pid]
                    want = desired_route(g)
                    if want is None:
                        continue
                    reg = r["registration"]
                    d = r["diff"]
                    if not reg["success"] or reg["coverage"]["sym_cov"] < min_cov \
                            or d["changed_ink_fraction"] > max_chg or d["n_regions_published"] > max_reg:
                        got = "MODE_2_REQUIRED"
                    else:
                        got = "MODE_1_APPLICABLE"
                    # degenerate inputs are routed away before the gates and are
                    # counted separately
                    if r["route"] in ("VISION_REQUIRED", "NO_GRAPHIC_COMPARISON"):
                        skipped += 1
                        continue
                    if got == want:
                        ok += 1
                    elif got == "MODE_2_REQUIRED":
                        wrong_m2 += 1
                    else:
                        wrong_m1 += 1
                rows.append({"min_sym_cov": min_cov, "max_changed_fraction": max_chg, "max_regions": max_reg,
                             "route_correct": ok, "handed_to_mode2_but_local": wrong_m2,
                             "kept_in_mode1_but_major": wrong_m1, "degenerate_skipped": skipped})
    return rows


def main():
    gt, runs = load()
    ink = sweep_min_ink(gt, runs)
    print("min_region_ink_pt sweep (MODE_1_APPLICABLE pairs only)")
    for r in ink:
        print(f"  T={r['min_region_ink_pt']:>3} pt  TP={r['pairs_TP']:>2} FP={r['pairs_FP']:>2} FN={r['pairs_FN']:>2} "
              f"TN={r['pairs_TN']:>2}  region_recall={r['region_recall']:.3f} ({r['gt_hit']}/{r['gt_regions']}) "
              f"false_regions={r['false_regions']:>3}")
    routes = sweep_routes(gt, runs)
    best = sorted(routes, key=lambda r: (-r["route_correct"], r["kept_in_mode1_but_major"]))[:8]
    print("\nrouting gate sweep — best rows")
    for r in best:
        print(f"  cov>={r['min_sym_cov']:.2f} chg<={r['max_changed_fraction']:.2f} regions<={r['max_regions']:>3} "
              f"-> correct {r['route_correct']:>2}, local sent to MODE 2: {r['handed_to_mode2_but_local']:>2}, "
              f"major kept in MODE 1: {r['kept_in_mode1_but_major']:>2}")
    (ART / "calibration.json").write_text(json.dumps(
        {"probe": "calibrate", "research_only": True,
         "min_ink_sweep": ink, "route_sweep": routes,
         "route_sweep_best": best}, ensure_ascii=False, indent=1), encoding="utf-8")
    print("\nwrote", ART / "calibration.json")


if __name__ == "__main__":
    main()

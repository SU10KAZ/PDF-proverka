#!/usr/bin/env python3
"""Score MODE 1 against the human ground truth.

Reported, per the brief: change-region recall, false change regions,
localization accuracy, pair-level FP/FN, border false positives, text-only and
table-only false positives, registration success, routing, latency.
"""
from __future__ import annotations

import json
import pathlib
import statistics
from collections import Counter

ART = pathlib.Path(__file__).resolve().parents[1] / "artifacts"


def iou(a, b):
    x0 = max(a[0], b[0]); y0 = max(a[1], b[1])
    x1 = min(a[2], b[2]); y1 = min(a[3], b[3])
    if x1 <= x0 or y1 <= y0:
        return 0.0
    inter = (x1 - x0) * (y1 - y0)
    ua = max(1e-6, (a[2] - a[0]) * (a[3] - a[1]))
    ub = max(1e-6, (b[2] - b[0]) * (b[3] - b[1]))
    return inter / (ua + ub - inter)


def touches(a, b, pad=6.0):
    return not (a[2] + pad < b[0] or a[0] - pad > b[2] or a[3] + pad < b[1] or a[1] - pad > b[3])


def main():
    gt = {r["pair_id"]: r for r in json.loads((ART / "human_ground_truth.json").read_text(encoding="utf-8"))["pairs"]}
    res = {r["pair_id"]: r for r in json.loads((ART / "local_diff_results.json").read_text(encoding="utf-8"))["results"]}
    rows = []
    for pid, g in gt.items():
        r = res.get(pid)
        if not r or "error" in (r or {}):
            rows.append({"pair_id": pid, "error": True})
            continue
        pub = r["change_regions"]
        pred_change = len(pub) > 0
        gt_change = (g["human_label"] == "LOCAL_CHANGE")
        gtb_all = [x["bbox_pt"] for x in g["gt_regions"]]
        gtb = [x["bbox_pt"] for x in g["gt_regions"] if (x.get("text_share") or 0.0) < 0.5]
        gtb_strict = [x["bbox_pt"] for x in g["gt_regions"] if (x.get("text_share") or 0.0) < 0.3]
        pb = [x["bbox"] for x in pub]
        matched_gt = sum(1 for a in gtb if any(touches(a, b) for b in pb))
        matched_pred = sum(1 for b in pb if any(touches(a, b) for a in gtb))
        ious = []
        for a in gtb:
            best = max((iou(a, b) for b in pb), default=0.0)
            if best > 0:
                ious.append(best)
        rows.append({
            "pair_id": pid, "bucket": g["bucket"], "discipline": g["discipline"],
            "gt_label": g["human_label"], "gt_scale": g["scale"], "eye": g["eye_verified"],
            "route": r["route"], "verdict": r["verdict"],
            "reg_success": r["registration"]["success"],
            "reg_method": r["registration"]["method"],
            "sym_cov": r["registration"]["coverage"]["sym_cov"],
            "residual": r["registration"]["residual"]["median"],
            "anchor_cov": r["registration"]["anchors"]["coverage"],
            "changed_fraction": r["diff"]["changed_ink_fraction"],
            "n_pred": len(pub), "n_gt": len(gtb), "n_gt_all": len(gtb_all),
            "n_gt_strict": len(gtb_strict),
            "matched_gt_strict": sum(1 for a in gtb_strict if any(touches(a, b) for b in pb)),
            "matched_gt": matched_gt, "matched_pred": matched_pred,
            "unmatched_pred": len(pb) - matched_pred,
            "median_iou": round(statistics.median(ious), 3) if ious else None,
            "pred_change": pred_change, "gt_change": gt_change,
            "latency_s": r["latency_s"],
            "extraction_precision": [r["extraction_quality"]["left"]["precision"],
                                     r["extraction_quality"]["right"]["precision"]],
            "extraction_recall": [r["extraction_quality"]["left"]["recall"],
                                  r["extraction_quality"]["right"]["recall"]],
            "text_as_curves": [r["extraction_quality"]["left"]["text_as_curves_suspected"],
                               r["extraction_quality"]["right"]["text_as_curves_suspected"]],
            "raster_backed": [r["extraction_quality"]["left"]["raster_backed"],
                              r["extraction_quality"]["right"]["raster_backed"]],
            "border_regions": sum(1 for x in pub if x.get("border_ambiguity")),
            "types": dict(Counter(x.get("change_type") for x in pub)),
        })

    ok = [x for x in rows if not x.get("error")]
    answered = [x for x in ok if x["route"] in ("MODE_1_APPLICABLE", "VISION_REQUIRED")]
    tp = sum(1 for x in answered if x["gt_change"] and x["pred_change"])
    fp = sum(1 for x in answered if not x["gt_change"] and x["pred_change"] and x["gt_label"] != "UNSURE")
    fn = sum(1 for x in answered if x["gt_change"] and not x["pred_change"])
    tn = sum(1 for x in answered if not x["gt_change"] and not x["pred_change"] and x["gt_label"] != "UNSURE")

    applicable = [x for x in ok if x["route"] == "MODE_1_APPLICABLE"]
    gt_regions_total = sum(x["n_gt"] for x in answered)
    gt_regions_hit = sum(x["matched_gt"] for x in answered)
    false_regions = sum(x["unmatched_pred"] for x in answered)
    false_regions_on_nochange = sum(x["n_pred"] for x in answered if not x["gt_change"])

    summary = {
        "pairs": len(ok),
        "routes": dict(Counter(x["route"] for x in ok)),
        "gt_labels": dict(Counter(x["gt_label"] for x in ok)),
        "answered_by_mode1": len(answered),
        "pair_level": {"TP": tp, "FP": fp, "FN": fn, "TN": tn,
                       "precision": round(tp / max(1, tp + fp), 3),
                       "recall": round(tp / max(1, tp + fn), 3)},
        "region_level_mode1_applicable": {
            "gt_regions": sum(x["n_gt"] for x in applicable),
            "gt_regions_hit": sum(x["matched_gt"] for x in applicable),
            "region_recall": round(sum(x["matched_gt"] for x in applicable) / max(1, sum(x["n_gt"] for x in applicable)), 3),
            "gt_regions_strict_text_cutoff_0.3": sum(x["n_gt_strict"] for x in applicable),
            "gt_regions_strict_hit": sum(x["matched_gt_strict"] for x in applicable),
            "region_recall_strict": round(sum(x["matched_gt_strict"] for x in applicable) / max(1, sum(x["n_gt_strict"] for x in applicable)), 3),
            "false_regions": sum(x["unmatched_pred"] for x in applicable),
            "false_regions_on_no_change_pairs": sum(x["n_pred"] for x in applicable if not x["gt_change"]),
            "median_iou": round(statistics.median([x["median_iou"] for x in applicable if x["median_iou"]]), 3)
            if any(x["median_iou"] for x in applicable) else None,
        },
        "pair_level_mode1_applicable": {
            "TP": sum(1 for x in applicable if x["gt_change"] and x["pred_change"]),
            "FP": sum(1 for x in applicable if not x["gt_change"] and x["pred_change"] and x["gt_label"] != "UNSURE"),
            "FN": sum(1 for x in applicable if x["gt_change"] and not x["pred_change"]),
            "TN": sum(1 for x in applicable if not x["gt_change"] and not x["pred_change"] and x["gt_label"] != "UNSURE"),
        },
        "region_level": {"gt_regions": gt_regions_total, "gt_regions_hit": gt_regions_hit,
                         "region_recall": round(gt_regions_hit / max(1, gt_regions_total), 3),
                         "false_regions_total": false_regions,
                         "false_regions_on_no_change_pairs": false_regions_on_nochange,
                         "median_iou": round(statistics.median([x["median_iou"] for x in answered if x["median_iou"]]), 3)
                         if any(x["median_iou"] for x in answered) else None},
        "registration": {
            "success_rate": round(sum(1 for x in ok if x["reg_success"]) / max(1, len(ok)), 3),
            "methods": dict(Counter(x["reg_method"] for x in ok)),
            "sym_cov_median": round(statistics.median([x["sym_cov"] for x in ok]), 4),
            "residual_median": round(statistics.median([x["residual"] for x in ok if x["residual"] is not None]), 4),
        },
        "negative_controls": {
            b: {"pairs": sum(1 for x in ok if x["bucket"] == b),
                "pairs_with_published_regions": sum(1 for x in ok if x["bucket"] == b and x["n_pred"] > 0),
                "regions": sum(x["n_pred"] for x in ok if x["bucket"] == b)}
            for b in ("unchanged", "text_only", "table_only", "repack")
        },
        "latency_s": {"median": round(statistics.median([x["latency_s"] for x in ok]), 1),
                      "p90": round(sorted(x["latency_s"] for x in ok)[int(0.9 * len(ok))], 1),
                      "max": round(max(x["latency_s"] for x in ok), 1)},
        "extraction": {
            "precision_min": min([v for x in ok for v in x["extraction_precision"] if v is not None]),
            "precision_median": round(statistics.median([v for x in ok for v in x["extraction_precision"] if v is not None]), 4),
            "recall_min": min([v for x in ok for v in x["extraction_recall"] if v is not None]),
            "recall_median": round(statistics.median([v for x in ok for v in x["extraction_recall"] if v is not None]), 4),
            "blocks_with_recall_below_0_95": sum(1 for x in ok for v in x["extraction_recall"] if v is not None and v < 0.95),
            "blocks_measured": sum(1 for x in ok for v in x["extraction_recall"] if v is not None),
            "text_as_curves_pairs": sum(1 for x in ok if any(x["text_as_curves"])),
            "raster_backed_pairs": sum(1 for x in ok if any(x["raster_backed"])),
        },
    }
    out = {"probe": "evaluate", "research_only": True, "summary": summary, "pairs": rows}
    (ART / "evaluation.json").write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=1))
    print()
    print(f"{'pair':28s} {'gt':16s} {'route':20s} pred/gt  falsepred  iou")
    for x in sorted(ok, key=lambda x: x["pair_id"]):
        flag = ""
        if x["route"] in ("MODE_1_APPLICABLE", "VISION_REQUIRED"):
            if x["gt_change"] and not x["pred_change"]:
                flag = " <-- MISS"
            elif not x["gt_change"] and x["pred_change"] and x["gt_label"] != "UNSURE":
                flag = " <-- FALSE"
        print(f"{x['pair_id']:28s} {x['gt_label']:16s} {x['route']:20s} {x['n_pred']:>3}/{x['n_gt']:<3} "
              f"{x['unmatched_pred']:>4}  {str(x['median_iou']):>6}{flag}")


if __name__ == "__main__":
    main()

# -*- coding: utf-8 -*-
"""Aggregate advB attack #1: is the object layer load-bearing?  Plus a FAIR
comparison of the global scalar against the ledger (same NOT_COMPARABLE guard)."""
import json, sys
from pathlib import Path
ART = Path(__file__).resolve().parent.parent / "artifacts"

rows = {}
for p in sorted((ART / "advB").glob("a1_*.jsonl")):
    for line in open(p, encoding="utf-8"):
        r = json.loads(line)
        rows[r["pair_id"]] = r

usable = {k: v for k, v in rows.items()
          if "error" not in v and v["expected"] in ("GRAPHIC_CHANGE", "NO_GRAPHIC_CHANGE")}
errors = {k: v["error"] for k, v in rows.items() if "error" in v}

def conf(pred_key):
    tp = fp = tn = fn = 0
    wrong = []
    for k, v in usable.items():
        pred = v[pred_key]
        exp = v["expected"]
        if exp == "GRAPHIC_CHANGE" and pred == "GRAPHIC_CHANGE": tp += 1
        elif exp == "GRAPHIC_CHANGE": fn += 1; wrong.append((k, "FN"))
        elif pred == "GRAPHIC_CHANGE": fp += 1; wrong.append((k, "FP"))
        else: tn += 1
    return {"TP": tp, "FP": fp, "TN": tn, "FN": fn,
            "accuracy": round((tp + tn) / max(1, len(usable)), 4), "wrong": wrong}

out = {"n_pairs_total": len(rows), "n_usable": len(usable), "errors": errors}
out["identical_obj_vs_seg"] = sum(1 for v in rows.values()
                                  if "error" not in v and v["v_obj"] == v["v_seg"])
out["n_compared"] = sum(1 for v in rows.values() if "error" not in v)
out["disagreements"] = [k for k, v in rows.items()
                        if "error" not in v and v["v_obj"] != v["v_seg"]]
out["top_record_iou_median"] = None
ious = sorted(v["top_iou"] for v in rows.values() if v.get("top_iou") is not None)
if ious:
    out["top_record_iou_median"] = round(ious[len(ious) // 2], 4)
    out["top_record_iou_min"] = round(ious[0], 4)
    out["n_top_record_iou"] = len(ious)
out["confusion_OBJ"] = conf("v_obj")
out["confusion_SEG"] = conf("v_seg")

# object-keyed comparators on the same pairs
def keyed(name, fn):
    tp = fp = tn = fnn = 0
    for k, v in usable.items():
        pred = "GRAPHIC_CHANGE" if fn(v) else "NO_GRAPHIC_CHANGE"
        exp = v["expected"]
        if exp == "GRAPHIC_CHANGE" and pred == "GRAPHIC_CHANGE": tp += 1
        elif exp == "GRAPHIC_CHANGE": fnn += 1
        elif pred == "GRAPHIC_CHANGE": fp += 1
        else: tn += 1
    return {"TP": tp, "FP": fp, "TN": tn, "FN": fnn,
            "accuracy": round((tp + tn) / max(1, len(usable)), 4)}

out["confusion_count_objects"] = keyed("count", lambda v: v["d_obj"] != 0)
out["confusion_object_id"] = keyed("objid", lambda v: v["objid_mismatch"] > 0)
out["confusion_churn"] = keyed("churn", lambda v: v["churn_1to1"] < 0.98)

# --- FAIR scalar: same registration guard the ledger gets (loc N-7 / L5) -----------
GUARD = 0.90
def scalar_curve(guard):
    pts = []
    for thr in [0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.10, 0.15, 0.20, 0.30]:
        tp = fp = tn = fnn = 0
        for k, v in usable.items():
            if guard and v["unmatched_share"] >= GUARD:
                pred = "NO_GRAPHIC_CHANGE"      # NOT_COMPARABLE -> routed away
            else:
                pred = "GRAPHIC_CHANGE" if v["unmatched_share"] >= thr else "NO_GRAPHIC_CHANGE"
            exp = v["expected"]
            if exp == "GRAPHIC_CHANGE" and pred == "GRAPHIC_CHANGE": tp += 1
            elif exp == "GRAPHIC_CHANGE": fnn += 1
            elif pred == "GRAPHIC_CHANGE": fp += 1
            else: tn += 1
        pts.append({"thr": thr, "TP": tp, "FP": fp, "TN": tn, "FN": fnn,
                    "accuracy": round((tp + tn) / max(1, len(usable)), 4)})
    return pts
out["scalar_with_guard"] = scalar_curve(True)
out["scalar_no_guard"] = scalar_curve(False)
out["rows"] = rows
json.dump(out, open(ART / "advB_ablation.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
print(json.dumps({k: v for k, v in out.items() if k != "rows"}, ensure_ascii=False, indent=1))

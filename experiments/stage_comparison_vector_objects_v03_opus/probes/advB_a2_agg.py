# -*- coding: utf-8 -*-
"""Aggregate advB attack #2 (rewrites outside set A) into the G1-a metrics."""
import json, statistics as st, sys
from pathlib import Path
ART = Path(__file__).resolve().parent.parent / "artifacts"

rows = []
for p in sorted((ART / "advB").glob("rw_*.jsonl")):
    for line in open(p, encoding="utf-8"):
        r = json.loads(line)
        if "error" not in r:
            rows.append(r)

names = []
for r in rows:
    for k in r["rewrites"]:
        if k not in names:
            names.append(k)

def q(v, p):
    if not v: return None
    v = sorted(v); return v[min(len(v)-1, max(0, int(round(p*(len(v)-1)))))]

out = {"n_blocks": len(rows),
       "disciplines": sorted({r["discipline"] for r in rows}),
       "n_disciplines": len({r["discipline"] for r in rows}),
       "n_segments_total": sum(r["n_seg"] for r in rows),
       "per_rewrite": {}}
for nm in names:
    vals = [r["rewrites"][nm] for r in rows if nm in r["rewrites"] and "error" not in r["rewrites"][nm]]
    vals = [v for v in vals if v["bite"] > 0]
    if not vals:
        out["per_rewrite"][nm] = {"n": 0}
        continue
    o11 = [v["churn"]["one_to_one"] for v in vals]
    bid = [v["boundary_identical"] for v in vals]
    dob = [v["d_obj"] for v in vals]
    out["per_rewrite"][nm] = {
        "n": len(vals),
        "d_obj_zero": sum(1 for v in dob if v == 0),
        "d_obj_zero_share": round(sum(1 for v in dob if v == 0)/len(dob), 4),
        "d_obj_abs_median": st.median([abs(v) for v in dob]),
        "d_obj_abs_max": max(abs(v) for v in dob),
        "boundary_identical_median": round(st.median(bid), 4),
        "boundary_identical_p10": round(q(bid, 0.10), 4),
        "boundary_identical_min": round(min(bid), 4),
        "n_blocks_boundary_perfect": sum(1 for v in bid if v >= 0.99999),
        "one_to_one_median": round(st.median(o11), 4),
        "one_to_one_p10": round(q(o11, 0.10), 4),
        "one_to_one_min": round(min(o11), 4),
        "n_blocks_1to1_below_0.9": sum(1 for v in o11 if v < 0.9),
        "n_blocks_layer_UNCHANGED": sum(1 for v in vals
                                        if v["d_obj"] == 0 and v["boundary_identical"] >= 0.99999
                                        and v["churn"]["one_to_one"] >= 0.99999),
    }
    out["per_rewrite"][nm]["share_layer_UNCHANGED"] = round(
        out["per_rewrite"][nm]["n_blocks_layer_UNCHANGED"] / len(vals), 4)

# by density
buckets = {}
for r in rows:
    b = r["bucket"]
    for nm in names:
        v = r["rewrites"].get(nm)
        if not v or "error" in v or v["bite"] == 0: continue
        buckets.setdefault(nm, {}).setdefault(b, []).append(v["churn"]["one_to_one"])
out["by_density_one_to_one_median"] = {nm: {b: round(st.median(v), 4) for b, v in d.items()}
                                       for nm, d in buckets.items()}
json.dump(out, open(ART / "advB_rewrites.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
print(json.dumps({k: v for k, v in out.items() if k != "by_density_one_to_one_median"}, ensure_ascii=False, indent=1))

# -*- coding: utf-8 -*-
"""Aggregate advB attack #3: single-axis counterfactual vs compound counterfactual."""
import json, statistics as st, sys
from pathlib import Path
ART = Path(__file__).resolve().parent.parent / "artifacts"

rows = []
for p in sorted(list((ART / "advB").glob("cmp_*.jsonl")) + list((ART / "advB").glob("cmpS_*.jsonl"))):
    for line in open(p, encoding="utf-8"):
        r = json.loads(line)
        if "error" not in r:
            rows.append(r)
seen = set()
uniq = []
for r in rows:
    k = (r["block_id"], r.get("obj_bucket"))
    if k in seen:
        continue
    seen.add(k)
    uniq.append(r)
rows = uniq

ARMS = ["single", "A1_pack", "A2_scale_0.2pct", "A3_scale_0.5pct", "A4_scale_1pct"]
NEG = ["N3_scale_0.5pct_NOEDIT", "N4_scale_1pct_NOEDIT"]
out = {"n_instances": len(rows),
       "n_carriers": len({r["block_id"] for r in rows}),
       "disciplines": sorted({r["discipline"] for r in rows}),
       "by_arm": {}, "by_arm_and_bucket": {}}
for a in ARMS + NEG:
    vs = [r[a] for r in rows if a in r and "error" not in r[a]]
    if not vs:
        continue
    d = {"n": len(vs),
         "n_false_records_median": st.median([v["n_false"] for v in vs]),
         "n_false_records_mean": round(st.mean([v["n_false"] for v in vs]), 3),
         "n_false_records_max": max(v["n_false"] for v in vs),
         "share_with_any_false": round(sum(1 for v in vs if v["n_false"] > 0) / len(vs), 4),
         "unmatched_share_median": round(st.median([v["unmatched_share"] for v in vs]), 6),
         "unmatched_share_p90": round(sorted(v["unmatched_share"] for v in vs)[int(0.9 * (len(vs) - 1))], 6),
         "n_rec_median": st.median([v["n_rec"] for v in vs])}
    if a in ARMS:
        d["recall_localised"] = round(sum(1 for v in vs if v["localised"]) / len(vs), 4)
        d["named"] = round(sum(1 for v in vs if v["named"]) / len(vs), 4)
    out["by_arm"][a] = d

for a in ARMS:
    for b in ("tiny", "small", "large"):
        vs = [r[a] for r in rows if r.get("obj_bucket") == b and a in r and "error" not in r[a]]
        if vs:
            out["by_arm_and_bucket"][f"{a}|{b}"] = {
                "n": len(vs),
                "recall": round(sum(1 for v in vs if v["localised"]) / len(vs), 4),
                "false_mean": round(st.mean([v["n_false"] for v in vs]), 3)}

# paired: how many carriers lose the change when the extra axes are added
for a in ARMS[1:]:
    lost = won = 0
    for r in rows:
        if a in r and "error" not in r[a] and "error" not in r["single"]:
            if r["single"]["localised"] and not r[a]["localised"]:
                lost += 1
            elif not r["single"]["localised"] and r[a]["localised"]:
                won += 1
    out["by_arm"].setdefault(a, {}).update({"paired_lost_vs_single": lost, "paired_won_vs_single": won})
json.dump(out, open(ART / "advB_compound.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
print(json.dumps(out, ensure_ascii=False, indent=1))

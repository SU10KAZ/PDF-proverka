# -*- coding: utf-8 -*-
"""Ablation of the relation whitelist and of the `label` field as an ADDRESS carrier.

Source: rel_r3_*.json (675 counterfactual changes on 58 real blocks; each row records,
per relation type, whether the changed object had an address that is any/stable/unique).
"""
from __future__ import annotations
import glob, json, itertools, sys
from pathlib import Path
ART = Path(__file__).resolve().parents[1] / "artifacts"

TYPES = ["ADJACENT", "CONNECTED_TO", "ALIGNED", "LABEL_ANCHOR",
         "LEADER_TO", "PART_OF", "INSIDE", "CONTAINS", "REPEATED_WITH"]
KEEP = ["ADJACENT", "CONNECTED_TO", "ALIGNED", "LABEL_ANCHOR"]

rows, skipped = [], 0
for f in sorted(glob.glob(str(ART / "rel_r3_*.json"))):
    for c in json.load(open(f, encoding="utf-8")):
        for r in c.get("runs", []):
            if "addr" in r:
                rows.append(r["addr"])
            else:
                skipped += 1

def cov(ts):
    return round(sum(1 for a in rows if any(a[t]["usable"] for t in ts)) / len(rows), 4)

out = {
    "n_instances": len(rows), "rows_without_addr": skipped,
    "coverage_all_9_types": cov(TYPES),
    "coverage_keep_4": cov(KEEP),
    "leave_one_out_of_keep_4": {t: cov([x for x in KEEP if x != t]) for t in KEEP},
    "solo": {t: cov([t]) for t in TYPES},
    "label_ablation": {
        "with_label_anchor": cov(KEEP),
        "without_label_anchor": cov([t for t in KEEP if t != "LABEL_ANCHOR"]),
        "instances_losing_their_only_address":
            sum(1 for a in rows
                if a["LABEL_ANCHOR"]["usable"]
                and not any(a[t]["usable"] for t in KEEP if t != "LABEL_ANCHOR")),
    },
}
(ART / "ctr_label_address.json").write_text(json.dumps(out, ensure_ascii=False, indent=1),
                                            encoding="utf-8")
print(json.dumps(out, ensure_ascii=False, indent=1))

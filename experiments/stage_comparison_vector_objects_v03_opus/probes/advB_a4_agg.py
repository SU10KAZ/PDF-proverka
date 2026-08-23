# -*- coding: utf-8 -*-
"""Aggregate advB attack #4 (real negative / positive controls)."""
import json, statistics as st
from pathlib import Path
ART = Path(__file__).resolve().parent.parent / "artifacts"
rows = []
for p in sorted((ART / "advB").glob("neg_*.jsonl")):
    for line in open(p, encoding="utf-8"):
        rows.append(json.loads(line))
ok = [r for r in rows if "error" not in r and "skip" not in r]
out = {"n_rows": len(rows), "n_ok": len(ok),
       "n_skipped_no_vector": sum(1 for r in rows if r.get("skip")),
       "populations": {}}
for pop in ("QUIET", "LOUD"):
    s = [r for r in ok if r["pop"] == pop]
    if not s:
        continue
    zero = [r for r in s if r["unmatched_len"] == 0.0]
    d = {"n": len(s), "n_disciplines": len({r["discipline"] for r in s}),
         "disciplines": sorted({r["discipline"] for r in s}),
         "n_docs": len({r["doc_id"] for r in s}),
         "ink_exactly_equal": len(zero),
         "unmatched_share_median": round(st.median([r["unmatched_share"] for r in s]), 8)}
    if zero:
        d["on_ink_identical"] = {
            "n": len(zero),
            "d_obj_nonzero": sum(1 for r in zero if r["d_obj"] != 0),
            "objid_mismatch_nonzero": sum(1 for r in zero if r["objid_mismatch"] > 0),
            "churn_below_098": sum(1 for r in zero if r["churn_1to1"] < 0.98),
            "worst": sorted([{k: r[k] for k in ("doc_id", "discipline", "block_a", "n_seg_a",
                                                "n_obj_a", "n_obj_b", "d_obj",
                                                "objid_mismatch", "churn_1to1")}
                             for r in zero], key=lambda x: -abs(x["d_obj"]))[:5]}
    d["layer_silent"] = sum(1 for r in s if r["d_obj"] == 0 and r["objid_mismatch"] == 0)
    d["S_source_differs"] = sum(1 for r in s if r["src_a"] != r["src_b"])
    d["S_value_differs"] = sum(1 for r in s if abs(r["S_a"] - r["S_b"]) > 1e-6)
    out["populations"][pop] = d
out["rows"] = ok
json.dump(out, open(ART / "advB_negctrl.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
print(json.dumps({k: v for k, v in out.items() if k != "rows"}, ensure_ascii=False, indent=1))

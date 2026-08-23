# -*- coding: utf-8 -*-
"""advB attack #6 — independent recomputation of stage-A/B numbers that are cheap."""
import json, statistics as st
from pathlib import Path
ART = Path(__file__).resolve().parent.parent / "artifacts"
out = {}

# --- census -----------------------------------------------------------------------
rows = [json.loads(l) for l in open(ART / "cns_block_classes.jsonl", encoding="utf-8")]
n = len(rows)
lt5_all = sum(1 for r in rows if (r.get("n_text") or 0) < 5)
with_geom = [r for r in rows if r["n_seg"] >= 1]
lt5_geom = sum(1 for r in with_geom if (r.get("n_text") or 0) < 5)
segs = [r["n_seg"] for r in rows]
out["census"] = {
    "n_blocks": n,
    "n_text_lt5_all": lt5_all, "share_all": round(lt5_all / n, 4),
    "n_blocks_with_geometry": len(with_geom),
    "n_text_lt5_with_geometry": lt5_geom, "share_with_geometry": round(lt5_geom / len(with_geom), 4),
    "n_seg_median": st.median(segs), "n_seg_mean": round(st.mean(segs), 1),
    "share_ge_10000_segments": round(sum(1 for s in segs if s >= 10000) / n, 4),
    "share_lt_10_segments": round(sum(1 for s in segs if s < 10) / n, 4),
    "checks": {"G5'_7.14pct_of_43261": lt5_all == 3089 and n == 43261,
               "N-4_5.3pct_of_42311": lt5_geom == 2247 and len(with_geom) == 42311},
}

# --- pair index -------------------------------------------------------------------
idx = json.load(open(ART / "mine_pair_index.json", encoding="utf-8"))
prs = idx["pairs"]
out["M2_same_pdf_sha256"] = {"n_version_pairs": len(prs),
                             "n_same_sha": sum(1 for p in prs if p.get("sha_a") == p.get("sha_b"))}

# --- screened block pairs ---------------------------------------------------------
al = [json.loads(l) for l in open(ART / "mine_align2.jsonl", encoding="utf-8")]
stamp = sum(1 for r in al if r.get("cat_a") == "stamp" or r.get("cat_b") == "stamp")
diffs = [(r.get("align2") or {}).get("diff_frac_block") for r in al]
diffs = [d for d in diffs if d is not None]
out["M6_stamp_share"] = {"n_pairs": len(al), "n_stamp": stamp, "share": round(stamp / len(al), 4)}
out["M13_quiet_pairs"] = {"n_le_2e-4": sum(1 for d in diffs if d <= 2e-4),
                          "n_exactly_zero": sum(1 for d in diffs if d == 0.0),
                          "n_with_measure": len(diffs)}

# --- carrier set of G1 ------------------------------------------------------------
rw = []
for p in sorted((ART / "advB").glob("rw_*.jsonl")):
    for line in open(p, encoding="utf-8"):
        rw.append(json.loads(line))
out["G1_carrier_set_reproduced"] = {"n_blocks": len(rw),
                                    "n_segments": sum(r["n_seg"] for r in rw),
                                    "n_disciplines": len({r["discipline"] for r in rw})}
json.dump(out, open(ART / "advB_recount.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
print(json.dumps(out, ensure_ascii=False, indent=1))
